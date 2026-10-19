/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.spark.extensions;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import org.apache.iceberg.index.vector.VectorIndexTypes;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A/B for the fetch-back leg of an indexed vector query: the same rows and the same vector index on
 * a Lance-backed table and on a parquet-backed table. The index lookup is identical in both, so the
 * difference is what Iceberg has to read to turn the hits into rows.
 *
 * <p>Result is written to build/lance-poc-fetchback.txt for the POC report.
 */
class TestLanceFetchBack {

  private static final int ROWS = 200_000;
  private static final int DIM = 32;
  private static final int TOP_K = 10;

  private static SparkSession spark;
  private static Path report;

  @BeforeAll
  static void startSpark(@TempDir Path warehouse) {
    VectorIndexTypes.register();
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("lance-fetch-back")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "4")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", warehouse.toUri().toString())
            .getOrCreate();
    report = Path.of("build", "lance-poc-fetchback.txt");
  }

  @AfterAll
  static void stopSpark() {
    if (spark != null) {
      spark.stop();
    }
  }

  private String createIndexedTable(String name, String format) {
    String table = "local.db." + name;
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
    spark.sql("DROP TABLE IF EXISTS " + table);
    spark.sql(
        "CREATE TABLE "
            + table
            + " (id bigint, payload string, embedding array<float>) USING iceberg TBLPROPERTIES ("
            + "'write.format.default' = '"
            + format
            + "')");

    // deterministic, incompressible payload and vectors: a synthetic payload of repeated
    // characters would compress to nothing and hide the read amplification this test is for
    spark.sql(
        "INSERT INTO "
            + table
            + " SELECT id, md5(cast(id as string)), "
            + "transform(sequence(0, "
            + (DIM - 1)
            + "), x -> cast(sin(cast(id as double) + cast(x as double)) as float)) FROM range("
            + ROWS
            + ")");

    spark.sql(
        "CALL local.system.build_vector_index("
            + "table => '"
            + table
            + "', column => 'embedding', dimension => "
            + DIM
            + ", mode => 'hnsw')");
    return table;
  }

  private List<Row> search(String table, String queryVector) {
    return spark
        .sql(
            "SELECT id, payload FROM "
                + table
                + " ORDER BY vec_distance(embedding, "
                + queryVector
                + ") LIMIT "
                + TOP_K)
        .collectAsList();
  }

  private long bestMillis(String table, String queryVector) {
    String query =
        "SELECT id, payload FROM "
            + table
            + " ORDER BY vec_distance(embedding, "
            + queryVector
            + ") LIMIT "
            + TOP_K;
    long best = Long.MAX_VALUE;
    for (int i = 0; i < 5; i++) {
      long start = System.nanoTime();
      spark.sql(query).collectAsList();
      best = Math.min(best, (System.nanoTime() - start) / 1_000_000);
    }
    return best;
  }

  @Test
  public void lanceAndParquetFetchBackTheSameRows() throws Exception {
    String lance = createIndexedTable("fetch_lance", "lance");
    String parquet = createIndexedTable("fetch_parquet", "parquet");

    // a query vector that is exactly row 12345's vector, so the top hit is unambiguous
    String queryVector = "array(" + vectorFor(12345) + ")";

    List<Row> lanceHits = search(lance, queryVector);
    List<Row> parquetHits = search(parquet, queryVector);

    assertThat(lanceHits).hasSize(TOP_K);
    assertThat(lanceHits).containsExactlyElementsOf(parquetHits);
    // the query vector is row 12345's own vector, so the closest row is 12345
    assertThat(lanceHits.get(0).getLong(0)).isEqualTo(12345L);
    assertThat(lanceHits.get(0).getString(1))
        .isEqualTo(md5Hex(String.valueOf(lanceHits.get(0).getLong(0))));

    long lanceMs = bestMillis(lance, queryVector);
    long parquetMs = bestMillis(parquet, queryVector);

    long lanceBytes = dataBytes(lance);
    long parquetBytes = dataBytes(parquet);

    // the fetch-back leg on its own: read the same ten row positions from one data file of each
    // table, with no index search and no merge in the measurement
    long lanceFetchMs = bestFetchMillis(lance);
    long parquetFetchMs = bestFetchMillis(parquet);
    long lanceFetchBytes = lanceBytes;
    long parquetFetchBytes = parquetBytes;

    // unscanned baseline: no index, the planner falls back to a full scan
    spark.sql("DROP TABLE IF EXISTS local.db.fetch_scan");
    spark.sql(
        "CREATE TABLE local.db.fetch_scan (id bigint, payload string, embedding array<float>) "
            + "USING iceberg TBLPROPERTIES ('write.format.default' = 'lance')");
    spark.sql("INSERT INTO local.db.fetch_scan SELECT * FROM " + lance);
    long scanMs = bestMillis("local.db.fetch_scan", queryVector);

    String summary =
        "rows="
            + ROWS
            + " dim="
            + DIM
            + " topK="
            + TOP_K
            + "\n"
            + "query whole plan   lance="
            + lanceMs
            + "ms parquet="
            + parquetMs
            + "ms\n"
            + "fetchback 10 rows  lance="
            + lanceFetchMs
            + "ms parquet="
            + parquetFetchMs
            + "ms\n"
            + "data bytes         lance="
            + lanceBytes
            + " parquet="
            + parquetBytes
            + "\n"
            + "full scan no index (lance)="
            + scanMs
            + "ms\n"
            + "topId="
            + lanceHits.get(0).getLong(0)
            + " sameResults=true\n";

    Files.createDirectories(report.getParent());
    Files.writeString(
        report, summary, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    System.out.println("[LANCE-FETCHBACK]\\n" + summary);
  }

  private String vectorFor(long seed) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < DIM; i++) {
      if (i > 0) {
        sb.append(", ");
      }
      // the query vector must match the stored floats exactly, so it is built the same way
      sb.append((float) Math.sin(seed + i)).append("f");
    }
    return sb.toString();
  }

  /**
   * Times the fetch-back leg alone: {@link org.apache.iceberg.index.spark.IndexEntries#rowsAt} for
   * ten spread-out positions of one data file, which is what turns index hits into rows.
   */
  /** md5 hex of a string, matching Spark's md5() so the payload can be checked without a helper. */
  private static String md5Hex(String value) throws Exception {
    byte[] digest =
        java.security.MessageDigest.getInstance("MD5")
            .digest(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    StringBuilder hex = new StringBuilder(digest.length * 2);
    for (byte b : digest) {
      hex.append(String.format("%02x", b));
    }
    return hex.toString();
  }

  private long bestFetchMillis(String table) throws Exception {
    org.apache.iceberg.Table icebergTable =
        org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark, table);
    long snapshotId = icebergTable.currentSnapshot().snapshotId();

    List<Row> files =
        spark.sql("SELECT file_path, record_count FROM " + table + ".files").collectAsList();
    String dataFile = files.get(0).getString(0);
    long rowCount = files.get(0).getLong(1);

    java.util.Map<String, java.util.List<Long>> positionsByFile = new java.util.HashMap<>();
    java.util.List<Long> positions = new java.util.ArrayList<>();
    for (int i = 0; i < TOP_K; i++) {
      positions.add(Math.min(rowCount - 1, (long) i * (rowCount / TOP_K)));
    }
    positionsByFile.put(dataFile, positions);

    long best = Long.MAX_VALUE;
    for (int i = 0; i < 5; i++) {
      long start = System.nanoTime();
      java.util.Map<String, java.util.Map<Long, Row>> rows =
          org.apache.iceberg.index.spark.IndexEntries.rowsAt(
              spark,
              icebergTable,
              snapshotId,
              positionsByFile,
              java.util.Arrays.asList("id", "payload", "embedding"),
              null);
      best = Math.min(best, (System.nanoTime() - start) / 1_000_000);
      if (rows.size() != 1 || rows.values().iterator().next().size() != TOP_K) {
        throw new IllegalStateException("fetch back returned " + rows);
      }
    }
    return best;
  }

  private long dataBytes(String table) {
    return spark.sql("SELECT sum(file_size_in_bytes) FROM " + table + ".files").head().getLong(0);
  }
}
