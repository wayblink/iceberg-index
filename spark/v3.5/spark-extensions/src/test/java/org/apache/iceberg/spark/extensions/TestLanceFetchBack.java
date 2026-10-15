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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import org.apache.iceberg.index.vector.VectorIndexTypes;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A/B for the fetch-back leg of an indexed vector query: the same rows and the same vector index
 * on a Lance-backed table and on a parquet-backed table. The index lookup is identical in both, so
 * the difference is what Iceberg has to read to turn the hits into rows.
 *
 * <p>Result is written to target/lance-poc-fetchback.txt for the POC report.
 */
class TestLanceFetchBack {

  private static final int ROWS = 50_000;
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
    report = Path.of("target", "lance-poc-fetchback.txt");
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

    // deterministic one-hot vectors: row id has 1.0 at position id % DIM, 0.001 elsewhere
    spark.sql(
        "INSERT INTO "
            + table
            + " SELECT id, rpad(cast(id as string), 40, 'p'), "
            + "transform(sequence(0, "
            + (DIM - 1)
            + "), x -> cast(case when x = cast(id % "
            + DIM
            + " as int) then 1.0 else 0.001 end as float)) FROM range("
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

  @Disabled(
      "Blocked: the index layer reads data files with spark.read().parquet(...) "
          + "(IndexEntries.liveRowsOf and the vector build path), so an index cannot be built on or "
          + "resolved against a Lance-backed table yet. Re-enable once those reads go through the "
          + "format model for the file's format.")
  @Test
  public void lanceAndParquetFetchBackTheSameRows() throws IOException {
    String lance = createIndexedTable("fetch_lance", "lance");
    String parquet = createIndexedTable("fetch_parquet", "parquet");

    // a query vector that is exactly row 12345's vector, so the top hit is unambiguous
    String queryVector = "array(" + unitVectorFor(12345) + ")";

    List<Row> lanceHits = search(lance, queryVector);
    List<Row> parquetHits = search(parquet, queryVector);

    assertThat(lanceHits).hasSize(TOP_K);
    assertThat(lanceHits).containsExactlyElementsOf(parquetHits);
    // the one-hot query vector is row 12345's vector, so the hits are the rows sharing id % DIM
    assertThat(Math.floorMod(lanceHits.get(0).getLong(0), DIM))
        .isEqualTo(Math.floorMod(12345L, DIM));
    assertThat(lanceHits.get(0).getString(1)).startsWith("payload-");

    long lanceMs = bestMillis(lance, queryVector);
    long parquetMs = bestMillis(parquet, queryVector);

    long lanceBytes = dataBytes(lance);
    long parquetBytes = dataBytes(parquet);

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
            + "\\n"
            + "lance:   bestMs="
            + lanceMs
            + " dataBytes="
            + lanceBytes
            + "\\n"
            + "parquet: bestMs="
            + parquetMs
            + " dataBytes="
            + parquetBytes
            + "\\n"
            + "fullScanWithoutIndex(lance, bestMs)="
            + scanMs
            + "\\n"
            + "topId="
            + lanceHits.get(0).getLong(0)
            + " sameResults=true\\n";

    Files.createDirectories(report.getParent());
    Files.writeString(
        report, summary, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    System.out.println("[LANCE-FETCHBACK]\\n" + summary);
  }

  private String unitVectorFor(long seed) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < DIM; i++) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(i == Math.floorMod(seed, DIM) ? "1.0f" : "0.001f");
    }
    return sb.toString();
  }

  private long dataBytes(String table) {
    return spark.sql("SELECT sum(file_size_in_bytes) FROM " + table + ".files").head().getLong(0);
  }
}
