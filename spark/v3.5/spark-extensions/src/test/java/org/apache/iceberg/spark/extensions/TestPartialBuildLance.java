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

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.Table;
import org.apache.iceberg.index.IndexFile;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.IndexReader;
import org.apache.iceberg.index.IndexSnapshot;
import org.apache.iceberg.index.IndexSnapshotWriter;
import org.apache.iceberg.index.SidecarIndexCatalog;
import org.apache.iceberg.index.spark.IndexEntries;
import org.apache.iceberg.index.spark.VectorIndexes;
import org.apache.iceberg.index.vector.VectorIndexTypes;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The partial build and the gap-filling refresh on Lance-backed tables.
 *
 * <p>A Lance data file is read through the format model, not the parquet reader, and a position
 * filter is a range list rather than a parquet page lookup, so a build over Lance and a fetch-back
 * of Lance rows take different paths through the code than the parquet tests exercise. What the
 * feature promises has to hold on both, or the promise is about one storage format only.
 */
class TestPartialBuildLance {

  private static final String COLUMNS = "(id bigint, embedding array<float>) USING iceberg";

  private static SparkSession spark;

  @BeforeAll
  static void startSpark(@TempDir Path warehouse) {
    VectorIndexTypes.register();
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("partial-build-lance")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", warehouse.toUri().toString())
            .getOrCreate();
  }

  @AfterAll
  static void stopSpark() {
    if (spark != null) {
      spark.stop();
    }
  }

  /**
   * A build that cannot index one group publishes the rest, and a refresh fills the gap.
   *
   * <p>The table is Lance-backed, so both the build's read of the group and the refresh's rebuild
   * of it go through the Lance format model.
   */
  @Test
  void aPartialBuildOnLancePublishesTheRestAndARefreshFillsTheGap() throws Exception {
    String table = "lance_partial";
    createLanceTable(table);
    // one group whose vectors match the requested dimension, one whose vectors do not
    spark.sql(String.format("INSERT INTO local.db.t_%s VALUES (1, array(1.0f, 0.0f))", table));
    spark.sql(
        String.format("INSERT INTO local.db.t_%s VALUES (2, array(1.0f, 0.0f, 0.0f))", table));

    Row built =
        spark
            .sql(
                String.format(
                    "CALL local.system.build_vector_index("
                        + "table => 'db.t_%s', column => 'embedding', dimension => 2, "
                        + "max_entries_per_index_file => 1)",
                    table))
            .head();
    assertThat(longValue(built, "index_files"))
        .as("the group that could be indexed is published")
        .isEqualTo(1L);

    Table indexed = Spark3Util.loadIcebergTable(spark, "local.db.t_" + table);
    long snapshot = indexed.currentSnapshot().snapshotId();
    IndexMetadata metadata = VectorIndexes.findIndex(indexed, "embedding", "vector", null);
    assertThat(
            new IndexReader(metadata, indexed)
                .uncoveredDataFiles(snapshot, IndexEntries.dataFiles(indexed, snapshot)))
        .as("the group that could not be indexed is left uncovered")
        .hasSize(1);

    assertThat(spark.sql("SELECT count(*) FROM local.db.t_" + table).head().getLong(0))
        .as("a partly covered table is still readable")
        .isEqualTo(2L);

    // removing the row the index could not take, then filling the gap
    spark.sql(String.format("DELETE FROM local.db.t_%s WHERE id = 2", table));
    spark.sql(
        String.format(
            "CALL local.system.refresh_vector_index(table => 'db.t_%s', column => 'embedding')",
            table));

    Table refreshed = Spark3Util.loadIcebergTable(spark, "local.db.t_" + table);
    IndexMetadata after = VectorIndexes.findIndex(refreshed, "embedding", "vector", null);
    long refreshedSnapshot = refreshed.currentSnapshot().snapshotId();
    assertThat(after.snapshots())
        .as(
            "the delete moved the table, so the refresh is an ordinary incremental one and each "
                + "table snapshot keeps its own build")
        .hasSize(2);
    assertThat(
            new IndexReader(after, refreshed)
                .uncoveredDataFiles(
                    refreshedSnapshot, IndexEntries.dataFiles(refreshed, refreshedSnapshot)))
        .as("the gap is closed")
        .isEmpty();
  }

  /**
   * A gap on a Lance table does not change the answer of a query that needs live rows.
   *
   * <p>The baseline comes from a copy of the table without an index, so no rewrite can answer it.
   */
  @Test
  void aGapOnLanceStillAnswersTheNearestRows() throws Exception {
    String table = "lance_gap_answer";
    createLanceTable(table);
    spark.sql(String.format("INSERT INTO local.db.t_%s VALUES (3, array(1.0f, 1.0f))", table));
    spark.sql(String.format("INSERT INTO local.db.t_%s VALUES (1, array(0.0f, 0.0f))", table));
    spark.sql(String.format("INSERT INTO local.db.t_%s VALUES (4, array(2.0f, 2.0f))", table));
    // the copy is taken before the index exists, so it answers by scanning
    spark.sql(
        "CREATE TABLE local.db.t_"
            + table
            + "_ref USING iceberg TBLPROPERTIES ('write.format.default' = 'lance') AS "
            + "SELECT * FROM local.db.t_"
            + table);

    spark
        .sql(
            String.format(
                "CALL local.system.build_vector_index("
                    + "table => 'db.t_%s', column => 'embedding', dimension => 2, "
                    + "metric => 'l2', structure => 'flat', max_entries_per_index_file => 1)",
                table))
        .collectAsList();

    // keep one index file of three, the way a build that stopped early would have
    dropAllButOneIndexFile(table);

    List<Long> expected = nearestByScan(table + "_ref", 3);
    assertThat(idsOf(search(table, 3, "table")))
        .as("the nearest rows of the whole Lance table, whatever the index covers")
        .containsExactlyElementsOf(expected);
  }

  /**
   * The same shape the parquet fetch-back gets wrong: two data files of a hundred thousand rows,
   * sparse hit positions, a gap over half the table.
   *
   * <p>Lance resolves a position filter through a range list, so the rows it returns cannot be
   * reordered against the positions that asked for them. A gap over a table of this size therefore
   * has to still answer the nearest rows of the whole table, and completing it has to keep
   * answering them.
   */
  @Test
  void aLargeLanceTableWithAGapAnswersTheNearestRows() throws Exception {
    int rowsPerFile = 100_000;
    int dimension = 32;
    String table = "lance_scale";
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
    spark.sql("DROP TABLE IF EXISTS local.db.t_" + table);
    spark.sql(
        "CREATE TABLE local.db.t_"
            + table
            + " (id bigint, embedding array<float>) USING iceberg "
            + "TBLPROPERTIES ('write.format.default' = 'lance')");
    for (int file = 0; file < 2; file++) {
      spark.sql(
          "INSERT INTO local.db.t_"
              + table
              + " SELECT /*+ REPARTITION(1) */ global, transform(sequence(1, "
              + dimension
              + "), x -> CAST(pmod(hash(global, x), 10007) AS double) / 10007.0) FROM "
              + "(SELECT id, id + "
              + ((long) file * rowsPerFile)
              + " AS global FROM range("
              + rowsPerFile
              + "))");
    }

    // the baseline comes from a copy without an index, so no rewrite can answer it
    spark.sql("DROP TABLE IF EXISTS local.db.t_" + table + "_ref");
    spark.sql(
        "CREATE TABLE local.db.t_"
            + table
            + "_ref USING iceberg TBLPROPERTIES ('write.format.default' = 'lance') AS "
            + "SELECT * FROM local.db.t_"
            + table);
    String query = "array(" + "0.5f,".repeat(dimension - 1) + "0.5f)";
    List<Long> expected =
        spark
            .sql(
                "SELECT id FROM local.db.t_"
                    + table
                    + "_ref ORDER BY vec_distance(embedding, "
                    + query
                    + ") LIMIT 10")
            .collectAsList()
            .stream()
            .map(row -> row.getLong(0))
            .collect(Collectors.toList());
    assertThat(expected).as("the table has the rows this test is about").hasSize(10);

    spark
        .sql(
            "CALL local.system.build_vector_index(table => 'db.t_"
                + table
                + "', column => 'embedding', dimension => "
                + dimension
                + ", metric => 'l2', structure => 'flat', max_entries_per_index_file => 1)")
        .collectAsList();
    dropAllButOneIndexFile(table);

    List<Long> answered =
        spark
            .sql(
                "CALL local.system.vector_search(table => 'db.t_"
                    + table
                    + "', column => 'embedding', query => "
                    + query
                    + ", k => 10)")
            .collectAsList()
            .stream()
            .map(row -> parseId(row.getString(3)))
            .collect(Collectors.toList());
    assertThat(answered)
        .as("a gap over a large Lance table still answers the nearest rows of the table")
        .containsExactlyElementsOf(expected);

    // filling the gap with the table where it is replaces the partial build
    String refresh =
        "CALL local.system.refresh_vector_index(table => 'db.t_"
            + table
            + "', column => 'embedding')";
    spark.sql(refresh).collectAsList();
    Table after = Spark3Util.loadIcebergTable(spark, "local.db.t_" + table);
    IndexMetadata metadata = VectorIndexes.findIndex(after, "embedding", "vector", null);
    long snapshot = after.currentSnapshot().snapshotId();
    assertThat(metadata.snapshots()).as("one build per table snapshot").hasSize(1);
    assertThat(
            new IndexReader(metadata, after)
                .uncoveredDataFiles(snapshot, IndexEntries.dataFiles(after, snapshot)))
        .as("the gap is closed")
        .isEmpty();
  }

  private void createLanceTable(String name) {
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
    spark.sql("DROP TABLE IF EXISTS local.db.t_" + name);
    spark.sql(
        "CREATE TABLE local.db.t_"
            + name
            + " "
            + COLUMNS
            + " TBLPROPERTIES ('write.format.default' = 'lance')");
  }

  /** Rewrites the published snapshot so that it names one index file of the build. */
  private void dropAllButOneIndexFile(String table) throws Exception {
    Table indexed = Spark3Util.loadIcebergTable(spark, "local.db.t_" + table);
    IndexMetadata metadata = VectorIndexes.findIndex(indexed, "embedding", "vector", null);
    List<IndexFile> files = VectorIndexes.indexFiles(indexed.io(), metadata.latestSnapshot());
    assertThat(files).as("one index file per group").hasSizeGreaterThan(1);

    IndexSnapshotWriter writer = new IndexSnapshotWriter(indexed.io());
    String manifestList =
        writer.writeManifestList(
            metadata.location(), metadata.latestSnapshot().snapshotId(), List.of(files.get(0)));
    IndexSnapshot partial =
        metadata.latestSnapshot().toBuilder().indexManifestList(manifestList).build();
    new SidecarIndexCatalog(indexed.io(), indexed)
        .updateIndex(
            indexed.location(), metadata.indexUuid(), current -> current.replaceSnapshot(partial));
    indexed.refresh();
  }

  private List<Row> search(String table, int k, String coverage) {
    return spark
        .sql(
            "CALL local.system.vector_search(table => 'db.t_"
                + table
                + "', column => 'embedding', query => array(0.0f, 0.0f), k => "
                + k
                + (coverage == null ? "" : ", coverage => '" + coverage + "'")
                + ")")
        .collectAsList();
  }

  private List<Long> nearestByScan(String table, int k) {
    List<Row> rows =
        spark
            .sql(
                "SELECT id FROM local.db.t_"
                    + table
                    + " ORDER BY vec_distance(embedding, array(0.0f, 0.0f)) LIMIT "
                    + k)
            .collectAsList();
    return rows.stream().map(row -> row.getLong(0)).collect(Collectors.toList());
  }

  private static List<Long> idsOf(List<Row> hits) {
    return hits.stream().map(hit -> parseId(hit.getString(3))).collect(Collectors.toList());
  }

  private static long parseId(String rowJson) {
    int start = rowJson.indexOf("\"id\":") + 5;
    int end = rowJson.indexOf(',', start);
    return Long.parseLong(rowJson.substring(start, end == -1 ? rowJson.indexOf('}', start) : end));
  }

  private static long longValue(Row row, String column) {
    return row.getLong(row.fieldIndex(column));
  }

  private static String stringValue(Row row, String column) {
    return row.getString(row.fieldIndex(column));
  }
}
