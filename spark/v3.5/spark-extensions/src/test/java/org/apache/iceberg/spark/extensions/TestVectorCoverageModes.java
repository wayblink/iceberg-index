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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.index.vector.VectorIndexTypes;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The two coverage modes answer two different questions about the same table.
 *
 * <p>The table mode answers for the whole table: the index supplies candidates and the files it
 * does not cover are scanned, so its answer is what a full scan would give. The indexed mode
 * answers only from what the index covers, which is shorter and never wrong about the rows it does
 * return. Neither returns a row the queried snapshot no longer has, while the diagnostic hit view
 * still shows one, and that difference is what the tests observe.
 *
 * <p>The table under test is behind its index in both directions: a file was appended after the
 * build, and one of the files the index did cover has been rewritten away by a delete. It is laid
 * out as three data files so the covered rows stay live while the deleted file is replaced.
 */
class TestVectorCoverageModes {
  private static final String COLUMNS =
      "(id bigint, embedding array<double> NOT NULL) USING iceberg";

  /** The query every test uses: the origin, so the nearest row is the one closest to it. */
  private static final double[] QUERY = {0.0d, 0.0d};

  private static SparkSession spark;

  @BeforeAll
  static void startSpark(@TempDir Path warehouse) {
    VectorIndexTypes.register();
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("vector-coverage-modes")
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

  @Test
  void theTableModeAnswersWithTheRowsAFullScanWouldFind() {
    String table = tableBehindItsIndex("table_mode");

    // the distance of every live row from the query, computed from the table alone
    List<Long> expectedIds = nearestIdsByScan(table, 3);
    List<Row> answered = search(table, QUERY, 3, "table");

    assertThat(idsOfHits(answered))
        .as("the k nearest rows of the whole table")
        .containsExactlyElementsOf(expectedIds);
    assertThat(idsOfHits(answered))
        .as("both rows appended after the build are nearer than anything the index holds")
        .contains(5L, 6L);
    assertThat(scoresOfHits(answered))
        .as("the distance of each answered row, which is the number the rewrite sorts by")
        .containsExactlyElementsOf(distanceOf(table, expectedIds));
  }

  @Test
  void theIndexedModeSearchesOnlyWhatTheIndexCovers() {
    String table = tableBehindItsIndex("indexed_mode");

    List<Row> answered = search(table, QUERY, 3, "indexed");
    List<Long> coveredAndLive = List.of(1L, 3L, 4L);

    assertThat(idsOfHits(answered))
        .as("the rows the index covers, and no others")
        .containsExactlyInAnyOrderElementsOf(coveredAndLive);
    assertThat(idsOfHits(answered))
        .as("a row the index never saw is not in its answer")
        .doesNotContain(5L, 6L)
        .as("and neither is a row that has been deleted")
        .doesNotContain(2L);

    List<Long> ordered = nearestOf(table, coveredAndLive);
    assertThat(idsOfHits(answered))
        .as("ordered by the same distance the table mode uses")
        .containsExactlyElementsOf(ordered);
    assertThat(scoresOfHits(answered))
        .as("scored with the metric the query names")
        .containsExactlyElementsOf(distanceOf(table, ordered));
  }

  @Test
  void theDefaultCoverageIsTheWholeTable() {
    String table = tableBehindItsIndex("default_mode");

    List<Row> answered = search(table, QUERY, 3, null);

    assertThat(idsOfHits(answered))
        .as("a caller who names no mode gets the answer that is complete")
        .containsExactlyElementsOf(nearestIdsByScan(table, 3));
  }

  @Test
  void theHitViewStillShowsTheHitTheCoverageModesDrop() {
    String table = tableBehindItsIndex("hits_mode");

    // the diagnostic view reports the index itself, so the file the delete replaced is still a hit
    // with no row to resolve — which is precisely what a coverage mode must not answer with
    List<Row> hits = search(table, QUERY, 10, "index_hits");

    assertThat(hits.stream().filter(row -> row.isNullAt(3)).count())
        .as("hits whose row the queried snapshot no longer has")
        .isPositive();
  }

  @Test
  void aCoverageModeThatDoesNotExistIsRefused() {
    String table = tableBehindItsIndex("invalid_mode");

    assertThatThrownBy(() -> search(table, QUERY, 2, "some"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("coverage");
  }

  /**
   * A table whose index is behind it in both directions.
   *
   * <p>Two files hold the rows the index sees, and one of them is rewritten away by a delete so the
   * covered rows of the other stay live; a third file arrives after the build.
   */
  private String tableBehindItsIndex(String name) {
    String full = "local.db.t_" + name;
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
    spark.sql("CREATE TABLE " + full + " " + COLUMNS);
    spark.sql("INSERT INTO " + full + " VALUES (1, array(0.0d, 0.0d)), (3, array(1.0d, 0.0d))");
    spark.sql("INSERT INTO " + full + " VALUES (4, array(1.0d, 1.0d))");
    spark.sql("INSERT INTO " + full + " VALUES (2, array(0.0d, 1.0d))");
    spark.sql(
        "CALL local.system.build_vector_index(table => 'db.t_"
            + name
            + "', column => 'embedding', dimension => 2, metric => 'euclidean', "
            + "structure => 'flat')");

    // the nearest rows arrive after the index was built, so no index snapshot covers their file
    // distinct distances, so the expected order does not depend on how a tie is broken
    spark.sql("INSERT INTO " + full + " VALUES (5, array(0.0d, 0.05d)), (6, array(0.06d, 0.0d))");
    // and the file that holds id 2 is rewritten away, so the index holds a hit nothing resolves
    spark.sql("DELETE FROM " + full + " WHERE id = 2");
    return name;
  }

  /**
   * A row the table deleted without touching its file: only resolving the position can drop it.
   *
   * <p>Every file a hit names is still in the snapshot, so the file-level check lets this hit
   * through and the answer depends on the position being gone. That is the gate the coverage modes
   * rely on when a table deletes rows and keeps its files, which is what merge-on-read does.
   */
  @Test
  void aPositionDeleteInsideACoveredFileIsNotAnAnswer() throws Exception {
    String name = "position_delete";
    String full = "local.db.t_" + name;
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
    spark.sql(
        "CREATE TABLE "
            + full
            + " "
            + COLUMNS
            + " TBLPROPERTIES ('format-version' = '2', 'write.delete.mode' = 'merge-on-read')");
    spark.sql("INSERT INTO " + full + " VALUES (1, array(0.0d, 0.0d)), (3, array(1.0d, 1.0d))");
    spark.sql(
        "CALL local.system.build_vector_index(table => 'db.t_"
            + name
            + "', column => 'embedding', dimension => 2, metric => 'euclidean', "
            + "structure => 'flat')");

    // the delete is a position delete of its own, so the data file the index saw stays in place
    String deletedFrom = addPositionDelete(full, 0L).location().toString();
    assertThat(dataFileLocations(full))
        .as("a position delete leaves the data file where the index saw it")
        .hasSize(2)
        .contains(deletedFrom);

    List<Long> live = liveIds(name);
    List<Row> indexed = search(name, QUERY, 2, "indexed");
    assertThat(indexed.stream().map(row -> row.isNullAt(3)))
        .as("a short answer is fewer rows, never a row reported as missing")
        .doesNotContain(true);
    assertThat(idsOfHits(indexed))
        .as("the deleted position is not an answer, and every live row still is")
        .containsExactlyElementsOf(nearestOf(name, live));
    assertThat(scoresOfHits(indexed)).containsExactlyElementsOf(distanceOf(name, live));

    List<Row> hits = search(name, QUERY, 4, "index_hits");
    assertThat(hits.stream().filter(row -> row.isNullAt(3)).count())
        .as("the index still holds the position, and the diagnostic view says so")
        .isPositive();
  }

  /**
   * Adds a position delete for one row, the way a job writes one.
   *
   * <p>The SQL path is not used here because this baseline's Spark DELETE rewrites the data file,
   * which would replace the file under test instead of deleting a row inside it.
   */
  private DataFile addPositionDelete(String tableName, long position) throws Exception {
    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    DataFile target;
    try (org.apache.iceberg.io.CloseableIterable<org.apache.iceberg.FileScanTask> tasks =
        table.newScan().planFiles()) {
      target = tasks.iterator().next().file();
    }

    assertThat(target).as("the table has a data file to delete a row from").isNotNull();
    String location = table.location() + "/data/probe-pos-" + position + ".parquet";
    PositionDeleteWriter<Record> writer =
        FormatModelRegistry.<Record>positionDeleteWriteBuilder(
                FileFormat.PARQUET,
                EncryptedFiles.plainAsEncryptedOutput(table.io().newOutputFile(location)))
            .spec(PartitionSpec.unpartitioned())
            .build();
    try (PositionDeleteWriter<Record> open = writer) {
      open.write(PositionDelete.<Record>create().set(target.location().toString(), position));
    }

    table.newRowDelta().addDeletes(writer.toDeleteFile()).commit();
    return target;
  }

  /** The locations of the table's current data files. */
  private List<String> dataFileLocations(String tableName) throws Exception {
    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    try (org.apache.iceberg.io.CloseableIterable<org.apache.iceberg.FileScanTask> tasks =
        table.newScan().planFiles()) {
      return java.util.stream.StreamSupport.stream(tasks.spliterator(), false)
          .map(task -> task.file().location().toString())
          .collect(Collectors.toList());
    }
  }

  private List<Long> nearestIdsByScan(String table, int k) {
    return nearestOf(table, liveIds(table)).stream().limit(k).collect(Collectors.toList());
  }

  /** The given rows, ordered by their distance from the query. */
  private List<Long> nearestOf(String table, List<Long> ids) {
    return ids.stream()
        .sorted(Comparator.comparingDouble(id -> distanceOf(table, id)))
        .collect(Collectors.toList());
  }

  private List<Long> liveIds(String table) {
    return spark.sql("SELECT id FROM local.db.t_" + table).collectAsList().stream()
        .map(row -> row.getLong(0))
        .collect(Collectors.toList());
  }

  /** The distance of one row, computed from the table rather than from any index. */
  private double distanceOf(String table, long id) {
    Row row = spark.sql("SELECT embedding FROM local.db.t_" + table + " WHERE id = " + id).first();
    List<?> values = row.getList(0);
    double sum = 0.0d;
    for (int i = 0; i < QUERY.length; i++) {
      double delta = ((Number) values.get(i)).doubleValue() - QUERY[i];
      sum += delta * delta;
    }

    return Math.sqrt(sum);
  }

  private List<Double> distanceOf(String table, List<Long> ids) {
    return ids.stream().map(id -> round(distanceOf(table, id))).collect(Collectors.toList());
  }

  private List<Row> search(String table, double[] query, int k, String coverage) {
    String sql =
        "CALL local.system.vector_search(table => 'db.t_"
            + table
            + "', column => 'embedding', query => array("
            + query[0]
            + "d, "
            + query[1]
            + "d), k => "
            + k
            + (coverage == null ? "" : ", coverage => '" + coverage + "'")
            + ")";
    return spark.sql(sql).collectAsList();
  }

  private static List<Long> idsOfHits(List<Row> hits) {
    return hits.stream().map(hit -> parseId(hit.getString(3))).collect(Collectors.toList());
  }

  private static List<Double> scoresOfHits(List<Row> hits) {
    return hits.stream().map(hit -> round(hit.getDouble(2))).collect(Collectors.toList());
  }

  private static double round(double value) {
    return Math.round(value * 1_000_000d) / 1_000_000d;
  }

  private static long parseId(String rowJson) {
    int start = rowJson.indexOf("\"id\":") + 5;
    int end = rowJson.indexOf(',', start);
    return Long.parseLong(rowJson.substring(start, end == -1 ? rowJson.indexOf('}', start) : end));
  }
}
