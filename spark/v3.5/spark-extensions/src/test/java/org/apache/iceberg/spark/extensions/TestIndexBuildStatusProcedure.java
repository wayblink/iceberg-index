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

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.spark.IndexBuildStatus;
import org.apache.iceberg.index.spark.SparkIndexBuilder;
import org.apache.iceberg.index.spark.VectorIndexes;
import org.apache.iceberg.index.vector.VectorIndexDefinition;
import org.apache.iceberg.index.vector.VectorIndexDefinition.Metric;
import org.apache.iceberg.index.vector.VectorIndexDefinition.VectorStructure;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.SparkException;
import org.junit.jupiter.api.TestTemplate;

/**
 * What a build of an index says about itself while it runs and after it ends.
 *
 * <p>A build of a large table runs for tens of minutes; until this existed the only trace of it was
 * a line in the driver log, which is unreadable from SQL, gone when the build ends, and silent
 * about a build that failed. These tests pin the three states a caller has to be able to tell
 * apart: still running, finished, and stopped without saying so.
 */
public class TestIndexBuildStatusProcedure extends ExtensionsTestBase {

  @TestTemplate
  public void aFinishedBuildReportsWhatItBuilt() {
    String table = tableName("build_status_done_" + catalogName);
    createAndIndex(table, 20);

    List<Object[]> rows = status(table);
    assertThat(rows).hasSize(1);

    Object[] row = rows.get(0);
    assertThat((String) row[0]).as("table").isEqualTo(table);
    assertThat((String) row[1]).as("column").isEqualTo("embedding");
    assertThat((String) row[4]).as("index type").isEqualTo("vector");
    assertThat((String) row[5]).as("state").isEqualTo("SUCCEEDED");
    assertThat((Long) row[10]).as("files built").isPositive();
    assertThat(row[10]).as("files built of total").isEqualTo(row[11]);
    assertThat((Long) row[13]).as("rows").isEqualTo(20L);
    assertThat(row[14]).as("no error").isNull();
    assertThat((Boolean) row[9]).as("stale").isFalse();
    assertThat((Long) row[8]).as("age").isNotNegative();
  }

  @TestTemplate
  public void aRunningBuildReportsHowFarItHasGot() throws Exception {
    String table = tableName("build_status_running_" + catalogName);
    createAndIndex(table, 20);
    write(table, withIndexOf(table, running(now(), 3L, 10L)));

    Object[] row = status(table).get(0);
    assertThat((String) row[5]).as("state").isEqualTo("RUNNING");
    assertThat((Long) row[10]).as("files built").isEqualTo(3L);
    assertThat((Long) row[11]).as("files total").isEqualTo(10L);
    assertThat((Boolean) row[9]).as("a build reporting now is not stale").isFalse();
    assertThat((Long) row[8]).as("age of the report").isNotNegative();
  }

  @TestTemplate
  public void aBuildThatStoppedReportingIsReportedAbandoned() throws Exception {
    String table = tableName("build_status_abandoned_" + catalogName);
    createAndIndex(table, 20);
    write(table, withIndexOf(table, running(now() - 10L * 60L * 1000L, 2L, 10L)));

    Object[] row = status(table).get(0);
    assertThat((String) row[5]).as("state").isEqualTo("ABANDONED");
    assertThat((Boolean) row[9]).as("stale").isTrue();
    assertThat((Long) row[10]).as("files built when it stopped").isEqualTo(2L);
  }

  @TestTemplate
  public void aFailedBuildReportsWhyItFailed() throws Exception {
    String table = tableName("build_status_failed_" + catalogName);
    createAndIndex(table, 20);
    write(
        table,
        withIndexOf(
            table,
            IndexBuildStatus.builder()
                .state(IndexBuildStatus.State.FAILED)
                .indexUuid("placeholder")
                .column("embedding")
                .sourceSnapshotId(1L)
                .startedAtMillis(now() - 60_000L)
                .updatedAtMillis(now())
                .filesBuilt(2L)
                .filesTotal(4L)
                .filesFailed(1L)
                .rows(12L)
                .error("java.lang.IllegalStateException: the kernel rejected the payload")
                .build()));

    Object[] row = status(table).get(0);
    assertThat((String) row[5]).as("state").isEqualTo("FAILED");
    assertThat((String) row[14]).as("error").contains("the kernel rejected the payload");
    assertThat((Long) row[12]).as("files failed").isEqualTo(1L);
    assertThat((Boolean) row[9]).as("a finished build is never stale").isFalse();
  }

  @TestTemplate
  public void anIndexWithNoReportIsReportedUnknown() throws Exception {
    String table = tableName("build_status_unknown_" + catalogName);
    createAndIndex(table, 20);
    deleteReport(table);

    Object[] row = status(table).get(0);
    assertThat((String) row[5]).as("state").isEqualTo("UNKNOWN");
    assertThat(row[6]).as("no start time").isNull();
    assertThat(row[10]).as("no counts").isNull();
    assertThat((Boolean) row[9]).as("stale").isFalse();
  }

  @TestTemplate
  public void aBuildThatFailsOutlivesItsRolledBackIndex() {
    String table = tableName("build_status_broken_" + catalogName);
    sql("CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg", table);
    sql(
        "INSERT INTO %s SELECT id, array(CAST(id AS double), CAST(10 - id AS double)) "
            + "FROM range(0, 20)",
        table);

    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.build_vector_index("
                        + "table => '%s', column => 'embedding', dimension => 7)",
                    catalogName, table))
        .isInstanceOf(RuntimeException.class);

    // the failed attempt took its registration and its index tree with it, so this is the only
    // place the failure can still be read
    List<Object[]> rows = status(table);
    assertThat(rows).hasSize(1);

    Object[] row = rows.get(0);
    assertThat((String) row[1]).as("the column the attempt was for").isEqualTo("embedding");
    assertThat((String) row[5]).as("state").isEqualTo("FAILED");
    assertThat((String) row[14]).as("the reason").isNotNull();
  }

  @TestTemplate
  public void aRefreshIsReportedAsTheLastAttempt() {
    String table = tableName("build_status_refresh_" + catalogName);
    createAndIndex(table, 20);
    sql("INSERT INTO %s VALUES (100, array(100.0d, 0.0d))", table);
    sql(
        "CALL %s.system.refresh_vector_index(table => '%s', column => 'embedding')",
        catalogName, table);

    Object[] row = status(table).get(0);
    assertThat((String) row[5]).as("state").isEqualTo("SUCCEEDED");
    assertThat((Long) row[10]).as("files rebuilt").isPositive();
    assertThat((Long) row[13]).as("rows rebuilt").isPositive();
  }

  @TestTemplate
  void aFailedInitialBuildAbortsItsAttempt() throws Exception {
    String tableName = tableName("build_status_initial_abort_" + catalogName);
    sql("CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (100, array(1.0d, 2.0d, 3.0d))", tableName);
    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    table.refresh();
    String uuid = UUID.randomUUID().toString();
    String location = table.location() + "/indexes/" + uuid;
    IndexMetadata metadata =
        IndexMetadata.builder()
            .indexUuid(uuid)
            .tableUuid(table.uuid().toString())
            .location(location)
            .indexType("vector")
            .definition(
                VectorIndexDefinition.builder()
                    .fieldId(table.schema().findField("embedding").fieldId())
                    .dimension(2)
                    .metric(Metric.EUCLIDEAN)
                    .structure(VectorStructure.FLAT)
                    .build())
            .build();
    try (RecordingFileIO io = new RecordingFileIO(spark.sessionState().newHadoopConf())) {
      Table observed =
          new BaseTable(
              new StaticTableOperations(((HasTableOperations) table).operations().current(), io),
              table.name());

      assertThatThrownBy(
              () ->
                  SparkIndexBuilder.build(
                      spark,
                      observed,
                      metadata,
                      observed.currentSnapshot().snapshotId(),
                      "embedding",
                      now()))
          .isInstanceOf(SparkException.class)
          .hasMessageContaining("Vector column has 3 dimensions");
      assertThat(io.stagingProbes)
          .as("the initial build's ledger was checked for cleanup")
          .isNotEmpty();
      IndexBuildStatus failed = IndexBuildStatus.read(io, IndexBuildStatus.statusDirFor(location));
      assertThat(failed).isNotNull();
      assertThat(failed.state())
          .as("the builder records failure without a procedure wrapper")
          .isEqualTo(IndexBuildStatus.State.FAILED);
      assertThat(failed.error()).contains("Vector column has 3 dimensions");
    }
  }

  @TestTemplate
  void aFailedRefreshReportsItsError() throws Exception {
    String tableName = tableName("build_status_refresh_failed_" + catalogName);
    Table table = tableWithInvalidRefreshInput(tableName);
    IndexMetadata original = VectorIndexes.findIndex(table, "embedding", "vector");

    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.refresh_vector_index(table => '%s', column => 'embedding')",
                    catalogName, tableName))
        .isInstanceOf(SparkException.class)
        .hasMessageContaining("Vector column has 3 dimensions");

    List<Object[]> rows = status(tableName);
    assertThat(rows).hasSize(1);
    Object[] row = rows.get(0);
    assertThat(row[2]).as("the existing index").isEqualTo(original.indexUuid());
    assertThat(row[5]).as("a failed refresh is not left running").isEqualTo("FAILED");
    assertThat((String) row[14]).contains("Vector column has 3 dimensions", "built for 2");
    assertThat(row[10]).as("no index files were built").isEqualTo(0L);
    assertThat(row[11]).as("the failed refresh had one new data file").isEqualTo(1L);
    assertThat(row[9]).as("a terminal failure is not abandoned").isEqualTo(false);
    assertThat(table.io().newInputFile(original.latestSnapshot().indexManifestList()).exists())
        .as("the parent index remains readable")
        .isTrue();
    assertThat(
            sql(
                "CALL %s.system.vector_search(table => '%s', column => 'embedding', "
                    + "query => array(0.0d, 10.0d), k => 3)",
                catalogName, tableName))
        .as("the failed refresh does not break searching the parent index")
        .hasSize(3);
  }

  @TestTemplate
  void aFailedRefreshAbortsItsAttempt() throws Exception {
    String tableName = tableName("build_status_refresh_abort_" + catalogName);
    Table table = tableWithInvalidRefreshInput(tableName);
    IndexMetadata original = VectorIndexes.findIndex(table, "embedding", "vector");
    try (RecordingFileIO io = new RecordingFileIO(spark.sessionState().newHadoopConf())) {
      Table observed =
          new BaseTable(
              new StaticTableOperations(((HasTableOperations) table).operations().current(), io),
              table.name());

      assertThatThrownBy(
              () ->
                  SparkIndexBuilder.refresh(
                      spark,
                      observed,
                      original,
                      observed.currentSnapshot().snapshotId(),
                      "embedding",
                      now()))
          .isInstanceOf(SparkException.class)
          .hasMessageContaining("Vector column has 3 dimensions");

      // The invalid vector fails before any payload is written. Observe the real ledger probes so
      // an empty directory alone cannot make a missing abort call appear correct.
      assertThat(io.stagingProbes)
          .as("the failed attempt's ledger was checked for cleanup")
          .isNotEmpty();
      Path staging = new Path(original.location(), "staging");
      FileSystem fs = staging.getFileSystem(spark.sessionState().newHadoopConf());
      if (fs.exists(staging)) {
        assertThat(fs.listFiles(staging, true).hasNext()).as("no staging ledger remains").isFalse();
      }

      assertThat(io.newInputFile(original.latestSnapshot().indexManifestList()).exists())
          .as("aborting the refresh does not delete the parent snapshot")
          .isTrue();
      assertThat(VectorIndexes.findIndex(table, "embedding", "vector").latestSnapshot())
          .as("the failed refresh was not published")
          .isEqualTo(original.latestSnapshot());
    }
  }

  @TestTemplate
  public void aLaterSuccessfulBuildClearsTheFailureItReplaced() {
    String table = tableName("build_status_retry_" + catalogName);
    sql("CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg", table);
    sql(
        "INSERT INTO %s SELECT id, array(CAST(id AS double), CAST(10 - id AS double)) "
            + "FROM range(0, 20)",
        table);

    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.build_vector_index("
                        + "table => '%s', column => 'embedding', dimension => 7)",
                    catalogName, table))
        .isInstanceOf(RuntimeException.class);
    assertThat(status(table)).hasSize(1);

    sql(
        "CALL %s.system.build_vector_index(table => '%s', column => 'embedding', dimension => 2)",
        catalogName, table);

    List<Object[]> rows = status(table);
    assertThat(rows).as("the failed attempt is replaced, not added to").hasSize(1);
    assertThat((String) rows.get(0)[5]).as("state").isEqualTo("SUCCEEDED");
    assertThat(rows.get(0)[14]).as("no error").isNull();
  }

  @TestTemplate
  public void everyIndexOfTheTableIsReported() {
    String table = tableName("build_status_two_" + catalogName);
    createAndIndex(table, 20);
    sql(
        "CALL %s.system.build_vector_index(table => '%s', column => 'embedding', dimension => 2, "
            + "name => 'second_arm', structure => 'FLAT')",
        catalogName, table);

    List<Object[]> rows = status(table);
    assertThat(rows).hasSize(2);
    assertThat(rows).allSatisfy(row -> assertThat((String) row[5]).isEqualTo("SUCCEEDED"));
  }

  private Table tableWithInvalidRefreshInput(String tableName) throws Exception {
    createAndIndex(tableName, 20);
    sql("INSERT INTO %s VALUES (100, array(1.0d, 2.0d, 3.0d))", tableName);
    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    table.refresh();
    return table;
  }

  private static class RecordingFileIO extends HadoopFileIO {
    private final Set<String> stagingProbes = ConcurrentHashMap.newKeySet();

    private RecordingFileIO(Configuration conf) {
      super(conf);
    }

    @Override
    public InputFile newInputFile(String path) {
      if (path.contains("/staging/")) {
        stagingProbes.add(path);
      }
      return super.newInputFile(path);
    }
  }

  private void createAndIndex(String table, int rows) {
    sql("CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg", table);
    sql(
        "INSERT INTO %s SELECT id, array(CAST(id AS double), CAST(%d - id AS double)) "
            + "FROM range(0, %d)",
        table, rows, rows);
    sql(
        "CALL %s.system.build_vector_index(table => '%s', column => 'embedding', dimension => 2, "
            + "metric => 'euclidean')",
        catalogName, table);
  }

  private List<Object[]> status(String table) {
    return sql("CALL %s.system.index_build_status(table => '%s')", catalogName, table);
  }

  private void write(String tableName, IndexBuildStatus status) throws Exception {
    Table table = org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark, tableName);
    IndexBuildStatus.write(table.io(), IndexBuildStatus.statusDirFor(indexLocation(table)), status);
  }

  /**
   * A handcrafted report for the oldest index of the table, which is the one these tests rewrite.
   */
  private IndexBuildStatus withIndexOf(String tableName, IndexBuildStatus status) throws Exception {
    Table table = org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark, tableName);
    return IndexBuildStatus.builder()
        .state(status.state())
        .indexUuid(VectorIndexes.indexesOf(table).get(0).indexUuid())
        .column(status.column())
        .sourceSnapshotId(status.sourceSnapshotId())
        .startedAtMillis(status.startedAtMillis())
        .updatedAtMillis(status.updatedAtMillis())
        .filesBuilt(status.filesBuilt())
        .filesTotal(status.filesTotal())
        .filesFailed(status.filesFailed())
        .rows(status.rows())
        .error(status.error())
        .build();
  }

  private void deleteReport(String tableName) throws Exception {
    Table table = org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark, tableName);
    IndexBuildStatus.delete(table.io(), IndexBuildStatus.statusDirFor(indexLocation(table)));
  }

  private String indexLocation(Table table) {
    String uuid = null;
    for (IndexMetadata metadata : VectorIndexes.indexesOf(table)) {
      uuid = metadata.indexUuid();
    }

    assertThat(uuid).as("the table has an index").isNotNull();
    return table.location() + "/indexes/" + uuid;
  }

  private static IndexBuildStatus running(long updatedAt, long built, long total) {
    return IndexBuildStatus.builder()
        .state(IndexBuildStatus.State.RUNNING)
        .indexUuid("placeholder")
        .column("embedding")
        .sourceSnapshotId(1L)
        .startedAtMillis(updatedAt - 1000L)
        .updatedAtMillis(updatedAt)
        .filesBuilt(built)
        .filesTotal(total)
        .filesFailed(0L)
        .rows(0L)
        .build();
  }

  private static long now() {
    return System.currentTimeMillis();
  }
}
