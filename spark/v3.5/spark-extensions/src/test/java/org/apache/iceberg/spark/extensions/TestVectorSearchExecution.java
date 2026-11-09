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

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.index.IndexFile;
import org.apache.iceberg.index.IndexManifest;
import org.apache.iceberg.index.IndexManifestList;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.spark.VectorIndexes;
import org.apache.iceberg.index.vector.VectorIndexTypes;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.metric.SQLMetric;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import scala.jdk.javaapi.CollectionConverters;

/**
 * The physical side of a vector query: what it decides, what it counts, and when it is allowed to
 * read data.
 *
 * <p>The plan carries the decisions the optimizer made, and the operator reports what crossed the
 * driver barrier so a query can be explained without reading its rows. Building the plan, including
 * the physical plan, starts no job; only running it searches.
 */
class TestVectorSearchExecution {
  private static final String COLUMNS =
      "(id bigint, embedding array<double> NOT NULL) USING iceberg";
  private static final String SOURCE = "VectorSearchSource";
  private static SparkSession spark;

  @BeforeAll
  static void startSpark(@TempDir Path warehouse) {
    VectorIndexTypes.register();
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("vector-search-execution")
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
  void sourceStatisticsIncludeUncoveredRowsBeforeTheLimit() {
    String table = tableWithIndex("exec_stats_partial");
    int appendedRows = 20;
    spark.sql(
        String.format(
            "INSERT INTO local.db.%s SELECT id, array(0.8d, 0.0d) FROM range(10, %d)",
            table, 10 + appendedRows));
    long tableRows = spark.table("local.db." + table).count();
    Dataset<Row> frame = spark.sql(nearest(table, 1));

    assertSourceEstimate(frame, tableRows);
    assertThat(frame.collectAsList()).hasSize(1);
    assertThat(metric(sourceExec(frame), "numOutputRows")).isEqualTo(appendedRows + 1L);
  }

  @Test
  void sourceStatisticsDoNotApplyTheLimitBeforeOversampling() {
    String table = tableWithIndex("exec_stats_oversampled");
    long tableRows = spark.table("local.db." + table).count();
    String parameter = "spark.iceberg.index.vector.over-sample";
    scala.Option<String> previous = spark.conf().getOption(parameter);
    spark.conf().set(parameter, "3");
    try {
      Dataset<Row> frame = spark.sql(nearest(table, 1));
      assertSourceEstimate(frame, tableRows);
      assertThat(frame.collectAsList()).hasSize(1);
      assertThat(metric(sourceExec(frame), "numOutputRows")).isEqualTo(3L);
    } finally {
      if (previous.isDefined()) {
        spark.conf().set(parameter, previous.get());
      } else {
        spark.conf().unset(parameter);
      }
    }
  }

  @Test
  void sourceStatisticsIncludeAllRowsWhenNoFileIsCovered() {
    String table = tableWithIndex("exec_stats_uncovered");
    spark.sql(
        String.format(
            "CALL local.system.rewrite_data_files(table => 'db.%s', "
                + "options => map('rewrite-all', 'true'))",
            table));
    long tableRows = spark.table("local.db." + table).count();
    Dataset<Row> frame = spark.sql(nearest(table, 1));

    assertSourceEstimate(frame, tableRows);
    assertThat(frame.collectAsList()).hasSize(1);
    assertThat(metric(sourceExec(frame), "numLiveCandidates")).isZero();
    assertThat(metric(sourceExec(frame), "numOutputRows")).isEqualTo(tableRows);
  }

  @Test
  void sourceStatisticsAreZeroForAnEmptySnapshot() {
    String table = tableWithIndex("exec_stats_empty");
    spark.sql(String.format("DELETE FROM local.db.%s", table));
    long tableRows = spark.table("local.db." + table).count();
    assertThat(tableRows).isZero();
    Dataset<Row> frame = spark.sql(nearest(table, 1));

    assertSourceEstimate(frame, tableRows);
    assertThat(frame.collectAsList()).isEmpty();
  }

  /** The source is below the limit; its estimate bounds candidates and residual rows together. */
  private static void assertSourceEstimate(Dataset<Row> frame, long tableRows) {
    String group = "vector-source-statistics";
    spark.sparkContext().setJobGroup(group, "Statistics read only frozen metadata", false);
    try {
      LogicalPlan source =
          CollectionConverters.asJava(frame.queryExecution().optimizedPlan().collectLeaves())
              .stream()
              .filter(leaf -> leaf.nodeName().equals(SOURCE))
              .findFirst()
              .orElseThrow(() -> new AssertionError("No " + SOURCE + " in the logical plan"));
      assertThat(source.stats().rowCount().get().longValue()).isEqualTo(tableRows);
      long bytesPerRow = 8L;
      for (Attribute attribute : CollectionConverters.asJava(source.output())) {
        bytesPerRow += attribute.dataType().defaultSize();
      }
      assertThat(source.stats().sizeInBytes().longValue()).isEqualTo(bytesPerRow * tableRows);
      frame.queryExecution().executedPlan();
      assertThat(spark.sparkContext().statusTracker().getJobIdsForGroup(group)).isEmpty();
    } finally {
      spark.sparkContext().clearJobGroup();
    }
  }

  @Test
  void explainShowsTheDecisionsAndTheDriverBarrier() {
    String table = tableWithIndex("exec_explain");
    String text =
        spark
            .sql(String.format("EXPLAIN EXTENDED %s", nearest(table, 2)))
            .collectAsList()
            .get(0)
            .getString(0);

    assertThat(text).contains("contract=TABLE_LIVE_TOPK");
    assertThat(text).contains("driverMaterialization=true");
    // the number the query ran with, and where it came from, are part of the plan
    // a flat index stores every vector, so the width cannot matter and the default is one
    assertThat(text).contains("overSample=1 [STRUCTURE_DEFAULT, documented-experience-values]");
    assertThat(text).contains("metric=EUCLIDEAN");
    // the request and the budget it was shrunk to are both visible
    assertThat(text).contains("requestedCandidates=");
    assertThat(text).contains("initialCandidates=");
    // both snapshots the plan is frozen to
    assertThat(text).contains("tableSnapshot=");
    assertThat(text).contains("indexSnapshot=");
  }

  @Test
  void planningAVectorQueryRunsNoJob() {
    String table = tableWithIndex("exec_no_jobs");
    String group = "vector-sql-planning";
    spark.sparkContext().setJobGroup(group, "A vector query must not search here", false);
    try {
      Dataset<Row> frame = spark.sql(nearest(table, 2));
      frame.queryExecution().optimizedPlan();
      // the physical plan is built from the lowered node, which must not search either
      frame.queryExecution().executedPlan();
      assertThat(spark.sparkContext().statusTracker().getJobIdsForGroup(group)).isEmpty();
    } finally {
      spark.sparkContext().clearJobGroup();
    }
  }

  @Test
  void aFullyCoveredQueryReportsNoResidualBranch() {
    String table = tableWithIndex("exec_covered");
    Dataset<Row> frame = spark.sql(nearest(table, 2));
    List<Row> rows = frame.collectAsList();

    SparkPlan source = sourceExec(frame);
    assertThat(metric(source, "numResidualFiles")).isZero();
    assertThat(metric(source, "numCandidates")).isEqualTo(2L);
    assertThat(metric(source, "numLiveCandidates")).isGreaterThanOrEqualTo(2L);
    assertThat(metric(source, "numOutputRows")).isEqualTo(rows.size());
    assertThat(metric(source, "numIndexUnderfilled")).isZero();
  }

  @Test
  void aPartlyCoveredQueryReportsBothBranches() {
    String table = tableWithIndex("exec_partly");
    // a row after the build leaves the index behind, so the uncovered file is scanned as well
    spark.sql(String.format("INSERT INTO local.db.%s VALUES (5, array(0.8d, 0.0d))", table));

    Dataset<Row> frame = spark.sql(nearest(table, 2));
    List<Row> rows = frame.collectAsList();

    assertThat(rows).extracting(row -> row.getLong(0)).containsExactly(1L, 5L);
    SparkPlan source = sourceExec(frame);
    assertThat(metric(source, "numResidualFiles")).isEqualTo(1L);
    // the answer holds the candidate branch and the residual branch together
    assertThat(metric(source, "numOutputRows")).isGreaterThanOrEqualTo(rows.size());
  }

  @Test
  void aDeletedHitIsFilledFromAWiderRound() throws Exception {
    String table = tableWithIndex("exec_deleted");
    // the closest row is deleted after the index was built, leaving a hole in the first candidates
    spark.sql(String.format("DELETE FROM local.db.%s WHERE id = 1", table));

    Dataset<Row> frame = spark.sql(nearest(table, 3));
    List<Row> rows = frame.collectAsList();

    assertThat(rows).hasSize(3);
    SparkPlan source = sourceExec(frame);
    // one deleted row is one unresolved hit, even though a wider round returns it again
    assertThat(metric(source, "numDeletedOrInvisibleHits")).isEqualTo(1L);
    // the rounds resolve against the same covered files, so each is counted once and not per round
    Table iceberg = Spark3Util.loadIcebergTable(spark, "local.db." + table);
    long coveredFiles = 0L;
    try (CloseableIterable<FileScanTask> tasks = iceberg.newScan().planFiles()) {
      for (FileScanTask ignored : tasks) {
        coveredFiles += 1;
      }
    }
    assertThat(metric(source, "numCandidateRounds")).isGreaterThanOrEqualTo(2L);
    assertThat(metric(source, "numRowsAtFiles")).isEqualTo(coveredFiles);
    assertThat(metric(source, "numCandidateRounds")).isGreaterThanOrEqualTo(2L);
    assertThat(metric(source, "numLiveCandidates")).isEqualTo(3L);
    assertThat(metric(source, "numIndexUnderfilled")).isZero();
  }

  @Test
  void aPartlyCoveredQueryReportsAnIndexShortfallTheResidualScanFilled() {
    String table = tableWithIndex("exec_partly_short");
    // two rows the index still ranks are deleted, and the uncovered row is what completes the
    // answer: the index branch is short, the query is not
    spark.sql(String.format("DELETE FROM local.db.%s WHERE id IN (1, 2)", table));
    spark.sql(String.format("INSERT INTO local.db.%s VALUES (5, array(0.8d, 0.0d))", table));

    Dataset<Row> frame = spark.sql(nearest(table, 3));
    List<Row> rows = frame.collectAsList();

    assertThat(rows).hasSize(3);
    SparkPlan source = sourceExec(frame);
    assertThat(metric(source, "numResidualFiles")).isEqualTo(1L);
    assertThat(metric(source, "numIndexUnderfilled"))
        .as("the index branch stopped short even though the answer is full")
        .isEqualTo(1L);
    assertThat(metric(source, "numLiveCandidates")).isEqualTo(2L);
  }

  @Test
  void aCompactionLeavesHitsOutsideTheFrozenSnapshotCounted() {
    String table = tableWithIndex("exec_compacted");
    // rewriting the data file the index points at leaves every hit outside the frozen snapshot
    spark.sql(
        String.format(
            "CALL local.system.rewrite_data_files(table => 'db.%s', "
                + "options => map('rewrite-all', 'true'))",
            table));

    Dataset<Row> frame = spark.sql(nearest(table, 2));
    List<Row> rows = frame.collectAsList();
    // the answer is still right: the rewritten file is not covered, so the residual scan supplies
    // it
    assertThat(rows).extracting(row -> row.getLong(0)).containsExactly(1L, 3L);

    SparkPlan source = sourceExec(frame);
    assertThat(metric(source, "numHitsOutsideSnapshot"))
        .as("the hits that point at the replaced file are reported, not silently dropped")
        .isPositive();
    assertThat(metric(source, "numLiveCandidates")).isZero();
    assertThat(metric(source, "numResidualFiles")).isEqualTo(1L);
  }

  @Test
  void aQueryThatCannotBeFilledReportsUnderfilled() {
    String table = tableWithIndex("exec_underfilled");
    spark.sql(String.format("DELETE FROM local.db.%s WHERE id IN (1, 2)", table));

    Dataset<Row> frame = spark.sql(nearest(table, 3));
    List<Row> rows = frame.collectAsList();

    // only two live rows are left, so the query returns them rather than failing or inventing rows
    assertThat(rows).hasSize(2);
    SparkPlan source = sourceExec(frame);
    assertThat(metric(source, "numIndexUnderfilled")).isEqualTo(1L);
    assertThat(metric(source, "numLiveCandidates")).isEqualTo(2L);
  }

  @Test
  void everyMetricAgreesWithAFullScan() {
    for (String metric : List.of("euclidean", "cosine", "dot_product")) {
      String table = tableWithIndex("exec_metric_" + metric, metric);
      String indexed =
          String.format(
              "SELECT id FROM local.db.%s "
                  + "ORDER BY vec_distance(embedding, array(0.9d, 0.1d)) LIMIT 4",
              table);
      // the addition is not a shape the rewrite recognizes, so this one scans the whole table
      String scanned =
          String.format(
              "SELECT id FROM local.db.%s "
                  + "ORDER BY vec_distance(embedding, array(0.9d, 0.1d)) + 0.0d LIMIT 4",
              table);
      List<Long> fromIndex =
          spark.sql(indexed).collectAsList().stream().map(row -> row.getLong(0)).toList();
      List<Long> fromScan =
          spark.sql(scanned).collectAsList().stream().map(row -> row.getLong(0)).toList();
      assertThat(fromIndex).as("metric %s answers from the index", metric).hasSize(4);
      assertThat(fromIndex).as("metric %s matches a scan", metric).isEqualTo(fromScan);
    }
  }

  @Test
  void aBranchAndATimeTravelledSnapshotAreAnsweredFromTheirOwnSnapshot() throws Exception {
    String table = tableWithIndex("exec_branch");
    Table iceberg = Spark3Util.loadIcebergTable(spark, "local.db." + table);
    long base = iceberg.currentSnapshot().snapshotId();
    iceberg.manageSnapshots().createBranch("b1", base).commit();

    // a write on the branch leaves the index behind, so the branch query scans the difference
    spark.sql(
        String.format("INSERT INTO local.db.%s.branch_b1 VALUES (9, array(0.9d, 0.0d))", table));

    assertThat(spark.sql(ordering("exec_branch.branch_b1", 2)).collectAsList())
        .extracting(row -> row.getLong(0))
        .containsExactly(1L, 9L);
    // the snapshot the index was built on is still answered from itself
    assertThat(
            spark
                .sql(
                    String.format(
                        "SELECT id FROM local.db.%s VERSION AS OF %d "
                            + "ORDER BY vec_distance(embedding, array(1.0d, 0.0d)) LIMIT 2",
                        table, base))
                .collectAsList())
        .extracting(row -> row.getLong(0))
        .containsExactly(1L, 3L);
  }

  @Test
  void aPlannedQuerySurvivesAnExpiredSnapshotWhileItsFilesExist() throws Exception {
    String table = tableWithIndex("exec_expired");
    Dataset<Row> frozen = spark.sql(ordering(table, 2));
    frozen.queryExecution().executedPlan();
    Table iceberg = Spark3Util.loadIcebergTable(spark, "local.db." + table);
    long planned = iceberg.currentSnapshot().snapshotId();

    // the table moves on and the snapshot the plan froze is expired, but its files are still there
    spark.sql(String.format("INSERT INTO local.db.%s VALUES (5, array(0.95d, 0.0d))", table));
    iceberg.refresh();
    iceberg.expireSnapshots().expireOlderThan(Long.MAX_VALUE).deleteWith(path -> {}).commit();
    iceberg.refresh();
    assertThat(iceberg.currentSnapshot().snapshotId()).isNotEqualTo(planned);

    List<Row> rows = frozen.collectAsList();
    assertThat(rows).as("the frozen plan still answers").hasSize(2);
    assertThat(rows).extracting(row -> row.getLong(0)).doesNotContain(5L);
  }

  @Test
  void aPlannedQueryFailsWhenItsDataFileIsGone() throws Exception {
    String table = tableWithIndex("exec_data_gone");
    Dataset<Row> frozen = spark.sql(ordering(table, 2));
    frozen.queryExecution().executedPlan();
    Table iceberg = Spark3Util.loadIcebergTable(spark, "local.db." + table);
    String dataFile = iceberg.newScan().planFiles().iterator().next().file().location();
    Files.deleteIfExists(Path.of(URI.create(dataFile)));

    assertThatThrownBy(frozen::collectAsList)
        .as("the plan reports the file it froze instead of switching to a new snapshot")
        .satisfies(error -> assertThat(messages(error)).contains(dataFile));
  }

  @Test
  void aPlannedQueryFailsWhenItsIndexFilesAreGone() throws Exception {
    String table = tableWithIndex("exec_index_gone");
    Dataset<Row> frozen = spark.sql(ordering(table, 2));
    frozen.queryExecution().executedPlan();
    Table iceberg = Spark3Util.loadIcebergTable(spark, "local.db." + table);
    IndexMetadata metadata = VectorIndexes.findIndex(iceberg, "embedding", "vector", null);
    String manifestList = metadata.latestSnapshot().indexManifestList();
    IndexManifestList list = IndexManifestList.read(iceberg.io().newInputFile(manifestList));
    int deleted = 0;
    for (IndexManifestList.ManifestRef ref : list.manifests()) {
      IndexManifest manifest = IndexManifest.read(iceberg.io().newInputFile(ref.location()));
      for (IndexFile file : manifest.indexFiles()) {
        Files.deleteIfExists(Path.of(URI.create(file.location())));
        deleted += 1;
      }
    }
    assertThat(deleted).as("the index has payload files to delete").isPositive();
    String frozenIndex = metadata.indexUuid();

    assertThatThrownBy(frozen::collectAsList)
        .as("a deleted index resource fails the first physical read")
        .satisfies(
            error ->
                assertThat(messages(error))
                    .as("the failure names the index the frozen plan points at")
                    .contains("File does not exist")
                    .contains(frozenIndex));
  }

  /**
   * An operator above the lowered limit captures its child plan in a closure, so whatever the
   * lowered node carries has to survive being serialised to an executor — even though the search
   * itself runs on the driver.
   */
  @Test
  void anAggregateOverARewrittenLimitAnswers() {
    String table = tableWithIndex("exec_aggregate");
    String inner = ordering(table, 3);

    List<Row> rows = spark.sql(String.format("SELECT count(*) FROM (%s)", inner)).collectAsList();
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).getLong(0)).as("the three nearest rows were counted").isEqualTo(3L);

    // what the node decided is still readable in the plan of the query, not only of the inner one
    String plan = spark.sql("EXPLAIN " + inner).collectAsList().get(0).getString(0);
    assertThat(plan).contains("VectorSearchSource");
    assertThat(plan).as("the parameters and the barrier").contains("driverMaterialization=true");
  }

  @Test
  void aGroupByOverARewrittenLimitAnswers() {
    String table = tableWithIndex("exec_group_by");
    String inner = ordering(table, 3);

    List<Row> rows =
        spark
            .sql(String.format("SELECT id, count(*) FROM (%s) GROUP BY id ORDER BY id", inner))
            .collectAsList();
    assertThat(rows)
        .as("the aggregate runs over the rows the index answered")
        .extracting(row -> row.getLong(0))
        .isNotEmpty()
        .hasSizeLessThanOrEqualTo(3);
  }

  @Test
  void aDistinctOverARewrittenLimitAnswers() {
    String table = tableWithIndex("exec_distinct");
    String inner = ordering(table, 3);

    List<Row> rows =
        spark.sql(String.format("SELECT DISTINCT id FROM (%s)", inner)).collectAsList();
    assertThat(rows).hasSize(3);
  }

  @Test
  void aJoinOverARewrittenLimitAnswers() {
    String table = tableWithIndex("exec_join");
    String inner = ordering(table, 3);

    long joined =
        spark
            .sql(
                String.format(
                    "SELECT a.id FROM (%s) a JOIN local.db.%s b ON a.id = b.id", inner, table))
            .collectAsList()
            .size();
    assertThat(joined).as("every answered row joined itself").isEqualTo(3L);
  }

  @Test
  void aWindowOverARewrittenLimitAnswers() {
    String table = tableWithIndex("exec_window");
    String inner = ordering(table, 3);

    List<Row> rows =
        spark
            .sql(String.format("SELECT id, row_number() OVER (ORDER BY id) FROM (%s)", inner))
            .collectAsList();
    assertThat(rows).hasSize(3);
  }

  private static String ordering(String table, int limit) {
    return String.format(
        "SELECT id FROM local.db.%s ORDER BY vec_distance(embedding, array(1.0d, 0.0d)) LIMIT %d",
        table, limit);
  }

  /** Every message in the chain, because Spark wraps a task failure a few times. */
  private static String messages(Throwable error) {
    StringBuilder text = new StringBuilder();
    Throwable current = error;
    while (current != null) {
      text.append(current).append('\n');
      current = current.getCause();
    }
    return text.toString();
  }

  private static String nearest(String table, int limit) {
    return String.format(
        "SELECT id FROM local.db.%s ORDER BY vec_distance(embedding, array(1.0d, 0.0d)) LIMIT %d",
        table, limit);
  }

  /** The lowered source in the executed plan, or a failure naming the plan that was built. */
  private static SparkPlan sourceExec(Dataset<Row> frame) {
    SparkPlan executed = frame.queryExecution().executedPlan();
    for (SparkPlan leaf : CollectionConverters.asJava(executed.collectLeaves())) {
      if (leaf.nodeName().contains(SOURCE)) {
        return leaf;
      }
    }
    throw new AssertionError("No " + SOURCE + " in the plan:\n" + executed);
  }

  private static long metric(SparkPlan plan, String name) {
    SQLMetric metric = CollectionConverters.asJava(plan.metrics()).get(name);
    if (metric == null) {
      throw new AssertionError(
          "No metric " + name + " in " + plan.metrics().keys() + " of " + plan.nodeName());
    }

    return metric.value();
  }

  private static String tableWithIndex(String name) {
    return tableWithIndex(name, "euclidean");
  }

  private static String tableWithIndex(String name, String metric) {
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
    // a position delete keeps the data file the index covers, so the deleted row stays a hit that
    // has to be resolved out and refilled rather than disappearing with a rewritten file
    spark.sql(
        String.format(
            "CREATE TABLE local.db.%s %s TBLPROPERTIES ('write.delete.mode' = 'merge-on-read')",
            name, COLUMNS));
    spark.sql(
        String.format(
            "INSERT INTO local.db.%s VALUES (1, array(1.0d, 0.0d)), (2, array(0.0d, 1.0d)), "
                + "(3, array(0.7d, 0.7d)), (4, array(0.0d, -1.0d))",
            name));
    spark.sql(
        String.format(
            "CALL local.system.build_vector_index(table => 'db.%s', column => 'embedding', "
                + "dimension => 2, metric => '%s', structure => 'flat')",
            name, metric));
    return name;
  }
}
