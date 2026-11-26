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

import java.io.FileNotFoundException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.index.IndexFile;
import org.apache.iceberg.index.IndexManifest;
import org.apache.iceberg.index.IndexManifestList;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.IndexQuery;
import org.apache.iceberg.index.IndexReader;
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
  void aSingleQueryOverManyFilesFetchesTheGlobalWindow() throws Exception {
    String table = "exec_global_fetch";
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
    spark.sql(String.format("CREATE TABLE local.db.%s %s", table, COLUMNS));
    for (int i = 0; i < 65; i++) {
      spark.sql(
          String.format(
              "INSERT INTO local.db.%s VALUES (%d, array(%.2fd, 0.0d))",
              table, i, 1.0d - i * 0.01d));
    }
    spark.sql(
        String.format(
            "CALL local.system.build_vector_index(table => 'db.%s', column => 'embedding', "
                + "dimension => 2, structure => 'flat')",
            table));

    assertThat(dataFileCount(table)).isGreaterThan(64L);
    List<Row> rows = spark.sql(nearest(table, 1)).collectAsList();
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).getLong(0)).isEqualTo(0L);
  }

  /**
   * The global window is the shared candidate budget, not a private multiple of it.
   *
   * <p>One query over many files is answered from the index's own global hits, and the window it
   * asks for is what the sort above re-scores. A hardcoded multiple here would both swallow the
   * multiples a caller sets between one and it, and make the same question cost more than it does
   * through the batch entry point.
   */
  @Test
  void aSingleQueryOverManyFilesAsksForTheSharedWindow() throws Exception {
    String table = "exec_global_window";
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
    spark.sql(String.format("CREATE TABLE local.db.%s %s", table, COLUMNS));
    for (int i = 0; i < 65; i++) {
      spark.sql(
          String.format(
              "INSERT INTO local.db.%s VALUES (%d, array(%.2fd, 0.0d))",
              table, i, 1.0d - i * 0.01d));
    }
    spark.sql(
        String.format(
            "CALL local.system.build_vector_index(table => 'db.%s', column => 'embedding', "
                + "dimension => 2, structure => 'flat')",
            table));
    assertThat(dataFileCount(table)).isGreaterThan(64L);

    int k = 3;
    try {
      for (String refineFactor : new String[] {"0", "1", "2", "4"}) {
        spark.conf().set("spark.iceberg.index.vector.refine-factor", refineFactor);
        Dataset<Row> frame = spark.sql(nearest(table, k));
        assertThat(frame.collectAsList()).hasSize(k);
        assertThat(metric(sourceExec(frame), "numCandidates"))
            .as(
                "the window of refine factor %s is k times the multiple, as the batch starts",
                refineFactor)
            .isEqualTo((long) k * Math.max(1, Integer.parseInt(refineFactor)));
      }
    } finally {
      spark.conf().unset("spark.iceberg.index.vector.refine-factor");
    }
  }

  @Test
  void aSingleQueryOverManyFilesResolvesDeletes() throws Exception {
    String table = "exec_global_fetch_deletes";
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
    spark.sql(
        String.format(
            "CREATE TABLE local.db.%s %s TBLPROPERTIES ('write.delete.mode' = 'merge-on-read')",
            table, COLUMNS));
    for (int i = 0; i < 65; i++) {
      spark.sql(
          String.format(
              "INSERT INTO local.db.%s VALUES (%d, array(%.2fd, 0.0d))",
              table, i, 1.0d - i * 0.01d));
    }
    spark.sql(
        String.format(
            "CALL local.system.build_vector_index(table => 'db.%s', column => 'embedding', "
                + "dimension => 2, structure => 'flat')",
            table));
    spark.sql(String.format("DELETE FROM local.db.%s WHERE id = 0", table));

    List<Row> rows = spark.sql(nearest(table, 1)).collectAsList();
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).getLong(0)).isEqualTo(1L);
  }

  @Test
  void aSingleQueryOverManyFilesAppliesAPostfilter() throws Exception {
    String table = "exec_global_fetch_filter";
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
    spark.sql(String.format("CREATE TABLE local.db.%s %s", table, COLUMNS));
    for (int i = 0; i < 65; i++) {
      spark.sql(
          String.format(
              "INSERT INTO local.db.%s VALUES (%d, array(%.2fd, 0.0d))",
              table, i, 1.0d - i * 0.01d));
    }
    spark.sql(
        String.format(
            "CALL local.system.build_vector_index(table => 'db.%s', column => 'embedding', "
                + "dimension => 2, structure => 'flat')",
            table));

    List<Row> rows =
        spark
            .sql(
                String.format(
                    "SELECT id FROM local.db.%s WHERE id >= 32 "
                        + "ORDER BY vec_distance(embedding, array(1.0d, 0.0d)) LIMIT 1",
                    table))
            .collectAsList();
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).getLong(0)).isEqualTo(32L);
  }

  @Test
  void sourceStatisticsDoNotApplyTheLimitBeforeOversampling() {
    String table = tableWithIndex("exec_stats_refineFactored");
    long tableRows = spark.table("local.db." + table).count();
    String parameter = "spark.iceberg.index.vector.refine-factor";
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
    assertThat(text).contains("refineFactor=1 [DEFAULT]");
    assertThat(text).contains("metric=L2");
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
  void aFullyCoveredQueryReportsNoResidualBranch() throws Exception {
    String table = tableWithIndex("exec_covered");
    Dataset<Row> frame = spark.sql(nearest(table, 2));
    List<Row> rows = frame.collectAsList();

    SparkPlan source = sourceExec(frame);
    assertThat(metric(source, "numResidualFiles")).isZero();
    assertThat(metric(source, "numCandidates")).isEqualTo(2L);
    assertThat(metric(source, "numLiveCandidates")).isGreaterThanOrEqualTo(2L);
    assertThat(metric(source, "numOutputRows")).isEqualTo(rows.size());
    assertThat(metric(source, "numIndexUnderfilled")).isZero();
    // a task is handed the scan tasks of the files its own index file covers, not the table's
    assertThat(metric(source, "numScanTasksShipped")).isEqualTo(coveredFileCount(table));
  }

  @Test
  void aPartlyCoveredQueryReportsBothBranches() throws Exception {
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
    // the covered files are what the index branch ships, so the file the index does not cover is
    // read by the scan branch only and is not carried into every index task
    assertThat(metric(source, "numScanTasksShipped")).isEqualTo(coveredFileCount(table));
    assertThat(metric(source, "numScanTasksShipped")).isLessThan(dataFileCount(table));
  }

  @Test
  void aDeletedHitIsFilledWithoutAWiderRound() throws Exception {
    String table = tableWithIndex("exec_deleted");
    // the closest row is deleted after the index was built, leaving a hole in the first candidates
    spark.sql(String.format("DELETE FROM local.db.%s WHERE id = 1", table));

    Dataset<Row> frame = spark.sql(nearest(table, 3));
    List<Row> rows = frame.collectAsList();

    assertThat(rows).hasSize(3);
    assertThat(rows).extracting(row -> row.getLong(0)).doesNotContain(1L);
    SparkPlan source = sourceExec(frame);
    // one deleted row is one unresolved hit, and it is counted once rather than on every round
    assertThat(metric(source, "numDeletedOrInvisibleHits")).isEqualTo(1L);
    // the rounds resolve against the same covered files, so each is counted once and not per round
    Table iceberg = Spark3Util.loadIcebergTable(spark, "local.db." + table);
    long coveredFiles = 0L;
    try (CloseableIterable<FileScanTask> tasks = iceberg.newScan().planFiles()) {
      for (FileScanTask ignored : tasks) {
        coveredFiles += 1;
      }
    }
    assertThat(metric(source, "numRowsAtFiles")).isEqualTo(coveredFiles);
    // the window is asked for with one candidate to spare, so this table fills it in one round: a
    // deleted hit is a hole the spare candidate covers, not a reason to search wider
    assertThat(metric(source, "numCandidateRounds")).isEqualTo(1L);
    assertThat(metric(source, "numLiveCandidates")).isEqualTo(3L);
    assertThat(metric(source, "numIndexUnderfilled")).isZero();
  }

  @Test
  void aWindowShortOfLiveRowsWidensInsideTheTask() {
    String table = "exec_widened";
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
    spark.sql(
        String.format(
            "CREATE TABLE local.db.%s %s TBLPROPERTIES ('write.delete.mode' = 'merge-on-read')",
            table, COLUMNS));
    // eight rows, and the four candidates a three-row window asks for are not all live: the task
    // that read them has no second job to fall back on, so it asks its own index file for more
    spark.sql(
        String.format(
            "INSERT INTO local.db.%s VALUES (1, array(1.0d, 0.0d)), (2, array(0.9d, 0.0d)), "
                + "(3, array(0.8d, 0.0d)), (4, array(0.7d, 0.0d)), (5, array(0.6d, 0.0d)), "
                + "(6, array(0.5d, 0.0d)), (7, array(0.4d, 0.0d)), (8, array(0.3d, 0.0d))",
            table));
    spark.sql(
        String.format(
            "CALL local.system.build_vector_index(table => 'db.%s', column => 'embedding', "
                + "dimension => 2, structure => 'flat')",
            table));
    spark.sql(String.format("DELETE FROM local.db.%s WHERE id IN (1, 2, 3)", table));

    String parameter = "spark.iceberg.index.vector.refine-factor";
    scala.Option<String> previous = spark.conf().getOption(parameter);
    // the tightest window the caller can ask for, so the deleted rows are not covered by the spare
    // candidates a wider budget would have asked for anyway
    spark.conf().set(parameter, "1");
    try {
      Dataset<Row> frame = spark.sql(nearest(table, 3));
      List<Row> rows = frame.collectAsList();

      assertThat(rows).hasSize(3);
      assertThat(rows).extracting(row -> row.getLong(0)).containsExactly(4L, 5L, 6L);
      SparkPlan source = sourceExec(frame);
      assertThat(metric(source, "numDeletedOrInvisibleHits")).isGreaterThanOrEqualTo(3L);
      assertThat(metric(source, "numCandidateRounds")).isGreaterThanOrEqualTo(2L);
      assertThat(metric(source, "numLiveCandidates")).isEqualTo(3L);
      assertThat(metric(source, "numIndexUnderfilled")).isZero();
    } finally {
      if (previous.isDefined()) {
        spark.conf().set(parameter, previous.get());
      } else {
        spark.conf().unset(parameter);
      }
    }
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
    for (String metric : List.of("l2", "cosine", "dot")) {
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
  void aQuantizedSearchReRanksItsFetchedCandidatesExactly() {
    // a one-bit code is a crude measure in two dimensions, so the row it ranks first is not always
    // the nearest one; the fixture is a deterministic set where the estimate puts the true nearest
    // below the second candidate, so a search that keeps by the estimate cannot return it
    float[][] vectors = quantizedFixture();
    String table = quantizedTable("exec_refine", vectors);
    long exact = exactNearest(vectors);
    assertThat(spark.sql(scanned(table, 1)).collectAsList())
        .as("the exact answer, from a scan")
        .extracting(row -> row.getLong(0))
        .containsExactly(exact);

    try {
      System.clearProperty(EXPERIMENT_REFINE);
      Dataset<Row> estimated = spark.sql(ordering(table, 1));
      assertThat(estimated.collectAsList())
          .as("the estimate alone orders another row first")
          .extracting(row -> row.getLong(0))
          .doesNotContain(exact);
      // without a re-rank the window follows k, with the one spare candidate a task asks for
      assertThat(metric(sourceExec(estimated), "numCandidates")).isEqualTo(1L);
      assertThat(metric(sourceExec(estimated), "numCandidateRequests")).isEqualTo(2L);
      assertThat(metric(sourceExec(estimated), "numFetchedRows"))
          .as("without a re-rank the window is what the task fetches")
          .isEqualTo(2L);

      System.setProperty(EXPERIMENT_REFINE, "true");
      Dataset<Row> refined = spark.sql(ordering(table, 1));
      assertThat(refined.collectAsList())
          .as("the exact re-rank of the fetched candidates finds the true nearest")
          .extracting(row -> row.getLong(0))
          .containsExactly(exact);
      SparkPlan source = sourceExec(refined);
      // the wider window is what the fetch-back paid: eight candidates per result, from which the
      // exact re-rank kept one
      assertThat(metric(source, "numCandidates")).isEqualTo(8L);
      assertThat(metric(source, "numCandidateRequests")).isEqualTo(8L);
      assertThat(metric(source, "numLiveCandidates")).isEqualTo(1L);
      // the fetch-back the wider window paid for; keeping one row does not undo those reads
      assertThat(metric(source, "numFetchedRows")).isEqualTo(8L);
    } finally {
      System.clearProperty(EXPERIMENT_REFINE);
    }
  }

  @Test
  void aRangeSearchReusesTheListBlocksOfThePreviousQuery() {
    String table = "local.db.exec_blocks_range";
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
    spark.sql("CREATE TABLE " + table + " (id bigint, embedding array<double>) USING iceberg");
    spark.sql(
        "INSERT INTO " + table + " SELECT id, array(CAST(id AS double), 1.0d) FROM range(0, 64)");
    spark.sql(
        "CALL local.system.build_vector_index(table => 'db.exec_blocks_range', "
            + "column => 'embedding', dimension => 2, metric => 'l2', structure => 'IVF', "
            + "max_entries_per_index_file => 128, "
            + "options => map('nlist', '8', 'ivf-list-blocks', 'true'))");
    String range = "spark.iceberg.index.vector.ivf.range-read.enabled";
    String probes = "spark.iceberg.index.vector.ivf.nprobe";
    String cache = "spark.iceberg.index.cache.enabled";
    String budget = "spark.iceberg.index.cache.bytes";
    String query =
        "SELECT id FROM "
            + table
            + " ORDER BY vec_distance(embedding, array(31.1d, 1.0d), 'l2') LIMIT 4";
    try {
      spark.conf().set(cache, "true");
      spark.conf().set(budget, "64m");
      spark.conf().set(range, "true");
      spark.conf().set(probes, "2");

      Dataset<Row> cold = spark.sql(query);
      assertThat(cold.collectAsList()).hasSize(4);
      SparkPlan coldSource = sourceExec(cold);
      assertThat(metric(coldSource, "numIndexBlockCacheMisses")).isPositive();
      assertThat(metric(coldSource, "numIndexBlockCacheHits")).isZero();
      assertThat(metric(coldSource, "numIndexBlockLoads")).isPositive();
      long coldBytes = metric(coldSource, "numIndexRangeBytesRead");
      assertThat(coldBytes).isPositive();

      Dataset<Row> warm = spark.sql(query);
      assertThat(warm.collectAsList()).hasSize(4);
      SparkPlan warmSource = sourceExec(warm);
      assertThat(metric(warmSource, "numIndexBlockCacheHits")).isPositive();
      assertThat(metric(warmSource, "numIndexBlockCacheMisses")).isZero();
      assertThat(metric(warmSource, "numIndexRangeBytesRead"))
          .as("the repeat query read its lists from the executor cache")
          .isLessThan(coldBytes);
    } finally {
      spark.conf().unset(range);
      spark.conf().unset(probes);
      spark.conf().unset(cache);
      spark.conf().unset(budget);
    }
  }

  @Test
  void aSearchReportsWhereItsTaskTimeWent() {
    String table = tableWithIndex("exec_stages");
    Dataset<Row> frame = spark.sql(nearest(table, 2));
    assertThat(frame.collectAsList()).hasSize(2);

    // a stage of a two-row fixture can be fast enough to round to zero, so this pins the three the
    // query cannot have done without: it opened and read an index file, scored it, and fetched the
    // rows its hits name
    SparkPlan source = sourceExec(frame);
    long indexRead = metric(source, "indexReadTimeNs");
    long probeScore = metric(source, "probeScoreTimeNs");
    long fetchBack = metric(source, "fetchBackTimeNs");
    long merge = metric(source, "mergeTimeNs");

    assertThat(indexRead).isPositive();
    assertThat(probeScore).isPositive();
    assertThat(fetchBack).isPositive();
    assertThat(metric(source, "numIndexFileBytesRead"))
        .as("a cache that is off reads its index file whole")
        .isPositive();
    assertThat(metric(source, "numIndexRangeBytesRead")).isZero();
    assertThat(metric(source, "numIndexCacheHits")).isZero();

    long stages = indexRead + probeScore + fetchBack + merge;
    long taskTime = metric(source, "taskTimeNs");
    assertThat(taskTime).isPositive();
    assertThat(percentOff(stages, taskTime))
        .as("the four stages cover the task's wall clock: %s of %s ns", stages, taskTime)
        .isLessThan(5.0);
  }

  @Test
  void rangeReadsReportTheBytesAndRequestsTheyTook() {
    String table = "local.db.exec_stages_range";
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
    spark.sql("CREATE TABLE " + table + " (id bigint, embedding array<double>) USING iceberg");
    spark.sql(
        "INSERT INTO " + table + " SELECT id, array(CAST(id AS double), 1.0d) FROM range(0, 32)");
    spark.sql(
        "INSERT INTO " + table + " SELECT id, array(CAST(id AS double), 1.0d) FROM range(32, 64)");
    spark.sql(
        "CALL local.system.build_vector_index(table => 'db.exec_stages_range', "
            + "column => 'embedding', dimension => 2, metric => 'l2', structure => 'IVF', "
            + "max_entries_per_index_file => 128, "
            + "options => map('nlist', '8', 'ivf-list-blocks', 'true'))");
    String range = "spark.iceberg.index.vector.ivf.range.read.enabled";
    String cache = "spark.iceberg.index.cache.enabled";
    try {
      spark.conf().set(cache, "false");
      spark.conf().set(range, "true");
      Dataset<Row> frame =
          spark.sql(
              "SELECT id FROM "
                  + table
                  + " ORDER BY vec_distance(embedding, array(31.1d, 1.0d), 'l2') LIMIT 4");
      assertThat(frame.collectAsList())
          .extracting(row -> row.getLong(0))
          .containsExactly(31L, 32L, 30L, 33L);

      SparkPlan source = sourceExec(frame);
      assertThat(metric(source, "numIndexFileBytesRead"))
          .as("a range read does not read an index file whole")
          .isZero();
      assertThat(metric(source, "numIndexRangeBytesRead")).isPositive();
      assertThat(metric(source, "numIndexRangeReadRequests")).isPositive();
      assertThat(metric(source, "numIndexListBlocksRead")).isPositive();
      assertThat(metric(source, "numIndexBlockCacheHits"))
          .as("the block cache is off, so no block is reused")
          .isZero();
      assertThat(metric(source, "numIndexBlockLoads"))
          .as("the block cache is off, so no block is kept")
          .isZero();
      long stages =
          metric(source, "indexReadTimeNs")
              + metric(source, "probeScoreTimeNs")
              + metric(source, "fetchBackTimeNs")
              + metric(source, "mergeTimeNs");
      assertThat(percentOff(stages, metric(source, "taskTimeNs"))).isLessThan(5.0);
    } finally {
      spark.conf().unset(range);
      spark.conf().unset(cache);
    }
  }

  @Test
  void aWarmCacheReportsHitsRatherThanReads() {
    String table = tableWithIndex("exec_stages_cache");
    String cache = "spark.iceberg.index.cache.enabled";
    String budget = "spark.iceberg.index.cache.bytes";
    try {
      spark.conf().set(cache, "true");
      spark.conf().set(budget, "16m");

      Dataset<Row> cold = spark.sql(nearest(table, 2));
      assertThat(cold.collectAsList()).hasSize(2);
      SparkPlan coldSource = sourceExec(cold);
      assertThat(metric(coldSource, "numIndexCacheMisses")).isPositive();
      assertThat(metric(coldSource, "numIndexCacheHits")).isZero();
      assertThat(metric(coldSource, "numIndexFileBytesRead")).isPositive();

      Dataset<Row> warm = spark.sql(nearest(table, 2));
      assertThat(warm.collectAsList()).hasSize(2);
      SparkPlan warmSource = sourceExec(warm);
      assertThat(metric(warmSource, "numIndexCacheHits")).isPositive();
      assertThat(metric(warmSource, "numIndexCacheMisses")).isZero();
      assertThat(metric(warmSource, "numIndexFileBytesRead")).isZero();
    } finally {
      spark.conf().unset(cache);
      spark.conf().unset(budget);
    }
  }

  @Test
  void anEmptyAnswerStillReportsWhereItsTimeWent() {
    // every row is deleted after the index was built, so the search reads its index file, scores it
    // and fetches candidates that turn out not to be live: an empty answer that still cost work,
    // and the stages have to say so rather than reporting zero because there is no row to return
    String table = tableWithIndex("exec_stages_empty");
    spark.sql(String.format("DELETE FROM local.db.%s", table));
    assertThat(spark.table("local.db." + table).count()).isZero();

    Dataset<Row> frame = spark.sql(nearest(table, 2));
    assertThat(frame.collectAsList()).isEmpty();

    SparkPlan source = sourceExec(frame);
    assertThat(metric(source, "numLiveCandidates")).isZero();
    assertThat(metric(source, "indexReadTimeNs")).isPositive();
    assertThat(metric(source, "probeScoreTimeNs")).isPositive();
    assertThat(metric(source, "fetchBackTimeNs")).isPositive();
    assertThat(metric(source, "numIndexFileBytesRead")).isPositive();
    long stages =
        metric(source, "indexReadTimeNs")
            + metric(source, "probeScoreTimeNs")
            + metric(source, "fetchBackTimeNs")
            + metric(source, "mergeTimeNs");
    assertThat(percentOff(stages, metric(source, "taskTimeNs"))).isLessThan(5.0);
  }

  /** How far a measured number is from the number it should cover, in percent. */
  private static double percentOff(long measured, long total) {
    return Math.abs(measured - total) * 100.0 / Math.max(1L, total);
  }

  /** The system property that enables the internal exact re-ranking, for measurement. */
  private static final String EXPERIMENT_REFINE = "iceberg.index.vector.refine";

  /**
   * Twenty spread two-dimensional vectors, seeded deterministically.
   *
   * <p>The seed was chosen so that a one-bit code, which is what this fixture builds, ranks a row
   * other than the true nearest first, and ranks the true nearest inside its eighth candidate: that
   * is the case an exact re-rank of a fetched window can correct.
   */
  private static float[][] quantizedFixture() {
    Random random = new Random(10L);
    float[][] vectors = new float[20][2];
    for (int i = 0; i < vectors.length; i++) {
      vectors[i][0] = (float) random.nextGaussian();
      vectors[i][1] = (float) random.nextGaussian();
    }

    return vectors;
  }

  /** A table of the fixture whose index is a single quantized IVF list. */
  private static String quantizedTable(String name, float[][] vectors) {
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
    spark.sql(String.format("CREATE TABLE local.db.%s %s", name, COLUMNS));
    StringBuilder values = new StringBuilder();
    for (int i = 0; i < vectors.length; i++) {
      values.append(i == 0 ? "" : ", ");
      values.append(String.format("(%d, array(%sd, %sd))", i + 1, vectors[i][0], vectors[i][1]));
    }

    spark.sql(String.format("INSERT INTO local.db.%s VALUES %s", name, values));
    spark.sql(
        String.format(
            "CALL local.system.build_vector_index(table => 'db.%s', column => 'embedding', "
                + "dimension => 2, metric => 'l2', structure => 'ivf', quantizer => 'rabitq', "
                + "options => map('nlist', '1', 'rabitq-bits', '1'))",
            name));
    return name;
  }

  /** The same query without the index rewrite, which is the exact distance ordered by a scan. */
  private static String scanned(String table, int limit) {
    return String.format(
        "SELECT id FROM local.db.%s "
            + "ORDER BY vec_distance(embedding, array(1.0d, 0.0d)) + 0.0d LIMIT %d",
        table, limit);
  }

  /** The id of the row nearest to the query, computed from the vectors the fixture inserted. */
  private static long exactNearest(float[][] vectors) {
    int best = 0;
    double bestDistance = Double.POSITIVE_INFINITY;
    for (int i = 0; i < vectors.length; i++) {
      double dx = vectors[i][0] - 1.0;
      double distance = dx * dx + (double) vectors[i][1] * vectors[i][1];
      if (distance < bestDistance) {
        bestDistance = distance;
        best = i;
      }
    }

    return best + 1;
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
    Set<Path> deletedPaths = new HashSet<>();
    for (IndexManifestList.ManifestRef ref : list.manifests()) {
      IndexManifest manifest = IndexManifest.read(iceberg.io().newInputFile(ref.location()));
      for (IndexFile file : manifest.indexFiles()) {
        Path path = Path.of(URI.create(file.location()));
        Files.delete(path);
        deletedPaths.add(path);
      }
    }
    assertThat(deletedPaths).as("the index has payload files to delete").isNotEmpty();

    assertThatThrownBy(frozen::collectAsList)
        .as("the frozen plan fails because an index payload is missing")
        .rootCause()
        .isInstanceOf(FileNotFoundException.class)
        .satisfies(
            cause ->
                assertThat(deletedPaths)
                    .anySatisfy(path -> assertThat(cause).hasMessageContaining(path.toString())));
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

  /** How many data files the table has, which is what a task used to be handed. */
  private static long dataFileCount(String table) throws Exception {
    Table iceberg = Spark3Util.loadIcebergTable(spark, "local.db." + table);
    long count = 0L;
    try (CloseableIterable<FileScanTask> tasks = iceberg.newScan().planFiles()) {
      for (FileScanTask ignored : tasks) {
        count += 1;
      }
    }

    return count;
  }

  /**
   * How many data files the table's vector index has entries for, which is what a task is handed.
   */
  private static long coveredFileCount(String table) throws Exception {
    Table iceberg = Spark3Util.loadIcebergTable(spark, "local.db." + table);
    IndexMetadata index = VectorIndexes.findIndex(iceberg, "embedding", "vector");
    IndexQuery query =
        IndexQuery.of(index.latestSnapshot().sourceTableSnapshotId(), ByteBuffer.allocate(0), 0);
    Set<String> covered = new HashSet<>();
    for (IndexFile file : new IndexReader(index, iceberg).indexFiles(query)) {
      covered.addAll(file.coveredDataFiles());
    }

    // the snapshot's own files only: an entry of a file a rewrite replaced is not a scan task
    try (CloseableIterable<FileScanTask> tasks = iceberg.newScan().planFiles()) {
      Set<String> current = new HashSet<>();
      for (FileScanTask task : tasks) {
        current.add(task.file().location().toString());
      }

      covered.retainAll(current);
    }

    return covered.size();
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
    return tableWithIndex(name, "l2");
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
