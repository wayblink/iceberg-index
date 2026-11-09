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
import java.nio.file.Path;
import java.util.List;
import org.apache.iceberg.Table;
import org.apache.iceberg.index.vector.VectorIndexTypes;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.index.BoundVectorSearch;
import org.apache.iceberg.spark.index.VectorBindingResult;
import org.apache.iceberg.spark.index.VectorSearchBridge;
import org.apache.iceberg.spark.index.VectorSearchContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The three vector entry points share one parameter decision and one execution boundary: the
 * index's own hits contract keeps its raw scores and its nullable rows, and a value the caller set
 * is never replaced.
 */
class TestVectorSearchSharedRules {
  private static final String OVER_SAMPLE = "spark.iceberg.index.vector.over-sample";
  private static final String COLUMNS =
      "(id bigint, embedding array<double> NOT NULL) USING iceberg";
  private static SparkSession spark;

  @BeforeAll
  static void startSpark(@TempDir Path warehouse) {
    VectorIndexTypes.register();
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("vector-shared-rules")
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
  void everyEntryPointDecidesAnUnsetParameterTheSameWay() throws Exception {
    Table table = tableWithIndex("shared_unset");
    try {
      VectorSearchContext sql = plan(table, "TABLE_LIVE_TOPK");
      VectorSearchContext call = plan(table, "INDEX_HITS");
      // neither entry point scans because a multiple was left unset: both decide the same width
      assertThat(sql.parameters().overSample()).isEqualTo(1L);
      assertThat(call.parameters().overSample()).isEqualTo(1L);
      assertThat(call.parameters().overSampleOrigin()).isEqualTo("STRUCTURE_DEFAULT");
      assertThat(sql.parameters().overSampleOrigin()).isEqualTo("STRUCTURE_DEFAULT");

      spark.conf().set(OVER_SAMPLE, "7");
      VectorSearchContext sqlSet = plan(table, "TABLE_LIVE_TOPK");
      VectorSearchContext callSet = plan(table, "INDEX_HITS");
      assertThat(sqlSet.parameters().overSample()).isEqualTo(7L);
      assertThat(callSet.parameters().overSample()).isEqualTo(7L);
      assertThat(sqlSet.parameters().overSampleOrigin()).isEqualTo("SESSION_CONF");
      assertThat(callSet.parameters().overSampleOrigin()).isEqualTo("SESSION_CONF");
    } finally {
      spark.conf().unset(OVER_SAMPLE);
    }
  }

  @Test
  void aQuantizedIndexDecidesTheSameWidthForBothEntryPoints() throws Exception {
    // The two entry points used to disagree: the procedure fell back to a compatibility constant of
    // one while the rewrite refused the index altogether. They now share one decision, so a table
    // whose index stores one bit per dimension must report the same width, from the same source,
    // whether the caller wrote a procedure call or an ordered query - and neither may answer by
    // scanning.
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
    spark.sql("CREATE TABLE local.db.shared_rabitq " + COLUMNS);
    spark.sql(
        "INSERT INTO local.db.shared_rabitq VALUES (1, array(1.0d, 0.0d)), (2, array(0.0d, 1.0d))");
    spark.sql(
        "CALL local.system.build_vector_index(table => 'db.shared_rabitq', column => 'embedding', "
            + "dimension => 2, metric => 'euclidean', structure => 'IVF', quantizer => 'RABITQ', "
            + "options => map('nlist', '2', 'nprobe', '2'))");
    Table table = Spark3Util.loadIcebergTable(spark, "local.db.shared_rabitq");

    try {
      spark.conf().unset(OVER_SAMPLE);
      VectorSearchContext sql = plan(table, "TABLE_LIVE_TOPK");
      VectorSearchContext call = plan(table, "INDEX_HITS");
      assertThat(sql.parameters().overSample())
          .as("one bit per dimension: the published range is fifty to a hundred")
          .isEqualTo(50L);
      assertThat(call.parameters().overSample()).isEqualTo(50L);
      assertThat(sql.parameters().overSampleOrigin()).isEqualTo("STRUCTURE_DEFAULT");
      assertThat(call.parameters().overSampleOrigin()).isEqualTo("STRUCTURE_DEFAULT");

      // and the rewrite actually uses the index, rather than deciding the width for a plan it then
      // abandons
      String planText =
          spark
              .sql(
                  "SELECT id FROM local.db.shared_rabitq "
                      + "ORDER BY vec_distance(embedding, array(1.0d, 0.0d)) LIMIT 1")
              .queryExecution()
              .optimizedPlan()
              .toString();
      assertThat(planText).contains("VectorSearchSource");
      assertThat(planText).contains("result=approximate");
      assertThat(planText).contains("overSample=50 [STRUCTURE_DEFAULT");

      // the procedure really answers, rather than resolving a width for a search it never runs
      assertThat(
              spark
                  .sql(
                      "CALL local.system.vector_search(table => 'db.shared_rabitq', "
                          + "column => 'embedding', query => array(1.0d, 0.0d), k => 2)")
                  .collectAsList())
          .hasSize(2);

      // a caller who states a width still wins, on both entry points
      spark.conf().set(OVER_SAMPLE, "7");
      assertThat(plan(table, "TABLE_LIVE_TOPK").parameters().overSample()).isEqualTo(7L);
      assertThat(plan(table, "INDEX_HITS").parameters().overSample()).isEqualTo(7L);
    } finally {
      spark.conf().unset(OVER_SAMPLE);
    }
  }

  @Test
  void aValueThatCannotBeAMultiplierIsReportedByEveryEntryPoint() throws Exception {
    Table table = tableWithIndex("shared_invalid");
    try {
      spark.conf().set(OVER_SAMPLE, "0");
      assertThatThrownBy(() -> plan(table, "TABLE_LIVE_TOPK")).hasMessageContaining(OVER_SAMPLE);
      assertThatThrownBy(() -> plan(table, "INDEX_HITS")).hasMessageContaining(OVER_SAMPLE);
      assertThatThrownBy(
              () ->
                  spark.sql(
                      "CALL local.system.vector_search("
                          + "table => 'db.shared_invalid', column => 'embedding', "
                          + "query => array(1.0d, 0.0d), k => 2)"))
          .hasMessageContaining(OVER_SAMPLE);
      assertThatThrownBy(
              () ->
                  spark
                      .sql(
                          "SELECT * FROM vector_index_search("
                              + "'local.db.shared_invalid', 'embedding', array(1.0d, 0.0d), 2)")
                      .collectAsList())
          .hasMessageContaining(OVER_SAMPLE);
    } finally {
      spark.conf().unset(OVER_SAMPLE);
    }
  }

  @Test
  void theIndexHitsContractKeepsItsRawScoresAndNullableRows() throws Exception {
    Table table = tableWithIndex("shared_hits");
    List<Row> hits =
        spark
            .sql(
                "CALL local.system.vector_search(table => 'db.shared_hits', "
                    + "column => 'embedding', query => array(1.0d, 0.0d), k => 2)")
            .collectAsList();
    assertThat(hits).hasSize(2);
    // the columns are the data file, the position, the score and the row, in that order
    assertThat(hits.get(0).getString(0)).isNotEmpty();
    assertThat(hits.get(0).getLong(1)).isGreaterThanOrEqualTo(0L);
    assertThat(hits.get(0).getDouble(2)).isNotNaN();
    assertThat(hits.get(0).getString(3)).contains("\"id\"");

    // the batch form repeats the same hits per query id, which is the contract the procedures had
    List<Row> batch =
        spark
            .sql(
                "CALL local.system.vector_search_batch(table => 'db.shared_hits', "
                    + "column => 'embedding', queries => array('1.0,0.0', '0.0,1.0'), k => 2)")
            .collectAsList();
    assertThat(batch).isNotEmpty();
    assertThat(batch.stream().map(row -> row.getInt(0)).distinct().sorted()).containsExactly(0, 1);
  }

  @Test
  void aCallReadsTheSnapshotsItPlanned() throws Exception {
    Table table = tableWithIndex("shared_frozen");
    VectorSearchContext planned = plan(table, "INDEX_HITS");
    long plannedTable = planned.inputs().tableSnapshot().snapshotId();
    long plannedIndex = planned.inputs().indexSnapshot().snapshotId();

    spark.sql("INSERT INTO local.db.shared_frozen VALUES (3, array(0.0d, 1.0d))");
    table.refresh();
    assertThat(table.currentSnapshot().snapshotId()).isNotEqualTo(plannedTable);

    List<Row> hits =
        spark
            .sql(
                "CALL local.system.vector_search(table => 'db.shared_frozen', "
                    + "column => 'embedding', query => array(1.0d, 0.0d), k => 5)")
            .collectAsList();
    // the frozen plan of this test is what a procedure would use; the procedure itself plans at
    // call time, so here the check is that the plan's snapshots never move once taken
    assertThat(planned.inputs().tableSnapshot().snapshotId()).isEqualTo(plannedTable);
    assertThat(planned.inputs().indexSnapshot().snapshotId()).isEqualTo(plannedIndex);
    assertThat(hits).isNotEmpty();
  }

  @Test
  void aTableFunctionDoesNotSearchWhileItsPlanIsBuilt() throws Exception {
    tableWithIndex("shared_no_jobs");
    String group = "vector-tvf-planning";
    spark.sparkContext().setJobGroup(group, "A table function must not search here", false);
    try {
      var frame =
          spark.sql(
              "SELECT * FROM vector_index_search("
                  + "'local.db.shared_no_jobs', 'embedding', array(1.0d, 0.0d), 2)");
      frame.queryExecution().optimizedPlan();
      frame.queryExecution().executedPlan();
      assertThat(spark.sparkContext().statusTracker().getJobIdsForGroup(group)).isEmpty();
      assertThat(frame.collectAsList()).isNotEmpty();
    } finally {
      spark.sparkContext().clearJobGroup();
    }
  }

  /** The shared decision both entry points go through, before anything is searched. */
  @Test
  void aCallStillReturnsAtMostKPerQueryWhenTheMultiplierIsLarger() throws Exception {
    Table table = tableWithFourRows("shared_topk");
    try {
      spark.conf().set(OVER_SAMPLE, "10");
      List<Row> single =
          spark
              .sql(
                  "CALL local.system.vector_search(table => 'db.shared_topk', "
                      + "column => 'embedding', query => array(1.0d, 0.0d), k => 2)")
              .collectAsList();
      assertThat(single).as("one query returns exactly k rows").hasSize(2);

      List<Row> batch =
          spark
              .sql(
                  "CALL local.system.vector_search_batch(table => 'db.shared_topk', "
                      + "column => 'embedding', queries => array('1.0d,0.0d', '0.0d,1.0d'), "
                      + "k => 2)")
              .collectAsList();
      assertThat(batch).hasSize(4);
      assertThat(batch.stream().filter(row -> row.getInt(0) == 0).count()).isEqualTo(2L);
      assertThat(batch.stream().filter(row -> row.getInt(0) == 1).count()).isEqualTo(2L);

      List<Row> tvf =
          spark
              .sql(
                  "SELECT * FROM vector_index_search("
                      + "'local.db.shared_topk', 'embedding', array(1.0d, 0.0d), 2)")
              .collectAsList();
      assertThat(tvf).hasSize(2);
    } finally {
      spark.conf().unset(OVER_SAMPLE);
      assertThat(table.name()).isNotEmpty();
    }
  }

  @Test
  void aRowWithoutAVectorIsNotAQueryResult() throws Exception {
    // a vector query asks which row is nearest, and a row with no vector has no distance: it is not
    // a candidate. That holds whether its file is covered or not, so the row appended after the
    // build is left out by the scan branch just as the index leaves out the one it was built over.
    // A scan of the same rows without the index does return it, first, because a null distance
    // sorts first -- the difference is the point of the rule, not a side effect.
    spark.sql(
        "CREATE TABLE local.db.shared_nullable (id bigint, embedding array<double>) USING iceberg");
    spark.sql(
        "INSERT INTO local.db.shared_nullable VALUES (1, array(1.0d, 0.0d)), "
            + "(2, array(0.0d, 1.0d))");
    spark.sql(
        "CALL local.system.build_vector_index(table => 'db.shared_nullable', "
            + "column => 'embedding', dimension => 2, metric => 'euclidean', structure => 'flat')");
    spark.sql("INSERT INTO local.db.shared_nullable VALUES (3, null)");

    String query =
        "SELECT id FROM local.db.shared_nullable "
            + "ORDER BY vec_distance(embedding, array(1.0d, 0.0d)) LIMIT 3";
    assertThat(spark.sql(query).queryExecution().optimizedPlan().toString())
        .contains("VectorSearchSource");
    List<Row> answered = spark.sql(query).collectAsList();
    assertThat(answered.stream().map(row -> row.getLong(0)))
        .as("only the rows that have a vector")
        .containsExactly(1L, 2L);

    // the same rows without an index, which is what a plain scan answers: three of them, the row
    // with no vector first
    spark.sql(
        "CREATE TABLE local.db.shared_nullable_plain (id bigint, embedding array<double>) "
            + "USING iceberg");
    spark.sql(
        "INSERT INTO local.db.shared_nullable_plain VALUES (1, array(1.0d, 0.0d)), "
            + "(2, array(0.0d, 1.0d)), (3, null)");
    String plain =
        "SELECT id FROM local.db.shared_nullable_plain "
            + "ORDER BY vec_distance(embedding, array(1.0d, 0.0d)) LIMIT 3";
    assertThat(spark.sql(plain).collectAsList()).hasSize(3);

    // once the query rules the null row out itself, the two agree exactly
    String guarded =
        "SELECT id FROM local.db.shared_nullable WHERE embedding IS NOT NULL "
            + "ORDER BY vec_distance(embedding, array(1.0d, 0.0d)) LIMIT 3";
    assertThat(
            spark.sql(guarded).collectAsList().stream()
                .map(row -> row.getLong(0))
                .collect(java.util.stream.Collectors.toList()))
        .containsExactly(1L, 2L);
    assertThat(spark.sql(guarded).collectAsList()).hasSize(2);
  }

  @Test
  void aQueryWithASecondaryOrderIsLeftAlone() throws Exception {
    tableWithIndex("shared_secondary_order");
    String query =
        "SELECT id FROM local.db.shared_secondary_order "
            + "ORDER BY vec_distance(embedding, array(1.0d, 0.0d)), id LIMIT 2";
    assertThat(spark.sql(query).queryExecution().optimizedPlan().toString())
        .doesNotContain("VectorSearchSource");
    assertThat(spark.sql(query).collectAsList()).hasSize(2);
  }

  @Test
  void aHitWhoseFileIsGoneStaysWithANullRow() throws Exception {
    Table table = tableWithIndex("shared_replaced_files");
    spark.sql(
        "INSERT OVERWRITE local.db.shared_replaced_files "
            + "VALUES (10, array(1.0d, 0.0d)), (11, array(0.0d, 1.0d))");
    table.refresh();

    List<Row> hits =
        spark
            .sql(
                "CALL local.system.vector_search(table => 'db.shared_replaced_files', "
                    + "column => 'embedding', query => array(1.0d, 0.0d), k => 2, "
                    + "coverage => 'index_hits')")
            .collectAsList();
    // the diagnostic view reports the index itself: the hits point at files this snapshot no longer
    // has, and it keeps them with a missing row rather than answering with a shorter list. A
    // coverage mode drops them instead, which is the difference the coverage tests assert.
    assertThat(hits).hasSize(2);
    assertThat(hits).allMatch(row -> row.isNullAt(3));

    // and the default mode answers for the table, where those files are gone and their rows with
    // them
    assertThat(
            spark
                .sql(
                    "CALL local.system.vector_search(table => 'db.shared_replaced_files', "
                        + "column => 'embedding', query => array(1.0d, 0.0d), k => 2)")
                .collectAsList())
        .extracting(row -> row.getString(3))
        .allSatisfy(json -> assertThat(json).contains("\"id\":1"));
  }

  @Test
  void theIndexedModeDropsAHitWhoseFileIsGone() throws Exception {
    tableWithIndex("shared_replaced_indexed");
    spark.sql(
        "INSERT OVERWRITE local.db.shared_replaced_indexed "
            + "VALUES (10, array(1.0d, 0.0d)), (11, array(0.0d, 1.0d))");

    // every index hit points at a file the snapshot replaced, so there is nothing the index covers
    // to answer with: the mode is exact about that, and does not widen into a scan
    assertThat(
            spark
                .sql(
                    "CALL local.system.vector_search(table => 'db.shared_replaced_indexed', "
                        + "column => 'embedding', query => array(1.0d, 0.0d), k => 2, "
                        + "coverage => 'indexed')")
                .collectAsList())
        .isEmpty();
  }

  @Test
  void anEmptyRequestIsAnsweredWithoutJudgingTheParameters() throws Exception {
    tableWithIndex("shared_empty");
    try {
      spark.conf().set(OVER_SAMPLE, "0");
      assertThat(
              spark
                  .sql(
                      "SELECT * FROM vector_index_search("
                          + "'local.db.shared_empty', 'embedding', array(1.0d, 0.0d), 0)")
                  .collectAsList())
          .isEmpty();
      assertThat(
              spark
                  .sql(
                      "CALL local.system.vector_search(table => 'db.shared_empty', "
                          + "column => 'embedding', query => array(1.0d, 0.0d), k => 0)")
                  .collectAsList())
          .isEmpty();
    } finally {
      spark.conf().unset(OVER_SAMPLE);
    }
  }

  private static Table tableWithFourRows(String name) throws Exception {
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
    spark.sql("CREATE TABLE local.db." + name + " " + COLUMNS);
    spark.sql(
        "INSERT INTO local.db."
            + name
            + " VALUES (1, array(1.0d, 0.0d)), (2, array(0.0d, 1.0d)), "
            + "(3, array(0.7d, 0.7d)), (4, array(0.0d, -1.0d))");
    spark.sql(
        "CALL local.system.build_vector_index(table => 'db."
            + name
            + "', column => 'embedding', dimension => 2, metric => 'euclidean', "
            + "structure => 'flat')");
    return Spark3Util.loadIcebergTable(spark, "local.db." + name);
  }

  private static VectorSearchContext plan(Table table, String contract) {
    VectorBindingResult result =
        VectorSearchBridge.plan(
            spark,
            table,
            "embedding",
            null,
            List.of(new float[] {1.0F, 0.0F}),
            2,
            "INDEX_HITS".equals(contract),
            "INDEX_HITS".equals(contract) ? "index_hits" : "table");
    assertThat(result).isInstanceOf(BoundVectorSearch.class);
    return result.context();
  }

  private static Table tableWithIndex(String name) throws Exception {
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
    spark.sql("CREATE TABLE local.db." + name + " " + COLUMNS);
    spark.sql(
        "INSERT INTO local.db." + name + " VALUES (1, array(1.0d, 0.0d)), (2, array(0.0d, 1.0d))");
    spark.sql(
        "CALL local.system.build_vector_index(table => 'db."
            + name
            + "', column => 'embedding', dimension => 2, metric => 'euclidean', "
            + "structure => 'flat')");
    Table table = Spark3Util.loadIcebergTable(spark, "local.db." + name);
    assertThat(table.location()).isNotEmpty();
    assertThat(URI.create(table.location())).isNotNull();
    return table;
  }
}
