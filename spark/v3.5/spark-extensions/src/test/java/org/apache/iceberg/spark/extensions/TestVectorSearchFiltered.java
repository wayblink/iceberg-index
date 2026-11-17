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
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.Table;
import org.apache.iceberg.index.vector.VectorIndexTypes;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.metric.SQLMetric;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import scala.jdk.javaapi.CollectionConverters;

/**
 * A vector query that also filters on another column is answered from the index, over the filtered
 * rows.
 *
 * <p>Without a carried filter the query still runs, but on a table scan: the rewrite only admits a
 * not-null guard on the vector column. These tests are what say the filter reaches the search
 * rather than ending the rewrite.
 */
class TestVectorSearchFiltered {
  private static final int DIMENSION = 4;
  private static final int ROWS = 200;
  private static final int KEPT = 150;

  private static SparkSession spark;

  @BeforeAll
  static void startSpark(@TempDir Path warehouse) {
    VectorIndexTypes.register();
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("vector-search-filtered")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.adaptive.enabled", "false")
            .config("spark.task.cpus", "2")
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
  void answersAFilteredQueryFromTheIndex() throws Exception {
    Table table = tableWithIndex("filtered_pre");

    List<Row> rows =
        spark
            .sql(
                "SELECT id, price FROM local.db.filtered_pre "
                    + "WHERE price < 40 ORDER BY vec_distance(embedding, array("
                    + query()
                    + "), 'l2') LIMIT 5")
            .collectAsList();

    assertThat(rows)
        .as("a filtered query keeps the window it asked for while answering only from the filter")
        .hasSize(5);
    assertThat(rows).allSatisfy(row -> assertThat(row.getLong(1)).isLessThan(40L));
    assertThat(table.currentSnapshot()).isNotNull();
  }

  @Test
  void skipsIndexSearchWhenNoRowsPassThePrefilter() throws Exception {
    String table = "filtered_empty_prefilter";
    tableWithIndex(table);
    spark.conf().set("spark.iceberg.index.vector.filter-mode", "pre");
    try {
      Dataset<Row> frame =
          spark.sql(
              "SELECT id FROM local.db."
                  + table
                  + " WHERE price < 0 ORDER BY vec_distance(embedding, array("
                  + query()
                  + "), 'l2') LIMIT 5");

      assertThat(frame.collectAsList()).isEmpty();
      SparkPlan source = sourceExec(frame);
      assertThat(metric(source, "numCandidateRounds")).isZero();
      assertThat(metric(source, "numRowsAtFiles")).isZero();
    } finally {
      spark.conf().unset("spark.iceberg.index.vector.filter-mode");
    }
  }

  @Test
  void plansTheFilteredQueryAsAnIndexSearch() throws Exception {
    tableWithIndex("filtered_plan");

    String plan =
        spark
            .sql(
                "SELECT id FROM local.db.filtered_plan "
                    + "WHERE price < 40 ORDER BY vec_distance(embedding, array("
                    + query()
                    + "), 'l2') LIMIT 5")
            .queryExecution()
            .executedPlan()
            .toString();

    assertThat(plan)
        .as("the filter no longer sends the whole query back to a table scan")
        .doesNotContain("BatchScan")
        .contains("VectorSearch");
  }

  @Test
  void answersTheUnfilteredQueryAsBefore() throws Exception {
    tableWithIndex("filtered_none");

    List<Row> rows =
        spark
            .sql(
                "SELECT id FROM local.db.filtered_none ORDER BY vec_distance(embedding, array("
                    + query()
                    + "), 'l2') LIMIT 5")
            .collectAsList();

    assertThat(rows).hasSize(5);
  }

  @Test
  void fillsTheWindowForADenseFilterByFilteringAfterTheSearch() throws Exception {
    tableWithIndex("filtered_dense");

    // every row passes, so the filter narrows nothing: the window still has to be full, which is
    // what filtering after the search has to deliver however few rows it has to drop
    List<Row> rows =
        withConf(
            "post",
            () ->
                spark
                    .sql(
                        "SELECT id, price FROM local.db.filtered_dense "
                            + "WHERE price >= 0 ORDER BY vec_distance(embedding, array("
                            + query()
                            + "), 'l2') LIMIT 5")
                    .collectAsList());

    assertThat(rows).as("a dense filter is still answered with a full window").hasSize(5);
    assertThat(rows).allSatisfy(row -> assertThat(row.getLong(1)).isGreaterThanOrEqualTo(0L));
  }

  @Test
  void appliesTheFilterBeforeTheSearchWhenAsked() throws Exception {
    tableWithIndex("filtered_mode_pre");

    List<Row> rows =
        withConf(
            "pre",
            () ->
                spark
                    .sql(
                        "SELECT id, price FROM local.db.filtered_mode_pre "
                            + "WHERE price < 40 ORDER BY vec_distance(embedding, array("
                            + query()
                            + "), 'l2') LIMIT 5")
                    .collectAsList());

    assertThat(rows).hasSize(5);
    assertThat(rows).allSatisfy(row -> assertThat(row.getLong(1)).isLessThan(40L));
  }

  @Test
  void appliesTheFilterAfterTheSearchWhenAsked() throws Exception {
    tableWithIndex("filtered_mode_post");

    List<Row> rows =
        withConf(
            "post",
            () ->
                spark
                    .sql(
                        "SELECT id, price FROM local.db.filtered_mode_post "
                            + "WHERE price < 40 ORDER BY vec_distance(embedding, array("
                            + query()
                            + "), 'l2') LIMIT 5")
                    .collectAsList());

    assertThat(rows)
        .as("the post-filter link widens the search until the window is full of rows that pass")
        .hasSize(5);
    assertThat(rows).allSatisfy(row -> assertThat(row.getLong(1)).isLessThan(40L));
  }

  @Test
  void rejectsAFilterModeItDoesNotKnow() throws Exception {
    tableWithIndex("filtered_mode_bad");

    assertThatThrownBy(
            () ->
                withConf(
                    "sometimes",
                    () ->
                        spark
                            .sql(
                                "SELECT id FROM local.db.filtered_mode_bad "
                                    + "WHERE price < 40 ORDER BY vec_distance(embedding, array("
                                    + query()
                                    + "), 'l2') LIMIT 5")
                            .collectAsList()))
        .as("an unknown mode is refused rather than answered some other way")
        .hasMessageContaining("filter-mode");
  }

  @Test
  void runsTheFilterWhereTheTaskSearches() throws Exception {
    // one row of two hundred passes this filter and it is the farthest from the query, so the two
    // links pay for it differently: reading the filter first scores that one row and answers in a
    // single request, while judging after the search rejects window after window until the search
    // has nothing left to offer. Both are answering the same question - the link decides what a
    // window that keeps being rejected costs, which is why pre can fill one and post may not.
    tableWithIndex("filtered_local");

    assertThat(idsFor("filtered_local", "pre", " WHERE price = 299"))
        .as("the filter runs before the search, so the admitted row is what gets scored")
        .containsExactly(199L);
    assertThat(roundsFor("filtered_local", "pre", " WHERE price = 299"))
        .as("a search restricted to the admitted rows asks its file once")
        .isEqualTo(1L);
    assertThat(roundsFor("filtered_local", "post", " WHERE price = 299"))
        .as("a search that scores everything first pays a round for every window it rejects")
        .isGreaterThan(1L);
  }

  private static long roundsFor(String name, String mode, String where) {
    return withConf(
        mode,
        () -> {
          Dataset<Row> frame =
              spark.sql(
                  "SELECT id FROM local.db."
                      + name
                      + where
                      + " ORDER BY vec_distance(embedding, array("
                      + query()
                      + "), 'l2') LIMIT 5");
          // the rounds are added by the tasks as they run, so the query has to have run before the
          // metric means anything
          frame.collectAsList();
          return metric(sourceExec(frame), "numCandidateRounds");
        });
  }

  private static List<Long> idsFor(String name, String mode, String where) {
    return withConf(
        mode,
        () ->
            spark
                .sql(
                    "SELECT id FROM local.db."
                        + name
                        + where
                        + " ORDER BY vec_distance(embedding, array("
                        + query()
                        + "), 'l2') LIMIT 5")
                .collectAsList()
                .stream()
                .map(row -> row.getLong(0))
                .collect(Collectors.toList()));
  }

  /** The lowered source in the executed plan, or a failure naming the plan that was built. */
  private static SparkPlan sourceExec(Dataset<Row> frame) {
    SparkPlan executed = frame.queryExecution().executedPlan();
    for (SparkPlan leaf : CollectionConverters.asJava(executed.collectLeaves())) {
      if (leaf.nodeName().contains("VectorSearch")) {
        return leaf;
      }
    }

    throw new AssertionError("No VectorSearch in the plan:\n" + executed);
  }

  private static long metric(SparkPlan plan, String name) {
    SQLMetric metric = CollectionConverters.asJava(plan.metrics()).get(name);
    if (metric == null) {
      throw new AssertionError("No " + name + " in the plan's metrics");
    }

    return metric.value();
  }

  private static <T> T withConf(String mode, java.util.function.Supplier<T> body) {
    spark.conf().set("spark.iceberg.index.vector.filter-mode", mode);
    try {
      return body.get();
    } finally {
      spark.conf().unset("spark.iceberg.index.vector.filter-mode");
    }
  }

  @Test
  void filtersOnADateColumn() throws Exception {
    tableWithIndex("filtered_date");

    List<Row> rows =
        spark
            .sql(
                "SELECT id, day FROM local.db.filtered_date "
                    + "WHERE day > DATE '2021-01-01' ORDER BY vec_distance(embedding, array("
                    + query()
                    + "), 'l2') LIMIT 5")
            .collectAsList();

    assertThat(rows)
        .as("a filter on a date column is judged the way the column means, not the way it is boxed")
        .hasSize(5);
    assertThat(rows)
        .allSatisfy(row -> assertThat(row.getDate(1).toString()).isEqualTo("2024-06-01"));
  }

  @Test
  void neverReturnsADeletedRowWhenTheFilterRunsBeforeTheSearch() throws Exception {
    tableWithIndex("filtered_deleted_pre");
    deleteLastQuarter("filtered_deleted_pre");

    List<Row> rows =
        withConf(
            "pre",
            () ->
                spark
                    .sql(
                        "SELECT id, price FROM local.db.filtered_deleted_pre "
                            + "WHERE price < 40 ORDER BY vec_distance(embedding, array("
                            + query()
                            + "), 'l2') LIMIT 5")
                    .collectAsList());

    assertThat(rows).isNotEmpty();
    assertThat(rows)
        .as("a row whose file still has it, but whose live set does not, is not an answer")
        .allSatisfy(row -> assertThat(row.getLong(0)).isLessThan(KEPT));
  }

  @Test
  void neverReturnsADeletedRowWhenTheFilterRunsAfterTheSearch() throws Exception {
    tableWithIndex("filtered_deleted_post");
    deleteLastQuarter("filtered_deleted_post");

    List<Row> rows =
        withConf(
            "post",
            () ->
                spark
                    .sql(
                        "SELECT id, price FROM local.db.filtered_deleted_post "
                            + "WHERE price < 40 ORDER BY vec_distance(embedding, array("
                            + query()
                            + "), 'l2') LIMIT 5")
                    .collectAsList());

    assertThat(rows)
        .as("the search may still rank a deleted row, and dropping it must not shorten the window")
        .hasSize(5);
    assertThat(rows).allSatisfy(row -> assertThat(row.getLong(0)).isLessThan(KEPT));
  }

  @Test
  void returnsWhatItFoundWhenTheFilterAdmitsFewerRowsThanAsked() throws Exception {
    tableWithIndex("filtered_short");

    for (String mode : new String[] {"pre", "post"}) {
      List<Row> rows =
          withConf(
              mode,
              () ->
                  spark
                      .sql(
                          "SELECT id, price FROM local.db.filtered_short WHERE price = 6 "
                              + "ORDER BY vec_distance(embedding, array("
                              + query()
                              + "), 'l2') LIMIT 5")
                      .collectAsList());

      assertThat(rows)
          .as("%s: one row passes, so one row is the answer, not a padded window", mode)
          .hasSize(1);
      assertThat(rows.get(0).getLong(1)).isEqualTo(6L);
    }
  }

  private static void deleteLastQuarter(String name) {
    // a merge-on-read delete leaves position deletes behind, so the data file still holds the rows
    // the index points at and the live set does not
    spark.sql(
        "ALTER TABLE local.db."
            + name
            + " SET TBLPROPERTIES ('write.delete.mode'='merge-on-read')");
    spark.sql("DELETE FROM local.db." + name + " WHERE id >= " + KEPT);
    // the test only means something if the rows are still in their data files: a copy-on-write
    // delete would remove them physically and the index would have nothing stale to point at
    long positionDeletes =
        spark
            .sql("SELECT count(*) FROM local.db." + name + ".files WHERE content = 1")
            .head()
            .getLong(0);
    assertThat(positionDeletes)
        .as("the table has to carry a position delete for this test to be testing one")
        .isGreaterThan(0L);
  }

  @Test
  void answersAFilteredQueryInOneJob() throws Exception {
    tableWithIndex("filtered_one_job");

    // measured before the fuse: three jobs without a filter, five with the filter read first. The
    // three are the search, the fetch-back and the re-parallelisation of what the fetch-back
    // collected; the two more are the driver-side read of the filter's rows.
    assertThat(jobsFor("filtered_one_job", ""))
        .as("an unfiltered search reads the rows it found in the tasks that found them")
        .isEqualTo(1);
    assertThat(jobsFor("filtered_one_job", " WHERE price < 40"))
        .as("the filter's rows are read where the search happens, not by two more jobs")
        .isEqualTo(1);
    assertThat(jobsFor("filtered_one_job", " WHERE price < 40", "post"))
        .as("filtering after the search costs no extra job either")
        .isEqualTo(1);
  }

  private static int jobsFor(String name, String where) {
    return jobsFor(name, where, null);
  }

  private static int jobsFor(String name, String where, String mode) {
    if (mode == null) {
      spark.conf().unset("spark.iceberg.index.vector.filter-mode");
    } else {
      spark.conf().set("spark.iceberg.index.vector.filter-mode", mode);
    }

    try {
      String select = "post".equals(mode) ? "id, price" : "id";
      return SparkJobCounter.jobsDuring(
          spark,
          () ->
              spark
                  .sql(
                      "SELECT "
                          + select
                          + " FROM local.db."
                          + name
                          + where
                          + " ORDER BY vec_distance(embedding, array("
                          + query()
                          + "), 'l2') LIMIT 5")
                  .collectAsList());
    } finally {
      spark.conf().unset("spark.iceberg.index.vector.filter-mode");
    }
  }

  private static String query() {
    StringBuilder builder = new StringBuilder();
    for (int d = 0; d < DIMENSION; d++) {
      if (d > 0) {
        builder.append(", ");
      }

      builder.append(0.1d * (d + 1));
    }

    return builder.toString();
  }

  private static Table tableWithIndex(String name) throws Exception {
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
    spark.sql("DROP TABLE IF EXISTS local.db." + name);
    spark.sql(
        "CREATE TABLE local.db."
            + name
            + " (id bigint, price bigint, day date, embedding array<double> NOT NULL) USING iceberg");

    StringBuilder values = new StringBuilder();
    for (int row = 0; row < ROWS; row++) {
      if (row > 0) {
        values.append(", ");
      }

      values
          .append('(')
          .append(row)
          .append(", ")
          // every third row is cheap, so a price filter keeps a third of the table
          .append(row % 3 == 0 ? row : 100 + row)
          .append(", DATE '")
          .append(row % 3 == 0 ? "2020-01-01" : "2024-06-01")
          .append("', array(");
      for (int d = 0; d < DIMENSION; d++) {
        if (d > 0) {
          values.append(", ");
        }

        values.append(0.01d * (row * DIMENSION + d) + 0.001d);
      }

      values.append("))");
    }

    spark.sql("INSERT INTO local.db." + name + " VALUES " + values);

    spark.sql(
        "CALL local.system.build_vector_index(table => 'db."
            + name
            + "', column => 'embedding', dimension => "
            + DIMENSION
            + ", metric => 'l2', structure => 'ivf')");
    return Spark3Util.loadIcebergTable(spark, "local.db." + name);
  }
}
