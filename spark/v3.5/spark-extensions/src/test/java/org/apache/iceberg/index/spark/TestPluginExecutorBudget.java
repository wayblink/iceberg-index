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
package org.apache.iceberg.index.spark;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestPluginExecutorBudget {
  private static final long MB = 1024L * 1024L;
  @TempDir Path warehouse;

  @Test
  void realPluginLoadOffloadAndReloadKeepQueryResults() {
    SparkSession spark = start(null);
    String table = "local.db.plugin_budget_vectors";
    ExecutorIndexCache cache = ExecutorIndexCache.get();
    try {
      spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
      spark.sql("CREATE TABLE " + table + " (id bigint, embedding array<double>) USING iceberg");
      spark.sql("INSERT INTO " + table + " VALUES (1, array(1.0d, 0.0d)), (2, array(0.0d, 1.0d))");
      String index =
          spark
              .sql(
                  "CALL local.system.build_vector_index(table=>'db.plugin_budget_vectors', column=>'embedding', dimension=>2, structure=>'FLAT', metric=>'EUCLIDEAN')")
              .head()
              .getString(0);
      String query =
          "CALL local.system.vector_search(table=>'db.plugin_budget_vectors', column=>'embedding', query=>array(1.0d,0.0d), k=>2)";
      List<Row> expected = spark.sql(query).collectAsList();
      assertThat(cache.stats().bytes()).isPositive();
      String load =
          "CALL local.system.load_index(table=>'local.db.plugin_budget_vectors', index=>'"
              + index
              + "')";
      assertThat(spark.sql(load).head().getString(0)).isEqualTo("COMPLETE");
      assertThat(spark.sql(query).collectAsList()).containsExactlyElementsOf(expected);
      assertThat(IndexCacheControl.status(spark))
          .isNotEmpty()
          .allSatisfy(
              status -> {
                assertThat(status.budgetBytes()).isEqualTo(512 * MB);
                assertThat(status.usedBytes() + status.reservedBytes())
                    .isLessThanOrEqualTo(status.budgetBytes());
              });
      assertThat(
              spark
                  .sql(
                      "CALL local.system.offload_index(table=>'local.db.plugin_budget_vectors', index=>'"
                          + index
                          + "')")
                  .head()
                  .getString(0))
          .isEqualTo("OFFLOADED");
      assertThat(spark.sql(query).collectAsList()).containsExactlyElementsOf(expected);
      assertThat(spark.sql(load).head().getString(0)).isEqualTo("COMPLETE");
      assertThat(spark.sql(query).collectAsList()).containsExactlyElementsOf(expected);
    } finally {
      try {
        spark.sql("DROP TABLE IF EXISTS " + table + " PURGE");
      } finally {
        cache.clear();
        spark.stop();
      }
    }
  }

  @Test
  void lazyQueryCannotExceedTheAdvertisedAutomaticBudget() {
    SparkSession spark = start(null);
    ExecutorIndexCache cache = ExecutorIndexCache.get();
    try {
      long advertised = ExecutorIndexCache.Config.advertisedBudget("true", null, "7g", null);
      ExecutorIndexCache.Config query = ExecutorIndexCache.Config.from(spark, 600 * MB);
      cache.file("larger-than-advertised", query, "test", () -> charged(600 * MB));
      assertThat(cache.stats().bytes()).isLessThanOrEqualTo(advertised);
      assertThat(cache.residentKeys()).isEmpty();
    } finally {
      cache.clear();
      spark.stop();
    }
  }

  @Test
  void clearingEntriesDoesNotRemoveTheExecutorBudget() {
    SparkSession spark = start(null);
    ExecutorIndexCache cache = ExecutorIndexCache.get();
    try {
      cache.file(
          "small", ExecutorIndexCache.Config.from(spark, 600 * MB), "test", () -> charged(MB));
      assertThat(cache.stats().entries()).isEqualTo(1);
      cache.clear();
      cache.file(
          "too-large-after-clear",
          ExecutorIndexCache.Config.from(spark, 600 * MB),
          "test",
          () -> charged(600 * MB));
      assertThat(cache.residentKeys()).isEmpty();
    } finally {
      cache.clear();
      spark.stop();
    }
  }

  @Test
  void anExplicitZeroBudgetCannotBeOverriddenByAQuery() {
    SparkSession spark = start("0");
    ExecutorIndexCache cache = ExecutorIndexCache.get();
    try {
      spark.conf().set(ExecutorIndexCache.Config.BYTES, "1g");
      cache.file(
          "zero-budget", ExecutorIndexCache.Config.from(spark, MB), "test", () -> charged(MB));
      assertThat(cache.residentKeys()).isEmpty();
    } finally {
      cache.clear();
      spark.stop();
    }
  }

  @Test
  void nonpositiveBudgetsDisableQueryCaching() {
    for (String configured : new String[] {"0", "-1"}) {
      SparkSession spark = start(configured);
      ExecutorIndexCache cache = ExecutorIndexCache.get();
      try {
        ExecutorIndexCache.Config query = ExecutorIndexCache.Config.from(spark, MB);
        assertThat(query.enabled()).isFalse();
        cache.file("disabled", query, "test", () -> charged(MB));
        assertThat(cache.residentKeys()).isEmpty();
      } finally {
        cache.clear();
        spark.stop();
      }
    }
  }

  @Test
  void blankBudgetMeansAutomaticOnBothPaths() {
    SparkSession spark = start(" ");
    ExecutorIndexCache cache = ExecutorIndexCache.get();
    try {
      ExecutorIndexCache.Config query = ExecutorIndexCache.Config.from(spark, 0);
      assertThat(query.maxBytes())
          .isEqualTo(ExecutorIndexCache.Config.advertisedBudget("true", " ", "7g", null));
    } finally {
      cache.clear();
      spark.stop();
    }
  }

  @Test
  void nodeReportIncludesLazyResidentBytes() {
    SparkSession spark = start(null);
    ExecutorIndexCache cache = ExecutorIndexCache.get();
    try {
      long budget = ExecutorIndexCache.Config.advertisedBudget("true", null, "7g", null);
      cache.file("lazy", ExecutorIndexCache.Config.from(spark, MB), "test", () -> charged(MB));
      ExecutorIndexCacheWorker observer =
          new ExecutorIndexCacheWorker(
              "observer",
              "executor",
              "incarnation",
              cache,
              ExecutorIndexCache.Config.of(true, budget),
              command -> charged(MB),
              IndexCacheWorkerConfig.parse(Map.of()));
      assertThat(observer.report("test", 1, budget, 0, Map.of()).usedBytes()).isEqualTo(MB);
    } finally {
      cache.clear();
      spark.stop();
    }
  }

  @Test
  void aCacheThatIsOffRefusesTheLoadItsQueriesCannotUse() {
    // The switch is read by both sides or by neither: a run that leaves the cache off must not
    // advertise a budget, must not answer a load with COMPLETE, and must not fill an executor with
    // files its own queries will never look at.
    SparkSession spark = start(null, "false");
    String table = "local.db.plugin_disabled_vectors";
    ExecutorIndexCache cache = ExecutorIndexCache.get();
    try {
      spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
      spark.sql("CREATE TABLE " + table + " (id bigint, embedding array<double>) USING iceberg");
      spark.sql("INSERT INTO " + table + " VALUES (1, array(1.0d, 0.0d)), (2, array(0.0d, 1.0d))");
      String index =
          spark
              .sql(
                  "CALL local.system.build_vector_index(table=>'db.plugin_disabled_vectors',"
                      + " column=>'embedding', dimension=>2, structure=>'FLAT', metric=>'EUCLIDEAN')")
              .head()
              .getString(0);

      assertThat(awaitReports(spark))
          .as("every executor reports the budget a query would see, which is none")
          .isNotEmpty()
          .allSatisfy(report -> assertThat(report.budgetBytes()).isZero());

      List<Row> load =
          spark
              .sql("CALL local.system.load_index(table=>'" + table + "', index=>'" + index + "')")
              .collectAsList();

      assertThat(load.get(0).getString(0))
          .as("a cache that is off cannot answer a load")
          .isEqualTo("FAILED");
      List<Row> files = load.subList(1, load.size());
      assertThat(files).as("a load with nothing planned still names the files").isNotEmpty();
      files.forEach(row -> assertThat(row.getString(10)).isEqualTo("CACHE_DISABLED"));
      assertThat(cache.stats().entries()).isZero();
    } finally {
      try {
        spark.sql("DROP TABLE IF EXISTS " + table + " PURGE");
      } finally {
        cache.clear();
        spark.stop();
      }
    }
  }

  /** Waits for a report from the executors, which is what a plan is placed from. */
  private static List<IndexCacheControl.Status> awaitReports(SparkSession spark) {
    List<IndexCacheControl.Status> reports = IndexCacheControl.status(spark);
    for (int attempt = 0; attempt < 300 && reports.isEmpty(); attempt += 1) {
      try {
        Thread.sleep(200L);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }

      reports = IndexCacheControl.status(spark);
    }

    return reports;
  }

  private SparkSession start(String configured) {
    return start(configured, "true");
  }

  private SparkSession start(String configured, String enabled) {
    ExecutorIndexCache.get().clear();
    SparkSession.Builder builder =
        SparkSession.builder()
            .master("local[2]")
            .appName("plugin-executor-budget")
            // Local executors initialize before Spark publishes its generated application ID.
            .config("spark.app.id", "plugin-executor-budget-test")
            .config("spark.ui.enabled", "false")
            .config("spark.executor.memory", "7g")
            .config("spark.plugins", IndexCachePlugin.class.getName())
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", warehouse.toUri().toString());
    if (enabled != null) {
      builder.config(ExecutorIndexCache.Config.ENABLED, enabled);
    }

    if (configured != null) {
      builder.config(ExecutorIndexCache.Config.BYTES, configured);
    }
    return builder.getOrCreate();
  }

  private static ExecutorIndexCache.CachedFile charged(long bytes) {
    // Exercise the real retained-state accounting without allocating a large payload buffer.
    return ExecutorIndexCache.loaded(new byte[] {1}, null, bytes);
  }
}
