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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.IndexQuery;
import org.apache.iceberg.index.IndexReader;
import org.apache.iceberg.index.text.TextIndexAdapter;
import org.apache.iceberg.index.text.TextIndexDefinition;
import org.apache.iceberg.io.InputFile;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestPluginExecutorBudget {
  private static final long MB = 1024L * 1024L;
  @TempDir Path warehouse;

  @Test
  void textResidencyUsesTheSameScoresWithoutOpeningShards() {
    SparkSession spark = start(null);
    String name = "local.db.cached_text";
    ExecutorIndexCache cache = ExecutorIndexCache.get();
    try {
      spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
      spark.sql("CREATE TABLE " + name + " (id bigint, body string) USING iceberg");
      spark.sql("INSERT INTO " + name + " VALUES (1, 'ice ice glacier'), (2, 'ice')");
      spark.sql("INSERT INTO " + name + " VALUES (3, 'glacier water')");
      String uuid =
          spark
              .sql(
                  "CALL local.system.build_text_index(table=>'db.cached_text', column=>'body',"
                      + " max_entries_per_index_file=>1L)")
              .head()
              .getString(0);
      Configuration conf = spark.sessionState().newHadoopConf();
      Table table =
          new HadoopTables(conf).load(warehouse.resolve("db/cached_text").toUri().toString());
      IndexMetadata metadata = VectorIndexes.indexCatalog(table).loadIndex(table.location(), uuid);
      TextIndexAdapter adapter = new TextIndexAdapter((TextIndexDefinition) metadata.definition());
      IndexQuery query =
          IndexQuery.of(
              table.currentSnapshot().snapshotId(), adapter.encodeQuery("ice glacier ice"), 3);
      ShardCountingFileIO io = new ShardCountingFileIO(conf);
      Table observed =
          new BaseTable(
              new StaticTableOperations(((HasTableOperations) table).operations().current(), io),
              table.name());
      ShardCountingFileIO.OPENS.set(0);
      var cold = DistributedIndexSearch.searchBatch(spark, metadata, observed, List.of(query));
      assertThat(cold.get(0)).hasSize(3);
      assertThat(ShardCountingFileIO.OPENS.get()).isPositive();
      assertThat(cache.stats().entries()).isZero();

      var shards = new IndexReader(metadata, table).indexFiles(query);
      assertThat(shards)
          .hasSizeGreaterThan(1)
          .allSatisfy(
              file -> {
                assertThat(file.memory()).isNotNull();
                assertThat(file.memory().preparedBytes())
                    .isGreaterThan(file.memory().payloadBytes());
                assertThat(file.memory().fileBytes()).isGreaterThan(file.memory().payloadBytes());
              });
      long payloadBytes = shards.stream().mapToLong(file -> file.memory().payloadBytes()).sum();
      long fileBytes = shards.stream().mapToLong(file -> file.memory().fileBytes()).sum();
      System.out.printf(
          "BM25 residency sample: %d payload bytes / %d Puffin bytes%n", payloadBytes, fileBytes);

      Row load =
          spark
              .sql("CALL local.system.load_index(table=>'" + name + "', index=>'" + uuid + "')")
              .head();
      assertThat(load.getString(0)).isEqualTo("COMPLETE");
      assertThat(cache.stats().entries()).isEqualTo(shards.size());
      ShardCountingFileIO.OPENS.set(0);
      var warm = DistributedIndexSearch.searchBatch(spark, metadata, observed, List.of(query));
      assertThat(warm).isEqualTo(cold);
      assertThat(ShardCountingFileIO.OPENS.get()).isZero();

      long textCharge = cache.stats().bytes();
      assertThat(textCharge)
          .isEqualTo(
              DistributedIndexSearch.workingSetBytes(
                  table.io(), shards, adapter, table.location()));

      spark.sql(
          "CREATE TABLE local.db.cached_vectors (id bigint, embedding array<double>) USING"
              + " iceberg");
      spark.sql("INSERT INTO local.db.cached_vectors VALUES (1, array(1.0d, 0.0d))");
      String vectorUuid =
          spark
              .sql(
                  "CALL local.system.build_vector_index(table=>'db.cached_vectors',"
                      + " column=>'embedding', dimension=>2, structure=>'FLAT',"
                      + " metric=>'L2')")
              .head()
              .getString(0);
      assertThat(
              spark
                  .sql(
                      "CALL local.system.load_index(table=>'local.db.cached_vectors', index=>'"
                          + vectorUuid
                          + "')")
                  .head()
                  .getString(0))
          .isEqualTo("COMPLETE");
      long combinedCharge = cache.stats().bytes();
      assertThat(combinedCharge).isGreaterThan(textCharge);
      assertThat(cache.stats().entries()).isEqualTo(shards.size() + 1);
      List<Row> status =
          spark.sql("CALL local.system.index_cache_status(refresh=>true)").collectAsList();
      assertThat(status)
          .anySatisfy(
              row -> {
                assertThat(row.getLong(4)).isEqualTo(shards.size() + 1);
                assertThat(row.getLong(5)).isEqualTo(combinedCharge);
              });

      assertThat(
              spark
                  .sql(
                      "CALL local.system.offload_index(table=>'"
                          + name
                          + "', index=>'"
                          + uuid
                          + "')")
                  .head()
                  .getString(0))
          .isEqualTo("OFFLOADED");
      assertThat(cache.stats().entries()).isEqualTo(1);
      long vectorCharge = cache.stats().bytes();
      ShardCountingFileIO.OPENS.set(0);
      assertThat(DistributedIndexSearch.searchBatch(spark, metadata, observed, List.of(query)))
          .isEqualTo(cold);
      assertThat(ShardCountingFileIO.OPENS.get()).isPositive();
      assertThat(cache.stats().bytes()).isEqualTo(vectorCharge);
    } finally {
      cache.clear();
      spark.stop();
    }
  }

  private static final class ShardCountingFileIO extends HadoopFileIO {
    private static final AtomicInteger OPENS = new AtomicInteger();

    private ShardCountingFileIO(Configuration conf) {
      super(conf);
    }

    @Override
    public InputFile newInputFile(String path) {
      if (path.contains("/data/index-file-")) {
        OPENS.incrementAndGet();
      }
      return super.newInputFile(path);
    }
  }

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
                  "CALL local.system.build_vector_index(table=>'db.plugin_budget_vectors', column=>'embedding', dimension=>2, structure=>'FLAT', metric=>'L2')")
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
  void aLoadOfManyIndexFilesCompletes() {
    SparkSession spark = start(null);
    String table = "local.db.plugin_many_files";
    ExecutorIndexCache cache = ExecutorIndexCache.get();
    try {
      spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
      spark.sql("CREATE TABLE " + table + " (id bigint, embedding array<double>) USING iceberg");
      // one data file per insert, and one index file per data file, so the load plans many files
      for (int i = 0; i < 12; i += 1) {
        spark.sql(
            "INSERT INTO "
                + table
                + " VALUES ("
                + i
                + ", array("
                + i
                + ".0d, "
                + (12 - i)
                + ".0d))");
      }
      String index =
          spark
              .sql(
                  "CALL local.system.build_vector_index(table=>'db.plugin_many_files', "
                      + "column=>'embedding', dimension=>2, structure=>'FLAT', metric=>'L2', "
                      + "max_entries_per_index_file=>1)")
              .head()
              .getString(0);
      String status =
          spark
              .sql(
                  "CALL local.system.load_index(table=>'local.db.plugin_many_files', index=>'"
                      + index
                      + "')")
              .head()
              .getString(0);
      assertThat(status).as("a load of every index file completes").isEqualTo("COMPLETE");
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
                      + " column=>'embedding', dimension=>2, structure=>'FLAT', metric=>'L2')")
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
