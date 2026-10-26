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

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import org.apache.iceberg.Table;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.IndexQuery;
import org.apache.iceberg.index.IndexReader;
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
 * What a cache budget derived from the index itself covers.
 *
 * <p>The budget used to be derived from the size of the files on disk, which is smaller than what
 * they cost once prepared, by a factor that differs per structure: a quantized list is a few bytes
 * per entry while its centres and factors are not, and a graph's connection lists cost nearly as
 * much as the vectors they connect. A derived budget therefore held only part of the files a query
 * reads, and the cache silently read the rest again.
 *
 * <p>What this pins is the end-to-end shape: with no budget configured, the derived one holds every
 * file the query reads. It cannot on its own tell a good estimate from a bad one, because a small
 * fixture's derived budget is dominated by the configured floor rather than by the working set, so
 * coverage is 1.0 whatever the estimate says. The arithmetic behind that budget is pinned by {@code
 * TestIndexCacheWorkingSetCharge}, which fails when the number the budget is derived from stops
 * being the number admission charges.
 */
class TestIndexCacheCoverageUnderDerivedBudget {
  private static final String CACHE_ENABLED = "spark.iceberg.index.cache.enabled";
  private static SparkSession spark;

  @BeforeAll
  static void startSpark(@TempDir Path warehouse) {
    VectorIndexTypes.register();
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("index-cache-coverage")
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
  void aDerivedBudgetHoldsEveryFileTheQueryReads() throws Exception {
    String table = "coverage_derived";
    createIndexedTable(table);

    spark.conf().set(CACHE_ENABLED, "true");
    // the cache lives in this JVM and other fixtures have filled it, so the count below only means
    // something once it starts empty
    org.apache.iceberg.index.spark.ExecutorIndexCacheProbe.clear();
    try {
      // no budget is configured, so the cache derives one from what the files cost prepared
      search(table);

      List<Row> status = spark.sql("CALL local.system.index_cache_status()").collectAsList();
      Row executor = reportingRow(status);
      long entries = executor.getLong(4);
      int files = indexFileCount(table);
      assertThat(files).as("the query reads more than one index file").isGreaterThan(1);
      assertThat(entries)
          .as("the cache holds every file the query read: %s", status)
          .isEqualTo(files);
      assertThat(executor.getDouble(10))
          .as("a derived budget covers every file: %s", status)
          .isEqualTo(1.0);
    } finally {
      spark.conf().unset(CACHE_ENABLED);
    }
  }

  /** The row that reports the executor's own cache, which is the one that has entries. */
  private static Row reportingRow(List<Row> status) {
    return status.stream()
        .filter(row -> row.getLong(4) > 0L)
        .findFirst()
        .orElseThrow(() -> new AssertionError("no executor reported a populated cache: " + status));
  }

  private static void search(String table) {
    // the same query twice: the first read is what fills the cache and reports coverage
    spark.sql(
        "CALL local.system.vector_search(table => 'db."
            + table
            + "', column => 'embedding', "
            + "query => array(0.0d, 10.0d), k => 3)");
  }

  private static final int FILE_COUNT = 8;

  /** How many index files the snapshot a query reads is made of. */
  private static int indexFileCount(String name) throws Exception {
    Table table = Spark3Util.loadIcebergTable(spark, "local.db." + name);
    IndexMetadata metadata = VectorIndexes.findIndex(table, "embedding", "vector");
    return new IndexReader(metadata, table)
        .indexFiles(
            IndexQuery.of(
                metadata.latestSnapshot().sourceTableSnapshotId(), ByteBuffer.allocate(0), 0))
        .size();
  }

  private static void createIndexedTable(String name) throws Exception {
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
    spark.sql("DROP TABLE IF EXISTS local.db." + name);
    spark.sql(
        "CREATE TABLE local.db." + name + " (id bigint, embedding array<double>) USING iceberg");
    for (int file = 0; file < FILE_COUNT; file++) {
      spark.sql(
          "INSERT INTO local.db."
              + name
              + " VALUES ("
              + file
              + ", array(CAST("
              + file
              + " AS double), CAST("
              + (FILE_COUNT - file)
              + " AS double)))");
    }
    spark.sql(
        "CALL local.system.build_vector_index(table => 'db."
            + name
            + "', column => 'embedding', dimension => 2, metric => 'euclidean', "
            + "structure => 'IVF', quantizer => 'RABITQ', options => map('nlist', '2', "
            + "'nprobe', '2'), max_entries_per_index_file => 2)");
  }
}
