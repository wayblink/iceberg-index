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
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.index.vector.VectorIndexTypes;
import org.apache.iceberg.io.CloseableIterable;
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
 * A single query over more data files than the per-file search is worth running is answered by the
 * index's own global hits: one driver-side window over every index file, fetched back as an RDD.
 *
 * <p>These are the SQL-level counterparts of the entry point's own tests. The rewrite only takes
 * that branch when the table has more than 64 data files, so the fixture writes 65
 * one-row-at-a-time appends; the query is one query, with a postfilter and without the exact
 * re-ranking, which is the other way into the branch.
 */
class TestVectorSearchGlobalFetch {

  private static final int FILES = 65;
  private static final int ROWS_PER_FILE = 10;
  private static final int ROWS = FILES * ROWS_PER_FILE;
  private static final int BODY = 4096;
  private static final String SOURCE = "VectorSearchSource";
  private static final String COLUMNS =
      "(id bigint, body string, embedding array<double> NOT NULL) USING iceberg";

  private static SparkSession spark;

  @BeforeAll
  static void startSpark(@TempDir Path warehouse) {
    VectorIndexTypes.register();
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("vector-search-global-fetch")
            .config("spark.ui.enabled", "false")
            // a fetch-back that collects its window on the driver has to fit every fetched row's
            // whole projection into one driver result; this is the smallest size it can be told
            .config("spark.driver.maxResultSize", "1m")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.adaptive.enabled", "false")
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
      spark = null;
    }
  }

  /**
   * A postfilter that clears the first ten doublings of a k=1 window still sees the hit past them.
   *
   * <p>The rows are ordered by distance as their id, and the filter admits only the farthest one,
   * so the search has to widen to the whole index to answer. The tenth doubling of a window that
   * starts at 1 reaches 512; a search that stops there returns nothing even though the index still
   * holds the row.
   */
  @Test
  void aPostfilterOverManyFilesReachesTheHitPastTheOldRoundCap() throws Exception {
    String table = tableWithIndex("global_past_cap");
    // the default link reads the filter's rows first, which narrows the search instead of leaving
    // it with a window to widen; the global branch is the one that searches first
    String parameter = "spark.iceberg.index.vector.filter-mode";
    spark.conf().set(parameter, "post");
    try {
      Dataset<Row> frame =
          spark.sql(
              String.format(
                  "SELECT id FROM local.db.%s WHERE id = %d "
                      + "ORDER BY vec_distance(embedding, array(1.0d, 0.0d), 'l2') LIMIT 1",
                  table, ROWS - 1));
      List<Row> rows = frame.collectAsList();

      assertThat(rows).hasSize(1);
      assertThat(rows.get(0).getLong(0)).isEqualTo(ROWS - 1L);
      SparkPlan source = sourceExec(frame);
      assertThat(metric(source, "numCandidateRounds"))
          .as("the widening has to pass the tenth doubling of a window that starts at one")
          .isGreaterThan(10L);
      assertThat(metric(source, "numFetchedRows"))
          .as("every round's candidates were read before the admitted row was reached")
          .isGreaterThanOrEqualTo((long) ROWS);
    } finally {
      spark.conf().unset(parameter);
    }
  }

  /**
   * The rows a wide window fetches stay on the executors instead of being collected to fill a
   * driver result.
   *
   * <p>The window is asked for with a refine factor, so the first round fetches 400 whole rows,
   * each carrying the 4 KiB body; collected on the driver that is more than the one-megabyte
   * maximum this session allows a driver result. The answer itself is one row, which is what the
   * final sort keeps.
   */
  @Test
  void aWideFetchBackOverManyFilesStaysOffTheDriver() throws Exception {
    String table = tableWithIndex("global_wide_projection");
    String parameter = "spark.iceberg.index.vector.refine-factor";
    spark.conf().set(parameter, "400");
    try {
      Dataset<Row> frame =
          spark.sql(
              String.format(
                  "SELECT id, body FROM local.db.%s "
                      + "ORDER BY vec_distance(embedding, array(1.0d, 0.0d), 'l2') LIMIT 1",
                  table));
      List<Row> rows = frame.collectAsList();

      assertThat(rows).hasSize(1);
      assertThat(rows.get(0).getLong(0)).isEqualTo(0L);
      assertThat(rows.get(0).getString(1)).hasSize(BODY);
      assertThat(metric(sourceExec(frame), "numFetchedRows"))
          .as("the first round fetched the whole window it was asked for")
          .isEqualTo(400L);
    } finally {
      spark.conf().unset(parameter);
    }
  }

  /**
   * A table of 65 data files of {@link #ROWS_PER_FILE} rows, and a flat vector index over it.
   *
   * <p>The id of a row is its position in the distance order: the row nearest the query is id 0 and
   * the farthest is id 649. Every row carries a body of {@link #BODY} bytes, so a fetch-back that
   * returns the whole row returns a wide one.
   */
  private static String tableWithIndex(String name) throws Exception {
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
    spark.sql("DROP TABLE IF EXISTS local.db." + name);
    spark.sql("CREATE TABLE local.db." + name + " " + COLUMNS);
    for (int file = 0; file < FILES; file++) {
      int start = file * ROWS_PER_FILE;
      spark.sql(
          String.format(
              "INSERT INTO local.db.%s "
                  + "SELECT id, repeat('x', %d), array(cast(id + 1 as double), 0.0d) "
                  + "FROM range(%d, %d)",
              name, BODY, start, start + ROWS_PER_FILE));
    }

    spark.sql(
        String.format(
            "CALL local.system.build_vector_index(table => 'db.%s', column => 'embedding', "
                + "dimension => 2, metric => 'l2', structure => 'flat')",
            name));

    assertThat(dataFileCount(name))
        .as("the global-hits branch needs more than 64 data files")
        .isGreaterThan(64L);
    return name;
  }

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
      throw new AssertionError("No " + name + " in the plan's metrics");
    }

    return metric.value();
  }
}
