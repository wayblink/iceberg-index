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
package org.apache.iceberg.spark.index;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.nio.file.Path;
import java.util.List;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.index.CreateIndexRequest;
import org.apache.iceberg.index.IndexCatalog;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.IndexQuery;
import org.apache.iceberg.index.SidecarIndexCatalog;
import org.apache.iceberg.index.spark.DistributedIndexSearch;
import org.apache.iceberg.index.spark.IndexRowFilter;
import org.apache.iceberg.index.spark.SparkIndexBuilder;
import org.apache.iceberg.index.vector.VectorIndexAdapter;
import org.apache.iceberg.index.vector.VectorIndexDefinition;
import org.apache.iceberg.index.vector.VectorIndexDefinition.Metric;
import org.apache.iceberg.index.vector.VectorIndexTypes;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * How the global-hits entry point fetches the rows of its hits.
 *
 * <p>Its window is widened while the rounds of a short answer run out; it must keep going until the
 * index has no more candidates, and it must read the rows where they live rather than collect them
 * on the driver.
 */
class TestDistributedIndexSearchRowsFromHits {

  private static SparkSession spark;
  private static Path warehouseDir;

  @BeforeAll
  static void startSpark(@TempDir Path warehouse) {
    VectorIndexTypes.register();
    warehouseDir = warehouse;
    spark =
        SparkSession.builder()
            .master("local[4]")
            .appName("distributed-index-rows-from-hits")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.host", InetAddress.getLoopbackAddress().getHostAddress())
            .config("spark.driver.maxResultSize", "1m")
            .config("spark.sql.shuffle.partitions", "4")
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

  @Test
  void returnsAHitTheDoublingWindowNeverReaches() {
    // ids 0 to 511 are the 512 nearest rows and the filter turns every one of them down; id 512 is
    // the 513th hit, the first hit of the second index file, so a window that stops at 512 never
    // sees it even though the index holds more hits
    spark.sql(
        "CREATE TABLE local.db.window_rows (id bigint, embedding array<double>) USING iceberg");
    spark.sql(
        "INSERT INTO local.db.window_rows "
            + "SELECT id, array(cast(id + 1 as double), cast(0 as double)) FROM range(512)");
    spark.sql(
        "INSERT INTO local.db.window_rows VALUES (512, array(cast(513 as double), cast(0 as double)))");

    Table table = loadTable("window_rows");
    long snapshotId = table.currentSnapshot().snapshotId();
    IndexMetadata metadata = buildVectorIndex(table, snapshotId, "window-vectors", 2);

    List<Row> collected;
    try (DistributedIndexSearch.SearchRows rows =
        DistributedIndexSearch.searchRowsFromHits(
            spark,
            metadata,
            table,
            query(metadata, snapshotId, 1),
            table.schema().select("id", "embedding"),
            spark.table("local.db.window_rows").schema(),
            2,
            1,
            1,
            scanTasks(table),
            new IndexRowFilter(table.schema().select("id"), Expressions.equal("id", 512L)),
            null,
            List.of("id"),
            null,
            null)) {
      collected = rows.rows().collect();
    }

    assertThat(collected).as("the only row the filter admits is the 513th hit").hasSize(1);
    assertThat(collected.get(0).getLong(0)).isEqualTo(512L);
  }

  /**
   * The hit-only path is what a SQL rewrite uses when the table has many data files; it has to
   * report the stages the other entries report, or the split its metrics promise stays zero.
   */
  @Test
  void reportsTheStagesOfTheHitOnlySearch() {
    spark.sql(
        "CREATE TABLE local.db.stage_rows (id bigint, embedding array<double>) USING iceberg");
    spark.sql(
        "INSERT INTO local.db.stage_rows "
            + "SELECT id, array(cast(id + 1 as double), cast(0 as double)) FROM range(64)");

    Table table = loadTable("stage_rows");
    long snapshotId = table.currentSnapshot().snapshotId();
    IndexMetadata metadata = buildVectorIndex(table, snapshotId, "stage-vectors", 2);
    DistributedIndexSearch.SearchStages stages =
        DistributedIndexSearch.SearchStages.create(spark.sparkContext());

    try (DistributedIndexSearch.SearchRows rows =
        DistributedIndexSearch.searchRowsFromHits(
            spark,
            metadata,
            table,
            query(metadata, snapshotId, 1),
            table.schema().select("id", "embedding"),
            spark.table("local.db.stage_rows").schema(),
            2,
            1,
            1,
            scanTasks(table),
            null,
            null,
            List.of("id"),
            null,
            stages)) {
      assertThat(rows.rows().collect()).isNotEmpty();
    }

    assertThat(stages.value(DistributedIndexSearch.SearchStages.PROBE_SCORE_TIME))
        .as("the hit-only path reports what its tasks spent instead of leaving zeros")
        .isPositive();
    assertThat(stages.value(DistributedIndexSearch.SearchStages.TASK_TIME)).isPositive();
  }

  @Test
  void readsTheFetchedRowsOnTheExecutorsRatherThanCollectingThem() {
    // the projection of a hit is a whole row, wide columns included; a fetch-back that collects the
    // window on the driver has to fit all of it into one driver result. Here every row's body is
    // large enough that the window the widening reaches is far past spark.driver.maxResultSize,
    // which is set to its one-megabyte minimum for this session.
    spark.sql(
        "CREATE TABLE local.db.wide_rows (id bigint, body string, embedding array<double>) "
            + "USING iceberg");
    spark.sql(
        "INSERT INTO local.db.wide_rows "
            + "SELECT id, repeat('x', 4096), array(cast(id + 1 as double), cast(0 as double)) "
            + "FROM range(400)");

    Table table = loadTable("wide_rows");
    long snapshotId = table.currentSnapshot().snapshotId();
    IndexMetadata metadata = buildVectorIndex(table, snapshotId, "wide-vectors", 3);

    // the filter admits only the 400th nearest row, so the search has to widen to the whole index
    List<Row> collected;
    try (DistributedIndexSearch.SearchRows rows =
        DistributedIndexSearch.searchRowsFromHits(
            spark,
            metadata,
            table,
            query(metadata, snapshotId, 1),
            table.schema().select("id", "body", "embedding"),
            spark.table("local.db.wide_rows").schema(),
            3,
            1,
            1,
            scanTasks(table),
            new IndexRowFilter(table.schema().select("id"), Expressions.equal("id", 399L)),
            null,
            List.of("id"),
            null,
            null)) {
      collected = rows.rows().collect();
    }

    assertThat(collected).hasSize(1);
    assertThat(collected.get(0).getLong(0)).isEqualTo(399L);
    assertThat(collected.get(0).getString(1)).hasSize(4096);
  }

  /**
   * The rows the answer is read from are kept on the executors until the answer is released.
   *
   * <p>The round that fills the answer reads its rows once, keeps them where they were read and
   * hands the kept copy on; a result that outlives the call has to carry what releases that copy,
   * or every query leaves a window of rows in the executors for a collection to reclaim.
   */
  @Test
  void keepsTheRowsItReadUntilTheAnswerIsReleased() {
    spark.sql(
        "CREATE TABLE local.db.released_rows (id bigint, embedding array<double>) USING iceberg");
    spark.sql(
        "INSERT INTO local.db.released_rows "
            + "SELECT id, array(cast(id + 1 as double), cast(0 as double)) FROM range(8)");

    Table table = loadTable("released_rows");
    long snapshotId = table.currentSnapshot().snapshotId();
    IndexMetadata metadata = buildVectorIndex(table, snapshotId, "released-vectors", 2);
    int before = spark.sparkContext().getPersistentRDDs().size();

    DistributedIndexSearch.SearchRows rows =
        DistributedIndexSearch.searchRowsFromHits(
            spark,
            metadata,
            table,
            query(metadata, snapshotId, 2),
            table.schema().select("id", "embedding"),
            spark.table("local.db.released_rows").schema(),
            2,
            2,
            2,
            scanTasks(table),
            null,
            null,
            List.of("id"),
            null,
            null);

    int kept = spark.sparkContext().getPersistentRDDs().size();
    assertThat(kept)
        .as("the rows the answer is read from are kept until the answer is released")
        .isEqualTo(before + 1);
    assertThat(rows.rows().collect()).hasSize(2);

    rows.close();
    rows.close();
    int released = spark.sparkContext().getPersistentRDDs().size();
    assertThat(released)
        .as("releasing the answer releases the rows it kept, once")
        .isEqualTo(before);
  }

  /**
   * A fetch-back that fails keeps nothing: the rows it read are released before the failure leaves
   * the call.
   */
  @Test
  void releasesTheRowsItReadWhenTheFetchFails() {
    spark.sql(
        "CREATE TABLE local.db.failed_rows (id bigint, embedding array<double>) USING iceberg");
    spark.sql(
        "INSERT INTO local.db.failed_rows "
            + "SELECT id, array(cast(id + 1 as double), cast(0 as double)) FROM range(8)");

    Table table = loadTable("failed_rows");
    long snapshotId = table.currentSnapshot().snapshotId();
    IndexMetadata metadata = buildVectorIndex(table, snapshotId, "failed-vectors", 2);
    List<FileScanTask> tasks = scanTasks(table);
    for (FileScanTask task : tasks) {
      // the index still names these files, so the fetch-back of its hits fails where it reads them
      table.io().deleteFile(task.file().location());
    }

    int before = spark.sparkContext().getPersistentRDDs().size();
    assertThatThrownBy(
            () ->
                DistributedIndexSearch.searchRowsFromHits(
                    spark,
                    metadata,
                    table,
                    query(metadata, snapshotId, 2),
                    table.schema().select("id", "embedding"),
                    spark.table("local.db.failed_rows").schema(),
                    1,
                    1,
                    4,
                    tasks,
                    null,
                    null,
                    List.of("id"),
                    null,
                    null))
        .as("a fetch-back whose data files are gone fails")
        .isInstanceOf(Exception.class)
        .hasMessageContaining("Job aborted");

    int afterFailure = spark.sparkContext().getPersistentRDDs().size();
    assertThat(afterFailure)
        .as("a fetch-back that failed releases the rows it read")
        .isEqualTo(before);
  }

  private static IndexQuery query(IndexMetadata metadata, long snapshotId, int topK) {
    VectorIndexAdapter adapter =
        new VectorIndexAdapter((VectorIndexDefinition) metadata.definition());
    return IndexQuery.of(snapshotId, adapter.encodeQuery(new float[] {0f, 0f}), topK)
        .withConsistency(IndexQuery.Consistency.AT_OR_BEFORE);
  }

  private static Table loadTable(String name) {
    return new HadoopTables(spark.sessionState().newHadoopConf())
        .load(warehouseDir.resolve("db/" + name).toUri().toString());
  }

  private static IndexMetadata buildVectorIndex(
      Table table, long snapshotId, String indexUuid, int fieldId) {
    IndexCatalog indexCatalog = new SidecarIndexCatalog(table.io(), table);
    IndexMetadata metadata =
        indexCatalog.createIndex(
            CreateIndexRequest.builder()
                .tableLocation(table.location())
                .tableUuid(table.uuid().toString())
                .indexUuid(indexUuid)
                .definition(
                    VectorIndexDefinition.builder()
                        .fieldId(fieldId)
                        .dimension(2)
                        .metric(Metric.L2)
                        .build())
                .build());
    IndexMetadata built =
        SparkIndexBuilder.build(spark, table, metadata, snapshotId, "embedding", 1L);
    indexCatalog.updateIndex(built);
    return built;
  }

  private static List<FileScanTask> scanTasks(Table table) {
    List<FileScanTask> tasks = Lists.newArrayList();
    try (CloseableIterable<FileScanTask> files = table.newScan().planFiles()) {
      for (FileScanTask task : files) {
        tasks.add(task);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    return tasks;
  }
}
