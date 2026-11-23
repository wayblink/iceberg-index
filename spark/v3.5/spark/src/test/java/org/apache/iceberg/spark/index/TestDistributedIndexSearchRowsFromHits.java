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
import org.apache.spark.api.java.JavaRDD;
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

    JavaRDD<Row> rows =
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
            null);

    List<Row> collected = rows.collect();
    assertThat(collected).as("the only row the filter admits is the 513th hit").hasSize(1);
    assertThat(collected.get(0).getLong(0)).isEqualTo(512L);
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
    JavaRDD<Row> rows =
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
            null);

    List<Row> collected = rows.collect();
    assertThat(collected).hasSize(1);
    assertThat(collected.get(0).getLong(0)).isEqualTo(399L);
    assertThat(collected.get(0).getString(1)).hasSize(4096);
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
