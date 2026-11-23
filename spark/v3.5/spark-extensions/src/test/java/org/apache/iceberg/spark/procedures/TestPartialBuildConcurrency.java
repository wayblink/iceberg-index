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
package org.apache.iceberg.spark.procedures;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.index.CreateIndexRequest;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.IndexSnapshot;
import org.apache.iceberg.index.SidecarIndexCatalog;
import org.apache.iceberg.index.text.TextIndexDefinition;
import org.apache.iceberg.index.text.TextIndexTypes;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * What concurrent publications of the same index do to each other.
 *
 * <p>A publication reads the metadata, builds against it, and commits by replacing the document it
 * read. The catalog applies the change to whatever is current when the commit lands, so a build can
 * name a snapshot that another publisher replaced in the meantime. That has to end in a metadata
 * the invariants accept, not in a failure after every index file was written.
 */
class TestPartialBuildConcurrency {

  private static final int ROUNDS = 5;

  private static SparkSession spark;
  private static Path warehouse;

  @BeforeAll
  static void start(@TempDir Path directory) {
    warehouse = directory;
    TextIndexTypes.register();
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("partial-build-concurrency")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", directory.toUri().toString())
            .getOrCreate();
  }

  @AfterAll
  static void stop() {
    if (spark != null) {
      spark.stop();
    }
  }

  /**
   * A replacement of one table snapshot and an append derived from it commit at the same time.
   *
   * <p>The replacement removes the snapshot the append names as its parent. Whichever order the two
   * commits land in, the index must end up with every source snapshot answered once and every
   * parent present — the append may not fail on a parent that is gone.
   *
   * <p>The rounds share one table: creating a table scans the warehouse directory, and a test that
   * builds one per round spends its time on that rather than on the race.
   */
  @Test
  void aReplacementAndAnAppendThatNamesItCommitTogether() throws Exception {
    String name = "race";
    spark.sql("CREATE TABLE local.db." + name + " (id bigint, body string) USING iceberg");
    spark.sql("INSERT INTO local.db." + name + " VALUES (1, 'ice')");
    Table table = table(name);
    SidecarIndexCatalog catalog = new SidecarIndexCatalog(table.io(), table);
    IndexMetadata initial =
        catalog.createIndex(
            CreateIndexRequest.builder()
                .tableLocation(table.location())
                .tableUuid(table.uuid().toString())
                .indexUuid("race")
                .definition(TextIndexDefinition.builder().fieldId(2).build())
                .build());

    long first = table.currentSnapshot().snapshotId();
    spark.sql("INSERT INTO local.db." + name + " VALUES (2, 'ice')");
    table.refresh();
    long second = table.currentSnapshot().snapshotId();

    VectorIndexProcedures.publishSnapshot(
        catalog, table, initial.indexUuid(), snapshot(2L, first, null, "/index/one"), true);

    for (int round = 0; round < ROUNDS; round += 1) {
      IndexMetadata current = catalog.loadIndex(table.location(), initial.indexUuid());
      IndexSnapshot published = current.latestSnapshot();

      // the append names the newest build as its parent; the replacement removes that build. The
      // provisional ids only have to avoid the ids already published, which publication replaces
      // with the id after the newest one anyway.
      IndexSnapshot appended =
          snapshot(1000L + 2L * round, second, published.snapshotId(), "/index/two-manifest-list");
      IndexSnapshot replaced = snapshot(1001L + 2L * round, first, null, "/index/replaced");

      runTogether(
          () ->
              VectorIndexProcedures.publishSnapshot(
                  catalog, table, initial.indexUuid(), replaced, true),
          () ->
              VectorIndexProcedures.publishSnapshot(
                  catalog, table, initial.indexUuid(), appended, true));

      assertMetadataIsSane(catalog.loadIndex(table.location(), initial.indexUuid()), first, second);
    }
  }

  /** The same index built twice at once ends with one build per source snapshot, not two. */
  @Test
  void twoBuildsOfTheSameSnapshotDoNotBothStay() throws Exception {
    String name = "same";
    spark.sql("CREATE TABLE local.db." + name + " (id bigint, body string) USING iceberg");
    spark.sql("INSERT INTO local.db." + name + " VALUES (1, 'ice')");
    Table table = table(name);
    SidecarIndexCatalog catalog = new SidecarIndexCatalog(table.io(), table);
    IndexMetadata initial =
        catalog.createIndex(
            CreateIndexRequest.builder()
                .tableLocation(table.location())
                .tableUuid(table.uuid().toString())
                .indexUuid("same")
                .definition(TextIndexDefinition.builder().fieldId(2).build())
                .build());

    long source = table.currentSnapshot().snapshotId();
    for (int round = 0; round < ROUNDS; round += 1) {
      runTogether(
          () ->
              VectorIndexProcedures.publishSnapshot(
                  catalog,
                  table,
                  initial.indexUuid(),
                  snapshot(2L, source, null, "/index/a"),
                  true),
          () ->
              VectorIndexProcedures.publishSnapshot(
                  catalog,
                  table,
                  initial.indexUuid(),
                  snapshot(3L, source, null, "/index/b"),
                  true));

      IndexMetadata metadata = catalog.loadIndex(table.location(), initial.indexUuid());
      assertThat(metadata.snapshots())
          .as("one build answers for the table snapshot, however many published")
          .hasSize(1);
      assertThat(metadata.snapshots().get(0).parentIndexSnapshotId())
          .as("the surviving build has no parent, since the one it replaced had none")
          .isNull();
    }
  }

  /** Every source snapshot is answered at most once, and every parent it names exists. */
  private static void assertMetadataIsSane(
      IndexMetadata metadata, long firstSource, long secondSource) {
    Set<Long> ids =
        metadata.snapshots().stream().map(IndexSnapshot::snapshotId).collect(Collectors.toSet());
    assertThat(ids).as("snapshot ids are unique").hasSize(metadata.snapshots().size());
    assertThat(
            metadata.snapshots().stream()
                .map(IndexSnapshot::sourceTableSnapshotId)
                .distinct()
                .count())
        .as("each source snapshot is answered by one build")
        .isEqualTo(metadata.snapshots().size());
    for (IndexSnapshot snapshot : metadata.snapshots()) {
      Long parent = snapshot.parentIndexSnapshotId();
      if (parent != null) {
        assertThat(ids).as("the parent %s a build names is in the index", parent).contains(parent);
      }
    }

    assertThat(metadata.snapshotForSourceTableSnapshot(firstSource))
        .as("the first table snapshot is still answered")
        .isNotNull();
    assertThat(metadata.snapshotForSourceTableSnapshot(secondSource))
        .as("the second table snapshot is answered too")
        .isNotNull();
  }

  private static void runTogether(Runnable left, Runnable right) throws Exception {
    ExecutorService pool = Executors.newFixedThreadPool(2);
    CountDownLatch start = new CountDownLatch(1);
    try {
      List<Future<?>> futures = new ArrayList<>();
      for (Runnable task : List.of(left, right)) {
        futures.add(
            pool.submit(
                () -> {
                  start.await();
                  task.run();
                  return null;
                }));
      }

      start.countDown();
      for (Future<?> future : futures) {
        // any failure inside either publication surfaces here
        future.get(5, TimeUnit.MINUTES);
      }
    } finally {
      pool.shutdownNow();
    }
  }

  private static IndexSnapshot snapshot(
      long provisionalId, long source, Long parent, String manifestList) {
    return IndexSnapshot.builder()
        .snapshotId(provisionalId)
        .sourceTableSnapshotId(source)
        .parentIndexSnapshotId(parent)
        .timestampMillis(10L)
        .indexManifestList(manifestList)
        .build();
  }

  /** Loads the table the way the other publication tests do: by its location. */
  private Table table(String name) {
    return new HadoopTables(spark.sessionState().newHadoopConf())
        .load(warehouse.resolve("db/" + name).toUri().toString());
  }
}
