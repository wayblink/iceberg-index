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
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.Table;
import org.apache.iceberg.index.IndexFile;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.IndexQuery;
import org.apache.iceberg.index.IndexReader;
import org.apache.iceberg.index.spark.VectorIndexes;
import org.apache.iceberg.index.vector.VectorIndexAdapter;
import org.apache.iceberg.index.vector.VectorIndexTypes;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.index.BindVectorSearch;
import org.apache.iceberg.spark.index.VectorBindingResult;
import org.apache.iceberg.spark.index.VectorSearchContext;
import org.apache.iceberg.spark.index.VectorSearchRequest;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestVectorSearchPlanning {
  private static SparkSession spark;

  @BeforeAll
  static void startSpark(@TempDir Path warehouse) {
    VectorIndexTypes.register();
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("vector-search-planning")
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
  void tableAndIndexSnapshotsAreFrozenIndependently() throws Exception {
    Table table = tableWithIndex("planning_snapshot");
    long indexed = table.currentSnapshot().snapshotId();
    spark.sql("INSERT INTO local.db.planning_snapshot VALUES (3, array(0.5d, 0.5d))");
    table.refresh();
    long current = table.currentSnapshot().snapshotId();
    VectorSearchContext context = bind(table, null, "INDEX_HITS").context();
    assertThat(context.inputs().tableSnapshot().snapshotId()).isEqualTo(current);
    assertThat(context.inputs().indexSnapshot().sourceTableSnapshotId()).isEqualTo(indexed);

    spark.sql("INSERT INTO local.db.planning_snapshot VALUES (4, array(0.3d, 0.7d))");
    table.refresh();
    assertThat(table.currentSnapshot().snapshotId()).isNotEqualTo(current);
    assertThat(context.inputs().table().currentSnapshot().snapshotId()).isEqualTo(current);
    assertThat(context.inputs().tableSnapshot().snapshotId()).isEqualTo(current);
  }

  @Test
  void pinnedReadUsesItsOwnAncestorChain() throws Exception {
    Table table = tableWithIndex("planning_pinned");
    long indexed = table.currentSnapshot().snapshotId();
    spark.sql("INSERT INTO local.db.planning_pinned VALUES (3, array(0.5d, 0.5d))");
    table.refresh();
    long later = table.currentSnapshot().snapshotId();
    VectorSearchContext pinned = bind(table, indexed, "TABLE_LIVE_TOPK").context();
    VectorSearchContext lagging = bind(table, later, "TABLE_LIVE_TOPK").context();
    assertThat(pinned.inputs().tableSnapshot().snapshotId()).isEqualTo(indexed);
    assertThat(pinned.inputs().indexSnapshot().sourceTableSnapshotId()).isEqualTo(indexed);
    assertThat(pinned.inputs().uncoveredDataFiles().isEmpty()).isTrue();
    assertThat(lagging.inputs().indexSnapshot().sourceTableSnapshotId()).isEqualTo(indexed);
    assertThat(lagging.inputs().uncoveredDataFiles().size()).isEqualTo(1);
  }

  @Test
  void pinnedReadNeverUsesARefreshedIndexSnapshot() throws Exception {
    Table table = tableWithIndex("planning_refreshed");
    long first = table.currentSnapshot().snapshotId();
    spark.sql("INSERT INTO local.db.planning_refreshed VALUES (3, array(0.5d, 0.5d))");
    table.refresh();
    spark.sql(
        "CALL local.system.refresh_vector_index("
            + "table => 'db.planning_refreshed', column => 'embedding')");
    table.refresh();
    IndexMetadata metadata = VectorIndexes.findIndex(table, "embedding", "vector");
    assertThat(metadata.latestSnapshot().sourceTableSnapshotId())
        .isNotEqualTo(first)
        .isEqualTo(table.currentSnapshot().snapshotId());

    VectorSearchContext pinned = bind(table, first, "TABLE_LIVE_TOPK").context();
    assertThat(pinned.inputs().indexSnapshot().sourceTableSnapshotId()).isEqualTo(first);
    assertThat(pinned.inputs().uncoveredDataFiles().isEmpty()).isTrue();
  }

  @Test
  void bindingDoesNotReadDataOrIndexPayloads() throws Exception {
    Table table = tableWithIndex("planning_no_payload");
    IndexMetadata index = VectorIndexes.findIndex(table, "embedding", "vector");
    IndexQuery query =
        IndexQuery.of(index.latestSnapshot().sourceTableSnapshotId(), ByteBuffer.allocate(0), 0);
    List<String> locations = new ArrayList<>();
    for (IndexFile file : new IndexReader(index, table).indexFiles(query)) {
      if (!locations.contains(file.location())) {
        locations.add(file.location());
      }
    }
    try (var tasks = table.newScan().planFiles()) {
      for (var task : tasks) {
        locations.add(task.file().location());
      }
    }
    List<Path> moved = new ArrayList<>();
    String group = "vector-bind-no-jobs";
    try {
      for (String location : locations) {
        URI uri = URI.create(location);
        Path path = uri.getScheme() == null ? Path.of(location) : Path.of(uri);
        Files.move(path, Path.of(path + ".planning-hidden"));
        moved.add(path);
      }
      spark.sparkContext().setJobGroup(group, "Binding must not start a job", false);
      assertThat(bind(table, null, "INDEX_HITS").status()).isEqualTo("BOUND");
      assertThat(spark.sparkContext().statusTracker().getJobIdsForGroup(group)).isEmpty();
    } finally {
      spark.sparkContext().clearJobGroup();
      for (Path path : moved) {
        Files.move(Path.of(path + ".planning-hidden"), path);
      }
    }
  }

  @Test
  void referencedManifestFailureIsNotNoIndexFallback() throws Exception {
    Table table = tableWithIndex("planning_broken_manifest");
    IndexMetadata metadata = VectorIndexes.findIndex(table, "embedding", "vector");
    Path path = Path.of(URI.create(metadata.latestSnapshot().indexManifestList()));
    Path hidden = Path.of(path + ".planning-hidden");
    Files.move(path, hidden);
    try {
      String fileName = path.getFileName().toString();
      assertThatThrownBy(() -> bind(table, null, "INDEX_HITS")).hasMessageContaining(fileName);
    } finally {
      Files.move(hidden, path);
    }
  }

  @Test
  void entryBoundOfARealIndexCountsItsEntries() throws Exception {
    Table table = tableWithIndex("planning_entry_bound");
    VectorSearchContext context = bind(table, null, "INDEX_HITS").context();
    assertThat(context.inputs().indexFiles().size())
        .as("index files: %s", context.inputs().indexFiles())
        .isEqualTo(1);
    assertThat(context.inputs().entryUpperBound())
        .as("the entry bound of a two-row flat index: %s", context.inputs().indexFiles())
        .isEqualTo(2L);
  }

  @Test
  void entryBoundsCountEverySearchablePayloadOnce() {
    // a structure over the entries keeps them in the auxiliary payload, so both kinds count and a
    // repeated descriptor is counted once
    IndexFile entry = indexFile("memory://entries", IndexFile.Kind.ENTRY_BEARING, 20L);
    IndexFile auxiliary = indexFile("memory://aux", IndexFile.Kind.AUXILIARY, 30L);
    assertThat(BindVectorSearch.entryUpperBound(List.of(entry, entry, auxiliary))).isEqualTo(50L);
  }

  @Test
  void conflictingDuplicateEntryCountsAreUnknown() {
    assertThat(
            BindVectorSearch.entryUpperBound(
                List.of(
                    indexFile("memory://entries", IndexFile.Kind.ENTRY_BEARING, 20L),
                    indexFile("memory://entries", IndexFile.Kind.ENTRY_BEARING, 30L))))
        .isNull();
  }

  @Test
  void overflowingEntryCountIsUnknownRatherThanSmaller() {
    assertThat(
            BindVectorSearch.entryUpperBound(
                List.of(
                    indexFile("memory://one", IndexFile.Kind.ENTRY_BEARING, Long.MAX_VALUE),
                    indexFile("memory://two", IndexFile.Kind.ENTRY_BEARING, 1L))))
        .isNull();
  }

  @Test
  void queryInputIsCopiedBeforeBinding() throws Exception {
    Table table = tableWithIndex("planning_copied_query");
    float[] query = {1.0F, 0.0F};
    VectorSearchRequest request =
        VectorSearchRequest.forTable(
            table, "embedding", List.of(query), 2, null, "INDEX_HITS", null, true);
    query[0] = 100.0F;
    VectorSearchContext context = BindVectorSearch.bind(spark, request).context();
    assertThat(context.queryVectors().get(0)).containsExactly(1.0F, 0.0F);
  }

  private static VectorBindingResult bind(Table table, Long snapshotId, String contract) {
    return BindVectorSearch.bind(
        spark,
        VectorSearchRequest.forTable(
            table,
            "embedding",
            List.of(new float[] {1.0F, 0.0F}),
            2,
            snapshotId,
            contract,
            null,
            true));
  }

  private static Table tableWithIndex(String name) throws Exception {
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
    spark.sql(
        "CREATE TABLE local.db."
            + name
            + " (id bigint, embedding array<double> NOT NULL) USING iceberg");
    spark.sql("INSERT INTO local.db." + name + " VALUES (1, array(1.0d, 0.0d)), (2, array(0.0d, 1.0d))");
    spark.sql(
        "CALL local.system.build_vector_index(table => 'db."
            + name
            + "', column => 'embedding', dimension => 2, metric => 'cosine', structure => 'flat')");
    return Spark3Util.loadIcebergTable(spark, "local.db." + name);
  }

  private static IndexFile indexFile(String location, IndexFile.Kind kind, long count) {
    return IndexFile.builder()
        .location(location)
        .kind(kind)
        .blobType(
            kind == IndexFile.Kind.ENTRY_BEARING
                ? VectorIndexAdapter.ENTRY_BLOB_TYPE
                : VectorIndexAdapter.AUXILIARY_BLOB_TYPE)
        .entryCount(count)
        .build();
  }
}
