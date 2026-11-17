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
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestVectorSearchPlanning {
  private static final String OVER_SAMPLE = "spark.iceberg.index.vector.over-sample";

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

  @Test
  void planSaysWhetherTheAnswerCanBeExact() throws Exception {
    // a flat scan over unquantized vectors enumerates every candidate, so its answer is the exact
    // top-k; every other structure samples, and the plan has to say so where a reader will look
    tableWithIndex("planning_exact");
    assertThat(planOf("planning_exact")).contains("result=exact");

    // an approximate structure is used once the caller has said how many candidates to ask for,
    // because one candidate per hit is not a candidate set the nearest rows are in
    createApproximateTable("planning_approximate");
    try {
      spark.conf().set(OVER_SAMPLE, "10");
      assertThat(planOf("planning_approximate")).contains("result=approximate");
    } finally {
      spark.conf().unset(OVER_SAMPLE);
    }
  }

  /**
   * An approximate kernel ranks candidates by an estimate, so asking it for k rows asks for k
   * estimates and not for the k nearest rows. The index is used anyway: whether a query is answered
   * approximately is a property of the index the caller built, not of whether they also set a
   * multiple. What the multiple decides is how many candidates the estimate hands to the exact
   * re-scoring. The plan shows when the base budget was selected by default.
   */
  @Test
  void anApproximateIndexIsUsedWithoutAnExplicitOverSample() {
    createApproximateTable("planning_approx_default");

    assertThat(plan(nearestOf("planning_approx_default")))
        .as("the index answers, and the plan says it did so approximately")
        .contains("VectorSearchSource")
        .contains("result=approximate")
        .as("and the width it used is stated with its source")
        .contains("overSample=0 [DEFAULT]");

    try {
      // a caller who knows what they want still decides: the configured value replaces the default
      spark.conf().set(OVER_SAMPLE, "10");
      assertThat(plan(nearestOf("planning_approx_default")))
          .contains("VectorSearchSource")
          .contains("result=approximate")
          .contains("overSample=10 [SESSION_CONF");
    } finally {
      spark.conf().unset(OVER_SAMPLE);
    }
  }

  @Test
  void anExplicitWiderWindowRecoversTheNearestRow() {
    createApproximateTable("planning_approx_correct");
    String nearest = nearestOf("planning_approx_correct");
    List<Row> scanned = spark.sql(nearest.replace("LIMIT 1", "+ 0.0d LIMIT 1")).collectAsList();
    List<Row> base = spark.sql(nearest).collectAsList();

    try {
      spark.conf().set(OVER_SAMPLE, "0");
      assertThat(spark.sql(nearest).collectAsList()).isEqualTo(base);
      assertThat(base).isNotEqualTo(scanned);

      spark.conf().set(OVER_SAMPLE, "50");
      assertThat(spark.sql(nearest).collectAsList()).isEqualTo(scanned);
    } finally {
      spark.conf().unset(OVER_SAMPLE);
    }
  }

  @Test
  void sessionProbeCountChangesTheListsVisitedWithoutRebuilding() {
    createApproximateTable("planning_query_probe");
    String key = "spark.iceberg.index.vector.ivf.nprobe";
    String query =
        "CALL local.system.vector_search(table => 'db.planning_query_probe', "
            + "column => 'embedding', query => array(0.95d, 0.05d), k => 32, "
            + "coverage => 'index_hits')";
    try {
      spark.conf().set(key, "32");
      List<Row> all = spark.sql(query).collectAsList();
      spark.conf().set(key, "1");
      assertThat(spark.sql(query).collectAsList()).isNotEmpty().hasSizeLessThan(all.size());
      spark.conf().set(key, "32");
      assertThat(spark.sql(query).collectAsList()).containsExactlyElementsOf(all);
    } finally {
      spark.conf().unset(key);
    }
  }

  @Test
  void groupedIvfBlocksCanBeReadWholeOrByRange() {
    String table = "local.db.planning_range_lists";
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
    spark.sql("CREATE TABLE " + table + " (id bigint, embedding array<double>) USING iceberg");
    spark.sql(
        "INSERT INTO " + table + " SELECT id, array(CAST(id AS double), 1.0d) FROM range(0, 32)");
    spark.sql(
        "INSERT INTO " + table + " SELECT id, array(CAST(id AS double), 1.0d) FROM range(32, 64)");
    spark.sql(
        "CALL local.system.build_vector_index(table => 'db.planning_range_lists', "
            + "column => 'embedding', dimension => 2, metric => 'euclidean', structure => 'IVF', "
            + "max_entries_per_index_file => 128, "
            + "options => map('nlist', '8', 'ivf-list-blocks', 'true'))");
    String range = "spark.iceberg.index.vector.ivf.range-read.enabled";
    String probes = "spark.iceberg.index.vector.ivf.nprobe";
    String cache = "spark.iceberg.index.cache.enabled";
    String budget = "spark.iceberg.index.cache.bytes";
    String query =
        "SELECT id FROM "
            + table
            + " ORDER BY vec_distance(embedding, array(31.1d, 1.0d), 'euclidean') LIMIT 4";
    try {
      spark.conf().set(cache, "false");
      spark.conf().set(probes, "2");
      List<Row> whole = spark.sql(query).collectAsList();
      assertThat(whole).extracting(row -> row.getLong(0)).containsExactly(31L, 32L, 30L, 33L);
      spark.conf().set(range, "true");
      assertThat(spark.sql(query).collectAsList()).containsExactlyElementsOf(whole);

      spark.conf().set(cache, "true");
      spark.conf().set(budget, "16m");
      spark.conf().set(range, "false");
      assertThat(spark.sql(query).collectAsList()).containsExactlyElementsOf(whole);
      long hits = cacheHits();
      spark.conf().set(range, "true");
      assertThat(spark.sql(query).collectAsList()).containsExactlyElementsOf(whole);
      assertThat(cacheHits()).isGreaterThan(hits);
    } finally {
      spark.conf().unset(range);
      spark.conf().unset(probes);
      spark.conf().unset(cache);
      spark.conf().unset(budget);
    }
  }

  private static long cacheHits() {
    return spark.sql("CALL local.system.index_cache_status(refresh => true)").collectAsList().stream()
        .mapToLong(row -> row.<Long>getAs("hits"))
        .sum();
  }

  private static String nearestOf(String name) {
    return "SELECT id FROM local.db."
        + name
        + " ORDER BY vec_distance(embedding, array(0.95d, 0.05d)) LIMIT 1";
  }

  private static String plan(String query) {
    return spark.sql(query).queryExecution().optimizedPlan().toString();
  }

  /**
   * A table of 32 vectors on the line x + y = 32, with an IVF index over 1-bit codes, which is the
   * shape whose first candidate under one candidate per hit is not the nearest row.
   */
  private static void createApproximateTable(String name) {
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
    spark.sql(
        "CREATE TABLE local.db."
            + name
            + " (id bigint, embedding array<double> NOT NULL) USING iceberg");
    spark.sql(
        "INSERT INTO local.db."
            + name
            + " SELECT id, array(CAST(id AS double), CAST(32 - id AS double)) FROM range(0, 32)");
    spark.sql(
        "CALL local.system.build_vector_index(table => 'db."
            + name
            + "', column => 'embedding', dimension => 2, metric => 'cosine', "
            + "structure => 'IVF', quantizer => 'RABITQ', options => map('nprobe', '32'))");
  }

  private static String planOf(String name) {
    return spark
        .sql(
            "SELECT id FROM local.db."
                + name
                + " ORDER BY vec_distance(embedding, array(1.0d, 0.0d)) LIMIT 2")
        .queryExecution()
        .optimizedPlan()
        .toString();
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
    spark.sql(
        "INSERT INTO local.db." + name + " VALUES (1, array(1.0d, 0.0d)), (2, array(0.0d, 1.0d))");
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
