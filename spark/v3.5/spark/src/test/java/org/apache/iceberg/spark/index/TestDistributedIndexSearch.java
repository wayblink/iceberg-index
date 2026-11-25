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

import static org.apache.iceberg.data.FileHelpers.encrypt;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.index.CreateIndexRequest;
import org.apache.iceberg.index.IndexCatalog;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.IndexQuery;
import org.apache.iceberg.index.IndexReader;
import org.apache.iceberg.index.IndexSearchResult;
import org.apache.iceberg.index.SidecarIndexCatalog;
import org.apache.iceberg.index.TextStatisticsPolicy;
import org.apache.iceberg.index.spark.DistributedIndexSearch;
import org.apache.iceberg.index.spark.SparkIndexBuilder;
import org.apache.iceberg.index.text.TextIndexAdapter;
import org.apache.iceberg.index.text.TextIndexDefinition;
import org.apache.iceberg.index.text.TextIndexTypes;
import org.apache.iceberg.index.vector.VectorIndexAdapter;
import org.apache.iceberg.index.vector.VectorIndexDefinition;
import org.apache.iceberg.index.vector.VectorIndexDefinition.Metric;
import org.apache.iceberg.index.vector.VectorIndexTypes;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import scala.runtime.BoxedUnit;

/**
 * The merge of the per-index-file results of a distributed search, over more index files than the
 * query asks for hits, and with hits that tie across files.
 *
 * <p>Each task keeps the best hits of its own index file and a tree reduce keeps the best of what
 * the tasks produced, so the merge never puts every per-file hit on the driver. That is only the
 * same answer as sorting everything if the order of the adapter is applied exactly, including the
 * way it breaks ties, which is what this checks against a brute force top-k.
 */
class TestDistributedIndexSearch {

  private static final int INSERTS = 12;

  @BeforeAll
  static void registerVectorIndexType() {
    VectorIndexTypes.register();
  }

  private static SparkSession spark;
  private static Path warehouseDir;

  @BeforeAll
  static void startSpark(@TempDir Path warehouse) {
    warehouseDir = warehouse;
    spark =
        SparkSession.builder()
            .master("local[4]")
            .appName("distributed-index-search")
            .withExtensions(
                extensions -> {
                  IndexTableFunctions.register(extensions);
                  extensions.injectPlannerStrategy(VectorSearchStrategy::new);
                  return BoxedUnit.UNIT;
                })
            .config("spark.ui.enabled", "false")
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
  void scoresTextShardsAcrossTheSnapshotInSpark() throws Exception {
    TextIndexTypes.register();
    spark.sql("CREATE TABLE local.db.text_shards (id bigint, body string) USING iceberg");
    spark.sql("INSERT INTO local.db.text_shards VALUES (1, 'ice ice glacier')");
    spark.sql("INSERT INTO local.db.text_shards VALUES (2, 'ice cold')");
    spark.sql("INSERT INTO local.db.text_shards VALUES (3, 'cold water')");
    Table table =
        new HadoopTables(spark.sessionState().newHadoopConf())
            .load(warehouseDir.resolve("db/text_shards").toUri().toString());
    long snapshotId = table.currentSnapshot().snapshotId();
    TextIndexDefinition definition = TextIndexDefinition.builder().fieldId(2).build();
    IndexCatalog catalog = new SidecarIndexCatalog(table.io(), table);
    IndexMetadata created =
        catalog.createIndex(
            CreateIndexRequest.builder()
                .tableLocation(table.location())
                .tableUuid(table.uuid().toString())
                .indexUuid("text-shards")
                .definition(definition)
                .build());
    IndexMetadata built = SparkIndexBuilder.build(spark, table, created, snapshotId, "body", 1L);
    catalog.updateIndex(built);
    TextIndexAdapter adapter = new TextIndexAdapter(definition);
    List<IndexQuery> queries =
        List.of(
            IndexQuery.of(snapshotId, adapter.encodeQuery("ice glacier ice"), 1),
            IndexQuery.of(snapshotId, adapter.encodeQuery("cold"), 2),
            IndexQuery.of(snapshotId, adapter.encodeQuery("missing"), 3));
    IndexReader reference = new IndexReader(built, table);
    assertThat(reference.indexFiles(queries.get(0))).hasSize(3);

    String group = "bm25-shards";
    spark.sparkContext().setJobGroup(group, "Distributed BM25", false);
    try {
      assertThat(DistributedIndexSearch.searchBatch(spark, built, table, queries))
          .isEqualTo(queries.stream().map(reference::search).toList());
      spark.sparkContext().listenerBus().waitUntilEmpty();
      assertThat(spark.sparkContext().statusTracker().getJobIdsForGroup(group)).hasSize(1);
    } finally {
      spark.sparkContext().clearJobGroup();
    }

    String sql =
        "SELECT * FROM search(table => 'local.db.text_shards', query => 'ice glacier ice', "
            + "search_columns => array('body'), limit => 1, columns => array('id'))";
    String explainGroup = "bm25-explain";
    spark.sparkContext().setJobGroup(explainGroup, "Explain BM25", false);
    try {
      assertThat(spark.sql(sql).queryExecution().executedPlan().toString()).contains("TextSearch");
      spark.sparkContext().listenerBus().waitUntilEmpty();
      assertThat(spark.sparkContext().statusTracker().getJobIdsForGroup(explainGroup)).isEmpty();
    } finally {
      spark.sparkContext().clearJobGroup();
    }

    Row winner = spark.sql(sql).head();
    assertThat(winner.getLong(0)).isEqualTo(1L);
    assertThat(winner.getDouble(1)).isEqualTo(reference.search(queries.get(0)).get(0).score());
    assertThat(spark.sql(sql.replace("ice glacier ice", "missing")).collectAsList()).isEmpty();

    Dataset<Row> bound = spark.sql(sql);
    spark.sql("INSERT INTO local.db.text_shards VALUES (4, 'ice cold water')");
    assertThat(bound.collectAsList()).containsExactly(winner);
    assertThatThrownBy(() -> spark.sql(sql).collectAsList())
        .hasMessageContaining("refresh_text_index");
  }

  @Test
  void publishesTextShardsOfTheLengthColumnLayout() throws Exception {
    TextIndexTypes.register();
    spark.sql("CREATE TABLE local.db.text_v3 (id bigint, body string) USING iceberg");
    spark.sql("INSERT INTO local.db.text_v3 VALUES (1, 'ice ice glacier')");
    spark.sql("INSERT INTO local.db.text_v3 VALUES (2, 'ice cold')");
    Table table =
        new HadoopTables(spark.sessionState().newHadoopConf())
            .load(warehouseDir.resolve("db/text_v3").toUri().toString());
    long snapshotId = table.currentSnapshot().snapshotId();
    TextIndexDefinition definition = TextIndexDefinition.builder().fieldId(2).build();
    IndexCatalog catalog = new SidecarIndexCatalog(table.io(), table);
    IndexMetadata created =
        catalog.createIndex(
            CreateIndexRequest.builder()
                .tableLocation(table.location())
                .tableUuid(table.uuid().toString())
                .indexUuid("text-v3")
                .definition(definition)
                .build());
    IndexMetadata built = SparkIndexBuilder.build(spark, table, created, snapshotId, "body", 1L);
    catalog.updateIndex(built);

    TextIndexAdapter adapter = new TextIndexAdapter(definition);
    IndexQuery query = IndexQuery.of(snapshotId, adapter.encodeQuery("ice glacier ice"), 2);
    IndexReader reader = new IndexReader(built, table);
    assertThat(reader.indexFiles(query))
        .isNotEmpty()
        .allSatisfy(file -> assertThat(file.blobType()).isEqualTo("iceberg-text-bm25-v1"));

    String statistics = built.latestSnapshot().properties().get("snapshot-statistics-location");
    assertThat(statistics).isNotBlank();
    assertThat(table.io().newInputFile(statistics).exists()).isTrue();
  }

  /**
   * An index built without the merged statistics file answers from the shards' own counts, and the
   * answers are the ones the merged file produces.
   *
   * <p>The snapshot's statistics are only an optimization over summing the per-shard counts, so a
   * build that cannot merge a large corpus publishes without them; a query of such an index has to
   * agree with a query of a merged one term for term, through both the single-process reader and
   * the distributed entry point.
   */
  @Test
  void answersFromShardCountsWhenNoStatisticsWereMerged() throws Exception {
    TextIndexTypes.register();
    spark.sql("CREATE TABLE local.db.text_summed (id bigint, body string) USING iceberg");
    spark.sql("INSERT INTO local.db.text_summed VALUES (1, 'ice ice glacier')");
    spark.sql("INSERT INTO local.db.text_summed VALUES (2, 'ice cold')");
    spark.sql("INSERT INTO local.db.text_summed VALUES (3, 'glacier cold ice')");
    Table table =
        new HadoopTables(spark.sessionState().newHadoopConf())
            .load(warehouseDir.resolve("db/text_summed").toUri().toString());
    long snapshotId = table.currentSnapshot().snapshotId();
    TextIndexDefinition definition = TextIndexDefinition.builder().fieldId(2).build();
    TextIndexAdapter adapter = new TextIndexAdapter(definition);
    IndexQuery query = IndexQuery.of(snapshotId, adapter.encodeQuery("ice glacier"), 2);

    IndexMetadata merged =
        buildTextIndex(table, definition, "text-merged", snapshotId, TextStatisticsPolicy.ALWAYS);
    IndexMetadata summed =
        buildTextIndex(table, definition, "text-summed", snapshotId, TextStatisticsPolicy.NEVER);

    assertThat(merged.latestSnapshot().statisticsLocation()).isNotBlank();
    assertThat(summed.latestSnapshot().statisticsLocation())
        .as("a build told to skip the merge publishes no statistics file")
        .isNull();

    List<IndexSearchResult> fromMerged = new IndexReader(merged, table).search(query);
    List<IndexSearchResult> fromSummed = new IndexReader(summed, table).search(query);
    assertThat(fromSummed).isEqualTo(fromMerged);

    assertThat(DistributedIndexSearch.searchBatch(spark, summed, table, List.of(query)).get(0))
        .as("the distributed entry point sums the same per-shard counts")
        .containsExactlyElementsOf(fromMerged);
  }

  /**
   * The terms of a query are summed once: the next query that names them pays for its search only.
   *
   * <p>Summing walks every shard, and the answer depends on the snapshot and the terms rather than
   * on the query that carried them, so a driver keeps it. The second run here names the same terms
   * in another order, which is the same set and must not cost another pass.
   */
  @Test
  void sumsTheTermsOfAQueryOnce() throws Exception {
    TextIndexTypes.register();
    spark.sql("CREATE TABLE local.db.text_summed_once (id bigint, body string) USING iceberg");
    spark.sql("INSERT INTO local.db.text_summed_once VALUES (1, 'ice ice glacier')");
    spark.sql("INSERT INTO local.db.text_summed_once VALUES (2, 'ice cold')");
    spark.sql("INSERT INTO local.db.text_summed_once VALUES (3, 'glacier cold ice')");
    Table table =
        new HadoopTables(spark.sessionState().newHadoopConf())
            .load(warehouseDir.resolve("db/text_summed_once").toUri().toString());
    long snapshotId = table.currentSnapshot().snapshotId();
    TextIndexDefinition definition = TextIndexDefinition.builder().fieldId(2).build();
    TextIndexAdapter adapter = new TextIndexAdapter(definition);
    IndexMetadata summed =
        buildTextIndex(
            table, definition, "text-summed-once", snapshotId, TextStatisticsPolicy.NEVER);
    assertThat(summed.latestSnapshot().statisticsLocation()).isNull();

    IndexQuery firstQuery = IndexQuery.of(snapshotId, adapter.encodeQuery("ice glacier"), 2);
    IndexQuery sameTermsOtherOrder =
        IndexQuery.of(snapshotId, adapter.encodeQuery("glacier ice"), 2);

    Counted first = searchInItsOwnGroup("bm25-first-sum", summed, table, firstQuery);
    Counted second = searchInItsOwnGroup("bm25-second-sum", summed, table, sameTermsOtherOrder);

    assertThat(second.hits)
        .as("summing the same terms in another order answers the same rows")
        .isEqualTo(first.hits);
    assertThat(first.jobs)
        .as("the first run sums the terms over the shards and then searches")
        .isGreaterThan(second.jobs);
    assertThat(second.jobs)
        .as("the second run reuses the statistics the first summed instead of walking the shards")
        .isEqualTo(1);
  }

  private record Counted(List<IndexSearchResult> hits, int jobs) {}

  private static Counted searchInItsOwnGroup(
      String group, IndexMetadata metadata, Table table, IndexQuery query) throws Exception {
    spark.sparkContext().setJobGroup(group, group, false);
    try {
      List<IndexSearchResult> hits =
          DistributedIndexSearch.searchBatch(spark, metadata, table, List.of(query)).get(0);
      spark.sparkContext().listenerBus().waitUntilEmpty();
      int jobs = spark.sparkContext().statusTracker().getJobIdsForGroup(group).length;
      return new Counted(hits, jobs);
    } finally {
      spark.sparkContext().clearJobGroup();
    }
  }

  private static IndexMetadata buildTextIndex(
      Table table,
      TextIndexDefinition definition,
      String indexUuid,
      long snapshotId,
      TextStatisticsPolicy policy) {
    IndexCatalog catalog = new SidecarIndexCatalog(table.io(), table);
    IndexMetadata created =
        catalog.createIndex(
            CreateIndexRequest.builder()
                .tableLocation(table.location())
                .tableUuid(table.uuid().toString())
                .indexUuid(indexUuid)
                .definition(definition)
                .property(IndexMetadata.TEXT_STATISTICS_PROPERTY, policy.value())
                .build());
    IndexMetadata built = SparkIndexBuilder.build(spark, table, created, snapshotId, "body", 1L);
    catalog.updateIndex(built);
    return built;
  }

  @Test
  void publishesRangeReadableTextShardsAndSnapshotStatistics() throws Exception {
    TextIndexTypes.register();
    spark.sql("CREATE TABLE local.db.text_ranges (id bigint, body string) USING iceberg");
    spark.sql("INSERT INTO local.db.text_ranges VALUES (1, 'ice ice glacier')");
    spark.sql("INSERT INTO local.db.text_ranges VALUES (2, 'ice cold')");
    Table table =
        new HadoopTables(spark.sessionState().newHadoopConf())
            .load(warehouseDir.resolve("db/text_ranges").toUri().toString());
    long snapshotId = table.currentSnapshot().snapshotId();
    TextIndexDefinition definition = TextIndexDefinition.builder().fieldId(2).build();
    IndexCatalog catalog = new SidecarIndexCatalog(table.io(), table);
    IndexMetadata created =
        catalog.createIndex(
            CreateIndexRequest.builder()
                .tableLocation(table.location())
                .tableUuid(table.uuid().toString())
                .indexUuid("text-ranges")
                .definition(definition)
                .build());
    IndexMetadata built = SparkIndexBuilder.build(spark, table, created, snapshotId, "body", 1L);
    catalog.updateIndex(built);
    TextIndexAdapter adapter = new TextIndexAdapter(definition);
    IndexQuery query = IndexQuery.of(snapshotId, adapter.encodeQuery("ice glacier ice"), 2);
    IndexReader reader = new IndexReader(built, table);
    assertThat(reader.indexFiles(query))
        .hasSize(2)
        .allSatisfy(file -> assertThat(file.blobType()).isEqualTo("iceberg-text-bm25-v1"));
    String statistics = built.latestSnapshot().properties().get("snapshot-statistics-location");
    assertThat(statistics).isNotBlank();
    assertThat(table.io().newInputFile(statistics).exists()).isTrue();

    String group = "bm25-range-query";
    spark.sparkContext().setJobGroup(group, "BM25 range query", false);
    try {
      assertThat(DistributedIndexSearch.searchBatch(spark, built, table, List.of(query)).get(0))
          .containsExactlyElementsOf(reader.search(query));
      spark.sparkContext().listenerBus().waitUntilEmpty();
      assertThat(spark.sparkContext().statusTracker().getJobIdsForGroup(group)).hasSize(1);
    } finally {
      spark.sparkContext().clearJobGroup();
    }
  }

  @Test
  void filtersTextCandidatesBeforeTakingTheTopK() throws IOException {
    TextIndexTypes.register();
    spark.sql(
        "CREATE TABLE local.db.text_filter (id bigint, tenant int, body string) USING iceberg "
            + "TBLPROPERTIES ('format-version' = '2')");
    spark.sql(
        "INSERT INTO local.db.text_filter VALUES "
            + "(1, 99, 'ice ice glacier glacier'), (2, 42, 'ice'), "
            + "(3, NULL, 'ice ice glacier')");
    spark.sql("INSERT INTO local.db.text_filter VALUES (4, 99, 'glacier glacier ice')");
    Table table =
        new HadoopTables(spark.sessionState().newHadoopConf())
            .load(warehouseDir.resolve("db/text_filter").toUri().toString());
    long snapshotId = table.currentSnapshot().snapshotId();
    TextIndexDefinition definition = TextIndexDefinition.builder().fieldId(3).build();
    IndexCatalog catalog = new SidecarIndexCatalog(table.io(), table);
    IndexMetadata index =
        catalog.createIndex(
            CreateIndexRequest.builder()
                .tableLocation(table.location())
                .tableUuid(table.uuid().toString())
                .indexUuid("text-filter")
                .definition(definition)
                .build());
    catalog.updateIndex(SparkIndexBuilder.build(spark, table, index, snapshotId, "body", 3L));
    String search =
        "SELECT * FROM search(table => 'local.db.text_filter', query => 'ice glacier', "
            + "search_columns => array('body'), columns => array('id')";
    List<Row> full = spark.sql(search + ", limit => 4)").collectAsList();
    assertThat(full.get(0).getLong(0)).isNotEqualTo(2L);
    Row expected = full.stream().filter(row -> row.getLong(0) == 2L).findFirst().orElseThrow();
    assertThat(spark.sql(search + ", limit => 1, filter => 'tenant = 42')").collectAsList())
        .containsExactly(expected);
    assertThat(spark.sql(search + ", limit => 1, filter => 'NOT (tenant = 99)')").collectAsList())
        .containsExactly(expected);
    assertThat(spark.sql(search + ", limit => 1, filter => 'tenant = 100')").collectAsList())
        .isEmpty();
    assertThat(spark.sql(search + ", limit => 4, filter => 'true')").collectAsList())
        .containsExactlyElementsOf(full);
    assertThat(spark.sql(search + ", limit => 4, filter => 'false')").collectAsList()).isEmpty();
    assertThatThrownBy(
            () -> spark.sql(search + ", filter => 'unknown_column = 42')").collectAsList())
        .hasMessageContaining("unknown_column");

    // Removing a match from another tenant changes the corpus statistics but not admission.
    Row deleted = spark.sql("SELECT _file, _pos FROM local.db.text_filter WHERE id = 1").head();
    PositionDeleteWriter<Record> writer =
        new GenericFileWriterFactory.Builder(table)
            .deleteFileFormat(FileFormat.PARQUET)
            .build()
            .newPositionDeleteWriter(
                encrypt(table.io().newOutputFile(table.location() + "/data/delete.parquet")),
                table.spec(),
                null);
    try (writer) {
      writer.write(PositionDelete.<Record>create().set(deleted.getString(0), deleted.getLong(1)));
    }
    table.newRowDelta().addDeletes(writer.toDeleteFile()).commit();
    String filtered = search + ", limit => 1, filter => 'tenant = 42')";
    assertThatThrownBy(() -> spark.sql(filtered).collectAsList())
        .hasMessageContaining("refresh_text_index");
    refreshTextIndex(table, catalog, index.indexUuid());
    IndexMetadata refreshed = catalog.loadIndex(table.location(), index.indexUuid());
    TextIndexAdapter adapter = new TextIndexAdapter(definition);
    IndexQuery refreshedQuery =
        IndexQuery.of(table.currentSnapshot().snapshotId(), adapter.encodeQuery("ice glacier"), 4);
    IndexReader refreshedReader = new IndexReader(refreshed, table);
    assertThat(refreshedReader.indexFiles(refreshedQuery))
        .extracting(file -> file.blobType())
        .containsOnly(adapter.auxiliaryBlobType());
    assertThat(refreshed.latestSnapshot().statisticsLocation()).isNotNull();
    assertThat(
            refreshedReader.search(
                IndexQuery.of(snapshotId, adapter.encodeQuery("ice glacier"), 4)))
        .hasSize(4);
    Row afterDelete = spark.sql(filtered).head();
    double averageLength = (1 + 3 + 3) / 3.0;
    double iceIdf = Math.log1p(0.5 / (3 + 0.5));
    double normalization =
        definition.k1() * (1 - definition.lengthNorm() + definition.lengthNorm() / averageLength);
    double expectedScore = iceIdf * (definition.k1() + 1) / (1 + normalization);
    assertThat(afterDelete.getLong(0)).isEqualTo(2L);
    assertThat(afterDelete.getDouble(1)).isCloseTo(expectedScore, within(1e-12));

    assertThat(
            SparkActions.get(spark)
                .rewriteDataFiles(table)
                .option("rewrite-all", "true")
                .execute()
                .rewrittenDataFilesCount())
        .isPositive();
    table.refresh();
    refreshTextIndex(table, catalog, index.indexUuid());
    assertThat(spark.sql(filtered).collectAsList()).containsExactly(afterDelete);
  }

  private static void refreshTextIndex(Table table, IndexCatalog catalog, String uuid) {
    IndexMetadata current = catalog.loadIndex(table.location(), uuid);
    catalog.updateIndex(
        SparkIndexBuilder.refresh(
            spark,
            table,
            current,
            table.currentSnapshot().snapshotId(),
            "body",
            System.currentTimeMillis()));
  }

  @Test
  public void mergesTheResultsOfManyIndexFilesInTheOrderOfTheAdapter() {
    spark.sql("CREATE TABLE local.db.spread (id bigint, embedding array<double>) USING iceberg");

    // one data file per insert, and one index file per data file, so the query has more index files
    // to merge than hits it asks for. The vectors repeat, so several rows of different files are at
    // exactly the same distance from the query, which is what the tie breaker is for
    float[][] vectors = {
      {0.0f, 0.0f}, {1.0f, 0.0f}, {0.0f, 1.0f}, {1.0f, 1.0f},
      {2.0f, 0.0f}, {0.0f, 2.0f}, {2.0f, 2.0f}, {1.0f, 1.0f},
      {0.0f, 0.0f}, {3.0f, 0.0f}, {0.0f, 3.0f}, {3.0f, 3.0f},
    };
    for (float[] vector : vectors) {
      spark.sql(
          String.format(
              "INSERT INTO local.db.spread VALUES (%d, array(%f, %f))",
              inserted++, vector[0], vector[1]));
    }
    assertThat(inserted).isEqualTo(INSERTS);

    Table table =
        new HadoopTables(spark.sessionState().newHadoopConf())
            .load(warehouseDir.resolve("db/spread").toUri().toString());
    long snapshotId = table.currentSnapshot().snapshotId();

    IndexCatalog indexCatalog = new SidecarIndexCatalog(table.io(), table);
    IndexMetadata metadata =
        indexCatalog.createIndex(
            CreateIndexRequest.builder()
                .tableLocation(table.location())
                .tableUuid(table.uuid().toString())
                .indexUuid("spread-vectors")
                .definition(
                    VectorIndexDefinition.builder()
                        .fieldId(2)
                        .dimension(2)
                        .metric(Metric.L2)
                        .build())
                .build());
    // one entry per index file, which is what gives the query many index files to merge
    IndexMetadata built =
        SparkIndexBuilder.build(spark, table, metadata, snapshotId, "embedding", 1L);
    indexCatalog.updateIndex(built);

    VectorIndexAdapter adapter = new VectorIndexAdapter((VectorIndexDefinition) built.definition());
    float[] query = {0.5f, 0.5f};

    for (int topK : new int[] {1, 3, 5, INSERTS}) {
      CountingListener listener = new CountingListener();
      spark.sparkContext().addSparkListener(listener);
      DistributedIndexSearch.SearchOutcome outcome;
      try {
        outcome =
            DistributedIndexSearch.search(
                spark, built, table, IndexQuery.of(snapshotId, adapter.encodeQuery(query), topK));
      } finally {
        spark.sparkContext().removeSparkListener(listener);
      }

      assertThat(outcome.hits()).containsExactlyElementsOf(bruteForceTopK(query, topK));
      assertThat(outcome.moreHits()).isEqualTo(topK < INSERTS);
      // one task per index file: the index file is the unit of work, so a byte budget or a minimum
      // partition count has no say in how the search is split up
      // the size a budget is derived from is measured by preparing each payload once, which reads
      // the files on the driver; the search itself still splits into one task per index file, and
      // the first search of a session is the one that pays for the measurement
      assertThat(listener.tasks).isBetween(INSERTS, 2 * INSERTS);
    }
  }

  /** Counts the tasks a search ran, which is what its parallelism was. */
  private static final class CountingListener extends SparkListener {
    private int tasks;

    @Override
    public void onTaskEnd(SparkListenerTaskEnd end) {
      tasks += 1;
    }
  }

  private static long inserted = 0;

  /** The top-k hits of a brute force scan, in the order the adapter puts them. */
  private List<IndexSearchResult> bruteForceTopK(float[] query, int topK) {
    List<IndexSearchResult> all = Lists.newArrayList();
    // the metadata columns give the row reference of every row, which is what an index entry holds
    for (Row row :
        spark.sql("SELECT _file, _pos, embedding FROM local.db.spread").collectAsList()) {
      List<Double> vector = row.getList(2);
      double squared = 0.0;
      for (int i = 0; i < vector.size(); i++) {
        double difference = vector.get(i) - query[i];
        squared += difference * difference;
      }
      all.add(IndexSearchResult.of(row.getString(0), row.getLong(1), Math.sqrt(squared)));
    }

    all.sort(
        Comparator.comparingDouble(IndexSearchResult::score)
            .thenComparing(IndexSearchResult::dataFile)
            .thenComparingLong(IndexSearchResult::rowPosition));
    return all.size() > topK ? all.subList(0, topK) : all;
  }
}
