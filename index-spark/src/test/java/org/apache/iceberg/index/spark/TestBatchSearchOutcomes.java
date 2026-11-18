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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.groups.Tuple.tuple;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.index.IndexBuildRequest;
import org.apache.iceberg.index.IndexBuilder;
import org.apache.iceberg.index.IndexDefinition;
import org.apache.iceberg.index.IndexEntry;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.IndexQuery;
import org.apache.iceberg.index.IndexReader;
import org.apache.iceberg.index.IndexSearchResult;
import org.apache.iceberg.index.IndexSnapshotWriter;
import org.apache.iceberg.index.IndexTypes;
import org.apache.iceberg.index.PuffinIndexFileContainer;
import org.apache.iceberg.index.TestingCorrelatedIndexDefinition;
import org.apache.iceberg.index.TestingIndexAdapter;
import org.apache.iceberg.index.vector.VectorIndexAdapter;
import org.apache.iceberg.index.vector.VectorIndexDefinition;
import org.apache.iceberg.index.vector.VectorIndexTypes;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A batch search answers every query the way a search of that query alone would, and says whether
 * the index holds more hits than the query's own window.
 */
class TestBatchSearchOutcomes {
  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
  private static final int DIMENSION = 2;
  // TestTables keeps one static metadata map per table name, so each fixture needs its own name.
  private static final AtomicInteger SEQUENCE = new AtomicInteger();

  private static SparkSession spark;

  @TempDir private Path temp;

  @BeforeAll
  static void startSpark() {
    VectorIndexTypes.register();
    IndexTypes.register(new TestingCorrelatedIndexDefinition.Type());
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("test-batch-search-outcomes")
            .config("spark.driver.host", InetAddress.getLoopbackAddress().getHostAddress())
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "4")
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
  void batchOutcomesMatchSingleSearchesOnAMultiFileVectorIndex() {
    Fixture fixture = vectorFixture(3);
    List<IndexQuery> queries =
        List.of(
            vectorQuery(fixture, 3, 0.0f, 0.0f),
            vectorQuery(fixture, 2, 1.0f, 0.0f),
            vectorQuery(fixture, 10, 0.0f, 0.0f),
            vectorQuery(fixture, 0, 0.0f, 0.0f));

    List<DistributedIndexSearch.SearchOutcome> outcomes =
        DistributedIndexSearch.searchBatchWithOutcomes(
            spark, fixture.metadata, fixture.table, queries);
    List<List<IndexSearchResult>> batched =
        DistributedIndexSearch.searchBatch(spark, fixture.metadata, fixture.table, queries);

    assertThat(outcomes).hasSize(queries.size());
    assertThat(batched).hasSize(queries.size());
    for (int i = 0; i < queries.size(); i++) {
      IndexQuery query = queries.get(i);
      DistributedIndexSearch.SearchOutcome single =
          DistributedIndexSearch.search(spark, fixture.metadata, fixture.table, query);
      DistributedIndexSearch.SearchOutcome outcome = outcomes.get(i);

      assertThat(outcome.hits()).as("hits of query %s", i).containsExactlyElementsOf(single.hits());
      assertThat(outcome.moreHits()).as("moreHits of query %s", i).isEqualTo(single.moreHits());
      assertThat(outcome.hits().size())
          .as("width of query %s", i)
          .isLessThanOrEqualTo(query.topK());
      assertThat(batched.get(i))
          .as("searchBatch hits of query %s", i)
          .containsExactlyElementsOf(outcome.hits());
    }

    // the nearest row is b.parquet's exact match, then the two distance-1 rows of a.parquet, which
    // are tied on score and ordered by data file and position
    assertThat(outcomes.get(0).hits())
        .extracting(IndexSearchResult::dataFile, IndexSearchResult::rowPosition)
        .containsExactly(tuple("b.parquet", 1L), tuple("a.parquet", 0L), tuple("a.parquet", 1L));
    assertThat(outcomes.get(0).moreHits()).isTrue();
    assertThat(outcomes.get(2).hits()).hasSize(9);
    assertThat(outcomes.get(2).moreHits())
        .as("the index holds nine hits, which is fewer than the ten the query asked for")
        .isFalse();
    assertThat(outcomes.get(3).hits()).isEmpty();
    assertThat(outcomes.get(3).moreHits()).as("the index holds more than zero hits").isTrue();
  }

  @Test
  void eachQueryIsAnsweredAtItsOwnWidth() {
    Fixture fixture = vectorFixture(3);
    IndexQuery narrow = vectorQuery(fixture, 2, 0.0f, 0.0f);
    IndexQuery wide = vectorQuery(fixture, 9, 0.0f, 0.0f);

    List<DistributedIndexSearch.SearchOutcome> outcomes =
        DistributedIndexSearch.searchBatchWithOutcomes(
            spark, fixture.metadata, fixture.table, List.of(narrow, wide));

    assertThat(outcomes.get(0).hits()).hasSize(2);
    assertThat(outcomes.get(1).hits()).hasSize(9);
    assertThat(outcomes.get(0).hits())
        .as("the merged result is cut to the narrow query's own width")
        .containsExactlyElementsOf(outcomes.get(1).hits().subList(0, 2));
    assertThat(outcomes.get(0).moreHits()).isTrue();
    assertThat(outcomes.get(1).moreHits()).isFalse();
  }

  @Test
  void equalScoresAreOrderedByDataFileThenPosition() {
    Fixture fixture = vectorFixture(3);
    IndexQuery all = vectorQuery(fixture, 9, 0.0f, 0.0f);

    List<DistributedIndexSearch.SearchOutcome> outcomes =
        DistributedIndexSearch.searchBatchWithOutcomes(
            spark, fixture.metadata, fixture.table, List.of(all));
    List<DistributedIndexSearch.SearchOutcome> again =
        DistributedIndexSearch.searchBatchWithOutcomes(
            spark, fixture.metadata, fixture.table, List.of(all));

    // five rows share the distance of one: a.parquet's rows 0 and 1, b.parquet's row 0, and
    // c.parquet's rows 1 and 2. They come out in (data file, position) order, and a repeated
    // search produces the same order.
    assertThat(outcomes.get(0).hits())
        .extracting(IndexSearchResult::dataFile, IndexSearchResult::rowPosition)
        .containsExactly(
            tuple("b.parquet", 1L),
            tuple("a.parquet", 0L),
            tuple("a.parquet", 1L),
            tuple("b.parquet", 0L),
            tuple("c.parquet", 1L),
            tuple("c.parquet", 2L),
            tuple("a.parquet", 2L),
            tuple("c.parquet", 0L),
            tuple("b.parquet", 2L));
    assertThat(again.get(0).hits()).containsExactlyElementsOf(outcomes.get(0).hits());
  }

  @Test
  void searchesThatMergePayloadsReportNoMoreHits() {
    CorrelatedFixture fixture = correlatedFixture(2);
    List<IndexQuery> queries = List.of(correlatedQuery(fixture, 1), correlatedQuery(fixture, 3));

    List<DistributedIndexSearch.SearchOutcome> outcomes =
        DistributedIndexSearch.searchBatchWithOutcomes(
            spark, fixture.metadata, fixture.table, queries);
    List<List<IndexSearchResult>> batched =
        DistributedIndexSearch.searchBatch(spark, fixture.metadata, fixture.table, queries);

    for (int i = 0; i < queries.size(); i++) {
      List<IndexSearchResult> together =
          new IndexReader(fixture.metadata, fixture.table).search(queries.get(i));

      assertThat(outcomes.get(i).hits())
          .as("hits of query %s", i)
          .containsExactlyElementsOf(together);
      assertThat(batched.get(i))
          .as("searchBatch hits of query %s", i)
          .containsExactlyElementsOf(together);
      assertThat(outcomes.get(i).moreHits())
          .as("a search that merges payloads only sees its own top-k")
          .isFalse();
    }

    assertThat(outcomes.get(0).hits()).hasSize(1);
    assertThat(outcomes.get(1).hits()).hasSize(3);
  }

  @Test
  void emptyBatchReturnsNoOutcomes() {
    Fixture fixture = vectorFixture(3);

    assertThat(
            DistributedIndexSearch.searchBatchWithOutcomes(
                spark, fixture.metadata, fixture.table, List.of()))
        .isEmpty();
    assertThat(
            DistributedIndexSearch.searchBatch(spark, fixture.metadata, fixture.table, List.of()))
        .isEmpty();
  }

  @Test
  void queriesWithDifferentConsistencyAreRejected() {
    Fixture fixture = vectorFixture(3);
    IndexQuery exact = vectorQuery(fixture, 1, 0.0f, 0.0f);
    IndexQuery atOrBefore = exact.withConsistency(IndexQuery.Consistency.AT_OR_BEFORE);

    assertThatThrownBy(
            () ->
                DistributedIndexSearch.searchBatchWithOutcomes(
                    spark, fixture.metadata, fixture.table, List.of(exact, atOrBefore)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("same consistency");
  }

  @Test
  void hnswAnswersEachQueryAtItsOwnWindow() {
    Fixture fixture =
        vectorFixture(
            VectorIndexDefinition.VectorStructure.HNSW,
            3,
            12,
            adapter -> {
              float[][] vectors = {
                {26, 2}, {81, 37}, {11, 32}, {70, 97}, {7, 6}, {22, 96},
                {12, 4}, {70, 30}, {84, 55}, {31, 2}, {54, 69}, {14, 30}
              };
              List<IndexEntry> entries = new ArrayList<>(vectors.length);
              for (int row = 0; row < vectors.length; row++) {
                entries.add(IndexEntry.of("a.parquet", row, adapter.encodeVector(vectors[row])));
              }
              return entries;
            });
    IndexQuery narrow = vectorQuery(fixture, 2, 45.0f, 30.0f);
    IndexQuery wide = vectorQuery(fixture, 6, 45.0f, 30.0f);

    // a graph search walks by the window it is given, so a narrower query must not be searched with
    // the wider query's window: each one has to answer what a search of itself answers
    List<IndexSearchResult> narrowAlone =
        DistributedIndexSearch.search(spark, fixture.metadata, fixture.table, narrow).hits();
    List<IndexSearchResult> wideAlone =
        DistributedIndexSearch.search(spark, fixture.metadata, fixture.table, wide).hits();
    // this fixture is chosen so the two windows really order the rows differently; it documents the
    // problem the per-query window fixes, not a property of the batch contract, so a change in the
    // graph library that made the two windows agree would mean new fixture vectors, not a bug
    assertThat(narrowAlone)
        .as("a wider HNSW walk finds a different second hit on this fixture")
        .isNotEqualTo(wideAlone.subList(0, narrow.topK()));

    List<DistributedIndexSearch.SearchOutcome> outcomes =
        DistributedIndexSearch.searchBatchWithOutcomes(
            spark, fixture.metadata, fixture.table, List.of(narrow, wide));

    assertThat(outcomes.get(0).hits()).containsExactlyElementsOf(narrowAlone);
    assertThat(outcomes.get(1).hits()).containsExactlyElementsOf(wideAlone);
  }

  @Test
  void queriesFromDifferentSnapshotsAreRejected() {
    Fixture fixture = vectorFixture(3);
    IndexQuery first = vectorQuery(fixture, 1, 0.0f, 0.0f);
    IndexQuery second =
        IndexQuery.of(
            first.sourceTableSnapshotId() + 1, fixture.adapter.encodeQuery(new float[] {0, 0}), 1);

    assertThatThrownBy(
            () ->
                DistributedIndexSearch.searchBatchWithOutcomes(
                    spark, fixture.metadata, fixture.table, List.of(first, second)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("same source table snapshot");
  }

  private Fixture vectorFixture(int entriesPerFile) {
    return vectorFixture(
        VectorIndexDefinition.VectorStructure.FLAT,
        0,
        entriesPerFile,
        TestBatchSearchOutcomes::vectorEntries);
  }

  private Fixture vectorFixture(
      VectorIndexDefinition.VectorStructure structure,
      int efSearch,
      int entriesPerFile,
      Function<VectorIndexAdapter, List<IndexEntry>> entries) {
    TestTables.TestTable table = createTable();
    VectorIndexDefinition.Builder definitionBuilder =
        VectorIndexDefinition.builder()
            .fieldId(1)
            .dimension(DIMENSION)
            .metric(VectorIndexDefinition.Metric.L2)
            .structure(structure);
    if (efSearch > 0) {
      // a small graph keeps the fixture fast; the window is what the test varies
      definitionBuilder.efSearch(efSearch).maxM(4).efConstruction(16);
    }

    VectorIndexDefinition definition = definitionBuilder.build();
    IndexMetadata metadata = indexMetadata(table, definition, "vector");
    VectorIndexAdapter adapter = new VectorIndexAdapter(definition);
    IndexMetadata built =
        new IndexBuilder(
                new IndexSnapshotWriter(table.io(), new PuffinIndexFileContainer(), entriesPerFile))
            .build(
                IndexBuildRequest.builder()
                    .currentMetadata(metadata)
                    .sourceTableSnapshotId(table.currentSnapshot().snapshotId())
                    .entries(entries.apply(adapter))
                    .timestampMillis(30L)
                    .build());
    return new Fixture(table, built, adapter);
  }

  private CorrelatedFixture correlatedFixture(int entriesPerFile) {
    TestTables.TestTable table = createTable();
    IndexMetadata metadata =
        indexMetadata(table, TestingCorrelatedIndexDefinition.of(4), "correlated");
    IndexMetadata built =
        new IndexBuilder(
                new IndexSnapshotWriter(table.io(), new PuffinIndexFileContainer(), entriesPerFile))
            .build(
                IndexBuildRequest.builder()
                    .currentMetadata(metadata)
                    .sourceTableSnapshotId(table.currentSnapshot().snapshotId())
                    .entries(
                        List.of(
                            IndexEntry.of("a.parquet", 0, TestingIndexAdapter.valuePayload(10)),
                            IndexEntry.of("a.parquet", 1, TestingIndexAdapter.valuePayload(30)),
                            IndexEntry.of("b.parquet", 0, TestingIndexAdapter.valuePayload(20)),
                            IndexEntry.of("b.parquet", 1, TestingIndexAdapter.valuePayload(40))))
                    .timestampMillis(30L)
                    .build());
    return new CorrelatedFixture(table, built);
  }

  private TestTables.TestTable createTable() {
    String tableName = "batch-outcomes-" + SEQUENCE.incrementAndGet();
    TestTables.TestTable table =
        TestTables.create(
            temp.toFile(),
            tableName,
            SCHEMA,
            PartitionSpec.unpartitioned(),
            SortOrder.unsorted(),
            2,
            new TestTables.TestTableOperations(
                tableName, temp.toFile(), new TestTables.LocalFileIO()));
    table
        .newAppend()
        .appendFile(
            DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath(temp.resolve("data-1.parquet").toString())
                .withFileSizeInBytes(10L)
                .withRecordCount(1L)
                .build())
        .commit();
    return table;
  }

  private IndexMetadata indexMetadata(
      TestTables.TestTable table, IndexDefinition definition, String indexUuid) {
    return IndexMetadata.builder()
        .indexUuid(indexUuid)
        .tableUuid(table.uuid().toString())
        .tableLocation(table.location())
        .location(temp.resolve("indexes/" + indexUuid).toString())
        .definition(definition)
        .build();
  }

  /**
   * Nine entries over three data files and three rows each. Built three to a file, the index has
   * three index files, and the vectors make several rows tie on distance: the entries of a.parquet
   * are the duplicates of the query axes, and b.parquet and c.parquet add more of them.
   */
  private static List<IndexEntry> vectorEntries(VectorIndexAdapter adapter) {
    return List.of(
        IndexEntry.of("a.parquet", 0, adapter.encodeVector(new float[] {1.0f, 0.0f})),
        IndexEntry.of("a.parquet", 1, adapter.encodeVector(new float[] {0.0f, 1.0f})),
        IndexEntry.of("a.parquet", 2, adapter.encodeVector(new float[] {2.0f, 0.0f})),
        IndexEntry.of("b.parquet", 0, adapter.encodeVector(new float[] {1.0f, 0.0f})),
        IndexEntry.of("b.parquet", 1, adapter.encodeVector(new float[] {0.0f, 0.0f})),
        IndexEntry.of("b.parquet", 2, adapter.encodeVector(new float[] {3.0f, 0.0f})),
        IndexEntry.of("c.parquet", 0, adapter.encodeVector(new float[] {0.0f, 2.0f})),
        IndexEntry.of("c.parquet", 1, adapter.encodeVector(new float[] {1.0f, 0.0f})),
        IndexEntry.of("c.parquet", 2, adapter.encodeVector(new float[] {0.0f, 1.0f})));
  }

  private static IndexQuery vectorQuery(Fixture fixture, int topK, float... vector) {
    return IndexQuery.of(
        fixture.table.currentSnapshot().snapshotId(), fixture.adapter.encodeQuery(vector), topK);
  }

  private static IndexQuery correlatedQuery(CorrelatedFixture fixture, int topK) {
    return IndexQuery.of(
        fixture.table.currentSnapshot().snapshotId(), ByteBuffer.allocate(4), topK);
  }

  private record Fixture(
      TestTables.TestTable table, IndexMetadata metadata, VectorIndexAdapter adapter) {}

  private record CorrelatedFixture(TestTables.TestTable table, IndexMetadata metadata) {}
}
