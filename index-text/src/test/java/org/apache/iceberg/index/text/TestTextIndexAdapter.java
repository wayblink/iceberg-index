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
package org.apache.iceberg.index.text;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.data.Offset.offset;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.index.IndexEntry;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.IndexMetadataParser;
import org.apache.iceberg.index.IndexSearchResult;
import org.apache.iceberg.index.IndexTypes;
import org.apache.iceberg.index.PuffinIndexFileContainer;
import org.apache.iceberg.index.text.TextIndexAdapter.CorpusCounts;
import org.apache.iceberg.index.text.TextIndexAdapter.CorpusStatistics;
import org.apache.iceberg.inmemory.InMemoryInputFile;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Unit tests of the BM25 text index adapter. */
class TestTextIndexAdapter {

  @BeforeAll
  static void registerTextIndexType() {
    TextIndexTypes.register();
  }

  @Test
  void snapshotStatisticsScoreWithLongDocumentFrequency() throws IOException {
    TextIndexDefinition definition = TextIndexDefinition.builder().fieldId(1).build();
    TextIndexAdapter adapter = new TextIndexAdapter(definition);
    CorpusCounts first =
        new CorpusCounts(
            Integer.MAX_VALUE, Integer.MAX_VALUE, Map.of("ice", (long) Integer.MAX_VALUE));
    CorpusCounts second = new CorpusCounts(2, 2, Map.of("ice", 1L, "snow", 1L));
    CorpusCounts merged = first.merge(second);
    long documents = (long) Integer.MAX_VALUE + 2;
    long frequency = documents - 1;
    InputFile input = new InMemoryInputFile(adapter.encodeSnapshotStatistics(merged).array());
    CorpusStatistics statistics = adapter.snapshotStatistics(input, Set.of("ice"));
    assertThat(statistics.totalDocuments()).isEqualTo(documents);
    assertThat(statistics.documentFrequency("ice")).isEqualTo(frequency);
    assertThat(statistics.documentFrequency("snow")).isZero();

    double expectedScore = Math.log1p((documents - frequency + 0.5) / (frequency + 0.5));
    assertThat(TextIndexAdapter.score(definition, statistics, "ice", "ice"))
        .isCloseTo(expectedScore, offset(1e-18));

    InMemoryOutputFile shard = new InMemoryOutputFile();
    ByteBuffer payload = adapter.encodeAuxiliaryPayload(List.of(entry("ice.parquet", 0, "ice")));
    new PuffinIndexFileContainer().write(shard, List.of(adapter.auxiliaryBlob(payload)));
    List<IndexSearchResult> hits =
        adapter
            .searchShardBatch(
                shard.toInputFile(),
                List.of(adapter.encodeQuery("ice").array()),
                new int[] {1},
                statistics,
                null)
            .get(0);
    assertThat(hits)
        .singleElement()
        .satisfies(hit -> assertThat(hit.score()).isCloseTo(expectedScore, offset(1e-18)));
  }

  @Test
  void searchStatisticsMergeLongDocumentFrequency() {
    TextIndexAdapter adapter =
        new TextIndexAdapter(TextIndexDefinition.builder().fieldId(1).build());
    ByteBuffer large =
        adapter.encodeSnapshotStatistics(
            new CorpusCounts(
                Integer.MAX_VALUE, Integer.MAX_VALUE, Map.of("ice", (long) Integer.MAX_VALUE)));
    ByteBuffer small = adapter.encodeSnapshotStatistics(new CorpusCounts(1, 1, Map.of("ice", 1L)));
    CorpusStatistics statistics =
        (CorpusStatistics) adapter.searchStatistics(List.of(large, small));
    assertThat(statistics.documentFrequency("ice")).isEqualTo((long) Integer.MAX_VALUE + 1);
  }

  @Test
  void rejectsOversizedShardDocumentCount() {
    TextIndexAdapter adapter =
        new TextIndexAdapter(TextIndexDefinition.builder().fieldId(1).build());
    long count = (long) Integer.MAX_VALUE + 1;
    ByteBuffer payload =
        adapter.encodeSnapshotStatistics(new CorpusCounts(count, count, Map.of("ice", count)));
    assertThatThrownBy(
            () -> adapter.searchAuxiliaryAll(List.of(payload), adapter.encodeQuery("ice"), 1))
        .isInstanceOf(UncheckedIOException.class)
        .hasCauseInstanceOf(IOException.class)
        .hasRootCauseMessage("Invalid BM25 shard document count");
  }

  @Test
  void readsOnlyRequestedPostingRanges() throws IOException {
    TextIndexAdapter adapter =
        new TextIndexAdapter(TextIndexDefinition.builder().fieldId(1).build());
    List<IndexEntry> entries = new ArrayList<>();
    // three document table chunks, one document with a rare term at the start and one with a rare
    // term at the end: a query for either has to read one chunk, not the whole table
    entries.add(entry("first.parquet", 0, "ice iceberg earliest"));
    entries.add(entry("other.parquet", 1, "ice ice iceberg glacier"));
    for (int row = 0; row < 12_300; row++) {
      entries.add(entry("many.parquet", row, "common"));
    }
    entries.add(entry("last.parquet", 0, "latest"));
    ByteBuffer payload = adapter.encodeAuxiliaryPayload(entries);

    TextIndexAdapter.FileIndex selected =
        TextIndexPayloadV2.open(
                TextIndexPayloadV2.source(new RangeOnlyStream(payload), 0), payload.remaining())
            .shard(Set.of("ice", "glacier"), false);
    assertThat(selected.postingsByTerm).containsOnlyKeys("ice", "glacier");

    long rare = bytesReadByQuery(payload, "earliest");
    long common = bytesReadByQuery(payload, "common");
    // the document table is read by chunk: a term of one document pays for one chunk while a term
    // in every document may pay for the whole table, and both stay below the payload they look at
    assertThat(rare).isLessThan(common);
    assertThat(common).isLessThan(payload.remaining());
  }

  private static long bytesReadByQuery(ByteBuffer payload, String term) throws IOException {
    RangeOnlyStream stream = new RangeOnlyStream(payload);
    TextIndexPayloadV2.Reader reader =
        TextIndexPayloadV2.open(TextIndexPayloadV2.source(stream, 0), payload.remaining());
    reader.shard(Set.of(term), false);
    return stream.bytesRead;
  }

  private static final class RangeOnlyStream extends SeekableInputStream implements RangeReadable {
    private final ByteBuffer bytes;
    private long bytesRead;

    private RangeOnlyStream(ByteBuffer bytes) {
      this.bytes = bytes;
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) {
      bytes.slice(Math.toIntExact(position), length).get(buffer, offset, length);
      bytesRead += length;
    }

    @Override
    public int readTail(byte[] buffer, int offset, int length) {
      throw new UnsupportedOperationException("Only positional reads are expected");
    }

    @Override
    public int read() {
      throw new UnsupportedOperationException("Only positional reads are expected");
    }

    @Override
    public long getPos() {
      throw new UnsupportedOperationException("Only positional reads are expected");
    }

    @Override
    public void seek(long newPos) {
      throw new UnsupportedOperationException("Only positional reads are expected");
    }
  }

  @Test
  public void testAnalyze() {
    TextIndexDefinition definition = TextIndexDefinition.builder().fieldId(1).build();
    assertThat(definition.analyze("Iceberg Vector Search"))
        .containsExactly("iceberg", "vector", "search");
    // punctuation and digits split tokens; short tokens are dropped by minTokenLength
    assertThat(definition.analyze("a table-2 index, x9")).containsExactly("table", "index", "x9");
    // stopwords are dropped
    TextIndexDefinition withStopwords =
        TextIndexDefinition.builder().fieldId(1).stopword("the").stopword("and").build();
    assertThat(withStopwords.analyze("the iceberg and the index"))
        .containsExactly("iceberg", "index");
    // case sensitivity off keeps case
    TextIndexDefinition caseSensitive =
        TextIndexDefinition.builder().fieldId(1).lowercase(false).build();
    assertThat(caseSensitive.analyze("Iceberg iceberg")).containsExactly("Iceberg", "iceberg");
    assertThat(definition.analyze(null)).isEmpty();
  }

  @Test
  public void testScoresMatchHandComputedBm25() {
    // a tiny corpus where every quantity can be computed by hand:
    //   doc 1: "ice iceberg"      (2 tokens)
    //   doc 2: "ice ice iceberg"  (3 tokens)
    //   doc 3: "vector search"    (2 tokens)
    // N = 3, total length = 7, avgdl = 7/3
    TextIndexDefinition definition =
        TextIndexDefinition.builder().fieldId(1).k1(1.2).lengthNorm(0.75).build();
    TextIndexAdapter adapter = new TextIndexAdapter(definition);

    ByteBuffer payload =
        adapter.encodeAuxiliaryPayload(
            List.of(
                entry("data-1.parquet", 0, "ice iceberg"),
                entry("data-1.parquet", 1, "ice ice iceberg"),
                entry("data-2.parquet", 0, "vector search")));

    List<IndexSearchResult> results =
        adapter.searchAuxiliaryAll(List.of(payload), adapter.encodeQuery("ice"), 3);

    // df(ice) = 2, idf = ln(1 + (3 - 2 + 0.5) / (2 + 0.5))
    double idf = Math.log(1.0 + 1.5 / 2.5);
    double averageLength = 7.0 / 3.0;
    // doc 2: tf = 2, length 3
    double doc2 = idf * 2 * (1.2 + 1.0) / (2 + 1.2 * (1.0 - 0.75 + 0.75 * 3.0 / averageLength));
    // doc 1: tf = 1, length 2
    double doc1 = idf * 1 * (1.2 + 1.0) / (1 + 1.2 * (1.0 - 0.75 + 0.75 * 2.0 / averageLength));

    assertThat(results).hasSize(2);
    assertThat(results.get(0).dataFile()).isEqualTo("data-1.parquet");
    assertThat(results.get(0).rowPosition()).isEqualTo(1);
    assertThat(results.get(0).score()).isCloseTo(doc2, org.assertj.core.data.Offset.offset(1e-9));
    assertThat(results.get(1).rowPosition()).isEqualTo(0);
    assertThat(results.get(1).score()).isCloseTo(doc1, org.assertj.core.data.Offset.offset(1e-9));
  }

  @Test
  public void testStatisticsMergeAcrossPayloads() {
    // the same corpus split over two index files must score exactly like the single-file index
    TextIndexDefinition definition = TextIndexDefinition.builder().fieldId(1).build();
    TextIndexAdapter adapter = new TextIndexAdapter(definition);

    List<IndexEntry> firstEntries =
        List.of(
            entry("data-1.parquet", 0, "ice iceberg"),
            entry("data-1.parquet", 1, "ice ice iceberg"));
    List<IndexEntry> secondEntries = List.of(entry("data-2.parquet", 0, "vector search"));

    ByteBuffer single = adapter.encodeAuxiliaryPayload(allEntries());
    List<IndexSearchResult> together =
        adapter.searchAuxiliaryAll(
            List.of(
                adapter.encodeAuxiliaryPayload(firstEntries),
                adapter.encodeAuxiliaryPayload(secondEntries)),
            adapter.encodeQuery("ice"),
            3);
    List<IndexSearchResult> alone =
        adapter.searchAuxiliaryAll(List.of(single), adapter.encodeQuery("ice"), 3);

    assertThat(together).hasSameSizeAs(alone);
    for (int i = 0; i < alone.size(); i++) {
      assertThat(together.get(i).dataFile()).isEqualTo(alone.get(i).dataFile());
      assertThat(together.get(i).rowPosition()).isEqualTo(alone.get(i).rowPosition());
      assertThat(together.get(i).score())
          .isCloseTo(alone.get(i).score(), org.assertj.core.data.Offset.offset(1e-9));
    }
  }

  @Test
  public void testQueryTermsAndTieBreaking() {
    TextIndexDefinition definition = TextIndexDefinition.builder().fieldId(1).build();
    TextIndexAdapter adapter = new TextIndexAdapter(definition);
    ByteBuffer payload =
        adapter.encodeAuxiliaryPayload(
            List.of(
                entry("data-1.parquet", 0, "alpha beta"),
                entry("data-1.parquet", 1, "alpha beta")));

    // both documents have identical content: ties break on the row reference
    List<IndexSearchResult> results =
        adapter.searchAuxiliaryAll(List.of(payload), adapter.encodeQuery("alpha"), 5);
    assertThat(results).hasSize(2);
    assertThat(results.get(0).rowPosition()).isEqualTo(0);
    assertThat(results.get(1).rowPosition()).isEqualTo(1);
    assertThat(results.get(0).score()).isEqualTo(results.get(1).score());

    // an unknown query term matches nothing
    assertThat(adapter.searchAuxiliaryAll(List.of(payload), adapter.encodeQuery("zzz"), 5))
        .isEmpty();
    // k = 0 returns nothing
    assertThat(adapter.searchAuxiliaryAll(List.of(payload), adapter.encodeQuery("alpha"), 0))
        .isEmpty();
    // repeated query terms contribute once per occurrence
    List<IndexSearchResult> repeated =
        adapter.searchAuxiliaryAll(List.of(payload), adapter.encodeQuery("alpha alpha"), 1);
    assertThat(repeated.get(0).score())
        .isCloseTo(2 * results.get(0).score(), org.assertj.core.data.Offset.offset(1e-9));
  }

  @Test
  public void testDefinitionRoundTripThroughParser() {
    IndexTypes.register(new TextIndexType());
    IndexMetadata metadata =
        IndexMetadata.builder()
            .indexUuid("index-uuid")
            .tableUuid("table-uuid")
            .tableLocation("/tmp/table")
            .location("/tmp/table/indexes/index-uuid")
            .definition(
                TextIndexDefinition.builder()
                    .fieldId(7)
                    .k1(1.4)
                    .lengthNorm(0.8)
                    .minTokenLength(3)
                    .lowercase(false)
                    .stopword("the")
                    .property("owner", "search-team")
                    .build())
            .build();

    IndexMetadata parsed = IndexMetadataParser.fromJson(IndexMetadataParser.toJson(metadata));
    assertThat(parsed).isEqualTo(metadata);
    TextIndexDefinition definition = (TextIndexDefinition) parsed.definition();
    assertThat(definition.fieldId()).isEqualTo(7);
    assertThat(definition.k1()).isEqualTo(1.4);
    assertThat(definition.lengthNorm()).isEqualTo(0.8);
    assertThat(definition.minTokenLength()).isEqualTo(3);
    assertThat(definition.lowercase()).isFalse();
    assertThat(definition.stopwords()).containsExactly("the");
    assertThat(definition.properties()).containsEntry("owner", "search-team");
  }

  @Test
  public void testPayloadValidation() {
    TextIndexAdapter adapter =
        new TextIndexAdapter(TextIndexDefinition.builder().fieldId(1).build());
    assertThatThrownBy(
            () ->
                adapter.searchAuxiliaryAll(
                    List.of(
                        ByteBuffer.wrap(
                            "NOTMAGIC".getBytes(java.nio.charset.StandardCharsets.US_ASCII))),
                    adapter.encodeQuery("ice"),
                    1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Not a text index payload");
  }

  private static List<IndexEntry> allEntries() {
    return List.of(
        entry("data-1.parquet", 0, "ice iceberg"),
        entry("data-1.parquet", 1, "ice ice iceberg"),
        entry("data-2.parquet", 0, "vector search"));
  }

  private static IndexEntry entry(String dataFile, long position, String text) {
    TextIndexDefinition definition = TextIndexDefinition.builder().fieldId(1).build();
    return IndexEntry.of(dataFile, position, new TextIndexAdapter(definition).encodeDocument(text));
  }
}
