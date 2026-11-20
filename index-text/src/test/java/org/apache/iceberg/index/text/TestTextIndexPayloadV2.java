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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.index.IndexSearchResult;
import org.apache.iceberg.index.text.TextIndexAdapter.CorpusCounts;
import org.apache.iceberg.index.text.TextIndexAdapter.Document;
import org.apache.iceberg.index.text.TextIndexAdapter.FileIndex;
import org.apache.iceberg.index.text.TextIndexAdapter.Posting;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;

/**
 * Unit tests of the BM25 v2 payload layout: what it stores, that it agrees with the v1 layout it
 * replaces, and that a corrupted payload is rejected instead of answered.
 */
class TestTextIndexPayloadV2 {
  // header offsets: magic, documents, tokens and the five ints before the dictionary start
  private static final int DICTIONARY_START_OFFSET =
      Integer.BYTES + 2 * Long.BYTES + 5 * Integer.BYTES;
  private static final int POSTINGS_START_OFFSET = DICTIONARY_START_OFFSET + Integer.BYTES;
  private static final int BUCKETS_OFFSET = POSTINGS_START_OFFSET + Integer.BYTES;
  private static final int BUCKET_COUNT = 64;

  @Test
  void v1AndV2EncodeTheSameShard() throws IOException {
    Corpus corpus = corpus();
    FileIndex v1 = decodeV1(encodeV1(corpus));
    FileIndex v2 = decodeV2(encodeV2(corpus));

    assertThat(v2.localDocuments).isEqualTo(v1.localDocuments).isEqualTo(corpus.documents());
    assertThat(v2.localTotalLength).isEqualTo(v1.localTotalLength).isEqualTo(corpus.tokens);
    assertThat(v2.localDocumentFrequency)
        .isEqualTo(v1.localDocumentFrequency)
        .isEqualTo(corpus.documentFrequency());
    assertThat(v2.postingsByTerm.keySet())
        .containsExactlyInAnyOrderElementsOf(v1.postingsByTerm.keySet());

    for (String term : corpus.terms()) {
      assertThat(v2.postingsByTerm.get(term)).hasSameSizeAs(v1.postingsByTerm.get(term));
      assertThat(postingRefs(v2, term))
          .as("postings of %s", term)
          .isEqualTo(postingRefs(v1, term))
          .isEqualTo(corpus.postingRefs(term));
    }

    assertThat(v2.documents).hasSameSizeAs(v1.documents);
    for (int document = 0; document < v1.documents.size(); document++) {
      Document left = v1.documents.get(document);
      Document right = v2.documents.get(document);
      assertThat(right.dataFile).as("data file of document %d", document).isEqualTo(left.dataFile);
      assertThat(right.rowPosition)
          .as("row position of document %d", document)
          .isEqualTo(left.rowPosition);
      assertThat(right.length).as("length of document %d", document).isEqualTo(left.length);
    }
  }

  @Test
  void v1AndV2EncodeTheSameTermScopedShard() throws IOException {
    Corpus corpus = corpus();
    Set<String> terms = Set.of("ice", "glacier");
    FileIndex v1 = shardV1(encodeV1(corpus), terms);
    FileIndex v2 = shardV2(encodeV2(corpus), terms);

    // only the requested terms are decoded, each with the postings the corpus has for it
    assertThat(v2.postingsByTerm.keySet()).containsExactlyInAnyOrderElementsOf(terms);
    assertThat(v1.postingsByTerm.keySet()).containsExactlyInAnyOrderElementsOf(terms);
    for (String term : terms) {
      assertThat(postingRefs(v2, term))
          .isEqualTo(postingRefs(v1, term))
          .isEqualTo(corpus.postingRefs(term));
      assertThat(v2.localDocumentFrequency.get(term))
          .isEqualTo(v1.localDocumentFrequency.get(term))
          .isEqualTo(corpus.documentFrequency().get(term));
    }

    // the document table is only read for the documents the requested postings reference
    assertThat(v2.documents).hasSameSizeAs(v1.documents);
  }

  @Test
  void adapterAnswersTheSameForV1AndV2Payloads() throws IOException {
    TextIndexAdapter adapter =
        new TextIndexAdapter(TextIndexDefinition.builder().fieldId(1).build());
    // a corpus small enough to rank by hand: the first two documents are identical, so they tie
    // on score and break the tie on the row reference
    Corpus corpus =
        new Corpus()
            .document("data-1.parquet", 0, List.of("ice", "iceberg"))
            .document("data-1.parquet", 1, List.of("ice", "iceberg"))
            .document("data-1.parquet", 2, List.of("ice", "ice", "iceberg"))
            .document("data-2.parquet", 0, List.of("vector", "search"));

    List<IndexSearchResult> v1 =
        adapter.searchAuxiliaryAll(List.of(encodeV1(corpus)), adapter.encodeQuery("ice"), 5);
    List<IndexSearchResult> v2 =
        adapter.searchAuxiliaryAll(List.of(encodeV2(corpus)), adapter.encodeQuery("ice"), 5);

    // term frequency decides first, then the tie, and both layouts have to produce that order
    assertThat(v2).hasSize(3);
    assertThat(v2.stream().map(IndexSearchResult::rowPosition)).containsExactly(2L, 0L, 1L);
    assertThat(v2).hasSameSizeAs(v1);
    for (int hit = 0; hit < v1.size(); hit++) {
      assertThat(v2.get(hit).dataFile()).isEqualTo(v1.get(hit).dataFile());
      assertThat(v2.get(hit).rowPosition()).isEqualTo(v1.get(hit).rowPosition());
      assertThat(v2.get(hit).score()).as("score of hit %d", hit).isEqualTo(v1.get(hit).score());
    }

    assertThat(v2.get(1).score()).isEqualTo(v2.get(2).score());
  }

  @Test
  void v1AndV2EncodeTheSameStatisticsDictionary() throws IOException {
    Corpus corpus = corpus();
    CorpusCounts counts = corpus.counts();
    ByteBuffer v1 = TextIndexPayload.encode(counts, null);
    ByteBuffer v2 = TextIndexPayloadV2.encode(counts, null);

    Set<String> terms = Set.of("ice", "vector");
    CorpusCounts v1Counts = countsV1(v1, terms);
    CorpusCounts v2Counts = countsV2(v2, terms);
    assertThat(v2Counts.documents).isEqualTo(v1Counts.documents).isEqualTo(counts.documents);
    assertThat(v2Counts.tokens).isEqualTo(v1Counts.tokens).isEqualTo(counts.tokens);
    assertThat(v2Counts.documentFrequency)
        .isEqualTo(v1Counts.documentFrequency)
        .containsOnlyKeys(terms.toArray(String[]::new));
    terms.forEach(
        term ->
            assertThat(v2Counts.documentFrequency.get(term))
                .isEqualTo(counts.documentFrequency.get(term)));

    // a null term set reads every term of the dictionary
    assertThat(countsV2(v2, null).documentFrequency)
        .isEqualTo(countsV1(v1, null).documentFrequency)
        .isEqualTo(counts.documentFrequency);
    // a term the dictionary does not hold reads as absent, it does not fail
    assertThat(countsV2(v2, Set.of("absent")).documentFrequency).isEmpty();

    // the statistics-only shard reads the same header and no postings or documents
    FileIndex statistics = shardV2(v2, null, true);
    assertThat(statistics.localDocuments).isEqualTo(counts.documents);
    assertThat(statistics.localTotalLength).isEqualTo(counts.tokens);
    assertThat(statistics.localDocumentFrequency).isEqualTo(counts.documentFrequency);
    assertThat(statistics.postingsByTerm).isEmpty();
    assertThat(statistics.documents).isEmpty();
  }

  @Test
  void rejectsCorruptStatisticsDictionary() throws IOException {
    // the trailing varint of the last dictionary entry never ends, which is what a payload cut
    // inside an entry looks like once the header bounds still fit
    byte[] payload = bytes(TextIndexPayloadV2.encode(corpus().counts(), null));
    payload[payload.length - 1] = (byte) 0xFF;
    assertThatThrownBy(() -> openV2(payload, payload.length).counts(null))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("Truncated BM25 v2 varint");
    assertThatThrownBy(() -> openV2(payload, payload.length).shard(null, true))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("Truncated BM25 v2 varint");
  }

  @Test
  void emptyShardRoundTrips() throws IOException {
    CorpusCounts counts = new CorpusCounts(0, 0, Map.of());
    FileIndex empty = new FileIndex(0, 0, Map.of(), Map.of(), List.of());

    FileIndex shard = decodeV2(TextIndexPayloadV2.encode(counts, empty));
    assertThat(shard.localDocuments).isZero();
    assertThat(shard.localTotalLength).isZero();
    assertThat(shard.localDocumentFrequency).isEmpty();
    assertThat(shard.postingsByTerm).isEmpty();
    assertThat(shard.documents).isEmpty();

    CorpusCounts read = countsV2(TextIndexPayloadV2.encode(counts, null), null);
    assertThat(read.documents).isZero();
    assertThat(read.tokens).isZero();
    assertThat(read.documentFrequency).isEmpty();
  }

  @Test
  void singleDocumentRareTermRoundTrips() throws IOException {
    Corpus corpus = new Corpus().document("only.parquet", 0, List.of("rare"));
    FileIndex decoded = decodeV2(encodeV2(corpus));

    assertThat(decoded.localDocuments).isEqualTo(1);
    assertThat(decoded.localTotalLength).isEqualTo(1);
    assertThat(decoded.localDocumentFrequency)
        .containsExactlyInAnyOrderEntriesOf(Map.of("rare", 1L));
    assertThat(postingRefs(decoded, "rare")).containsExactly("only.parquet@0/1#1");
  }

  @Test
  void documentTableChunkBoundaryRoundTrips() throws IOException {
    // one full chunk
    Corpus exact = chunkedCorpus(TextIndexPayloadV2.CHUNK_DOCS);
    FileIndex exactShard = shardV2(encodeV2(exact), Set.of("shared"));
    assertThat(exactShard.localDocuments).isEqualTo(TextIndexPayloadV2.CHUNK_DOCS);
    assertThat(exactShard.postingsByTerm.get("shared")).hasSize(TextIndexPayloadV2.CHUNK_DOCS);
    assertThat(postingRefs(exactShard, "shared")).isEqualTo(exact.postingRefs("shared"));

    // a full chunk plus a short tail chunk: a term only the tail document holds must read it
    Corpus tail = chunkedCorpus(TextIndexPayloadV2.CHUNK_DOCS + 1);
    FileIndex tailShard = shardV2(encodeV2(tail), Set.of("shared", "tail"));
    assertThat(tailShard.localDocuments).isEqualTo(TextIndexPayloadV2.CHUNK_DOCS + 1);
    assertThat(postingRefs(tailShard, "shared")).isEqualTo(tail.postingRefs("shared"));
    assertThat(postingRefs(tailShard, "tail")).containsExactly("tail.parquet@4096/2#1");
  }

  @Test
  void dataFileTableMapsDocumentsBack() throws IOException {
    // the file table is written once and a document carries its id, so an interleaved set of files
    // must come back exactly, including the file of every posting's document
    Corpus corpus =
        new Corpus()
            .document("third.parquet", 3, List.of("alpha", "beta"))
            .document("first.parquet", 0, List.of("alpha"))
            .document("second.parquet", 1, List.of("beta", "beta", "gamma"));
    FileIndex decoded = decodeV2(encodeV2(corpus));

    assertThat(postingRefs(decoded, "alpha"))
        .containsExactly("third.parquet@3/2#1", "first.parquet@0/1#1");
    assertThat(postingRefs(decoded, "beta"))
        .containsExactly("third.parquet@3/2#1", "second.parquet@1/3#2");
    assertThat(postingRefs(decoded, "gamma")).containsExactly("second.parquet@1/3#1");
  }

  @Test
  void largeRowPositionRoundTrips() throws IOException {
    long large = 1L << 33;
    long larger = (1L << 40) + 17;
    Corpus corpus =
        new Corpus()
            .document("huge.parquet", large, List.of("ice"))
            .document("huge.parquet", larger, List.of("ice", "ice"));
    FileIndex decoded = decodeV2(encodeV2(corpus));

    assertThat(postingRefs(decoded, "ice"))
        .containsExactly("huge.parquet@" + large + "/1#1", "huge.parquet@" + larger + "/2#2");
    // v1 writes the row position as a fixed long and must agree
    assertThat(postingRefs(decodeV1(encodeV1(corpus)), "ice"))
        .isEqualTo(postingRefs(decoded, "ice"));
  }

  @Test
  void frontCodedTermsRoundTrip() throws IOException {
    // a bucket's first term is written without a shared prefix; later terms of the same bucket
    // share a prefix with the previous term, which is where front coding can lose a term
    List<String> terms = Lists.newArrayList("a", "ab", "abc", "abd");
    terms.addAll(sameBucket("ice", 4));

    Corpus corpus = new Corpus();
    long row = 0;
    for (String term : terms) {
      corpus.document("data.parquet", row++, List.of(term));
    }
    FileIndex decoded = decodeV2(encodeV2(corpus));
    assertThat(decoded.localDocumentFrequency.keySet()).containsExactlyInAnyOrderElementsOf(terms);
    for (String term : terms) {
      assertThat(postingRefs(decoded, term)).as("posting of %s", term).hasSize(1);
    }

    // the terms above must span more than one bucket, otherwise the first-term case is not read
    assertThat(terms.stream().map(TestTextIndexPayloadV2::bucketOf).distinct().count())
        .isGreaterThan(1L);
    // and the front coded family must sit in one bucket, sharing a prefix with the previous term
    assertThat(sameBucket("ice", 4).stream().map(TestTextIndexPayloadV2::bucketOf).distinct())
        .hasSize(1);
  }

  @Test
  void unicodeTermsRoundTrip() throws IOException {
    Corpus corpus =
        new Corpus()
            .document("utf.parquet", 0, List.of("雪", "雪山", "café"))
            .document("utf.parquet", 1, List.of("❄️", "雪", "😀😀"));
    FileIndex decoded = decodeV2(encodeV2(corpus));

    assertThat(decoded.localDocumentFrequency.keySet())
        .containsExactlyInAnyOrderElementsOf(List.of("雪", "雪山", "café", "❄️", "😀😀"));
    assertThat(postingRefs(decoded, "雪")).containsExactly("utf.parquet@0/3#1", "utf.parquet@1/3#1");
    assertThat(postingRefs(decoded, "😀😀")).containsExactly("utf.parquet@1/3#1");
    assertThat(postingRefs(decoded, "❄️")).containsExactly("utf.parquet@1/3#1");
  }

  @Test
  void termLongerThanWriteUtfLimitRoundTrips() throws IOException {
    // 70000 bytes: past the 65535 byte limit of the modified UTF-8 a v1 dictionary term uses
    String term = "x".repeat(70_000);
    Corpus corpus = new Corpus().document("long.parquet", 5, List.of(term));
    FileIndex decoded = decodeV2(encodeV2(corpus));

    assertThat(decoded.localDocumentFrequency).containsExactlyInAnyOrderEntriesOf(Map.of(term, 1L));
    assertThat(postingRefs(decoded, term)).containsExactly("long.parquet@5/1#1");
    assertThat(countsV2(TextIndexPayloadV2.encode(corpus.counts(), null), null).documentFrequency)
        .containsExactlyInAnyOrderEntriesOf(Map.of(term, 1L));
  }

  @Test
  void fullShardReadMatchesScopedRead() throws IOException {
    Corpus corpus = corpus();
    FileIndex everyTerm = decodeV2(encodeV2(corpus));
    FileIndex scoped = shardV2(encodeV2(corpus), corpus.terms());
    assertThat(everyTerm.postingsByTerm.keySet())
        .containsExactlyInAnyOrderElementsOf(scoped.postingsByTerm.keySet());
    for (String term : scoped.postingsByTerm.keySet()) {
      assertThat(postingRefs(everyTerm, term)).isEqualTo(postingRefs(scoped, term));
    }

    assertThat(everyTerm.documents).hasSameSizeAs(scoped.documents);
  }

  @Test
  void matchesOnlyItsOwnSignature() throws IOException {
    ByteBuffer v2 = encodeV2(corpus());
    assertThat(TextIndexPayloadV2.matches(v2)).isTrue();
    assertThat(TextIndexPayloadV2.matches(encodeV1(corpus()))).isFalse();
    assertThat(TextIndexPayloadV2.matches(ByteBuffer.wrap(new byte[] {0x42, 0x4D, 0x32})))
        .isFalse();
  }

  @Test
  void rejectsForeignMagic() throws IOException {
    byte[] v1 = bytes(encodeV1(corpus()));
    assertThatThrownBy(() -> TextIndexPayloadV2.open(sourceOf(v1), v1.length))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Not a BM25 v2 payload");

    byte[] corrupt = bytes(encodeV2(corpus()));
    corrupt[0] = (byte) 0x00;
    assertThatThrownBy(() -> TextIndexPayloadV2.open(sourceOf(corrupt), corrupt.length))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Not a BM25 v2 payload");
  }

  @Test
  void rejectsTruncatedPayload() throws IOException {
    byte[] payload = bytes(encodeV2(corpus()));
    assertThatThrownBy(() -> TextIndexPayloadV2.open(sourceOf(payload), 3))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("Truncated BM25 v2 signature");
    assertThatThrownBy(() -> TextIndexPayloadV2.open(sourceOf(payload), Integer.BYTES))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("Truncated BM25 v2 header");
    // a size the header disagrees with: the header names offsets past the end
    assertThatThrownBy(
            () -> TextIndexPayloadV2.open(sourceOf(payload), TextIndexPayloadV2.HEADER_SIZE + 4))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("bucket offset");
  }

  @Test
  void rejectsCorruptBucketOffsets() throws IOException {
    assertThat(BUCKETS_OFFSET + (BUCKET_COUNT + 1) * Integer.BYTES)
        .isEqualTo(TextIndexPayloadV2.HEADER_SIZE);

    byte[] payload = bytes(encodeV2(corpus()));
    putInt(payload, BUCKETS_OFFSET + 8 * Integer.BYTES, intAt(payload, BUCKETS_OFFSET) - 1);
    assertThatThrownBy(() -> TextIndexPayloadV2.open(sourceOf(payload), payload.length))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("bucket offset");

    // an offset past the end of the payload
    byte[] past = bytes(encodeV2(corpus()));
    putInt(past, BUCKETS_OFFSET + BUCKET_COUNT * Integer.BYTES, past.length + 1);
    assertThatThrownBy(() -> TextIndexPayloadV2.open(sourceOf(past), past.length))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("bucket offset");

    // a header that disagrees with the bucket offsets it should name
    byte[] dictionaryStart = bytes(encodeV2(corpus()));
    putInt(
        dictionaryStart,
        DICTIONARY_START_OFFSET,
        intAt(dictionaryStart, DICTIONARY_START_OFFSET) + 4);
    assertThatThrownBy(
            () -> TextIndexPayloadV2.open(sourceOf(dictionaryStart), dictionaryStart.length))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("dictionary start");

    byte[] postingsStart = bytes(encodeV2(corpus()));
    putInt(postingsStart, POSTINGS_START_OFFSET, intAt(postingsStart, POSTINGS_START_OFFSET) + 4);
    assertThatThrownBy(() -> TextIndexPayloadV2.open(sourceOf(postingsStart), postingsStart.length))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("postings start");
  }

  @Test
  void rejectsPostingRangePastEnd() throws IOException {
    byte[] payload = bytes(encodeV2(corpus()));
    // the last posting range of the payload no longer fits, so reading the dictionary must fail
    // rather than answer with the terms that still fit
    // the postings are written in sorted term order, so the last byte belongs to "vector"
    assertThatThrownBy(
            () ->
                TextIndexPayloadV2.open(sourceOf(payload), payload.length - 1)
                    .counts(Set.of("vector")))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("posting range");
    assertThatThrownBy(
            () ->
                TextIndexPayloadV2.open(sourceOf(payload), payload.length - 1)
                    .shard(Set.of("vector"), false))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("posting range");
  }

  @Test
  void rejectsCorruptPostings() throws IOException {
    byte[] payload = bytes(encodeV2(corpus()));
    // the postings section starts right after the dictionary; overwriting its first bytes makes
    // the zlib stream unreadable, which must fail instead of reading a posting that is not there
    int postingsStart = intAt(payload, POSTINGS_START_OFFSET);
    for (int index = 0; index < 8; index++) {
      payload[postingsStart + index] = (byte) 0xFF;
    }
    // the first posting range in sorted term order is "berg", and decoding it must fail
    assertThatThrownBy(
            () ->
                TextIndexPayloadV2.open(sourceOf(payload), payload.length)
                    .shard(Set.of("berg"), false))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("incorrect header check");
  }

  @Test
  void trailingBytesAreIgnoredByRangeAddressedReads() throws IOException {
    // bytes past the last posting range are ignored by design: the reader only reads the ranges
    // the header names and the blob length is the authoritative size, so a padded payload has to
    // answer exactly like the clean one
    byte[] payload = bytes(encodeV2(corpus()));
    byte[] padded = new byte[payload.length + 3];
    System.arraycopy(payload, 0, padded, 0, payload.length);
    padded[payload.length] = (byte) 0xDE;
    padded[payload.length + 1] = (byte) 0xAD;
    padded[payload.length + 2] = (byte) 0xBE;

    FileIndex paddedShard = openV2(padded, padded.length).shard(null, false);
    FileIndex cleanShard = openV2(payload, payload.length).shard(null, false);
    assertThat(paddedShard.localDocuments).isEqualTo(cleanShard.localDocuments);
    assertThat(paddedShard.documents).hasSameSizeAs(cleanShard.documents);
    assertThat(paddedShard.postingsByTerm.keySet())
        .containsExactlyInAnyOrderElementsOf(cleanShard.postingsByTerm.keySet());
    for (String term : cleanShard.postingsByTerm.keySet()) {
      assertThat(postingRefs(paddedShard, term)).isEqualTo(postingRefs(cleanShard, term));
    }
  }

  @Test
  void v2PayloadIsUnderSixtyPercentOfV1() throws IOException {
    // 2000 documents, 60 distinct terms each, drawn from a 600 term vocabulary so every term has
    // a realistic document frequency instead of being unique to one document
    int documents = 2000;
    int termsPerDocument = 60;
    int vocabularySize = 600;
    Corpus corpus = new Corpus();
    for (int document = 0; document < documents; document++) {
      List<String> documentTokens = Lists.newArrayListWithCapacity(termsPerDocument);
      for (int term = 0; term < termsPerDocument; term++) {
        documentTokens.add("term" + ((document * 7 + term * 11) % vocabularySize));
      }

      corpus.document(
          "data-" + (document % 8) + ".parquet", 1_000_000L + document * 37L, documentTokens);
    }

    int v1Bytes = encodeV1(corpus).remaining();
    int v2Bytes = encodeV2(corpus).remaining();
    double ratio = (double) v2Bytes / v1Bytes;
    assertThat(ratio)
        .as("v2 %d bytes against v1 %d bytes", v2Bytes, v1Bytes)
        .isLessThan(0.60d)
        .isGreaterThan(0.0d);
  }

  /** The corpus every equivalence test uses: three data files, repeated terms, varied lengths. */
  private static Corpus corpus() {
    return new Corpus()
        .document("a-000.parquet", 0, List.of("ice", "ice", "berg"))
        .document("a-000.parquet", 1, List.of("ice", "vector", "search"))
        .document("b-001.parquet", 7, List.of("ice", "glacier"))
        .document("c-002.parquet", 12, List.of("vector", "search", "search", "ice"));
  }

  /** Documents sharing one term, plus a tail document that only the tail chunk holds. */
  private static Corpus chunkedCorpus(int documents) {
    Corpus corpus = new Corpus();
    for (int document = 0; document < documents; document++) {
      boolean tail = document == TextIndexPayloadV2.CHUNK_DOCS;
      corpus.document(
          tail ? "tail.parquet" : "chunk.parquet",
          document,
          tail ? List.of("shared", "tail") : List.of("shared"));
    }

    return corpus;
  }

  /** Terms that share a prefix with {@code base} and land in its bucket, in sorted order. */
  private static List<String> sameBucket(String base, int count) {
    List<String> terms = Lists.newArrayList();
    for (int candidate = 0; terms.size() < count; candidate++) {
      String term = base + candidate;
      if (bucketOf(term) == bucketOf(base)) {
        terms.add(term);
      }

      assertThat(candidate).as("terms of the bucket of %s", base).isLessThan(10_000);
    }

    terms.add(base);
    terms.sort(String::compareTo);
    return terms;
  }

  /** The bucket a term is written to, the same function the reader and the writer use. */
  private static int bucketOf(String term) {
    return term.hashCode() & (BUCKET_COUNT - 1);
  }

  private static List<String> postingRefs(FileIndex index, String term) {
    List<String> refs = Lists.newArrayList();
    for (Posting posting : index.postingsByTerm.get(term)) {
      Document document = index.documents.get(posting.docId);
      refs.add(ref(document) + "#" + posting.termFrequency);
    }

    return refs;
  }

  private static String ref(Document document) {
    return document.dataFile + "@" + document.rowPosition + "/" + document.length;
  }

  private static ByteBuffer encodeV1(Corpus corpus) throws IOException {
    return TextIndexPayload.encode(corpus.counts(), corpus.index());
  }

  private static ByteBuffer encodeV2(Corpus corpus) throws IOException {
    return TextIndexPayloadV2.encode(corpus.counts(), corpus.index());
  }

  private static FileIndex shardV1(ByteBuffer payload, Set<String> terms) throws IOException {
    return TextIndexPayload.open(TextIndexPayload.source(payload), payload.remaining())
        .shard(terms, false);
  }

  private static FileIndex decodeV1(ByteBuffer payload) throws IOException {
    return shardV1(payload, null);
  }

  private static FileIndex decodeV2(ByteBuffer payload) throws IOException {
    return shardV2(payload, null);
  }

  private static FileIndex shardV2(ByteBuffer payload, Set<String> terms) throws IOException {
    return shardV2(payload, terms, false);
  }

  private static FileIndex shardV2(ByteBuffer payload, Set<String> terms, boolean statisticsOnly)
      throws IOException {
    return openV2(bytes(payload), payload.remaining()).shard(terms, statisticsOnly);
  }

  private static CorpusCounts countsV1(ByteBuffer payload, Set<String> terms) throws IOException {
    return TextIndexPayload.open(TextIndexPayload.source(payload), payload.remaining())
        .counts(terms);
  }

  private static CorpusCounts countsV2(ByteBuffer payload, Set<String> terms) throws IOException {
    return openV2(bytes(payload), payload.remaining()).counts(terms);
  }

  private static TextIndexPayloadV2.Reader openV2(byte[] payload, int size) throws IOException {
    return TextIndexPayloadV2.open(sourceOf(payload), size);
  }

  private static TextIndexPayload.Source sourceOf(byte[] payload) {
    return TextIndexPayloadV2.source(ByteBuffer.wrap(payload));
  }

  private static byte[] bytes(ByteBuffer buffer) {
    byte[] bytes = new byte[buffer.remaining()];
    buffer.duplicate().get(bytes);
    return bytes;
  }

  private static int intAt(byte[] bytes, int offset) {
    return ByteBuffer.wrap(bytes).getInt(offset);
  }

  private static void putInt(byte[] bytes, int offset, int value) {
    ByteBuffer.wrap(bytes).putInt(offset, value);
  }

  /** A hand-built corpus: documents in document order and the postings the encoders need. */
  private static final class Corpus {
    private final List<Document> documents = Lists.newArrayList();
    private final Map<String, List<Posting>> postings = Maps.newHashMap();
    private final Map<String, List<String>> postingRefs = Maps.newHashMap();
    private long tokens;

    private Corpus document(String dataFile, long rowPosition, List<String> documentTokens) {
      int docId = documents.size();
      documents.add(new Document(dataFile, rowPosition, documentTokens.size()));
      tokens += documentTokens.size();
      Map<String, Integer> frequencies = new LinkedHashMap<>();
      documentTokens.forEach(token -> frequencies.merge(token, 1, Math::addExact));
      String ref = dataFile + "@" + rowPosition + "/" + documentTokens.size();
      frequencies.forEach(
          (term, frequency) -> {
            postings
                .computeIfAbsent(term, key -> Lists.newArrayList())
                .add(new Posting(docId, frequency));
            postingRefs
                .computeIfAbsent(term, key -> Lists.newArrayList())
                .add(ref + "#" + frequency);
          });
      return this;
    }

    private int documents() {
      return documents.size();
    }

    private Set<String> terms() {
      return postings.keySet();
    }

    private List<String> postingRefs(String term) {
      return postingRefs.get(term);
    }

    private Map<String, Long> documentFrequency() {
      Map<String, Long> frequency = Maps.newHashMap();
      postings.forEach((term, list) -> frequency.put(term, (long) list.size()));
      return frequency;
    }

    private FileIndex index() {
      return new FileIndex(documents.size(), tokens, documentFrequency(), postings, documents);
    }

    private CorpusCounts counts() {
      return new CorpusCounts(documents.size(), tokens, documentFrequency());
    }
  }
}
