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
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.index.text.TextIndexAdapter.CorpusCounts;
import org.apache.iceberg.index.text.TextIndexAdapter.Document;
import org.apache.iceberg.index.text.TextIndexAdapter.FileIndex;
import org.apache.iceberg.index.text.TextIndexAdapter.Posting;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;

/**
 * The v3 layout: file table with its row start, one dense length column, postings and dictionary.
 */
class TestTextIndexPayloadV3 {
  private static final Set<String> ALL = null;

  @Test
  void roundTripKeepsDocumentsRowsAndLengths() throws IOException {
    FileIndex index = index(false);
    FileIndex decoded = decode(encode(index));

    assertThat(decoded.localDocuments).isEqualTo(index.localDocuments);
    assertThat(decoded.localTotalLength).isEqualTo(index.localTotalLength);
    assertThat(decoded.localDocumentFrequency).isEqualTo(index.localDocumentFrequency);
    for (String term : index.postingsByTerm.keySet()) {
      assertThat(postingRefs(decoded, term))
          .as("postings of %s", term)
          .containsExactlyElementsOf(postingRefs(index, term));
    }
  }

  @Test
  void readsAcrossFileBoundariesResolveTheOwningFile() throws IOException {
    FileIndex index = index(false);
    FileIndex decoded = decode(encode(index));

    // the file pointer advances within a term and has to restart for the next one, so read the
    // same term twice and check both answers
    assertThat(postingRefs(decoded, "ice")).containsExactlyElementsOf(postingRefs(index, "ice"));
    assertThat(postingRefs(decoded, "vector"))
        .containsExactlyElementsOf(postingRefs(index, "vector"));
    assertThat(postingRefs(decoded, "ice")).containsExactlyElementsOf(postingRefs(index, "ice"));
  }

  @Test
  void equalsTheV2LayoutForTheSameShard() throws IOException {
    FileIndex index = index(false);
    CorpusCounts counts = counts(index);
    FileIndex v3 = decode(TextIndexPayloadV3.encode(counts, index));
    FileIndex v2 = decodeV2(TextIndexPayloadV2.encode(counts, index));

    assertThat(refs(v3)).containsExactlyElementsOf(refs(v2));
    assertThat(v3.localDocuments).isEqualTo(v2.localDocuments);
    assertThat(v3.localTotalLength).isEqualTo(v2.localTotalLength);
    assertThat(v3.localDocumentFrequency).isEqualTo(v2.localDocumentFrequency);
  }

  @Test
  void lengthsAboveTheInlineLimitRoundTripExactly() throws IOException {
    FileIndex index =
        index(
            List.of(
                new Document("a.parquet", 0, TextIndexPayloadV3.MAX_INLINE_LENGTH),
                new Document("a.parquet", 1, TextIndexPayloadV3.MAX_INLINE_LENGTH + 1),
                new Document("a.parquet", 2, 7)),
            Map.of("ice", List.of(0, 1, 2)));

    FileIndex decoded = decode(encode(index));
    assertThat(refs(decoded)).containsExactlyElementsOf(refs(index));
    assertThat(decoded.documents.get(0).length).isEqualTo(TextIndexPayloadV3.MAX_INLINE_LENGTH);
    assertThat(decoded.documents.get(1).length).isEqualTo(TextIndexPayloadV3.MAX_INLINE_LENGTH + 1);
  }

  @Test
  void rowsThatSkipARowAreRecordedAsExceptions() throws IOException {
    FileIndex index =
        index(
            List.of(
                new Document("a.parquet", 0, 3),
                new Document("a.parquet", 1, 3),
                new Document("a.parquet", 5, 3),
                new Document("b.parquet", 0, 3)),
            Map.of("ice", List.of(0, 1, 2, 3)));

    FileIndex decoded = decode(encode(index));
    assertThat(refs(decoded)).containsExactlyElementsOf(refs(index));
  }

  @Test
  void statisticsPayloadHoldsOnlyTheDictionary() throws IOException {
    FileIndex index = index(false);
    FileIndex decoded = decode(TextIndexPayloadV3.encode(counts(index), index));
    ByteBuffer statistics = TextIndexPayloadV3.encode(counts(index), null);

    FileIndex read =
        TextIndexPayloadV3.open(source(bytes(statistics)), statistics.remaining())
            .shard(Set.of("ice", "vector"), true);
    assertThat(read.documents).isEmpty();
    assertThat(read.localDocuments).isEqualTo(index.localDocuments);
    assertThat(read.localDocumentFrequency).isEqualTo(Map.of("ice", 4L, "vector", 2L));
    assertThat(decoded.documents).isNotEmpty();
  }

  @Test
  void emptyShardRoundTrips() throws IOException {
    FileIndex index = index(List.of(), Map.of());
    FileIndex decoded = decode(encode(index));

    assertThat(decoded.localDocuments).isZero();
    assertThat(decoded.documents).isEmpty();
    assertThat(decoded.postingsByTerm).isEmpty();
  }

  @Test
  void singleDocumentRoundTrips() throws IOException {
    FileIndex index = index(List.of(new Document("a.parquet", 42, 1)), Map.of("ice", List.of(0)));
    FileIndex decoded = decode(encode(index));

    assertThat(refs(decoded)).containsExactly("ice/a.parquet@42/1");
  }

  @Test
  void readsStayConstantAsTheShardGrows() throws IOException {
    int documents = 20_000;
    List<Document> list = Lists.newArrayList();
    for (int document = 0; document < documents; document++) {
      list.add(new Document("a.parquet", document, 5));
    }

    FileIndex index = index(list, Map.of("ice", List.of(0, documents - 1), "berg", List.of(1)));
    ByteBuffer payload = encode(index);
    byte[] bytes = bytes(payload);
    int[] reads = new int[1];
    TextIndexPayload.Source counting =
        (offset, length) -> {
          reads[0] += 1;
          return ByteBuffer.wrap(bytes, offset, length).slice();
        };

    FileIndex decoded = TextIndexPayloadV3.open(counting, bytes.length).shard(Set.of("ice"), false);
    assertThat(decoded.documents).hasSize(2);
    // the lengths are one range and the posting range is another: the read count cannot grow with
    // the documents of the shard
    assertThat(reads[0]).as("reads for two matches in a 20k document shard").isLessThanOrEqualTo(8);
  }

  @Test
  void fullReadMatchesScopedRead() throws IOException {
    FileIndex index = index(false);
    ByteBuffer payload = encode(index);
    FileIndex full = decode(payload);
    FileIndex scoped =
        TextIndexPayloadV3.open(source(bytes(payload)), payload.remaining())
            .shard(Set.of("ice"), false);

    assertThat(refs(scoped))
        .isEqualTo(
            refs(full).stream()
                .filter(ref -> ref.contains("ice"))
                .collect(java.util.stream.Collectors.toList()));
  }

  @Test
  void theLayoutIsSmallerThanV2ForADenseShard() throws IOException {
    int documents = 2_000;
    List<Document> list = Lists.newArrayList();
    for (int document = 0; document < documents; document++) {
      list.add(new Document("a.parquet", document, 200 + document % 300));
    }

    Map<String, List<Integer>> postings = Maps.newLinkedHashMap();
    List<String> terms = List.of("ice", "berg", "vector", "search", "glacier");
    for (int term = 0; term < terms.size(); term++) {
      List<Integer> ids = Lists.newArrayList();
      for (int document = term; document < documents; document += terms.size()) {
        ids.add(document);
      }

      postings.put(terms.get(term), ids);
    }

    FileIndex index = index(list, postings);
    CorpusCounts counts = counts(index);
    ByteBuffer v2 = TextIndexPayloadV2.encode(counts, index);
    ByteBuffer v3 = TextIndexPayloadV3.encode(counts, index);
    // the per-document row references disappear: rows follow from the file table, so the column
    // holds only the lengths
    assertThat(v3.remaining()).as("v3 bytes vs v2 bytes").isLessThan(v2.remaining());
  }

  @Test
  void rejectsForeignMagic() {
    ByteBuffer payload = ByteBuffer.allocate(TextIndexPayloadV3.HEADER_SIZE);
    payload.putInt(0x424D3201);
    payload.position(0);
    assertThatThrownBy(() -> TextIndexPayloadV3.open(source(bytes(payload)), payload.remaining()))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("Not a BM25 v3 payload");
  }

  @Test
  void rejectsTruncatedPayload() {
    assertThatThrownBy(() -> TextIndexPayloadV3.open(source(new byte[8]), 8))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("Invalid BM25 v3 payload size");
  }

  private static FileIndex index(boolean unused) {
    return index(
        List.of(
            new Document("a-000.parquet", 0, 3),
            new Document("a-000.parquet", 1, 3),
            new Document("b-001.parquet", 7, 2),
            new Document("c-002.parquet", 12, 4)),
        Map.of(
            "ice", List.of(0, 1, 2, 3),
            "vector", List.of(1, 3)));
  }

  private static FileIndex index(List<Document> documents, Map<String, List<Integer>> postings) {
    Map<String, Long> frequencies = Maps.newLinkedHashMap();
    Map<String, List<Posting>> byTerm = Maps.newLinkedHashMap();
    postings.forEach(
        (term, postingIds) -> {
          List<Posting> list = Lists.newArrayList();
          for (int id : postingIds) {
            list.add(new Posting(id, 1));
          }

          frequencies.put(term, (long) list.size());
          byTerm.put(term, list);
        });
    long total = documents.stream().mapToLong(document -> document.length).sum();
    return new FileIndex(documents.size(), total, frequencies, byTerm, documents);
  }

  private static CorpusCounts counts(FileIndex index) {
    return new CorpusCounts(
        index.localDocuments, index.localTotalLength, index.localDocumentFrequency);
  }

  private static ByteBuffer encode(FileIndex index) throws IOException {
    return TextIndexPayloadV3.encode(counts(index), index);
  }

  private static FileIndex decode(ByteBuffer payload) throws IOException {
    return TextIndexPayloadV3.open(source(bytes(payload)), payload.remaining()).shard(ALL, false);
  }

  private static FileIndex decodeV2(ByteBuffer payload) throws IOException {
    byte[] bytes = bytes(payload);
    return TextIndexPayloadV2.open(source(bytes), bytes.length).shard(ALL, false);
  }

  private static TextIndexPayload.Source source(byte[] payload) {
    return (offset, length) -> ByteBuffer.wrap(payload, offset, length).slice();
  }

  private static byte[] bytes(ByteBuffer buffer) {
    ByteBuffer copy = buffer.duplicate();
    byte[] bytes = new byte[copy.remaining()];
    copy.get(bytes);
    return bytes;
  }

  private static List<String> refs(FileIndex index) {
    List<String> refs = Lists.newArrayList();
    for (Map.Entry<String, List<Posting>> entry : index.postingsByTerm.entrySet()) {
      for (Posting posting : entry.getValue()) {
        refs.add(entry.getKey() + "/" + ref(index.documents.get(posting.docId)));
      }
    }

    return refs;
  }

  private static List<String> postingRefs(FileIndex index, String term) {
    List<String> refs = Lists.newArrayList();
    for (Posting posting : index.postingsByTerm.get(term)) {
      refs.add(ref(index.documents.get(posting.docId)));
    }

    return refs;
  }

  private static String ref(Document document) {
    return document.dataFile + "@" + document.rowPosition + "/" + document.length;
  }
}
