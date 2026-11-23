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
 * The payload layout this build writes: file table with its row start, one dense length column,
 * postings and dictionary, behind a signature that names the format version.
 */
class TestTextIndexPayload {
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
  void lengthsAboveTheInlineLimitRoundTripExactly() throws IOException {
    FileIndex index =
        index(
            List.of(
                new Document("a.parquet", 0, TextIndexPayload.MAX_INLINE_LENGTH),
                new Document("a.parquet", 1, TextIndexPayload.MAX_INLINE_LENGTH + 1),
                new Document("a.parquet", 2, 7)),
            Map.of("ice", List.of(0, 1, 2)));

    FileIndex decoded = decode(encode(index));
    assertThat(refs(decoded)).containsExactlyElementsOf(refs(index));
    assertThat(decoded.documents.get(0).length).isEqualTo(TextIndexPayload.MAX_INLINE_LENGTH);
    assertThat(decoded.documents.get(1).length).isEqualTo(TextIndexPayload.MAX_INLINE_LENGTH + 1);
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
    FileIndex decoded = decode(TextIndexPayload.encode(counts(index), index));
    ByteBuffer statistics = TextIndexPayload.encode(counts(index), null);

    FileIndex read =
        TextIndexPayload.open(source(bytes(statistics)), statistics.remaining())
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
    TextIndexSource.Source counting =
        (offset, length) -> {
          reads[0] += 1;
          return ByteBuffer.wrap(bytes, offset, length).slice();
        };

    FileIndex decoded = TextIndexPayload.open(counting, bytes.length).shard(Set.of("ice"), false);
    assertThat(decoded.documents).hasSize(2);
    // the lengths are one range and the posting range is another: the read count cannot grow with
    // the documents of the shard
    assertThat(reads[0]).as("reads for two matches in a 20k document shard").isLessThanOrEqualTo(8);
  }

  @Test
  void refusesADocumentThatComesBackToAFileAfterAnother() {
    FileIndex index =
        index(
            List.of(
                new Document("a.parquet", 0, 3),
                new Document("b.parquet", 0, 3),
                new Document("a.parquet", 1, 3)),
            Map.of("ice", List.of(0, 1, 2)));

    assertThatThrownBy(() -> encode(index))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("not contiguous");
  }

  @Test
  void aTermTheShardDoesNotHoldReadsNoDocumentTable() throws IOException {
    int documents = 20_000;
    List<Document> list = Lists.newArrayList();
    for (int document = 0; document < documents; document++) {
      list.add(new Document("a.parquet", document, 5));
    }

    ByteBuffer payload = encode(index(list, Map.of("ice", List.of(0))));
    byte[] bytes = bytes(payload);
    // the dictionary starts after the file table, the length column, the overflow table and the
    // row exceptions: a term the shard does not hold needs none of those
    int dictionaryStart = ByteBuffer.wrap(bytes).getInt(40);
    List<long[]> reads = Lists.newArrayList();
    TextIndexSource.Source counting =
        (offset, length) -> {
          reads.add(new long[] {offset, length});
          return ByteBuffer.wrap(bytes, offset, length).slice();
        };

    FileIndex decoded =
        TextIndexPayload.open(counting, bytes.length).shard(Set.of("missing"), false);
    assertThat(decoded.documents).isEmpty();
    for (long[] read : reads) {
      assertThat(read[0] == 0 || read[0] >= dictionaryStart)
          .as("read of %s bytes at offset %s", read[1], read[0])
          .isTrue();
    }
  }

  @Test
  void fullReadMatchesScopedRead() throws IOException {
    FileIndex index = index(false);
    ByteBuffer payload = encode(index);
    FileIndex full = decode(payload);
    FileIndex scoped =
        TextIndexPayload.open(source(bytes(payload)), payload.remaining())
            .shard(Set.of("ice"), false);

    assertThat(refs(scoped))
        .isEqualTo(
            refs(full).stream()
                .filter(ref -> ref.contains("ice"))
                .collect(java.util.stream.Collectors.toList()));
  }

  @Test
  void rejectsForeignMagic() {
    ByteBuffer payload = ByteBuffer.allocate(TextIndexPayload.HEADER_SIZE);
    payload.putInt(0x424D3201);
    payload.position(0);
    assertThatThrownBy(() -> TextIndexPayload.open(source(bytes(payload)), payload.remaining()))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("Not a BM25 payload");
  }

  @Test
  void rejectsTruncatedPayload() {
    // four bytes is the signature, so nothing shorter can even say what it is
    assertThatThrownBy(() -> TextIndexPayload.open(source(new byte[2]), 2))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("Invalid BM25 payload size");
  }

  @Test
  void writesTheFormatVersionInTheSignature() throws IOException {
    ByteBuffer payload = encode(index(false));

    assertThat(payload.getInt(payload.position()))
        .as("the signature names the version this build writes")
        .isEqualTo(0x424D3102);
    assertThat(TextIndexPayload.matches(payload)).isTrue();
    assertThat(TextIndexPayload.HEADER_SIZE)
        .isEqualTo(Integer.BYTES + 2 * Long.BYTES + 4 * 7 + (64 + 1) * Integer.BYTES);
  }

  @Test
  void asksForARebuildWhenThePayloadWasWrittenByAnotherVersion() {
    ByteBuffer payload = versionOnePayload();

    assertThat(TextIndexPayload.matches(payload))
        .as("a payload of another version is not the payload this reader decodes")
        .isFalse();
    assertThatThrownBy(() -> TextIndexPayload.open(source(bytes(payload)), payload.remaining()))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("Unsupported BM25 payload format version: 1 (this build reads 2)")
        .hasMessageContaining("rebuild the index");
  }

  /**
   * The header a version 1 build wrote: its signature, the two counts and the sixty-five bucket
   * offsets, 280 bytes in all.
   */
  private static ByteBuffer versionOnePayload() {
    int headerSize = Integer.BYTES + 2 * Long.BYTES + (64 + 1) * Integer.BYTES;
    ByteBuffer payload = ByteBuffer.allocate(headerSize);
    payload.putInt(0x424D3101);
    payload.putLong(3);
    payload.putLong(9);
    for (int bucket = 0; bucket <= 64; bucket++) {
      payload.putInt(headerSize);
    }

    payload.position(0);
    return payload;
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
    return TextIndexPayload.encode(counts(index), index);
  }

  private static FileIndex decode(ByteBuffer payload) throws IOException {
    return TextIndexPayload.open(source(bytes(payload)), payload.remaining()).shard(ALL, false);
  }

  private static TextIndexSource.Source source(byte[] payload) {
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
