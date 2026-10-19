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
package org.apache.iceberg.index.vector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.iceberg.index.IndexAdapter;
import org.apache.iceberg.index.IndexEntry;
import org.apache.iceberg.index.IndexSearchResult;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

class TestVectorIndexAdapter {

  /**
   * Rewrites a payload's header into the form written before the kernel was named: a bare format
   * version, then the body.
   *
   * <p>An index built by an earlier build is recorded with that form, so the reader has to accept
   * it or every such index silently stops being readable.
   */
  private static ByteBuffer asLegacyPayload(ByteBuffer payload) {
    byte[] bytes = new byte[payload.remaining()];
    payload.duplicate().get(bytes);
    java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(bytes);
    int headerVersion = buffer.getInt();
    assertThat(headerVersion).as("the header this helper rewrites").isEqualTo(3);
    int kernelNameLength = buffer.getShort();
    buffer.position(buffer.position() + kernelNameLength);
    buffer.getInt(); // the kernel's own format version
    byte[] body = new byte[buffer.remaining()];
    buffer.get(body);

    java.nio.ByteBuffer legacy = java.nio.ByteBuffer.allocate(4 + body.length);
    legacy.putInt(2);
    legacy.put(body);
    legacy.flip();
    return legacy;
  }

  @Test
  public void readsPayloadsWrittenBeforeTheKernelWasNamed() {
    VectorIndexAdapter adapter =
        new VectorIndexAdapter(
            VectorIndexDefinition.builder()
                .fieldId(1)
                .dimension(2)
                .metric(VectorIndexDefinition.Metric.EUCLIDEAN)
                .structure(VectorIndexDefinition.VectorStructure.HNSW)
                .maxM(8)
                .efConstruction(100)
                .efSearch(64)
                .build());

    List<IndexEntry> entries =
        Lists.newArrayList(
            IndexEntry.of("data-1.parquet", 0, adapter.encodeVector(new float[] {1.0f, 0.0f})),
            IndexEntry.of("data-2.parquet", 1, adapter.encodeVector(new float[] {0.0f, 1.0f})));

    ByteBuffer query = adapter.encodeQuery(new float[] {1.0f, 0.1f});

    // both roles, because both headers changed
    ByteBuffer entryPayload = asLegacyPayload(adapter.encodeEntries(entries));
    assertThat(adapter.decodeEntries(entryPayload))
        .as("an entry payload from before the header named the kernel")
        .hasSize(2);
    assertThat(adapter.search(entryPayload, query, 1)).hasSize(1);

    ByteBuffer auxiliaryPayload = asLegacyPayload(adapter.encodeAuxiliaryPayload(entries, 1));
    assertThat(adapter.prepareAuxiliary(auxiliaryPayload).search(query, 1))
        .as("an auxiliary payload from before the header named the kernel")
        .hasSize(1);
  }

  @Test
  public void refusesAPayloadFromAKernelItDoesNotHave() {
    VectorIndexAdapter adapter =
        new VectorIndexAdapter(
            VectorIndexDefinition.builder()
                .fieldId(1)
                .dimension(2)
                .metric(VectorIndexDefinition.Metric.EUCLIDEAN)
                .structure(VectorIndexDefinition.VectorStructure.HNSW)
                .build());

    ByteBuffer entries =
        adapter.encodeEntries(
            Lists.newArrayList(
                IndexEntry.of(
                    "data-1.parquet", 0, adapter.encodeVector(new float[] {1.0f, 0.0f}))));

    // replace the kernel name with one this build has no searcher for
    byte[] bytes = new byte[entries.remaining()];
    entries.duplicate().get(bytes);
    java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(bytes);
    buffer.getInt();
    int nameLength = buffer.getShort();
    String name =
        new String(bytes, buffer.position(), nameLength, java.nio.charset.StandardCharsets.UTF_8);
    int at = buffer.position();
    byte[] replacement = "faiss".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    byte[] rewritten = new byte[bytes.length - nameLength + replacement.length];
    System.arraycopy(bytes, 0, rewritten, 0, at - 2);
    rewritten[at - 2] = (byte) ((replacement.length >> 8) & 0xFF);
    rewritten[at - 1] = (byte) (replacement.length & 0xFF);
    System.arraycopy(replacement, 0, rewritten, at, replacement.length);
    System.arraycopy(
        bytes, at + nameLength, rewritten, at + replacement.length, bytes.length - at - nameLength);

    assertThat(name).isEqualTo("bruteforce");
    assertThatThrownBy(() -> adapter.decodeEntries(java.nio.ByteBuffer.wrap(rewritten)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("written by kernel faiss")
        .hasMessageContaining("this build reads kernel bruteforce");
  }

  private final VectorIndexAdapter cosine = adapter(VectorIndexDefinition.Metric.COSINE);

  private static VectorIndexAdapter adapter(VectorIndexDefinition.Metric metric) {
    return new VectorIndexAdapter(
        VectorIndexDefinition.builder().fieldId(1).dimension(2).metric(metric).build());
  }

  private static VectorIndexAdapter hnswAdapter(VectorIndexDefinition.Metric metric) {
    return new VectorIndexAdapter(
        VectorIndexDefinition.builder()
            .fieldId(1)
            .dimension(2)
            .metric(metric)
            .structure(VectorIndexDefinition.VectorStructure.HNSW)
            .maxM(8)
            .efConstruction(100)
            .efSearch(64)
            .build());
  }

  @Test
  public void encodesAndDecodesVectorValues() {
    float[] vector = {0.5f, -1.5f};
    assertThat(cosine.decodeVector(cosine.encodeVector(vector))).isEqualTo(vector);
    assertThat(cosine.decodeQuery(cosine.encodeQuery(vector))).isEqualTo(vector);
  }

  @Test
  public void roundTripsEntries() {
    VectorIndexAdapter adapter = adapter(VectorIndexDefinition.Metric.EUCLIDEAN);
    List<IndexEntry> entries =
        List.of(
            IndexEntry.of("data-1.parquet", 0, adapter.encodeVector(new float[] {1.0f, 0.0f})),
            IndexEntry.of("data-2.parquet", 7, adapter.encodeVector(new float[] {0.0f, 2.0f})));
    ByteBuffer payload = adapter.encodeEntries(entries);
    assertThat(adapter.decodeEntries(payload)).containsExactlyElementsOf(entries);
    assertThat(adapter.entryBlobType()).isEqualTo("iceberg-vector-entries-v1");
  }

  @Test
  public void writesADataFileOncePerIndexFileRatherThanOncePerEntry() {
    VectorIndexAdapter adapter = adapter(VectorIndexDefinition.Metric.EUCLIDEAN);
    // many entries over few data files is the shape a real index file has, and it is where writing
    // the path per entry costs the most
    List<IndexEntry> entries = Lists.newArrayList();
    for (int i = 0; i < 500; i++) {
      String dataFile = "a-rather-long-data-file-path-" + (i % 2) + ".parquet";
      entries.add(IndexEntry.of(dataFile, i, adapter.encodeVector(new float[] {i, -i})));
    }

    ByteBuffer payload = adapter.encodeEntries(entries);

    assertThat(adapter.decodeEntries(payload)).containsExactlyElementsOf(entries);

    // the two paths appear once each in the payload, not once per entry
    byte[] bytes = new byte[payload.remaining()];
    payload.duplicate().get(bytes);
    assertThat(countOccurrences(bytes, "a-rather-long-data-file-path-0.parquet")).isEqualTo(1);
    assertThat(countOccurrences(bytes, "a-rather-long-data-file-path-1.parquet")).isEqualTo(1);
  }

  private static int countOccurrences(byte[] bytes, String needle) {
    byte[] pattern = needle.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    int count = 0;
    for (int i = 0; i <= bytes.length - pattern.length; i++) {
      boolean found = true;
      for (int j = 0; j < pattern.length; j++) {
        if (bytes[i + j] != pattern[j]) {
          found = false;
          break;
        }
      }
      if (found) {
        count += 1;
      }
    }
    return count;
  }

  @Test
  public void ordersResultsByMetric() {
    List<IndexEntry> entries =
        List.of(
            IndexEntry.of("data-a.parquet", 0, cosine.encodeVector(new float[] {1.0f, 0.0f})),
            IndexEntry.of("data-b.parquet", 1, cosine.encodeVector(new float[] {2.0f, 0.0f})));
    ByteBuffer payload = cosine.encodeEntries(entries);
    ByteBuffer query = cosine.encodeQuery(new float[] {1.0f, 0.0f});

    // COSINE ignores magnitude: both entries score 1.0, so ties break on the row reference
    assertThat(cosine.search(payload, query, 2))
        .extracting(IndexSearchResult::dataFile)
        .containsExactly("data-a.parquet", "data-b.parquet");

    // DOT_PRODUCT prefers the larger magnitude
    VectorIndexAdapter dot = adapter(VectorIndexDefinition.Metric.DOT_PRODUCT);
    assertThat(dot.search(payload, query, 2))
        .extracting(IndexSearchResult::dataFile)
        .containsExactly("data-b.parquet", "data-a.parquet");

    // EUCLIDEAN prefers the smaller distance
    VectorIndexAdapter euclidean = adapter(VectorIndexDefinition.Metric.EUCLIDEAN);
    assertThat(euclidean.search(payload, query, 2))
        .extracting(IndexSearchResult::dataFile)
        .containsExactly("data-a.parquet", "data-b.parquet");
  }

  @Test
  public void truncatesToTopK() {
    List<IndexEntry> entries =
        List.of(
            IndexEntry.of("data-1.parquet", 0, cosine.encodeVector(new float[] {1.0f, 0.0f})),
            IndexEntry.of("data-2.parquet", 1, cosine.encodeVector(new float[] {0.0f, 1.0f})),
            IndexEntry.of("data-3.parquet", 2, cosine.encodeVector(new float[] {0.7f, 0.7f})));
    ByteBuffer payload = cosine.encodeEntries(entries);

    assertThat(cosine.search(payload, cosine.encodeQuery(new float[] {0.95f, 0.05f}), 2))
        .extracting(IndexSearchResult::dataFile)
        .containsExactly("data-1.parquet", "data-3.parquet");
    assertThat(cosine.search(payload, cosine.encodeQuery(new float[] {0.95f, 0.05f}), 0)).isEmpty();
  }

  @Test
  public void rejectsInvalidQueries() {
    assertThatThrownBy(() -> cosine.encodeQuery(new float[] {1.0f}))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("dimension");
    assertThatThrownBy(() -> cosine.encodeQuery(new float[] {Float.NaN, 0.0f}))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("finite");
  }

  @Test
  public void hnswAuxiliarySearchMatchesExactForAllMetrics() {
    // 40 vectors on a circle; efSearch (64) exceeds the entry count so HNSW is exact
    for (VectorIndexDefinition.Metric metric : VectorIndexDefinition.Metric.values()) {
      VectorIndexAdapter hnsw = hnswAdapter(metric);
      VectorIndexAdapter exact = adapter(metric);
      List<IndexEntry> entries = circleEntries(hnsw, 40);
      ByteBuffer auxiliary = hnsw.encodeAuxiliaryPayload(entries);
      ByteBuffer entryPayload = exact.encodeEntries(entries);
      ByteBuffer query = exact.encodeQuery(new float[] {0.8f, 0.6f});

      assertThat(hnsw.searchAuxiliary(auxiliary, query, 5))
          .as("metric %s", metric)
          .isEqualTo(exact.search(entryPayload, query, 5));
      assertThat(hnsw.searchAuxiliary(auxiliary, query, 0)).isEmpty();
      // the name says the payload's role, not the algorithm: the algorithm is in the payload header
      assertThat(hnsw.auxiliaryBlobType()).isEqualTo("iceberg-vector-auxiliary-v1");
      assertThat(hnsw.readableAuxiliaryBlobTypes())
          .as("the name it used to write is still readable")
          .contains("iceberg-vector-auxiliary-v1", "iceberg-vector-hnsw-v1");
      assertThat(hnsw.writesAuxiliaryFiles()).isTrue();
      assertThat(exact.writesAuxiliaryFiles()).isFalse();
    }
  }

  @Test
  public void hnswAuxiliarySearchTruncatesAndResolvesRows() {
    VectorIndexAdapter hnsw = hnswAdapter(VectorIndexDefinition.Metric.COSINE);
    List<IndexEntry> entries = circleEntries(hnsw, 40);
    ByteBuffer auxiliary = hnsw.encodeAuxiliaryPayload(entries);
    ByteBuffer query = hnsw.encodeQuery(new float[] {0.8f, 0.6f});

    List<IndexSearchResult> results = hnsw.searchAuxiliary(auxiliary, query, 3);
    assertThat(results).hasSize(3);
    // row references resolve back to the encoded entries; the query at ~36.9 degrees is
    // closest to circle-4 at 36 degrees
    assertThat(results.get(0).dataFile()).isEqualTo("circle-4.parquet");
    assertThat(results.get(0).rowPosition()).isEqualTo(4L);
  }

  private static List<IndexEntry> circleEntries(VectorIndexAdapter adapter, int count) {
    List<IndexEntry> entries = Lists.newArrayList();
    for (int i = 0; i < count; i++) {
      double angle = 2.0 * Math.PI * i / count;
      entries.add(
          IndexEntry.of(
              "circle-" + i + ".parquet",
              i,
              adapter.encodeVector(
                  new float[] {(float) Math.cos(angle), (float) Math.sin(angle)})));
    }
    return entries;
  }

  @Test
  public void preparedSearchDoesNotReadThePayloadAgain() {
    // the whole point of preparing a payload is that the decode happens once, so a search that
    // happens after the payload has been overwritten still answers from the loaded form
    VectorIndexAdapter hnsw = hnswAdapter(VectorIndexDefinition.Metric.EUCLIDEAN);
    List<IndexEntry> entries = circleEntries(hnsw, 40);
    ByteBuffer auxiliary = hnsw.encodeAuxiliaryPayload(entries);
    ByteBuffer query = hnsw.encodeQuery(new float[] {0.8f, 0.6f});

    IndexAdapter.PreparedSearch prepared = hnsw.prepareAuxiliary(auxiliary);
    List<IndexSearchResult> first = prepared.search(query, 5);

    // destroy the payload the searcher was prepared from
    while (auxiliary.hasRemaining()) {
      auxiliary.put((byte) 0);
    }

    assertThat(prepared.search(query, 5))
        .as("a second search must not touch the payload")
        .isEqualTo(first);
    assertThat(first).isNotEmpty();
  }

  @Test
  public void preparedEntrySearchDoesNotDecodeThePayloadAgain() {
    VectorIndexAdapter exact = adapter(VectorIndexDefinition.Metric.EUCLIDEAN);
    List<IndexEntry> entries = circleEntries(exact, 40);
    ByteBuffer payload = exact.encodeEntries(entries);
    ByteBuffer query = exact.encodeQuery(new float[] {0.8f, 0.6f});

    IndexAdapter.PreparedSearch prepared = exact.prepare(payload);
    List<IndexSearchResult> first = prepared.search(query, 5);

    while (payload.hasRemaining()) {
      payload.put((byte) 0);
    }

    assertThat(prepared.search(query, 5)).isEqualTo(first).isNotEmpty();
  }
}
