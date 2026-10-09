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
package org.apache.iceberg.index;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.iceberg.index.VectorIndexDefinition.IndexMode;
import org.apache.iceberg.index.VectorIndexDefinition.Metric;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

class TestVectorIndexAdapter {

  private final VectorIndexAdapter cosine = adapter(Metric.COSINE);

  private static VectorIndexAdapter adapter(Metric metric) {
    return new VectorIndexAdapter(
        VectorIndexDefinition.builder().fieldId(1).dimension(2).metric(metric).build());
  }

  private static VectorIndexAdapter hnswAdapter(Metric metric) {
    return new VectorIndexAdapter(
        VectorIndexDefinition.builder()
            .fieldId(1)
            .dimension(2)
            .metric(metric)
            .mode(IndexMode.HNSW)
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
    VectorIndexAdapter adapter = adapter(Metric.EUCLIDEAN);
    List<IndexEntry> entries =
        List.of(
            IndexEntry.of("data-1.parquet", 0, adapter.encodeVector(new float[] {1.0f, 0.0f})),
            IndexEntry.of("data-2.parquet", 7, adapter.encodeVector(new float[] {0.0f, 2.0f})));
    ByteBuffer payload = adapter.encodeEntries(entries);
    assertThat(adapter.decodeEntries(payload)).containsExactlyElementsOf(entries);
    assertThat(adapter.entryBlobType()).isEqualTo("iceberg-vector-entries-v1");
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
    VectorIndexAdapter dot = adapter(Metric.DOT_PRODUCT);
    assertThat(dot.search(payload, query, 2))
        .extracting(IndexSearchResult::dataFile)
        .containsExactly("data-b.parquet", "data-a.parquet");

    // EUCLIDEAN prefers the smaller distance
    VectorIndexAdapter euclidean = adapter(Metric.EUCLIDEAN);
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
    for (Metric metric : Metric.values()) {
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
      assertThat(hnsw.auxiliaryBlobType()).isEqualTo("iceberg-vector-hnsw-v1");
      assertThat(hnsw.writesAuxiliaryFiles()).isTrue();
      assertThat(exact.writesAuxiliaryFiles()).isFalse();
    }
  }

  @Test
  public void hnswAuxiliarySearchTruncatesAndResolvesRows() {
    VectorIndexAdapter hnsw = hnswAdapter(Metric.COSINE);
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
}
