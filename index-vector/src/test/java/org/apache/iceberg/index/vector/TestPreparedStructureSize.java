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

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.iceberg.index.IndexAdapter;
import org.apache.iceberg.index.IndexEntry;
import org.apache.iceberg.index.vector.VectorIndexDefinition.Metric;
import org.apache.iceberg.index.vector.VectorIndexDefinition.VectorQuantizer;
import org.apache.iceberg.index.vector.VectorIndexDefinition.VectorStructure;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Pins the size a prepared structure reports.
 *
 * <p>The cache budgets on this number, and the fallback it replaced -- twice the payload -- was
 * wrong in both directions: too high for a quantized list, whose entries are a few bytes each, and
 * too low for a graph, whose connection lists cost about as much again as the vectors they index. A
 * budget derived from the wrong number is why the cache covered 7 of 13 files per executor.
 */
class TestPreparedStructureSize {

  private static final int DIMENSION = 16;

  @BeforeAll
  static void registerVectorIndexType() {
    VectorIndexTypes.register();
  }

  @Test
  void anUnquantizedListReportsMorePerEntryThanAQuantizedOne() {
    VectorIndexAdapter exact = adapter(VectorStructure.IVF, "NONE", nprobe());
    VectorIndexAdapter quantized = adapter(VectorStructure.IVF, "RABITQ", nprobe());

    long exactBytes = preparedBytes(exact, payload(exact, 400, false));
    long quantizedBytes = preparedBytes(quantized, payload(quantized, 400, false));

    // four bits per dimension is a sixteenth of the vector it replaced, so a single factor for
    // both is what made a derived budget too small for an index that is mostly quantized
    assertThat(quantizedBytes).isLessThan(exactBytes);
    assertThat(quantizedBytes).isPositive();
  }

  @Test
  void aQuantizedIndexStillReportsItsCentres() {
    VectorIndexAdapter quantized = adapter(VectorStructure.IVF, "RABITQ", nprobe());

    // the same entries in more lists means more centroids and more per-list centres, which the
    // entries alone do not account for
    long few = preparedBytes(quantized, payload(quantized, 400, false));
    long many = preparedBytes(quantized, payload(quantized, 400, true));

    assertThat(many).isGreaterThanOrEqualTo(few);
  }

  @Test
  void aFlatIndexReportsWhatItsEntriesCost() {
    VectorIndexAdapter flat = adapter(VectorStructure.FLAT, "NONE", ImmutableMap.of());
    ByteBuffer payload = payload(flat, 400, false);

    long reported = preparedBytes(flat, payload);

    // the entries are decoded and kept, so the structure is at least the payload it read
    assertThat(reported).isGreaterThanOrEqualTo(payload.remaining());
  }

  @Test
  void aGraphReportsMoreThanTheVectorsItIndexes() {
    VectorIndexAdapter graph =
        adapter(
            VectorStructure.HNSW, "NONE", ImmutableMap.of("max-m", "16", "ef-construction", "100"));
    ByteBuffer payload = payload(graph, 400, false);

    long reported = preparedBytes(graph, payload);
    long storedBytes = 400L * DIMENSION * Float.BYTES;

    // a node's connection lists are a comparable cost to its vector: reporting only the payload
    // leaves the cache under-charged for exactly the structure that costs the most
    assertThat(reported).isGreaterThan(storedBytes);
  }

  @Test
  void everyKernelReportsSomething() {
    for (VectorStructure structure : VectorStructure.values()) {
      VectorIndexAdapter adapter = adapter(structure, "NONE", defaultOptions(structure));
      long reported = preparedBytes(adapter, payload(adapter, 20, false));
      assertThat(reported).as("reported bytes of %s", structure).isPositive();
    }
  }

  private static long preparedBytes(VectorIndexAdapter adapter, ByteBuffer payload) {
    IndexAdapter.PreparedSearch search =
        adapter.writesAuxiliaryFiles()
            ? adapter.prepareAuxiliary(payload)
            : adapter.prepare(payload);
    return search.estimatedBytes();
  }

  private static ImmutableMap<String, String> nprobe() {
    return ImmutableMap.of("nprobe", "4");
  }

  private static ImmutableMap<String, String> defaultOptions(VectorStructure structure) {
    return structure == VectorStructure.HNSW
        ? ImmutableMap.of("max-m", "8", "ef-construction", "50")
        : structure == VectorStructure.IVF ? ImmutableMap.of("nprobe", "2") : ImmutableMap.of();
  }

  private static VectorIndexAdapter adapter(
      VectorStructure structure, String quantizer, ImmutableMap<String, String> options) {
    return new VectorIndexAdapter(
        VectorIndexDefinition.builder()
            .fieldId(1)
            .dimension(DIMENSION)
            .metric(Metric.EUCLIDEAN)
            .structure(structure)
            .quantizer(VectorQuantizer.valueOf(quantizer))
            .properties(options)
            .build());
  }

  /** An index of {@code count} vectors, written the way a build writes one. */
  private static ByteBuffer payload(VectorIndexAdapter adapter, int count, boolean manyLists) {
    List<IndexEntry> entries = Lists.newArrayListWithCapacity(count);
    for (int i = 0; i < count; i++) {
      float[] vector = new float[DIMENSION];
      for (int d = 0; d < DIMENSION; d++) {
        vector[d] = (i + d) % 7;
      }

      entries.add(
          IndexEntry.of(
              "data-" + (manyLists ? i : i % 4) + ".parquet", i, adapter.encodeVector(vector)));
    }

    return adapter.writesAuxiliaryFiles()
        ? adapter.encodeAuxiliaryPayload(entries)
        : adapter.encodeEntries(entries);
  }
}
