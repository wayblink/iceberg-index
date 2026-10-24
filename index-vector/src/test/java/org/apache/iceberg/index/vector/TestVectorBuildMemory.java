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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;
import org.apache.iceberg.index.IndexAdapter.PreparedSearch;
import org.apache.iceberg.index.IndexBuildResult;
import org.apache.iceberg.index.IndexEntry;
import org.apache.iceberg.index.vector.VectorIndexDefinition.Metric;
import org.apache.iceberg.index.vector.VectorIndexDefinition.VectorQuantizer;
import org.apache.iceberg.index.vector.VectorIndexDefinition.VectorStructure;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

class TestVectorBuildMemory {
  private static final int DIMENSION = 16;

  @BeforeAll
  static void registerVectorIndexType() {
    VectorIndexTypes.register();
  }

  @ParameterizedTest
  @EnumSource(VectorStructure.class)
  void estimatesRetainedStructureWithoutCountingContainer(VectorStructure structure) {
    VectorIndexAdapter adapter = adapter(structure, VectorQuantizer.NONE, options(structure));
    List<IndexEntry> entries = entries(adapter, 100, i -> "data-" + (i % 4) + ".parquet");
    IndexBuildResult built = buildWithPreparationForbidden(adapter, entries);
    assertEstimate(adapter, built);
    assertThat(built.memory().fileBytes()).isNull();
    assertThat(built.memory().readerId()).isNotBlank();
    assertThat(built.memory().runtimeProfile()).isNotBlank();
  }

  @ParameterizedTest
  @EnumSource(VectorStructure.class)
  void adapterPropagatesKernelEstimate(VectorStructure structure) {
    VectorIndexAdapter adapter = adapter(structure, VectorQuantizer.NONE, options(structure));
    List<IndexEntry> entries = entries(adapter, 32, i -> "data.parquet");
    IndexBuildResult built =
        adapter.writesAuxiliaryFiles()
            ? adapter.encodeAuxiliaryPayloadWithMemory(entries, 1)
            : adapter.encodeEntriesWithMemory(entries);
    assertEstimate(adapter, built);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3, 4})
  void quantizationBitsAreReflectedInPreparedEstimate(int bits) {
    VectorIndexAdapter adapter =
        adapter(
            VectorStructure.IVF,
            VectorQuantizer.RABITQ,
            Map.of("nlist", "4", "nprobe", "1", "rabitq-bits", String.valueOf(bits)));
    assertEstimate(
        adapter, buildWithPreparationForbidden(adapter, entries(adapter, 100, i -> "data")));
  }

  @ParameterizedTest
  @EnumSource(VectorStructure.class)
  void estimatesEmptyPayloads(VectorStructure structure) {
    VectorIndexAdapter adapter = adapter(structure, VectorQuantizer.NONE, options(structure));
    assertEstimate(adapter, adapter.kernel().buildWithMemory(List.of(), 1));
  }

  @Test
  void flatEstimateAccountsForDictionarySharingNotInputStringIdentity() {
    VectorIndexAdapter adapter = adapter(VectorStructure.FLAT, VectorQuantizer.NONE, Map.of());
    IndexBuildResult built =
        adapter
            .kernel()
            .buildWithMemory(entries(adapter, 100, i -> new String(new char[] {'中', '文'})), 1);
    assertEstimate(adapter, built);
  }

  @Test
  void ivfPartitionCountChangesRetainedEstimate() {
    VectorIndexAdapter few =
        adapter(VectorStructure.IVF, VectorQuantizer.RABITQ, Map.of("nlist", "1", "nprobe", "1"));
    VectorIndexAdapter many =
        adapter(VectorStructure.IVF, VectorQuantizer.RABITQ, Map.of("nlist", "8", "nprobe", "1"));
    List<IndexEntry> entries = entries(few, 100, i -> "data.parquet");
    IndexBuildResult fewBuilt = few.kernel().buildWithMemory(entries, 1);
    IndexBuildResult manyBuilt = many.kernel().buildWithMemory(entries, 1);
    assertEstimate(few, fewBuilt);
    assertEstimate(many, manyBuilt);
    assertThat(manyBuilt.memory().preparedBytes()).isGreaterThan(fewBuilt.memory().preparedBytes());
  }

  @Test
  void hnswDegreeChangesRetainedGraphEstimate() {
    VectorIndexAdapter few =
        adapter(VectorStructure.HNSW, VectorQuantizer.NONE, Map.of("max-m", "4"));
    VectorIndexAdapter many =
        adapter(VectorStructure.HNSW, VectorQuantizer.NONE, Map.of("max-m", "16"));
    List<IndexEntry> entries = entries(few, 100, i -> "data.parquet");
    IndexBuildResult fewBuilt = few.kernel().buildWithMemory(entries, 1);
    IndexBuildResult manyBuilt = many.kernel().buildWithMemory(entries, 1);
    assertEstimate(few, fewBuilt);
    assertEstimate(many, manyBuilt);
    assertThat(manyBuilt.memory().preparedBytes()).isGreaterThan(fewBuilt.memory().preparedBytes());
    assertThat(manyBuilt.memory().runtimeProfile())
        .contains("processors=" + Runtime.getRuntime().availableProcessors());
  }

  @Test
  void hnswModelChargesAllocatedCapacity() {
    long small = HnswlibKernel.preparedBytes(100, 48, 100, DIMENSION, 8, 100, 1);
    long large = HnswlibKernel.preparedBytes(100, 48, 100, DIMENSION, 8, 200, 1);
    // 100 additional four-byte node slots and 16 extra bytes in each of two visited bitsets.
    assertThat(large - small).isEqualTo(100L * Integer.BYTES + 2L * 16);
  }

  @Test
  void hnswModelAndProfileChargeProcessorSizedVisitedPool() {
    long one = HnswlibKernel.preparedBytes(100, 48, 100, DIMENSION, 8, 100, 1);
    long two = HnswlibKernel.preparedBytes(100, 48, 100, DIMENSION, 8, 100, 2);
    // The extra bitset retains a 24-byte object and a 32-byte four-int array.
    assertThat(two - one).isEqualTo(24L + 32);
    assertThat(VectorMemoryEstimates.profile(1)).isNotEqualTo(VectorMemoryEstimates.profile(2));
  }

  @Test
  void legacyKernelDoesNotInventAnEstimateOrPrepareWhileBuilding() {
    VectorIndexAdapter adapter = adapter(VectorStructure.FLAT, VectorQuantizer.NONE, Map.of());
    VectorKernel delegate = adapter.kernel();
    VectorKernel legacy =
        new VectorKernel() {
          @Override
          public String name() {
            return "legacy";
          }

          @Override
          public KernelCapabilities capabilities() {
            return delegate.capabilities();
          }

          @Override
          public int payloadFormatVersion() {
            return delegate.payloadFormatVersion();
          }

          @Override
          public ByteBuffer build(List<IndexEntry> entries, int parallelism) {
            return delegate.build(entries, parallelism);
          }

          @Override
          public List<IndexEntry> entries(ByteBuffer payload) {
            return delegate.entries(payload);
          }

          @Override
          public PreparedSearch prepare(ByteBuffer payload) {
            throw new AssertionError("Building must not prepare a second retained structure");
          }
        };
    IndexBuildResult built = legacy.buildWithMemory(entries(adapter, 1, i -> "data.parquet"), 1);
    assertThat(built.memory()).isNull();
    assertThat(legacy.entries(built.payload()))
        .extracting(IndexEntry::rowPosition)
        .containsExactly(0L);
  }

  private static IndexBuildResult buildWithPreparationForbidden(
      VectorIndexAdapter adapter, List<IndexEntry> entries) {
    VectorKernel kernel = spy(adapter.kernel());
    doThrow(new AssertionError("Building must not prepare a second retained structure"))
        .when(kernel)
        .prepare(any(ByteBuffer.class));
    return kernel.buildWithMemory(entries, 1);
  }

  private static void assertEstimate(VectorIndexAdapter adapter, IndexBuildResult built) {
    assertThat(built.memory()).isNotNull();
    assertThat(built.memory().payloadBytes()).isEqualTo(built.payload().remaining());
    assertThat(built.memory().preparedBytes())
        .isEqualTo(adapter.kernel().prepare(built.payload()).estimatedBytes());
  }

  private static Map<String, String> options(VectorStructure structure) {
    return switch (structure) {
      case FLAT -> Map.of();
      case HNSW -> Map.of("max-m", "8", "ef-construction", "50");
      case IVF -> Map.of("nlist", "4", "nprobe", "1");
    };
  }

  private static VectorIndexAdapter adapter(
      VectorStructure structure, VectorQuantizer quantizer, Map<String, String> options) {
    return new VectorIndexAdapter(
        VectorIndexDefinition.builder()
            .fieldId(1)
            .dimension(DIMENSION)
            .metric(Metric.EUCLIDEAN)
            .structure(structure)
            .quantizer(quantizer)
            .properties(options)
            .build());
  }

  private static List<IndexEntry> entries(
      VectorIndexAdapter adapter, int count, IntFunction<String> path) {
    List<IndexEntry> entries = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      float[] vector = new float[DIMENSION];
      for (int d = 0; d < DIMENSION; d++) {
        vector[d] = (i + d) % 127;
      }

      entries.add(IndexEntry.of(path.apply(i), i, adapter.encodeVector(vector)));
    }

    return entries;
  }
}
