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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.index.IndexBuildResult;
import org.apache.iceberg.index.IndexEntry;
import org.apache.iceberg.index.IndexFileMemory;
import org.apache.iceberg.index.vector.VectorIndexDefinition.Metric;
import org.apache.iceberg.index.vector.VectorIndexDefinition.VectorQuantizer;
import org.apache.iceberg.index.vector.VectorIndexDefinition.VectorStructure;
import org.junit.jupiter.api.Test;

/**
 * What each kernel can and cannot price from the counts a build records.
 *
 * <p>These cases construct the kernel behind the registry rather than resolving one through it: a
 * kernel this test measures must be the kernel that priced the payload, and another test in this
 * module is free to register a factory of the same name.
 */
class TestVectorRetainedBytes {
  private static final int DIMENSION = 16;

  private static VectorIndexDefinition definition(
      VectorStructure structure, VectorQuantizer quantizer, Map<String, String> options) {
    return VectorIndexDefinition.builder()
        .fieldId(1)
        .dimension(DIMENSION)
        .metric(Metric.EUCLIDEAN)
        .structure(structure)
        .quantizer(quantizer)
        .properties(options)
        .build();
  }

  /**
   * The figures one production index recorded, taken from its manifest: 100M rows of 128 dimensions
   * in 256 files of 390,625 rows, 3-bit RaBitQ over 625 lists.
   *
   * <p>A recomputation that does not land on the recorded number to the byte is a formula that
   * describes something other than what the build measured, so this is the case that says the
   * resolver can price a file without reading it.
   */
  @Test
  void reproducesTheFigureAProductionBuildRecorded() {
    VectorIndexDefinition production =
        VectorIndexDefinition.builder()
            .fieldId(2)
            .dimension(128)
            .metric(Metric.EUCLIDEAN)
            .structure(VectorStructure.IVF)
            .quantizer(VectorQuantizer.RABITQ)
            .properties(Map.of("nprobe", "32", "rabitq-bits", "3", "nlist", "625"))
            .build();
    RetainedBytes kernel = (RetainedBytes) new IvfKernel.QuantizedFactory().create(production);

    long payloadBytes = 28_767_710L;
    long preparedBytes = 30_130_688L;
    Map<String, Long> structure =
        Map.of(
            "code-bytes", 48L,
            "data-file-count", 1L,
            "data-file-string-bytes", 208L,
            "dimension", 128L,
            "entry-count", 390_625L,
            "list-count", 625L,
            "list-directory-bytes", 5_040L,
            "payload-bytes", payloadBytes,
            "quantizer-bits", 3L,
            "quantizer-bytes", 690_000L);

    assertThat(kernel.retainedBytes(payloadBytes, structure))
        .as("the counts a build records price the file it wrote")
        .isEqualTo(preparedBytes);
  }

  @Test
  void aFlatScanRepricesItsPayload() {
    RetainedBytes kernel =
        (RetainedBytes)
            new BruteforceKernel.Factory()
                .create(definition(VectorStructure.FLAT, VectorQuantizer.NONE, Map.of()));
    IndexFileMemory memory = memory((VectorKernel) kernel, 100);

    assertThat(kernel.retainedBytes(memory.payloadBytes(), memory.structure()))
        .as("the same formula over the same counts is the same number of bytes")
        .isEqualTo(memory.preparedBytes());
  }

  @Test
  void anUnquantizedIvfRepricesItsPayload() {
    RetainedBytes kernel =
        (RetainedBytes)
            new IvfKernel.Factory()
                .create(
                    definition(
                        VectorStructure.IVF,
                        VectorQuantizer.NONE,
                        Map.of("nlist", "4", "nprobe", "1")));
    IndexFileMemory memory = memory((VectorKernel) kernel, 100);

    assertThat(kernel.retainedBytes(memory.payloadBytes(), memory.structure()))
        .isEqualTo(memory.preparedBytes());
  }

  @Test
  void aQuantizedIvfRepricesItsPayload() {
    RetainedBytes kernel =
        (RetainedBytes)
            new IvfKernel.QuantizedFactory()
                .create(
                    definition(
                        VectorStructure.IVF,
                        VectorQuantizer.RABITQ,
                        Map.of("nlist", "4", "nprobe", "1", "rabitq-bits", "2")));
    IndexFileMemory memory = memory((VectorKernel) kernel, 100);

    assertThat(kernel.retainedBytes(memory.payloadBytes(), memory.structure()))
        .isEqualTo(memory.preparedBytes());
  }

  @Test
  void aGraphIsNotRecomputableFromTheCountsItRecords() {
    VectorKernel kernel =
        new HnswlibKernel.Factory()
            .create(
                definition(
                    VectorStructure.HNSW,
                    VectorQuantizer.NONE,
                    Map.of("max-m", "8", "ef-construction", "50")));
    IndexFileMemory memory = memory((VectorKernel) kernel, 100);

    // The counts are there, but they do not describe the graph: its retained size also depends on
    // how many layers each node occupies and on how much neighbour capacity every layer list was
    // allocated, which only a loaded graph states.
    assertThat(memory.structure())
        .containsKeys("mapping-count", "node-count", "dimension", "max-m", "max-count");
    assertThat(kernel)
        .as("a graph is measured rather than derived, so it prices nothing from a record")
        .isNotInstanceOf(RetainedBytes.class);
  }

  @Test
  void aRecordThatDoesNotStateItsShapeIsUnknown() {
    RetainedBytes kernel =
        (RetainedBytes)
            new IvfKernel.Factory()
                .create(
                    definition(
                        VectorStructure.IVF,
                        VectorQuantizer.NONE,
                        Map.of("nlist", "4", "nprobe", "1")));
    IndexFileMemory memory = memory((VectorKernel) kernel, 100);
    Map<String, Long> incomplete =
        Map.of(
            "dimension", memory.structure().get("dimension"),
            "data-file-count", memory.structure().get("data-file-count"));

    assertThat(kernel.retainedBytes(memory.payloadBytes(), incomplete))
        .as("a count the formula needs is missing, so the payload cannot be priced")
        .isZero();
  }

  @Test
  void aRecordWithoutStructureIsUnknown() {
    RetainedBytes kernel =
        (RetainedBytes)
            new IvfKernel.Factory()
                .create(
                    definition(
                        VectorStructure.IVF,
                        VectorQuantizer.NONE,
                        Map.of("nlist", "4", "nprobe", "1")));
    IndexFileMemory memory = memory((VectorKernel) kernel, 100);

    assertThat(kernel.retainedBytes(memory.payloadBytes(), Map.of()))
        .as("no counts means no price, which the adapter reports as unknown rather than free")
        .isZero();
    // The kernel prices whatever counts it is given; refusing a payload of no length is the
    // adapter's guard, because a file whose length is unknown cannot be planned either.
    assertThat(kernel.retainedBytes(0, memory.structure())).isPositive();
  }

  private static IndexFileMemory memory(VectorKernel kernel, int count) {
    IndexBuildResult built = kernel.buildWithMemory(entries(kernel, count), 1);
    assertThat(built.memory()).isNotNull();
    return built.memory();
  }

  private static List<IndexEntry> entries(VectorKernel kernel, int count) {
    List<IndexEntry> entries = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      float[] vector = new float[DIMENSION];
      for (int d = 0; d < DIMENSION; d++) {
        vector[d] = (i + d) % 127;
      }

      entries.add(
          IndexEntry.of(
              "data-" + (i % 4) + ".parquet",
              i,
              VectorPayloads.encodeVector(vector, DIMENSION, "Test vector")));
    }

    return entries;
  }
}
