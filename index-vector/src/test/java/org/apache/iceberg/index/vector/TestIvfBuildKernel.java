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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Stream;
import org.apache.iceberg.index.IndexBuildResult;
import org.apache.iceberg.index.IndexEntry;
import org.apache.iceberg.index.vector.VectorIndexDefinition.Metric;
import org.apache.iceberg.index.vector.VectorIndexDefinition.VectorQuantizer;
import org.apache.iceberg.index.vector.VectorIndexDefinition.VectorStructure;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The definition's {@code ivf-build-kernel} selects the kernel a build runs, and the native one
 * writes the payload the Java one writes.
 *
 * <p>Each case builds the same entries through two definitions that differ only in that property
 * and compares the payloads byte for byte, plus the structure except the flag that says which
 * kernel ran. That flag is what makes a fallback visible: a native definition whose payload matches
 * but whose flag is zero silently ran the Java kernel, which is a failure of this test too.
 *
 * <p>The cases skip while the library is not built, and fail rather than skip once it is.
 */
class TestIvfBuildKernel {

  private static final String SIMD_ASSIGN_FLAG = "assign-kernel-simd";

  private static final String NATIVE_FLAG = "build-kernel-native";

  private static final List<String> DATA_FILES =
      List.of("文件/中文-名的数据文件.parquet", "nul\u0000in-the-middle.parquet", "plain.parquet");

  @BeforeAll
  static void loadNativeLibrary() {
    VectorIndexTypes.register();
    if (NativeIvfBuild.available()) {
      return;
    }

    Path library =
        Path.of(
            System.getProperty("user.dir"),
            "native",
            "target",
            "debug",
            System.mapLibraryName(NativeIvfBuild.LIBRARY_NAME));
    Assumptions.assumeTrue(
        Files.isRegularFile(library), "The native IVF kernel is not built: " + library);
    NativeIvfBuild.load(library);
  }

  @Test
  void defaultBuildsWithTheJavaKernel() {
    VectorIndexAdapter adapter = adapter(8, VectorQuantizer.NONE, Metric.L2, Map.of());

    IndexBuildResult built = build(adapter, entries(adapter, 8, 40));

    assertThat(structure(built))
        .as("a definition that sets no build kernel runs the Java one")
        .containsEntry(NATIVE_FLAG, 0L);
  }

  static Stream<Arguments> nativeCases() {
    List<Arguments> cases = new ArrayList<>();
    for (int dimension : List.of(17, 256)) {
      for (VectorQuantizer quantizer : VectorQuantizer.values()) {
        for (boolean listBlocks : List.of(false, true)) {
          cases.add(Arguments.of(dimension, quantizer, listBlocks));
        }
      }
    }

    return cases.stream();
  }

  @ParameterizedTest(name = "dimension={0}, quantizer={1}, listBlocks={2}")
  @MethodSource("nativeCases")
  void nativeKernelWritesTheJavaPayload(
      int dimension, VectorQuantizer quantizer, boolean listBlocks) {
    Map<String, String> properties = new LinkedHashMap<>();
    properties.put(IvfOptions.LIST_BLOCKS, Boolean.toString(listBlocks));
    VectorIndexAdapter javaAdapter = adapter(dimension, quantizer, Metric.L2, properties);

    Map<String, String> nativeProperties = new LinkedHashMap<>(properties);
    nativeProperties.put(IvfOptions.BUILD_KERNEL, "native");
    VectorIndexAdapter nativeAdapter = adapter(dimension, quantizer, Metric.L2, nativeProperties);

    List<IndexEntry> entries =
        entries(javaAdapter, dimension, quantizer == VectorQuantizer.NONE ? 120 : 300);
    IndexBuildResult javaBuilt = build(javaAdapter, entries);
    IndexBuildResult nativeBuilt = build(nativeAdapter, entries);

    assertThat(structure(nativeBuilt))
        .as("a definition that asks for the native kernel records that it ran")
        .containsEntry(NATIVE_FLAG, 1L);
    assertThat(bytes(nativeBuilt.payload()))
        .as("%s, dimension %s: the native kernel writes the Java payload", quantizer, dimension)
        .isEqualTo(bytes(javaBuilt.payload()));

    Map<String, Long> expected = new LinkedHashMap<>(structure(javaBuilt));
    expected.put(NATIVE_FLAG, 1L);
    assertThat(structure(nativeBuilt))
        .as("%s, dimension %s: the structure other than the kernel that ran", quantizer, dimension)
        .containsExactlyInAnyOrderEntriesOf(expected);
  }

  @Test
  void nativeIsRecordedInStructure() {
    VectorIndexAdapter adapter =
        adapter(8, VectorQuantizer.NONE, Metric.L2, Map.of(IvfOptions.BUILD_KERNEL, "native"));

    IndexBuildResult built = build(adapter, entries(adapter, 8, 40));

    assertThat(structure(built))
        .as("a build says which kernel wrote its payload")
        .containsEntry(NATIVE_FLAG, 1L)
        .containsEntry(SIMD_ASSIGN_FLAG, 0L);
  }

  @Test
  void fallsBackForCosine() {
    VectorIndexAdapter javaAdapter = adapter(16, VectorQuantizer.NONE, Metric.COSINE, Map.of());
    VectorIndexAdapter nativeAdapter =
        adapter(16, VectorQuantizer.NONE, Metric.COSINE, Map.of(IvfOptions.BUILD_KERNEL, "native"));

    List<IndexEntry> entries = entries(javaAdapter, 16, 80);
    IndexBuildResult javaBuilt = build(javaAdapter, entries);
    IndexBuildResult nativeBuilt = build(nativeAdapter, entries);

    assertThat(structure(nativeBuilt))
        .as("the native kernel does not build a cosine payload, so the Java one did")
        .containsEntry(NATIVE_FLAG, 0L);
    assertThat(bytes(nativeBuilt.payload()))
        .as("the Java kernel writes the same payload a Java definition gets")
        .isEqualTo(bytes(javaBuilt.payload()));
  }

  @Test
  void acceptsTheBuildKernelCaseInsensitively() {
    assertThat(IvfOptions.from(Map.of(IvfOptions.BUILD_KERNEL, " Native ")).nativeBuildKernel())
        .as("the value is read case-insensitively and without its whitespace")
        .isTrue();
    assertThat(IvfOptions.from(Map.of(IvfOptions.BUILD_KERNEL, "JAVA")).nativeBuildKernel())
        .as("a definition that names the Java kernel keeps it")
        .isFalse();
    assertThat(IvfOptions.from(Map.of()).nativeBuildKernel())
        .as("a definition that sets nothing runs the Java kernel")
        .isFalse();
  }

  @Test
  void rejectsAnUnknownBuildKernel() {
    assertThatThrownBy(() -> IvfOptions.from(Map.of(IvfOptions.BUILD_KERNEL, "gpu")))
        .as("a build kernel nothing implements")
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(IvfOptions.BUILD_KERNEL)
        .hasMessageContaining("gpu");
  }

  @Test
  void switchingTheKernelOnAnExistingAdapterKeepsThePayload() {
    VectorIndexDefinition javaDefinition =
        definition(
            64,
            VectorQuantizer.RABITQ,
            Metric.L2,
            Map.of(IvfOptions.NLIST, "7", IvfOptions.BUILD_KERNEL, "java"));
    VectorIndexAdapter javaAdapter = new VectorIndexAdapter(javaDefinition);
    VectorIndexAdapter nativeAdapter =
        new VectorIndexAdapter(switchKernel(javaDefinition, "native"));

    List<IndexEntry> entries = entries(javaAdapter, 64, 200);
    IndexBuildResult javaBuilt = build(javaAdapter, entries);
    IndexBuildResult nativeBuilt = build(nativeAdapter, entries);

    assertThat(bytes(nativeBuilt.payload()))
        .as("switching the property does not change what the index contains")
        .isEqualTo(bytes(javaBuilt.payload()));
  }

  @ParameterizedTest(name = "quantizer={0}")
  @EnumSource(VectorQuantizer.class)
  void theNativeKernelReadsTheEntryPayloadsAsTheyAre(VectorQuantizer quantizer) {
    VectorIndexAdapter adapter = adapter(64, quantizer, Metric.L2, Map.of(IvfOptions.NLIST, "7"));
    List<IndexEntry> entries = entries(adapter, 64, 200);

    NativeIvfBuild.Built built =
        NativeIvfBuild.buildFromEntries((IvfKernel) adapter.kernel(), entries, 2);
    byte[] javaBytes = bytes(adapter.encodeAuxiliaryPayloadWithMemory(entries, 1).payload());

    assertThat(built.entryCount())
        .as("the entries the native build read")
        .isEqualTo(entries.size());
    assertThat(bytes(built.payload()))
        .as("%s: the payload built from the entry payloads as they are", quantizer)
        .isEqualTo(javaBytes);
  }

  @Test
  void nativeKernelWritesTheJavaPayloadAtTheProductDimension() {
    // 2,560 is the dimension of the tables in use, and a directory with 4-bit codes is what a
    // product unit sets: this runs the production native path over the payloads those entries hold
    Map<String, String> properties = new LinkedHashMap<>();
    properties.put(IvfOptions.LIST_BLOCKS, "true");
    properties.put(RaBitQQuantizer.BITS, "4");
    VectorIndexAdapter javaAdapter = adapter(2560, VectorQuantizer.RABITQ, Metric.L2, properties);

    Map<String, String> nativeProperties = new LinkedHashMap<>(properties);
    nativeProperties.put(IvfOptions.BUILD_KERNEL, "native");
    VectorIndexAdapter nativeAdapter =
        adapter(2560, VectorQuantizer.RABITQ, Metric.L2, nativeProperties);

    List<IndexEntry> entries = entries(javaAdapter, 2560, 300);
    IndexBuildResult javaBuilt = build(javaAdapter, entries);
    IndexBuildResult nativeBuilt = build(nativeAdapter, entries);

    assertThat(structure(nativeBuilt))
        .as("the production native path wrote the payload")
        .containsEntry(NATIVE_FLAG, 1L);
    assertThat(bytes(nativeBuilt.payload()))
        .as("the native kernel writes the Java payload from the entry payloads")
        .isEqualTo(bytes(javaBuilt.payload()));
  }

  @Test
  void treatsALibraryOfAnotherAbiVersionAsUnusable() {
    assertThat(NativeIvfBuild.versionMismatch(NativeIvfBuild.ABI_VERSION, "the library"))
        .as("a library that speaks this bridge's ABI version")
        .isNull();
    assertThat(NativeIvfBuild.versionMismatch(NativeIvfBuild.ABI_VERSION - 1, "the library"))
        .as("an old library is reported instead of being handed a frame it cannot read")
        .contains("speaks native IVF ABI version " + (NativeIvfBuild.ABI_VERSION - 1))
        .contains("this bridge reads " + NativeIvfBuild.ABI_VERSION);
  }

  private static IndexBuildResult build(VectorIndexAdapter adapter, List<IndexEntry> entries) {
    IndexBuildResult built = adapter.encodeAuxiliaryPayloadWithMemory(entries, 2);
    assertThat(built.memory()).as("a build records what its payload costs").isNotNull();
    return built;
  }

  private static Map<String, Long> structure(IndexBuildResult built) {
    return built.memory().structure();
  }

  private static byte[] bytes(ByteBuffer payload) {
    ByteBuffer copy = payload.duplicate();
    byte[] bytes = new byte[copy.remaining()];
    copy.get(bytes);
    return bytes;
  }

  private static VectorIndexAdapter adapter(
      int dimension, VectorQuantizer quantizer, Metric metric, Map<String, String> properties) {
    return new VectorIndexAdapter(definition(dimension, quantizer, metric, properties));
  }

  private static VectorIndexDefinition definition(
      int dimension, VectorQuantizer quantizer, Metric metric, Map<String, String> properties) {
    return VectorIndexDefinition.builder()
        .fieldId(1)
        .dimension(dimension)
        .metric(metric)
        .structure(VectorStructure.IVF)
        .quantizer(quantizer)
        .properties(properties)
        .build();
  }

  /** The same definition with only the build kernel replaced. */
  private static VectorIndexDefinition switchKernel(
      VectorIndexDefinition definition, String buildKernel) {
    Map<String, String> properties = new LinkedHashMap<>(definition.properties());
    properties.put(IvfOptions.BUILD_KERNEL, buildKernel);
    return VectorIndexDefinition.builder()
        .fieldId(definition.fieldId())
        .dimension(definition.dimension())
        .metric(definition.metric())
        .structure(definition.structure())
        .quantizer(definition.quantizer())
        .properties(properties)
        .build();
  }

  private static List<IndexEntry> entries(VectorIndexAdapter adapter, int dimension, int count) {
    Random random = new Random(31L * count + dimension);
    List<IndexEntry> entries = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      float[] vector = new float[dimension];
      for (int d = 0; d < dimension; d++) {
        vector[d] = random.nextFloat() * 2.0F - 1.0F;
      }

      entries.add(
          IndexEntry.of(DATA_FILES.get(i % DATA_FILES.size()), i, adapter.encodeVector(vector)));
    }

    return entries;
  }
}
