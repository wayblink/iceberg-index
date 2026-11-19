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
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Stream;
import org.apache.iceberg.index.IndexBuildResult;
import org.apache.iceberg.index.IndexEntry;
import org.apache.iceberg.index.vector.NativeIvfBuild.Built;
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
 * The native IVF build kernel writes the bytes the Java one writes.
 *
 * <p>Each case builds the same entries twice, once with the Java kernel and once with the native
 * kernel, and compares the payloads byte for byte: the two implementations share no code, so only
 * identical inputs and identical arithmetic produce identical bytes. The parameters the Java kernel
 * reads from a definition are what the native one is handed, so the matrix covers the shapes and
 * options a definition can ask for.
 *
 * <p>The cases skip while the library is not built, and fail rather than skip once it is.
 */
class TestNativeIvfBuild {
  private static final List<String> DATA_FILES =
      List.of("data-file-0.parquet", "data-file-1.parquet", "data-file-2.parquet");

  @BeforeAll
  static void loadNativeLibrary() {
    VectorIndexTypes.register();
    Path library =
        Path.of(
            System.getProperty("user.dir"),
            "native",
            "target",
            "debug",
            System.mapLibraryName("iceberg_ivf_native"));
    Assumptions.assumeTrue(
        Files.isRegularFile(library), "The native IVF kernel is not built: " + library);
    NativeIvfBuild.load(library);
    assertThat(NativeIvfBuild.available()).as("the native library is loaded").isTrue();
  }

  @Test
  void theNativeLibraryReportsItsAbiVersion() {
    assertThat(NativeIvfBuild.version())
        .as("the ABI version the native library reports")
        .isEqualTo(NativeIvfBuild.ABI_VERSION);
  }

  static Stream<Arguments> plainCases() {
    List<Arguments> cases = new ArrayList<>();
    for (int dimension : List.of(17, 100)) {
      for (int nlist : List.of(1, 7, 0)) {
        for (boolean listBlocks : List.of(false, true)) {
          cases.add(Arguments.of(dimension, nlist, listBlocks));
        }
      }
    }

    return cases.stream();
  }

  @ParameterizedTest(name = "dimension={0}, nlist={1}, listBlocks={2}")
  @MethodSource("plainCases")
  void thePayloadMatchesTheJavaKernelForPlainVectors(int dimension, int nlist, boolean listBlocks) {
    VectorIndexAdapter adapter =
        adapter(dimension, VectorQuantizer.NONE, options(nlist, listBlocks));
    assertSamePayload(adapter, entries(adapter, dimension, 100, DATA_FILES), "plain vectors");
  }

  static Stream<Arguments> quantizedCases() {
    List<Arguments> cases = new ArrayList<>();
    for (int dimension : List.of(17, 100)) {
      for (int nlist : List.of(7, 23, 0)) {
        // 1 and 2 are the defaults and the common settings; 3 and 5 make a level cross a byte
        // boundary, and 4 is what the product tables are built with
        for (int bits : List.of(1, 2, 3, 4, 5)) {
          for (boolean listBlocks : List.of(false, true)) {
            cases.add(Arguments.of(dimension, nlist, bits, listBlocks));
          }
        }
      }
    }

    return cases.stream();
  }

  @ParameterizedTest(name = "dimension={0}, nlist={1}, bits={2}, listBlocks={3}")
  @MethodSource("quantizedCases")
  void thePayloadMatchesTheJavaKernelForQuantizedVectors(
      int dimension, int nlist, int bits, boolean listBlocks) {
    Map<String, String> properties = options(nlist, listBlocks);
    // one bit per dimension is the default the definition takes when nothing is set
    if (bits != 1) {
      properties.put(RaBitQQuantizer.BITS, Integer.toString(bits));
    }

    VectorIndexAdapter adapter = adapter(dimension, VectorQuantizer.RABITQ, properties);
    assertSamePayload(adapter, entries(adapter, dimension, 1000, DATA_FILES), "quantized vectors");
  }

  static Stream<Arguments> trainingBudgets() {
    return Stream.of(
        Arguments.of("default budget", Map.of()),
        Arguments.of("eight samples per list", Map.of(IvfOptions.TRAIN_SAMPLES_PER_LIST, "8")),
        Arguments.of("three iterations", Map.of(IvfOptions.TRAIN_MAX_ITERATIONS, "3")),
        Arguments.of(
            "a convergence tolerance", Map.of(IvfOptions.TRAIN_CONVERGENCE_TOLERANCE, "0.001")),
        Arguments.of(
            "all of them",
            Map.of(
                IvfOptions.TRAIN_SAMPLES_PER_LIST,
                "8",
                IvfOptions.TRAIN_MAX_ITERATIONS,
                "3",
                IvfOptions.TRAIN_CONVERGENCE_TOLERANCE,
                "0.001")));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("trainingBudgets")
  void thePayloadMatchesTheJavaKernelForTheTrainingBudget(
      String description, Map<String, String> trainingBudget) {
    Map<String, String> properties = new LinkedHashMap<>(trainingBudget);
    properties.put(IvfOptions.NLIST, "5");
    VectorIndexAdapter adapter = adapter(32, VectorQuantizer.NONE, properties);
    assertSamePayload(adapter, entries(adapter, 32, 200, DATA_FILES), description);
  }

  @ParameterizedTest(name = "{0}")
  @EnumSource(VectorQuantizer.class)
  void thePayloadMatchesTheJavaKernelForUnusualDataFilePaths(VectorQuantizer quantizer) {
    List<String> dataFiles =
        List.of(
            "文件/中文-名的数据文件.parquet",
            "nul\u0000in-the-middle.parquet",
            "emoji-\uD83D\uDE00-data.parquet",
            "plain.parquet");
    VectorIndexAdapter adapter = adapter(17, quantizer, options(7, false));
    assertSamePayload(adapter, entries(adapter, 17, 50, dataFiles), "the data file dictionary");
  }

  @Test
  void thePayloadMatchesTheJavaKernelForAProductShapedUnit() {
    // a derived list count and a directory, at the size one data file of a build reaches
    VectorIndexAdapter adapter = adapter(256, VectorQuantizer.NONE, options(0, true));
    assertSamePayload(
        adapter, entries(adapter, 256, 5000, DATA_FILES), "5000 vectors of 256 dimensions");
  }

  @Test
  void thePayloadMatchesTheJavaKernelForTheProductDimension() {
    // 2560 is the dimension of the tables in use; its codes cover a padded transform of 4096
    VectorIndexAdapter adapter = adapter(2560, VectorQuantizer.RABITQ, options(17, false));
    assertSamePayload(
        adapter, entries(adapter, 2560, 300, DATA_FILES), "300 codes of 2560 dimensions");
  }

  @Test
  void thePayloadMatchesTheJavaKernelWhenListsStayEmpty() {
    // every vector is the same point, so every centroid but one ends up with no entries and the
    // payload still has to write those lists, quantized centre and all
    VectorIndexAdapter adapter = adapter(8, VectorQuantizer.RABITQ, options(7, false));
    assertSamePayload(
        adapter, identicalEntries(adapter, 8, 50, DATA_FILES), "lists nothing lands in");
  }

  @ParameterizedTest(name = "{0}")
  @EnumSource(VectorQuantizer.class)
  void thePayloadDoesNotDependOnTheNativeThreadCount(VectorQuantizer quantizer) {
    VectorIndexAdapter adapter = adapter(32, quantizer, options(7, false));
    List<IndexEntry> entries = entries(adapter, 32, 300, DATA_FILES);

    Built single = NativeIvfBuild.build(adapter, entries, 1);
    Built many = NativeIvfBuild.build(adapter, entries, 4);

    assertThat(many.payload())
        .as("%s: one thread and four threads write the same bytes", quantizer)
        .isEqualTo(single.payload());
  }

  private static void assertSamePayload(
      VectorIndexAdapter adapter, List<IndexEntry> entries, String description) {
    IndexBuildResult java = adapter.encodeAuxiliaryPayloadWithMemory(entries, 1);
    byte[] javaBytes = bytes(java.payload());
    Built nativeBuild = NativeIvfBuild.build(adapter, entries, 1);
    byte[] nativeBytes = nativeBuild.payload();

    assertThat(java.memory())
        .as("%s: the Java kernel reports what the payload remembers", description)
        .isNotNull();
    Map<String, Long> structure = java.memory().structure();
    assertThat(nativeBuild.listCount())
        .as("%s: the native list count against the Java structure", description)
        .isEqualTo(structure.get("list-count").intValue());
    assertThat(nativeBuild.sampleCount())
        .as("%s: the native training sample count against the Java structure", description)
        .isEqualTo(structure.get("train-samples").intValue());
    assertThat(nativeBuild.iterations())
        .as("%s: the native training iterations against the Java structure", description)
        .isEqualTo(structure.get("train-iterations").intValue());
    assertThat(nativeBuild.entryCount())
        .as("%s: the native entry count", description)
        .isEqualTo(entries.size());

    assertThat(nativeBytes)
        .as(
            "%s: the native payload against the Java payload (java %s bytes, sha256 %s; native %s"
                + " bytes, sha256 %s)",
            description,
            javaBytes.length,
            sha256(javaBytes),
            nativeBytes.length,
            sha256(nativeBytes))
        .isEqualTo(javaBytes);
  }

  private static byte[] bytes(ByteBuffer payload) {
    ByteBuffer copy = payload.duplicate();
    byte[] bytes = new byte[copy.remaining()];
    copy.get(bytes);
    return bytes;
  }

  private static String sha256(byte[] bytes) {
    try {
      return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is not available", e);
    }
  }

  private static Map<String, String> options(int nlist, boolean listBlocks) {
    Map<String, String> properties = new LinkedHashMap<>();
    if (nlist > 0) {
      properties.put(IvfOptions.NLIST, Integer.toString(nlist));
    }

    properties.put(IvfOptions.LIST_BLOCKS, Boolean.toString(listBlocks));
    return properties;
  }

  private static VectorIndexAdapter adapter(
      int dimension, VectorQuantizer quantizer, Map<String, String> properties) {
    return new VectorIndexAdapter(
        VectorIndexDefinition.builder()
            .fieldId(1)
            .dimension(dimension)
            .metric(Metric.L2)
            .structure(VectorStructure.IVF)
            .quantizer(quantizer)
            .properties(properties)
            .build());
  }

  private static List<IndexEntry> identicalEntries(
      VectorIndexAdapter adapter, int dimension, int count, List<String> dataFiles) {
    float[] vector = new float[dimension];
    for (int d = 0; d < dimension; d++) {
      vector[d] = (float) Math.sin(d * 0.3);
    }

    byte[] encoded = adapter.encodeVector(vector);
    List<IndexEntry> entries = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      entries.add(IndexEntry.of(dataFiles.get(i % dataFiles.size()), i, encoded));
    }

    return entries;
  }

  private static List<IndexEntry> entries(
      VectorIndexAdapter adapter, int dimension, int count, List<String> dataFiles) {
    Random random = new Random(31L * count + dimension);
    List<IndexEntry> entries = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      float[] vector = new float[dimension];
      for (int d = 0; d < dimension; d++) {
        vector[d] = random.nextFloat() * 2.0F - 1.0F;
      }

      entries.add(
          IndexEntry.of(dataFiles.get(i % dataFiles.size()), i, adapter.encodeVector(vector)));
    }

    return entries;
  }
}
