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
import java.util.Map;
import java.util.Random;
import org.apache.iceberg.index.IndexBuildResult;
import org.apache.iceberg.index.IndexEntry;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The assignment kernel a definition selects, and what a build does when it cannot run it.
 *
 * <p>Choosing the kernel is a build-time choice: the default stays exact, writing {@code exact}
 * produces the bytes the definition without the property produces, and asking for {@code simd} runs
 * the vector API kernel when this JVM can load it and falls back to the exact kernel when it
 * cannot. Both outcomes are recorded in the structure counts, so a payload says what built it.
 */
class TestIvfAssignKernel {

  private static final String ASSIGN_KERNEL = IvfOptions.ASSIGN_KERNEL;

  private static final int DIMENSION = 32;
  private static final int ENTRIES = 400;

  @BeforeAll
  static void registerVectorIndexType() {
    VectorIndexTypes.register();
  }

  @Test
  void readsTheAssignmentKernelFromTheDefinition() {
    assertThat(IvfOptions.keys())
        .as("the keys a caller may write into an IVF definition")
        .contains(ASSIGN_KERNEL);

    assertThat(IvfOptions.from(Map.of()).simdAssignKernel())
        .as("a definition that says nothing keeps the exact kernel")
        .isFalse();
    assertThat(IvfOptions.from(Map.of(ASSIGN_KERNEL, "exact")).simdAssignKernel()).isFalse();
    assertThat(IvfOptions.from(Map.of(ASSIGN_KERNEL, "simd")).simdAssignKernel()).isTrue();
    assertThat(IvfOptions.from(Map.of(ASSIGN_KERNEL, "SIMD")).simdAssignKernel())
        .as("the value is read case-insensitively")
        .isTrue();
    assertThat(IvfOptions.from(Map.of(ASSIGN_KERNEL, " Simd ")).simdAssignKernel())
        .as("surrounding whitespace is ignored")
        .isTrue();
  }

  @Test
  void refusesAnAssignKernelItDoesNotKnow() {
    for (String value : new String[] {"cuda", "vector", "", " ", "simd2", "fastest"}) {
      assertThatThrownBy(() -> IvfOptions.from(Map.of(ASSIGN_KERNEL, value)))
          .as("an assignment kernel %s", value)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining(ASSIGN_KERNEL)
          .hasMessageContaining("exact or simd");
    }
  }

  @Test
  void writingTheExactKernelIsTheSameAsWritingNothing() {
    List<IndexEntry> entries = entries(spreadVectors());

    assertThat(payload(Map.of(ASSIGN_KERNEL, "exact"), entries))
        .as("the default kernel is exact, so spelling it out cannot change the payload")
        .isEqualTo(payload(Map.of(), entries));
    assertThat(structure(Map.of(ASSIGN_KERNEL, "exact"), entries))
        .containsEntry("assign-kernel-simd", 0L);
  }

  @Test
  void aBuildWithTheDefaultKernelRecordsTheExactKernel() {
    Map<String, Long> structure = structure(Map.of(), entries(spreadVectors()));

    assertThat(structure)
        .as("the structure records the kernel the build actually ran")
        .containsEntry("assign-kernel-simd", 0L);
  }

  @Test
  void withTheModuleTheSimdKernelAssignsNearlyEveryRowLikeTheExactKernel() {
    Assumptions.assumeTrue(
        vectorApiAvailable(), "the vector API is not on this JVM's module graph");

    Random random = new Random(3141L);
    float[][] vectors = randomVectors(random, 2000, 64);
    float[][] centroids = randomVectors(random, 64, 64);

    int[] exact = IvfKernel.nearestCentroidL2(vectors, centroids, 4);
    int[] simd = IvfKernel.nearestCentroidL2Simd(vectors, centroids, 4);

    assertThat(simd).as("the kernel is loadable, so it must run").isNotNull().hasSameSizeAs(exact);
    int different = 0;
    for (int i = 0; i < exact.length; i++) {
      if (exact[i] != simd[i]) {
        different += 1;
      }
    }

    assertThat(different / (double) exact.length)
        .as("rows the float kernel assigns to a different list than the exact kernel")
        .isLessThanOrEqualTo(0.001);
    assertThat(IvfKernel.nearestCentroidL2Simd(vectors, centroids, 1))
        .as("the assignment does not depend on how many threads walk it")
        .containsExactly(simd);
    assertThat(IvfKernel.nearestCentroidL2Simd(vectors, centroids, 4))
        .as("a rebuild of the same input assigns the same lists")
        .containsExactly(simd);
  }

  @Test
  void theSimdKernelKeepsTheEarlierListOnAnEqualDistance() {
    Assumptions.assumeTrue(
        vectorApiAvailable(), "the vector API is not on this JVM's module graph");

    float[][] vectors = {{0.0f, 0.0f, 0.0f, 0.0f}};
    float[][] tieForNearest = {
      {3.0f, 0.0f, 0.0f, 0.0f},
      {1.0f, 0.0f, 0.0f, 0.0f},
      {-1.0f, 0.0f, 0.0f, 0.0f},
    };
    assertThat(IvfKernel.nearestCentroidL2Simd(vectors, tieForNearest, 1)).containsExactly(1);

    float[][] allEqual = {
      {1.0f, 0.0f, 0.0f, 0.0f},
      {1.0f, 0.0f, 0.0f, 0.0f},
      {1.0f, 0.0f, 0.0f, 0.0f},
    };
    assertThat(IvfKernel.nearestCentroidL2Simd(vectors, allEqual, 1))
        .as("equal distances keep the first list, as the exact kernel does")
        .containsExactly(0);
  }

  @Test
  void theSimdKernelMatchesTheExactKernelWhenASquareOverflows() {
    Assumptions.assumeTrue(
        vectorApiAvailable(), "the vector API is not on this JVM's module graph");

    // every float square here is infinite: the sqrt of the double sum still orders the lists,
    // while a float sum makes both distances infinity and the first list wins
    float[][] centroids = {{0.0f, 0.0f, 0.0f, 0.0f}, {2.0e20f, 0.0f, 0.0f, 0.0f}};
    float[][] one = {{3.0e20f, 0.0f, 0.0f, 0.0f}};
    assertThat(IvfKernel.nearestCentroidL2(one, centroids, 1)).containsExactly(1);
    assertThat(IvfKernel.nearestCentroidL2Simd(one, centroids, 1))
        .as("the one-row path")
        .containsExactly(1);

    float[][] block = {
      {3.0e20f, 0.0f, 0.0f, 0.0f},
      {3.0e20f, 0.0f, 0.0f, 0.0f},
      {3.0e20f, 0.0f, 0.0f, 0.0f},
      {3.0e20f, 0.0f, 0.0f, 0.0f}
    };
    assertThat(IvfKernel.nearestCentroidL2(block, centroids, 1)).containsExactly(1, 1, 1, 1);
    assertThat(IvfKernel.nearestCentroidL2Simd(block, centroids, 1))
        .as("the four-row path")
        .containsExactly(1, 1, 1, 1);
  }

  @Test
  void theSimdKernelMatchesTheExactKernelWhenASquareUnderflows() {
    Assumptions.assumeTrue(
        vectorApiAvailable(), "the vector API is not on this JVM's module graph");

    // a float square underflows to zero, which would leave both distances equal at zero; the
    // double sum keeps them apart and puts the row under the second list
    float[][] centroids = {{0.0f, 0.0f, 0.0f, 0.0f}, {1.0e-30f, 0.0f, 0.0f, 0.0f}};
    float[][] one = {{3.0e-30f, 0.0f, 0.0f, 0.0f}};
    assertThat(IvfKernel.nearestCentroidL2(one, centroids, 1)).containsExactly(1);
    assertThat(IvfKernel.nearestCentroidL2Simd(one, centroids, 1))
        .as("the one-row path")
        .containsExactly(1);

    float[][] block = {
      {3.0e-30f, 0.0f, 0.0f, 0.0f},
      {3.0e-30f, 0.0f, 0.0f, 0.0f},
      {3.0e-30f, 0.0f, 0.0f, 0.0f},
      {3.0e-30f, 0.0f, 0.0f, 0.0f}
    };
    assertThat(IvfKernel.nearestCentroidL2(block, centroids, 1)).containsExactly(1, 1, 1, 1);
    assertThat(IvfKernel.nearestCentroidL2Simd(block, centroids, 1))
        .as("the four-row path")
        .containsExactly(1, 1, 1, 1);
  }

  @Test
  void withTheModuleTheSimdBuildIsDeterministicAndSearchable() {
    Assumptions.assumeTrue(
        vectorApiAvailable(), "the vector API is not on this JVM's module graph");

    List<IndexEntry> entries = entries(spreadVectors());
    Map<String, String> options = Map.of(ASSIGN_KERNEL, "simd", "nlist", "8");

    byte[] first = payload(options, entries);
    byte[] second = payload(options, entries);
    assertThat(second)
        .as("the same vectors and centroids assign the same lists every time")
        .isEqualTo(first);
    assertThat(structure(options, entries))
        .as("the build records that the vector API kernel ran")
        .containsEntry("assign-kernel-simd", 1L);

    VectorIndexAdapter adapter = adapter(options);
    ByteBuffer built = adapter.encodeAuxiliaryPayload(entries, 4);
    assertThat(adapter.prepareAuxiliary(built).search(adapter.encodeQuery(spreadVectors()[0]), 5))
        .as("a payload the simd kernel built is readable")
        .isNotEmpty();
  }

  @Test
  void withoutTheModuleASimdBuildFallsBackToTheExactKernel() {
    Assumptions.assumeFalse(vectorApiAvailable(), "the vector API is on this JVM's module graph");

    Random random = new Random(3141L);
    float[][] vectors = randomVectors(random, 300, 64);
    float[][] centroids = randomVectors(random, 64, 64);
    assertThat(IvfKernel.nearestCentroidL2Simd(vectors, centroids, 2))
        .as("a JVM without the module cannot run the kernel")
        .isNull();

    List<IndexEntry> entries = entries(spreadVectors());
    Map<String, String> options = Map.of(ASSIGN_KERNEL, "simd");
    assertThat(payload(options, entries))
        .as("the build keeps the exact kernel's payload")
        .isEqualTo(payload(Map.of(), entries));
    assertThat(structure(options, entries))
        .as("the structure records the kernel that actually ran, not the one that was asked for")
        .containsEntry("assign-kernel-simd", 0L);
  }

  @Test
  void aKernelThatCannotBeLoadedLeavesTheBuildOnTheExactKernel() {
    ClassLoader failingLoader =
        new ClassLoader(IvfKernel.class.getClassLoader()) {
          @Override
          protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if (name.equals("org.apache.iceberg.index.vector.SimdAssignKernel")) {
              throw new ClassNotFoundException(name);
            }

            return super.loadClass(name, resolve);
          }
        };

    assertThat(IvfKernel.loadKernel(failingLoader))
        .as("a loader that cannot produce the kernel yields no kernel")
        .isNull();

    float[][] vectors = randomVectors(new Random(3141L), 300, 64);
    float[][] centroids = randomVectors(new Random(3142L), 64, 64);
    assertThat(IvfKernel.nearestCentroidL2Simd(vectors, centroids, 2, null))
        .as("without a kernel the caller is told to keep the exact one")
        .isNull();
  }

  @Test
  void theVectorizedKernelKeepsTheEarlierListOnAnExactTie() {
    Assumptions.assumeTrue(
        vectorApiAvailable(), "the vector API is not on this JVM's module graph");

    // dimension 64 is four full lanes of the preferred species, so the tie is decided by the
    // vector path rather than by the tail loop that a 4-dimension test would reach
    float[][] vectors = {new float[64]};
    float[] first = new float[64];
    first[0] = 1.0f;
    first[8] = 1.0f;
    float[] second = new float[64];
    second[1] = 1.0f;
    second[9] = 1.0f;
    float[][] centroids = {first, second};

    AssignKernel kernel = IvfKernel.loadKernel(IvfKernel.class.getClassLoader());
    assertThat(IvfKernel.nearestCentroidL2Simd(vectors, centroids, 1, kernel))
        .as("both centroids are exactly as far away, so the earlier list keeps the row")
        .containsExactly(IvfKernel.nearestCentroidL2(vectors, centroids, 1));
  }

  @Test
  void theSimdKernelOnlyReplacesTheL2Assignment() {
    Assumptions.assumeTrue(
        vectorApiAvailable(), "the vector API is not on this JVM's module graph");

    List<IndexEntry> entries = entries(spreadVectors());
    Map<String, String> requested = Map.of(ASSIGN_KERNEL, "simd");
    assertThat(payload(VectorIndexDefinition.Metric.DOT, requested, entries))
        .as("a cosine or dot-product assignment is not the kernel the option replaces")
        .isEqualTo(payload(VectorIndexDefinition.Metric.DOT, Map.of(), entries));
    assertThat(structure(VectorIndexDefinition.Metric.DOT, requested, entries))
        .containsEntry("assign-kernel-simd", 0L);
  }

  /** Whether this JVM resolved the vector API module, which is what loading the kernel needs. */
  private static boolean vectorApiAvailable() {
    try {
      Class.forName("jdk.incubator.vector.FloatVector");
      return true;
    } catch (ClassNotFoundException | LinkageError e) {
      return false;
    }
  }

  private static byte[] payload(Map<String, String> options, List<IndexEntry> entries) {
    return payload(VectorIndexDefinition.Metric.L2, options, entries);
  }

  private static byte[] payload(
      VectorIndexDefinition.Metric metric, Map<String, String> options, List<IndexEntry> entries) {
    ByteBuffer payload = adapter(metric, options).encodeAuxiliaryPayload(entries, 4);
    return VectorPayloads.toBytes(payload);
  }

  private static Map<String, Long> structure(
      Map<String, String> options, List<IndexEntry> entries) {
    return structure(VectorIndexDefinition.Metric.L2, options, entries);
  }

  private static Map<String, Long> structure(
      VectorIndexDefinition.Metric metric, Map<String, String> options, List<IndexEntry> entries) {
    IndexBuildResult built = adapter(metric, options).encodeAuxiliaryPayloadWithMemory(entries, 4);
    return built.memory().structure();
  }

  private static VectorIndexAdapter adapter(Map<String, String> options) {
    return adapter(VectorIndexDefinition.Metric.L2, options);
  }

  private static VectorIndexAdapter adapter(
      VectorIndexDefinition.Metric metric, Map<String, String> options) {
    VectorIndexDefinition.Builder builder =
        VectorIndexDefinition.builder()
            .fieldId(1)
            .dimension(DIMENSION)
            .metric(metric)
            .structure(VectorIndexDefinition.VectorStructure.IVF);
    options.forEach(builder::property);
    return new VectorIndexAdapter(builder.build());
  }

  private static List<IndexEntry> entries(float[][] vectors) {
    VectorIndexAdapter flat =
        new VectorIndexAdapter(
            VectorIndexDefinition.builder()
                .fieldId(1)
                .dimension(DIMENSION)
                .metric(VectorIndexDefinition.Metric.L2)
                .structure(VectorIndexDefinition.VectorStructure.FLAT)
                .build());
    List<IndexEntry> entries = Lists.newArrayListWithCapacity(vectors.length);
    for (int i = 0; i < vectors.length; i++) {
      entries.add(IndexEntry.of("data-1.parquet", i, flat.encodeVector(vectors[i])));
    }
    return entries;
  }

  /** Vectors spread over the space, so training runs several rounds before it settles. */
  private static float[][] spreadVectors() {
    float[][] vectors = new float[ENTRIES][DIMENSION];
    for (int i = 0; i < ENTRIES; i++) {
      for (int d = 0; d < DIMENSION; d++) {
        // integer arithmetic rather than a transcendental function, so the payload is the same on
        // every platform and the golden hash pins the build rather than a math library
        long slot = (((long) i * 2_654_435_761L + (long) d * 40_503L) >>> 9) % 2048 - 1024;
        vectors[i][d] = (float) slot / 1024.0f;
      }
    }
    return vectors;
  }

  private static float[][] randomVectors(Random random, int count, int dimension) {
    float[][] vectors = new float[count][dimension];
    for (float[] vector : vectors) {
      for (int d = 0; d < dimension; d++) {
        vector[d] = (random.nextFloat() - 0.5f) * 4.0f;
      }
    }
    return vectors;
  }
}
