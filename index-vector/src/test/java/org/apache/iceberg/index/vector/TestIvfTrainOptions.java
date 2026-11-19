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
import java.security.MessageDigest;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.index.IndexEntry;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The training budget an IVF definition may set, and what a build does with it.
 *
 * <p>Every figure the definition does not set has to keep the behaviour it always had: the same
 * centroids, the same rounds, and the same payload bytes. What the definition does set is recorded
 * in the structure counts of the build, so a build says which budget it actually spent.
 */
class TestIvfTrainOptions {

  private static final String SAMPLES = "ivf-train-samples-per-list";
  private static final String ITERATIONS = "ivf-train-max-iterations";
  private static final String TOLERANCE = "ivf-train-convergence-tolerance";

  private static final int DIMENSION = 16;
  private static final int ENTRIES = 400;

  @BeforeAll
  static void registerVectorIndexType() {
    VectorIndexTypes.register();
  }

  @Test
  void readsTheTrainingBudgetFromTheDefinition() {
    assertThat(IvfOptions.keys())
        .as("the keys a caller may write into an IVF definition")
        .contains(SAMPLES, ITERATIONS, TOLERANCE)
        .doesNotContain("ivf-trains-everything");

    IvfOptions configured =
        IvfOptions.from(Map.of(SAMPLES, "8", ITERATIONS, "3", TOLERANCE, "0.001"));
    assertThat(configured.trainSamplesPerList()).isEqualTo(8);
    assertThat(configured.trainMaxIterations()).isEqualTo(3);
    assertThat(configured.trainConvergenceTolerance()).isEqualTo(0.001);
    assertThat(configured.tolerancePartsPerMillion()).isEqualTo(1_000L);
  }

  @Test
  void refusesATrainingBudgetOutsideItsRange() {
    for (String value : new String[] {"0", "-1", "many"}) {
      assertThatThrownBy(() -> IvfOptions.from(Map.of(SAMPLES, value)))
          .as("samples per list %s", value)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining(SAMPLES);
    }

    for (String value : new String[] {"0", "-1", "101", "many"}) {
      assertThatThrownBy(() -> IvfOptions.from(Map.of(ITERATIONS, value)))
          .as("the iteration cap %s", value)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining(ITERATIONS);
    }

    for (String value : new String[] {"-0.001", "0.101", "NaN", "Infinity", "many"}) {
      assertThatThrownBy(() -> IvfOptions.from(Map.of(TOLERANCE, value)))
          .as("the convergence tolerance %s", value)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining(TOLERANCE);
    }
  }

  @Test
  void writingTheDefaultsIsTheSameAsWritingNothing() {
    List<IndexEntry> entries = entries(spreadVectors());

    assertThat(payload(Map.of(SAMPLES, "39", ITERATIONS, "25", TOLERANCE, "0.0"), entries))
        .as("a definition that spells out the defaults builds the payload of one that omits them")
        .isEqualTo(payload(Map.of(), entries));
  }

  @Test
  void zeroToleranceKeepsThePayloadOfAnUnsetDefinition() {
    List<IndexEntry> entries = entries(spreadVectors());

    assertThat(payload(Map.of(TOLERANCE, "0"), entries))
        .as("zero means the bitwise criterion, which is the behaviour the parameter was added to")
        .isEqualTo(payload(Map.of(), entries));
    assertThat(structure(Map.of(TOLERANCE, "0"), entries))
        .as("a zero tolerance is recorded as zero parts per million")
        .containsEntry("train-tolerance-ppm", 0L);
  }

  @Test
  void oneIterationCapRunsOneRound() {
    Map<String, Long> structure = structure(Map.of(ITERATIONS, "1"), entries(spreadVectors()));

    assertThat(structure)
        .as("the cap is the number of rounds the build runs, not a hint")
        .containsEntry("train-iterations", 1L);
  }

  @Test
  void samplesPerListSetsTheSampleCount() {
    // 20 lists of 5 samples each is 100 samples, inside the 400 entries the file has
    Map<String, Long> structure =
        structure(Map.of("nlist", "20", SAMPLES, "5"), entries(spreadVectors()));

    assertThat(structure)
        .as("the sample count is the list count times the samples that were asked for")
        .containsEntry("train-samples", 100L);
  }

  @Test
  void aLargeSampleCountStopsAtTheEntriesOfTheFile() {
    Map<String, Long> structure =
        structure(Map.of("nlist", "20", SAMPLES, "1000"), entries(spreadVectors()));

    assertThat(structure)
        .as("a file cannot train on more vectors than it holds")
        .containsEntry("train-samples", (long) ENTRIES);
  }

  @Test
  void toleranceStopsBeforeTheBitwiseCriterion() {
    // one cluster per starting centroid, each point a ten-thousandth away from it: the first round
    // moves every centroid by less than the tolerance, while a second round would find the
    // assignment unchanged and stop on the bitwise criterion
    List<IndexEntry> entries = entries(clusteredVectors());

    Map<String, Long> byTolerance = structure(Map.of(TOLERANCE, "0.001"), entries);
    Map<String, Long> byBits = structure(Map.of(), entries);

    assertThat(byTolerance)
        .as("a displacement under the tolerance ends training after the round that measured it")
        .containsEntry("train-iterations", 1L)
        .containsEntry("train-tolerance-ppm", 1000L);
    assertThat(byBits.get("train-iterations"))
        .as("the bitwise criterion needs the next round to see that nothing changed")
        .isGreaterThan(byTolerance.get("train-iterations"));
  }

  @Test
  void aDefinitionWithoutTrainingOptionsBuildsThePayloadItAlwaysDid() throws Exception {
    byte[] payload = payload(Map.of(), entries(spreadVectors()));
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    StringBuilder hex = new StringBuilder();
    for (byte value : digest.digest(payload)) {
      hex.append(String.format("%02x", value));
    }

    assertThat(hex.toString())
        .as("the payload this exact input built before the training parameters existed")
        .isEqualTo("33c9cb19a106a492108f027ceabb1b752a186787dabcf1c0f8b6a35a7b9fce26");
  }

  @Test
  void takesTheTrainingDefaultsWhenTheDefinitionSaysNothing() {
    IvfOptions options = IvfOptions.from(Map.of());

    assertThat(options.trainSamplesPerList()).isEqualTo(39);
    assertThat(options.trainMaxIterations()).isEqualTo(25);
    assertThat(options.trainConvergenceTolerance()).isZero();
    assertThat(options.tolerancePartsPerMillion()).isZero();
  }

  private static byte[] payload(Map<String, String> options, List<IndexEntry> entries) {
    ByteBuffer payload = adapter(options).encodeAuxiliaryPayload(entries, 1);
    return VectorPayloads.toBytes(payload);
  }

  private static Map<String, Long> structure(
      Map<String, String> options, List<IndexEntry> entries) {
    return adapter(options).encodeAuxiliaryPayloadWithMemory(entries, 1).memory().structure();
  }

  private static VectorIndexAdapter adapter(Map<String, String> options) {
    VectorIndexDefinition.Builder builder =
        VectorIndexDefinition.builder()
            .fieldId(1)
            .dimension(DIMENSION)
            .metric(VectorIndexDefinition.Metric.L2)
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

  /**
   * One tight cluster per starting centroid.
   *
   * <p>Training starts each centroid at a point of the sample, spread across it, which with 400
   * entries and 20 lists is the first point of each block of 20. Those points are the cluster
   * centres here, and every point of a block is a ten-thousandth away from its centre, so the first
   * round moves each centroid by far less than a tolerance of 0.001 without leaving the bitwise
   * criterion any room to stop earlier: the first round does change the centroids.
   */
  private static float[][] clusteredVectors() {
    int lists = IvfOptions.from(Map.of()).nlistFor(ENTRIES);
    int perList = ENTRIES / lists;
    float[][] vectors = new float[ENTRIES][DIMENSION];
    for (int list = 0; list < lists; list++) {
      for (int member = 0; member < perList; member++) {
        int index = list * perList + member;
        for (int d = 0; d < DIMENSION; d++) {
          float centre = ((list * 37 + d * 17) % 41 - 20) / 20.0f;
          float offset = ((index * 29 + d * 13) % 7 - 3) * 1e-4f;
          vectors[index][d] = centre + offset;
        }
      }
    }
    return vectors;
  }
}
