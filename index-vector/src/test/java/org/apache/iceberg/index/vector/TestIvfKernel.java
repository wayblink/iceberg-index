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
import java.util.Set;
import org.apache.iceberg.index.IndexEntry;
import org.apache.iceberg.index.IndexSearchResult;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * What an IVF index answers.
 *
 * <p>An IVF index keeps the vectors and partitions them, so a search that visits every partition
 * must give exactly the answer the flat scan gives: everything it misses it misses because it did
 * not visit the partition, not because of anything it did to the vectors. That is the property this
 * leans on, because a recall figure for a smaller {@code nprobe} means nothing if visiting all of
 * them is not exactly right.
 *
 * <p>Training is deterministic, so the same entries build the same payload; a rebuild of a data
 * file therefore reproduces its partitions rather than shifting them.
 */
class TestIvfKernel {

  private static final int DIMENSION = 8;
  private static final int ENTRIES = 400;

  @BeforeAll
  static void registerVectorIndexType() {
    VectorIndexTypes.register();
  }

  @Test
  public void answersExactlyWhenItVisitsEveryPartition() {
    List<IndexEntry> entries = entries(VectorIndexDefinition.Metric.EUCLIDEAN);

    VectorIndexAdapter ivf = adapter(VectorIndexDefinition.Metric.EUCLIDEAN, 0, ENTRIES);
    ByteBuffer payload = ivf.encodeAuxiliaryPayload(entries, 1);

    VectorIndexAdapter exact = flatAdapter(VectorIndexDefinition.Metric.EUCLIDEAN);
    ByteBuffer exactPayload = exact.encodeEntries(entries);

    for (int query = 0; query < 20; query++) {
      ByteBuffer encoded = exact.encodeQuery(vector(query * 7));
      List<IndexSearchResult> fromIvf = ivf.prepareAuxiliary(payload).search(encoded, 5);
      List<IndexSearchResult> fromScan = exact.prepare(exactPayload).search(encoded, 5);
      assertThat(fromIvf)
          .as("every partition visited is the same answer as the flat scan, for query %s", query)
          .isEqualTo(fromScan);
    }
  }

  @Test
  public void visitsOnlyThePartitionsItWasTold() {
    List<IndexEntry> entries = entries(VectorIndexDefinition.Metric.EUCLIDEAN);

    VectorIndexAdapter patient = adapter(VectorIndexDefinition.Metric.EUCLIDEAN, 0, ENTRIES);
    ByteBuffer payload = patient.encodeAuxiliaryPayload(entries, 1);
    ByteBuffer query = patient.encodeQuery(vector(11));
    List<IndexSearchResult> all = patient.prepareAuxiliary(payload).search(query, 10);

    VectorIndexAdapter hasty = adapter(VectorIndexDefinition.Metric.EUCLIDEAN, 1, ENTRIES);
    List<IndexSearchResult> one = hasty.prepareAuxiliary(payload).search(query, 10);

    // visiting one partition cannot return more than one partition holds, and cannot return more
    // than was asked for
    assertThat(one).hasSizeLessThanOrEqualTo(10);
    assertThat(one.size()).isLessThanOrEqualTo(all.size());
    assertThat(all).containsAll(one);
  }

  @Test
  public void derivesTheListCountFromTheGroupSize() {
    // a group is one data file, and data files differ in size, so a fixed count would fragment a
    // small one and leave a large one with lists too long to scan
    assertThat(IvfOptions.from(java.util.Map.of()).nlistFor(262_144)).isEqualTo(512);
    assertThat(IvfOptions.from(java.util.Map.of()).nlistFor(38_528)).isEqualTo(196);
    assertThat(IvfOptions.from(java.util.Map.of()).nlistFor(1)).isEqualTo(1);
    assertThat(IvfOptions.from(java.util.Map.of()).nlistFor(0)).isZero();

    // never more lists than vectors, and an explicit count is clamped the same way
    assertThat(IvfOptions.from(java.util.Map.of(IvfOptions.NLIST, "1000")).nlistFor(10))
        .isEqualTo(10);
    assertThat(IvfOptions.from(java.util.Map.of(IvfOptions.NLIST, "16")).nlistFor(400))
        .isEqualTo(16);
  }

  @Test
  public void buildsTheSamePayloadTwice() {
    List<IndexEntry> entries = entries(VectorIndexDefinition.Metric.EUCLIDEAN);
    VectorIndexAdapter ivf = adapter(VectorIndexDefinition.Metric.EUCLIDEAN, 0, ENTRIES);

    ByteBuffer first = ivf.encodeAuxiliaryPayload(entries, 1);
    ByteBuffer second = ivf.encodeAuxiliaryPayload(entries, 1);

    assertThat(VectorPayloads.toBytes(second))
        .as("training samples deterministically, so a rebuild reproduces the partitions")
        .isEqualTo(VectorPayloads.toBytes(first));
  }

  @Test
  public void indexesASingleVector() {
    VectorIndexAdapter ivf = adapter(VectorIndexDefinition.Metric.EUCLIDEAN, 0, 1);
    List<IndexEntry> entries =
        Lists.newArrayList(
            IndexEntry.of(
                "data-1.parquet", 0, ivf.encodeVector(new float[] {1, 0, 0, 0, 0, 0, 0, 0})));

    ByteBuffer payload = ivf.encodeAuxiliaryPayload(entries, 1);
    assertThat(
            ivf.prepareAuxiliary(payload)
                .search(ivf.encodeQuery(new float[] {1, 0, 0, 0, 0, 0, 0, 0}), 1))
        .extracting(IndexSearchResult::dataFile)
        .containsExactly("data-1.parquet");
  }

  @Test
  public void indexesVectorsThatAreAllTheSame() {
    VectorIndexAdapter ivf = adapter(VectorIndexDefinition.Metric.EUCLIDEAN, 0, 50);
    float[] same = {1, 2, 3, 4, 5, 6, 7, 8};
    List<IndexEntry> entries = Lists.newArrayList();
    for (int i = 0; i < 50; i++) {
      entries.add(IndexEntry.of("data-1.parquet", i, ivf.encodeVector(same)));
    }

    // every centroid ends up identical, so every vector lands in one list and the rest are empty
    ByteBuffer payload = ivf.encodeAuxiliaryPayload(entries, 1);
    List<IndexSearchResult> results =
        ivf.prepareAuxiliary(payload).search(ivf.encodeQuery(same), 50);
    assertThat(results).hasSize(50);
  }

  @Test
  public void refusesAZeroVectorUnderCosine() {
    VectorIndexAdapter ivf = adapter(VectorIndexDefinition.Metric.COSINE, 0, 10);
    float[] zero = new float[DIMENSION];
    assertThatThrownBy(
            () ->
                ivf.encodeAuxiliaryPayload(
                    Lists.newArrayList(IndexEntry.of("data-1.parquet", 7, ivf.encodeVector(zero))),
                    1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("A zero vector cannot be indexed by cosine")
        .hasMessageContaining("data-1.parquet")
        .hasMessageContaining("7");
  }

  @Test
  public void rejectsAnOptionItDoesNotRead() {
    assertThatThrownBy(
            () ->
                new VectorIndexAdapter(
                    VectorIndexDefinition.builder()
                        .fieldId(1)
                        .dimension(DIMENSION)
                        .metric(VectorIndexDefinition.Metric.EUCLIDEAN)
                        .structure(VectorIndexDefinition.VectorStructure.IVF)
                        // an option of the graph structure, not of this one
                        .property(HnswOptions.MAX_M, "16")
                        .build()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unknown IVF option max-m")
        .hasMessageContaining("nlist");
  }

  @Test
  public void rejectsAnOptionThatIsNotANumber() {
    assertThatThrownBy(
            () ->
                new VectorIndexAdapter(
                    VectorIndexDefinition.builder()
                        .fieldId(1)
                        .dimension(DIMENSION)
                        .metric(VectorIndexDefinition.Metric.EUCLIDEAN)
                        .structure(VectorIndexDefinition.VectorStructure.IVF)
                        .property(IvfOptions.NPROBE, "many")
                        .build()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("IVF option nprobe is not a number: many");
  }

  @Test
  public void quantizesTheVectorsWhenAskedTo() {
    // the same partitioning with the vectors stored as codes: this is the form whose payload is
    // small enough to matter, and what it costs is accuracy of the scores
    List<IndexEntry> entries = entries(VectorIndexDefinition.Metric.EUCLIDEAN);
    VectorIndexAdapter quantized =
        quantizedAdapter(VectorIndexDefinition.Metric.EUCLIDEAN, ENTRIES);
    VectorIndexAdapter exact = flatAdapter(VectorIndexDefinition.Metric.EUCLIDEAN);

    ByteBuffer codes = quantized.encodeAuxiliaryPayload(entries, 1);
    ByteBuffer vectors = exact.encodeEntries(entries);
    assertThat(codes.remaining())
        .as("a code per vector plus three numbers, against the vector itself")
        .isLessThan(vectors.remaining());

    // the estimate is close to the exact order, not equal to it, which is the whole point: what it
    // is for is naming a wide candidate set that an exact re-scoring then orders
    int found = 0;
    int probes = 10;
    for (int q = 0; q < probes; q++) {
      ByteBuffer query = exact.encodeQuery(vector(q * 13));
      List<IndexSearchResult> fromCodes =
          quantized.prepareAuxiliary(codes).search(query, ENTRIES / 2);
      Set<Long> wide = Sets.newHashSet();
      for (int i = 0; i < 100 && i < fromCodes.size(); i++) {
        wide.add(fromCodes.get(i).rowPosition());
      }
      for (IndexSearchResult exactHit : exact.prepare(vectors).search(query, 10)) {
        if (wide.contains(exactHit.rowPosition())) {
          found += 1;
        }
      }
    }

    System.out.println(
        "IVFQ proxyBytes="
            + codes.remaining()
            + " exactBytes="
            + vectors.remaining()
            + " exactTop10InsideQuantizedTop100="
            + String.format("%.3f", found / (double) (probes * 10)));
    assertThat(found / (double) (probes * 10))
        .as("the exact top ten inside the quantized top hundred")
        .isGreaterThan(0.9);
  }

  private static VectorIndexAdapter quantizedAdapter(
      VectorIndexDefinition.Metric metric, int expectedEntries) {
    return new VectorIndexAdapter(
        VectorIndexDefinition.builder()
            .fieldId(1)
            .dimension(DIMENSION)
            .metric(metric)
            .structure(VectorIndexDefinition.VectorStructure.IVF)
            .quantizer(VectorIndexDefinition.VectorQuantizer.RABITQ)
            .property(IvfOptions.NPROBE, Integer.toString(expectedEntries))
            .build());
  }

  private static VectorIndexAdapter adapter(
      VectorIndexDefinition.Metric metric, int nprobe, int expectedEntries) {
    VectorIndexDefinition.Builder builder =
        VectorIndexDefinition.builder()
            .fieldId(1)
            .dimension(DIMENSION)
            .metric(metric)
            .structure(VectorIndexDefinition.VectorStructure.IVF);
    if (nprobe > 0) {
      // visiting every partition is what makes the answer exact; the count is clamped to the lists
      builder.property(IvfOptions.NPROBE, Integer.toString(nprobe));
    }
    return new VectorIndexAdapter(builder.build());
  }

  private static VectorIndexAdapter flatAdapter(VectorIndexDefinition.Metric metric) {
    return new VectorIndexAdapter(
        VectorIndexDefinition.builder()
            .fieldId(1)
            .dimension(DIMENSION)
            .metric(metric)
            .structure(VectorIndexDefinition.VectorStructure.FLAT)
            .build());
  }

  private static List<IndexEntry> entries(VectorIndexDefinition.Metric metric) {
    VectorIndexAdapter adapter = flatAdapter(metric);
    List<IndexEntry> entries = Lists.newArrayListWithCapacity(ENTRIES);
    for (int i = 0; i < ENTRIES; i++) {
      entries.add(
          IndexEntry.of("data-" + (i / 100) + ".parquet", i, adapter.encodeVector(vector(i))));
    }
    return entries;
  }

  /** A vector distinct for every entry, so a hit can only be the entry itself. */
  private static float[] vector(int entry) {
    float[] values = new float[DIMENSION];
    for (int d = 0; d < DIMENSION; d++) {
      values[d] = (float) Math.sin(entry * 0.7 + d * 1.3);
    }
    return values;
  }
}
