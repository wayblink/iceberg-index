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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.index.IndexAdapter.PreparedSearch;
import org.apache.iceberg.index.IndexCandidates;
import org.apache.iceberg.index.IndexEntry;
import org.apache.iceberg.index.IndexSearchResult;
import org.apache.iceberg.index.vector.VectorIndexDefinition.Metric;
import org.apache.iceberg.index.vector.VectorIndexDefinition.VectorQuantizer;
import org.apache.iceberg.index.vector.VectorIndexDefinition.VectorStructure;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * A filtered search answers only from the rows the candidate set admits.
 *
 * <p>The searches here probe every list, so the answer is the exact one a scan of the candidate
 * rows would give: with the whole index visited, a filtering search and a scan have nothing to
 * differ on.
 */
class TestIvfFilteredSearch {
  private static final int DIMENSION = 16;
  private static final int COUNT = 2000;
  private static final int FILES = 4;

  @BeforeAll
  static void registerVectorIndexType() {
    VectorIndexTypes.register();
  }

  @Test
  void reportsThatItCanSkipRowsItself() {
    VectorIndexAdapter adapter = adapter(VectorQuantizer.NONE, Map.of("nlist", "8", "nprobe", "8"));
    PreparedSearch search = adapter.prepareAuxiliary(payload(adapter));

    assertThat(search.joinsCandidateSet())
        .as("the IVF searcher fills the window from the candidate rows instead of dropping hits")
        .isTrue();
  }

  @Test
  void answersTheSameRowsAnExactScanOfTheCandidatesWould() {
    VectorIndexAdapter adapter = adapter(VectorQuantizer.NONE, Map.of("nlist", "8", "nprobe", "8"));
    PreparedSearch search = adapter.prepareAuxiliary(payload(adapter));
    IndexCandidates candidates = candidatesKeeping(row -> row % 7 == 0);
    float[] query = vector(1_234);

    List<IndexSearchResult> filtered = search.search(adapter.encodeQuery(query), 10, candidates);

    assertThat(filtered).hasSize(10);
    assertThat(filtered)
        .allSatisfy(
            result ->
                assertThat(candidates.contains(result.dataFile(), result.rowPosition())).isTrue());
    assertThat(filtered)
        .extracting(IndexSearchResult::dataFile, IndexSearchResult::rowPosition)
        .containsExactlyElementsOf(exactScan(query, candidates, 10));
  }

  @Test
  void returnsWhatItFoundWhenTheCandidatesRunOut() {
    VectorIndexAdapter adapter = adapter(VectorQuantizer.NONE, Map.of("nlist", "8", "nprobe", "8"));
    PreparedSearch search = adapter.prepareAuxiliary(payload(adapter));
    IndexCandidates candidates = candidatesKeeping(row -> row == 42 || row == 99 || row == 1500);

    List<IndexSearchResult> filtered =
        search.search(adapter.encodeQuery(vector(1_234)), 10, candidates);

    assertThat(filtered)
        .as(
            "asking for more rows than the filter admits returns the ones that exist, not a full window")
        .hasSize(3);
    assertThat(filtered)
        .allSatisfy(
            result ->
                assertThat(candidates.contains(result.dataFile(), result.rowPosition())).isTrue());
  }

  @Test
  void answersNothingWhenNoRowSurvivesTheFilter() {
    VectorIndexAdapter adapter = adapter(VectorQuantizer.NONE, Map.of("nlist", "8", "nprobe", "8"));
    PreparedSearch search = adapter.prepareAuxiliary(payload(adapter));

    assertThat(search.search(adapter.encodeQuery(vector(1)), 10, IndexCandidates.empty()))
        .isEmpty();
  }

  @Test
  void answersOnlyFromTheCandidatesWhenTheCodesAreQuantized() {
    VectorIndexAdapter adapter =
        adapter(VectorQuantizer.RABITQ, Map.of("nlist", "8", "nprobe", "8", "rabitq-bits", "3"));
    PreparedSearch search = adapter.prepareAuxiliary(payload(adapter));
    IndexCandidates candidates = candidatesKeeping(row -> row % 11 == 0);

    List<IndexSearchResult> filtered =
        search.search(adapter.encodeQuery(vector(1_234)), 20, candidates);

    assertThat(filtered).hasSize(20);
    assertThat(filtered)
        .as("a code is scored only for a row the filter admits")
        .allSatisfy(
            result ->
                assertThat(candidates.contains(result.dataFile(), result.rowPosition())).isTrue());
  }

  @Test
  void findsTheCandidatesThatItsFirstProbeMissed() {
    VectorIndexAdapter hasty = adapter(VectorQuantizer.NONE, Map.of("nlist", "64", "nprobe", "1"));
    PreparedSearch search = hasty.prepareAuxiliary(payload(hasty));
    // a filter that keeps one row per list region, so the lists nearest the query cannot hold all
    // of the ten rows asked for
    IndexCandidates candidates = candidatesKeeping(row -> row % 97 == 0);
    float[] query = vector(700);

    List<IndexSearchResult> filtered = search.search(hasty.encodeQuery(query), 10, candidates);

    assertThat(filtered)
        .as("probing one list is not enough, so the search widens instead of returning short")
        .hasSize(10);
    assertThat(filtered)
        .allSatisfy(
            result ->
                assertThat(candidates.contains(result.dataFile(), result.rowPosition())).isTrue());
    assertThat(filtered)
        .extracting(IndexSearchResult::dataFile, IndexSearchResult::rowPosition)
        .containsExactlyElementsOf(exactScan(query, candidates, 10));
  }

  @Test
  void leavesTheUnfilteredSearchAsItWas() {
    VectorIndexAdapter adapter = adapter(VectorQuantizer.NONE, Map.of("nlist", "8", "nprobe", "2"));
    PreparedSearch search = adapter.prepareAuxiliary(payload(adapter));
    byte[] query = bytes(adapter.encodeQuery(vector(1_234)));

    List<IndexSearchResult> before = search.search(adapter.encodeQuery(vector(1_234)), 10);
    List<IndexSearchResult> all =
        search.search(adapter.encodeQuery(vector(1_234)), 10, candidatesKeeping(row -> true));

    assertThat(before).hasSize(10);
    assertThat(all)
        .as("a set that admits every row asks the same question as no set at all")
        .extracting(IndexSearchResult::dataFile, IndexSearchResult::rowPosition)
        .containsExactlyElementsOf(
            before.stream()
                .map(
                    result ->
                        org.assertj.core.groups.Tuple.tuple(
                            result.dataFile(), result.rowPosition()))
                .toList());
    assertThat(bytes(adapter.encodeQuery(vector(1_234))))
        .as("a search must not consume the query buffer its caller may reuse")
        .isEqualTo(query);
  }

  private static IndexCandidates candidatesKeeping(java.util.function.IntPredicate keep) {
    Map<String, List<Long>> byFile = new LinkedHashMap<>();
    for (int row = 0; row < COUNT; row++) {
      if (keep.test(row)) {
        byFile.computeIfAbsent(dataFile(row), file -> new ArrayList<>()).add((long) row);
      }
    }

    Map<String, long[]> positions = new LinkedHashMap<>();
    byFile.forEach(
        (file, rows) -> positions.put(file, rows.stream().mapToLong(Long::longValue).toArray()));
    return IndexCandidates.of(positions);
  }

  /** The rows a scan of the candidate rows would return, best first. */
  private static List<org.assertj.core.groups.Tuple> exactScan(
      float[] query, IndexCandidates candidates, int limit) {
    List<Map.Entry<Double, IndexSearchResult>> scored = new ArrayList<>();
    for (long row : allPositions(candidates)) {
      float[] value = vector((int) row);
      double sum = 0.0;
      for (int d = 0; d < DIMENSION; d++) {
        double difference = (double) query[d] - value[d];
        sum += difference * difference;
      }

      IndexSearchResult result = IndexSearchResult.of(dataFile((int) row), row, Math.sqrt(sum));
      scored.add(Map.entry(result.score(), result));
    }

    scored.sort(
        Map.Entry.<Double, IndexSearchResult>comparingByKey()
            .thenComparing(entry -> entry.getValue().dataFile())
            .thenComparingLong(entry -> entry.getValue().rowPosition()));

    return scored.stream()
        .limit(limit)
        .map(
            entry ->
                org.assertj.core.groups.Tuple.tuple(
                    entry.getValue().dataFile(), entry.getValue().rowPosition()))
        .toList();
  }

  private static List<Long> allPositions(IndexCandidates candidates) {
    List<Long> positions = new ArrayList<>();
    for (int row = 0; row < COUNT; row++) {
      if (candidates.contains(dataFile(row), row)) {
        positions.add((long) row);
      }
    }

    return positions;
  }

  private static String dataFile(int row) {
    return "data-" + (row % FILES) + ".parquet";
  }

  private static float[] vector(int seed) {
    float[] value = new float[DIMENSION];
    for (int d = 0; d < DIMENSION; d++) {
      value[d] = (float) Math.sin(0.003 * (seed * DIMENSION + d));
    }

    return value;
  }

  private static java.nio.ByteBuffer payload(VectorIndexAdapter adapter) {
    return adapter.encodeAuxiliaryPayload(entries(adapter));
  }

  private static List<IndexEntry> entries(VectorIndexAdapter adapter) {
    List<IndexEntry> entries = new ArrayList<>(COUNT);
    for (int row = 0; row < COUNT; row++) {
      List<Float> value = new ArrayList<>(DIMENSION);
      for (float component : vector(row)) {
        value.add(component);
      }

      entries.add(IndexEntry.of(dataFile(row), row, adapter.encodeEntryValue(value)));
    }

    return entries;
  }

  private static VectorIndexAdapter adapter(
      VectorQuantizer quantizer, Map<String, String> options) {
    return new VectorIndexAdapter(
        VectorIndexDefinition.builder()
            .fieldId(1)
            .dimension(DIMENSION)
            .metric(Metric.EUCLIDEAN)
            .structure(VectorStructure.IVF)
            .quantizer(quantizer)
            .properties(options)
            .build());
  }

  private static byte[] bytes(java.nio.ByteBuffer buffer) {
    java.nio.ByteBuffer copy = buffer.duplicate();
    byte[] bytes = new byte[copy.remaining()];
    copy.get(bytes);
    return bytes;
  }
}
