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
package org.apache.iceberg.index.spark;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.index.IndexAdapter.PreparedSearch;
import org.apache.iceberg.index.IndexCandidates;
import org.apache.iceberg.index.IndexEntry;
import org.apache.iceberg.index.IndexSearchResult;
import org.apache.iceberg.index.vector.VectorIndexAdapter;
import org.apache.iceberg.index.vector.VectorIndexDefinition;
import org.apache.iceberg.index.vector.VectorIndexDefinition.Metric;
import org.apache.iceberg.index.vector.VectorIndexDefinition.VectorQuantizer;
import org.apache.iceberg.index.vector.VectorIndexDefinition.VectorStructure;
import org.apache.iceberg.index.vector.VectorIndexTypes;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** One index file answers only from the rows the candidate set admits. */
class TestDistributedIndexSearchCandidates {
  private static final int DIMENSION = 8;
  private static final int COUNT = 400;

  @BeforeAll
  static void registerVectorIndexType() {
    VectorIndexTypes.register();
  }

  @Test
  void answersFromTheAdmittedRowsWhenTheKernelCanFilter() {
    VectorIndexAdapter adapter = adapter();
    PreparedSearch search = adapter.prepareAuxiliary(payload(adapter));
    IndexCandidates candidates = candidatesFor(row -> row % 5 == 0);

    List<IndexSearchResult> hits =
        DistributedIndexSearch.searchFile(search, adapter.encodeQuery(vector(37)), 10, candidates);

    assertThat(hits).hasSize(10);
    assertThat(hits)
        .allSatisfy(
            hit -> assertThat(candidates.contains(hit.dataFile(), hit.rowPosition())).isTrue());
  }

  @Test
  void answersTheUnfilteredQuestionWhenNoFilterIsGiven() {
    VectorIndexAdapter adapter = adapter();
    PreparedSearch search = adapter.prepareAuxiliary(payload(adapter));

    List<IndexSearchResult> hits =
        DistributedIndexSearch.searchFile(search, adapter.encodeQuery(vector(37)), 10, null);

    assertThat(hits).hasSize(10);
    assertThat(hits)
        .extracting(IndexSearchResult::dataFile, IndexSearchResult::rowPosition)
        .containsExactlyElementsOf(
            search.search(adapter.encodeQuery(vector(37)), 10).stream()
                .map(hit -> org.assertj.core.groups.Tuple.tuple(hit.dataFile(), hit.rowPosition()))
                .toList());
  }

  @Test
  void dropsTheHitsAKernelCannotFilterForAndSaysSoByReturningLess() {
    // a kernel that knows nothing about candidates: it answers the unfiltered question
    PreparedSearch blind =
        new PreparedSearch() {
          @Override
          public List<IndexSearchResult> search(ByteBuffer query, int topK) {
            List<IndexSearchResult> hits = new ArrayList<>();
            for (int row = 0; row < topK; row++) {
              hits.add(IndexSearchResult.of("a.parquet", row, row));
            }

            return hits;
          }
        };
    IndexCandidates candidates = IndexCandidates.of(Map.of("a.parquet", new long[] {2, 4, 6}));

    List<IndexSearchResult> hits =
        DistributedIndexSearch.searchFile(blind, ByteBuffer.allocate(0), 10, candidates);

    assertThat(blind.joinsCandidateSet())
        .as("a kernel that does not override this cannot be relied on for a full window")
        .isFalse();
    assertThat(hits)
        .as("what the set admits is what is left, even when it is fewer than were asked for")
        .extracting(IndexSearchResult::rowPosition)
        .containsExactly(2L, 4L, 6L);
  }

  @Test
  void answersNothingWhenNothingSurvivesTheFilter() {
    VectorIndexAdapter adapter = adapter();
    PreparedSearch search = adapter.prepareAuxiliary(payload(adapter));

    assertThat(
            DistributedIndexSearch.searchFile(
                search, adapter.encodeQuery(vector(37)), 10, IndexCandidates.empty()))
        .isEmpty();
  }

  private static IndexCandidates candidatesFor(java.util.function.IntPredicate keep) {
    List<Long> rows = new ArrayList<>();
    for (int row = 0; row < COUNT; row++) {
      if (keep.test(row)) {
        rows.add((long) row);
      }
    }

    return IndexCandidates.of(
        Map.of(dataFile(), rows.stream().mapToLong(Long::longValue).toArray()));
  }

  private static String dataFile() {
    return "a.parquet";
  }

  private static float[] vector(int seed) {
    float[] value = new float[DIMENSION];
    for (int d = 0; d < DIMENSION; d++) {
      value[d] = (float) Math.sin(0.01 * (seed * DIMENSION + d));
    }

    return value;
  }

  private static ByteBuffer payload(VectorIndexAdapter adapter) {
    List<IndexEntry> entries = new ArrayList<>(COUNT);
    for (int row = 0; row < COUNT; row++) {
      List<Float> value = new ArrayList<>(DIMENSION);
      for (float component : vector(row)) {
        value.add(component);
      }

      entries.add(IndexEntry.of(dataFile(), row, adapter.encodeEntryValue(value)));
    }

    return adapter.encodeAuxiliaryPayload(entries);
  }

  private static VectorIndexAdapter adapter() {
    return new VectorIndexAdapter(
        VectorIndexDefinition.builder()
            .fieldId(1)
            .dimension(DIMENSION)
            .metric(Metric.EUCLIDEAN)
            .structure(VectorStructure.IVF)
            .quantizer(VectorQuantizer.NONE)
            .properties(Map.of("nlist", "8", "nprobe", "8"))
            .build());
  }
}
