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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.iceberg.index.IndexSearchResult;
import org.junit.jupiter.api.Test;

/**
 * The bounded top-k every vector search shares.
 *
 * <p>The reference here is a full sort of every candidate with the same rules, done by this test:
 * score direction, then the path exactly as {@code String.compareTo} orders it, then the row, then
 * the order the candidates arrived in. The heap is not allowed to disagree with that on any input,
 * including equal scores, duplicate paths and the non-finite doubles a metric can produce.
 */
class TestVectorTopK {

  private static final VectorScoring EUCLIDEAN =
      new VectorScoring(VectorIndexDefinition.Metric.EUCLIDEAN);
  private static final VectorScoring DOT =
      new VectorScoring(VectorIndexDefinition.Metric.DOT_PRODUCT);
  private static final VectorScoring COSINE =
      new VectorScoring(VectorIndexDefinition.Metric.COSINE);

  private static final String[] PATHS = {"a.parquet", "b.parquet", "c.parquet"};

  @Test
  void emptyAndZeroTopKReturnNothing() {
    assertThat(topK(0, 0, EUCLIDEAN).results()).isEmpty();
    assertThat(topK(5, 0, EUCLIDEAN).results()).isEmpty();
    VectorTopK zero = topK(0, 100, EUCLIDEAN);
    zero.offer(0, 0, 1.0);
    assertThat(zero.size()).isZero();
    assertThat(zero.results()).isEmpty();
    assertThatThrownBy(() -> topK(-1, 10, EUCLIDEAN))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Top-k");
    assertThatThrownBy(() -> topK(1, -1, EUCLIDEAN))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Candidate count");
  }

  @Test
  void aMillionRequestedFromThreeCandidatesAllocatesForThree() {
    VectorTopK top = topK(1_000_000, 3, EUCLIDEAN);
    assertThat(top.capacity()).isEqualTo(3);
    top.offer(0, 10, 0.3);
    top.offer(1, 11, 0.1);
    top.offer(2, 12, 0.2);

    assertThat(top.capacity()).isEqualTo(3);
    assertThat(top.results())
        .extracting(IndexSearchResult::rowPosition)
        .containsExactly(11L, 12L, 10L);
  }

  @Test
  void capacityGrowsOnlyAsCandidatesArrive() {
    VectorTopK top = topK(1000, 1000, EUCLIDEAN);
    assertThat(top.capacity()).isEqualTo(16);
    for (int i = 0; i < 100; i++) {
      top.offer(i % PATHS.length, i, i);
    }

    assertThat(top.capacity()).isGreaterThanOrEqualTo(100).isLessThan(1000);
    for (int i = 100; i < 1000; i++) {
      top.offer(i % PATHS.length, i, i);
    }

    assertThat(top.capacity()).isEqualTo(1000);
    assertThat(top.size()).isEqualTo(1000);
  }

  @Test
  void matchesAFullSortWhenScoresAreEqualAndPathsAreShuffledOrRepeated() {
    VectorTopK top = topK(7, 7, EUCLIDEAN);
    List<Candidate> offered = new ArrayList<>();
    Object[][] all = {
      {1.0, 0, 5L},
      {1.0, 2, 1L},
      {0.5, 1, 9L},
      {1.0, 0, 5L},
      {1.0, 0, 4L},
      {0.0, 2, 2L},
      {1.0, 1, 0L}
    };
    for (Object[] candidate : all) {
      offered.add(
          new Candidate(
              (Double) candidate[0], (Integer) candidate[1], (Long) candidate[2], offered.size()));
      top.offer((Integer) candidate[1], (Long) candidate[2], (Double) candidate[0]);
    }

    assertThat(top.results())
        .usingRecursiveComparison()
        .isEqualTo(sortedResults(offered, EUCLIDEAN));
  }

  @Test
  void ordersSmallerDistancesFirstAndLargerSimilaritiesFirst() {
    double[] scores = {2.0, -1.0, 0.5, 0.0};
    for (VectorScoring scoring : List.of(EUCLIDEAN, DOT, COSINE)) {
      VectorTopK top = topK(scores.length, scores.length, scoring);
      List<Candidate> offered = new ArrayList<>();
      for (int i = 0; i < scores.length; i++) {
        offered.add(new Candidate(scores[i], i % PATHS.length, i, i));
        top.offer(i % PATHS.length, i, scores[i]);
      }

      assertThat(top.results())
          .as("order under %s", scoring.metric())
          .usingRecursiveComparison()
          .isEqualTo(sortedResults(offered, scoring));
    }
  }

  @Test
  void keepsTheDoubleCompareOrderOfNonFiniteScores() {
    double[] scores = {
      Double.NaN, 0.0, -0.0, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 1.0, -1.0
    };
    for (VectorScoring scoring : List.of(EUCLIDEAN, DOT)) {
      VectorTopK top = topK(scores.length, scores.length, scoring);
      List<Candidate> offered = new ArrayList<>();
      for (int i = 0; i < scores.length; i++) {
        offered.add(new Candidate(scores[i], i % PATHS.length, i, i));
        top.offer(i % PATHS.length, i, scores[i]);
      }

      List<Candidate> expected = sortedCandidates(offered, scoring);
      for (int rank = 0; rank < expected.size(); rank++) {
        assertThat(Double.doubleToLongBits(top.scoreAt(rank)))
            .as("score bits at rank %s under %s", rank, scoring.metric())
            .isEqualTo(Double.doubleToLongBits(expected.get(rank).score));
        assertThat(top.rowAt(rank)).isEqualTo(expected.get(rank).row);
      }
    }
  }

  @Test
  void doesNotReadAPathUntilTwoScoresTie() {
    int[] calls = new int[1];
    VectorTopK top =
        new VectorTopK(
            3,
            100,
            EUCLIDEAN,
            location -> {
              calls[0] += 1;
              return PATHS[location];
            });
    top.offer(0, 0, 3.0);
    top.offer(1, 1, 2.0);
    top.offer(2, 2, 1.0);
    top.offer(0, 3, 4.0);

    assertThat(calls[0]).isZero();
    top.scoreAt(0);
    assertThat(calls[0]).isZero();
  }

  @Test
  void identicalKeysKeepTheVisitOrderAndAreNotDropped() {
    int[] calls = new int[1];
    VectorTopK top =
        new VectorTopK(
            2,
            4,
            EUCLIDEAN,
            location -> {
              calls[0] += 1;
              return "same.parquet";
            });
    top.offer(0, 7, 1.0);
    top.offer(1, 7, 1.0);
    top.offer(2, 8, 2.0);

    // the two candidates are equal on every key, so only the order they arrived in separates them,
    // and the third one is worse than both and is not held
    assertThat(top.size()).isEqualTo(2);
    assertThat(top.recordAt(0)).isZero();
    assertThat(top.recordAt(1)).isEqualTo(1);
    assertThat(top.rowAt(0)).isEqualTo(7L);
    assertThat(calls[0]).isPositive();
    assertThat(top.results()).extracting(IndexSearchResult::rowPosition).containsExactly(7L, 7L);
  }

  @Test
  void refusesATargetItCannotRepresentRatherThanWrapping() {
    // asking for more results than an array can hold is refused before anything is allocated; the
    // same request against a small index is clamped to the candidates it has and is allowed
    assertThatThrownBy(
            () -> new VectorTopK(Integer.MAX_VALUE, Long.MAX_VALUE, EUCLIDEAN, l -> PATHS[l]))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("top-k");
    assertThat(new VectorTopK(Integer.MAX_VALUE, 3, EUCLIDEAN, l -> PATHS[l]).capacity())
        .isEqualTo(3);
  }

  private static VectorTopK topK(int topK, long candidates, VectorScoring scoring) {
    return new VectorTopK(topK, candidates, scoring, location -> PATHS[location]);
  }

  private static List<IndexSearchResult> sortedResults(
      List<Candidate> offered, VectorScoring scoring) {
    List<IndexSearchResult> results = new ArrayList<>();
    for (Candidate candidate : sortedCandidates(offered, scoring)) {
      results.add(IndexSearchResult.of(PATHS[candidate.path], candidate.row, candidate.score));
    }

    return results;
  }

  private static List<Candidate> sortedCandidates(List<Candidate> offered, VectorScoring scoring) {
    List<Candidate> copy = new ArrayList<>(offered);
    Comparator<Candidate> order =
        (left, right) -> {
          int byScore = scoring.compareScores(left.score, right.score);
          if (byScore != 0) {
            return byScore;
          }

          int byPath = PATHS[left.path].compareTo(PATHS[right.path]);
          if (byPath != 0) {
            return byPath;
          }

          int byRow = Long.compare(left.row, right.row);
          return byRow != 0 ? byRow : Integer.compare(left.visit, right.visit);
        };
    copy.sort(order);
    return copy;
  }

  private static final class Candidate {
    private final double score;
    private final int path;
    private final long row;
    private final int visit;

    private Candidate(double score, int path, long row, int visit) {
      this.score = score;
      this.path = path;
      this.row = row;
      this.visit = visit;
    }
  }
}
