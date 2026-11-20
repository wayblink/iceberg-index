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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import org.apache.iceberg.index.IndexSearchResult;
import org.junit.jupiter.api.Test;
import scala.Tuple3;

/**
 * A bounded top-k of hits holds exactly what "copy the list, sort it, keep the first limit" holds,
 * for every limit and in both score directions.
 *
 * <p>The reference here is the implementation this replaces, written out from the contract: the
 * score in the direction of the search, then the data file path as {@link String#compareTo} orders
 * it, then the row position. A hit that compares equal on all three is the same hit, so ties among
 * duplicates are unobservable and any order of equal-valued hits is the same answer.
 */
class TestHitTopK {

  /** A distance is better when smaller; a similarity when larger. */
  private static final boolean SMALLEST_FIRST = false;

  private static final boolean LARGEST_FIRST = true;

  @Test
  void emptyAccumulatorReturnsNothing() {
    HitTopK hits = new HitTopK(SMALLEST_FIRST, 5);

    assertThat(hits.size()).isZero();
    assertThat(hits.isEmpty()).isTrue();
    assertThat(hits.results(5)).isEmpty();
    assertThat(hits.results(0)).isEmpty();
  }

  @Test
  void keepsEveryHitWhenThereAreFewerThanTheLimit() {
    List<Tuple3<String, Long, Double>> candidates =
        List.of(hit("b", 2L, 0.5), hit("a", 1L, 0.1), hit("a", 3L, 0.1));

    HitTopK hits = accumulate(SMALLEST_FIRST, candidates, 10);

    assertThat(hits.size()).isEqualTo(3);
    assertThat(hits.results(10))
        .containsExactlyElementsOf(reference(candidates, SMALLEST_FIRST, 3));
  }

  @Test
  void keepsEveryHitWhenTheCountEqualsTheLimit() {
    List<Tuple3<String, Long, Double>> candidates =
        List.of(hit("c", 0L, 3.0), hit("a", 7L, 3.0), hit("b", 1L, 1.0), hit("a", 0L, 2.0));

    HitTopK hits = accumulate(LARGEST_FIRST, candidates, 4);

    assertThat(hits.size()).isEqualTo(4);
    assertThat(hits.results(4)).containsExactlyElementsOf(reference(candidates, LARGEST_FIRST, 4));
  }

  @Test
  void dropsTheWorstHitsBeyondTheLimit() {
    List<Tuple3<String, Long, Double>> candidates =
        List.of(
            hit("a", 0L, 0.1),
            hit("a", 1L, 0.2),
            hit("a", 2L, 0.3),
            hit("a", 3L, 0.4),
            hit("a", 4L, 0.5));

    HitTopK hits = accumulate(SMALLEST_FIRST, candidates, 3);

    assertThat(hits.size()).isEqualTo(3);
    assertThat(hits.results(3))
        .containsExactly(
            IndexSearchResult.of("a", 0L, 0.1),
            IndexSearchResult.of("a", 1L, 0.2),
            IndexSearchResult.of("a", 2L, 0.3));
  }

  @Test
  void keepsTheLargestScoresWhenTheSearchIsASimilarity() {
    List<Tuple3<String, Long, Double>> candidates =
        List.of(hit("a", 0L, 0.1), hit("b", 0L, 0.9), hit("c", 0L, 0.5), hit("d", 0L, 0.7));

    HitTopK hits = accumulate(LARGEST_FIRST, candidates, 2);

    assertThat(hits.results(2))
        .containsExactly(IndexSearchResult.of("b", 0L, 0.9), IndexSearchResult.of("d", 0L, 0.7));
  }

  @Test
  void breaksTiesByDataFileThenRowPosition() {
    // every score is equal, so the data file path decides first and the row position within it
    List<Tuple3<String, Long, Double>> candidates =
        List.of(hit("c", 1L, 0.5), hit("a", 9L, 0.5), hit("a", 2L, 0.5), hit("b", 0L, 0.5));

    HitTopK hits = accumulate(SMALLEST_FIRST, candidates, 2);

    assertThat(hits.results(2))
        .containsExactly(IndexSearchResult.of("a", 2L, 0.5), IndexSearchResult.of("a", 9L, 0.5));
  }

  @Test
  void distinguishesNegativeZeroFromZeroLikeTheMergeOrder() {
    // Double.compare separates -0.0 from 0.0, and so must the heap
    List<Tuple3<String, Long, Double>> candidates = List.of(hit("a", 0L, 0.0), hit("b", 0L, -0.0));

    HitTopK hits = accumulate(SMALLEST_FIRST, candidates, 1);

    assertThat(hits.results(1)).containsExactlyElementsOf(reference(candidates, SMALLEST_FIRST, 1));
  }

  @Test
  void keepsNothingWhenTheLimitIsZero() {
    List<Tuple3<String, Long, Double>> candidates =
        List.of(hit("a", 0L, 0.1), hit("b", 1L, 0.2), hit("c", 2L, 0.3));

    HitTopK hits = accumulate(SMALLEST_FIRST, candidates, 0);

    assertThat(hits.size()).isZero();
    assertThat(hits.results(0)).isEmpty();
    assertThat(hits.results(5)).isEmpty();
  }

  @Test
  void sizeSaturatesAtTheLimit() {
    HitTopK hits = new HitTopK(SMALLEST_FIRST, 4);
    for (int i = 0; i < 100; i += 1) {
      hits.add("f", i, i);
    }

    assertThat(hits.size()).isEqualTo(4);
    assertThat(hits.results(4)).hasSize(4);
  }

  @Test
  void mergingAccumulatorsKeepsTheGlobalTopK() {
    // the shape of the distributed merge: each task keeps its own best, then the accumulators merge
    List<Tuple3<String, Long, Double>> candidates = new ArrayList<>();
    Random random = new Random(31L);
    for (int i = 0; i < 500; i += 1) {
      candidates.add(randomHit(random));
    }

    for (boolean higherScoreFirst : new boolean[] {SMALLEST_FIRST, LARGEST_FIRST}) {
      for (int limit : new int[] {1, 2, 7, 50}) {
        List<HitTopK> parts = new ArrayList<>();
        int chunk = Math.max(1, candidates.size() / 8);
        for (int start = 0; start < candidates.size(); start += chunk) {
          List<Tuple3<String, Long, Double>> part =
              candidates.subList(start, Math.min(candidates.size(), start + chunk));
          parts.add(accumulate(higherScoreFirst, part, limit));
        }

        HitTopK merged = new HitTopK(higherScoreFirst, limit);
        for (HitTopK part : parts) {
          merged.addAll(part);
        }

        assertThat(merged.size()).isEqualTo(Math.min(limit, candidates.size()));
        assertThat(merged.results(limit))
            .as("direction %s, limit %s", higherScoreFirst, limit)
            .containsExactlyElementsOf(reference(candidates, higherScoreFirst, limit));
      }
    }
  }

  @Test
  void matchesTheFullSortOverRandomCandidates() {
    Random random = new Random(20261120L);
    for (int round = 0; round < 40; round += 1) {
      List<Tuple3<String, Long, Double>> candidates = new ArrayList<>();
      int count = random.nextInt(300);
      for (int i = 0; i < count; i += 1) {
        candidates.add(randomHit(random));
      }

      for (boolean higherScoreFirst : new boolean[] {SMALLEST_FIRST, LARGEST_FIRST}) {
        for (int limit : new int[] {0, 1, 2, 5, 17, 300}) {
          HitTopK hits = accumulate(higherScoreFirst, candidates, limit);
          assertThat(hits.size()).isEqualTo(Math.min(limit, count));
          assertThat(hits.results(limit))
              .as("round %s, direction %s, limit %s", round, higherScoreFirst, limit)
              .containsExactlyElementsOf(reference(candidates, higherScoreFirst, limit));
        }
      }
    }
  }

  @Test
  void matchesTheFullSortOverManyCandidates() {
    // enough candidates that the heap is deep and the replacements go through every level
    List<Tuple3<String, Long, Double>> candidates = new ArrayList<>();
    Random random = new Random(7L);
    for (int i = 0; i < 50_000; i += 1) {
      candidates.add(randomHit(random));
    }

    for (boolean higherScoreFirst : new boolean[] {SMALLEST_FIRST, LARGEST_FIRST}) {
      HitTopK hits = accumulate(higherScoreFirst, candidates, 512);
      assertThat(hits.size()).isEqualTo(512);
      assertThat(hits.results(512))
          .containsExactlyElementsOf(reference(candidates, higherScoreFirst, 512));
    }
  }

  @Test
  void compareMatchesTheMergeOrder() {
    Random random = new Random(11L);
    Comparator<Tuple3<String, Long, Double>> order = mergeOrder(SMALLEST_FIRST);
    for (int i = 0; i < 2_000; i += 1) {
      Tuple3<String, Long, Double> left = randomHit(random);
      Tuple3<String, Long, Double> right = randomHit(random);
      for (boolean higherScoreFirst : new boolean[] {SMALLEST_FIRST, LARGEST_FIRST}) {
        assertThat(
                Integer.signum(
                    HitTopK.compare(
                        higherScoreFirst,
                        left._3(),
                        left._1(),
                        left._2(),
                        right._3(),
                        right._1(),
                        right._2())))
            .as("%s against %s, direction %s", left, right, higherScoreFirst)
            .isEqualTo(Integer.signum(mergeOrder(higherScoreFirst).compare(left, right)));
      }
    }
  }

  @Test
  void theAnswerDoesNotDependOnTheOrderHitsArriveIn() {
    // the merge order is a function of the score and the tie breakers, so two tasks that receive
    // the same hits in a different order must answer the same
    List<Tuple3<String, Long, Double>> candidates = new ArrayList<>();
    Random random = new Random(41L);
    for (int i = 0; i < 400; i += 1) {
      candidates.add(randomHit(random));
    }

    for (boolean higherScoreFirst : new boolean[] {SMALLEST_FIRST, LARGEST_FIRST}) {
      List<IndexSearchResult> expected = reference(candidates, higherScoreFirst, 12);
      for (int shuffle = 0; shuffle < 5; shuffle += 1) {
        List<Tuple3<String, Long, Double>> arrivalOrder = new ArrayList<>(candidates);
        Collections.shuffle(arrivalOrder, random);
        HitTopK hits = accumulate(higherScoreFirst, arrivalOrder, 12);
        assertThat(hits.results(12))
            .as("direction %s, shuffle %s", higherScoreFirst, shuffle)
            .containsExactlyElementsOf(expected);
      }
    }
  }

  @Test
  void resultsAreRepeatable() {
    List<Tuple3<String, Long, Double>> candidates =
        List.of(hit("b", 0L, 0.5), hit("a", 3L, 0.5), hit("a", 1L, 0.1), hit("c", 2L, 0.9));

    HitTopK hits = accumulate(SMALLEST_FIRST, candidates, 3);
    List<IndexSearchResult> first = hits.results(3);

    assertThat(hits.results(3)).containsExactlyElementsOf(first);
    assertThat(hits.results(1)).containsExactlyElementsOf(first.subList(0, 1));
  }

  @Test
  void survivesSerialization() throws IOException, ClassNotFoundException {
    // an accumulator crosses a shuffle as a serialized value, so it has to come back holding the
    // same hits
    List<Tuple3<String, Long, Double>> candidates = new ArrayList<>();
    Random random = new Random(3L);
    for (int i = 0; i < 200; i += 1) {
      candidates.add(randomHit(random));
    }

    HitTopK hits = accumulate(SMALLEST_FIRST, candidates, 9);
    HitTopK copy = roundTrip(hits);

    assertThat(copy.size()).isEqualTo(hits.size());
    assertThat(copy.results(9)).containsExactlyElementsOf(reference(candidates, SMALLEST_FIRST, 9));
  }

  private static HitTopK accumulate(
      boolean higherScoreFirst, List<Tuple3<String, Long, Double>> candidates, int limit) {
    HitTopK hits = new HitTopK(higherScoreFirst, limit);
    for (Tuple3<String, Long, Double> candidate : candidates) {
      hits.add(candidate._1(), candidate._2(), candidate._3());
    }

    return hits;
  }

  /**
   * What the merge kept before the heap replaced it: copy the list, sort it with the merge order,
   * keep the first limit.
   */
  private static List<IndexSearchResult> reference(
      List<Tuple3<String, Long, Double>> candidates, boolean higherScoreFirst, int limit) {
    List<Tuple3<String, Long, Double>> sorted = new ArrayList<>(candidates);
    sorted.sort(mergeOrder(higherScoreFirst));
    int kept = Math.min(limit, sorted.size());
    List<IndexSearchResult> hits = new ArrayList<>(kept);
    for (int rank = 0; rank < kept; rank += 1) {
      Tuple3<String, Long, Double> candidate = sorted.get(rank);
      hits.add(IndexSearchResult.of(candidate._1(), candidate._2(), candidate._3()));
    }

    return hits;
  }

  /** The order of the merge, as a function of the score direction, the file and the row. */
  private static Comparator<Tuple3<String, Long, Double>> mergeOrder(boolean higherScoreFirst) {
    return (left, right) -> {
      int byScore = Double.compare(left._3(), right._3());
      int primary = higherScoreFirst ? -byScore : byScore;
      if (primary != 0) {
        return primary;
      }

      int byFile = left._1().compareTo(right._1());
      if (byFile != 0) {
        return byFile;
      }

      return Long.compare(left._2(), right._2());
    };
  }

  private static Tuple3<String, Long, Double> randomHit(Random random) {
    String dataFile = "file-" + random.nextInt(4) + ".parquet";
    long rowPosition = random.nextInt(4);
    // a small value set so equal scores, files and rows (exact duplicates) are common
    double score = new double[] {-0.0, 0.0, 0.5, 1.0, 2.0}[random.nextInt(5)];
    return hit(dataFile, rowPosition, score);
  }

  private static Tuple3<String, Long, Double> hit(String dataFile, long rowPosition, double score) {
    return new Tuple3<>(dataFile, rowPosition, score);
  }

  private static HitTopK roundTrip(HitTopK hits) throws IOException, ClassNotFoundException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(hits);
    }

    try (ObjectInputStream in =
        new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      return (HitTopK) in.readObject();
    }
  }
}
