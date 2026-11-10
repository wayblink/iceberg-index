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
package org.apache.iceberg.index;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** A candidate set answers "may this row be in the answer" for a row the index proposes. */
class TestIndexCandidates {

  @Test
  void answersForThePositionsItHolds() {
    IndexCandidates candidates =
        IndexCandidates.of(
            Map.of("a.parquet", new long[] {0, 5, 900}, "b.parquet", new long[] {7}));

    assertThat(candidates.count()).isEqualTo(4);
    assertThat(candidates.isEmpty()).isFalse();
    assertThat(candidates.contains("a.parquet", 0)).isTrue();
    assertThat(candidates.contains("a.parquet", 900)).isTrue();
    assertThat(candidates.contains("b.parquet", 7)).isTrue();
  }

  @Test
  void answersNoForWhatItDoesNotHold() {
    IndexCandidates candidates = IndexCandidates.of(Map.of("a.parquet", new long[] {3}));

    assertThat(candidates.contains("a.parquet", 4)).isFalse();
    assertThat(candidates.contains("a.parquet", -1)).isFalse();
    assertThat(candidates.contains("other.parquet", 3)).isFalse();
  }

  @Test
  void holdsPositionsBeyondTheIntRange() {
    long beyond = (long) Integer.MAX_VALUE + 42;
    IndexCandidates candidates = IndexCandidates.of(Map.of("a.parquet", new long[] {beyond}));

    assertThat(candidates.contains("a.parquet", beyond)).isTrue();
    assertThat(candidates.contains("a.parquet", beyond + 1)).isFalse();
  }

  @Test
  void reportsNoCandidatesForAFileItDoesNotMention() {
    IndexCandidates candidates = IndexCandidates.of(Map.of("a.parquet", new long[] {1}));

    assertThat(candidates.forFile("a.parquet")).isNotNull();
    assertThat(candidates.forFile("other.parquet")).isNull();
  }

  @Test
  void holdsManyClusteredPositions() {
    long[] positions = new long[10_000];
    for (int i = 0; i < positions.length; i++) {
      positions[i] = i * 3L;
    }

    IndexCandidates candidates = IndexCandidates.of(Map.of("a.parquet", positions));
    IndexCandidates.LongSet set = candidates.forFile("a.parquet");
    for (long position : positions) {
      assertThat(set.contains(position)).isTrue();
      assertThat(set.contains(position + 1)).isFalse();
    }

    assertThat(set.size()).isEqualTo(positions.length);
  }

  @Test
  void keepsTheResultsTheSetAdmits() {
    IndexCandidates candidates =
        IndexCandidates.of(Map.of("a.parquet", new long[] {1, 3}, "b.parquet", new long[] {2}));
    List<IndexSearchResult> results =
        List.of(
            IndexSearchResult.of("a.parquet", 1, 0.1),
            IndexSearchResult.of("a.parquet", 2, 0.2),
            IndexSearchResult.of("b.parquet", 2, 0.3),
            IndexSearchResult.of("c.parquet", 1, 0.4));

    assertThat(candidates.retain(results))
        .extracting(IndexSearchResult::dataFile, IndexSearchResult::rowPosition)
        .containsExactly(
            org.assertj.core.groups.Tuple.tuple("a.parquet", 1L),
            org.assertj.core.groups.Tuple.tuple("b.parquet", 2L));
  }

  @Test
  void isEmptyWhenNothingSurvivesTheFilter() {
    IndexCandidates empty = IndexCandidates.empty();

    assertThat(empty.isEmpty()).isTrue();
    assertThat(empty.count()).isZero();
    assertThat(empty.forFile("a.parquet")).isNull();
    assertThat(empty.retain(List.of(IndexSearchResult.of("a.parquet", 1, 0.1)))).isEmpty();
  }

  @Test
  void travelsToAnExecutor() throws Exception {
    IndexCandidates candidates =
        IndexCandidates.of(
            Map.of("a.parquet", new long[] {0, 5, 900}, "b.parquet", new long[] {7}));

    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(candidates);
    }

    IndexCandidates restored;
    try (ObjectInputStream in =
        new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      restored = (IndexCandidates) in.readObject();
    }

    assertThat(restored.count()).isEqualTo(candidates.count());
    assertThat(restored.contains("a.parquet", 900)).isTrue();
    assertThat(restored.contains("b.parquet", 7)).isTrue();
    assertThat(restored.contains("b.parquet", 8)).isFalse();
  }

  @Test
  void keepsPositionsInTheOrderItWasGiven() {
    long[] unsorted = new long[] {9, 1, 5};
    IndexCandidates candidates = IndexCandidates.of(Map.of("a.parquet", unsorted));

    assertThat(candidates.forFile("a.parquet").positions())
        .as("a caller may rely on a stable order when it reads them back")
        .containsExactly(1L, 5L, 9L);
    assertThat(
            Arrays.stream(unsorted)
                .allMatch(position -> candidates.contains("a.parquet", position)))
        .isTrue();
  }
}
