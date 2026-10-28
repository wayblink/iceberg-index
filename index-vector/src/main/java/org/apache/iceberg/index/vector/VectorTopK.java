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

import java.util.List;
import org.apache.iceberg.index.IndexSearchResult;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

/**
 * Keeps the best {@code topK} candidates without holding a result object for each of them.
 *
 * <p>A search that scanned every candidate and sorted at the end paid for an object per candidate
 * and for a sort over all of them. This keeps a bounded heap of primitive scores instead: the root
 * is the worst candidate held, so a better one replaces it, and only the survivors become results.
 *
 * <p>The order is the one results are returned in: the metric's direction first (a distance is
 * better when smaller, a similarity when larger), then the data file path exactly as {@link
 * String#compareTo} orders it, then the row position. Two candidates equal on all three are ordered
 * by the one visited first, the same stability a full sort of the same records has. Where a path or
 * a row is not needed for a comparison it is not read.
 *
 * <p>The requested {@code topK} is not the size of anything: the target is the smaller of it and
 * the number of candidates the search knows it has, and the heap grows from a small capacity as
 * candidates arrive. Asking for a million results from three records allocates for three.
 */
final class VectorTopK {

  /** What a candidate is called, resolved only when two candidates tie on score. */
  @FunctionalInterface
  interface Locations {
    String dataFile(int location);
  }

  private static final int INITIAL_CAPACITY = 16;

  /** The longest array a JVM is allowed to allocate, minus the usual header allowance. */
  private static final long MAX_TARGET = Integer.MAX_VALUE - 8L;

  private final long target;
  private final VectorScoring scoring;
  private final Locations locations;

  private int capacity;
  private int size;
  private int visits;
  private boolean ordered;

  private double[] scores;
  private long[] rows;
  private int[] records;
  private int[] visitOrder;

  VectorTopK(int topK, long candidateCount, VectorScoring scoring, Locations locations) {
    Preconditions.checkArgument(topK >= 0, "Top-k must be non-negative: %s", topK);
    Preconditions.checkArgument(
        candidateCount >= 0, "Candidate count must be non-negative: %s", candidateCount);
    Preconditions.checkNotNull(scoring, "Scoring is null");
    Preconditions.checkNotNull(locations, "Locations are null");
    this.target = Math.min((long) topK, candidateCount);
    Preconditions.checkArgument(
        target <= MAX_TARGET,
        "A top-k of %s results cannot be held: at most %s can be",
        target,
        MAX_TARGET);
    this.scoring = scoring;
    this.locations = locations;
    this.capacity = (int) Math.min(target, INITIAL_CAPACITY);
    allocate(capacity);
  }

  /** How many candidates the heap can hold before it has to grow, for tests and diagnostics. */
  int capacity() {
    return capacity;
  }

  int size() {
    return size;
  }

  boolean isEmpty() {
    return size == 0;
  }

  /**
   * Offers one candidate.
   *
   * @param location what {@link Locations} turns into a data file path
   * @param rowPosition the row the candidate points at
   * @param score the score, in the metric's own direction
   */
  void offer(int location, long rowPosition, double score) {
    if (target == 0) {
      return;
    }

    int visit = visits++;
    if ((long) size < target) {
      if (size == capacity) {
        grow();
      }

      place(size, location, rowPosition, score, visit);
      size += 1;
      siftUp(size - 1);
      return;
    }

    if (compare(0, score, rowPosition, location, visit) <= 0) {
      // the worst candidate held is at least as good as this one
      return;
    }

    place(0, location, rowPosition, score, visit);
    siftDown(0);
  }

  /** The candidates that survived, best first. Orders the heap, so call it once. */
  List<IndexSearchResult> results() {
    order();
    ImmutableList.Builder<IndexSearchResult> results = ImmutableList.builderWithExpectedSize(size);
    for (int rank = 0; rank < size; rank++) {
      results.add(
          IndexSearchResult.of(locations.dataFile(records[rank]), rows[rank], scores[rank]));
    }

    return results.build();
  }

  /** The score at {@code rank}, best first; for tests that compare non-finite scores. */
  double scoreAt(int rank) {
    order();
    return scores[rank];
  }

  int recordAt(int rank) {
    order();
    return records[rank];
  }

  long rowAt(int rank) {
    order();
    return rows[rank];
  }

  /**
   * Turns the heap into the output order.
   *
   * <p>The root is the worst candidate, so removing it repeatedly yields worst first; writing each
   * removed candidate from the end of the arrays towards the front leaves the best at rank zero.
   */
  private void order() {
    if (ordered) {
      return;
    }

    int remaining = size;
    for (int rank = size - 1; rank >= 0; rank--) {
      swap(0, remaining - 1);
      remaining -= 1;
      siftDown(0, remaining);
    }

    ordered = true;
  }

  private void allocate(int newCapacity) {
    scores = new double[newCapacity];
    rows = new long[newCapacity];
    records = new int[newCapacity];
    visitOrder = new int[newCapacity];
  }

  private void grow() {
    long grown = Math.min(target, Math.max((long) capacity + 1L, (long) capacity * 2L));
    int newCapacity = (int) grown;
    double[] newScores = new double[newCapacity];
    long[] newRows = new long[newCapacity];
    int[] newRecords = new int[newCapacity];
    int[] newVisits = new int[newCapacity];
    System.arraycopy(scores, 0, newScores, 0, size);
    System.arraycopy(rows, 0, newRows, 0, size);
    System.arraycopy(records, 0, newRecords, 0, size);
    System.arraycopy(visitOrder, 0, newVisits, 0, size);
    this.scores = newScores;
    this.rows = newRows;
    this.records = newRecords;
    this.visitOrder = newVisits;
    this.capacity = newCapacity;
  }

  private void place(int at, int location, long rowPosition, double score, int visit) {
    scores[at] = score;
    rows[at] = rowPosition;
    records[at] = location;
    visitOrder[at] = visit;
  }

  /** Negative when the candidate at {@code at} is better than the offered one. */
  private int compare(int at, double score, long rowPosition, int location, int visit) {
    int byScore = scoring.compareScores(scores[at], score);
    if (byScore != 0) {
      return byScore;
    }

    int byPath = locations.dataFile(records[at]).compareTo(locations.dataFile(location));
    if (byPath != 0) {
      return byPath;
    }

    int byRow = Long.compare(rows[at], rowPosition);
    return byRow != 0 ? byRow : Integer.compare(visitOrder[at], visit);
  }

  /** Negative when the candidate at {@code one} is better than the one at {@code other}. */
  private int compare(int one, int other) {
    int byScore = scoring.compareScores(scores[one], scores[other]);
    if (byScore != 0) {
      return byScore;
    }

    int oneLocation = records[one];
    int otherLocation = records[other];
    int byPath = locations.dataFile(oneLocation).compareTo(locations.dataFile(otherLocation));
    if (byPath != 0) {
      return byPath;
    }

    int byRow = Long.compare(rows[one], rows[other]);
    return byRow != 0 ? byRow : Integer.compare(visitOrder[one], visitOrder[other]);
  }

  private void siftUp(int at) {
    int child = at;
    while (child > 0) {
      int parent = (child - 1) >>> 1;
      if (compare(child, parent) <= 0) {
        // the parent is at least as bad as the child, so the heap order holds
        break;
      }

      swap(child, parent);
      child = parent;
    }
  }

  /** Restores the heap property below {@code at}, where the root is the worst candidate. */
  private void siftDown(int at) {
    siftDown(at, size);
  }

  private void siftDown(int at, int heapSize) {
    int parent = at;
    while (true) {
      int left = 2 * parent + 1;
      if (left >= heapSize) {
        break;
      }

      int worst = left;
      int right = left + 1;
      if (right < heapSize && compare(right, left) > 0) {
        worst = right;
      }

      if (compare(worst, parent) <= 0) {
        break;
      }

      swap(worst, parent);
      parent = worst;
    }
  }

  private void swap(int left, int right) {
    double score = scores[left];
    scores[left] = scores[right];
    scores[right] = score;
    long row = rows[left];
    rows[left] = rows[right];
    rows[right] = row;
    int record = records[left];
    records[left] = records[right];
    records[right] = record;
    int visit = visitOrder[left];
    visitOrder[left] = visitOrder[right];
    visitOrder[right] = visit;
  }
}
