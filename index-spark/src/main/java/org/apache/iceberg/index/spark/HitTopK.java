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

import java.io.Serializable;
import java.util.List;
import org.apache.iceberg.index.IndexSearchResult;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * The best hits of one query, kept in a bounded heap instead of a list that is copied and sorted on
 * every merge step.
 *
 * <p>A merge that receives one candidate at a time would otherwise sort the whole list for each of
 * them, which costs a copy of the list and a comparison of every pair on the way. This keeps the
 * worst of the {@code limit} best hits at the root instead: a candidate that is not better than it
 * is dropped at the cost of one comparison, a better one replaces it at the cost of a path down the
 * heap, and only the survivors are sorted, once, when they are turned into results. The heap holds
 * no object per candidate: the score, the data file and the row position are three parallel arrays,
 * and arrays grow from a small capacity as candidates arrive, so asking for a large top-k from a
 * partition that saw few hits does not allocate for the top-k.
 *
 * <p>The order is the one the results are returned in: the direction of the score first (a distance
 * is better when smaller, a similarity when larger), then the data file path as {@link
 * String#compareTo} orders it, then the row position. The result is ordered even when nothing is
 * dropped: a list shorter than the limit arrives in whatever order the merge produced, and two hits
 * with the same score would then reach the caller in an order that depends on how the tasks were
 * scheduled. The order is a function of the score and of the data file and position that break
 * ties, so a query has one answer. Two hits equal on all three are the same hit, so unlike a heap
 * that keeps candidate indices this one needs no arrival counter to keep a tie stable.
 *
 * <p>Instances are the accumulators of a distributed merge, so they are serialized between the
 * tasks that build them and the driver that collects them; see {@code aggregateHits}.
 */
final class HitTopK implements Serializable {

  private static final long serialVersionUID = 1L;

  private static final int INITIAL_CAPACITY = 16;

  private final boolean higherScoreFirst;
  private final int limit;

  private int capacity;
  private int size;
  private boolean sorted;

  private String[] dataFiles;
  private long[] rowPositions;
  private double[] scores;

  HitTopK(boolean higherScoreFirst, int limit) {
    Preconditions.checkArgument(limit >= 0, "Limit must be non-negative: %s", limit);
    this.higherScoreFirst = higherScoreFirst;
    this.limit = limit;
    this.capacity = Math.min(limit, INITIAL_CAPACITY);
    this.dataFiles = new String[capacity];
    this.rowPositions = new long[capacity];
    this.scores = new double[capacity];
  }

  int size() {
    return size;
  }

  boolean isEmpty() {
    return size == 0;
  }

  /**
   * Offers one candidate, keeping it only when it is better than the worst hit held.
   *
   * <p>{@code limit} caps what is kept: a candidate beyond the limit is dropped, and the size of
   * the accumulator saturates at the limit rather than counting every candidate.
   */
  void add(String dataFile, long rowPosition, double score) {
    if (limit == 0) {
      return;
    }

    if (size < limit) {
      if (size == capacity) {
        grow();
      }

      place(size, dataFile, rowPosition, score);
      size += 1;
      siftUp(size - 1);
      return;
    }

    if (compare(0, dataFile, rowPosition, score) <= 0) {
      // the worst hit held is at least as good as this one
      return;
    }

    place(0, dataFile, rowPosition, score);
    siftDown(0);
  }

  /**
   * Offers every hit of another accumulator.
   *
   * <p>The other one holds only its own best {@code limit} hits, so the best of a partition is kept
   * when two accumulators built from the same limit merge: the two together hold every hit that can
   * reach the global top-k.
   */
  void addAll(HitTopK other) {
    for (int at = 0; at < other.size; at += 1) {
      add(other.dataFiles[at], other.rowPositions[at], other.scores[at]);
    }
  }

  /**
   * The best hits held, best first, at most {@code requested} of them.
   *
   * <p>Sorts the survivors on the first call, so later calls see the same order.
   */
  List<IndexSearchResult> results(int requested) {
    Preconditions.checkArgument(
        requested >= 0, "Requested count must be non-negative: %s", requested);
    sort();
    int count = Math.min(requested, size);
    List<IndexSearchResult> hits = Lists.newArrayListWithCapacity(count);
    for (int rank = 0; rank < count; rank += 1) {
      hits.add(IndexSearchResult.of(dataFiles[rank], rowPositions[rank], scores[rank]));
    }

    return ImmutableList.copyOf(hits);
  }

  /**
   * Orders the survivors the way results are returned.
   *
   * <p>The root is the worst hit held, so removing it repeatedly yields worst first; writing each
   * removed hit from the end of the arrays towards the front leaves the best at rank zero.
   */
  private void sort() {
    if (sorted) {
      return;
    }

    int remaining = size;
    for (int rank = size - 1; rank >= 0; rank -= 1) {
      swap(0, remaining - 1);
      remaining -= 1;
      siftDown(0, remaining);
    }

    sorted = true;
  }

  private void grow() {
    int grown = (int) Math.min((long) limit, Math.max(INITIAL_CAPACITY, (long) capacity * 2L));
    String[] grownDataFiles = new String[grown];
    long[] grownRowPositions = new long[grown];
    double[] grownScores = new double[grown];
    System.arraycopy(dataFiles, 0, grownDataFiles, 0, size);
    System.arraycopy(rowPositions, 0, grownRowPositions, 0, size);
    System.arraycopy(scores, 0, grownScores, 0, size);
    this.dataFiles = grownDataFiles;
    this.rowPositions = grownRowPositions;
    this.scores = grownScores;
    this.capacity = grown;
  }

  private void place(int at, String dataFile, long rowPosition, double score) {
    dataFiles[at] = dataFile;
    rowPositions[at] = rowPosition;
    scores[at] = score;
  }

  /** Negative when the hit at {@code at} is better than the offered one. */
  private int compare(int at, String dataFile, long rowPosition, double score) {
    return compare(
        higherScoreFirst,
        scores[at],
        dataFiles[at],
        rowPositions[at],
        score,
        dataFile,
        rowPosition);
  }

  /** Negative when the hit at {@code one} is better than the hit at {@code other}. */
  private int compare(int one, int other) {
    return compare(
        higherScoreFirst,
        scores[one],
        dataFiles[one],
        rowPositions[one],
        scores[other],
        dataFiles[other],
        rowPositions[other]);
  }

  /**
   * Orders two hits the way results are returned: score direction, data file, row position.
   *
   * <p>The merge of the per-file results and the heap that bounds it share this, so a hit that
   * sorts first in one sorts first in the other.
   */
  static int compare(
      boolean higherScoreFirst,
      double leftScore,
      String leftDataFile,
      long leftRowPosition,
      double rightScore,
      String rightDataFile,
      long rightRowPosition) {
    int byScore = Double.compare(leftScore, rightScore);
    int primary = higherScoreFirst ? -byScore : byScore;
    if (primary != 0) {
      return primary;
    }

    int byFile = leftDataFile.compareTo(rightDataFile);
    if (byFile != 0) {
      return byFile;
    }

    return Long.compare(leftRowPosition, rightRowPosition);
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

  private void siftDown(int at) {
    siftDown(at, size);
  }

  /** Restores the heap property below {@code at}, where the root is the worst hit held. */
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
    String dataFile = dataFiles[left];
    dataFiles[left] = dataFiles[right];
    dataFiles[right] = dataFile;
    long rowPosition = rowPositions[left];
    rowPositions[left] = rowPositions[right];
    rowPositions[right] = rowPosition;
    double score = scores[left];
    scores[left] = scores[right];
    scores[right] = score;
  }
}
