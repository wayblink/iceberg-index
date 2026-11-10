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

import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

/**
 * The rows a scalar filter admits, per data file, for an index search that skips the rest.
 *
 * <p>An index proposes rows by {@code (data file, row position)}, so a candidate set is the same
 * pairs the filter kept. A searcher asks it whether a row it is about to score may be in the
 * answer; a row it turns down is never scored, which is the whole cost saving of filtering before
 * the search rather than after it.
 *
 * <p>The positions of a file are held as a sorted array and looked up through an open-addressed
 * table, so both the bytes held and the time to answer follow the number of rows that survived the
 * filter, not the number of rows in the table. A filter that keeps a large share of the table makes
 * the set large and the saving small, which is why a caller decides whether to filter before the
 * search at all rather than always doing it.
 *
 * <p>A candidate set is built on the driver and read on an executor, so it is serializable and
 * holds nothing but primitives and strings.
 */
public final class IndexCandidates implements Serializable {
  private static final long serialVersionUID = 1L;

  private static final IndexCandidates EMPTY = new IndexCandidates(new LinkedHashMap<>(), 0L);

  private final Map<String, LongSet> byFile;
  private final long count;

  private IndexCandidates(Map<String, LongSet> byFile, long count) {
    this.byFile = byFile;
    this.count = count;
  }

  /**
   * Returns the candidate set holding the given positions.
   *
   * @param positionsByFile the admitted row positions of each data file, in any order
   */
  public static IndexCandidates of(Map<String, long[]> positionsByFile) {
    Preconditions.checkArgument(positionsByFile != null, "Candidate positions are null");

    Map<String, LongSet> byFile = new LinkedHashMap<>();
    long count = 0;
    for (Map.Entry<String, long[]> entry : positionsByFile.entrySet()) {
      String dataFile = entry.getKey();
      Preconditions.checkArgument(dataFile != null && !dataFile.isEmpty(), "Data file is empty");
      Preconditions.checkArgument(entry.getValue() != null, "Positions of %s are null", dataFile);
      if (entry.getValue().length == 0) {
        continue;
      }

      LongSet set = LongSet.of(entry.getValue());
      byFile.put(dataFile, set);
      count = Math.addExact(count, set.size());
    }

    return byFile.isEmpty() ? EMPTY : new IndexCandidates(byFile, count);
  }

  /**
   * Returns the set that admits no row at all, which is what a filter that keeps nothing leaves.
   */
  public static IndexCandidates empty() {
    return EMPTY;
  }

  /** Returns the rows of {@code dataFile} that survived the filter, or null when none did. */
  public LongSet forFile(String dataFile) {
    return byFile.get(dataFile);
  }

  /** Whether {@code rowPosition} of {@code dataFile} survived the filter. */
  public boolean contains(String dataFile, long rowPosition) {
    LongSet set = byFile.get(dataFile);
    return set != null && set.contains(rowPosition);
  }

  /** How many rows survived the filter, across every file. */
  public long count() {
    return count;
  }

  public boolean isEmpty() {
    return count == 0;
  }

  /** The results of a search the set admits, in the order they were given. */
  public List<IndexSearchResult> retain(List<IndexSearchResult> results) {
    if (results.isEmpty() || isEmpty()) {
      return ImmutableList.of();
    }

    ImmutableList.Builder<IndexSearchResult> admitted = ImmutableList.builder();
    for (IndexSearchResult result : results) {
      if (contains(result.dataFile(), result.rowPosition())) {
        admitted.add(result);
      }
    }

    return admitted.build();
  }

  @Override
  public String toString() {
    return String.format(Locale.ROOT, "IndexCandidates(rows=%d, files=%d)", count, byFile.size());
  }

  /** The admitted positions of one data file, in ascending order, and a lookup over them. */
  public static final class LongSet implements Serializable {
    private static final long serialVersionUID = 1L;

    /** Row positions are never negative, so -1 marks a free slot. */
    private static final long FREE = -1L;

    private final long[] positions;
    private final long[] table;
    private final int mask;

    private LongSet(long[] positions, long[] table, int mask) {
      this.positions = positions;
      this.table = table;
      this.mask = mask;
    }

    private static LongSet of(long[] unsorted) {
      long[] positions = unsorted.clone();
      Arrays.sort(positions);
      int distinct = 0;
      for (int index = 0; index < positions.length; index++) {
        Preconditions.checkArgument(
            positions[index] >= 0, "Invalid row position: %s", positions[index]);
        if (index == 0 || positions[index] != positions[index - 1]) {
          positions[distinct++] = positions[index];
        }
      }

      if (distinct != positions.length) {
        positions = Arrays.copyOf(positions, distinct);
      }

      // a table twice the size of the set keeps probing short: the load factor stays at or below
      // one half, so a miss rarely walks more than a couple of slots
      int capacity = Integer.highestOneBit(Math.max(4, distinct * 2 - 1)) << 1;
      long[] table = new long[capacity];
      Arrays.fill(table, FREE);
      int mask = capacity - 1;
      for (long position : positions) {
        int slot = hash(position) & mask;
        while (table[slot] != FREE) {
          slot = (slot + 1) & mask;
        }

        table[slot] = position;
      }

      return new LongSet(positions, table, mask);
    }

    private static int hash(long position) {
      // the mixer keeps clustered positions, which a row range produces, from piling on one slot
      long mixed = position * -7046029254386353131L;
      return (int) (mixed ^ (mixed >>> 32));
    }

    /** Whether this file admits {@code rowPosition}. */
    public boolean contains(long rowPosition) {
      if (rowPosition < 0) {
        return false;
      }

      int slot = hash(rowPosition) & mask;
      while (true) {
        long key = table[slot];
        if (key == rowPosition) {
          return true;
        }

        if (key == FREE) {
          return false;
        }

        slot = (slot + 1) & mask;
      }
    }

    /** The admitted positions in ascending order. */
    public long[] positions() {
      return positions.clone();
    }

    public int size() {
      return positions.length;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }

      if (!(other instanceof LongSet)) {
        return false;
      }

      LongSet that = (LongSet) other;
      return Arrays.equals(positions, that.positions);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(Arrays.hashCode(positions));
    }

    @Override
    public String toString() {
      return String.format(Locale.ROOT, "LongSet(positions=%d)", positions.length);
    }
  }
}
