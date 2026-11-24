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

import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

/**
 * The positions a lookup asked for are searched, not scanned.
 *
 * <p>A fetch-back judges every row of a data file against the positions it was asked for, and a
 * window can hold millions of them. Asking whether a row is one of them by scanning the window per
 * row makes the read quadratic in its own answer: the positions are sorted once, on the driver, and
 * each row then costs a binary search.
 */
class TestFetchBackAskedPositions {

  private static final int WINDOW = 10_000;
  private static final int ASKS = 20_000;

  @Test
  void aLookupIsSortedSoThatItsRowsAreSearched() {
    // the order a caller happened to collect the positions in, and a duplicate a lookup may carry
    List<Long> positions = Lists.newArrayList(7L, 3L, 9L, 1L, 3L, 5L);
    long[] sorted = IndexEntries.askedPositions(positions);

    assertThat(sorted).as("the positions, sorted").containsExactly(1L, 3L, 3L, 5L, 7L, 9L);
    for (long position : positions) {
      assertThat(IndexEntries.askedFor(sorted, position))
          .as("the row at position %d was asked for", position)
          .isTrue();
    }

    assertThat(IndexEntries.askedFor(sorted, 0L)).as("a row nobody asked for").isFalse();
    assertThat(IndexEntries.askedFor(sorted, 4L)).as("a row nobody asked for").isFalse();
    assertThat(IndexEntries.askedFor(sorted, 10L)).as("a row past the window").isFalse();
  }

  @Test
  void aLookupOfNoPositionsAsksForEveryRow() {
    assertThat(IndexEntries.askedPositions(null)).isNull();
    assertThat(IndexEntries.askedPositions(Collections.emptyList())).isNull();
    assertThat(IndexEntries.askedFor(null, 0L)).isTrue();
    assertThat(IndexEntries.askedFor(null, Long.MAX_VALUE)).isTrue();
  }

  /**
   * A row search does not cost what a scan of the window costs.
   *
   * <p>The two are measured on the same window in the same run, so the number that is compared is
   * the shape of the lookup rather than the speed of the machine: a lookup that scans the window
   * per row is hundreds of times more expensive here, and only its being a search keeps that from
   * growing with the square of the window.
   */
  @Test
  void aRowSearchDoesNotScanTheWindow() {
    long[] window = IndexEntries.askedPositions(shuffled(WINDOW));
    List<Long> scanned = Lists.newArrayList(shuffled(WINDOW));
    long[] asks = asks(ASKS);

    // warm both up so the measurement is of compiled code
    long found = search(window, asks) + scan(scanned, asks);

    long searchNanos = time(() -> search(window, asks));
    long scanNanos = time(() -> scan(scanned, asks));

    assertThat(found).as("some of the asks name a row of the window").isNotZero();
    assertThat(searchNanos * 20)
        .as(
            "a search of %d positions costs %d ns, a scan of them costs %d ns",
            WINDOW, searchNanos, scanNanos)
        .isLessThan(scanNanos);
  }

  private static long search(long[] window, long[] asks) {
    long found = 0L;
    for (long ask : asks) {
      if (IndexEntries.askedFor(window, ask)) {
        found++;
      }
    }

    return found;
  }

  private static long scan(List<Long> window, long[] asks) {
    long found = 0L;
    for (long ask : asks) {
      if (window.contains(ask)) {
        found++;
      }
    }

    return found;
  }

  private static long time(Runnable body) {
    long start = System.nanoTime();
    body.run();
    return System.nanoTime() - start;
  }

  private static List<Long> shuffled(int size) {
    List<Long> positions = Lists.newArrayListWithCapacity(size);
    for (long position = 0; position < size; position++) {
      positions.add(position);
    }

    Collections.shuffle(positions, new Random(11L));
    return positions;
  }

  private static long[] asks(int count) {
    Random random = new Random(17L);
    long[] asks = new long[count];
    for (int i = 0; i < count; i++) {
      asks[i] = random.nextInt(2 * WINDOW);
    }

    return asks;
  }
}
