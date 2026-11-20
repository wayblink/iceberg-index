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
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.index.IndexAdapter;
import org.apache.iceberg.index.IndexSearchResult;
import org.apache.iceberg.index.vector.ListBlockCache;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class TestExecutorIndexCache {

  private static final int OPAQUE_SEARCHER_BYTES = 32;
  private static final IndexAdapter.PreparedSearch EMPTY = (query, topK) -> ImmutableList.of();

  private final ExecutorIndexCache cache = ExecutorIndexCache.get();

  @AfterEach
  void clearCache() {
    cache.clear();
    ExecutorIndexCache.sweepIntervalForTests(java.util.concurrent.TimeUnit.SECONDS.toNanos(30));
  }

  @Test
  void everyTaskOfAQueryReportsAndOneOfThemCarriesTheChargeProjection() {
    // A query runs one task per index file and each task adds a file, so each task's report is a
    // more complete picture than the one before it and the driver keeps the newest. The projection
    // is a map over every cached file, so it travels once per query rather than once per task.
    IndexCacheLocations.Report first = cache.reportForTask("query-1", "1", "host");
    IndexCacheLocations.Report second = cache.reportForTask("query-1", "1", "host");
    IndexCacheLocations.Report other = cache.reportForTask("query-2", "1", "host");

    assertThat(first).isNotNull();
    assertThat(first.metadataShape()).as("the first task of a query carries the shape").isNotNull();
    assertThat(second).isNotNull();
    assertThat(second.metadataShape()).as("a later task does not").isNull();
    assertThat(second.seq()).as("and it is ordered after the first").isGreaterThan(first.seq());
    assertThat(other.metadataShape()).as("a new query carries one again").isNotNull();
  }

  @Test
  void listBlocksAreKeptAndReused() {
    ExecutorIndexCache.Config enabled = config(1024 * 1024);
    ListBlockCache blocks = cache.listBlocks(enabled);

    assertThat(blocks.get("s3://bucket/index-a.puffin", 0, 16)).isNull();
    byte[] block = new byte[16];
    blocks.put("s3://bucket/index-a.puffin", 0, block);

    assertThat(blocks.get("s3://bucket/index-a.puffin", 0, 16)).isSameAs(block);
    assertThat(blocks.get("s3://bucket/index-a.puffin", 0, 8))
        .as("a block of another length is another block")
        .isNull();
    assertThat(blocks.stats().hits()).isEqualTo(1);
    assertThat(blocks.stats().misses()).isEqualTo(2);
    assertThat(blocks.stats().loads()).isEqualTo(1);
    assertThat(blocks.stats().bytes()).isEqualTo(16);
  }

  @Test
  void listBlocksStayWithinTheBudgetTheyShareWithWholeFiles() {
    ExecutorIndexCache.Config enabled = config(1024);
    ListBlockCache blocks = cache.listBlocks(enabled);

    blocks.put("s3://bucket/index-a.puffin", 0, new byte[2048]);
    assertThat(blocks.get("s3://bucket/index-a.puffin", 0, 2048))
        .as("a block larger than the whole budget is not admitted")
        .isNull();
    assertThat(blocks.stats().loads()).isZero();

    blocks.put("s3://bucket/index-a.puffin", 0, new byte[500]);
    blocks.put("s3://bucket/index-a.puffin", 500, new byte[600]);
    assertThat(blocks.get("s3://bucket/index-a.puffin", 0, 500))
        .as("the older block was evicted to keep the newer one inside the budget")
        .isNull();
    assertThat(blocks.get("s3://bucket/index-a.puffin", 500, 600)).isNotNull();
  }

  @Test
  void aListBlockCannotSpendAnOutstandingLoadReservation() {
    int budget = 16_384;
    ExecutorIndexCache.Config enabled = config(budget);
    ListBlockCache blocks = cache.listBlocks(enabled);
    cache.file(
        "s3://bucket/whole.puffin",
        enabled,
        "adapter",
        12_000L,
        () -> {
          blocks.put("s3://bucket/blocks.puffin", 0, new byte[8_000]);
          assertThat(blocks.contains("s3://bucket/blocks.puffin", 0, 8_000)).isFalse();
          return load(new AtomicInteger(), 12_000);
        });
  }

  @Test
  void offloadClearsBlocksCachedByQueriesWithoutAManagedCopy() {
    ExecutorIndexCache.Config enabled = config(1 << 20);
    String location = "s3://bucket/query.puffin";
    ListBlockCache first = cache.listBlocks(enabled, "first");
    ListBlockCache second = cache.listBlocks(enabled, "second");
    first.put(location, 0, new byte[16]);
    second.put(location, 0, new byte[16]);

    assertThat(cache.releaseIndex("first", 1)).isTrue();
    first.put(location, 0, new byte[16]);
    assertThat(first.contains(location, 0, 16)).isFalse();
    assertThat(second.contains(location, 0, 16)).isTrue();

    ListBlockCache later = cache.listBlocks(enabled, "first");
    later.put(location, 0, new byte[16]);
    assertThat(later.contains(location, 0, 16)).isTrue();
  }

  @Test
  void aDisabledCacheKeepsNoBlocks() {
    ListBlockCache blocks = cache.listBlocks(ExecutorIndexCache.Config.of(false, 1024));
    blocks.put("s3://bucket/index-a.puffin", 0, new byte[16]);

    assertThat(blocks.get("s3://bucket/index-a.puffin", 0, 16)).isNull();
    assertThat(blocks.stats().loads()).isZero();
  }

  @Test
  void aWholeFileReclaimsListBlocksToFit() {
    AtomicInteger loads = new AtomicInteger();
    ExecutorIndexCache.Config enabled = config(16_384);
    ListBlockCache blocks = cache.listBlocks(enabled);
    blocks.put("s3://bucket/a.puffin", 0, new byte[7_500]);
    blocks.put("s3://bucket/a.puffin", 7_500, new byte[7_500]);

    cache.file("s3://bucket/whole.puffin", enabled, "adapter", () -> load(loads, 1_500));

    assertThat(cache.stats().entries()).as("the whole file is cached").isEqualTo(1);
    assertThat(blocks.contains("s3://bucket/a.puffin", 0, 7_500))
        .as("the least recently used block made room for the whole file")
        .isFalse();
    assertThat(blocks.contains("s3://bucket/a.puffin", 7_500, 7_500))
        .as("the blocks the file did not need keep their room")
        .isTrue();
    assertThat(cache.stats().bytes())
        .as("files and blocks together stay within the budget")
        .isLessThanOrEqualTo(16_384L);
    assertThat(blocks.stats().evictions())
        .as("the whole file's reclaim is not the search's own block eviction")
        .isZero();
  }

  @Test
  void blocksAndFilesTogetherStayWithinTheBudget() {
    AtomicInteger loads = new AtomicInteger();
    ExecutorIndexCache.Config enabled = config(16_384);
    ListBlockCache blocks = cache.listBlocks(enabled);
    blocks.put("s3://bucket/a.puffin", 0, new byte[5_000]);
    blocks.put("s3://bucket/a.puffin", 5_000, new byte[5_000]);

    cache.file("s3://bucket/one.puffin", enabled, "adapter", () -> load(loads, 1_000));
    cache.file("s3://bucket/two.puffin", enabled, "adapter", () -> load(loads, 1_000));

    assertThat(cache.stats().entries()).as("both files fit beside the blocks").isEqualTo(2);
    assertThat(blocks.contains("s3://bucket/a.puffin", 0, 5_000)).isTrue();
    assertThat(blocks.contains("s3://bucket/a.puffin", 5_000, 5_000)).isTrue();
    assertThat(cache.stats().bytes())
        .as("resident files and blocks share one budget")
        .isLessThanOrEqualTo(16_384L);
  }

  @Test
  void theCacheCountsTheBytesOfItsListBlocks() {
    ExecutorIndexCache.Config enabled = config(1 << 20);
    ListBlockCache blocks = cache.listBlocks(enabled);

    blocks.put("s3://bucket/a.puffin", 0, new byte[16]);
    assertThat(cache.stats().entries()).as("a block is not a whole-file entry").isZero();
    assertThat(cache.stats().bytes()).isEqualTo(16L);

    blocks.put("s3://bucket/a.puffin", 16, new byte[20]);
    assertThat(cache.stats().bytes()).isEqualTo(36L);
  }

  @Test
  void releasingAnIndexClearsItsListBlocks() {
    ExecutorIndexCache.Config enabled = config(1 << 20);
    String first = cacheKey("table", "adapter", "s3://bucket/a.puffin");
    String second = cacheKey("table", "adapter", "s3://bucket/b.puffin");
    ListBlockCache blocks = cache.listBlocks(enabled);

    cache.applyTarget("index-1", 1, Set.of(first, second));
    blocks.put("s3://bucket/a.puffin", 0, new byte[16]);
    blocks.put("s3://bucket/b.puffin", 0, new byte[16]);

    cache.applyTarget("index-1", 2, Set.of(first));
    assertThat(blocks.contains("s3://bucket/b.puffin", 0, 16))
        .as("a file the new target no longer names gives up its blocks")
        .isFalse();
    assertThat(blocks.contains("s3://bucket/a.puffin", 0, 16))
        .as("a file the target keeps keeps its blocks")
        .isTrue();

    assertThat(cache.releaseCopy("index-1", first)).isTrue();
    assertThat(blocks.contains("s3://bucket/a.puffin", 0, 16)).isFalse();
    assertThat(cache.stats().bytes()).isZero();

    cache.applyTarget("index-1", 3, Set.of(second));
    blocks.put("s3://bucket/b.puffin", 0, new byte[16]);
    assertThat(cache.releaseIndex("index-1", 3)).isTrue();
    assertThat(blocks.contains("s3://bucket/b.puffin", 0, 16)).isFalse();
    assertThat(cache.stats().bytes()).isZero();
  }

  @Test
  void theIdleSweepDropsBlocksNoSearchRead() throws Exception {
    ExecutorIndexCache.Config ttl =
        ExecutorIndexCache.Config.of(true, 1 << 20, ExecutorIndexCache.Mode.PIN_ALL, 1);
    ExecutorIndexCache.sweepIntervalForTests(0);
    ListBlockCache blocks = cache.listBlocks(ttl);
    blocks.put("s3://bucket/a.puffin", 0, new byte[16]);
    assertThat(blocks.contains("s3://bucket/a.puffin", 0, 16)).isTrue();

    Thread.sleep(30);
    // another load makes the sweep due
    cache.file("s3://bucket/b.puffin", ttl, "adapter", () -> load(new AtomicInteger(), 10));

    assertThat(blocks.get("s3://bucket/a.puffin", 0, 16))
        .as("a block no search has read within the idle window is dropped")
        .isNull();
    assertThat(blocks.stats().evictions()).as("an idle block is not a budget eviction").isZero();
    assertThat(cache.stats().evictions()).as("nor a whole-file eviction").isZero();
  }

  @Test
  void aViewReportsOnlyItsOwnSearch() {
    ExecutorIndexCache.Config enabled = config(1024);
    ListBlockCache first = cache.listBlocks(enabled);
    first.put("s3://bucket/a.puffin", 0, new byte[500]);
    first.put("s3://bucket/a.puffin", 500, new byte[600]);
    assertThat(first.get("s3://bucket/a.puffin", 500, 600)).isNotNull();
    assertThat(first.get("s3://bucket/a.puffin", 0, 500)).isNull();
    assertThat(first.stats().hits()).isEqualTo(1);
    assertThat(first.stats().misses()).isEqualTo(1);
    assertThat(first.stats().loads()).isEqualTo(2);
    assertThat(first.stats().evictions()).isEqualTo(1);
    assertThat(first.stats().bytes()).isEqualTo(1_100);

    ListBlockCache second = cache.listBlocks(enabled);
    assertThat(second.contains("s3://bucket/a.puffin", 500, 600)).isTrue();
    assertThat(second.stats().hits()).isZero();
    assertThat(second.stats().misses()).as("contains does not count as a lookup").isZero();
    assertThat(second.stats().loads()).as("another view's puts are not this search's").isZero();
    assertThat(second.stats().evictions()).isZero();
    assertThat(second.stats().bytes()).isZero();

    assertThat(second.get("s3://bucket/a.puffin", 500, 600)).isNotNull();
    assertThat(second.stats().hits()).isEqualTo(1);
    assertThat(first.stats().hits()).as("this view's counters did not move").isEqualTo(1);
  }

  @Test
  void aViewDoesNotReportTheExecutorLifetimeCounters() {
    AtomicInteger loads = new AtomicInteger();
    ExecutorIndexCache.Config pinHot = mode(ExecutorIndexCache.Mode.PIN_HOT, 25_000);
    for (String name : List.of("a", "b", "c")) {
      cache.file("s3://bucket/" + name + ".puffin", pinHot, "adapter", () -> load(loads, 10_000));
      cache.file("s3://bucket/" + name + ".puffin", pinHot, "adapter", () -> load(loads, 10_000));
    }
    assertThat(cache.stats().evictions()).as("the executor evicted a whole file").isPositive();

    ListBlockCache view = cache.listBlocks(pinHot);
    assertThat(view.stats().hits()).isZero();
    assertThat(view.stats().misses()).isZero();
    assertThat(view.stats().loads()).isZero();
    assertThat(view.stats().evictions()).isZero();
    assertThat(view.stats().bytes()).isZero();
  }

  @Test
  void loadsEachFileOnce() {
    AtomicInteger loads = new AtomicInteger();
    ExecutorIndexCache.Config enabled = config(1024 * 1024);

    for (int i = 0; i < 3; i++) {
      ExecutorIndexCache.CachedFile file =
          cache.file("s3://bucket/index-a.puffin", enabled, "adapter", () -> load(loads, 100));
      assertThat(file.bytes()).isEqualTo(100);
    }

    assertThat(loads).hasValue(1);
    assertThat(cache.stats().hits()).isEqualTo(2);
    assertThat(cache.stats().loads()).isEqualTo(1);
  }

  @Test
  void keepsFilesApartByLocationAndAdapter() {
    AtomicInteger loads = new AtomicInteger();
    ExecutorIndexCache.Config enabled = config(1024 * 1024);

    cache.file("s3://bucket/a.puffin", enabled, "adapter", () -> load(loads, 10));
    cache.file("s3://bucket/b.puffin", enabled, "adapter", () -> load(loads, 20));
    // the same payload read by another adapter prepares differently, so it is a different entry
    cache.file("s3://bucket/a.puffin", enabled, "other", () -> load(loads, 30));

    assertThat(loads).hasValue(3);
    assertThat(cache.stats().entries()).isEqualTo(3);
  }

  @Test
  void evictsLeastRecentlyUsedWhenOverBudget() {
    AtomicInteger loads = new AtomicInteger();
    ExecutorIndexCache.Config enabled = mode(ExecutorIndexCache.Mode.PIN_HOT, 25_000);

    cache.file("s3://bucket/a.puffin", enabled, "adapter", () -> load(loads, 10_000));
    cache.file("s3://bucket/b.puffin", enabled, "adapter", () -> load(loads, 10_000));
    // Two independent prepared states fit with metadata; the third requires eviction.
    cache.file("s3://bucket/a.puffin", enabled, "adapter", () -> load(loads, 10_000));
    cache.file("s3://bucket/b.puffin", enabled, "adapter", () -> load(loads, 10_000));
    assertThat(cache.stats().entries()).isEqualTo(2);
    cache.file("s3://bucket/c.puffin", enabled, "adapter", () -> load(loads, 10_000));
    cache.file("s3://bucket/c.puffin", enabled, "adapter", () -> load(loads, 10_000));

    assertThat(cache.stats().evictions()).isEqualTo(1);
    assertThat(cache.chargeSnapshot().usedBytes()).isLessThanOrEqualTo(25_000);
    assertThat(cache.stats().entries()).isEqualTo(2);
  }

  @Test
  void cachesAFileWithoutASizeLimitOfItsOwn() {
    // one entry is one prepared index file, and a file bigger than the others is still a file the
    // query reads every time: refusing it only makes the cache read it again. What bounds the cache
    // is the budget, not a second, smaller ceiling per file.
    AtomicInteger loads = new AtomicInteger();
    ExecutorIndexCache.Config enabled = config(1024 * 1024);

    cache.file("s3://bucket/big.puffin", enabled, "adapter", () -> load(loads, 100));
    cache.file("s3://bucket/big.puffin", enabled, "adapter", () -> load(loads, 100));

    assertThat(loads).as("the second read is served from the cache").hasValue(1);
    assertThat(cache.stats().entries()).isEqualTo(1);
  }

  @Test
  void pinHotAdmitsAFileOnlyOnceItIsAskedForTwice() {
    AtomicInteger loads = new AtomicInteger();
    ExecutorIndexCache.Config pinHot = mode(ExecutorIndexCache.Mode.PIN_HOT, 1 << 20);

    cache.file("s3://bucket/a.puffin", pinHot, "adapter", () -> load(loads, 10));
    assertThat(cache.stats().entries()).isZero();

    cache.file("s3://bucket/a.puffin", pinHot, "adapter", () -> load(loads, 10));
    assertThat(cache.stats().entries()).isEqualTo(1);

    cache.file("s3://bucket/a.puffin", pinHot, "adapter", () -> load(loads, 10));
    assertThat(cache.stats().hits()).isEqualTo(1);
  }

  @Test
  void streamModeKeepsNothing() {
    AtomicInteger loads = new AtomicInteger();
    ExecutorIndexCache.Config stream = mode(ExecutorIndexCache.Mode.STREAM, 1 << 20);

    for (int i = 0; i < 3; i++) {
      cache.file("s3://bucket/a.puffin", stream, "adapter", () -> load(loads, 10));
    }

    assertThat(loads).hasValue(3);
    assertThat(cache.stats().entries()).isZero();
  }

  @Test
  void aFileAskedForTwiceOutranksOneSeenOnce() {
    AtomicInteger loads = new AtomicInteger();
    ExecutorIndexCache.Config pinHot = mode(ExecutorIndexCache.Mode.PIN_HOT, 25_000);

    // A is admitted after its second ask and promoted on its third; B is still probationary.
    cache.file("s3://bucket/a.puffin", pinHot, "adapter", () -> load(loads, 10_000));
    cache.file("s3://bucket/a.puffin", pinHot, "adapter", () -> load(loads, 10_000));
    cache.file("s3://bucket/a.puffin", pinHot, "adapter", () -> load(loads, 10_000));
    cache.file("s3://bucket/b.puffin", pinHot, "adapter", () -> load(loads, 10_000));
    cache.file("s3://bucket/b.puffin", pinHot, "adapter", () -> load(loads, 10_000));
    assertThat(cache.stats().entries()).isEqualTo(2);
    cache.file("s3://bucket/c.puffin", pinHot, "adapter", () -> load(loads, 10_000));
    cache.file("s3://bucket/c.puffin", pinHot, "adapter", () -> load(loads, 10_000));
    cache.file("s3://bucket/c.puffin", pinHot, "adapter", () -> load(loads, 10_000));

    assertThat(ExecutorIndexCache.get().residentKeys()).anyMatch(key -> key.endsWith("a.puffin"));
  }

  @Test
  void pinAllDoesNotEvictToMakeRoom() {
    AtomicInteger loads = new AtomicInteger();
    ExecutorIndexCache.Config pinAll = mode(ExecutorIndexCache.Mode.PIN_ALL, 25_000);

    cache.file("s3://bucket/a.puffin", pinAll, "adapter", () -> load(loads, 10_000));
    cache.file("s3://bucket/b.puffin", pinAll, "adapter", () -> load(loads, 10_000));
    assertThat(cache.stats().entries()).isEqualTo(2);
    // Pinned entries are not evicted to make room for a third independent state.
    cache.file("s3://bucket/c.puffin", pinAll, "adapter", () -> load(loads, 10_000));

    assertThat(ExecutorIndexCache.get().residentKeys()).noneMatch(key -> key.endsWith("c.puffin"));
  }

  @Test
  void unloadsEntriesThatNoQueryAskedFor() throws Exception {
    AtomicInteger loads = new AtomicInteger();
    ExecutorIndexCache.Config ttl =
        ExecutorIndexCache.Config.of(true, 1 << 20, ExecutorIndexCache.Mode.PIN_ALL, 1);
    ExecutorIndexCache.sweepIntervalForTests(0);

    cache.file("s3://bucket/a.puffin", ttl, "adapter", () -> load(loads, 10));
    assertThat(cache.stats().entries()).isEqualTo(1);

    Thread.sleep(30);
    cache.file("s3://bucket/b.puffin", ttl, "adapter", () -> load(loads, 10));

    assertThat(ExecutorIndexCache.get().residentKeys()).noneMatch(key -> key.endsWith("a.puffin"));
  }

  @Test
  void twoLoadsInFlightReserveTheirRoomInsteadOfBothSeeingItFree() throws Exception {
    // Two 100-byte loads can hold room at once; the third must wait for a reservation to finish.
    // Each load says how large its file is before reading it, which is what a caller that reads a
    // file can do and what keeps the reservation proportional to the file without a ceiling.
    ExecutorIndexCache.Config enabled = config(250);
    CountDownLatch allStarted = new CountDownLatch(2);
    CountDownLatch mayFinish = new CountDownLatch(1);
    ExecutorService pool = Executors.newFixedThreadPool(3);
    List<Future<?>> futures = new java.util.ArrayList<>();
    try {
      for (String name : ImmutableList.of("a", "b", "c")) {
        futures.add(
            pool.submit(
                () -> {
                  cache.file(
                      "s3://bucket/" + name + ".puffin",
                      enabled,
                      "adapter",
                      100L,
                      () -> {
                        allStarted.countDown();
                        try {
                          mayFinish.await(30, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                          Thread.currentThread().interrupt();
                        }
                        return ExecutorIndexCache.loaded(new byte[100], EMPTY, 100);
                      });
                  return null;
                }));
      }

      assertThat(allStarted.await(30, TimeUnit.SECONDS)).isTrue();
      mayFinish.countDown();
      for (Future<?> future : futures) {
        future.get(30, TimeUnit.SECONDS);
      }
    } finally {
      mayFinish.countDown();
      pool.shutdown();
      assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
    }

    // whatever the interleaving, the cache never reported holding more than it was given
    assertThat(cache.stats().bytes()).isLessThanOrEqualTo(250);
  }

  @Test
  void aTableBeyondItsShareIsTheOneThatLosesRoom() {
    AtomicInteger loads = new AtomicInteger();
    ExecutorIndexCache.Config measuring = ownerConfig("small", Long.MAX_VALUE);
    fairAsk("s3://bucket/small-a.puffin", measuring, loads, null);
    for (String suffix : List.of("a", "b", "c")) {
      fairAsk("s3://bucket/large-" + suffix + ".puffin", measuring.withOwner("large"), loads, null);
    }
    long[] peak = {0};
    fairAsk("s3://bucket/large-d.puffin", measuring.withOwner("large"), loads, peak);
    long budget = peak[0];
    cache.clear();
    ExecutorIndexCache.Config small = ownerConfig("small", budget);
    ExecutorIndexCache.Config large = ownerConfig("large", budget);

    fairAsk("s3://bucket/small-a.puffin", small, loads, null);
    fairAsk("s3://bucket/large-a.puffin", large, loads, null);
    fairAsk("s3://bucket/large-b.puffin", large, loads, null);
    fairAsk("s3://bucket/large-c.puffin", large, loads, null);
    fairAsk("s3://bucket/large-d.puffin", large, loads, null);

    // The fixture covers four residents and the fifth load's necessary metadata. Large borrows
    // most of that collection and must give back its own oldest file, not the small table's file.
    assertThat(ExecutorIndexCache.get().residentKeys())
        .anyMatch(key -> key.endsWith("small-a.puffin"));
    assertThat(ExecutorIndexCache.get().residentKeys())
        .noneMatch(key -> key.endsWith("large-a.puffin"));
  }

  @Test
  void aTableBorrowsRoomAnotherIsNotUsing() {
    AtomicInteger loads = new AtomicInteger();
    ExecutorIndexCache.Config measuring = ownerConfig("large", Long.MAX_VALUE);
    fairAsk("s3://bucket/large-a.puffin", measuring, loads, null);
    fairAsk("s3://bucket/large-b.puffin", measuring, loads, null);
    fairAsk("s3://bucket/small-a.puffin", measuring.withOwner("small"), loads, null);
    long[] peak = {0};
    fairAsk("s3://bucket/small-b.puffin", measuring.withOwner("small"), loads, peak);
    long budget = peak[0];
    cache.clear();
    ExecutorIndexCache.Config small = ownerConfig("small", budget);
    ExecutorIndexCache.Config large = ownerConfig("large", budget);

    // With only one table holding anything, it may borrow the available component budget.
    fairAsk("s3://bucket/large-a.puffin", large, loads, null);
    fairAsk("s3://bucket/large-b.puffin", large, loads, null);

    assertThat(ExecutorIndexCache.get().residentKeys()).hasSize(2);

    // when the other table arrives it takes its share back from the borrower
    fairAsk("s3://bucket/small-a.puffin", small, loads, null);
    fairAsk("s3://bucket/small-b.puffin", small, loads, null);

    assertThat(ExecutorIndexCache.get().residentKeys())
        .anyMatch(key -> key.endsWith("small-a.puffin"));
    assertThat(ExecutorIndexCache.get().residentKeys())
        .anyMatch(key -> key.endsWith("small-b.puffin"));
  }

  @Test
  void readsAgainWhenDisabled() {
    AtomicInteger loads = new AtomicInteger();

    for (int i = 0; i < 3; i++) {
      cache.file(
          "s3://bucket/a.puffin",
          ExecutorIndexCache.Config.disabled(),
          "adapter",
          () -> load(loads, 10));
    }

    assertThat(loads).hasValue(3);
    assertThat(cache.stats().entries()).isZero();
  }

  @Test
  void preparesOneFileOnceWhenTwoThreadsAskAtTheSameTime() throws Exception {
    AtomicInteger loads = new AtomicInteger();
    ExecutorIndexCache.Config enabled = config(1024 * 1024);
    CountDownLatch start = new CountDownLatch(1);
    ExecutorService threads = Executors.newFixedThreadPool(4);
    try {
      List<Future<ExecutorIndexCache.CachedFile>> asked = new java.util.ArrayList<>();
      for (int i = 0; i < 4; i++) {
        asked.add(
            threads.submit(
                () -> {
                  try {
                    start.await(30, TimeUnit.SECONDS);
                  } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                  }
                  return cache.file(
                      "s3://bucket/a.puffin",
                      enabled,
                      "adapter",
                      () -> {
                        // slow enough that the other threads are already waiting
                        try {
                          Thread.sleep(200);
                        } catch (InterruptedException e) {
                          throw new IllegalStateException(e);
                        }
                        return load(loads, 10);
                      });
                }));
      }

      start.countDown();
      ExecutorIndexCache.CachedFile first = asked.get(0).get(30, TimeUnit.SECONDS);
      for (Future<ExecutorIndexCache.CachedFile> future : asked) {
        assertThat(future.get(30, TimeUnit.SECONDS)).isSameAs(first);
      }
    } finally {
      threads.shutdownNow();
    }

    assertThat(loads).hasValue(1);
  }

  @Test
  void searchesOneFileFromOneThreadAtATime() throws Exception {
    ExecutorIndexCache.Config enabled = config(1024 * 1024);
    ExecutorIndexCache.CachedFile file =
        cache.file("s3://bucket/a.puffin", enabled, "adapter", () -> load(new AtomicInteger(), 10));

    AtomicInteger inside = new AtomicInteger();
    AtomicInteger overlapped = new AtomicInteger();
    ExecutorService threads = Executors.newFixedThreadPool(4);
    try {
      CountDownLatch start = new CountDownLatch(1);
      List<Future<?>> searches = new java.util.ArrayList<>();
      for (int i = 0; i < 8; i++) {
        searches.add(
            threads.submit(
                () -> {
                  try {
                    start.await(30, TimeUnit.SECONDS);
                  } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                  }
                  file.searching(
                      prepared -> {
                        if (inside.incrementAndGet() > 1) {
                          overlapped.incrementAndGet();
                        }
                        try {
                          Thread.sleep(20);
                        } catch (InterruptedException e) {
                          throw new IllegalStateException(e);
                        }
                        inside.decrementAndGet();
                        return null;
                      });
                  return null;
                }));
      }

      start.countDown();
      for (Future<?> search : searches) {
        search.get(30, TimeUnit.SECONDS);
      }
    } finally {
      threads.shutdownNow();
    }

    assertThat(overlapped).hasValue(0);
  }

  private static ExecutorIndexCache.Config config(long maxBytes) {
    return ExecutorIndexCache.Config.of(true, maxBytes);
  }

  /**
   * The key shape {@code DistributedIndexSearch.cacheKey} builds for a managed index file.
   *
   * <p>The adapter segment carries the blob type and the reader key, so the key has four segments
   * and the file location is the last of them. A key with the location as the third segment would
   * hide a parser that stops before the adapter's own separator.
   */
  private static String cacheKey(String owner, String adapter, String location) {
    return owner + "|" + adapter + "|" + adapter + ":reader|" + location;
  }

  /** A PIN_HOT cache admits a file on its second ask, so asking twice is what plants an entry. */
  private void ask(String location, ExecutorIndexCache.Config config, AtomicInteger loads) {
    cache.file(location, config, "adapter", () -> load(loads, 100));
    cache.file(location, config, "adapter", () -> load(loads, 100));
  }

  private void fairAsk(
      String location, ExecutorIndexCache.Config config, AtomicInteger loads, long[] metadataPeak) {
    for (int i = 0; i < 2; i++) {
      cache.file(
          location,
          config,
          "adapter",
          () -> {
            loads.incrementAndGet();
            if (metadataPeak != null) {
              metadataPeak[0] = Math.max(metadataPeak[0], cache.chargeSnapshot().usedBytes());
            }
            byte[] state = new byte[10_000 - 32];
            IndexAdapter.PreparedSearch prepared =
                new IndexAdapter.PreparedSearch() {
                  @Override
                  public List<IndexSearchResult> search(ByteBuffer query, int topK) {
                    return List.of();
                  }

                  @Override
                  public long estimatedBytes() {
                    return state.length + 32L;
                  }
                };
            return ExecutorIndexCache.loaded(0, prepared);
          });
    }
  }

  private static ExecutorIndexCache.Config ownerConfig(String owner, long maxBytes) {
    return ExecutorIndexCache.Config.of(true, maxBytes, ExecutorIndexCache.Mode.PIN_HOT, 0)
        .withOwner(owner);
  }

  private static ExecutorIndexCache.Config owned(String owner, long maxBytes) {
    return ExecutorIndexCache.Config.from(
        org.apache.spark.sql.SparkSession.builder().getOrCreate(), 0, owner);
  }

  private static ExecutorIndexCache.Config mode(ExecutorIndexCache.Mode mode, long maxBytes) {
    return ExecutorIndexCache.Config.of(true, maxBytes, mode, 0);
  }

  private static ExecutorIndexCache.CachedFile load(AtomicInteger loads, int bytes) {
    loads.incrementAndGet();
    IndexAdapter.PreparedSearch prepared =
        new IndexAdapter.PreparedSearch() {
          @Override
          public List<IndexSearchResult> search(ByteBuffer query, int topK) {
            return List.of();
          }

          @Override
          public long estimatedBytes() {
            return Math.max(OPAQUE_SEARCHER_BYTES, bytes);
          }
        };
    return ExecutorIndexCache.loaded(new byte[bytes], prepared, bytes);
  }
}
