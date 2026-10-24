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

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.index.IndexAdapter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class TestExecutorIndexCache {

  private static final IndexAdapter.PreparedSearch EMPTY = (query, topK) -> ImmutableList.of();

  private final ExecutorIndexCache cache = ExecutorIndexCache.get();

  @AfterEach
  void clearCache() {
    cache.clear();
    ExecutorIndexCache.sweepIntervalForTests(java.util.concurrent.TimeUnit.SECONDS.toNanos(30));
  }

  @Test
  void loadsEachFileOnce() {
    AtomicInteger loads = new AtomicInteger();
    ExecutorIndexCache.Config enabled = config(1024 * 1024, 1024 * 1024);

    for (int i = 0; i < 3; i++) {
      ExecutorIndexCache.CachedFile file =
          cache.file("s3://bucket/index-a.puffin", enabled, "adapter", () -> load(loads, 100));
      assertThat(file.payload()).hasSize(100);
    }

    assertThat(loads).hasValue(1);
    assertThat(cache.stats().hits()).isEqualTo(2);
    assertThat(cache.stats().loads()).isEqualTo(1);
  }

  @Test
  void keepsFilesApartByLocationAndAdapter() {
    AtomicInteger loads = new AtomicInteger();
    ExecutorIndexCache.Config enabled = config(1024 * 1024, 1024 * 1024);

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
    ExecutorIndexCache.Config enabled = mode(ExecutorIndexCache.Mode.PIN_HOT, 250, 250);

    cache.file("s3://bucket/a.puffin", enabled, "adapter", () -> load(loads, 100));
    cache.file("s3://bucket/b.puffin", enabled, "adapter", () -> load(loads, 100));
    // each is asked twice, so both are admitted; c then needs room
    cache.file("s3://bucket/a.puffin", enabled, "adapter", () -> load(loads, 100));
    cache.file("s3://bucket/b.puffin", enabled, "adapter", () -> load(loads, 100));
    cache.file("s3://bucket/c.puffin", enabled, "adapter", () -> load(loads, 100));
    cache.file("s3://bucket/c.puffin", enabled, "adapter", () -> load(loads, 100));

    assertThat(cache.stats().evictions()).isEqualTo(1);
    assertThat(cache.stats().bytes()).isLessThanOrEqualTo(250);
    assertThat(cache.stats().entries()).isEqualTo(2);
  }

  @Test
  void doesNotCacheAFileOverTheEntryLimit() {
    AtomicInteger loads = new AtomicInteger();
    ExecutorIndexCache.Config enabled = config(1024 * 1024, 50);

    cache.file("s3://bucket/big.puffin", enabled, "adapter", () -> load(loads, 100));
    cache.file("s3://bucket/big.puffin", enabled, "adapter", () -> load(loads, 100));

    assertThat(loads).hasValue(2);
    assertThat(cache.stats().skippedTooLarge()).isEqualTo(2);
    assertThat(cache.stats().entries()).isZero();
  }

  @Test
  void pinHotAdmitsAFileOnlyOnceItIsAskedForTwice() {
    AtomicInteger loads = new AtomicInteger();
    ExecutorIndexCache.Config pinHot = mode(ExecutorIndexCache.Mode.PIN_HOT, 1 << 20, 1 << 20);

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
    ExecutorIndexCache.Config stream = mode(ExecutorIndexCache.Mode.STREAM, 1 << 20, 1 << 20);

    for (int i = 0; i < 3; i++) {
      cache.file("s3://bucket/a.puffin", stream, "adapter", () -> load(loads, 10));
    }

    assertThat(loads).hasValue(3);
    assertThat(cache.stats().entries()).isZero();
  }

  @Test
  void aFileAskedForTwiceOutranksOneSeenOnce() {
    AtomicInteger loads = new AtomicInteger();
    ExecutorIndexCache.Config pinHot = mode(ExecutorIndexCache.Mode.PIN_HOT, 250, 250);

    // a is admitted after its second ask and promoted on its third, so when c needs room the entry
    // seen once is the one that goes
    cache.file("s3://bucket/a.puffin", pinHot, "adapter", () -> load(loads, 100));
    cache.file("s3://bucket/a.puffin", pinHot, "adapter", () -> load(loads, 100));
    cache.file("s3://bucket/a.puffin", pinHot, "adapter", () -> load(loads, 100));
    cache.file("s3://bucket/b.puffin", pinHot, "adapter", () -> load(loads, 100));
    cache.file("s3://bucket/b.puffin", pinHot, "adapter", () -> load(loads, 100));
    cache.file("s3://bucket/c.puffin", pinHot, "adapter", () -> load(loads, 100));
    cache.file("s3://bucket/c.puffin", pinHot, "adapter", () -> load(loads, 100));
    cache.file("s3://bucket/c.puffin", pinHot, "adapter", () -> load(loads, 100));

    assertThat(ExecutorIndexCache.get().residentKeys()).anyMatch(key -> key.endsWith("a.puffin"));
  }

  @Test
  void pinAllDoesNotEvictToMakeRoom() {
    AtomicInteger loads = new AtomicInteger();
    ExecutorIndexCache.Config pinAll = mode(ExecutorIndexCache.Mode.PIN_ALL, 250, 250);

    cache.file("s3://bucket/a.puffin", pinAll, "adapter", () -> load(loads, 100));
    cache.file("s3://bucket/b.puffin", pinAll, "adapter", () -> load(loads, 100));
    // the budget cannot hold a third file, and a pin-all entry is not evicted for one
    cache.file("s3://bucket/c.puffin", pinAll, "adapter", () -> load(loads, 100));

    assertThat(ExecutorIndexCache.get().residentKeys()).noneMatch(key -> key.endsWith("c.puffin"));
  }

  @Test
  void unloadsEntriesThatNoQueryAskedFor() throws Exception {
    AtomicInteger loads = new AtomicInteger();
    ExecutorIndexCache.Config ttl =
        ExecutorIndexCache.Config.of(true, 1 << 20, 1 << 20, ExecutorIndexCache.Mode.PIN_ALL, 1);
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
    ExecutorIndexCache.Config enabled = config(250, 100);
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
    ExecutorIndexCache.Config small = ownerConfig("small", 400, 400);
    ExecutorIndexCache.Config large = ownerConfig("large", 400, 400);

    // small keeps one entry of 100; large fills the rest and then needs more
    ask("s3://bucket/small-a.puffin", small, loads);
    ask("s3://bucket/large-a.puffin", large, loads);
    ask("s3://bucket/large-b.puffin", large, loads);
    ask("s3://bucket/large-c.puffin", large, loads);
    ask("s3://bucket/large-d.puffin", large, loads);

    // 400 bytes over two tables is 200 each, so large has to give up its own oldest entry rather
    // than push small out
    assertThat(ExecutorIndexCache.get().residentKeys())
        .anyMatch(key -> key.endsWith("small-a.puffin"));
    assertThat(ExecutorIndexCache.get().residentKeys())
        .noneMatch(key -> key.endsWith("large-a.puffin"));
  }

  @Test
  void aTableBorrowsRoomAnotherIsNotUsing() {
    AtomicInteger loads = new AtomicInteger();
    ExecutorIndexCache.Config small = ownerConfig("small", 300, 300);
    ExecutorIndexCache.Config large = ownerConfig("large", 300, 300);

    // with only one table holding anything, it may use the whole budget
    ask("s3://bucket/large-a.puffin", large, loads);
    ask("s3://bucket/large-b.puffin", large, loads);

    assertThat(ExecutorIndexCache.get().residentKeys()).hasSize(2);

    // when the other table arrives it takes its share back from the borrower
    ask("s3://bucket/small-a.puffin", small, loads);
    ask("s3://bucket/small-b.puffin", small, loads);

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
    ExecutorIndexCache.Config enabled = config(1024 * 1024, 1024 * 1024);
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
    ExecutorIndexCache.Config enabled = config(1024 * 1024, 1024 * 1024);
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

  private static ExecutorIndexCache.Config config(long maxBytes, long maxEntryBytes) {
    return ExecutorIndexCache.Config.of(true, maxBytes, maxEntryBytes);
  }

  /** A PIN_HOT cache admits a file on its second ask, so asking twice is what plants an entry. */
  private void ask(String location, ExecutorIndexCache.Config config, AtomicInteger loads) {
    cache.file(location, config, "adapter", () -> load(loads, 100));
    cache.file(location, config, "adapter", () -> load(loads, 100));
  }

  private static ExecutorIndexCache.Config ownerConfig(
      String owner, long maxBytes, long maxEntryBytes) {
    return ExecutorIndexCache.Config.of(
            true, maxBytes, maxEntryBytes, ExecutorIndexCache.Mode.PIN_HOT, 0)
        .withOwner(owner);
  }

  private static ExecutorIndexCache.Config owned(String owner, long maxBytes, long maxEntryBytes) {
    return ExecutorIndexCache.Config.from(
        org.apache.spark.sql.SparkSession.builder().getOrCreate(), 0, owner);
  }

  private static ExecutorIndexCache.Config mode(
      ExecutorIndexCache.Mode mode, long maxBytes, long maxEntryBytes) {
    return ExecutorIndexCache.Config.of(true, maxBytes, maxEntryBytes, mode, 0);
  }

  private static ExecutorIndexCache.CachedFile load(AtomicInteger loads, int bytes) {
    loads.incrementAndGet();
    return ExecutorIndexCache.loaded(new byte[bytes], EMPTY, bytes);
  }
}
