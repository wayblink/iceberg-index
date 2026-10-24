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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iceberg.index.IndexAdapter;
import org.apache.iceberg.index.IndexSearchResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestExecutorIndexCacheReservations {
  private static final IndexAdapter.PreparedSearch EMPTY = (query, topK) -> List.of();
  private final ExecutorIndexCache cache = ExecutorIndexCache.get();

  @AfterEach
  void clearCache() {
    cache.clear();
  }

  @Test
  void usesPreparedEstimateInsteadOfPayloadMultiplier() {
    IndexAdapter.PreparedSearch prepared =
        new IndexAdapter.PreparedSearch() {
          @Override
          public List<IndexSearchResult> search(ByteBuffer query, int topK) {
            return List.of();
          }

          @Override
          public long estimatedBytes() {
            return 70;
          }
        };
    // Check the returned entry and actual resident accounting.
    ExecutorIndexCache.CachedFile loaded = ExecutorIndexCache.loaded(new byte[10], prepared);
    cache.file("a", ExecutorIndexCache.Config.of(true, 1000, 1000), "adapter", () -> loaded);
    assertThat(loaded.bytes()).isEqualTo(80);
    assertThat(cache.stats().bytes()).isEqualTo(80);
  }

  @Test
  void waitingLoadDoesNotReleaseAnotherLoadsHeldRoom() throws Exception {
    ExecutorService pool = Executors.newFixedThreadPool(2);
    CountDownLatch firstStarted = new CountDownLatch(1);
    CountDownLatch secondStarted = new CountDownLatch(1);
    CountDownLatch finishFirst = new CountDownLatch(1);
    ExecutorIndexCache.Config config = ExecutorIndexCache.Config.of(true, 250, 250);
    try {
      Future<?> first =
          pool.submit(
              () ->
                  cache.file(
                      "first",
                      config.withOwner("A"),
                      "adapter",
                      () -> {
                        firstStarted.countDown();
                        await(finishFirst);
                        return ExecutorIndexCache.loaded(new byte[100], EMPTY, 100);
                      }));
      assertThat(firstStarted.await(10, TimeUnit.SECONDS)).isTrue();
      Future<?> second =
          pool.submit(
              () ->
                  cache.file(
                      "second",
                      config.withOwner("B"),
                      "adapter",
                      () -> {
                        secondStarted.countDown();
                        return ExecutorIndexCache.loaded(new byte[100], EMPTY, 100);
                      }));
      assertThat(secondStarted.await(200, TimeUnit.MILLISECONDS)).isFalse();
      assertThat(reserved()).as("A still holds its own reservation").isEqualTo(250);
      finishFirst.countDown();
      first.get(10, TimeUnit.SECONDS);
      second.get(10, TimeUnit.SECONDS);
      assertThat(reserved()).isZero();
      assertThat(cache.stats().bytes()).isEqualTo(200);
    } finally {
      finishFirst.countDown();
      pool.shutdownNow();
      assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    }
  }

  @Test
  void reservationMustConstrainLoadsNotOnlyFinalResidentBytes() throws Exception {
    ExecutorService pool = Executors.newFixedThreadPool(3);
    CountDownLatch allStarted = new CountDownLatch(3);
    CountDownLatch finish = new CountDownLatch(1);
    AtomicLong allocated = new AtomicLong();
    ExecutorIndexCache.Config config = ExecutorIndexCache.Config.of(true, 250, 100);
    List<Future<?>> futures = new ArrayList<>();
    try {
      for (int i = 0; i < 3; i++) {
        String location = "file-" + i;
        String owner = i == 0 ? "A" : "B";
        futures.add(
            pool.submit(
                () ->
                    cache.file(
                        location,
                        config.withOwner(owner),
                        "adapter",
                        () -> {
                          ExecutorIndexCache.CachedFile loaded =
                              ExecutorIndexCache.loaded(new byte[100], EMPTY, 100);
                          allocated.addAndGet(loaded.bytes());
                          allStarted.countDown();
                          await(finish);
                          return loaded;
                        })));
      }
      allStarted.await(1, TimeUnit.SECONDS);
      // Existing test checks only stats after put/evict: it hides simultaneous live allocations.
      assertThat(allocated.get() + cache.stats().bytes())
          .as("live loads + resident bytes")
          .isLessThanOrEqualTo(250);
    } finally {
      finish.countDown();
      for (Future<?> future : futures) {
        future.get(10, TimeUnit.SECONDS);
      }
      pool.shutdownNow();
      assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    }
  }

  @Test
  void partialReservationIsReleasedAfterLoaderFailure() throws Exception {
    ExecutorIndexCache.Config config = ExecutorIndexCache.Config.of(true, 250, 250);
    cache.file(
        "resident",
        config.withOwner("A"),
        "adapter",
        () -> ExecutorIndexCache.loaded(new byte[100], EMPTY, 100));
    assertThatThrownBy(
            () ->
                cache.file(
                    "failed",
                    config.withOwner("B"),
                    "adapter",
                    () -> {
                      try {
                        assertThat(reserved()).isEqualTo(150);
                      } catch (Exception e) {
                        throw new IllegalStateException(e);
                      }
                      throw new IllegalArgumentException("Cannot prepare index");
                    }))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot prepare index");
    assertThat(reserved()).isZero();
    assertThat(cache.stats().bytes()).isEqualTo(100);
    cache.file(
        "next",
        config.withOwner("B"),
        "adapter",
        () -> ExecutorIndexCache.loaded(new byte[100], EMPTY, 100));
    assertThat(cache.stats().bytes()).isEqualTo(200);
    assertThat(reserved()).isZero();
  }

  @Test
  void oversizedEntryCannotSpendAnotherLoadsReservation() throws Exception {
    ExecutorService pool = Executors.newFixedThreadPool(2);
    CountDownLatch bothStarted = new CountDownLatch(2);
    CountDownLatch finishLarge = new CountDownLatch(1);
    CountDownLatch finishOther = new CountDownLatch(1);
    ExecutorIndexCache.Config config = ExecutorIndexCache.Config.of(true, 250, 100);
    try {
      Future<?> large =
          pool.submit(
              () ->
                  cache.file(
                      "large",
                      config.withOwner("A"),
                      "adapter",
                      () -> {
                        bothStarted.countDown();
                        await(finishLarge);
                        return ExecutorIndexCache.loaded(new byte[150], EMPTY, 150);
                      }));
      Future<?> other =
          pool.submit(
              () ->
                  cache.file(
                      "other",
                      config.withOwner("B"),
                      "adapter",
                      () -> {
                        bothStarted.countDown();
                        await(finishOther);
                        return ExecutorIndexCache.loaded(new byte[100], EMPTY, 100);
                      }));
      assertThat(bothStarted.await(10, TimeUnit.SECONDS)).isTrue();
      finishLarge.countDown();
      large.get(10, TimeUnit.SECONDS);
      assertThat(reserved()).isEqualTo(100);
      assertThat(cache.stats().entries()).isZero();
      assertThat(cache.stats().skippedTooLarge()).isEqualTo(1);
      finishOther.countDown();
      other.get(10, TimeUnit.SECONDS);
      assertThat(reserved()).isZero();
      assertThat(cache.stats().bytes()).isEqualTo(100);
    } finally {
      finishLarge.countDown();
      finishOther.countDown();
      pool.shutdownNow();
      assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    }
  }

  @Test
  void reservationTransfersToResidentWithoutSpendingOtherPromises() throws Exception {
    ExecutorService pool = Executors.newFixedThreadPool(2);
    CountDownLatch bothStarted = new CountDownLatch(2);
    CountDownLatch finishFirst = new CountDownLatch(1);
    CountDownLatch finishSecond = new CountDownLatch(1);
    ExecutorIndexCache.Config config = ExecutorIndexCache.Config.of(true, 250, 100);
    try {
      Future<?> first =
          pool.submit(
              () ->
                  cache.file(
                      "first",
                      config.withOwner("A"),
                      "adapter",
                      () -> {
                        bothStarted.countDown();
                        await(finishFirst);
                        return ExecutorIndexCache.loaded(new byte[80], EMPTY, 80);
                      }));
      Future<?> second =
          pool.submit(
              () ->
                  cache.file(
                      "second",
                      config.withOwner("B"),
                      "adapter",
                      () -> {
                        bothStarted.countDown();
                        await(finishSecond);
                        return ExecutorIndexCache.loaded(new byte[100], EMPTY, 100);
                      }));
      assertThat(bothStarted.await(10, TimeUnit.SECONDS)).isTrue();
      finishFirst.countDown();
      first.get(10, TimeUnit.SECONDS);
      assertThat(cache.stats().bytes()).isEqualTo(80);
      assertThat(reserved()).isEqualTo(100);
      assertThat(cache.stats().bytes() + reserved()).isLessThanOrEqualTo(250);
      finishSecond.countDown();
      second.get(10, TimeUnit.SECONDS);
      assertThat(reserved()).isZero();
      assertThat(cache.stats().bytes()).isEqualTo(180);
    } finally {
      finishFirst.countDown();
      finishSecond.countDown();
      pool.shutdownNow();
      assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    }
  }

  @Test
  void admissionCountsMeasuredIncomingBytesForFairEviction() {
    ExecutorIndexCache.Config a =
        ExecutorIndexCache.Config.of(true, 100, 100, ExecutorIndexCache.Mode.PIN_HOT, 0)
            .withOwner("A");
    ExecutorIndexCache.Config b = a.withOwner("B");
    askTwice("a-old", a, 40);
    askTwice("b", b, 60);
    askTwice("a-new", a, 40);
    assertThat(cache.residentKeys())
        .contains("B|adapter|b", "A|adapter|a-new")
        .doesNotContain("A|adapter|a-old");
    assertThat(cache.stats().bytes()).isEqualTo(100);
  }

  @Test
  void clearRemovesResidentOwnerAccounting() throws Exception {
    cache.file(
        "old",
        ExecutorIndexCache.Config.of(true, 1000, 1000).withOwner("old-table"),
        "adapter",
        () -> ExecutorIndexCache.loaded(new byte[1000], EMPTY, 1000));
    cache.clear();
    Field ownerBytes = ExecutorIndexCache.class.getDeclaredField("bytesByOwner");
    ownerBytes.setAccessible(true);
    Field ownerEntries = ExecutorIndexCache.class.getDeclaredField("entriesByOwner");
    ownerEntries.setAccessible(true);
    assertThat((Map<?, ?>) ownerBytes.get(cache)).isEmpty();
    assertThat((Map<?, ?>) ownerEntries.get(cache)).isEmpty();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void releasedResidentRoomWakesReservationWaiter(boolean clear) throws Exception {
    ExecutorIndexCache.Config config = ExecutorIndexCache.Config.of(true, 140, 40);
    ExecutorIndexCache.CachedFile resident =
        cache.file(
            "resident",
            config,
            "adapter",
            () -> ExecutorIndexCache.loaded(new byte[40], EMPTY, 40));
    cache.file(
        "fresh", config, "adapter", () -> ExecutorIndexCache.loaded(new byte[40], EMPTY, 40));
    ExecutorService pool = Executors.newFixedThreadPool(2);
    CountDownLatch firstStarted = new CountDownLatch(1);
    CountDownLatch finishFirst = new CountDownLatch(1);
    CountDownLatch secondStarted = new CountDownLatch(1);
    AtomicReference<Thread> waiter = new AtomicReference<>();
    Future<?> first = null;
    Future<?> second = null;
    try {
      first =
          pool.submit(
              () ->
                  cache.file(
                      "in-flight",
                      config,
                      "adapter",
                      () -> {
                        firstStarted.countDown();
                        await(finishFirst);
                        return ExecutorIndexCache.loaded(new byte[40], EMPTY, 40);
                      }));
      assertThat(firstStarted.await(10, TimeUnit.SECONDS)).isTrue();
      second =
          pool.submit(
              () -> {
                waiter.set(Thread.currentThread());
                return cache.file(
                    "waiting",
                    config,
                    "adapter",
                    () -> {
                      secondStarted.countDown();
                      return ExecutorIndexCache.loaded(new byte[40], EMPTY, 40);
                    });
              });
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while ((waiter.get() == null || waiter.get().getState() != Thread.State.WAITING)
          && System.nanoTime() < deadline) {
        Thread.yield();
      }
      assertThat(waiter.get().getState()).isEqualTo(Thread.State.WAITING);
      assertThat(reserved()).isEqualTo(40);
      if (clear) {
        cache.clear();
      } else {
        Field lastAccess = resident.getClass().getDeclaredField("lastAccessNanos");
        lastAccess.setAccessible(true);
        lastAccess.setLong(resident, System.nanoTime() - TimeUnit.MINUTES.toNanos(31));
        ExecutorIndexCache.sweepIntervalForTests(0);
        cache.file(
            "fresh",
            config,
            "adapter",
            () -> {
              throw new AssertionError("Fresh entry must remain a cache hit");
            });
      }
      assertThat(cache.residentKeys()).doesNotContain("|adapter|resident");
      assertThat(secondStarted.await(2, TimeUnit.SECONDS))
          .as("60 bytes are now free, enough to grant the waiting 40-byte request")
          .isTrue();
      second.get(10, TimeUnit.SECONDS);
      assertThat(reserved()).as("The unfinished first load still owns its token").isEqualTo(40);
      assertThat(cache.stats().bytes() + reserved()).isLessThanOrEqualTo(140);
    } finally {
      ExecutorIndexCache.sweepIntervalForTests(TimeUnit.SECONDS.toNanos(30));
      finishFirst.countDown();
      try {
        if (first != null) {
          first.get(10, TimeUnit.SECONDS);
        }
        if (second != null) {
          second.get(10, TimeUnit.SECONDS);
        }
      } finally {
        pool.shutdownNow();
        assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
      }
    }
  }

  @Test
  void interruptedWaitKeepsOtherToken() throws Exception {
    ExecutorIndexCache.Config config = ExecutorIndexCache.Config.of(true, 100, 100);
    ExecutorService pool = Executors.newFixedThreadPool(2);
    CountDownLatch firstStarted = new CountDownLatch(1);
    CountDownLatch finishFirst = new CountDownLatch(1);
    AtomicReference<Thread> waiter = new AtomicReference<>();
    Future<?> first = null;
    try {
      first =
          pool.submit(
              () ->
                  cache.file(
                      "held",
                      config,
                      "adapter",
                      () -> {
                        firstStarted.countDown();
                        await(finishFirst);
                        return ExecutorIndexCache.loaded(new byte[40], EMPTY, 40);
                      }));
      assertThat(firstStarted.await(10, TimeUnit.SECONDS)).isTrue();
      Future<?> second =
          pool.submit(
              () -> {
                waiter.set(Thread.currentThread());
                assertThatThrownBy(
                        () ->
                            cache.file(
                                "interrupted",
                                config,
                                "adapter",
                                () -> {
                                  throw new AssertionError(
                                      "Interrupted waiter must not start its loader");
                                }))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("Interrupted while reserving index cache room");
                assertThat(Thread.currentThread().isInterrupted()).isTrue();
              });
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while ((waiter.get() == null || waiter.get().getState() != Thread.State.WAITING)
          && System.nanoTime() < deadline) {
        Thread.yield();
      }
      assertThat(waiter.get().getState()).isEqualTo(Thread.State.WAITING);
      waiter.get().interrupt();
      second.get(10, TimeUnit.SECONDS);
      assertThat(reserved()).isEqualTo(100);
      finishFirst.countDown();
      first.get(10, TimeUnit.SECONDS);
      assertThat(reserved()).isZero();
    } finally {
      finishFirst.countDown();
      try {
        if (first != null) {
          first.get(10, TimeUnit.SECONDS);
        }
      } finally {
        pool.shutdownNow();
        assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
      }
    }
  }

  private void askTwice(String key, ExecutorIndexCache.Config config, int bytes) {
    for (int i = 0; i < 2; i++) {
      cache.file(
          key, config, "adapter", () -> ExecutorIndexCache.loaded(new byte[bytes], EMPTY, bytes));
    }
  }

  private long reserved() throws Exception {
    Field field = ExecutorIndexCache.class.getDeclaredField("reservedBytes");
    field.setAccessible(true);
    return field.getLong(cache);
  }

  private static void await(CountDownLatch latch) {
    try {
      if (!latch.await(10, TimeUnit.SECONDS)) {
        throw new IllegalStateException("Latch timed out");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }
}
