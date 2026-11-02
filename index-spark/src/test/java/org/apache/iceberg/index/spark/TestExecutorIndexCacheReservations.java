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
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
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
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

class TestExecutorIndexCacheReservations {
  private static final IndexAdapter.PreparedSearch EMPTY = (query, topK) -> List.of();
  private static final long UNLIMITED = Long.MAX_VALUE;
  private static final long MAX_SCENARIO_BYTES = 1L << 22;

  private final ExecutorIndexCache cache = ExecutorIndexCache.get();

  @AfterEach
  void clearCache() {
    cache.clear();
    ExecutorIndexCache.sweepIntervalForTests(TimeUnit.SECONDS.toNanos(30));
  }

  /** A state of the cache, built from the same owners and entries the test exercises. */
  @FunctionalInterface
  private interface Fixture {
    void build(ExecutorIndexCache.Config config);
  }

  /**
   * What a scenario costs, measured rather than derived: the least capacity that keeps the entries
   * it publishes, and the charge it holds at rest.
   */
  private record Measured(long capacity, long resident) {}

  private static ExecutorIndexCache.Config unlimited() {
    return ExecutorIndexCache.Config.of(true, UNLIMITED);
  }

  private static ExecutorIndexCache.CachedFile entry(int bytes) {
    return ExecutorIndexCache.loaded(new byte[bytes], EMPTY, bytes);
  }

  private static ExecutorIndexCache.CachedFile managedEntry() {
    return ExecutorIndexCache.loaded(new byte[10], EMPTY);
  }

  private static ExecutorIndexCache.Config withMode(
      ExecutorIndexCache.Config config, ExecutorIndexCache.Mode mode, long idleTtlMillis) {
    return ExecutorIndexCache.Config.of(true, config.maxBytes(), mode, idleTtlMillis);
  }

  /** The charge the cache publishes for a fixture whose budget refuses nothing. */
  private ExecutorIndexCache.ChargeSnapshot atRest(Fixture fixture) {
    cache.clear();
    try {
      fixture.build(unlimited());
      return cache.chargeSnapshot();
    } finally {
      cache.clear();
    }
  }

  /**
   * The room a load holds before the entry it loads exists: the monitor that tracks it, the promise
   * it keeps and the names it owns. Measured for the owner and location a scenario uses, because
   * those names are what this bookkeeping costs.
   */
  private long inFlightRoom(String owner, String location, int bytes) {
    long[] during = {0};
    cache.clear();
    try {
      cache.file(
          location,
          unlimited().withOwner(owner),
          "adapter",
          (long) bytes,
          () -> {
            during[0] = cache.chargeSnapshot().usedBytes();
            return entry(bytes);
          });
      return during[0];
    } finally {
      cache.clear();
    }
  }

  /**
   * Measures a fixture: the charge it holds at rest, and the least capacity that keeps everything
   * it publishes.
   *
   * <p>Reading the charge at rest is not enough on its own. Publishing an entry transiently holds
   * the room that load reserved and the bookkeeping that tracks it on top of the entries already
   * kept, so a budget equal to the resident charge can be too small to ever reach that state. The
   * capacity is therefore measured by replaying the same owners and entries and asking the cache,
   * which keeps both numbers independent of the formulas that produce them.
   */
  private Measured measure(Fixture fixture, String... retained) {
    Set<String> keys = Set.of(retained);
    long atRest = atRest(fixture).usedBytes();
    long lower = atRest;
    long upper = atRest;
    while (!retains(fixture, upper, keys)) {
      upper = upper <= 0 ? 1 : Math.multiplyExact(upper, 2);
      if (upper > MAX_SCENARIO_BYTES) {
        throw new IllegalStateException(
            "The fixture never kept " + keys + " within " + MAX_SCENARIO_BYTES + " bytes");
      }
    }

    while (lower < upper) {
      long middle = lower + (upper - lower) / 2;
      if (retains(fixture, middle, keys)) {
        upper = middle;
      } else {
        lower = middle + 1;
      }
    }

    return new Measured(lower, atRest(fixture).residentBytes());
  }

  private boolean retains(Fixture fixture, long capacity, Set<String> retained) {
    cache.clear();
    try {
      fixture.build(ExecutorIndexCache.Config.of(true, capacity));
      return cache.residentKeys().containsAll(retained)
          && cache.chargeSnapshot().usedBytes() <= capacity;
    } finally {
      cache.clear();
    }
  }

  @Test
  void usesPreparedEstimateInsteadOfPayloadMultiplier() {
    // the same payload under two prepared estimates: a payload multiplier would charge them alike
    long smaller = residentOf(prepared(70));
    long larger = residentOf(prepared(700));
    assertThat(larger)
        .as("the prepared estimate decides the charge, not the %s byte payload", 10)
        .isGreaterThan(smaller);
    assertThat(larger - smaller).isEqualTo(700 - 70);
  }

  private long residentOf(IndexAdapter.PreparedSearch prepared) {
    return atRest(
            config ->
                cache.file(
                    "a",
                    config,
                    "adapter",
                    () -> ExecutorIndexCache.loaded(new byte[10], prepared)))
        .residentBytes();
  }

  @Test
  void waitingLoadDoesNotReleaseAnotherLoadsHeldRoom() throws Exception {
    int bytes = 100;
    Measured measured =
        measure(
            config -> {
              cache.file("first", config.withOwner("A"), "adapter", bytes, () -> entry(bytes));
              cache.file("second", config.withOwner("B"), "adapter", bytes, () -> entry(bytes));
            },
            "A|adapter|first",
            "B|adapter|second");
    // The waiter needs room for its own bookkeeping while the first load keeps the room for its
    // entry; without it the waiter's monitor cannot be published and the file is read uncached.
    ExecutorIndexCache.Config config =
        ExecutorIndexCache.Config.of(
            true, measured.capacity() + inFlightRoom("B", "second", bytes));
    ExecutorService pool = Executors.newFixedThreadPool(2);
    CountDownLatch firstStarted = new CountDownLatch(1);
    CountDownLatch secondStarted = new CountDownLatch(1);
    CountDownLatch finishFirst = new CountDownLatch(1);
    AtomicReference<Thread> waiter = new AtomicReference<>();
    try {
      Future<?> first =
          pool.submit(
              () ->
                  cache.file(
                      "first",
                      config.withOwner("A"),
                      "adapter",
                      bytes,
                      () -> {
                        firstStarted.countDown();
                        await(finishFirst);
                        return entry(bytes);
                      }));
      assertThat(firstStarted.await(10, TimeUnit.SECONDS)).isTrue();
      Future<?> second =
          pool.submit(
              () -> {
                waiter.set(Thread.currentThread());
                return cache.file(
                    "second",
                    config.withOwner("B"),
                    "adapter",
                    () -> {
                      secondStarted.countDown();
                      return entry(bytes);
                    });
              });
      assertThat(secondStarted.await(200, TimeUnit.MILLISECONDS))
          .as("the second load must wait instead of starting its loader")
          .isFalse();
      awaitState(waiter, Thread.State.WAITING);
      ExecutorIndexCache.ChargeSnapshot held = cache.chargeSnapshot();
      assertThat(held.reservedBytes())
          .as("A still holds the room for its own load")
          .isEqualTo(bytes);
      assertThat(held.usedBytes() + held.reservedBytes())
          .as("the waiter is not admitted by spending A's promise")
          .isLessThanOrEqualTo(config.maxBytes());
      finishFirst.countDown();
      first.get(10, TimeUnit.SECONDS);
      second.get(10, TimeUnit.SECONDS);
      assertThat(reserved()).isZero();
      assertThat(cache.stats().bytes()).isEqualTo(measured.resident());
    } finally {
      finishFirst.countDown();
      pool.shutdownNow();
      assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    }
  }

  @Test
  void reservationMustConstrainLoadsNotOnlyFinalResidentBytes() throws Exception {
    Measured measured =
        measure(
            config -> {
              cache.file("file-0", config.withOwner("A"), "adapter", () -> entry(100));
              cache.file("file-1", config.withOwner("B"), "adapter", () -> entry(100));
              cache.file("file-2", config.withOwner("B"), "adapter", () -> entry(100));
            },
            "A|adapter|file-0",
            "B|adapter|file-1",
            "B|adapter|file-2");
    ExecutorIndexCache.Config config = ExecutorIndexCache.Config.of(true, measured.capacity());
    ExecutorService pool = Executors.newFixedThreadPool(3);
    CountDownLatch inFlight = new CountDownLatch(1);
    CountDownLatch finish = new CountDownLatch(1);
    AtomicLong allocated = new AtomicLong();
    AtomicReference<Thread> racerOne = new AtomicReference<>();
    AtomicReference<Thread> racerTwo = new AtomicReference<>();
    List<Future<ExecutorIndexCache.CachedFile>> futures = new ArrayList<>();
    try {
      for (int i = 0; i < 3; i++) {
        int position = i;
        String location = "file-" + i;
        String owner = i == 0 ? "A" : "B";
        futures.add(
            pool.submit(
                () -> {
                  if (position == 1) {
                    racerOne.set(Thread.currentThread());
                  } else if (position == 2) {
                    racerTwo.set(Thread.currentThread());
                  }
                  return cache.file(
                      location,
                      config.withOwner(owner),
                      "adapter",
                      () -> {
                        ExecutorIndexCache.CachedFile loaded = entry(100);
                        allocated.addAndGet(loaded.bytes());
                        inFlight.countDown();
                        await(finish);
                        return loaded;
                      });
                }));
      }
      assertThat(inFlight.await(10, TimeUnit.SECONDS)).isTrue();
      // A promise is not a resident entry. Whether a load waits for room or runs its loader
      // uncached is scheduling; what must hold is the charge the cache reports while loads overlap.
      awaitStalled(racerOne);
      awaitStalled(racerTwo);
      for (int sample = 0; sample < 100; sample++) {
        ExecutorIndexCache.ChargeSnapshot held = cache.chargeSnapshot();
        assertThat(held.reservedBytes()).as("promised room is never negative").isNotNegative();
        assertThat(held.usedBytes() + held.reservedBytes())
            .as("live loads and promised room stay within the budget")
            .isLessThanOrEqualTo(measured.capacity());
        assertThat(cache.residentKeys().size())
            .as("a load still holding room has published at most two entries")
            .isLessThanOrEqualTo(2);
        Thread.yield();
      }
      finish.countDown();
      for (Future<ExecutorIndexCache.CachedFile> future : futures) {
        assertThat(future.get(10, TimeUnit.SECONDS)).isNotNull();
      }
      assertThat(allocated.get()).as("every loader runs to completion").isEqualTo(300);
      assertThat(reserved()).isZero();
      assertThat(cache.chargeSnapshot().usedBytes()).isLessThanOrEqualTo(measured.capacity());
      assertThat(cache.stats().bytes()).isLessThanOrEqualTo(measured.resident());
    } finally {
      finish.countDown();
      pool.shutdownNow();
      assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    }
  }

  @Test
  void partialReservationIsReleasedAfterLoaderFailure() throws Exception {
    int bytes = 100;
    Measured alone =
        measure(
            config ->
                cache.file("resident", config.withOwner("A"), "adapter", bytes, () -> entry(bytes)),
            "A|adapter|resident");
    Measured together =
        measure(
            config -> {
              cache.file("resident", config.withOwner("A"), "adapter", bytes, () -> entry(bytes));
              cache.file("next", config.withOwner("B"), "adapter", bytes, () -> entry(bytes));
            },
            "A|adapter|resident",
            "B|adapter|next");
    // Room for the failing load's own promise, so the reservation it has to release is not zero.
    ExecutorIndexCache.Config config =
        ExecutorIndexCache.Config.of(
            true, together.capacity() + inFlightRoom("B", "failed", bytes));
    cache.file("resident", config.withOwner("A"), "adapter", bytes, () -> entry(bytes));
    assertThat(cache.stats().bytes()).isEqualTo(alone.resident());
    AtomicLong promised = new AtomicLong(-1);
    assertThatThrownBy(
            () ->
                cache.file(
                    "failed",
                    config.withOwner("B"),
                    "adapter",
                    bytes,
                    () -> {
                      // the load announces its length, so it promises the room for its own file
                      try {
                        promised.set(reserved());
                      } catch (Exception e) {
                        throw new IllegalStateException(e);
                      }
                      throw new IllegalArgumentException("Cannot prepare index");
                    }))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot prepare index");
    assertThat(promised.get()).as("the failed load held room for its file").isEqualTo(bytes);
    assertThat(reserved()).as("a failed load releases the room it promised").isZero();
    assertThat(cache.stats().bytes()).isEqualTo(alone.resident());
    cache.file("next", config.withOwner("B"), "adapter", bytes, () -> entry(bytes));
    assertThat(cache.stats().bytes()).isEqualTo(together.resident());
    assertThat(reserved()).isZero();
  }

  @Test
  void aLoadCannotSpendAnotherLoadsReservation() throws Exception {
    Measured measured =
        measure(
            config -> {
              cache.file("large", config.withOwner("A"), "adapter", 150L, () -> entry(150));
              cache.file("other", config.withOwner("B"), "adapter", 100L, () -> entry(100));
            },
            "A|adapter|large",
            "B|adapter|other");
    // The second load keeps its own room while the first one publishes its entry.
    ExecutorIndexCache.Config config =
        ExecutorIndexCache.Config.of(true, measured.capacity() + inFlightRoom("B", "other", 100));
    ExecutorService pool = Executors.newFixedThreadPool(2);
    CountDownLatch bothStarted = new CountDownLatch(2);
    CountDownLatch finishLarge = new CountDownLatch(1);
    CountDownLatch finishOther = new CountDownLatch(1);
    try {
      Future<?> large =
          pool.submit(
              () ->
                  cache.file(
                      "large",
                      config.withOwner("A"),
                      "adapter",
                      150L,
                      () -> {
                        bothStarted.countDown();
                        await(finishLarge);
                        return entry(150);
                      }));
      Future<?> other =
          pool.submit(
              () ->
                  cache.file(
                      "other",
                      config.withOwner("B"),
                      "adapter",
                      100L,
                      () -> {
                        bothStarted.countDown();
                        await(finishOther);
                        return entry(100);
                      }));
      assertThat(bothStarted.await(10, TimeUnit.SECONDS)).isTrue();
      finishLarge.countDown();
      large.get(10, TimeUnit.SECONDS);
      assertThat(reserved()).as("the second load's promise is untouched").isEqualTo(100);
      finishOther.countDown();
      other.get(10, TimeUnit.SECONDS);
      assertThat(reserved()).isZero();
      assertThat(cache.stats().bytes()).isEqualTo(measured.resident());
    } finally {
      finishLarge.countDown();
      finishOther.countDown();
      pool.shutdownNow();
      assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    }
  }

  @Test
  void reservationTransfersToResidentWithoutSpendingOtherPromises() throws Exception {
    Measured first =
        measure(
            config -> cache.file("first", config.withOwner("A"), "adapter", 80L, () -> entry(80)),
            "A|adapter|first");
    Measured both =
        measure(
            config -> {
              cache.file("first", config.withOwner("A"), "adapter", 80L, () -> entry(80));
              cache.file("second", config.withOwner("B"), "adapter", 100L, () -> entry(100));
            },
            "A|adapter|first",
            "B|adapter|second");
    long secondRoom = inFlightRoom("B", "second", 100);
    ExecutorIndexCache.Config config =
        ExecutorIndexCache.Config.of(true, both.capacity() + secondRoom);
    ExecutorService pool = Executors.newFixedThreadPool(2);
    CountDownLatch bothStarted = new CountDownLatch(2);
    CountDownLatch finishFirst = new CountDownLatch(1);
    CountDownLatch finishSecond = new CountDownLatch(1);
    try {
      Future<?> firstLoad =
          pool.submit(
              () ->
                  cache.file(
                      "first",
                      config.withOwner("A"),
                      "adapter",
                      80L,
                      () -> {
                        bothStarted.countDown();
                        await(finishFirst);
                        return entry(80);
                      }));
      Future<?> second =
          pool.submit(
              () ->
                  cache.file(
                      "second",
                      config.withOwner("B"),
                      "adapter",
                      100L,
                      () -> {
                        bothStarted.countDown();
                        await(finishSecond);
                        return entry(100);
                      }));
      assertThat(bothStarted.await(10, TimeUnit.SECONDS)).isTrue();
      finishFirst.countDown();
      firstLoad.get(10, TimeUnit.SECONDS);
      // the first load publishes while the second still holds the room for its own entry
      assertThat(cache.residentKeys()).containsExactly("A|adapter|first");
      assertThat(cache.stats().bytes())
          .as("the first entry is charged next to the room the second load holds, not evicted")
          .isBetween(first.resident(), first.resident() + secondRoom);
      assertThat(reserved()).isEqualTo(100);
      assertThat(cache.stats().bytes() + reserved()).isLessThanOrEqualTo(config.maxBytes());
      finishSecond.countDown();
      second.get(10, TimeUnit.SECONDS);
      assertThat(reserved()).isZero();
      assertThat(cache.stats().bytes()).isEqualTo(both.resident());
    } finally {
      finishFirst.countDown();
      finishSecond.countDown();
      pool.shutdownNow();
      assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    }
  }

  @Test
  void admissionCountsMeasuredIncomingBytesForFairEviction() {
    // The fixture keeps the entries the admission of a-new has to keep, and asks for the entry it
    // has to evict once: the bookkeeping of that name is part of what admission has to fit, the
    // entry is not. Measuring the whole three entry fixture would also budget for a-old staying.
    Fixture fixture =
        config -> {
          askTwice(
              "a-old", withMode(config, ExecutorIndexCache.Mode.PIN_HOT, 0).withOwner("A"), 40);
          askTwice("b", withMode(config, ExecutorIndexCache.Mode.PIN_HOT, 0).withOwner("B"), 60);
          askTwice(
              "a-new", withMode(config, ExecutorIndexCache.Mode.PIN_HOT, 0).withOwner("A"), 40);
        };
    Measured target =
        measure(
            config -> {
              askTwice(
                  "b", withMode(config, ExecutorIndexCache.Mode.PIN_HOT, 0).withOwner("B"), 60);
              askTwice(
                  "a-new", withMode(config, ExecutorIndexCache.Mode.PIN_HOT, 0).withOwner("A"), 40);
            },
            "B|adapter|b",
            "A|adapter|a-new");
    // Enough room for the retained pair, but not for all three: the new entry has to make
    // admission reclaim the one its table no longer needs.
    long all = atRest(fixture).usedBytes();
    long capacity = Math.subtractExact(all, 24L);
    ExecutorIndexCache.Config a =
        withMode(ExecutorIndexCache.Config.of(true, capacity), ExecutorIndexCache.Mode.PIN_HOT, 0)
            .withOwner("A");
    ExecutorIndexCache.Config b = a.withOwner("B");
    askTwice("a-old", a, 40);
    askTwice("b", b, 60);
    askTwice("a-new", a, 40);
    assertThat(cache.stats().bytes()).isLessThan(all);
    assertThat(cache.stats().bytes()).isLessThanOrEqualTo(capacity);
  }

  @Test
  void clearRemovesResidentOwnerAccounting() throws Exception {
    Measured measured =
        measure(
            config ->
                cache.file("old", config.withOwner("old-table"), "adapter", () -> entry(1000)),
            "old-table|adapter|old");
    cache.file(
        "old",
        ExecutorIndexCache.Config.of(true, measured.capacity()).withOwner("old-table"),
        "adapter",
        () -> entry(1000));
    assertThat(cache.residentKeys()).containsExactly("old-table|adapter|old");
    assertThat(cache.stats().bytes()).isEqualTo(measured.resident());
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
    Measured measured =
        measure(
            config -> {
              cache.file("resident", config, "adapter", () -> entry(40));
              cache.file("fresh", config, "adapter", () -> entry(40));
            },
            "|adapter|resident",
            "|adapter|fresh");
    ExecutorIndexCache.Config config = ExecutorIndexCache.Config.of(true, measured.capacity());
    ExecutorIndexCache.CachedFile resident =
        cache.file("resident", config, "adapter", () -> entry(40));
    cache.file("fresh", config, "adapter", () -> entry(40));
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
                        return entry(40);
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
                      return entry(40);
                    });
              });
      awaitState(waiter, Thread.State.WAITING);
      long token = reserved();
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
          .as("the room the released entry held is enough to grant the waiting request")
          .isTrue();
      second.get(10, TimeUnit.SECONDS);
      assertThat(reserved()).as("The unfinished first load still owns its token").isEqualTo(token);
      assertThat(cache.chargeSnapshot().usedBytes() + reserved())
          .isLessThanOrEqualTo(measured.capacity());
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
    int bytes = 40;
    Measured measured =
        measure(
            config -> cache.file("held", config, "adapter", bytes, () -> entry(bytes)),
            "|adapter|held");
    ExecutorIndexCache.Config config =
        ExecutorIndexCache.Config.of(
            true, measured.capacity() + inFlightRoom("", "interrupted", bytes));
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
                      bytes,
                      () -> {
                        firstStarted.countDown();
                        await(finishFirst);
                        return entry(bytes);
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
      awaitState(waiter, Thread.State.WAITING);
      long token = reserved();
      assertThat(token)
          .as("the load in flight owns the room the waiter cannot use")
          .isEqualTo(bytes);
      waiter.get().interrupt();
      second.get(10, TimeUnit.SECONDS);
      assertThat(reserved()).isEqualTo(token);
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

  @Test
  void oldLoadCannotPublishAfterOffload() throws Exception {
    String key = "table|adapter|managed";
    Measured measured =
        measure(
            config -> {
              cache.applyTarget("A", 1, Set.of(key));
              cache.installManaged(
                  "A",
                  1,
                  key,
                  config.withOwner("table"),
                  TestExecutorIndexCacheReservations::managedEntry);
            },
            key);
    ExecutorIndexCache.Config config =
        ExecutorIndexCache.Config.of(true, measured.capacity()).withOwner("table");
    cache.applyTarget("A", 1, Set.of(key));
    ExecutorService pool = Executors.newSingleThreadExecutor();
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch finish = new CountDownLatch(1);
    try {
      Future<ExecutorIndexCache.CachedFile> load =
          pool.submit(
              () ->
                  cache.installManaged(
                      "A",
                      1,
                      key,
                      config,
                      () -> {
                        started.countDown();
                        await(finish);
                        return managedEntry();
                      }));
      assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
      cache.releaseIndex("A", 2);
      finish.countDown();
      assertThat(load.get(10, TimeUnit.SECONDS)).isNull();
      assertThat(cache.stats().bytes()).isZero();
      assertThat(cache.stats().entries()).isZero();
      assertThat(reserved()).isZero();
    } finally {
      finish.countDown();
      pool.shutdownNow();
      assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    }
  }

  @Test
  void sharedFileKeepsOtherIndexOwner() {
    String key = "table|adapter|managed";
    Measured measured =
        measure(
            config -> {
              cache.applyTarget("A", 1, Set.of(key));
              cache.installManaged(
                  "A",
                  1,
                  key,
                  config.withOwner("table"),
                  TestExecutorIndexCacheReservations::managedEntry);
              cache.applyTarget("B", 1, Set.of(key));
            },
            key);
    ExecutorIndexCache.Config config =
        ExecutorIndexCache.Config.of(true, measured.capacity()).withOwner("table");
    ExecutorIndexCache.CachedFile file = install("A", 1, key, config);
    cache.applyTarget("B", 1, Set.of(key));
    assertThat(
            cache.installManaged(
                "B",
                1,
                key,
                config,
                () -> {
                  throw new AssertionError("Shared file must be reused");
                }))
        .isSameAs(file);
    cache.releaseIndex("A", 2);
    assertThat(cache.stats().bytes()).isEqualTo(measured.resident());
    try (ExecutorIndexCache.Lease lease = cache.acquireManaged("B", 1, key)) {
      assertThat(lease).isNotNull();
      cache.releaseIndex("B", 2);
      assertThat(cache.stats().bytes()).isEqualTo(measured.resident());
    }
    assertThat(cache.stats().bytes()).isZero();
  }

  @Test
  void releaseWaitsForSearch() throws Exception {
    String key = "table|adapter|managed";
    Measured measured =
        measure(
            config -> {
              cache.applyTarget("A", 1, Set.of(key));
              cache.installManaged(
                  "A",
                  1,
                  key,
                  config.withOwner("table"),
                  TestExecutorIndexCacheReservations::managedEntry);
            },
            key);
    ExecutorIndexCache.Config config =
        ExecutorIndexCache.Config.of(true, measured.capacity()).withOwner("table");
    install("A", 1, key, config);
    ExecutorService pool = Executors.newSingleThreadExecutor();
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch finish = new CountDownLatch(1);
    try {
      Future<?> search =
          pool.submit(
              () -> {
                try (ExecutorIndexCache.Lease lease = cache.acquireManaged("A", 1, key)) {
                  assertThat(lease).isNotNull();
                  lease
                      .file()
                      .searching(
                          prepared -> {
                            started.countDown();
                            await(finish);
                            return List.of();
                          });
                }
              });
      assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
      cache.releaseIndex("A", 2);
      assertThat(cache.stats().bytes()).isEqualTo(measured.resident());
      finish.countDown();
      search.get(10, TimeUnit.SECONDS);
      assertThat(cache.stats().bytes()).isZero();
    } finally {
      finish.countDown();
      pool.shutdownNow();
      assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    }
  }

  @Test
  void leaseBeforeSearchingBlocksRelease() {
    String key = "table|adapter|managed";
    Measured measured =
        measure(
            config -> {
              cache.applyTarget("A", 1, Set.of(key));
              cache.installManaged(
                  "A",
                  1,
                  key,
                  config.withOwner("table"),
                  TestExecutorIndexCacheReservations::managedEntry);
            },
            key);
    ExecutorIndexCache.Config config =
        ExecutorIndexCache.Config.of(true, measured.capacity()).withOwner("table");
    install("A", 1, key, config);
    try (ExecutorIndexCache.Lease lease = cache.acquireManaged("A", 1, key)) {
      assertThat(lease).isNotNull();
      cache.releaseIndex("A", 2);
      assertThat(cache.acquireManaged("A", 1, key)).isNull();
      assertThat(cache.stats().bytes()).isEqualTo(measured.resident());
      List<IndexSearchResult> results =
          lease.file().searching(prepared -> prepared.search(ByteBuffer.allocate(0), 1));
      assertThat(results).isEmpty();
    }
    assertThat(cache.stats().bytes()).isZero();
  }

  @Test
  void oldLeaseCloseCannotReleaseNewOwnerAndCloseIsIdempotent() {
    String key = "table|adapter|managed";
    // The charge the test holds once epoch 3 owns the key: the target and the installed entry.
    // The open lease keeps the file alive across the release but owns no bytes of its own.
    Measured measured =
        measure(
            config -> {
              cache.applyTarget("A", 1, Set.of(key));
              cache.installManaged(
                  "A",
                  1,
                  key,
                  config.withOwner("table"),
                  TestExecutorIndexCacheReservations::managedEntry);
              cache.applyTarget("A", 3, Set.of(key));
            },
            key);
    ExecutorIndexCache.Config config =
        ExecutorIndexCache.Config.of(true, measured.capacity()).withOwner("table");
    ExecutorIndexCache.CachedFile file = install("A", 1, key, config);
    ExecutorIndexCache.Lease lease = cache.acquireManaged("A", 1, key);
    try {
      assertThat(lease).isNotNull();
      cache.releaseIndex("A", 2);
      cache.applyTarget("A", 3, Set.of(key));
      assertThat(
              cache.installManaged(
                  "A",
                  3,
                  key,
                  config,
                  () -> {
                    throw new AssertionError("Leased file must be reused");
                  }))
          .isSameAs(file);
      lease.close();
      lease.close();
      assertThat(cache.stats().bytes()).isEqualTo(measured.resident());
      cache.releaseIndex("A", 4);
      assertThat(cache.stats().bytes()).isZero();
    } finally {
      if (lease != null) {
        lease.close();
      }
    }
  }

  @Test
  void targetReplacementDefersOnlyOldUniqueFile() {
    String old = "table|adapter|old";
    String common = "table|adapter|common";
    String next = "table|adapter|next";
    long held =
        atRest(
                config -> {
                  cache.applyTarget("A", 1, Set.of(old, common));
                  cache.installManaged(
                      "A", 1, old, config.withOwner("table"), () -> managedEntry());
                  cache.installManaged(
                      "A", 1, common, config.withOwner("table"), () -> managedEntry());
                })
            .residentBytes();
    Measured measured =
        measure(
            config -> {
              cache.applyTarget("A", 1, Set.of(old, common));
              cache.installManaged("A", 1, old, config.withOwner("table"), () -> managedEntry());
              cache.installManaged("A", 1, common, config.withOwner("table"), () -> managedEntry());
              cache.applyTarget("A", 2, Set.of(common, next));
            },
            common);
    ExecutorIndexCache.Config config =
        ExecutorIndexCache.Config.of(true, measured.capacity()).withOwner("table");
    cache.applyTarget("A", 1, Set.of(old, common));
    cache.installManaged("A", 1, old, config, () -> managedEntry());
    cache.installManaged("A", 1, common, config, () -> managedEntry());
    try (ExecutorIndexCache.Lease lease = cache.acquireManaged("A", 1, old)) {
      assertThat(lease).isNotNull();
      cache.applyTarget("A", 2, Set.of(common, next));
      assertThat(cache.stats().bytes()).isEqualTo(held);
      assertThat(cache.acquireManaged("A", 1, common)).isNull();
    }
    assertThat(cache.stats().bytes()).isEqualTo(measured.resident());
    assertThat(cache.residentKeys()).containsExactly(common);
  }

  @Test
  void managedKeyCannotBeFilledByLegacyLazyQuery() {
    String key = "table|adapter|managed";
    long capacity = atRest(config -> cache.applyTarget("A", 1, Set.of(key))).usedBytes();
    ExecutorIndexCache.Config config =
        ExecutorIndexCache.Config.of(true, capacity).withOwner("table");
    cache.applyTarget("A", 1, Set.of(key));
    assertThat(cache.file("managed", config, "adapter", () -> managedEntry())).isNotNull();
    assertThat(cache.stats().entries()).isZero();
    cache.releaseIndex("A", 2);
    cache.file("managed", config, "adapter", () -> managedEntry());
    assertThat(cache.stats().entries()).isZero();
  }

  @Test
  void managedEntryIsNotAnIdleOrFairShareVictim() throws Exception {
    String key = "A|adapter|managed";
    Measured measured =
        measure(
            config -> {
              cache.applyTarget("A", 1, Set.of(key));
              cache.installManaged("A", 1, key, config.withOwner("A"), () -> managedEntry());
              askTwice(
                  "other", withMode(config, ExecutorIndexCache.Mode.PIN_HOT, 1).withOwner("B"), 20);
            },
            key,
            "B|adapter|other");
    ExecutorIndexCache.Config config = ExecutorIndexCache.Config.of(true, measured.capacity());
    cache.applyTarget("A", 1, Set.of(key));
    ExecutorIndexCache.CachedFile file =
        cache.installManaged("A", 1, key, config.withOwner("A"), () -> managedEntry());
    assertThat(file).isNotNull();
    Field lastAccess = file.getClass().getDeclaredField("lastAccessNanos");
    lastAccess.setAccessible(true);
    lastAccess.setLong(file, System.nanoTime() - TimeUnit.MINUTES.toNanos(31));
    try {
      ExecutorIndexCache.Config b =
          withMode(config, ExecutorIndexCache.Mode.PIN_HOT, 1).withOwner("B");
      ExecutorIndexCache.sweepIntervalForTests(0);
      askTwice("other", b, 20);
      askTwice("new", b, 20);
      assertThat(cache.residentKeys()).contains(key);
      assertThat(cache.stats().bytes()).isLessThanOrEqualTo(measured.capacity());
    } finally {
      ExecutorIndexCache.sweepIntervalForTests(TimeUnit.SECONDS.toNanos(30));
    }
  }

  @Test
  void failedManagedLoadReleasesItsReservation() throws Exception {
    String key = "table|adapter|managed";
    Measured measured =
        measure(
            config -> {
              cache.applyTarget("A", 1, Set.of(key));
              cache.installManaged("A", 1, key, config.withOwner("table"), () -> managedEntry());
            },
            key);
    ExecutorIndexCache.Config config =
        ExecutorIndexCache.Config.of(true, measured.capacity()).withOwner("table");
    cache.applyTarget("A", 1, Set.of(key));
    assertThatThrownBy(
            () ->
                cache.installManaged(
                    "A",
                    1,
                    key,
                    config,
                    () -> {
                      throw new IllegalArgumentException("Cannot prepare index");
                    }))
        .hasMessage("Cannot prepare index");
    assertThat(reserved()).isZero();
    ExecutorIndexCache.CachedFile installed =
        cache.installManaged("A", 1, key, config, () -> managedEntry());
    assertThat(installed).isNotNull();
    assertThat(reserved()).isZero();
    assertThat(cache.stats().bytes()).isEqualTo(measured.resident());
  }

  @Test
  void disabledManagedLoadDoesNotRunLoader() {
    String key = "table|adapter|managed";
    cache.applyTarget("A", 1, Set.of(key));
    assertThat(
            cache.installManaged(
                "A",
                1,
                key,
                ExecutorIndexCache.Config.disabled(),
                () -> {
                  throw new AssertionError("Disabled cache must not load");
                }))
        .isNull();
  }

  @ParameterizedTest
  @EnumSource(ExecutorIndexCache.Mode.class)
  void explicitInstallationDoesNotInheritLazyAdmissionMode(ExecutorIndexCache.Mode mode) {
    String key = "table|adapter|managed";
    // An explicit installation admits the file on its first ask whatever the mode is; the lazy
    // path is the only one that waits for a second ask. The budget is measured, the mode is the
    // variable under test.
    Measured measured =
        measure(
            config -> {
              cache.applyTarget("A", 1, Set.of(key));
              cache.installManaged(
                  "A", 1, key, withMode(config, mode, 0).withOwner("table"), () -> managedEntry());
            },
            key);
    cache.applyTarget("A", 1, Set.of(key));
    ExecutorIndexCache.CachedFile installed =
        cache.installManaged(
            "A",
            1,
            key,
            withMode(ExecutorIndexCache.Config.of(true, measured.capacity()), mode, 0)
                .withOwner("table"),
            () -> managedEntry());
    assertThat(installed).isNotNull();
    assertThat(cache.residentKeys()).containsExactly(key);
    assertThat(cache.stats().bytes()).isEqualTo(measured.resident());
  }

  @Test
  void closedLeaseDoesNotRetainFile() throws Exception {
    String key = "table|adapter|managed";
    Measured measured =
        measure(
            config -> {
              cache.applyTarget("A", 1, Set.of(key));
              cache.installManaged("A", 1, key, config.withOwner("table"), () -> managedEntry());
            },
            key);
    install(
        "A", 1, key, ExecutorIndexCache.Config.of(true, measured.capacity()).withOwner("table"));
    ExecutorIndexCache.Lease lease = cache.acquireManaged("A", 1, key);
    assertThat(lease).isNotNull();
    lease.close();
    assertThatThrownBy(lease::file).hasMessage("Index file lease is closed");
    Field retained = lease.getClass().getDeclaredField("file");
    retained.setAccessible(true);
    assertThat(retained.get(lease)).isNull();
  }

  @Test
  void clearKeepsLeasedBytesUntilClose() {
    String key = "table|adapter|managed";
    Measured measured =
        measure(
            config -> {
              cache.applyTarget("A", 1, Set.of(key));
              cache.installManaged("A", 1, key, config.withOwner("table"), () -> managedEntry());
            },
            key);
    ExecutorIndexCache.Config config =
        ExecutorIndexCache.Config.of(true, measured.capacity()).withOwner("table");
    install("A", 1, key, config);
    try (ExecutorIndexCache.Lease lease = cache.acquireManaged("A", 1, key)) {
      assertThat(lease).isNotNull();
      cache.clear();
      assertThat(cache.stats().bytes()).isEqualTo(measured.resident());
    }
    assertThat(cache.stats().bytes()).isZero();
  }

  @Test
  void reservationWaiterIsCancelledByOffload() throws Exception {
    String a = "table|adapter|a";
    String b = "table|adapter|b";
    Measured measured =
        measure(
            config -> {
              cache.applyTarget("A", 1, Set.of(a));
              cache.applyTarget("B", 1, Set.of(b));
              cache.installManaged("A", 1, a, config.withOwner("table"), () -> managedEntry());
            },
            a);
    ExecutorIndexCache.Config config =
        ExecutorIndexCache.Config.of(true, measured.capacity()).withOwner("table");
    cache.applyTarget("A", 1, Set.of(a));
    cache.applyTarget("B", 1, Set.of(b));
    ExecutorService pool = Executors.newFixedThreadPool(2);
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch finish = new CountDownLatch(1);
    AtomicReference<Thread> waiter = new AtomicReference<>();
    try {
      Future<?> first =
          pool.submit(
              () ->
                  cache.installManaged(
                      "A",
                      1,
                      a,
                      config,
                      () -> {
                        started.countDown();
                        await(finish);
                        return managedEntry();
                      }));
      assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
      Future<ExecutorIndexCache.CachedFile> second =
          pool.submit(
              () -> {
                waiter.set(Thread.currentThread());
                return cache.installManaged(
                    "B",
                    1,
                    b,
                    config,
                    () -> {
                      throw new AssertionError("Offloaded waiter must not load");
                    });
              });
      awaitState(waiter, Thread.State.WAITING);
      long token = reserved();
      cache.releaseIndex("B", 2);
      assertThat(second.get(10, TimeUnit.SECONDS)).isNull();
      assertThat(reserved()).as("the offloaded waiter never got a token").isEqualTo(token);
      finish.countDown();
      first.get(10, TimeUnit.SECONDS);
      assertThat(reserved()).isZero();
    } finally {
      finish.countDown();
      pool.shutdownNow();
      assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    }
  }

  @Test
  void legacyLoadStartedBeforeManagementCannotPublish() throws Exception {
    String key = "table|adapter|managed";
    int bytes = 10;
    Measured measured =
        measure(
            config -> {
              cache.applyTarget("A", 1, Set.of(key));
              cache.installManaged("A", 1, key, config.withOwner("table"), () -> managedEntry());
            },
            key);
    // The legacy load holds the room for its entry while the controller publishes the target.
    ExecutorIndexCache.Config config =
        ExecutorIndexCache.Config.of(
                true, measured.capacity() + inFlightRoom("table", "managed", bytes))
            .withOwner("table");
    ExecutorService pool = Executors.newSingleThreadExecutor();
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch finish = new CountDownLatch(1);
    try {
      Future<?> load =
          pool.submit(
              () ->
                  cache.file(
                      "managed",
                      config,
                      "adapter",
                      bytes,
                      () -> {
                        started.countDown();
                        await(finish);
                        return managedEntry();
                      }));
      assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
      cache.applyTarget("A", 1, Set.of(key));
      assertThat(cache.managedKeys("A")).containsExactly(key);
      finish.countDown();
      load.get(10, TimeUnit.SECONDS);
      assertThat(cache.stats().bytes()).isZero();
      assertThat(reserved()).isZero();
    } finally {
      finish.countDown();
      pool.shutdownNow();
      assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    }
  }

  @Test
  void sameFileWaitersKeepOneMonitorAfterFailure() throws Exception {
    String key = "table|adapter|managed";
    Measured measured =
        measure(
            config -> {
              for (String index : List.of("A", "B", "C")) {
                cache.applyTarget(index, 1, Set.of(key));
              }
              cache.installManaged("B", 1, key, config.withOwner("table"), () -> managedEntry());
            },
            key);
    ExecutorIndexCache.Config config =
        ExecutorIndexCache.Config.of(true, measured.capacity()).withOwner("table");
    for (String index : List.of("A", "B", "C")) {
      cache.applyTarget(index, 1, Set.of(key));
    }
    ExecutorService pool = Executors.newFixedThreadPool(3);
    CountDownLatch firstStarted = new CountDownLatch(1);
    CountDownLatch failFirst = new CountDownLatch(1);
    CountDownLatch secondStarted = new CountDownLatch(1);
    CountDownLatch finishSecond = new CountDownLatch(1);
    AtomicReference<Thread> secondThread = new AtomicReference<>();
    AtomicReference<Thread> thirdThread = new AtomicReference<>();
    try {
      Future<?> first =
          pool.submit(
              () ->
                  cache.installManaged(
                      "A",
                      1,
                      key,
                      config,
                      () -> {
                        firstStarted.countDown();
                        await(failFirst);
                        throw new IllegalArgumentException("Cannot prepare index");
                      }));
      assertThat(firstStarted.await(10, TimeUnit.SECONDS)).isTrue();
      Future<ExecutorIndexCache.CachedFile> second =
          pool.submit(
              () -> {
                secondThread.set(Thread.currentThread());
                return cache.installManaged(
                    "B",
                    1,
                    key,
                    config,
                    () -> {
                      secondStarted.countDown();
                      await(finishSecond);
                      return managedEntry();
                    });
              });
      awaitState(secondThread, Thread.State.BLOCKED);
      failFirst.countDown();
      assertThatThrownBy(() -> first.get(10, TimeUnit.SECONDS))
          .isInstanceOf(ExecutionException.class)
          .hasCauseInstanceOf(IllegalArgumentException.class);
      assertThat(secondStarted.await(10, TimeUnit.SECONDS)).isTrue();
      Future<ExecutorIndexCache.CachedFile> third =
          pool.submit(
              () -> {
                thirdThread.set(Thread.currentThread());
                return cache.installManaged(
                    "C",
                    1,
                    key,
                    config,
                    () -> {
                      throw new AssertionError("Waiter must reuse shared file");
                    });
              });
      awaitState(thirdThread, Thread.State.BLOCKED);
      finishSecond.countDown();
      assertThat(third.get(10, TimeUnit.SECONDS)).isSameAs(second.get(10, TimeUnit.SECONDS));
      assertThat(cache.stats().bytes()).isEqualTo(measured.resident());
      assertThat(reserved()).isZero();
    } finally {
      failFirst.countDown();
      finishSecond.countDown();
      pool.shutdownNow();
      assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    }
  }

  @Test
  void legacyHitAfterControlCannotReturnManagedObject() throws Exception {
    String key = "table|adapter|managed";
    ExecutorIndexCache.Config config =
        ExecutorIndexCache.Config.of(true, Long.MAX_VALUE).withOwner("table");
    ExecutorIndexCache.CachedFile ephemeral = ExecutorIndexCache.loaded(new byte[10], EMPTY);
    Method acquireMonitor =
        ExecutorIndexCache.class.getDeclaredMethod("loadingMonitor", String.class, String.class);
    acquireMonitor.setAccessible(true);
    Object monitor = acquireMonitor.invoke(cache, key, "table");
    Method releaseMonitor =
        ExecutorIndexCache.class.getDeclaredMethod(
            "releaseLoadingMonitor", String.class, monitor.getClass());
    releaseMonitor.setAccessible(true);
    ExecutorService pool = Executors.newSingleThreadExecutor();
    AtomicReference<Thread> legacyThread = new AtomicReference<>();
    try {
      Future<ExecutorIndexCache.CachedFile> legacy;
      synchronized (monitor) {
        legacy =
            pool.submit(
                () -> {
                  legacyThread.set(Thread.currentThread());
                  return cache.file("managed", config, "adapter", () -> ephemeral);
                });
        awaitState(legacyThread, Thread.State.BLOCKED);
        install("A", 1, key, config);
      }
      assertThat(legacy.get(10, TimeUnit.SECONDS)).isSameAs(ephemeral);
      cache.releaseIndex("A", 2);
      assertThat(cache.residentKeys()).isEmpty();
    } finally {
      pool.shutdownNow();
      try {
        assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
      } finally {
        releaseMonitor.invoke(cache, key, monitor);
        assertThat(cache.stats().bytes()).isZero();
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"0,true", "-1,false"})
  void reuseRespectsTheNewBudgetWithoutReleasingSharedLease(long budgetDelta, boolean reusable)
      throws Exception {
    String key = "table|adapter|managed";
    ExecutorIndexCache.Config original =
        ExecutorIndexCache.Config.of(true, Long.MAX_VALUE).withOwner("table");
    install("A", 1, key, original);
    cache.applyTarget("B", 1, Set.of(key));
    try (ExecutorIndexCache.Lease lease = cache.acquireManaged("B", 1, key)) {
      assertThat(lease).isNotNull();
      cache.applyTarget("A", 2, Set.of(key));
      long resident = cache.chargeSnapshot().residentBytes();
      long full = cache.chargeSnapshot().usedBytes();
      ExecutorIndexCache.Config limited =
          ExecutorIndexCache.Config.of(true, full + budgetDelta).withOwner("table");
      ExecutorIndexCache.CachedFile reused =
          cache.installManaged(
              "A",
              2,
              key,
              limited,
              () -> {
                throw new AssertionError("Reuse must not reload");
              });
      // Reuse requires the complete R+Q charge; lowering that capacity by one byte must reject it.
      assertThat(reused != null).isEqualTo(reusable);
      cache.releaseIndex("A", 3);
      assertThat(cache.stats().bytes()).isEqualTo(resident);
      assertThat(lease.file().bytes()).isEqualTo(20);
      assertThat(reserved()).isZero();
    }
    cache.releaseIndex("B", 2);
    assertThat(cache.stats().bytes()).isZero();
  }

  @Test
  void reuseCannotAcknowledgeWhenOccupancyExceedsNewBudget() {
    String a = "table|adapter|a";
    String b = "table|adapter|b";
    ExecutorIndexCache.Config original =
        ExecutorIndexCache.Config.of(true, Long.MAX_VALUE).withOwner("table");
    install("A", 1, a, original);
    install("B", 1, b, original);
    cache.applyTarget("A", 2, Set.of(a));
    long resident = cache.chargeSnapshot().residentBytes();
    long limited = cache.chargeSnapshot().usedBytes() - 1;
    assertThat(
            cache.installManaged(
                "A",
                2,
                a,
                ExecutorIndexCache.Config.of(true, limited).withOwner("table"),
                () -> {
                  throw new AssertionError("Rejected reuse must not reload");
                }))
        .isNull();
    assertThat(cache.stats().bytes()).isEqualTo(resident);
    assertThat(cache.residentKeys()).containsExactlyInAnyOrder(a, b);
  }

  @Test
  void optionalAttributionPreservesLazySharedFileBehavior() {
    String key = "table|adapter|unmanaged";
    long budget = measuredAttributionBudget("table", "unmanaged", "table#A", "table#B");
    ExecutorIndexCache.Config config =
        ExecutorIndexCache.Config.of(true, budget).withOwner("table");
    ExecutorIndexCache.CachedFile file =
        cache.file(
            "unmanaged", config, "adapter", () -> ExecutorIndexCache.loaded(new byte[10], EMPTY));
    cache.recordIndexAttribution("table#A", Set.of(key));
    cache.recordIndexAttribution("table#B", Set.of(key));
    Map<String, Set<String>> snapshot = cache.indexAttribution();
    assertThat(snapshot.get(key)).containsExactlyInAnyOrder("table#A", "table#B");
    assertThatThrownBy(() -> snapshot.put("other", Set.of("table#C")))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> snapshot.get(key).clear())
        .isInstanceOf(UnsupportedOperationException.class);
    assertThat(
            cache.file(
                "unmanaged",
                config,
                "adapter",
                () -> {
                  throw new AssertionError("Attribution must not disable lazy hits");
                }))
        .isSameAs(file);
    assertThat(cache.acquireManaged("table#A", 1, key)).isNull();
    assertThat(cache.stats().bytes()).isEqualTo(cache.chargeSnapshot().residentBytes());
  }

  @Test
  void attributionDoesNotRenewAccessOrProtectIdleFile() throws Exception {
    String key = "table|adapter|unmanaged";
    long budget = measuredAttributionBudget("table", "unmanaged", "table#A");
    ExecutorIndexCache.Config config =
        ExecutorIndexCache.Config.of(
                true, budget, ExecutorIndexCache.Mode.PIN_ALL, TimeUnit.MINUTES.toMillis(30))
            .withOwner("table");
    ExecutorIndexCache.CachedFile file =
        cache.file(
            "unmanaged", config, "adapter", () -> ExecutorIndexCache.loaded(new byte[10], EMPTY));
    long residentBeforeAssociation = cache.stats().bytes();
    Field lastAccess = file.getClass().getDeclaredField("lastAccessNanos");
    lastAccess.setAccessible(true);
    long expired = System.nanoTime() - TimeUnit.MINUTES.toNanos(31);
    lastAccess.setLong(file, expired);
    long hitsBefore = cache.stats().hits();
    cache.recordIndexAttribution("table#A", Set.of(key));
    assertThat(cache.indexAttribution()).containsKey(key);
    assertThat(lastAccess.getLong(file)).isEqualTo(expired);
    assertThat(cache.stats().hits()).isEqualTo(hitsBefore);
    try {
      ExecutorIndexCache.sweepIntervalForTests(0);
      cache.file("fresh", config, "adapter", () -> ExecutorIndexCache.loaded(new byte[10], EMPTY));
      assertThat(cache.residentKeys()).doesNotContain(key);
      assertThat(cache.indexAttribution()).doesNotContainKey(key);
      assertThat(cache.stats().bytes()).isEqualTo(residentBeforeAssociation);
    } finally {
      ExecutorIndexCache.sweepIntervalForTests(TimeUnit.SECONDS.toNanos(30));
    }
  }

  @Test
  void attributionDoesNotChangeFairShareEviction() {
    ExecutorIndexCache.Config measuring =
        ExecutorIndexCache.Config.of(true, Long.MAX_VALUE, ExecutorIndexCache.Mode.PIN_HOT, 0);
    askTwiceWithOwnedPrepared("a-old", measuring.withOwner("A"), 40_000);
    askTwiceWithOwnedPrepared("b", measuring.withOwner("B"), 60_000);
    cache.recordIndexAttribution("table#A", Set.of("A|adapter|a-old"));
    cache.recordIndexAttribution("table#B", Set.of("A|adapter|a-old", "B|adapter|b"));
    long[] metadataPeak = {cache.chargeSnapshot().usedBytes()};
    for (int i = 0; i < 2; i++) {
      cache.file(
          "a-new",
          measuring.withOwner("A"),
          "adapter",
          () -> {
            metadataPeak[0] = Math.max(metadataPeak[0], cache.chargeSnapshot().usedBytes());
            return ExecutorIndexCache.loaded(new byte[0], ownedPrepared(40_000), 40_000);
          });
    }
    // The fixture includes the new asked/loading/reservation roots, not another prepared copy.
    long budget = metadataPeak[0];
    cache.releaseCopy("fixture", "A|adapter|a-old");
    long expectedResident = cache.chargeSnapshot().residentBytes();
    cache.clear();
    ExecutorIndexCache.Config a =
        ExecutorIndexCache.Config.of(true, budget, ExecutorIndexCache.Mode.PIN_HOT, 0)
            .withOwner("A");
    ExecutorIndexCache.Config b = a.withOwner("B");
    askTwiceWithOwnedPrepared("a-old", a, 40_000);
    askTwiceWithOwnedPrepared("b", b, 60_000);
    cache.recordIndexAttribution("table#A", Set.of("A|adapter|a-old"));
    cache.recordIndexAttribution("table#B", Set.of("A|adapter|a-old", "B|adapter|b"));
    assertThat(cache.indexAttribution().get("A|adapter|a-old"))
        .containsExactlyInAnyOrder("table#A", "table#B");
    askTwiceWithOwnedPrepared("a-new", a, 40_000);
    assertThat(cache.residentKeys()).containsExactlyInAnyOrder("B|adapter|b", "A|adapter|a-new");
    assertThat(cache.indexAttribution()).doesNotContainKey("A|adapter|a-old");
    assertThat(cache.stats().bytes()).isEqualTo(expectedResident);
    assertThat(cache.chargeSnapshot().usedBytes() + cache.chargeSnapshot().reservedBytes())
        .isLessThanOrEqualTo(budget);
  }

  @Test
  void attributionCannotReportNonResidentFiles() {
    cache.recordIndexAttribution("table#A", Set.of("table|adapter|missing"));
    assertThat(cache.indexAttribution()).isEmpty();
    assertThat(cache.stats().entries()).isZero();
  }

  private long measuredAttributionBudget(String owner, String location, String... indexes) {
    cache.clear();
    try {
      cache.file(
          location,
          ExecutorIndexCache.Config.of(true, Long.MAX_VALUE).withOwner(owner),
          "adapter",
          () -> ExecutorIndexCache.loaded(new byte[10], EMPTY));
      for (String index : indexes) {
        cache.recordIndexAttribution(index, Set.of(owner + "|adapter|" + location));
      }
      return cache.chargeSnapshot().usedBytes();
    } finally {
      cache.clear();
    }
  }

  private void askTwiceWithOwnedPrepared(
      String location, ExecutorIndexCache.Config config, int bytes) {
    for (int i = 0; i < 2; i++) {
      cache.file(
          location,
          config,
          "adapter",
          () -> {
            IndexAdapter.PreparedSearch prepared = ownedPrepared(bytes);
            return ExecutorIndexCache.loaded(new byte[0], prepared, bytes);
          });
    }
  }

  private static IndexAdapter.PreparedSearch ownedPrepared(int bytes) {
    byte[] state = new byte[bytes - 32];
    return prepared(state.length + 32L);
  }

  private static IndexAdapter.PreparedSearch prepared(long estimatedBytes) {
    return new IndexAdapter.PreparedSearch() {
      @Override
      public List<IndexSearchResult> search(ByteBuffer query, int topK) {
        return List.of();
      }

      @Override
      public long estimatedBytes() {
        return estimatedBytes;
      }
    };
  }

  private static void awaitState(AtomicReference<Thread> thread, Thread.State state) {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while ((thread.get() == null || thread.get().getState() != state)
        && System.nanoTime() < deadline) {
      Thread.yield();
    }
    assertThat(thread.get()).isNotNull();
    assertThat(thread.get().getState()).isEqualTo(state);
  }

  /**
   * A load that has not published an entry is either waiting for room or already running its own
   * loader without retaining it; which of the two is scheduling, not a charge.
   */
  private static void awaitStalled(AtomicReference<Thread> thread) {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (!stalled(thread.get()) && System.nanoTime() < deadline) {
      Thread.yield();
    }
    assertThat(thread.get()).isNotNull();
    assertThat(thread.get().getState()).isIn(Thread.State.WAITING, Thread.State.TIMED_WAITING);
  }

  private static boolean stalled(Thread thread) {
    return thread != null
        && (thread.getState() == Thread.State.WAITING
            || thread.getState() == Thread.State.TIMED_WAITING);
  }

  private ExecutorIndexCache.CachedFile install(
      String index, long epoch, String key, ExecutorIndexCache.Config config) {
    cache.applyTarget(index, epoch, Set.of(key));
    ExecutorIndexCache.CachedFile file =
        cache.installManaged(index, epoch, key, config, () -> managedEntry());
    assertThat(file).isNotNull();
    return file;
  }

  private void askTwice(String key, ExecutorIndexCache.Config config, int bytes) {
    ask(key, config, bytes);
    ask(key, config, bytes);
  }

  private void ask(String key, ExecutorIndexCache.Config config, int bytes) {
    cache.file(key, config, "adapter", () -> entry(bytes));
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
