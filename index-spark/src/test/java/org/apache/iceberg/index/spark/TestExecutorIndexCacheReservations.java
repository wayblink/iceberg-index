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
    cache.file("a", ExecutorIndexCache.Config.of(true, 1000), "adapter", () -> loaded);
    long expected = IndexCacheMemory.residentBytes(10, prepared.estimatedBytes());
    assertThat(loaded.bytes()).isEqualTo(expected);
    assertThat(cache.stats().bytes()).isEqualTo(expected);
  }

  @Test
  void waitingLoadDoesNotReleaseAnotherLoadsHeldRoom() throws Exception {
    ExecutorService pool = Executors.newFixedThreadPool(2);
    CountDownLatch firstStarted = new CountDownLatch(1);
    CountDownLatch secondStarted = new CountDownLatch(1);
    CountDownLatch finishFirst = new CountDownLatch(1);
    ExecutorIndexCache.Config config = ExecutorIndexCache.Config.of(true, 250);
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
    ExecutorIndexCache.Config config = ExecutorIndexCache.Config.of(true, 250);
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
    ExecutorIndexCache.Config config = ExecutorIndexCache.Config.of(true, 250);
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
                        // one entry is estimated by the entry already resident, not by a ceiling
                        assertThat(reserved()).isEqualTo(100);
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
  void aLoadCannotSpendAnotherLoadsReservation() throws Exception {
    ExecutorService pool = Executors.newFixedThreadPool(2);
    CountDownLatch bothStarted = new CountDownLatch(2);
    CountDownLatch finishLarge = new CountDownLatch(1);
    CountDownLatch finishOther = new CountDownLatch(1);
    ExecutorIndexCache.Config config = ExecutorIndexCache.Config.of(true, 250);
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
                        return ExecutorIndexCache.loaded(new byte[150], EMPTY, 150);
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
                        return ExecutorIndexCache.loaded(new byte[100], EMPTY, 100);
                      }));
      assertThat(bothStarted.await(10, TimeUnit.SECONDS)).isTrue();
      finishLarge.countDown();
      large.get(10, TimeUnit.SECONDS);
      assertThat(reserved()).as("the second load's promise is untouched").isEqualTo(100);
      finishOther.countDown();
      other.get(10, TimeUnit.SECONDS);
      assertThat(reserved()).isZero();
      assertThat(cache.stats().bytes()).isEqualTo(250);
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
    ExecutorIndexCache.Config config = ExecutorIndexCache.Config.of(true, 250);
    try {
      Future<?> first =
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
                        return ExecutorIndexCache.loaded(new byte[80], EMPTY, 80);
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
        ExecutorIndexCache.Config.of(true, 100, ExecutorIndexCache.Mode.PIN_HOT, 0).withOwner("A");
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
        ExecutorIndexCache.Config.of(true, 1000).withOwner("old-table"),
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
    ExecutorIndexCache.Config config = ExecutorIndexCache.Config.of(true, 140);
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
    ExecutorIndexCache.Config config = ExecutorIndexCache.Config.of(true, 100);
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

  @Test
  void oldLoadCannotPublishAfterOffload() throws Exception {
    String key = "table|adapter|managed";
    ExecutorIndexCache.Config config = ExecutorIndexCache.Config.of(true, 100).withOwner("table");
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
                        return ExecutorIndexCache.loaded(new byte[10], EMPTY);
                      }));
      assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
      cache.releaseIndex("A", 2);
      finish.countDown();
      assertThat(load.get(10, TimeUnit.SECONDS)).isNull();
      assertThat(cache.stats().bytes()).isZero();
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
    ExecutorIndexCache.Config config = ExecutorIndexCache.Config.of(true, 100).withOwner("table");
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
    assertThat(cache.stats().bytes()).isEqualTo(file.bytes());
    try (ExecutorIndexCache.Lease lease = cache.acquireManaged("B", 1, key)) {
      assertThat(lease).isNotNull();
      cache.releaseIndex("B", 2);
      assertThat(cache.stats().bytes()).isEqualTo(file.bytes());
    }
    assertThat(cache.stats().bytes()).isZero();
  }

  @Test
  void releaseWaitsForSearch() throws Exception {
    String key = "table|adapter|managed";
    ExecutorIndexCache.Config config = ExecutorIndexCache.Config.of(true, 100).withOwner("table");
    ExecutorIndexCache.CachedFile file = install("A", 1, key, config);
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
      assertThat(cache.stats().bytes()).isEqualTo(file.bytes());
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
    ExecutorIndexCache.Config config = ExecutorIndexCache.Config.of(true, 100).withOwner("table");
    ExecutorIndexCache.CachedFile file = install("A", 1, key, config);
    try (ExecutorIndexCache.Lease lease = cache.acquireManaged("A", 1, key)) {
      assertThat(lease).isNotNull();
      cache.releaseIndex("A", 2);
      assertThat(cache.acquireManaged("A", 1, key)).isNull();
      assertThat(cache.stats().bytes()).isEqualTo(file.bytes());
      List<IndexSearchResult> results =
          lease.file().searching(prepared -> prepared.search(ByteBuffer.allocate(0), 1));
      assertThat(results).isEmpty();
    }
    assertThat(cache.stats().bytes()).isZero();
  }

  @Test
  void oldLeaseCloseCannotReleaseNewOwnerAndCloseIsIdempotent() {
    String key = "table|adapter|managed";
    ExecutorIndexCache.Config config = ExecutorIndexCache.Config.of(true, 100).withOwner("table");
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
      assertThat(cache.stats().bytes()).isEqualTo(file.bytes());
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
    ExecutorIndexCache.Config config = ExecutorIndexCache.Config.of(true, 100).withOwner("table");
    cache.applyTarget("A", 1, Set.of(old, common));
    ExecutorIndexCache.CachedFile oldFile =
        cache.installManaged(
            "A", 1, old, config, () -> ExecutorIndexCache.loaded(new byte[10], EMPTY));
    ExecutorIndexCache.CachedFile commonFile =
        cache.installManaged(
            "A", 1, common, config, () -> ExecutorIndexCache.loaded(new byte[10], EMPTY));
    try (ExecutorIndexCache.Lease lease = cache.acquireManaged("A", 1, old)) {
      assertThat(lease).isNotNull();
      cache.applyTarget("A", 2, Set.of(common, next));
      assertThat(cache.stats().bytes()).isEqualTo(oldFile.bytes() + commonFile.bytes());
      assertThat(cache.acquireManaged("A", 1, common)).isNull();
    }
    assertThat(cache.stats().bytes()).isEqualTo(commonFile.bytes());
    assertThat(cache.residentKeys()).containsExactly(common);
  }

  @Test
  void managedKeyCannotBeFilledByLegacyLazyQuery() {
    String key = "table|adapter|managed";
    ExecutorIndexCache.Config config = ExecutorIndexCache.Config.of(true, 100).withOwner("table");
    cache.applyTarget("A", 1, Set.of(key));
    assertThat(
            cache.file(
                "managed", config, "adapter", () -> ExecutorIndexCache.loaded(new byte[10], EMPTY)))
        .isNotNull();
    assertThat(cache.stats().entries()).isZero();
    cache.releaseIndex("A", 2);
    cache.file("managed", config, "adapter", () -> ExecutorIndexCache.loaded(new byte[10], EMPTY));
    assertThat(cache.stats().entries()).isZero();
  }

  @Test
  void managedEntryIsNotAnIdleOrFairShareVictim() throws Exception {
    String key = "A|adapter|managed";
    ExecutorIndexCache.Config config =
        ExecutorIndexCache.Config.of(true, 40, ExecutorIndexCache.Mode.PIN_HOT, 1).withOwner("A");
    ExecutorIndexCache.CachedFile file = install("A", 1, key, config);
    Field lastAccess = file.getClass().getDeclaredField("lastAccessNanos");
    lastAccess.setAccessible(true);
    lastAccess.setLong(file, System.nanoTime() - TimeUnit.MINUTES.toNanos(31));
    try {
      ExecutorIndexCache.sweepIntervalForTests(0);
      askTwice("other", config.withOwner("B"), 20);
      askTwice("new", config.withOwner("B"), 20);
      assertThat(cache.residentKeys()).contains(key);
      assertThat(cache.stats().bytes()).isLessThanOrEqualTo(config.maxBytes());
    } finally {
      ExecutorIndexCache.sweepIntervalForTests(TimeUnit.SECONDS.toNanos(30));
    }
  }

  @Test
  void failedManagedLoadReleasesItsReservation() throws Exception {
    String key = "table|adapter|managed";
    ExecutorIndexCache.Config config = ExecutorIndexCache.Config.of(true, 100).withOwner("table");
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
        cache.installManaged(
            "A", 1, key, config, () -> ExecutorIndexCache.loaded(new byte[10], EMPTY));
    assertThat(installed).isNotNull();
    assertThat(reserved()).isZero();
    assertThat(cache.stats().bytes()).isEqualTo(installed.bytes());
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
    ExecutorIndexCache.Config config =
        ExecutorIndexCache.Config.of(true, 100, mode, 0).withOwner("table");
    assertThat(install("A", 1, key, config).bytes()).isEqualTo(cache.stats().bytes());
  }

  @Test
  void closedLeaseDoesNotRetainFile() throws Exception {
    String key = "table|adapter|managed";
    install("A", 1, key, ExecutorIndexCache.Config.of(true, 100).withOwner("table"));
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
    ExecutorIndexCache.CachedFile file =
        install("A", 1, key, ExecutorIndexCache.Config.of(true, 100).withOwner("table"));
    try (ExecutorIndexCache.Lease lease = cache.acquireManaged("A", 1, key)) {
      assertThat(lease).isNotNull();
      cache.clear();
      assertThat(cache.stats().bytes()).isEqualTo(file.bytes());
    }
    assertThat(cache.stats().bytes()).isZero();
  }

  @Test
  void reservationWaiterIsCancelledByOffload() throws Exception {
    String a = "table|adapter|a";
    String b = "table|adapter|b";
    ExecutorIndexCache.Config config = ExecutorIndexCache.Config.of(true, 20).withOwner("table");
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
                        return ExecutorIndexCache.loaded(new byte[10], EMPTY);
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
      cache.releaseIndex("B", 2);
      assertThat(second.get(10, TimeUnit.SECONDS)).isNull();
      assertThat(reserved()).isEqualTo(20);
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
    ExecutorIndexCache.Config config = ExecutorIndexCache.Config.of(true, 100).withOwner("table");
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
                      () -> {
                        started.countDown();
                        await(finish);
                        return ExecutorIndexCache.loaded(new byte[10], EMPTY);
                      }));
      assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
      cache.applyTarget("A", 1, Set.of(key));
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
    ExecutorIndexCache.Config config = ExecutorIndexCache.Config.of(true, 100).withOwner("table");
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
                      return ExecutorIndexCache.loaded(new byte[10], EMPTY);
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
      assertThat(cache.stats().bytes()).isEqualTo(20);
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
    ExecutorIndexCache.Config config = ExecutorIndexCache.Config.of(true, 100).withOwner("table");
    ExecutorIndexCache.CachedFile ephemeral = ExecutorIndexCache.loaded(new byte[10], EMPTY);
    Method acquireMonitor =
        ExecutorIndexCache.class.getDeclaredMethod("loadingMonitor", String.class);
    acquireMonitor.setAccessible(true);
    Object monitor = acquireMonitor.invoke(cache, key);
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
      assertThat(cache.stats().bytes()).isZero();
    } finally {
      pool.shutdownNow();
      try {
        assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
      } finally {
        releaseMonitor.invoke(cache, key, monitor);
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"100,true", "10,false"})
  void reuseRespectsTheNewBudgetWithoutReleasingSharedLease(long budget, boolean reusable)
      throws Exception {
    String key = "table|adapter|managed";
    ExecutorIndexCache.Config original = ExecutorIndexCache.Config.of(true, 100).withOwner("table");
    install("A", 1, key, original);
    cache.applyTarget("B", 1, Set.of(key));
    try (ExecutorIndexCache.Lease lease = cache.acquireManaged("B", 1, key)) {
      assertThat(lease).isNotNull();
      cache.applyTarget("A", 2, Set.of(key));
      ExecutorIndexCache.Config limited =
          ExecutorIndexCache.Config.of(true, budget).withOwner("table");
      ExecutorIndexCache.CachedFile reused =
          cache.installManaged(
              "A",
              2,
              key,
              limited,
              () -> {
                throw new AssertionError("Reuse must not reload");
              });
      // a file of 20 bytes fits a budget of 100 but not one of 10, and the budget is now the only
      // thing that decides that
      assertThat(reused != null).isEqualTo(reusable);
      cache.releaseIndex("A", 3);
      assertThat(cache.stats().bytes()).isEqualTo(20);
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
    ExecutorIndexCache.Config original = ExecutorIndexCache.Config.of(true, 100).withOwner("table");
    install("A", 1, a, original);
    install("B", 1, b, original);
    cache.applyTarget("A", 2, Set.of(a));
    assertThat(
            cache.installManaged(
                "A",
                2,
                a,
                ExecutorIndexCache.Config.of(true, 30).withOwner("table"),
                () -> {
                  throw new AssertionError("Rejected reuse must not reload");
                }))
        .isNull();
    assertThat(cache.stats().bytes()).isEqualTo(40);
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
    assertThat(cache.stats().bytes()).isEqualTo(20);
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
      assertThat(cache.stats().bytes()).isEqualTo(20);
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
    long budget = cache.chargeSnapshot().usedBytes();
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
    assertThat(cache.stats().bytes()).isEqualTo(100_000);
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
    return new IndexAdapter.PreparedSearch() {
      @Override
      public List<IndexSearchResult> search(ByteBuffer query, int topK) {
        return List.of();
      }

      @Override
      public long estimatedBytes() {
        return 16 + ((16L + state.length + 7) / 8) * 8;
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

  private ExecutorIndexCache.CachedFile install(
      String index, long epoch, String key, ExecutorIndexCache.Config config) {
    cache.applyTarget(index, epoch, Set.of(key));
    ExecutorIndexCache.CachedFile file =
        cache.installManaged(
            index, epoch, key, config, () -> ExecutorIndexCache.loaded(new byte[10], EMPTY));
    assertThat(file).isNotNull();
    return file;
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
