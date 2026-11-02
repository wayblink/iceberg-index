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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestExecutorBudgetBinding {
  private static final long MB = 1024L * 1024L;

  /**
   * Bookkeeping a published copy is charged above the size a policy fixture states: the entry
   * wrapper, its owner text and the map node that holds it. It is a few hundred bytes, so a bound
   * keeps these fixtures from pinning an exact layout while still proving the stated charge.
   */
  private static final long CONTROL_BYTES = 4096L;

  private final ExecutorIndexCache cache = ExecutorIndexCache.get();

  @BeforeEach
  @AfterEach
  void clear() {
    cache.clear();
  }

  @Test
  void managedAndLazyCopiesShareTheExecutorCapacity() {
    try (ExecutorIndexCache.ExecutorBudget ignored = cache.bindExecutorBudget(512 * MB)) {
      ExecutorIndexCache.Config largerQuery = ExecutorIndexCache.Config.of(true, 1024 * MB);
      cache.file("lazy", largerQuery, "test", () -> charged(100 * MB));
      cache.applyTarget("managed", 1, Set.of("managed-key"));
      assertThat(
              cache.installManaged(
                  "managed", 1, "managed-key", largerQuery, () -> charged(450 * MB)))
          .isNull();
      assertThat(cache.stats().bytes()).isBetween(100 * MB, 100 * MB + CONTROL_BYTES);
      assertThat(cache.memoryUsage().reservedBytes()).isZero();
    }
  }

  @Test
  void nodeReportsIncludeActualLazyReservationsWithoutDoubleCounting() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    CountDownLatch loading = new CountDownLatch(1);
    CountDownLatch finish = new CountDownLatch(1);
    try (ExecutorIndexCache.ExecutorBudget ignored = cache.bindExecutorBudget(512 * MB)) {
      ExecutorIndexCache.Config largerQuery = ExecutorIndexCache.Config.of(true, 1024 * MB);
      cache.file("resident", largerQuery, "test", () -> charged(100 * MB));
      Future<?> load =
          executor.submit(
              () ->
                  cache.file(
                      "in-flight",
                      largerQuery,
                      "test",
                      400 * MB,
                      () -> {
                        loading.countDown();
                        await(finish);
                        return charged(400 * MB);
                      }));
      assertThat(loading.await(5, TimeUnit.SECONDS)).isTrue();
      ExecutorIndexCacheWorker observer =
          new ExecutorIndexCacheWorker(
              "session",
              "executor",
              "incarnation",
              cache,
              ExecutorIndexCache.Config.of(true, 512 * MB),
              command -> charged(MB),
              IndexCacheWorkerConfig.parse(Map.of()));
      IndexCacheProtocol.NodeReport report = observer.report("test", 1, 512 * MB, 0, Map.of());
      assertThat(report.usedBytes()).isBetween(100 * MB, 100 * MB + CONTROL_BYTES);
      assertThat(report.reservedBytes()).isBetween(400 * MB, 400 * MB + CONTROL_BYTES);
      assertThat(report.holders()).containsKey("|test|resident");
      assertThat(report.holders().get("|test|resident"))
          .isBetween(100 * MB, 100 * MB + CONTROL_BYTES);
      assertThat(report.usedBytes() + report.reservedBytes())
          .isLessThanOrEqualTo(report.budgetBytes());
      finish.countDown();
      load.get(5, TimeUnit.SECONDS);
      assertThat(cache.memoryUsage().usedBytes()).isBetween(500 * MB, 500 * MB + CONTROL_BYTES);
      assertThat(cache.memoryUsage().reservedBytes()).isZero();
    } finally {
      finish.countDown();
      executor.shutdownNow();
      assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    }
  }

  @Test
  void aManagedLoadCannotSpendALazyLoadsReservation() throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(2);
    CountDownLatch loading = new CountDownLatch(1);
    CountDownLatch finish = new CountDownLatch(1);
    CountDownLatch managedAttempt = new CountDownLatch(1);
    try (ExecutorIndexCache.ExecutorBudget ignored = cache.bindExecutorBudget(512 * MB)) {
      ExecutorIndexCache.Config largerQuery = ExecutorIndexCache.Config.of(true, 1024 * MB);
      Future<?> lazy =
          executor.submit(
              () ->
                  cache.file(
                      "lazy",
                      largerQuery,
                      "test",
                      300 * MB,
                      () -> {
                        loading.countDown();
                        await(finish);
                        return charged(300 * MB);
                      }));
      assertThat(loading.await(5, TimeUnit.SECONDS)).isTrue();
      cache.applyTarget("managed", 1, Set.of("managed-key"));
      Future<ExecutorIndexCache.CachedFile> managed =
          executor.submit(
              () -> {
                managedAttempt.countDown();
                return cache.installManaged(
                    "managed", 1, "managed-key", largerQuery, 300 * MB, () -> charged(300 * MB));
              });
      assertThat(managedAttempt.await(5, TimeUnit.SECONDS)).isTrue();
      ExecutorIndexCache.MemoryUsage memory = cache.memoryUsage();
      assertThat(memory.usedBytes() + memory.reservedBytes()).isLessThanOrEqualTo(512 * MB);
      finish.countDown();
      lazy.get(5, TimeUnit.SECONDS);
      assertThat(managed.get(5, TimeUnit.SECONDS)).isNull();
      assertThat(cache.memoryUsage().usedBytes()).isBetween(300 * MB, 300 * MB + CONTROL_BYTES);
      assertThat(cache.memoryUsage().reservedBytes()).isZero();
    } finally {
      finish.countDown();
      executor.shutdownNow();
      assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    }
  }

  @Test
  void failureReleasesReservationsButNotTheExecutorCapacity() {
    try (ExecutorIndexCache.ExecutorBudget ignored = cache.bindExecutorBudget(512 * MB)) {
      try {
        cache.file(
            "failure",
            ExecutorIndexCache.Config.of(true, 1024 * MB),
            "test",
            400 * MB,
            () -> {
              throw new IllegalStateException("Loader failed");
            });
        throw new AssertionError("Loader did not fail");
      } catch (IllegalStateException expected) {
        assertThat(expected).hasMessage("Loader failed");
      }
      assertThat(cache.memoryUsage().reservedBytes()).isZero();
      cache.file(
          "after-failure",
          ExecutorIndexCache.Config.of(true, 1024 * MB),
          "test",
          () -> charged(600 * MB));
      assertThat(cache.residentKeys()).isEmpty();
    }
  }

  @Test
  void closingThePluginBudgetFencesLoadsAlreadyInFlight() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    CountDownLatch loading = new CountDownLatch(1);
    CountDownLatch finish = new CountDownLatch(1);
    ExecutorIndexCache.ExecutorBudget binding = cache.bindExecutorBudget(512 * MB);
    try {
      Future<?> load =
          executor.submit(
              () ->
                  cache.file(
                      "old-app",
                      ExecutorIndexCache.Config.of(true, 1024 * MB),
                      "test",
                      300 * MB,
                      () -> {
                        loading.countDown();
                        await(finish);
                        return charged(300 * MB);
                      }));
      assertThat(loading.await(5, TimeUnit.SECONDS)).isTrue();
      binding.close();
      finish.countDown();
      load.get(5, TimeUnit.SECONDS);
      assertThat(cache.residentKeys()).isEmpty();
      assertThat(cache.memoryUsage().reservedBytes()).isZero();
      try (ExecutorIndexCache.ExecutorBudget next = cache.bindExecutorBudget(512 * MB)) {
        assertThat(cache.memoryUsage().usedBytes()).isZero();
      }
    } finally {
      finish.countDown();
      binding.close();
      executor.shutdownNow();
      assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    }
  }

  @Test
  void closingAnOldBindingDoesNotRemoveANewerOne() {
    ExecutorIndexCache.ExecutorBudget first = cache.bindExecutorBudget(512 * MB);
    first.close();
    try (ExecutorIndexCache.ExecutorBudget second = cache.bindExecutorBudget(256 * MB)) {
      first.close();
      cache.file(
          "new-app",
          ExecutorIndexCache.Config.of(true, 1024 * MB),
          "test",
          () -> charged(300 * MB));
      assertThat(cache.residentKeys()).isEmpty();
    }
  }

  @Test
  void aCacheWithoutAnActivePluginKeepsTheQueryBudget() {
    ExecutorIndexCache.ExecutorBudget binding = cache.bindExecutorBudget(512 * MB);
    binding.close();
    cache.file(
        "no-plugin",
        ExecutorIndexCache.Config.of(true, 1024 * MB),
        "test",
        () -> charged(600 * MB));
    assertThat(cache.stats().bytes()).isBetween(600 * MB, 600 * MB + CONTROL_BYTES);
  }

  private static void await(CountDownLatch latch) {
    try {
      if (!latch.await(5, TimeUnit.SECONDS)) {
        throw new IllegalStateException("Loader timed out");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

  private static ExecutorIndexCache.CachedFile charged(long bytes) {
    return ExecutorIndexCache.loaded(new byte[] {1}, null, bytes);
  }
}
