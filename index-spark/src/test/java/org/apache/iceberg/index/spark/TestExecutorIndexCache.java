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
    ExecutorIndexCache.Config enabled = config(250, 250);

    cache.file("s3://bucket/a.puffin", enabled, "adapter", () -> load(loads, 100));
    cache.file("s3://bucket/b.puffin", enabled, "adapter", () -> load(loads, 100));
    // whichever of a and b is the least recently used is evicted to make room for c
    cache.file("s3://bucket/a.puffin", enabled, "adapter", () -> load(loads, 100));
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

  private static ExecutorIndexCache.CachedFile load(AtomicInteger loads, int bytes) {
    loads.incrementAndGet();
    return ExecutorIndexCache.loaded(new byte[bytes], EMPTY, bytes);
  }
}
