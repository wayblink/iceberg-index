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
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.index.IndexAdapter;
import org.apache.iceberg.index.IndexDefinition;
import org.apache.iceberg.index.IndexFileMemory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestExecutorIndexCacheWorker {
  private static final String SESSION = "application-1";
  private static final String EXECUTOR = "executor-1";
  private static final String INCARNATION = "executor-1#1";
  private static final IndexDefinition DEFINITION = new VectorDefinition(3);
  private static final IndexFileMemory MEMORY =
      IndexFileMemory.builder()
          .fileBytes(80L)
          .payloadBytes(80L)
          .preparedBytes(40L)
          .modelVersion(1)
          .readerId("vector/hnswlib/1")
          .runtimeProfile("reference-bytes=4")
          .structure(Map.of("entries", 10L))
          .build();

  private final ExecutorIndexCache cache = ExecutorIndexCache.get();
  private final AtomicInteger loaderCalls = new AtomicInteger();
  private final List<String> loadedLocations = new ArrayList<>();
  private ExecutorIndexCacheWorker worker;

  @BeforeEach
  void setUp() {
    cache.clear();
    worker = newWorker(countingLoader());
  }

  @AfterEach
  void tearDown() {
    worker.close();
    cache.clear();
  }

  @Test
  void repeatedCommandLoadsOnce() {
    IndexCacheProtocol.Command command = load("A", "f1", 1);

    IndexCacheProtocol.Ack first = worker.execute(command);
    IndexCacheProtocol.Ack second = worker.execute(command);

    assertThat(first.status()).isEqualTo(IndexCacheProtocol.AckStatus.LOADED);
    assertThat(second.status()).isEqualTo(IndexCacheProtocol.AckStatus.LOADED);
    assertThat(loaderCalls.get()).isEqualTo(1);
    assertThat(worker.holders()).containsKey("adapter|A|f1");
  }

  @Test
  void commandForAnotherIncarnationIsRejected() {
    IndexCacheProtocol.Command stale =
        IndexCacheProtocol.Command.builder()
            .session(SESSION)
            .executorId(EXECUTOR)
            .incarnation("executor-1#0")
            .indexUuid("A")
            .epoch(1)
            .planId("plan-1")
            .commandId("plan-1#1-LOAD")
            .type(IndexCacheProtocol.CommandType.LOAD)
            .mode("adapter")
            .file(file("A", "f1"))
            .definition(DEFINITION)
            .io(
                new org.apache.iceberg.hadoop.HadoopFileIO(
                    new org.apache.hadoop.conf.Configuration()))
            .build();

    IndexCacheProtocol.Ack ack = worker.execute(stale);

    assertThat(ack.status()).isEqualTo(IndexCacheProtocol.AckStatus.REJECTED);
    assertThat(ack.reason()).isEqualTo("STALE_INCARNATION");
    assertThat(loaderCalls.get()).isZero();
  }

  @Test
  void commandFromAnotherApplicationIsRejected() {
    IndexCacheProtocol.Command command =
        IndexCacheProtocol.Command.builder()
            .session("another-application")
            .executorId(EXECUTOR)
            .incarnation(INCARNATION)
            .indexUuid("A")
            .epoch(1)
            .planId("plan-1")
            .commandId("plan-1#1-LOAD")
            .type(IndexCacheProtocol.CommandType.LOAD)
            .mode("adapter")
            .file(file("A", "f1"))
            .definition(DEFINITION)
            .io(
                new org.apache.iceberg.hadoop.HadoopFileIO(
                    new org.apache.hadoop.conf.Configuration()))
            .build();

    assertThat(worker.execute(command).reason()).isEqualTo("STALE_SESSION");
    assertThat(loaderCalls.get()).isZero();
  }

  @Test
  void releaseOlderThanTheInstalledCopyIsRejected() {
    worker.execute(load("A", "f1", 2));

    IndexCacheProtocol.Ack staleRelease = worker.execute(release("A", "f1", 1));

    assertThat(staleRelease.status()).isEqualTo(IndexCacheProtocol.AckStatus.REJECTED);
    assertThat(staleRelease.reason()).isEqualTo("STALE_EPOCH");
    assertThat(worker.holders()).containsKey("adapter|A|f1");
  }

  @Test
  void releaseAtTheInstalledEpochRemovesOnlyThatCopy() {
    worker.execute(load("A", "f1", 2));
    worker.execute(load("A", "f2", 2));

    IndexCacheProtocol.Ack released = worker.execute(release("A", "f1", 2));

    assertThat(released.status()).isEqualTo(IndexCacheProtocol.AckStatus.RELEASED);
    assertThat(worker.holders()).doesNotContainKey("adapter|A|f1");
    assertThat(worker.holders()).containsKey("adapter|A|f2");
    assertThat(cache.stats().entries()).isEqualTo(1);
  }

  @Test
  void stateStaysResponsiveWhileALoadIsRunning() throws Exception {
    CountDownLatch loading = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    worker.close();
    worker =
        newWorker(
            command -> {
              loading.countDown();
              try {
                release.await(30, TimeUnit.SECONDS);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
              return loaded(command);
            });

    Thread load = new Thread(() -> worker.execute(load("A", "slow", 1)));
    load.start();
    assertThat(loading.await(30, TimeUnit.SECONDS)).isTrue();

    // The control channel must answer reports while a large prepare is still running.
    IndexCacheProtocol.NodeReport report = worker.report("profile", 3, 1_000L, 0L, Map.of());
    assertThat(report.sequence()).isEqualTo(3);
    assertThat(report.budgetBytes()).isEqualTo(1_000L);

    release.countDown();
    load.join(30_000);
    assertThat(worker.holders()).containsKey("adapter|A|slow");
  }

  @Test
  void closedWorkerRejectsFurtherCommands() {
    worker.close();

    IndexCacheProtocol.Ack ack = worker.execute(load("A", "f1", 1));

    assertThat(worker.closed()).isTrue();
    assertThat(ack.status()).isEqualTo(IndexCacheProtocol.AckStatus.REJECTED);
    assertThat(ack.reason()).isEqualTo("CLOSED");
    assertThat(loaderCalls.get()).isZero();
  }

  @Test
  void acknowledgementCarriesStatusOnly() {
    IndexCacheProtocol.Ack ack = worker.execute(load("A", "f1", 1));

    for (Field field : ack.getClass().getDeclaredFields()) {
      if (Modifier.isStatic(field.getModifiers())) {
        continue;
      }

      Class<?> type = field.getType();
      assertThat(type.isPrimitive() || type == String.class || type.isEnum())
          .as("ACK field %s must not carry payloads or read context", field.getName())
          .isTrue();
    }
  }

  @Test
  void fileWithoutAnEstimateIsNotTreatedAsZero() {
    IndexCacheProtocol.File unknown =
        IndexCacheProtocol.File.builder().location("f9").cacheKey("adapter|A|f9").build();
    IndexCacheProtocol.Command command =
        IndexCacheProtocol.Command.builder()
            .session(SESSION)
            .executorId(EXECUTOR)
            .incarnation(INCARNATION)
            .indexUuid("A")
            .epoch(1)
            .planId("plan-1")
            .commandId("plan-1#9-LOAD")
            .type(IndexCacheProtocol.CommandType.LOAD)
            .mode("adapter")
            .file(unknown)
            .definition(DEFINITION)
            .io(
                new org.apache.iceberg.hadoop.HadoopFileIO(
                    new org.apache.hadoop.conf.Configuration()))
            .build();

    IndexCacheProtocol.Ack ack = worker.execute(command);

    assertThat(ack.status()).isEqualTo(IndexCacheProtocol.AckStatus.REJECTED);
    assertThat(ack.reason()).isEqualTo("SIZE_UNKNOWN");
    assertThat(loaderCalls.get()).isZero();
  }

  @Test
  void reportDescribesOnlyConfirmedCopies() {
    worker.execute(load("A", "f1", 1));

    IndexCacheProtocol.NodeReport report =
        worker.report("profile", 7, 500L, 0L, Map.of("A", 5_000L));

    assertThat(report.session()).isEqualTo(SESSION);
    assertThat(report.executorId()).isEqualTo(EXECUTOR);
    assertThat(report.incarnation()).isEqualTo(INCARNATION);
    assertThat(report.holders()).containsEntry("adapter|A|f1", 120L);
    assertThat(report.accessAgeNanos()).containsEntry("A", 5_000L);
  }

  @Test
  void aFailingLoadIsRetriedBeforeItIsReported() {
    worker.close();
    worker =
        newWorker(
            command -> {
              int attempt = loaderCalls.incrementAndGet();
              if (attempt < 3) {
                throw new IllegalStateException("transient read failure " + attempt);
              }

              return loaded(command);
            });

    IndexCacheProtocol.Ack ack = worker.execute(load("A", "f1", 1));

    assertThat(ack.status()).isEqualTo(IndexCacheProtocol.AckStatus.LOADED);
    assertThat(loaderCalls.get()).isEqualTo(3);
  }

  @Test
  void aLoadThatAlwaysFailsStopsAtTheAttemptLimit() {
    worker.close();
    worker =
        newWorker(
            command -> {
              loaderCalls.incrementAndGet();
              throw new IllegalStateException("unreadable");
            });

    IndexCacheProtocol.Ack ack = worker.execute(load("A", "f1", 1));

    assertThat(ack.status()).isEqualTo(IndexCacheProtocol.AckStatus.FAILED);
    assertThat(ack.reason()).contains("unreadable");
    assertThat(loaderCalls.get()).isEqualTo(3);
  }

  @Test
  void aLoadBeyondTheOperationDeadlineIsReportedAsTimeout() {
    worker.close();
    worker =
        newWorker(
            command -> {
              loaderCalls.incrementAndGet();
              try {
                Thread.sleep(1_200);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
              throw new IllegalStateException("slow read failure");
            },
            IndexCacheWorkerConfig.parse(
                Map.of(
                    "spark.iceberg.index.cache.load.timeout", "1",
                    "spark.iceberg.index.cache.load.max-attempts", "1")));

    IndexCacheProtocol.Ack ack = worker.execute(load("A", "f1", 1));

    assertThat(ack.status()).isEqualTo(IndexCacheProtocol.AckStatus.FAILED);
    // The deadline is the reason, not the last read error.
    assertThat(ack.reason()).isEqualTo("TIMEOUT");
  }

  @Test
  void aSlowLoadThatSucceedsIsStillKept() {
    worker.close();
    worker =
        newWorker(
            command -> {
              loaderCalls.incrementAndGet();
              try {
                Thread.sleep(1_200);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
              return loaded(command);
            },
            IndexCacheWorkerConfig.parse(Map.of("spark.iceberg.index.cache.load.timeout", "1")));

    IndexCacheProtocol.Ack ack = worker.execute(load("A", "f1", 1));

    // The copy exists, so dropping it would only force another load; the driver's plan deadline
    // decides whether a late copy is still useful.
    assertThat(ack.status()).isEqualTo(IndexCacheProtocol.AckStatus.LOADED);
    assertThat(worker.holders()).containsKey("adapter|A|f1");
  }

  @Test
  void aReleaseFromANewerPlanWithdrawsTheOldCopy() {
    worker.execute(load("A", "f1", 4));
    // The replacement plan at epoch 5 wants f2 and withdraws f1, whose copy was installed at 4.
    worker.execute(load("A", "f2", 5));

    IndexCacheProtocol.Ack ack = worker.execute(release("A", "f1", 5));

    // The release is authoritative for this index even though the copy is older than the command.
    assertThat(ack.status()).isEqualTo(IndexCacheProtocol.AckStatus.RELEASED);
    assertThat(worker.holders()).containsOnlyKeys("adapter|A|f2");
  }

  @Test
  void aNewTargetKeepsItsOtherFilesAndOtherIndexes() {
    worker.execute(load("A", "f1", 4));
    worker.execute(load("B", "g1", 4));
    // A newer epoch for A adds f2; nothing it did not mention may be dropped, because an executor
    // only ever sees one file at a time and the driver is the one that decides what to withdraw.
    worker.execute(load("A", "f2", 5));

    assertThat(worker.holders()).containsKeys("adapter|A|f1", "adapter|A|f2", "adapter|B|g1");
  }

  @Test
  void aWithdrawnFileLeavesOnlyWhenTheDriverReleasesIt() {
    worker.execute(load("A", "f1", 4));
    worker.execute(load("A", "f2", 4));

    IndexCacheProtocol.Ack ack = worker.execute(release("A", "f1", 4));

    assertThat(ack.status()).isEqualTo(IndexCacheProtocol.AckStatus.RELEASED);
    assertThat(worker.holders()).containsOnlyKeys("adapter|A|f2");
  }

  @Test
  void releaseThatArrivesBeforeTheLoadPublishesLeavesNoCopy() throws Exception {
    CountDownLatch loading = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    worker.close();
    worker =
        newWorker(
            command -> {
              loading.countDown();
              try {
                release.await(30, TimeUnit.SECONDS);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
              return loaded(command);
            });

    IndexCacheProtocol.Command load = load("A", "f1", 4);
    Thread loadingThread = new Thread(() -> worker.execute(load));
    loadingThread.start();
    assertThat(awaitLatch(loading)).isTrue();

    // The driver gives up on this file while the executor is still preparing it.
    worker.execute(release("A", "f1", 4));
    release.countDown();
    loadingThread.join(30_000);

    assertThat(worker.holders()).doesNotContainKey("adapter|A|f1");
  }

  @Test
  void anOlderEpochTargetDoesNotRollBackANewerOne() {
    worker.execute(load("A", "f1", 5));

    IndexCacheProtocol.Ack stale = worker.execute(load("A", "f2", 4));

    assertThat(stale.status()).isEqualTo(IndexCacheProtocol.AckStatus.REJECTED);
    assertThat(stale.reason()).isEqualTo("STALE_EPOCH");
    assertThat(worker.holders()).containsKey("adapter|A|f1");
  }

  @Test
  void aRejectedAdmissionIsReportedAsRetryable() {
    worker.close();
    worker =
        new ExecutorIndexCacheWorker(
            SESSION,
            EXECUTOR,
            INCARNATION,
            cache,
            // Only 100 bytes of budget: the 120-byte file cannot be admitted right now.
            ExecutorIndexCache.Config.of(true, 100L),
            countingLoader(),
            IndexCacheWorkerConfig.parse(Map.of()));

    IndexCacheProtocol.Ack ack = worker.execute(load("A", "f1", 1));

    assertThat(ack.status()).isEqualTo(IndexCacheProtocol.AckStatus.REJECTED);
    assertThat(ack.reason()).isEqualTo("BUSY");
  }

  @Test
  void aRetryableRefusalIsNotMemoized() {
    worker.close();
    // The cache only has room for one file of this size.
    worker =
        new ExecutorIndexCacheWorker(
            SESSION,
            EXECUTOR,
            INCARNATION,
            cache,
            ExecutorIndexCache.Config.of(true, 130L),
            countingLoader(),
            IndexCacheWorkerConfig.parse(Map.of()));

    IndexCacheProtocol.Command first = load("A", "f1", 1);
    IndexCacheProtocol.Command second = load("A", "f2", 1);
    assertThat(worker.execute(first).status()).isEqualTo(IndexCacheProtocol.AckStatus.LOADED);
    IndexCacheProtocol.Ack refused = worker.execute(second);
    assertThat(refused.reason()).isEqualTo("BUSY");
    int afterRefusal = loaderCalls.get();

    // Free the space and retry the very same command: it must run, not replay the cached refusal.
    IndexCacheProtocol.Ack released = worker.execute(release("A", "f1", 1));
    assertThat(released.status()).isEqualTo(IndexCacheProtocol.AckStatus.RELEASED);
    assertThat(cache.stats().bytes()).as("released bytes").isZero();
    IndexCacheProtocol.Ack retried = worker.execute(second);

    assertThat(retried.reason()).isNull();
    assertThat(retried.status()).isEqualTo(IndexCacheProtocol.AckStatus.LOADED);
    assertThat(afterRefusal).as("a refusal must not read the file").isEqualTo(1);
    assertThat(loaderCalls.get()).isEqualTo(2);
  }

  @Test
  void aCopyEvictedByTheCacheStopsBeingReported() {
    worker.execute(load("A", "f1", 1));
    assertThat(worker.holders()).containsKey("adapter|A|f1");

    // The cache can drop a copy on its own, for example for idle time or another index's pressure.
    cache.releaseCopy("A", "adapter|A|f1");

    assertThat(worker.holders()).doesNotContainKey("adapter|A|f1");
    assertThat(worker.report("p", 1, 1_000L, 0L, Map.of()).usedBytes()).isZero();
  }

  @Test
  void theFirstManagedTargetWaitsForReferencesAQueryAlreadyHolds() {
    IndexCacheProtocol.Command first = load("A", "f1", 1);
    assertThat(worker.execute(first).status()).isEqualTo(IndexCacheProtocol.AckStatus.LOADED);

    // A query is still using the copy the executor handed out.
    ExecutorIndexCache.Lease lease = cache.acquireManaged("A", 1, "adapter|A|f1");
    assertThat(lease).isNotNull();
    try {
      // Taking over another index for the first time must not change what that query is using.
      IndexCacheProtocol.Ack blocked = worker.execute(load("B", "g1", 1));
      assertThat(blocked.reason()).isEqualTo("WAIT_REFERENCES");
      assertThat(worker.holders()).doesNotContainKey("adapter|B|g1");
    } finally {
      lease.close();
    }

    assertThat(worker.execute(load("B", "g1", 1)).status())
        .isEqualTo(IndexCacheProtocol.AckStatus.LOADED);
    assertThat(worker.holders()).containsKey("adapter|B|g1");
  }

  @Test
  void aLaterEpochOfTheSameIndexIsNotBlockedByAQuery() {
    assertThat(worker.execute(load("A", "f1", 1)).status())
        .isEqualTo(IndexCacheProtocol.AckStatus.LOADED);
    ExecutorIndexCache.Lease lease = cache.acquireManaged("A", 1, "adapter|A|f1");
    try {
      // A newer epoch keeps the same managed copies valid, so a re-load proceeds.
      assertThat(worker.execute(load("A", "f2", 2)).status())
          .isEqualTo(IndexCacheProtocol.AckStatus.LOADED);
    } finally {
      lease.close();
    }
  }

  @Test
  void workerConfigurationRejectsNonPositiveValues() {
    assertThatThrownBy(
            () ->
                IndexCacheWorkerConfig.parse(
                    Map.of("spark.iceberg.index.cache.control.poll-interval", "0")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("poll-interval");
    assertThatThrownBy(
            () ->
                IndexCacheWorkerConfig.parse(
                    Map.of("spark.iceberg.index.cache.load.max-concurrent", "-1")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("max-concurrent");
    assertThat(IndexCacheWorkerConfig.parse(Map.of()).pollMillis()).isEqualTo(1_000L);
    assertThat(IndexCacheWorkerConfig.parse(Map.of()).maxCommands()).isEqualTo(64);
    assertThat(IndexCacheWorkerConfig.parse(Map.of()).operationTimeoutMillis())
        .isEqualTo(TimeUnit.MINUTES.toMillis(10));
  }

  private static boolean awaitLatch(CountDownLatch latch) {
    try {
      return latch.await(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }

  private ExecutorIndexCacheWorker newWorker(ExecutorIndexCacheWorker.Loader loader) {
    return newWorker(loader, IndexCacheWorkerConfig.parse(Map.of()));
  }

  private ExecutorIndexCacheWorker newWorker(
      ExecutorIndexCacheWorker.Loader loader, IndexCacheWorkerConfig config) {
    return new ExecutorIndexCacheWorker(
        SESSION,
        EXECUTOR,
        INCARNATION,
        cache,
        ExecutorIndexCache.Config.of(true, 1_000_000L),
        loader,
        config);
  }

  private IndexCacheProtocol.Command load(String indexUuid, String location, long epoch) {
    return IndexCacheProtocol.Command.builder()
        .session(SESSION)
        .executorId(EXECUTOR)
        .incarnation(INCARNATION)
        .indexUuid(indexUuid)
        .epoch(epoch)
        .planId("plan-" + epoch)
        .commandId("plan-" + epoch + "#" + location + "-LOAD")
        .type(IndexCacheProtocol.CommandType.LOAD)
        .mode("adapter")
        .file(file(indexUuid, location))
        .definition(DEFINITION)
        .io(new org.apache.iceberg.hadoop.HadoopFileIO(new org.apache.hadoop.conf.Configuration()))
        .build();
  }

  private IndexCacheProtocol.Command release(String indexUuid, String location, long epoch) {
    return IndexCacheProtocol.Command.builder()
        .session(SESSION)
        .executorId(EXECUTOR)
        .incarnation(INCARNATION)
        .indexUuid(indexUuid)
        .epoch(epoch)
        .planId("plan-" + epoch)
        .commandId("plan-" + epoch + "#" + location + "-RELEASE")
        .type(IndexCacheProtocol.CommandType.RELEASE)
        .mode("OTHER")
        .file(
            IndexCacheProtocol.File.builder()
                .location(location)
                .cacheKey("adapter|" + indexUuid + "|" + location)
                .build())
        .build();
  }

  private static IndexCacheProtocol.File file(String indexUuid, String location) {
    return IndexCacheProtocol.File.builder()
        .location(location)
        .cacheKey("adapter|" + indexUuid + "|" + location)
        .estimatedBytes(120L)
        .memory(MEMORY)
        .build();
  }

  private static ExecutorIndexCache.CachedFile loaded(IndexCacheProtocol.Command command) {
    return ExecutorIndexCache.loaded(
        new byte[] {1, 2, 3},
        new IndexAdapter.PreparedSearch() {
          @Override
          public List<org.apache.iceberg.index.IndexSearchResult> search(
              java.nio.ByteBuffer query, int topK) {
            return List.of();
          }

          @Override
          public long estimatedBytes() {
            return 40L;
          }
        },
        120L);
  }

  private ExecutorIndexCacheWorker.Loader countingLoader() {
    return command -> {
      loaderCalls.incrementAndGet();
      loadedLocations.add(command.file().location());
      return loaded(command);
    };
  }

  private static final class VectorDefinition implements IndexDefinition {
    private static final long serialVersionUID = 1L;
    private final int fieldId;

    private VectorDefinition(int fieldId) {
      this.fieldId = fieldId;
    }

    @Override
    public String type() {
      return "vector";
    }

    @Override
    public int fieldId() {
      return fieldId;
    }
  }
}
