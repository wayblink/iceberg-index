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

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.index.IndexAdapter;
import org.apache.iceberg.index.IndexDefinition;
import org.apache.iceberg.index.IndexFileMemory;
import org.apache.iceberg.index.IndexSearchResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Exercises the control channel through its Spark-free seam: the same messages Spark's plugin
 * context would carry, with a fake transport. Real {@code PluginContext} wiring is a cluster test.
 */
class TestIndexCachePlugin {
  private static final String SESSION = "application-1";
  private static final String EXECUTOR = "executor-1";
  private static final String INCARNATION = "executor-1#1";
  private static final IndexDefinition DEFINITION = new VectorDefinition(2);
  private static final IndexFileMemory MEMORY =
      IndexFileMemory.builder()
          .fileBytes(80L)
          .payloadBytes(60L)
          .preparedBytes(40L)
          .modelVersion(1)
          .readerId("vector/hnswlib/1")
          .runtimeProfile("reference-bytes=4")
          .structure(Map.of())
          .build();

  private final ExecutorIndexCache cache = ExecutorIndexCache.get();
  private final AtomicInteger loaderCalls = new AtomicInteger();
  private long now = Duration.ofMinutes(5).toNanos();
  private IndexResidencyCoordinator coordinator;
  private IndexCacheDriverControl control;

  @BeforeEach
  void setUp() {
    cache.clear();
    coordinator = new IndexResidencyCoordinator(() -> now);
    control = new IndexCacheDriverControl(coordinator);
  }

  @AfterEach
  void tearDown() {
    cache.clear();
  }

  @Test
  void driverAnswersAReportWithThePlannedCommands() {
    control.receive(report(1, 1_000, 0));

    String plan =
        coordinator.acceptLoad(target("A", "f1"), IndexResidencyPolicy.parse("forever"), now);
    IndexCacheProtocol.Reply reply = control.receive(report(2, 1_000, 0));

    assertThat(reply.commands()).hasSize(1);
    assertThat(reply.commands().get(0).type()).isEqualTo(IndexCacheProtocol.CommandType.LOAD);
    assertThat(reply.commands().get(0).planId()).isEqualTo(plan);
    assertThat(reply.commands().get(0).file().location()).isEqualTo("f1");
  }

  @Test
  void driverAppliesAcknowledgements() {
    control.receive(report(1, 1_000, 0));
    String plan =
        coordinator.acceptLoad(target("A", "f1"), IndexResidencyPolicy.parse("forever"), now);
    IndexCacheProtocol.Command command = control.receive(report(2, 1_000, 0)).commands().get(0);

    control.receive(
        IndexCacheProtocol.Ack.builder()
            .session(SESSION)
            .planId(plan)
            .commandId(command.commandId())
            .executorId(EXECUTOR)
            .incarnation(INCARNATION)
            .epoch(command.epoch())
            .sequence(command.sequence())
            .status(IndexCacheProtocol.AckStatus.LOADED)
            .bytes(120)
            .build());

    assertThat(coordinator.result(plan).status()).isEqualTo(IndexCacheProtocol.Status.COMPLETE);
  }

  @Test
  void aReplyCarriesOnlyOneBatchAndTheRestFollowsLater() {
    IndexResidencyCoordinator bounded = new IndexResidencyCoordinator(() -> now, 2);
    IndexCacheDriverControl boundedControl = new IndexCacheDriverControl(bounded);
    boundedControl.receive(report(1, 1_000, 0));
    bounded.acceptLoad(target("A", "f1", "f2", "f3"), IndexResidencyPolicy.parse("forever"), now);

    IndexCacheProtocol.Reply first = boundedControl.receive(report(2, 1_000, 0));

    assertThat(first.commands()).hasSize(2);
    assertThat(first.commands().get(0).definition()).isNotNull();
    assertThat(first.commands().get(0).io()).isNotNull();
  }

  @Test
  void aFailedAcknowledgementRoundTripIsRetriedInsteadOfDropped() {
    control.receive(report(1, 1_000, 0));
    String plan =
        coordinator.acceptLoad(target("A", "f1"), IndexResidencyPolicy.parse("forever"), now);
    java.util.concurrent.atomic.AtomicBoolean failOnce =
        new java.util.concurrent.atomic.AtomicBoolean(true);
    List<Object> sent = new ArrayList<>();
    IndexCacheRunner runner =
        runner(
            message -> {
              if (message instanceof IndexCacheProtocol.Ack && failOnce.getAndSet(false)) {
                throw new java.io.IOException("control channel hiccup");
              }
              return control.receive(message);
            },
            sent);
    runner.loop.pollOnce();
    for (int i = 0;
        i < 100 && coordinator.result(plan).status() != IndexCacheProtocol.Status.COMPLETE;
        i++) {
      runner.loop.pollOnce();
      sleepQuietly(20);
    }

    // The acknowledgement is retried, and the command is not executed a second time.
    assertThat(coordinator.result(plan).status()).isEqualTo(IndexCacheProtocol.Status.COMPLETE);
    assertThat(loaderCalls.get()).isEqualTo(1);
  }

  @Test
  void aBusyExecutorIsAskedAgainAndTheLimitIsEnforced() {
    control.receive(report(1, 1_000, 0));
    String plan =
        coordinator.acceptLoad(target("A", "f1"), IndexResidencyPolicy.parse("forever"), now);
    IndexCacheProtocol.Command command = control.receive(report(2, 1_000, 0)).commands().get(0);

    control.receive(
        IndexCacheProtocol.Ack.builder()
            .session(SESSION)
            .planId(plan)
            .commandId(command.commandId())
            .executorId(EXECUTOR)
            .incarnation(INCARNATION)
            .epoch(command.epoch())
            .sequence(command.sequence())
            .status(IndexCacheProtocol.AckStatus.REJECTED)
            .bytes(0)
            .reason("BUSY")
            .build());

    // The command is put back, so the next poll carries it again instead of dropping it.
    assertThat(coordinator.result(plan).uncovered()).containsEntry("f1", "PENDING");
    assertThat(control.receive(report(3, 1_000, 0)).commands())
        .extracting(IndexCacheProtocol.Command::commandId)
        .containsExactly(command.commandId());

    // After the attempt limit the file is a real failure rather than an endless retry.
    for (long sequence = 4; sequence < 4 + 3; sequence++) {
      control.receive(
          IndexCacheProtocol.Ack.builder()
              .session(SESSION)
              .planId(plan)
              .commandId(command.commandId())
              .executorId(EXECUTOR)
              .incarnation(INCARNATION)
              .epoch(command.epoch())
              .sequence(command.sequence())
              .status(IndexCacheProtocol.AckStatus.REJECTED)
              .bytes(0)
              .reason("BUSY")
              .build());
      control.receive(report(sequence, 1_000, 0));
    }

    assertThat(coordinator.result(plan).uncovered()).containsEntry("f1", "BUSY_EXHAUSTED");
  }

  @Test
  void executorHonoursTheReplyBatchLimit() {
    control.receive(report(1, 10_000, 0));
    String plan =
        coordinator.acceptLoad(
            target("A", "f1", "f2", "f3"), IndexResidencyPolicy.parse("forever"), now);
    List<Object> sent = new ArrayList<>();

    IndexCacheRunner runner = runner(message -> control.receive(message), sent, 2);
    runner.loop.pollOnce();
    for (int i = 0; i < 100 && loaderCalls.get() < 2; i++) {
      sleepQuietly(20);
    }

    // Two commands are executed in this round; the rest wait for the next reply instead of
    // flooding the executor.
    assertThat(loaderCalls.get()).isEqualTo(2);
    assertThat(coordinator.result(plan).uncovered()).isNotEmpty();
  }

  @Test
  void driverRejectsUnknownMessages() {
    assertThatThrownBy(() -> control.receive("not a control message"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsupported control message");
  }

  @Test
  void executorPollsExecutesOnceAndReportsBack() {
    control.receive(report(1, 1_000, 0));
    String plan =
        coordinator.acceptLoad(target("A", "f1"), IndexResidencyPolicy.parse("forever"), now);
    List<Object> sent = new ArrayList<>();

    IndexCacheRunner runner = runner(message -> control.receive(message), sent);
    runner.loop.pollOnce();
    awaitComplete(runner, plan);
    runner.loop.pollOnce();

    assertThat(loaderCalls.get()).isEqualTo(1);
    assertThat(coordinator.result(plan).status()).isEqualTo(IndexCacheProtocol.Status.COMPLETE);
    assertThat(sent)
        .filteredOn(IndexCacheProtocol.NodeReport.class::isInstance)
        .hasSizeGreaterThanOrEqualTo(2);
    assertThat(sent).filteredOn(IndexCacheProtocol.Ack.class::isInstance).hasSize(1);
    assertThat(sent.get(0)).isInstanceOf(IndexCacheProtocol.NodeReport.class);
  }

  @Test
  void executorStopsPollingAfterClose() {
    control.receive(report(1, 1_000, 0));
    List<Object> sent = new ArrayList<>();
    IndexCacheRunner runner = runner(message -> control.receive(message), sent);

    runner.loop.pollOnce();
    int before = sent.size();
    runner.loop.close();
    runner.loop.pollOnce();

    assertThat(sent).hasSize(before);
    assertThat(runner.loop.running()).isFalse();
    assertThat(runner.worker.closed()).isTrue();
  }

  @Test
  void lateCopyAfterOffloadIsReleasedThroughTheChannel() {
    control.receive(report(1, 1_000, 0));
    String plan =
        coordinator.acceptLoad(target("A", "f1"), IndexResidencyPolicy.parse("forever"), now);
    IndexCacheProtocol.Command command = control.receive(report(2, 1_000, 0)).commands().get(0);
    // The driver offloads before the executor answers, so the copy arrives without an owner.
    String offload = coordinator.acceptOffload("A", now);
    assertThat(plan).isNotEmpty();

    List<Object> sent = new ArrayList<>();
    IndexCacheRunner runner = runner(message -> control.receive(message), sent);
    runner.worker.execute(command);
    control.receive(
        IndexCacheProtocol.Ack.builder()
            .session(SESSION)
            .planId(plan)
            .commandId(command.commandId())
            .executorId(EXECUTOR)
            .incarnation(INCARNATION)
            .epoch(command.epoch())
            .sequence(command.sequence())
            .status(IndexCacheProtocol.AckStatus.LOADED)
            .bytes(120)
            .build());
    assertThat(runner.worker.holders()).containsKey("adapter|A|f1");

    // The next poll carries the orphan release, and executing it frees the copy.
    for (int i = 0; i < 100 && !runner.worker.holders().isEmpty(); i++) {
      runner.loop.pollOnce();
      sleepQuietly(20);
    }

    assertThat(runner.worker.holders()).isEmpty();
    assertThat(coordinator.result(offload).status()).isEqualTo(IndexCacheProtocol.Status.OFFLOADED);
  }

  /**
   * Waits until the asynchronous load pool has finished and its acknowledgement reached the driver.
   */
  private void awaitComplete(IndexCacheRunner runner, String plan) {
    for (int i = 0; i < 100; i++) {
      runner.loop.pollOnce();
      if (coordinator.result(plan).status() == IndexCacheProtocol.Status.COMPLETE) {
        return;
      }

      Thread.onSpinWait();
    }
  }

  private static void sleepQuietly(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private IndexCacheRunner runner(IndexCacheControlChannel channel, List<Object> sent) {
    return runner(channel, sent, 64);
  }

  private IndexCacheRunner runner(
      IndexCacheControlChannel channel, List<Object> sent, int maxCommands) {
    ExecutorIndexCacheWorker worker =
        new ExecutorIndexCacheWorker(
            SESSION,
            EXECUTOR,
            INCARNATION,
            cache,
            ExecutorIndexCache.Config.of(true, 1_000_000L, 1_000_000L),
            command -> {
              loaderCalls.incrementAndGet();
              return loaded();
            },
            IndexCacheWorkerConfig.parse(Map.of()));
    IndexCacheExecutorLoop loop =
        new IndexCacheExecutorLoop(
            message -> {
              sent.add(message);
              return channel.ask(message);
            },
            worker,
            IndexCacheWorkerConfig.parse(
                Map.of(
                    "spark.iceberg.index.cache.control.max-commands", String.valueOf(maxCommands))),
            1_000L);
    return new IndexCacheRunner(worker, loop);
  }

  private record IndexCacheRunner(ExecutorIndexCacheWorker worker, IndexCacheExecutorLoop loop) {}

  private IndexCacheProtocol.NodeReport report(long sequence, long budget, long used) {
    return IndexCacheProtocol.NodeReport.builder()
        .session(SESSION)
        .executorId(EXECUTOR)
        .incarnation(INCARNATION)
        .sequence(sequence)
        .budgetBytes(budget)
        .usedBytes(used)
        .build();
  }

  private IndexCacheProtocol.Target target(String indexUuid, String... locations) {
    return IndexCacheProtocol.Target.builder()
        .indexUuid(indexUuid)
        .snapshotId(1)
        .files(
            java.util.Arrays.stream(locations)
                .map(
                    location ->
                        IndexCacheProtocol.File.builder()
                            .location(location)
                            .cacheKey("adapter|" + indexUuid + "|" + location)
                            .estimatedBytes(120L)
                            .memory(MEMORY)
                            .build())
                .toList())
        .definition(DEFINITION)
        .io(new HadoopFileIO(new Configuration()))
        .build();
  }

  private static ExecutorIndexCache.CachedFile loaded() {
    return ExecutorIndexCache.loaded(
        new byte[] {1, 2, 3},
        new IndexAdapter.PreparedSearch() {
          @Override
          public List<IndexSearchResult> search(ByteBuffer query, int topK) {
            return List.of();
          }

          @Override
          public long estimatedBytes() {
            return 40L;
          }
        },
        120L);
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
