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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.index.IndexAdapter;
import org.apache.iceberg.index.IndexFileMemory;
import org.apache.iceberg.index.vector.VectorIndexAdapter;
import org.apache.iceberg.index.vector.VectorIndexDefinition;
import org.apache.iceberg.index.vector.VectorIndexDefinition.Metric;
import org.apache.iceberg.index.vector.VectorIndexDefinition.VectorStructure;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
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
  private static final String INDEX_UUID = "index-1";
  // The owner a query plans with is the table location, and its cache key carries that owner.
  private static final String OWNER = "table-1";
  // An executor reports the budget its cache is configured with, so both sides agree.
  private static final long CACHE_BYTES = 1_000_000L;

  private final ExecutorIndexCache cache = ExecutorIndexCache.get();
  private final VectorIndexAdapter adapter = new VectorIndexAdapter(definition());
  private final AtomicInteger loaderCalls = new AtomicInteger();
  private long now = Duration.ofMinutes(5).toNanos();
  private IndexFileMemory memory;
  private long estimate;
  private IndexResidencyCoordinator coordinator;
  private IndexCacheDriverControl control;

  @BeforeAll
  static void registerTypes() {
    IndexCacheTypeRegistration.registerAll();
  }

  @BeforeEach
  void setUp() {
    cache.clear();
    // The real memory model of the reader that will hold the copy, not a hand-written descriptor.
    memory = IndexCacheMemory.currentDescriptor(adapter);
    assertThat(memory).as("current index memory descriptor").isNotNull();
    estimate = IndexCacheMemory.residentBytes(memory);
    coordinator = new IndexResidencyCoordinator(() -> now);
    control = new IndexCacheDriverControl(coordinator);
  }

  @AfterEach
  void tearDown() {
    cache.clear();
  }

  @Test
  void driverAnswersAReportWithThePlannedCommands() {
    control.receive(report(1));

    String plan = coordinator.acceptLoad(target("f1"), IndexResidencyPolicy.parse("forever"), now);
    IndexCacheProtocol.Reply reply = control.receive(report(2));

    assertThat(reply.commands()).hasSize(1);
    assertThat(reply.commands().get(0).type()).isEqualTo(IndexCacheProtocol.CommandType.LOAD);
    assertThat(reply.commands().get(0).planId()).isEqualTo(plan);
    assertThat(reply.commands().get(0).file().location()).isEqualTo("f1");
  }

  @Test
  void driverAppliesAcknowledgements() {
    control.receive(report(1));
    String plan = coordinator.acceptLoad(target("f1"), IndexResidencyPolicy.parse("forever"), now);
    IndexCacheProtocol.Command command = control.receive(report(2)).commands().get(0);
    IndexCacheProtocol.Ack loaded = loadedAck(command);

    control.receive(loaded);

    IndexCacheProtocol.Result result = coordinator.result(plan);
    assertThat(result.status()).isEqualTo(IndexCacheProtocol.Status.COMPLETE);
    // Resident bytes come back from the executor's shape, not from the size the command carried.
    assertThat(result.residentBytes())
        .isEqualTo(loaded.metadataShape().residentBytes(Set.of(command.file().cacheKey())))
        .isPositive();
  }

  @Test
  void aReplyCarriesOnlyOneBatchAndTheRestFollowsLater() {
    IndexResidencyCoordinator bounded = new IndexResidencyCoordinator(() -> now, 2);
    IndexCacheDriverControl boundedControl = new IndexCacheDriverControl(bounded);
    boundedControl.receive(report(1));
    bounded.acceptLoad(target("f1", "f2", "f3"), IndexResidencyPolicy.parse("forever"), now);

    IndexCacheProtocol.Reply first = boundedControl.receive(report(2));

    assertThat(first.commands()).hasSize(2);
    assertThat(first.commands().get(0).definition()).isNotNull();
    assertThat(first.commands().get(0).io()).isNotNull();
  }

  @Test
  void aFailedAcknowledgementRoundTripIsRetriedInsteadOfDropped() {
    control.receive(report(1));
    String plan = coordinator.acceptLoad(target("f1"), IndexResidencyPolicy.parse("forever"), now);
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
    control.receive(report(1));
    String plan = coordinator.acceptLoad(target("f1"), IndexResidencyPolicy.parse("forever"), now);
    IndexCacheProtocol.Command command = control.receive(report(2)).commands().get(0);

    control.receive(rejected(command, "BUSY"));

    // The command is put back, so the next poll carries it again instead of dropping it.
    assertThat(coordinator.result(plan).uncovered()).containsEntry("f1", "PENDING");
    assertThat(control.receive(report(3)).commands())
        .extracting(IndexCacheProtocol.Command::commandId)
        .containsExactly(command.commandId());

    // After the attempt limit the file is a real failure rather than an endless retry.
    for (long sequence = 4; sequence < 4 + 3; sequence++) {
      control.receive(rejected(command, "BUSY"));
      control.receive(report(sequence));
    }

    assertThat(coordinator.result(plan).uncovered()).containsEntry("f1", "BUSY_EXHAUSTED");
  }

  @Test
  void executorHonoursTheReplyBatchLimit() {
    control.receive(report(1));
    String plan =
        coordinator.acceptLoad(
            target("f1", "f2", "f3"), IndexResidencyPolicy.parse("forever"), now);
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
    control.receive(report(1));
    String plan = coordinator.acceptLoad(target("f1"), IndexResidencyPolicy.parse("forever"), now);
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
    control.receive(report(1));
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
    control.receive(report(1));
    String plan = coordinator.acceptLoad(target("f1"), IndexResidencyPolicy.parse("forever"), now);
    IndexCacheProtocol.Command command = control.receive(report(2)).commands().get(0);
    // The driver offloads before the executor answers, so the copy arrives without an owner.
    String offload = coordinator.acceptOffload(INDEX_UUID, now);
    assertThat(plan).isNotEmpty();

    List<Object> sent = new ArrayList<>();
    IndexCacheRunner runner = runner(message -> control.receive(message), sent);
    // The executor's own acknowledgement arrives late, after the driver withdrew the target.
    control.receive(runner.worker.execute(command));
    assertThat(runner.worker.holders()).containsKey(command.file().cacheKey());

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
            ExecutorIndexCache.Config.of(true, CACHE_BYTES),
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
            CACHE_BYTES);
    return new IndexCacheRunner(worker, loop);
  }

  private record IndexCacheRunner(ExecutorIndexCacheWorker worker, IndexCacheExecutorLoop loop) {}

  @Test
  void controlReportIncludesActualRuntimeIdentity() {
    List<Object> sent = new ArrayList<>();
    IndexCacheRunner runner =
        runner(message -> IndexCacheProtocol.Reply.builder().commands(List.of()).build(), sent);
    try {
      runner.loop.pollOnce();
      assertThat(sent).hasSize(1);
      IndexCacheProtocol.NodeReport report = (IndexCacheProtocol.NodeReport) sent.get(0);
      assertThat(report.profile())
          .contains(System.getProperty("java.vm.name"))
          .contains("processors=" + Runtime.getRuntime().availableProcessors());
    } finally {
      runner.loop.close();
    }
  }

  private static IndexCacheProtocol.NodeReport report(long sequence) {
    return IndexCacheProtocol.NodeReport.builder()
        .session(SESSION)
        .executorId(EXECUTOR)
        .incarnation(INCARNATION)
        .sequence(sequence)
        .budgetBytes(CACHE_BYTES)
        // A report without the capacity shape is planned as unknown capacity, so it plans nothing.
        .metadataShape(ExecutorIndexCache.Shape.empty())
        .build();
  }

  /**
   * The acknowledgement an executor that loaded this command would send: its own capacity shape,
   * with the copy's resident size read back from that shape.
   */
  private IndexCacheProtocol.Ack loadedAck(IndexCacheProtocol.Command command) {
    // A synthetic ACK must carry a newer shape version than the report it follows. The executor's
    // cache increments its snapshot version on each committed state; do the same here.
    ExecutorIndexCache.get().chargeSnapshot();
    ExecutorIndexCache.Shape shape =
        ExecutorIndexCache.get()
            .shape()
            .project(INDEX_UUID, command.epoch(), List.of(command.file()));
    return IndexCacheProtocol.Ack.builder()
        .session(SESSION)
        .planId(command.planId())
        .commandId(command.commandId())
        .executorId(EXECUTOR)
        .incarnation(INCARNATION)
        .epoch(command.epoch())
        .sequence(command.sequence())
        .status(IndexCacheProtocol.AckStatus.LOADED)
        .bytes(shape.residentBytes(Set.of(command.file().cacheKey())))
        .metadataShape(shape)
        .build();
  }

  /** A refusal carries no new capacity, so it reports no shape. */
  private static IndexCacheProtocol.Ack rejected(
      IndexCacheProtocol.Command command, String reason) {
    return IndexCacheProtocol.Ack.builder()
        .session(SESSION)
        .planId(command.planId())
        .commandId(command.commandId())
        .executorId(EXECUTOR)
        .incarnation(INCARNATION)
        .epoch(command.epoch())
        .sequence(command.sequence())
        .status(IndexCacheProtocol.AckStatus.REJECTED)
        .bytes(0)
        .reason(reason)
        .build();
  }

  private IndexCacheProtocol.Target target(String... locations) {
    return IndexCacheProtocol.Target.builder()
        .indexUuid(INDEX_UUID)
        .owner(OWNER)
        .snapshotId(1)
        .files(Arrays.stream(locations).map(this::file).toList())
        .definition(adapter.definition())
        .io(new HadoopFileIO(new Configuration()))
        .build();
  }

  private IndexCacheProtocol.File file(String location) {
    return IndexCacheProtocol.File.builder()
        .location(location)
        .owner(OWNER)
        .cacheKey(cacheKey(location))
        .estimatedBytes(estimate)
        .memory(memory)
        .build();
  }

  private String cacheKey(String location) {
    return DistributedIndexSearch.cacheKey(
        OWNER, DistributedIndexSearch.adapterKey(adapter), location);
  }

  /** The prepared copy the read path publishes, stamped with the reader that prepared it. */
  private ExecutorIndexCache.CachedFile loaded() {
    IndexAdapter.PreparedSearch prepared = adapter.prepare(adapter.encodeEntries(List.of()));
    return ExecutorIndexCache.loaded(new byte[] {1, 2, 3}, prepared).withContext(adapter);
  }

  private static VectorIndexDefinition definition() {
    return VectorIndexDefinition.builder()
        .fieldId(1)
        .dimension(2)
        .metric(Metric.COSINE)
        .structure(VectorStructure.FLAT)
        .build();
  }
}
