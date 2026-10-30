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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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

class TestIndexCacheOwnedProtocol {
  @BeforeAll
  static void registerTypes() {
    IndexCacheTypeRegistration.registerAll();
  }

  private final ExecutorIndexCache cache = ExecutorIndexCache.get();
  private final AtomicInteger loads = new AtomicInteger();
  private final VectorIndexAdapter adapter =
      new VectorIndexAdapter(
          VectorIndexDefinition.builder()
              .fieldId(1)
              .dimension(2)
              .metric(Metric.EUCLIDEAN)
              .structure(VectorStructure.FLAT)
              .build());
  private ExecutorIndexCacheWorker worker;

  @BeforeEach
  void setUp() {
    cache.clear();
    worker =
        new ExecutorIndexCacheWorker(
            "app",
            "node",
            "node-1",
            cache,
            ExecutorIndexCache.Config.of(true, 100_000),
            command -> {
              loads.incrementAndGet();
              return ExecutorIndexCache.loaded(0, adapter.prepare(adapter.encodeEntries(List.of())))
                  .withContext(adapter);
            },
            IndexCacheWorkerConfig.parse(Map.of()));
  }

  @AfterEach
  void tearDown() {
    worker.close();
    cache.clear();
  }

  @Test
  void committedShapeMatchesNodeProjectionAndSerializesWithoutPreparedState() throws Exception {
    IndexCacheProtocol.Target target = target("index", "table", "first", "second");
    ExecutorIndexCache.Shape predicted =
        ExecutorIndexCache.Shape.empty().project("index", 1, target.files());
    IndexResidencyCoordinator coordinator = new IndexResidencyCoordinator(() -> 1L);
    coordinator.report(worker.report("profile", 1, 100_000, 0, 0, Map.of()));
    String plan = coordinator.acceptLoad(target, IndexResidencyPolicy.parse("0"), 1);
    for (IndexCacheProtocol.Command command : coordinator.commands("node", "node-1")) {
      coordinator.ack(worker.execute(command));
    }
    ExecutorIndexCache.Shape actual = cache.shape();
    assertThat(actual.charge().known()).isTrue();
    assertThat(actual.charge().usedBytes()).isEqualTo(predicted.charge().usedBytes());
    assertThat(actual.charge().residentBytes()).isEqualTo(predicted.charge().residentBytes());
    assertThat(coordinator.result(plan).status()).isEqualTo(IndexCacheProtocol.Status.COMPLETE);
    assertThat(coordinator.result(plan).residentBytes()).isEqualTo(actual.charge().residentBytes());
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
      output.writeObject(actual);
    }
    ExecutorIndexCache.Shape copy;
    try (ObjectInputStream input =
        new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      copy = (ExecutorIndexCache.Shape) input.readObject();
    }
    assertThat(copy.project("another", 1, target.files()).charge().usedBytes())
        .isEqualTo(actual.project("another", 1, target.files()).charge().usedBytes());
    assertThat(copy.residentBytes(copy.holders().keySet()))
        .isEqualTo(actual.charge().residentBytes());
  }

  @Test
  void olderAndDuplicateAckCannotRollbackCommittedCapacityOrGrantExtraCoverage() {
    IndexResidencyCoordinator coordinator = new IndexResidencyCoordinator(() -> 1L);
    IndexCacheProtocol.NodeReport old = worker.report("profile", 1, 100_000, 0, 0, Map.of());
    coordinator.report(old);
    String plan =
        coordinator.acceptLoad(
            target("index", "table", "first", "second"), IndexResidencyPolicy.parse("0"), 1);
    List<IndexCacheProtocol.Command> commands = coordinator.commands("node", "node-1");
    assertThat(commands).hasSize(2);
    IndexCacheProtocol.Ack first = worker.execute(commands.get(0));
    IndexCacheProtocol.Ack second = worker.execute(commands.get(1));
    coordinator.ack(second);
    long used = coordinator.state("node", "node-1").usedBytes();
    assertThat(coordinator.result(plan).status()).isEqualTo(IndexCacheProtocol.Status.PARTIAL);
    coordinator.ack(first);
    coordinator.ack(first);
    coordinator.report(old.copy().sequence(100).build());
    assertThat(coordinator.state("node", "node-1").usedBytes()).isEqualTo(used);
    assertThat(coordinator.state("node", "node-1").metadataShape().charge().version())
        .isEqualTo(second.chargeVersion());
    assertThat(coordinator.result(plan).status()).isEqualTo(IndexCacheProtocol.Status.COMPLETE);
    assertThat(loads.get()).isEqualTo(2);
  }

  @Test
  void existingFileStillNeedsBudgetAndAckForAnotherIndexOwner() {
    IndexResidencyCoordinator coordinator = new IndexResidencyCoordinator(() -> 1L);
    coordinator.report(worker.report("profile", 1, 100_000, 0, 0, Map.of()));
    IndexCacheProtocol.Target first = target("owner-a", "table", "file");
    String plan = coordinator.acceptLoad(first, IndexResidencyPolicy.parse("0"), 1);
    IndexCacheProtocol.Command command = coordinator.commands("node", "node-1").get(0);
    coordinator.ack(worker.execute(command));
    assertThat(coordinator.result(plan).status()).isEqualTo(IndexCacheProtocol.Status.COMPLETE);
    long used = cache.chargeSnapshot().usedBytes();
    IndexCacheProtocol.Target another = target("owner-b", "table", "file");
    IndexPlacementPlanner planner = new IndexPlacementPlanner();
    IndexCacheProtocol.Plan refused =
        planner.plan(another, List.of(worker.report("profile", 2, used, 0, 0, Map.of())));
    assertThat(refused.placements()).isEmpty();
    assertThat(refused.rejected()).containsEntry("file", "INSUFFICIENT_CAPACITY");
    coordinator.report(worker.report("profile", 2, 100_000, 0, 0, Map.of()));
    String next = coordinator.acceptLoad(another, IndexResidencyPolicy.parse("0"), 1);
    assertThat(coordinator.result(next).status()).isNotEqualTo(IndexCacheProtocol.Status.COMPLETE);
    IndexCacheProtocol.Command shared =
        coordinator.commands("node", "node-1").stream()
            .filter(candidate -> candidate.planId().equals(next))
            .findFirst()
            .orElseThrow();
    coordinator.ack(worker.execute(shared));
    assertThat(loads.get()).isEqualTo(1);
    assertThat(cache.chargeSnapshot().groupRefs()).isEqualTo(2);
    assertThat(coordinator.result(next).status()).isEqualTo(IndexCacheProtocol.Status.COMPLETE);
    ExecutorIndexCache.Shape before = cache.shape();
    ExecutorIndexCache.Shape released =
        before.releaseKeys("owner-a", Set.of(command.file().cacheKey()));
    assertThat(released.charge().residentBytes()).isEqualTo(before.charge().residentBytes());
    assertThat(before.charge().groupRefs()).isEqualTo(2);
  }

  @Test
  void releasedKeyCannotBeReadmittedByALateCommandAtTheSameEpoch() {
    IndexResidencyCoordinator coordinator = new IndexResidencyCoordinator(() -> 1L);
    coordinator.report(worker.report("profile", 1, 100_000, 0, 0, Map.of()));
    String plan =
        coordinator.acceptLoad(
            target("index", "table", "file"), IndexResidencyPolicy.parse("0"), 1);
    IndexCacheProtocol.Command original = coordinator.commands("node", "node-1").get(0);
    coordinator.ack(worker.execute(original));
    cache.releaseCopy("index", original.file().cacheKey());
    assertThat(cache.stats().bytes()).isZero();
    IndexCacheProtocol.Command late =
        IndexCacheProtocol.Command.builder()
            .session(original.session())
            .executorId(original.executorId())
            .incarnation(original.incarnation())
            .indexUuid(original.indexUuid())
            .epoch(original.epoch())
            .planId(plan)
            .commandId("late-load")
            .type(IndexCacheProtocol.CommandType.LOAD)
            .file(original.file())
            .definition(original.definition())
            .io(original.io())
            .sequence(original.sequence() + 1)
            .build();
    assertThat(worker.execute(late).status()).isEqualTo(IndexCacheProtocol.AckStatus.REJECTED);
    assertThat(cache.stats().bytes()).isZero();
    assertThat(loads.get()).isEqualTo(1);
  }

  @Test
  void shrinkingTargetAndLastLeaseReleaseCannotBeRejectedByALoweredBudget() {
    IndexCacheProtocol.File file = target("index", "table", "file").files().get(0);
    cache.applyTarget("index", 1, Set.of(file.cacheKey()));
    cache.installManaged(
        "index",
        1,
        file.cacheKey(),
        ExecutorIndexCache.Config.of(true, 100_000).withOwner("table"),
        () ->
            ExecutorIndexCache.loaded(0, adapter.prepare(adapter.encodeEntries(List.of())))
                .withContext(adapter));
    try (ExecutorIndexCache.Lease lease = cache.acquireManaged("index", 1, file.cacheKey())) {
      assertThat(lease).isNotNull();
      long before = cache.chargeSnapshot().usedBytes();
      cache.configureBudget(1);
      cache.applyTarget("index", 2, Set.of());
      assertThat(cache.managedEpoch("index")).isEqualTo(2);
      assertThat(cache.chargeSnapshot().usedBytes()).isLessThanOrEqualTo(before);
      cache.addManagedKeys("index", 2, Set.of("large".repeat(1_000)));
      assertThat(cache.managedKeys("index")).isEmpty();
      cache.releaseIndex("index", 3);
      assertThat(cache.chargeSnapshot().residentBytes()).isPositive();
    }
    assertThat(cache.stats().bytes()).isZero();
    assertThat(cache.chargeSnapshot().controlBytes()).isPositive();
    cache.clear();
    assertThat(cache.chargeSnapshot().usedBytes()).isZero();
  }

  @Test
  void ownerPartitionDeduplicatesSharedRootsAndOnlyReplacesItsOwnPromise() throws Exception {
    IndexAdapter.PreparedSearch shared = adapter.prepare(adapter.encodeEntries(List.of()));
    ExecutorIndexCache.Config config = ExecutorIndexCache.Config.of(true, 100_000);
    for (String owner : List.of("A", "B")) {
      cache.file(
          "file",
          config.withOwner(owner),
          "reader",
          () -> ExecutorIndexCache.loaded(0, shared).withContext(adapter));
    }
    AtomicReference<Map<String, Long>> before = new AtomicReference<>();
    cache.file(
        "file",
        config.withOwner("C"),
        "reader",
        20_000,
        () -> {
          try {
            before.set(ownerPartition());
            long total = before.get().values().stream().mapToLong(Long::longValue).sum();
            ExecutorIndexCache.ChargeSnapshot charge = cache.chargeSnapshot();
            assertThat(total + charge.controlBytes())
                .isEqualTo(charge.usedBytes() + charge.reservedBytes());
            assertThat(charge.reservedBytes()).isPositive();
          } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
          }
          return ExecutorIndexCache.loaded(0, shared).withContext(adapter);
        });
    Map<String, Long> after = ownerPartition();
    assertThat(after.get("A")).isEqualTo(before.get().get("A"));
    assertThat(after.get("B")).isEqualTo(before.get().get("B"));
    assertThat(after.get("C")).isLessThan(before.get().get("C"));
    assertThat(
            after.values().stream().mapToLong(Long::longValue).sum()
                + cache.chargeSnapshot().controlBytes())
        .isEqualTo(cache.chargeSnapshot().usedBytes());
  }

  @SuppressWarnings("unchecked")
  private Map<String, Long> ownerPartition() throws ReflectiveOperationException {
    Method method = ExecutorIndexCache.class.getDeclaredMethod("ownerCharges");
    method.setAccessible(true);
    Object charges = method.invoke(cache);
    Method values = charges.getClass().getDeclaredMethod("effectiveBytes");
    values.setAccessible(true);
    return (Map<String, Long>) values.invoke(charges);
  }

  @Test
  void offloadResultKeepsTheLeasedResidentUnionUntilTheNextReleasedSnapshot() {
    IndexResidencyCoordinator coordinator = new IndexResidencyCoordinator(() -> 1L);
    coordinator.report(worker.report("profile", 1, 100_000, 0, 0, Map.of()));
    String plan =
        coordinator.acceptLoad(
            target("index", "table", "file"), IndexResidencyPolicy.parse("0"), 1);
    IndexCacheProtocol.Command load = coordinator.commands("node", "node-1").get(0);
    coordinator.ack(worker.execute(load));
    String offload;
    try (ExecutorIndexCache.Lease lease =
        cache.acquireManaged("index", 1, load.file().cacheKey())) {
      assertThat(lease).isNotNull();
      offload = coordinator.acceptOffload("index", 1);
      for (IndexCacheProtocol.Command command : coordinator.commands("node", "node-1")) {
        if (command.planId().equals(offload)) {
          coordinator.ack(worker.execute(command));
        }
      }
      assertThat(coordinator.result(offload).residentBytes())
          .isEqualTo(cache.stats().bytes())
          .isPositive();
    }
    assertThat(cache.stats().bytes()).isZero();
    coordinator.report(worker.report("profile", 2, 100_000, 0, 0, Map.of()));
    assertThat(coordinator.result(offload).residentBytes()).isZero();
    assertThat(coordinator.state("node", "node-1").usedBytes()).isPositive();
  }

  @Test
  void anAnswerWithoutChargeCountersLeavesCapacityUnknownInsteadOfFree() {
    IndexResidencyCoordinator coordinator = new IndexResidencyCoordinator(() -> 1L);
    coordinator.report(worker.report("profile", 1, 100_000, 0, 0, Map.of()));
    String plan =
        coordinator.acceptLoad(
            target("index", "table", "file"), IndexResidencyPolicy.parse("0"), 1);
    IndexCacheProtocol.Command command = coordinator.commands("node", "node-1").get(0);
    // An answer that states no counters describes no charge state, so the driver cannot project it.
    coordinator.ack(worker.execute(command).copy().chargeVersion(0).controlBytes(0).build());
    assertThat(coordinator.result(plan).status()).isEqualTo(IndexCacheProtocol.Status.COMPLETE);
    // The copy is confirmed, but a state nobody described is not free space.
    assertThat(coordinator.result(plan).residentBytes()).isEqualTo(-1);
    assertThat(coordinator.state("node", "node-1").metadataShape().charge().known()).isFalse();
    assertThat(coordinator.state("node", "node-1").usedBytes()).isPositive();
    coordinator.report(worker.report("profile", 2, 100_000, 0, 0, Map.of()));
    assertThat(coordinator.state("node", "node-1").metadataShape().charge().known()).isTrue();
    assertThat(coordinator.result(plan).residentBytes())
        .isEqualTo(cache.stats().bytes())
        .isPositive();
  }

  @Test
  void plannerDoesNotTreatMissingShapeAsFreeSpace() {
    IndexCacheProtocol.NodeReport unknown =
        IndexCacheProtocol.NodeReport.builder()
            .session("app")
            .executorId("node")
            .incarnation("node-1")
            .budgetBytes(100_000)
            .build();
    IndexCacheProtocol.Plan plan =
        new IndexPlacementPlanner().plan(target("index", "table", "file"), List.of(unknown));
    assertThat(plan.placements()).isEmpty();
    assertThat(plan.rejected()).containsEntry("file", "SIZE_UNKNOWN");
  }

  private IndexCacheProtocol.Target target(String index, String owner, String... locations) {
    IndexFileMemory memory = IndexCacheMemory.currentDescriptor(adapter);
    List<IndexCacheProtocol.File> files =
        Arrays.stream(locations)
            .map(
                location ->
                    IndexCacheProtocol.File.builder()
                        .location(location)
                        .owner(owner)
                        .cacheKey(
                            DistributedIndexSearch.cacheKey(
                                owner, DistributedIndexSearch.adapterKey(adapter), location))
                        .memory(memory)
                        .estimatedBytes(IndexCacheMemory.residentBytes(memory))
                        .build())
            .toList();
    return IndexCacheProtocol.Target.builder()
        .indexUuid(index)
        .owner(owner)
        .files(files)
        .definition(adapter.definition())
        .io(new HadoopFileIO(new Configuration()))
        .build();
  }
}
