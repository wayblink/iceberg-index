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

import java.time.Duration;
import org.junit.jupiter.api.Test;

class TestIndexResidencyCoordinator {
  private static final long SECOND = Duration.ofSeconds(1).toNanos();
  private static final long BASE = 1_000 * SECOND;
  private final FakeClock clock = new FakeClock(BASE);
  private final IndexResidencyCoordinator coordinator = new IndexResidencyCoordinator(clock);

  @Test
  void firstLoadStartsProtectionAndRepeatKeepsTheDeadline() {
    coordinator.report(node("a", "a-1", 1, 100, 0));
    coordinator.acceptLoad(target("A", "f1", 10), IndexResidencyPolicy.parse("30m"), clock.now());
    clock.advance(10 * SECOND);
    coordinator.acceptLoad(target("A", "f1", 10), null, clock.now());
    assertThat(coordinator.result(coordinator.lastPlanId()).protection().deadlineNanos())
        .isEqualTo(BASE + 30 * 60 * SECOND);
  }

  @Test
  void explicitRetentionRestartsProtectionButAccessDoesNot() {
    coordinator.report(node("a", "a-1", 1, 100, 0));
    String plan =
        coordinator.acceptLoad(
            target("A", "f1", 10), IndexResidencyPolicy.parse("1h"), clock.now());
    clock.advance(30 * 60 * SECOND);
    coordinator.report(withAccess(node("a", "a-1", 2, 100, 0), "A", 5));
    assertThat(coordinator.result(plan).protection().deadlineNanos())
        .isEqualTo(BASE + 60 * 60 * SECOND);
    coordinator.acceptLoad(target("A", "f1", 10), IndexResidencyPolicy.parse("10m"), clock.now());
    assertThat(coordinator.result(coordinator.lastPlanId()).protection().deadlineNanos())
        .isEqualTo(clock.now() + 10 * 60 * SECOND);
  }

  @Test
  void permanentProtectionNeverExpires() {
    coordinator.report(node("a", "a-1", 1, 100, 0));
    String plan =
        coordinator.acceptLoad(
            target("A", "f1", 10), IndexResidencyPolicy.parse("forever"), clock.now());
    clock.advance(365 * 24 * 60 * 60 * SECOND);
    assertThat(coordinator.result(plan).protection().permanent()).isTrue();
    assertThat(coordinator.expiredProtections()).isEmpty();
  }

  @Test
  void expiryAloneDoesNotOffload() {
    coordinator.report(node("a", "a-1", 1, 100, 0));
    coordinator.acceptLoad(target("A", "f1", 10), IndexResidencyPolicy.parse("0"), clock.now());
    clock.advance(60 * SECOND);
    assertThat(coordinator.commands("a", "a-1"))
        .extracting(IndexCacheProtocol.Command::type)
        .containsExactly(IndexCacheProtocol.CommandType.LOAD);
  }

  @Test
  void lruUsesIndexAccessAgeRatherThanReportArrival() {
    coordinator.report(node("a", "a-1", 1, 100, 0));
    coordinator.acceptLoad(target("old", "o1", 30), IndexResidencyPolicy.parse("0"), clock.now());
    ackNode("a", "a-1");
    coordinator.report(node("a", "a-1", 2, 100, 30));
    coordinator.acceptLoad(target("hot", "h1", 30), IndexResidencyPolicy.parse("0"), clock.now());
    ackNode("a", "a-1");
    coordinator.report(node("a", "a-1", 3, 100, 60));
    clock.advance(10 * SECOND);
    coordinator.report(withAccess(node("a", "a-1", 3, 100, 60), "hot", 9));
    String plan =
        coordinator.acceptLoad(
            target("new", "n1", 30), IndexResidencyPolicy.parse("0"), clock.now());
    assertThat(coordinator.result(plan).evictedIndexes()).containsExactly("old");
  }

  @Test
  void protectedAndPermanentIndexesAreNotEvicted() {
    coordinator.report(node("a", "a-1", 1, 100, 0));
    coordinator.acceptLoad(
        target("pinned", "p1", 40), IndexResidencyPolicy.parse("1h"), clock.now());
    ackNode("a", "a-1");
    coordinator.report(node("a", "a-1", 2, 100, 40));
    coordinator.acceptLoad(
        target("forever", "e1", 40), IndexResidencyPolicy.parse("forever"), clock.now());
    ackNode("a", "a-1");
    coordinator.report(node("a", "a-1", 3, 100, 80));
    String plan =
        coordinator.acceptLoad(
            target("new", "n1", 30), IndexResidencyPolicy.parse("0"), clock.now());
    assertThat(coordinator.result(plan).evictedIndexes()).isEmpty();
    assertThat(coordinator.result(plan).uncovered()).containsKey("n1");
  }

  @Test
  void offloadPreemptionDoesNotWaitForAnotherIndexPlan() {
    coordinator.report(node("a", "a-1", 1, 100, 0));
    coordinator.acceptLoad(
        target("A", "f1", 10), IndexResidencyPolicy.parse("forever"), clock.now());
    ackNode("a", "a-1");
    coordinator.report(node("a", "a-1", 2, 100, 10));
    coordinator.acceptLoad(
        target("B", "g1", 10), IndexResidencyPolicy.parse("forever"), clock.now());
    ackNode("a", "a-1");
    coordinator.report(node("a", "a-1", 3, 100, 20));
    String plan = coordinator.acceptOffload("A", clock.now());
    assertThat(coordinator.result(plan).status()).isEqualTo(IndexCacheProtocol.Status.OFFLOADED);
    assertThat(coordinator.commands("a", "a-1"))
        .filteredOn(c -> c.type() == IndexCacheProtocol.CommandType.RELEASE)
        .isNotEmpty();
  }

  @Test
  void snapshotReplacementReusesSharedFilesAndWithdrawsOldOnes() {
    coordinator.report(node("a", "a-1", 1, 100, 0));
    coordinator.acceptLoad(
        target("A", "a-old", 20, "B", 20), IndexResidencyPolicy.parse("forever"), clock.now());
    ackNode("a", "a-1");
    coordinator.report(node("a", "a-1", 2, 100, 40));
    String plan =
        coordinator.acceptLoad(
            target("A", "B", 20, "C", 20), IndexResidencyPolicy.parse("forever"), clock.now());
    assertThat(coordinator.result(plan).targetFiles()).containsExactly("B", "C");
    // The shared B keeps its one confirmed copy; the next plan loads only what is new.
    assertThat(coordinator.pendingCommands("a", "a-1"))
        .flatExtracting(IndexCacheProtocol.Plan::files)
        .extracting(IndexCacheProtocol.File::location)
        .contains("C");
    assertThat(coordinator.result(plan).reusedFiles()).contains("B");
    assertThat(coordinator.commands("a", "a-1"))
        .anyMatch(
            c ->
                c.type() == IndexCacheProtocol.CommandType.RELEASE
                    && "a-old".equals(c.file().location()));
  }

  @Test
  void oldEpochAndIncarnationCannotChangeCoverage() {
    coordinator.report(node("a", "a-1", 5, 100, 0));
    String plan =
        coordinator.acceptLoad(target("A", "f1", 10), IndexResidencyPolicy.parse("0"), clock.now());
    IndexCacheProtocol.Command command = loadCommand(plan);
    coordinator.report(node("a", "a-2", 6, 100, 0));
    // A stale report still claiming the old incarnation must not confirm the command.
    coordinator.ack(ack(plan, "a", "a-1", command, IndexCacheProtocol.AckStatus.LOADED));
    assertThat(coordinator.result(plan).status()).isEqualTo(IndexCacheProtocol.Status.PENDING);
    coordinator.report(node("a", "a-1", 7, 100, 0));
    coordinator.ack(ack(plan, "a", "a-1", command, IndexCacheProtocol.AckStatus.LOADED));
    coordinator.ack(ack(plan, "a", "a-1", command, IndexCacheProtocol.AckStatus.LOADED));
    assertThat(coordinator.result(plan).status()).isEqualTo(IndexCacheProtocol.Status.COMPLETE);
  }

  @Test
  void staleNodeReportIsNotSafeSpace() {
    coordinator.report(node("a", "a-1", 1, 100, 0));
    String plan =
        coordinator.acceptLoad(target("A", "f1", 10), IndexResidencyPolicy.parse("0"), clock.now());
    clock.advance(5 * SECOND);
    coordinator.report(node("a", "a-1", 2, 100, 0));
    clock.advance(11 * SECOND);
    coordinator.refresh(clock.now());
    // The command was never issued anywhere; the node left before it could answer.
    assertThat(coordinator.result(plan).status()).isEqualTo(IndexCacheProtocol.Status.FAILED);
  }

  @Test
  void timeoutKeepsSuccessAndFencesOtherFiles() {
    coordinator.report(node("a", "a-1", 1, 100, 0));
    String plan =
        coordinator.acceptLoad(
            target("A", "f1", 10, "f2", 10), IndexResidencyPolicy.parse("0"), clock.now());
    ackFile(plan, "f1", IndexCacheProtocol.AckStatus.LOADED);
    clock.advance(Duration.ofMinutes(1).toNanos());
    coordinator.expire(plan, clock.now());
    ackFile(plan, "f2", IndexCacheProtocol.AckStatus.LOADED);
    // Keep the node report fresh: only unreported nodes are treated as lost.
    coordinator.report(node("a", "a-1", 2, 100, 10));
    IndexCacheProtocol.Result result = coordinator.result(plan);
    assertThat(result.status()).isEqualTo(IndexCacheProtocol.Status.PARTIAL);
    assertThat(result.uncovered()).containsOnlyKeys("f2");
    assertThat(result.residentBytes()).isEqualTo(10);
    assertThat(result.evictedIndexes()).isEmpty();
  }

  @Test
  void concurrentLoadsQueueRatherThanReserveTwice() {
    coordinator.report(node("a", "a-1", 1, 40, 0));
    String first =
        coordinator.acceptLoad(target("A", "f1", 30), IndexResidencyPolicy.parse("0"), clock.now());
    // The first plan's space is reserved; the report only keeps the node current.
    coordinator.report(node("a", "a-1", 2, 40, 0));
    assertThat(coordinator.result(first).status()).isEqualTo(IndexCacheProtocol.Status.PENDING);
    String second =
        coordinator.acceptLoad(target("B", "g1", 30), IndexResidencyPolicy.parse("0"), clock.now());
    // The second plan may be materialized, but it cannot commit with the first plan's space.
    assertThat(coordinator.result(second).status()).isEqualTo(IndexCacheProtocol.Status.QUEUED);
    assertThat(coordinator.result(second).uncovered()).containsKey("g1");
    // Only the first plan's file got a command; the second waits for that space instead of
    // reserving it twice.
    assertThat(coordinator.commands("a", "a-1"))
        .filteredOn(c -> c.type() == IndexCacheProtocol.CommandType.LOAD)
        .extracting(c -> c.planId())
        .containsExactly(first);
  }

  @Test
  void rejectedAckReleasesItsReservation() {
    coordinator.report(node("a", "a-1", 1, 40, 0));
    String plan =
        coordinator.acceptLoad(target("A", "f1", 30), IndexResidencyPolicy.parse("0"), clock.now());
    ackFile(plan, "f1", IndexCacheProtocol.AckStatus.REJECTED);
    assertThat(coordinator.result(plan).status()).isEqualTo(IndexCacheProtocol.Status.FAILED);
    assertThat(coordinator.state("a", "a-1").reservedBytesFor(plan)).isZero();
  }

  @Test
  void failedInFlightIndexIsNotAnLruVictim() {
    coordinator.report(node("a", "a-1", 1, 60, 0));
    coordinator.acceptLoad(
        target("loading", "l1", 30), IndexResidencyPolicy.parse("0"), clock.now());
    String plan =
        coordinator.acceptLoad(target("A", "f1", 30), IndexResidencyPolicy.parse("0"), clock.now());
    assertThat(coordinator.result(plan).evictedIndexes()).isEmpty();
  }

  @Test
  void duplicateAcknowledgementCountsOnce() {
    coordinator.report(node("a", "a-1", 1, 100, 0));
    String plan =
        coordinator.acceptLoad(target("A", "f1", 10), IndexResidencyPolicy.parse("0"), clock.now());
    IndexCacheProtocol.Command command = loadCommand(plan);
    IndexCacheProtocol.Ack ack =
        ack(plan, "a", "a-1", command, IndexCacheProtocol.AckStatus.LOADED);
    coordinator.ack(ack);
    coordinator.ack(ack);
    assertThat(coordinator.result(plan).residentBytes()).isEqualTo(10);
    assertThat(coordinator.state("a", "a-1").holders()).containsOnlyKeys("adapter|A|f1");
    assertThat(coordinator.state("a", "a-1").usedBytes()).isEqualTo(10);
  }

  @Test
  void offloadFencesInFlightLoadSoALateAckCannotPublish() {
    coordinator.report(node("a", "a-1", 1, 100, 0));
    String load =
        coordinator.acceptLoad(
            target("A", "f1", 10, "f2", 10), IndexResidencyPolicy.parse("forever"), clock.now());
    ackFile(load, "f1", IndexCacheProtocol.AckStatus.LOADED);
    String offload = coordinator.acceptOffload("A", clock.now());
    // The executor answers the fenced file after the pause was published.
    ackFile(load, "f2", IndexCacheProtocol.AckStatus.LOADED);
    assertThat(coordinator.result(offload).status()).isEqualTo(IndexCacheProtocol.Status.OFFLOADED);
    assertThat(coordinator.state("a", "a-1").holders()).doesNotContainKey("adapter|A|f2");
    assertThat(coordinator.commands("a", "a-1"))
        .filteredOn(c -> c.type() == IndexCacheProtocol.CommandType.RELEASE)
        .extracting(c -> c.file().location())
        .contains("f1");
  }

  @Test
  void offloadAllowsANewLoadOnTheNextEpochOnly() {
    coordinator.report(node("a", "a-1", 1, 100, 0));
    String first =
        coordinator.acceptLoad(
            target("A", "f1", 10), IndexResidencyPolicy.parse("forever"), clock.now());
    ackFile(first, "f1", IndexCacheProtocol.AckStatus.LOADED);
    coordinator.acceptOffload("A", clock.now());
    String reload =
        coordinator.acceptLoad(target("A", "f1", 10), IndexResidencyPolicy.parse("0"), clock.now());
    IndexCacheProtocol.Command command = loadCommand(reload);
    coordinator.ack(ack(reload, "a", "a-1", command, IndexCacheProtocol.AckStatus.LOADED));
    assertThat(coordinator.result(reload).status()).isEqualTo(IndexCacheProtocol.Status.COMPLETE);
    assertThat(coordinator.result(reload).protection().permanent()).isFalse();
  }

  @Test
  void migrationAddsTheSecondCopyBeforeReleasingTheFirst() {
    coordinator.report(node("a", "a-1", 1, 100, 0));
    String first =
        coordinator.acceptLoad(
            target("A", "f", 40), IndexResidencyPolicy.parse("forever"), clock.now());
    ackFile(first, "f", IndexCacheProtocol.AckStatus.LOADED);
    coordinator.report(holding("a", "a-1", 2, 100, 40, "adapter|A|f", 40));
    coordinator.report(node("b", "b-1", 1, 100, 0));

    String plan =
        coordinator.acceptLoad(
            target("A", "f", 40), IndexResidencyPolicy.parse("forever"), clock.now());
    assertThat(coordinator.result(plan).migrations()).containsEntry("f", "b");
    IndexCacheProtocol.Command load = loadCommand(plan, "f", "b");
    assertThat(coordinator.commands("a", "a-1"))
        .filteredOn(c -> c.type() == IndexCacheProtocol.CommandType.RELEASE)
        .isEmpty();
    assertThat(coordinator.state("a", "a-1").holders()).containsKey("adapter|A|f");

    coordinator.ack(ack(plan, "b", "b-1", load, IndexCacheProtocol.AckStatus.LOADED));
    assertThat(coordinator.commands("a", "a-1"))
        .filteredOn(c -> c.type() == IndexCacheProtocol.CommandType.RELEASE)
        .extracting(c -> c.executorId())
        .containsExactly("a");
    // The new copy is confirmed before the old one is released.
    assertThat(coordinator.state("b", "b-1").holders()).containsKey("adapter|A|f");
  }

  @Test
  void migrationIsSkippedWithoutRoomForASecondCopy() {
    coordinator.report(node("a", "a-1", 1, 100, 0));
    String first =
        coordinator.acceptLoad(
            target("A", "f", 40), IndexResidencyPolicy.parse("forever"), clock.now());
    ackFile(first, "f", IndexCacheProtocol.AckStatus.LOADED);
    coordinator.report(holding("a", "a-1", 2, 100, 40, "adapter|A|f", 40));
    // b is proportionally colder (15/50) but only 35 bytes are free, less than a second copy.
    coordinator.report(node("b", "b-1", 1, 50, 15));

    String plan =
        coordinator.acceptLoad(
            target("A", "f", 40), IndexResidencyPolicy.parse("forever"), clock.now());
    assertThat(coordinator.result(plan).migrations()).isEmpty();
    assertThat(coordinator.result(plan).migrationNotices())
        .containsEntry("f", "NEEDS_DOUBLE_SPACE");
    assertThat(coordinator.commands("a", "a-1"))
        .filteredOn(c -> c.type() == IndexCacheProtocol.CommandType.RELEASE)
        .isEmpty();
    assertThat(coordinator.state("a", "a-1").holders()).containsKey("adapter|A|f");
  }

  @Test
  void missingIncarnationAckCannotClaimCoverage() {
    coordinator.report(node("a", "a-1", 1, 100, 0));
    String plan =
        coordinator.acceptLoad(target("A", "f1", 10), IndexResidencyPolicy.parse("0"), clock.now());
    coordinator.ack(
        ack(plan, "ghost", "ghost-1", 0, "plan-1#0", IndexCacheProtocol.AckStatus.LOADED, 10));
    assertThat(coordinator.result(plan).status()).isEqualTo(IndexCacheProtocol.Status.PENDING);
  }

  private IndexCacheProtocol.Result result(String plan) {
    return coordinator.result(plan);
  }

  private void ackAll(String executor, String incarnation, String location) {
    for (IndexCacheProtocol.Plan pending : coordinator.pendingCommands(executor, incarnation)) {
      for (IndexCacheProtocol.File file : pending.files()) {
        if (file.location().equals(location)) {
          ackFile(pending.planId(), location, IndexCacheProtocol.AckStatus.LOADED);
        }
      }
    }
  }

  private void ackNode(String executor, String incarnation) {
    for (IndexCacheProtocol.Plan pending : coordinator.pendingCommands(executor, incarnation)) {
      for (IndexCacheProtocol.File file : pending.files()) {
        ackFile(pending.planId(), file.location(), IndexCacheProtocol.AckStatus.LOADED);
      }
    }
  }

  private void ackFile(String planId, String location, IndexCacheProtocol.AckStatus status) {
    coordinator.ack(ack(planId, "a", "a-1", loadCommand(planId, location), status));
  }

  private IndexCacheProtocol.Command loadCommand(String planId) {
    return coordinator.commands("a", "a-1").stream()
        .filter(c -> c.planId().equals(planId) && c.type() == IndexCacheProtocol.CommandType.LOAD)
        .findFirst()
        .orElseThrow();
  }

  private IndexCacheProtocol.Command loadCommand(String planId, String location, String executor) {
    return coordinator.commands(executor, executor + "-1").stream()
        .filter(c -> c.planId().equals(planId) && c.file().location().equals(location))
        .filter(c -> c.type() == IndexCacheProtocol.CommandType.LOAD)
        .findFirst()
        .orElseThrow();
  }

  private IndexCacheProtocol.Command loadCommand(String planId, String location) {
    return coordinator.commands("a", "a-1").stream()
        .filter(c -> c.planId().equals(planId) && c.file().location().equals(location))
        .filter(c -> c.type() == IndexCacheProtocol.CommandType.LOAD)
        .findFirst()
        .orElseThrow();
  }

  private static IndexCacheProtocol.Ack ack(
      String planId,
      String executor,
      String incarnation,
      IndexCacheProtocol.Command command,
      IndexCacheProtocol.AckStatus status) {
    return ack(
        planId,
        executor,
        incarnation,
        command.sequence(),
        command.commandId(),
        status,
        command.file().estimatedBytes());
  }

  private static IndexCacheProtocol.Ack ack(
      String planId,
      String executor,
      String incarnation,
      long sequence,
      String commandId,
      IndexCacheProtocol.AckStatus status,
      long bytes) {
    return IndexCacheProtocol.Ack.builder()
        .session("session")
        .planId(planId)
        .commandId(commandId)
        .executorId(executor)
        .incarnation(incarnation)
        .sequence(sequence)
        .status(status)
        .bytes(bytes)
        .build();
  }

  // Cache keys start with the adapter namespace, the middle segment is the index identity.
  private static String cacheKey(String indexUuid, String location) {
    return "adapter|" + indexUuid + "|" + location;
  }

  private static IndexCacheProtocol.NodeReport withAccess(
      IndexCacheProtocol.NodeReport report, String indexUuid, long age) {
    java.util.Map<String, Long> ages = new java.util.HashMap<>(report.accessAgeNanos());
    ages.put(indexUuid, age);
    return report.copy().accessAgeNanos(ages).build();
  }

  private static IndexCacheProtocol.NodeReport holding(
      String executor,
      String incarnation,
      long sequence,
      long budget,
      long used,
      String cacheKey,
      long bytes) {
    return node(executor, incarnation, sequence, budget, used)
        .copy()
        .holders(java.util.Map.of(cacheKey, bytes))
        .build();
  }

  private static IndexCacheProtocol.NodeReport node(
      String executor, String incarnation, long sequence, long budget, long used) {
    return IndexCacheProtocol.NodeReport.builder()
        .session("session")
        .executorId(executor)
        .incarnation(incarnation)
        .sequence(sequence)
        .budgetBytes(budget)
        .usedBytes(used)
        .build();
  }

  private static IndexCacheProtocol.Target target(String indexUuid, Object... files) {
    java.util.List<IndexCacheProtocol.File> list = new java.util.ArrayList<>();
    for (int i = 0; i < files.length; i += 2) {
      String location = (String) files[i];
      long bytes = ((Number) files[i + 1]).longValue();
      list.add(
          IndexCacheProtocol.File.builder()
              .location(location)
              .cacheKey(cacheKey(indexUuid, location))
              .estimatedBytes(bytes)
              .build());
    }

    return IndexCacheProtocol.Target.builder()
        .indexUuid(indexUuid)
        .snapshotId(1)
        .files(list)
        .definition(
            new org.apache.iceberg.index.IndexDefinition() {
              @Override
              public String type() {
                return "vector";
              }

              @Override
              public int fieldId() {
                return 1;
              }
            })
        .io(new org.apache.iceberg.hadoop.HadoopFileIO(new org.apache.hadoop.conf.Configuration()))
        .build();
  }

  private static final class FakeClock implements IndexResidencyCoordinator.Clock {
    private long now;

    private FakeClock(long now) {
      this.now = now;
    }

    @Override
    public long nowNanos() {
      return now;
    }

    private void advance(long nanos) {
      this.now += nanos;
    }

    private long now() {
      return now;
    }
  }
}
