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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
    // 60 used + 50 new exceeds the 100-byte budget, so the oldest index has to go.
    String plan =
        coordinator.acceptLoad(
            target("new", "n1", 50), IndexResidencyPolicy.parse("0"), clock.now());
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
    // A is still resident, so the offload has one release to confirm.
    assertThat(coordinator.result(plan).status()).isEqualTo(IndexCacheProtocol.Status.RELEASING);
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
    // The command was issued to a-1, so neither a-2 nor a returning a-1 can confirm it.
    coordinator.ack(ack(plan, "a", "a-2", command, IndexCacheProtocol.AckStatus.LOADED));
    assertThat(coordinator.result(plan).status()).isEqualTo(IndexCacheProtocol.Status.FAILED);
    coordinator.report(node("a", "a-1", 7, 100, 0));
    coordinator.ack(ack(plan, "a", "a-1", command, IndexCacheProtocol.AckStatus.LOADED));
    assertThat(coordinator.result(plan).status()).isEqualTo(IndexCacheProtocol.Status.FAILED);
    assertThat(coordinator.state("a", "a-1").holders()).isEmpty();
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
    assertThat(coordinator.result(offload).status()).isEqualTo(IndexCacheProtocol.Status.RELEASING);
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
  void restartedIncarnationDropsPreviouslyConfirmedCoverage() {
    coordinator.report(node("a", "a-1", 1, 100, 0));
    String plan =
        coordinator.acceptLoad(
            target("A", "f1", 10), IndexResidencyPolicy.parse("forever"), clock.now());
    ackFile(plan, "f1", IndexCacheProtocol.AckStatus.LOADED);
    assertThat(coordinator.result(plan).status()).isEqualTo(IndexCacheProtocol.Status.COMPLETE);

    // The executor restarts with an empty local cache.
    coordinator.report(node("a", "a-2", 1, 100, 0));

    assertThat(coordinator.result(plan).status()).isEqualTo(IndexCacheProtocol.Status.FAILED);
    assertThat(coordinator.result(plan).uncovered()).containsEntry("f1", "NODE_LOST");
    String reload =
        coordinator.acceptLoad(
            target("A", "f1", 10), IndexResidencyPolicy.parse("forever"), clock.now());
    assertThat(coordinator.result(reload).reusedFiles()).isEmpty();
    assertThat(coordinator.commands("a", "a-2"))
        .filteredOn(c -> c.type() == IndexCacheProtocol.CommandType.LOAD)
        .extracting(c -> c.file().location())
        .containsExactly("f1");
  }

  @Test
  void completedLoadReleasesItsReservation() {
    coordinator.report(node("a", "a-1", 1, 100, 0));
    String plan =
        coordinator.acceptLoad(
            target("A", "f1", 10), IndexResidencyPolicy.parse("forever"), clock.now());
    ackFile(plan, "f1", IndexCacheProtocol.AckStatus.LOADED);

    assertThat(coordinator.state("a", "a-1").reservedBytesFor(plan)).isZero();
    assertThat(coordinator.state("a", "a-1").usedBytes()).isEqualTo(10);
    // The confirmed copy must not be charged a second time through a stale reservation.
    String next =
        coordinator.acceptLoad(target("B", "g1", 90), IndexResidencyPolicy.parse("0"), clock.now());
    assertThat(coordinator.result(next).uncovered()).containsEntry("g1", "PENDING");
  }

  @Test
  void firstLoadWithoutRetentionIsEvictableAndReported() {
    coordinator.report(node("a", "a-1", 1, 100, 0));
    String old = coordinator.acceptLoad(target("old", "o1", 40), null, clock.now());
    ackFile(old, "o1", IndexCacheProtocol.AckStatus.LOADED);
    coordinator.report(holding("a", "a-1", 2, 100, 40, "adapter|old|o1", 40));
    clock.advance(5 * SECOND);
    String hot = coordinator.acceptLoad(target("hot", "h1", 40), null, clock.now());
    ackFile(hot, "h1", IndexCacheProtocol.AckStatus.LOADED);
    coordinator.report(holding("a", "a-1", 3, 100, 80, "adapter|hot|h1", 40));

    String plan = coordinator.acceptLoad(target("new", "n1", 30), null, clock.now());

    assertThat(coordinator.result(plan).evictedIndexes()).containsExactly("old");
    assertThat(coordinator.result(old).protection()).isNotNull();
    assertThat(coordinator.result(old).protection().permanent()).isFalse();
  }

  @Test
  void lruOrderFollowsAccessAgeNotReportOrder() {
    coordinator.report(node("a", "a-1", 1, 100, 0));
    String old =
        coordinator.acceptLoad(
            target("old", "o1", 40), IndexResidencyPolicy.parse("0"), clock.now());
    ackFile(old, "o1", IndexCacheProtocol.AckStatus.LOADED);
    coordinator.report(holding("a", "a-1", 2, 100, 40, "adapter|old|o1", 40));
    String hot =
        coordinator.acceptLoad(
            target("hot", "h1", 40), IndexResidencyPolicy.parse("0"), clock.now());
    ackFile(hot, "h1", IndexCacheProtocol.AckStatus.LOADED);

    // One report lists the hot index first, but its recorded access is far more recent.
    Map<String, Long> ages = new LinkedHashMap<>();
    ages.put("hot", SECOND);
    ages.put("old", 20 * SECOND);
    coordinator.report(
        node("a", "a-1", 3, 100, 80)
            .copy()
            .holders(Map.of("adapter|old|o1", 40L, "adapter|hot|h1", 40L))
            .accessAgeNanos(ages)
            .build());

    String plan =
        coordinator.acceptLoad(
            target("new", "n1", 30), IndexResidencyPolicy.parse("0"), clock.now());
    assertThat(coordinator.result(plan).evictedIndexes()).containsExactly("old");
  }

  @Test
  void oversizedFileDoesNotEvictAnotherIndex() {
    coordinator.report(node("a", "a-1", 1, 100, 0));
    String victim =
        coordinator.acceptLoad(target("V", "v1", 40), IndexResidencyPolicy.parse("0"), clock.now());
    ackFile(victim, "v1", IndexCacheProtocol.AckStatus.LOADED);
    coordinator.report(holding("a", "a-1", 2, 100, 40, "adapter|V|v1", 40));
    coordinator.report(node("b", "b-1", 1, 100, 100));

    // No node can hold a 200-byte file, so evicting anything cannot help.
    String plan =
        coordinator.acceptLoad(
            target("W", "big", 200), IndexResidencyPolicy.parse("0"), clock.now());

    assertThat(coordinator.result(plan).uncovered()).containsEntry("big", "FILE_TOO_LARGE");
    assertThat(coordinator.result(plan).evictedIndexes()).isEmpty();
    assertThat(coordinator.state("a", "a-1").holders()).containsKey("adapter|V|v1");
  }

  @Test
  void capacityEvictionKeepsOtherRejectionReasons() {
    coordinator.report(node("a", "a-1", 1, 100, 0));
    String victim =
        coordinator.acceptLoad(target("V", "v1", 40), IndexResidencyPolicy.parse("0"), clock.now());
    ackFile(victim, "v1", IndexCacheProtocol.AckStatus.LOADED);
    coordinator.report(holding("a", "a-1", 2, 100, 40, "adapter|V|v1", 40));

    String plan =
        coordinator.acceptLoad(
            target("W", "big", 200, "small", 70), IndexResidencyPolicy.parse("0"), clock.now());

    assertThat(coordinator.result(plan).uncovered()).containsEntry("big", "FILE_TOO_LARGE");
    assertThat(coordinator.result(plan).uncovered())
        .containsEntry("small", "INSUFFICIENT_CAPACITY_PENDING_EVICTION");
    assertThat(coordinator.result(plan).evictedIndexes()).containsExactly("V");
  }

  @Test
  void lateLoadAfterOffloadIsReleasedInsteadOfLeaked() {
    coordinator.report(node("a", "a-1", 1, 100, 0));
    String load =
        coordinator.acceptLoad(
            target("A", "f1", 10, "f2", 10), IndexResidencyPolicy.parse("forever"), clock.now());
    ackFile(load, "f1", IndexCacheProtocol.AckStatus.LOADED);
    coordinator.acceptOffload("A", clock.now());

    IndexCacheProtocol.Command late = loadCommand(load, "f2");
    coordinator.ack(ack(load, "a", "a-1", late, IndexCacheProtocol.AckStatus.LOADED));

    assertThat(coordinator.commands("a", "a-1"))
        .filteredOn(c -> c.type() == IndexCacheProtocol.CommandType.RELEASE)
        .extracting(c -> c.file().location())
        .contains("f2");
    assertThat(coordinator.state("a", "a-1").holders()).doesNotContainKey("adapter|A|f2");
  }

  @Test
  void acknowledgementFromAnotherApplicationSessionIsIgnored() {
    coordinator.report(node("a", "a-1", 1, 100, 0));
    String plan =
        coordinator.acceptLoad(
            target("A", "f1", 10), IndexResidencyPolicy.parse("forever"), clock.now());
    IndexCacheProtocol.Command command = loadCommand(plan);

    coordinator.ack(
        IndexCacheProtocol.Ack.builder()
            .session("another-application")
            .planId(plan)
            .commandId(command.commandId())
            .executorId("a")
            .incarnation("a-1")
            .epoch(command.epoch())
            .sequence(command.sequence())
            .status(IndexCacheProtocol.AckStatus.LOADED)
            .bytes(10)
            .build());

    assertThat(coordinator.result(plan).status()).isEqualTo(IndexCacheProtocol.Status.PENDING);
    assertThat(coordinator.state("a", "a-1").holders()).isEmpty();
  }

  @Test
  void inFlightLoadToRestartedIncarnationIsFenced() {
    coordinator.report(node("a", "a-1", 1, 100, 0));
    String plan =
        coordinator.acceptLoad(
            target("A", "f1", 10), IndexResidencyPolicy.parse("forever"), clock.now());

    // The executor restarts before answering; its command will never be delivered.
    coordinator.report(node("a", "a-2", 1, 100, 0));

    assertThat(coordinator.result(plan).status()).isEqualTo(IndexCacheProtocol.Status.FAILED);
    assertThat(coordinator.result(plan).uncovered()).containsEntry("f1", "NODE_LOST");
    String reload =
        coordinator.acceptLoad(
            target("A", "f1", 10), IndexResidencyPolicy.parse("forever"), clock.now());
    assertThat(coordinator.result(reload).status()).isNotEqualTo(IndexCacheProtocol.Status.QUEUED);
  }

  @Test
  void releasingInAPlanKeepsItsOwnLoadReservation() {
    coordinator.report(node("a", "a-1", 1, 100, 0));
    String first =
        coordinator.acceptLoad(
            target("A", "old", 40), IndexResidencyPolicy.parse("forever"), clock.now());
    ackFile(first, "old", IndexCacheProtocol.AckStatus.LOADED);
    coordinator.report(holding("a", "a-1", 2, 100, 40, "adapter|A|old", 40));

    // Replacing the target both loads a new file and releases the old one.
    String replace =
        coordinator.acceptLoad(
            target("A", "new", 40), IndexResidencyPolicy.parse("forever"), clock.now());
    IndexCacheProtocol.Command release =
        coordinator.commands("a", "a-1").stream()
            .filter(
                c ->
                    c.planId().equals(replace)
                        && c.type() == IndexCacheProtocol.CommandType.RELEASE)
            .findFirst()
            .orElseThrow();
    coordinator.ack(ack(replace, "a", "a-1", release, IndexCacheProtocol.AckStatus.RELEASED));

    assertThat(coordinator.state("a", "a-1").reservedBytesFor(replace)).isEqualTo(40);
  }

  @Test
  void orphanReleaseKeepsTheEpochThatLoadedTheCopy() {
    coordinator.report(node("a", "a-1", 1, 100, 0));
    String load =
        coordinator.acceptLoad(
            target("A", "f1", 10, "f2", 10), IndexResidencyPolicy.parse("forever"), clock.now());
    IndexCacheProtocol.Command f2 = loadCommand(load, "f2");
    coordinator.acceptOffload("A", clock.now());
    coordinator.ack(ack(load, "a", "a-1", f2, IndexCacheProtocol.AckStatus.LOADED));

    IndexCacheProtocol.Command orphan =
        coordinator.commands("a", "a-1").stream()
            .filter(
                c ->
                    c.type() == IndexCacheProtocol.CommandType.RELEASE
                        && "f2".equals(c.file().location()))
            .findFirst()
            .orElseThrow();
    // A tombstone must not be mistaken for authority over a newer load of the same key.
    assertThat(orphan.epoch()).isEqualTo(f2.epoch());
    assertThat(orphan.mode()).isEqualTo("ORPHAN");
  }

  @Test
  void repeatedLateAcknowledgementEnqueuesOneRelease() {
    coordinator.report(node("a", "a-1", 1, 100, 0));
    String load =
        coordinator.acceptLoad(
            target("A", "f1", 10, "f2", 10), IndexResidencyPolicy.parse("forever"), clock.now());
    IndexCacheProtocol.Command f2 = loadCommand(load, "f2");
    coordinator.acceptOffload("A", clock.now());
    IndexCacheProtocol.Ack late = ack(load, "a", "a-1", f2, IndexCacheProtocol.AckStatus.LOADED);
    coordinator.ack(late);
    coordinator.ack(late);

    assertThat(coordinator.commands("a", "a-1"))
        .filteredOn(
            c ->
                c.type() == IndexCacheProtocol.CommandType.RELEASE
                    && "f2".equals(c.file().location()))
        .hasSize(1);
  }

  @Test
  void acknowledgementFromANewIncarnationCannotConfirmAnOldCommand() {
    coordinator.report(node("a", "a-1", 1, 100, 0));
    String plan =
        coordinator.acceptLoad(
            target("A", "f1", 10), IndexResidencyPolicy.parse("forever"), clock.now());
    IndexCacheProtocol.Command command = loadCommand(plan);
    coordinator.report(node("a", "a-2", 1, 100, 0));

    coordinator.ack(ack(plan, "a", "a-2", command, IndexCacheProtocol.AckStatus.LOADED));

    assertThat(coordinator.result(plan).status()).isEqualTo(IndexCacheProtocol.Status.FAILED);
    assertThat(coordinator.state("a", "a-2").holders()).isEmpty();
  }

  @Test
  void aReusedFileStillNamesTheExecutorThatHoldsIt() {
    coordinator.report(node("a", "a-1", 1, 1_000, 0));
    String first =
        coordinator.acceptLoad(
            target("A", "f", 40), IndexResidencyPolicy.parse("forever"), clock.now());
    ackFile(first, "f", IndexCacheProtocol.AckStatus.LOADED);
    coordinator.report(holding("a", "a-1", 2, 1_000, 40, "adapter|A|f", 40));

    String second =
        coordinator.acceptLoad(
            target("A", "f", 40), IndexResidencyPolicy.parse("forever"), clock.now());

    // The copy is already there, so this plan commands nothing; the holder still has to be named or
    // a COMPLETE plan reports files whose executor is unknown.
    assertThat(coordinator.result(second).reusedFiles()).isNotEmpty();
    assertThat(coordinator.placements(second)).containsEntry("f", "a");
  }

  @Test
  void finishedPlansAreKeptInABoundedWindow() {
    coordinator.report(node("a", "a-1", 1, 1_000_000, 0));
    for (int i = 0; i < 70; i++) {
      String plan =
          coordinator.acceptLoad(
              target("A", "f" + i, 1), IndexResidencyPolicy.parse("forever"), clock.now());
      ackFile(plan, "f" + i, IndexCacheProtocol.AckStatus.LOADED);
      coordinator.result(plan);
    }

    // Every plan here is superseded, so the window drops the oldest instead of growing forever.
    assertThat(coordinator.planCount()).isLessThanOrEqualTo(64);
  }

  @Test
  void offloadWaitsForEveryCopyOfAFileNotJustTheFirst() {
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
    coordinator.ack(
        ack(plan, "b", "b-1", loadCommand(plan, "f", "b"), IndexCacheProtocol.AckStatus.LOADED));

    String offload = coordinator.acceptOffload("A", clock.now());
    assertThat(coordinator.result(offload).status()).isEqualTo(IndexCacheProtocol.Status.RELEASING);

    IndexCacheProtocol.Command releaseOnA =
        coordinator.commands("a", "a-1").stream()
            .filter(c -> c.type() == IndexCacheProtocol.CommandType.RELEASE)
            .findFirst()
            .orElseThrow();
    coordinator.ack(ack(offload, "a", "a-1", releaseOnA, IndexCacheProtocol.AckStatus.RELEASED));

    // The migrated copy on b is still resident, so one acknowledged release is not the end.
    assertThat(coordinator.result(offload).status()).isEqualTo(IndexCacheProtocol.Status.RELEASING);

    IndexCacheProtocol.Command releaseOnB =
        coordinator.commands("b", "b-1").stream()
            .filter(c -> c.type() == IndexCacheProtocol.CommandType.RELEASE)
            .findFirst()
            .orElseThrow();
    coordinator.ack(ack(offload, "b", "b-1", releaseOnB, IndexCacheProtocol.AckStatus.RELEASED));

    assertThat(coordinator.result(offload).status()).isEqualTo(IndexCacheProtocol.Status.OFFLOADED);
  }

  @Test
  void migrationReleaseKeepsTheNewCopyConfirmed() {
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
    IndexCacheProtocol.Command migrate = loadCommand(plan, "f", "b");
    coordinator.ack(ack(plan, "b", "b-1", migrate, IndexCacheProtocol.AckStatus.LOADED));
    IndexCacheProtocol.Command release =
        coordinator.commands("a", "a-1").stream()
            .filter(
                c -> c.planId().equals(plan) && c.type() == IndexCacheProtocol.CommandType.RELEASE)
            .findFirst()
            .orElseThrow();
    // The released copy was installed under an earlier plan, so the tombstone names that epoch.
    assertThat(release.epoch()).isLessThan(planEpoch(plan));
    coordinator.ack(ack(plan, "a", "a-1", release, IndexCacheProtocol.AckStatus.RELEASED));

    // The old copy is gone, but the migrated one must still count as confirmed coverage.
    assertThat(coordinator.result(plan).status()).isEqualTo(IndexCacheProtocol.Status.COMPLETE);
    assertThat(coordinator.result(plan).residentBytes()).isEqualTo(40);

    // Offload must find the live copy on b and must not chase the released copy on a.
    coordinator.acceptOffload("A", clock.now());
    assertThat(coordinator.pendingCommands("b", "b-1"))
        .flatExtracting(IndexCacheProtocol.Plan::files)
        .extracting(IndexCacheProtocol.File::cacheKey)
        .contains("adapter|A|f");
    assertThat(coordinator.pendingCommands("a", "a-1")).isEmpty();
  }

  @Test
  void fencedPlanDoesNotBlockAnotherIndex() {
    coordinator.report(node("a", "a-1", 1, 100, 0));
    coordinator.acceptLoad(
        target("A", "f1", 10), IndexResidencyPolicy.parse("forever"), clock.now());
    // The executor restarts before answering, which fences the outstanding command.
    coordinator.report(node("a", "a-2", 1, 100, 0));

    String other =
        coordinator.acceptLoad(target("B", "g1", 10), IndexResidencyPolicy.parse("0"), clock.now());

    assertThat(coordinator.result(other).status()).isNotEqualTo(IndexCacheProtocol.Status.QUEUED);
    assertThat(coordinator.state("a", "a-2").reservedBytesFor(other)).isEqualTo(10);
  }

  @Test
  void timedOutPlanDoesNotHoldItsReservationForever() {
    coordinator.report(node("a", "a-1", 1, 100, 0));
    String plan =
        coordinator.acceptLoad(target("A", "f1", 40), IndexResidencyPolicy.parse("0"), clock.now());
    coordinator.expire(plan, clock.now());

    assertThat(coordinator.state("a", "a-1").reservedBytesFor(plan)).isZero();
  }

  @Test
  void lruComparesAccessAgesOnTheObservationClock() {
    coordinator.report(node("a", "a-1", 1, 100, 0));
    String old =
        coordinator.acceptLoad(
            target("old", "o1", 40), IndexResidencyPolicy.parse("0"), clock.now());
    ackFile(old, "o1", IndexCacheProtocol.AckStatus.LOADED);
    coordinator.report(holding("a", "a-1", 2, 100, 40, "adapter|old|o1", 40));
    // 30 seconds old when reported.
    coordinator.report(
        holding("a", "a-1", 3, 100, 40, "adapter|old|o1", 40)
            .copy()
            .accessAgeNanos(Map.of("old", 30 * SECOND))
            .build());

    clock.advance(60 * SECOND);
    String hot =
        coordinator.acceptLoad(
            target("hot", "h1", 40), IndexResidencyPolicy.parse("0"), clock.now());
    ackFile(hot, "h1", IndexCacheProtocol.AckStatus.LOADED);
    coordinator.report(holding("a", "a-1", 4, 100, 80, "adapter|hot|h1", 40));

    String plan =
        coordinator.acceptLoad(
            target("new", "n1", 50), IndexResidencyPolicy.parse("0"), clock.now());
    assertThat(coordinator.result(plan).evictedIndexes()).containsExactly("old");
  }

  @Test
  void lateReleaseCannotForgetAReloadedCopy() {
    coordinator.report(node("a", "a-1", 1, 100, 0));
    String first =
        coordinator.acceptLoad(
            target("A", "f", 40), IndexResidencyPolicy.parse("forever"), clock.now());
    ackFile(first, "f", IndexCacheProtocol.AckStatus.LOADED);
    coordinator.report(holding("a", "a-1", 2, 100, 40, "adapter|A|f", 40));

    // Offload issues a release that the executor has not answered yet.
    String offload = coordinator.acceptOffload("A", clock.now());
    IndexCacheProtocol.Command release =
        coordinator.commands("a", "a-1").stream()
            .filter(
                c ->
                    c.planId().equals(offload)
                        && c.type() == IndexCacheProtocol.CommandType.RELEASE)
            .findFirst()
            .orElseThrow();

    // A new load puts a fresh copy of the same key on the same node and incarnation.
    String reload =
        coordinator.acceptLoad(
            target("A", "f", 40), IndexResidencyPolicy.parse("forever"), clock.now());
    ackFile(reload, "f", IndexCacheProtocol.AckStatus.LOADED);
    assertThat(coordinator.result(reload).status()).isEqualTo(IndexCacheProtocol.Status.COMPLETE);

    // The stale release finally arrives; it must not forget the copy that replaced it.
    coordinator.ack(ack(offload, "a", "a-1", release, IndexCacheProtocol.AckStatus.RELEASED));

    assertThat(coordinator.result(reload).status()).isEqualTo(IndexCacheProtocol.Status.COMPLETE);
    assertThat(coordinator.result(reload).uncovered()).isEmpty();
  }

  @Test
  void replacedTargetReportsCancelledRatherThanFailed() {
    coordinator.report(node("a", "a-1", 1, 100, 0));
    String first =
        coordinator.acceptLoad(
            target("A", "old", 10), IndexResidencyPolicy.parse("forever"), clock.now());
    // A repeat load replaces the target before the first plan ever answered.
    coordinator.acceptLoad(
        target("A", "new", 10), IndexResidencyPolicy.parse("forever"), clock.now());

    assertThat(coordinator.result(first).status()).isEqualTo(IndexCacheProtocol.Status.CANCELLED);
  }

  @Test
  void evictionReleaseCarriesTheVictimCopyEpochNotTheRequesters() {
    coordinator.report(node("a", "a-1", 1, 100, 0));
    String victim =
        coordinator.acceptLoad(target("V", "v1", 40), IndexResidencyPolicy.parse("0"), clock.now());
    ackFile(victim, "v1", IndexCacheProtocol.AckStatus.LOADED);
    coordinator.report(holding("a", "a-1", 2, 100, 40, "adapter|V|v1", 40));
    // The requesting index itself advances to epoch 3 before it needs to evict.
    for (int i = 0; i < 2; i++) {
      clock.advance(SECOND);
      coordinator.acceptLoad(
          target("W", "warm" + i, 1), IndexResidencyPolicy.parse("forever"), clock.now());
    }

    String plan =
        coordinator.acceptLoad(
            target("W", "big", 200, "small", 70), IndexResidencyPolicy.parse("0"), clock.now());

    IndexCacheProtocol.Command release =
        coordinator.commands("a", "a-1").stream()
            .filter(
                c -> c.planId().equals(plan) && c.type() == IndexCacheProtocol.CommandType.RELEASE)
            .findFirst()
            .orElseThrow();
    // The executor compares this epoch with the epoch that installed the victim's copy, so the
    // eviction must carry 1 even though the requesting plan is much later.
    assertThat(planEpoch(plan)).isGreaterThan(1);
    assertThat(release.epoch()).isEqualTo(1);
    assertThat(coordinator.result(plan).evictedIndexes()).containsExactly("V");
  }

  @Test
  void aTableScopedCacheKeyFromTheQueryPathIsRoutable() {
    coordinator.report(node("a", "a-1", 1, 1_000, 0));
    IndexCacheProtocol.File file =
        IndexCacheProtocol.File.builder()
            .location("f1")
            .cacheKey("s3://bucket/table|Adapter|vector-hnsw|f1")
            .estimatedBytes(10L)
            .build();
    IndexCacheProtocol.Target target =
        IndexCacheProtocol.Target.builder()
            .indexUuid("table#A")
            .snapshotId(1)
            .files(List.of(file))
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
            .io(
                new org.apache.iceberg.hadoop.HadoopFileIO(
                    new org.apache.hadoop.conf.Configuration()))
            .build();

    String plan =
        coordinator.acceptLoad(target, IndexResidencyPolicy.parse("forever"), clock.now());

    assertThat(coordinator.result(plan).uncovered()).doesNotContainKey("CACHE_KEY_NOT_OWNED");
    assertThat(coordinator.commands("a", "a-1"))
        .filteredOn(c -> c.type() == IndexCacheProtocol.CommandType.LOAD)
        .extracting(c -> c.file().cacheKey())
        .containsExactly("s3://bucket/table|Adapter|vector-hnsw|f1");
  }

  @Test
  void aCacheKeyThatNamesAnotherFileIsRejected() {
    coordinator.report(node("a", "a-1", 1, 1_000, 0));
    IndexCacheProtocol.File file =
        IndexCacheProtocol.File.builder()
            .location("f1")
            .cacheKey("s3://bucket/table|Adapter|vector-hnsw|f2")
            .estimatedBytes(10L)
            .build();
    IndexCacheProtocol.Target target =
        IndexCacheProtocol.Target.builder()
            .indexUuid("table#A")
            .snapshotId(1)
            .files(List.of(file))
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
            .io(
                new org.apache.iceberg.hadoop.HadoopFileIO(
                    new org.apache.hadoop.conf.Configuration()))
            .build();

    String plan =
        coordinator.acceptLoad(target, IndexResidencyPolicy.parse("forever"), clock.now());

    assertThat(coordinator.result(plan).uncovered()).containsEntry("f1", "CACHE_KEY_NOT_OWNED");
  }

  @Test
  void aRecordedQueryAccessMakesTheIndexTheMostRecentlyUsed() {
    coordinator.report(node("a", "a-1", 1, 100, 0));
    String old =
        coordinator.acceptLoad(
            target("old", "o1", 40), IndexResidencyPolicy.parse("0"), clock.now());
    ackFile(old, "o1", IndexCacheProtocol.AckStatus.LOADED);
    coordinator.report(holding("a", "a-1", 2, 100, 40, "adapter|old|o1", 40));
    clock.advance(SECOND);
    String hot =
        coordinator.acceptLoad(
            target("hot", "h1", 40), IndexResidencyPolicy.parse("0"), clock.now());
    ackFile(hot, "h1", IndexCacheProtocol.AckStatus.LOADED);
    coordinator.report(holding("a", "a-1", 3, 100, 80, "adapter|hot|h1", 40));

    // The old index is used again after the hot one was loaded.
    clock.advance(SECOND);
    coordinator.recordAccess("old", clock.now());

    clock.advance(SECOND);
    String plan =
        coordinator.acceptLoad(
            target("new", "n1", 50), IndexResidencyPolicy.parse("0"), clock.now());
    assertThat(coordinator.result(plan).evictedIndexes()).containsExactly("hot");
  }

  @Test
  void recordingAccessForAnUnmanagedIndexChangesNothing() {
    coordinator.recordAccess("never-loaded", clock.now());
    assertThat(coordinator.protectedIndexes()).isEmpty();
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

  /** The policy epoch the plan was accepted under. */
  private long planEpoch(String plan) {
    return coordinator.result(plan).epoch();
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
    Long bytes = command.file().estimatedBytes();
    return ack(
        planId,
        executor,
        incarnation,
        command.sequence(),
        command.commandId(),
        status,
        bytes == null ? 0 : bytes);
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

  @Test
  void lruKeepsOrderAfterTheDriverClockRestarts() {
    coordinator.report(node("a", "a-1", 1, 100, 0));
    String old =
        coordinator.acceptLoad(
            target("old", "o1", 40), IndexResidencyPolicy.parse("0"), clock.now());
    ackFile(old, "o1", IndexCacheProtocol.AckStatus.LOADED);
    coordinator.report(
        holding("a", "a-1", 2, 100, 40, "adapter|old|o1", 40)
            .copy()
            .accessAgeNanos(Map.of("old", 5 * SECOND))
            .build());
    clock.advance(1000 * SECOND);

    // The driver restarts: the monotonic clock starts near zero while executors keep reporting
    // ages.
    clock.reset(SECOND);
    String hot =
        coordinator.acceptLoad(
            target("hot", "h1", 40), IndexResidencyPolicy.parse("0"), clock.now());
    ackFile(hot, "h1", IndexCacheProtocol.AckStatus.LOADED);
    coordinator.report(holding("a", "a-1", 3, 100, 80, "adapter|hot|h1", 40));

    String plan =
        coordinator.acceptLoad(
            target("new", "n1", 50), IndexResidencyPolicy.parse("0"), clock.now());
    assertThat(coordinator.result(plan).evictedIndexes()).containsExactly("old");
  }

  @Test
  void extremeReportedAgeDoesNotOverflowTheOrdering() {
    coordinator.report(node("a", "a-1", 1, 100, 0));
    String stale =
        coordinator.acceptLoad(
            target("stale", "s1", 40), IndexResidencyPolicy.parse("0"), clock.now());
    ackFile(stale, "s1", IndexCacheProtocol.AckStatus.LOADED);
    String fresh =
        coordinator.acceptLoad(
            target("fresh", "f1", 20), IndexResidencyPolicy.parse("0"), clock.now());
    ackFile(fresh, "f1", IndexCacheProtocol.AckStatus.LOADED);
    coordinator.report(
        node("a", "a-1", 2, 100, 60)
            .copy()
            .holders(Map.of("adapter|stale|s1", 40L, "adapter|fresh|f1", 20L))
            .accessAgeNanos(Map.of("stale", Long.MAX_VALUE - 1, "fresh", SECOND))
            .build());
    // Time must pass after the report, otherwise an overflowing addition never happens. One second
    // is enough and stays away from the ten-second staleness boundary.
    clock.advance(SECOND);

    String plan =
        coordinator.acceptLoad(
            target("new", "n1", 60), IndexResidencyPolicy.parse("0"), clock.now());
    assertThat(coordinator.result(plan).evictedIndexes()).containsExactly("stale");
  }

  private static final class FakeClock implements IndexResidencyCoordinator.Clock {
    private long now;

    private FakeClock(long now) {
      this.now = now;
    }

    private void reset(long now) {
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
