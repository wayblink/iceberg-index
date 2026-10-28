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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.index.IndexDefinition;
import org.apache.iceberg.index.IndexFileMemory;
import org.apache.iceberg.io.FileIO;
import org.junit.jupiter.api.Test;

class TestIndexPlacementPlanner {
  // Model 2 prices an entry at its prepared state plus the fixed wrapper. Keep the old unit values
  // as hundreds of bytes so the wrapper does not change the relative scenarios under test.
  private static final long UNIT = 1_000L;
  private static final long WRAPPER = 168L;

  private final IndexPlacementPlanner planner = new IndexPlacementPlanner();

  @Test
  void placesNewFileOnLeastOccupiedNode() {
    IndexCacheProtocol.Target target = target(file("new-2g", 2));
    assertThat(
            planner
                .plan(
                    target,
                    List.of(
                        node("executor-1", 8, 6),
                        node("executor-2", 8, 2),
                        node("executor-3", 8, 2),
                        node("executor-4", 8, 0)))
                .placements())
        .containsEntry("new-2g", "executor-4");
  }

  @Test
  void comparesOccupancyRatiosRatherThanAbsoluteBytes() {
    assertThat(
            planner
                .plan(target(file("new", 1)), List.of(node("small", 4, 2), node("large", 16, 3)))
                .placements())
        .containsEntry("new", "large");
  }

  @Test
  void sortsLargeFilesFirstAndUsesStableNodeTieBreak() {
    IndexCacheProtocol.Target target = target(file("small", 2), file("large", 6));
    List<IndexCacheProtocol.NodeReport> nodes = List.of(node("b", 6, 0), node("a", 6, 0));
    assertThat(planner.plan(target, nodes).placements())
        .containsExactlyInAnyOrderEntriesOf(Map.of("large", "a", "small", "b"));
    assertThat(
            planner
                .plan(
                    target(file("large", 6), file("small", 2)), List.of(nodes.get(1), nodes.get(0)))
                .placements())
        .containsExactlyInAnyOrderEntriesOf(Map.of("large", "a", "small", "b"));
  }

  @Test
  void aggregateCapacityCannotFitAnOversizedSingleFile() {
    assertThat(
            planner
                .plan(target(file("oversized", 5)), List.of(node("a", 4, 0), node("b", 4, 0)))
                .rejected())
        .containsEntry("oversized", "FILE_TOO_LARGE");
  }

  @Test
  void doesNotSpendAnotherLoadReservationOrPendingRelease() {
    System.out.println("DBG held? " + units(4));
    IndexCacheProtocol.NodeReport held =
        IndexCacheProtocol.NodeReport.builder()
            .session("session")
            .executorId("a")
            .incarnation("a-1")
            .budgetBytes(rawBudget(8))
            .usedBytes(units(4))
            .reservedBytes(units(4))
            .releasingBytes(units(4))
            .build();
    System.out.println("DBG held used=" + held.usedBytes() + " res=" + held.reservedBytes());
    assertThat(planner.plan(target(file("new", 2)), List.of(held)).rejected())
        .containsEntry("new", "INSUFFICIENT_CAPACITY");
  }

  @Test
  void releasesAreIncludedInUsedRatherThanCountedTwice() {
    IndexCacheProtocol.NodeReport held =
        IndexCacheProtocol.NodeReport.builder()
            .session("session")
            .executorId("a")
            .incarnation("a-1")
            .budgetBytes(rawBudget(8))
            .usedBytes(units(4))
            .releasingBytes(units(4))
            .build();
    assertThat(planner.plan(target(file("new", 4)), List.of(held)).placements())
        .containsEntry("new", "a");
  }

  @Test
  void everyFileHasAtMostOnePlannedReplica() {
    IndexCacheProtocol.Plan plan =
        planner.plan(
            target(file("a", 2), file("b", 2), file("c", 2)),
            List.of(node("a", 8, 0), node("b", 8, 0)));
    assertThat(plan.placements()).hasSize(3);
    assertThat(plan.rejected()).isEmpty();
  }

  @Test
  void unknownEstimateIsNotZeroCost() {
    IndexCacheProtocol.File file =
        IndexCacheProtocol.File.builder()
            .location("unknown")
            .cacheKey("table|adapter|unknown")
            .build();
    assertThat(planner.plan(target(file), List.of(node("a", 8, 0))).rejected())
        .containsEntry("unknown", "SIZE_UNKNOWN");
  }

  @Test
  void confirmedHolderCanBeReusedWithoutDoubleCharge() {
    IndexCacheProtocol.NodeReport holder =
        IndexCacheProtocol.NodeReport.builder()
            .session("session")
            .executorId("a")
            .incarnation("a-1")
            .budgetBytes(rawBudget(8))
            .usedBytes(units(8))
            .holders(Map.of("table|adapter|reused", 8L))
            .build();
    assertThat(planner.plan(target(file("reused", 8)), List.of(holder)).placements())
        .containsEntry("reused", "a");
  }

  @Test
  void balancesExistingFileOnlyWithRoomForTheSecondCopy() {
    IndexCacheProtocol.NodeReport holder =
        IndexCacheProtocol.NodeReport.builder()
            .session("session")
            .executorId("a")
            .incarnation("a-1")
            .budgetBytes(rawBudget(8))
            .usedBytes(units(8))
            .holders(Map.of("table|adapter|reused", 4L))
            .build();
    assertThat(
            planner.plan(target(file("reused", 4)), List.of(holder, node("b", 8, 2))).placements())
        .containsEntry("reused", "b");
    assertThat(
            planner.plan(target(file("reused", 4)), List.of(holder, node("b", 8, 6))).placements())
        .containsEntry("reused", "a");
  }

  @Test
  void targetAndNodeCollectionsAreDefensivelyCopied() {
    List<IndexCacheProtocol.File> files = new ArrayList<>(List.of(file("a", 1)));
    IndexCacheProtocol.Target target =
        IndexCacheProtocol.Target.builder().indexUuid("table#A").snapshotId(1).files(files).build();
    files.clear();
    assertThat(target.files()).hasSize(1);
    assertThatThrownBy(() -> target.files().clear())
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> planner.plan(target, List.of(node("a", 8, 0))).placements().clear())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void comparisonsAndChargesDoNotWrapNearLongLimit() {
    IndexCacheProtocol.Plan plan =
        planner.plan(
            target(file("large", Long.MAX_VALUE / 2)),
            List.of(node("a", Long.MAX_VALUE, Long.MAX_VALUE / 2), node("b", Long.MAX_VALUE, 0)));
    assertThat(plan.placements()).containsEntry("large", "b");
  }

  @Test
  void reportedOccupancyOverflowIsRejected() {
    assertThatThrownBy(
            () ->
                IndexCacheProtocol.NodeReport.builder()
                    .session("session")
                    .executorId("a")
                    .incarnation("a-1")
                    .budgetBytes(Long.MAX_VALUE)
                    .usedBytes(Long.MAX_VALUE)
                    .reservedBytes(units(1))
                    .build())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void nodeHolderCollectionsAreDefensivelyCopied() {
    Map<String, Long> holders = new HashMap<>(Map.of("a", 1L));
    IndexCacheProtocol.NodeReport node =
        IndexCacheProtocol.NodeReport.builder()
            .session("session")
            .executorId("a")
            .incarnation("a-1")
            .budgetBytes(rawBudget(8))
            .usedBytes(units(1))
            .holders(holders)
            .build();
    holders.clear();
    assertThat(node.holders()).containsEntry("a", 1L);
    assertThatThrownBy(() -> node.holders().clear())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void migrationDoesNotMakeOccupancyRatiosWorse() {
    IndexCacheProtocol.NodeReport holder =
        IndexCacheProtocol.NodeReport.builder()
            .session("session")
            .executorId("a")
            .incarnation("a-1")
            .budgetBytes(rawBudget(16))
            .usedBytes(units(8))
            .holders(Map.of("table|adapter|reused", 4L))
            .build();
    assertThat(
            planner.plan(target(file("reused", 4)), List.of(holder, node("b", 4, 0))).placements())
        .containsEntry("reused", "a");
  }

  @Test
  void migrationCannotConsumeRoomNeededForAnUncoveredFile() {
    IndexCacheProtocol.NodeReport holder =
        IndexCacheProtocol.NodeReport.builder()
            .session("session")
            .executorId("a")
            .incarnation("a-1")
            .budgetBytes(rawBudget(8))
            .usedBytes(units(8))
            .holders(Map.of("table|adapter|old", 8L))
            .build();
    assertThat(
            planner
                .plan(target(file("old", 8), file("new", 6)), List.of(holder, node("b", 10, 0)))
                .placements())
        .containsExactlyInAnyOrderEntriesOf(Map.of("old", "a", "new", "b"));
  }

  private static IndexCacheProtocol.File file(String location, long bytes) {
    long prepared = units(bytes);
    IndexFileMemory memory =
        IndexFileMemory.builder()
            .payloadBytes(1)
            .preparedBytes(prepared)
            .modelVersion(2)
            .readerId("test-reader")
            .runtimeProfile(IndexCacheMemory.runtimeIdentity())
            .structure(java.util.Map.of())
            .build();
    return IndexCacheProtocol.File.builder()
        .location(location)
        .owner("table")
        .cacheKey("table|adapter|" + location)
        .memory(memory)
        .estimatedBytes(IndexCacheMemory.residentBytes(memory))
        .build();
  }

  private static long units(long bytes) {
    return bytes > 1_000_000L ? bytes : Math.multiplyExact(bytes, UNIT);
  }

  @Test
  void targetWithoutReadContextIsNotExecutable() {
    IndexCacheProtocol.Target bare =
        IndexCacheProtocol.Target.builder()
            .indexUuid("table#A")
            .snapshotId(1)
            .files(List.of(file("a", 1)))
            .build();
    assertThat(bare.hasReadContext()).isFalse();
    assertThat(bare.definition()).isNull();
    assertThat(bare.io()).isNull();
    assertThat(planner.plan(bare, List.of(node("a", 8, 0))).placements()).containsEntry("a", "a");
  }

  private static IndexCacheProtocol.Target target(IndexCacheProtocol.File... files) {
    return IndexCacheProtocol.Target.builder()
        .indexUuid("table#A")
        .snapshotId(1)
        .files(List.of(files))
        .definition(definition())
        .io(io)
        .build();
  }

  private static IndexDefinition definition() {
    return new IndexDefinition() {
      @Override
      public String type() {
        return "vector";
      }

      @Override
      public int fieldId() {
        return 1;
      }
    };
  }

  private static final FileIO io = new HadoopFileIO(new Configuration());

  private static long rawBudget(long budget) {
    return budget > 1_000_000L ? budget : Math.addExact(units(budget), WRAPPER);
  }

  private static IndexCacheProtocol.NodeReport node(String executor, long budget, long used) {
    return IndexCacheProtocol.NodeReport.builder()
        .session("session")
        .executorId(executor)
        .incarnation(executor + "-1")
        .budgetBytes(rawBudget(budget))
        .usedBytes(units(used))
        .build();
  }
}
