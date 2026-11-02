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
  private static final long UNIT = 1L << 20;
  private static final long WRAPPER = 168L;
  private static final long SLACK = UNIT;

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
    // An executor that states no shape is unknown capacity, not free space: the planner refuses to
    // place rather than assuming the reservations it states leave room. The reservation and pending
    // release the report carries are what the coordinator subtracts from a known shape.
    assertThat(planner.plan(target(file("new", 2)), List.of(node("a", 8, 8))).rejected())
        .containsEntry("new", "INSUFFICIENT_CAPACITY");
    IndexCacheProtocol.NodeReport bare =
        IndexCacheProtocol.NodeReport.builder()
            .session("session")
            .executorId("a")
            .incarnation("a-1")
            .budgetBytes(rawBudget(8))
            .usedBytes(units(4))
            .reservedBytes(units(4))
            .metadataShape(ExecutorIndexCache.Shape.empty().unknownCharge())
            .build();
    assertThat(planner.plan(target(file("new", 2)), List.of(bare)).rejected())
        .containsEntry("new", "SIZE_UNKNOWN");
  }

  @Test
  void aNodeWhoseNumbersThisJvmCannotModelIsNotReportedAsFull() {
    // Another JVM family is a different matter from another processor count: the shape cannot be
    // projected here at all, so its free space is unknown rather than small.
    IndexCacheProtocol.NodeReport foreign =
        IndexCacheProtocol.NodeReport.builder()
            .session("session")
            .executorId("a")
            .incarnation("a-1")
            .budgetBytes(rawBudget(512))
            .metadataShape(
                ExecutorIndexCache.Shape.empty()
                    .withRuntime("vm=Another VM;version=0;processors=8"))
            .build();
    assertThat(planner.plan(target(file("new", 2)), List.of(foreign)).rejected())
        .containsEntry("new", "RUNTIME_MISMATCH");
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
            .metadataShape(ExecutorIndexCache.Shape.empty())
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
    IndexCacheProtocol.NodeReport holder = holding("a", 8, 8, "reused", 8);
    assertThat(planner.plan(target(file("reused", 8)), List.of(holder)).placements())
        .containsEntry("reused", "a");
  }

  @Test
  void balancesExistingFileOnlyWithRoomForTheSecondCopy() {
    IndexCacheProtocol.NodeReport holder = holding("a", 8, 8, "reused", 4);
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
            List.of(
                report("a", Long.MAX_VALUE / 2, Long.MAX_VALUE / 2),
                report("b", Long.MAX_VALUE, 0)));
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
    IndexCacheProtocol.NodeReport holder = holding("a", 16, 8, "reused", 4);
    assertThat(
            planner.plan(target(file("reused", 4)), List.of(holder, node("b", 4, 0))).placements())
        .containsEntry("reused", "a");
  }

  @Test
  void migrationCannotConsumeRoomNeededForAnUncoveredFile() {
    IndexCacheProtocol.NodeReport holder = holding("a", 8, 8, "old", 8);
    assertThat(
            planner
                .plan(target(file("old", 8), file("new", 6)), List.of(holder, node("b", 7, 0)))
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
    return budget > 1_000_000L
        ? budget
        : Math.addExact(Math.addExact(units(budget), WRAPPER), SLACK);
  }

  private static IndexCacheProtocol.NodeReport node(String executor, long budget, long used) {
    return IndexCacheProtocol.NodeReport.builder()
        .session("session")
        .executorId(executor)
        .incarnation(executor + "-1")
        .budgetBytes(rawBudget(budget))
        .metadataShape(shape(used))
        .build();
  }

  @Test
  void aShapeFromAnExecutorWithAnotherProcessorCountIsStillPlaced() {
    // The driver plans for executors it does not share a processor count with. That difference is a
    // prepared-state concern, not a reason to call a 2 MiB file too large for an empty 8 GiB node.
    String foreignRuntime =
        IndexCacheMemory.runtimeIdentity().replaceAll(";processors=\\d+", ";processors=1");
    IndexCacheProtocol.NodeReport report =
        IndexCacheProtocol.NodeReport.builder()
            .session("session")
            .executorId("executor-1")
            .incarnation("executor-1-1")
            .budgetBytes(rawBudget(8))
            .metadataShape(ExecutorIndexCache.Shape.empty().withRuntime(foreignRuntime))
            .build();

    IndexCacheProtocol.Plan plan = planner.plan(target(file("new", 2)), List.of(report));

    assertThat(plan.placements()).containsEntry("new", "executor-1");
    assertThat(plan.rejected()).isEmpty();
  }

  private static ExecutorIndexCache.Shape shape(long units) {
    return units == 0
        ? ExecutorIndexCache.Shape.empty()
        : ExecutorIndexCache.Shape.empty()
            .project("table#A", 1, List.of(file("held-" + units, units)));
  }

  private static IndexCacheProtocol.NodeReport report(String executor, long budget, long used) {
    return IndexCacheProtocol.NodeReport.builder()
        .session("session")
        .executorId(executor)
        .incarnation(executor + "-1")
        .budgetBytes(rawBudget(budget))
        .usedBytes(units(used))
        .metadataShape(ExecutorIndexCache.Shape.empty())
        .build();
  }

  private static IndexCacheProtocol.NodeReport holding(
      String executor, long budget, long used, String location, long fileUnits) {
    IndexCacheProtocol.File held = file(location, fileUnits);
    List<IndexCacheProtocol.File> files = new ArrayList<>();
    files.add(held);
    if (used > fileUnits) {
      files.add(file("padding-" + location, used - fileUnits));
    }
    ExecutorIndexCache.Shape shape = ExecutorIndexCache.Shape.empty().project("table#A", 1, files);
    return IndexCacheProtocol.NodeReport.builder()
        .session("session")
        .executorId(executor)
        .incarnation(executor + "-1")
        .budgetBytes(rawBudget(budget))
        .metadataShape(shape)
        .build();
  }
}
