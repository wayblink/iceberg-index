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

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.index.IndexFile;
import org.apache.iceberg.index.IndexFileMemory;
import org.apache.iceberg.index.IndexManifest;
import org.apache.iceberg.index.IndexManifestList;
import org.apache.iceberg.index.vector.VectorIndexAdapter;
import org.apache.iceberg.index.vector.VectorIndexDefinition;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestIndexCacheControl {
  private static final long BASE = Duration.ofMinutes(10).toNanos();

  @TempDir private File tempDir;

  @Test
  void loadPlansEveryIndexFileWithTheKeyTheQueryBuilds() {
    TestIndexFixtures.BuiltIndex built =
        TestIndexFixtures.build(tempDir, "idx-1", "data-1.parquet", "data-2.parquet");
    IndexResidencyCoordinator coordinator = new IndexResidencyCoordinator(() -> BASE);
    IndexCacheDriverControl control = new IndexCacheDriverControl(coordinator);
    control.receive(node());

    // Planning is inspected before anything waits: a plan that is allowed to reach its deadline is
    // ended by it, which is what the deadline tests below cover.
    IndexCacheProtocol.Result result = control.result(plan(control, built, "30m"));

    assertThat(result.status()).isEqualTo(IndexCacheProtocol.Status.PENDING);
    assertThat(result.protection().deadlineNanos())
        .isEqualTo(BASE + Duration.ofMinutes(30).toNanos());
    List<String> expectedKeys =
        indexFiles(built).stream()
            .map(file -> TestIndexFixtures.cacheKeyFor(built.metadata(), file))
            .toList();
    assertThat(expectedKeys).isNotEmpty();
    assertThat(coordinator.commands("executor-1", "executor-1#1"))
        .filteredOn(c -> c.type() == IndexCacheProtocol.CommandType.LOAD)
        .extracting(c -> c.file().cacheKey())
        .containsExactlyInAnyOrderElementsOf(expectedKeys);
    assertThat(coordinator.commands("executor-1", "executor-1#1"))
        .filteredOn(c -> c.type() == IndexCacheProtocol.CommandType.LOAD)
        .allSatisfy(c -> assertThat(c.file().estimatedBytes()).isPositive());
  }

  @Test
  void omittedRetentionKeepsTheDeadlineAndExplicitZeroReplacesIt() {
    TestIndexFixtures.BuiltIndex built =
        TestIndexFixtures.build(tempDir, "idx-1", "data-1.parquet");
    IndexResidencyCoordinator coordinator = new IndexResidencyCoordinator(() -> BASE);
    IndexCacheDriverControl control = new IndexCacheDriverControl(coordinator);
    control.receive(node());

    IndexCacheProtocol.Result first =
        IndexCacheController.load(control, built.table(), built.metadata(), "30m", BASE, 0L);
    IndexCacheProtocol.Result repeated =
        IndexCacheController.load(control, built.table(), built.metadata(), null, BASE, 0L);
    assertThat(repeated.protection().deadlineNanos()).isEqualTo(first.protection().deadlineNanos());

    IndexCacheProtocol.Result zero =
        IndexCacheController.load(control, built.table(), built.metadata(), "0", BASE, 0L);
    assertThat(zero.protection().permanent()).isFalse();
    assertThat(zero.protection().deadlineNanos()).isZero();
  }

  @Test
  void foreverProtectionIsReportedAsPermanent() {
    TestIndexFixtures.BuiltIndex built =
        TestIndexFixtures.build(tempDir, "idx-1", "data-1.parquet");
    IndexResidencyCoordinator coordinator = new IndexResidencyCoordinator(() -> BASE);
    IndexCacheDriverControl control = new IndexCacheDriverControl(coordinator);
    control.receive(node());

    IndexCacheProtocol.Result result =
        IndexCacheController.load(control, built.table(), built.metadata(), "forever", BASE, 0L);

    assertThat(result.protection().permanent()).isTrue();
    assertThat(result.protection().text()).isEqualTo("forever");
  }

  @Test
  void offloadIsIdempotentAndReportsTheIndex() {
    TestIndexFixtures.BuiltIndex built =
        TestIndexFixtures.build(tempDir, "idx-1", "data-1.parquet");
    IndexResidencyCoordinator coordinator = new IndexResidencyCoordinator(() -> BASE);
    IndexCacheDriverControl control = new IndexCacheDriverControl(coordinator);
    control.receive(node());
    IndexCacheController.load(control, built.table(), built.metadata(), "forever", BASE, 0L);

    IndexCacheProtocol.Result first =
        IndexCacheController.offload(control, built.table(), built.metadata(), BASE, 0L);
    IndexCacheProtocol.Result second =
        IndexCacheController.offload(control, built.table(), built.metadata(), BASE, 0L);

    assertThat(first.status()).isEqualTo(IndexCacheProtocol.Status.OFFLOADED);
    assertThat(second.status()).isEqualTo(IndexCacheProtocol.Status.OFFLOADED);
    assertThat(first.indexUuid()).isEqualTo("idx-1");
    assertThat(second.indexUuid()).isEqualTo("idx-1");
  }

  @Test
  void thePublicResultCarriesOneRowPerTargetFile() {
    TestIndexFixtures.BuiltIndex built =
        TestIndexFixtures.build(tempDir, "idx-1", "data-1.parquet");
    IndexResidencyCoordinator coordinator = new IndexResidencyCoordinator(() -> BASE);
    IndexCacheDriverControl control = new IndexCacheDriverControl(coordinator);
    control.receive(node());
    IndexCacheProtocol.Result protocol =
        IndexCacheController.load(control, built.table(), built.metadata(), "30m", BASE, 0L);

    IndexCacheControl.Result result = IndexCacheControl.Result.from(control, protocol);

    assertThat(result.indexUuid()).isEqualTo("idx-1");
    assertThat(result.snapshotId()).isEqualTo(built.table().currentSnapshot().snapshotId());
    assertThat(result.protection()).isEqualTo("30m");
    assertThat(result.permanent()).isFalse();
    assertThat(result.files()).isNotEmpty();
    assertThat(result.files())
        .allSatisfy(row -> assertThat(row.executorId()).isEqualTo("executor-1"));
    assertThat(result.files().get(0).fileLocation()).isIn(indexFiles(built));
  }

  @Test
  void managedIndexesListsWhatHasAProtection() {
    TestIndexFixtures.BuiltIndex built =
        TestIndexFixtures.build(tempDir, "idx-1", "data-1.parquet");
    IndexResidencyCoordinator coordinator = new IndexResidencyCoordinator(() -> BASE);
    IndexCacheDriverControl control = new IndexCacheDriverControl(coordinator);
    control.receive(node());
    IndexCacheController.load(control, built.table(), built.metadata(), "0", BASE, 0L);

    assertThat(coordinator.protectedIndexes()).containsKey("idx-1");
  }

  @Test
  void statusReportsEachExecutorsOccupancyAndManagedIndexes() {
    TestIndexFixtures.BuiltIndex built =
        TestIndexFixtures.build(tempDir, "idx-1", "data-1.parquet");
    IndexResidencyCoordinator coordinator = new IndexResidencyCoordinator(() -> BASE);
    IndexCacheDriverControl control = new IndexCacheDriverControl(coordinator);
    control.receive(node());
    IndexCacheController.load(control, built.table(), built.metadata(), "30m", BASE, 0L);

    List<IndexCacheControl.Status> status = IndexCacheControl.statusOf(control, BASE);

    assertThat(status).hasSize(1);
    IndexCacheControl.Status row = status.get(0);
    assertThat(row.executorId()).isEqualTo("executor-1");
    assertThat(row.incarnation()).isEqualTo("executor-1#1");
    assertThat(row.budgetBytes()).isEqualTo(1_000_000L);
    assertThat(row.managedIndexes()).containsExactly("idx-1");
    assertThat(row.stale()).isFalse();
  }

  @Test
  void aLoadWaitsForTheAcknowledgementsInsteadOfReturningPending() throws Exception {
    TestIndexFixtures.BuiltIndex built =
        TestIndexFixtures.build(tempDir, "idx-1", "data-1.parquet");
    IndexResidencyCoordinator coordinator = new IndexResidencyCoordinator(() -> BASE);
    IndexCacheDriverControl control = new IndexCacheDriverControl(coordinator);
    control.receive(node());

    Thread acknowledger =
        new Thread(
            () -> {
              sleep(200);
              // The driver hands the command out on the next report, then the executor answers.
              IndexCacheProtocol.Reply reply = control.receive(node(2L));
              for (IndexCacheProtocol.Command command : reply.commands()) {
                ExecutorIndexCache.Shape charged =
                    ExecutorIndexCache.Shape.empty()
                        .project(command.indexUuid(), command.epoch(), List.of(command.file()));
                control.receive(
                    IndexCacheProtocol.Ack.builder()
                        .session("application-1")
                        .planId(command.planId())
                        .commandId(command.commandId())
                        .executorId("executor-1")
                        .incarnation("executor-1#1")
                        .epoch(command.epoch())
                        .sequence(command.sequence())
                        .status(IndexCacheProtocol.AckStatus.LOADED)
                        .bytes(
                            command.file().estimatedBytes() == null
                                ? 0
                                : command.file().estimatedBytes())
                        .chargeVersion(charged.charge().version())
                        .controlBytes(charged.charge().controlBytes())
                        .build());
              }
            });
    acknowledger.start();

    IndexCacheProtocol.Result result =
        IndexCacheController.load(
            control,
            built.table(),
            built.metadata(),
            "30m",
            BASE,
            System.nanoTime() + 10_000_000_000L);
    acknowledger.join(10_000);

    assertThat(result.status()).isEqualTo(IndexCacheProtocol.Status.COMPLETE);
    assertThat(result.residentBytes()).isPositive();
  }

  @Test
  void aSlowTargetDoesNotSpendTheWaitWindowBeforeThePlanIsAccepted() {
    // Resolving the target (listing its files and estimating the memory of each) runs on the
    // driver before the plan exists, and takes minutes for an index of 6.3 GB in 256 files. The
    // window is for waiting on acknowledgements, so it starts when the plan is accepted: charging
    // it from the call's own start fences the plan the moment it is accepted.
    TestIndexFixtures.BuiltIndex built =
        TestIndexFixtures.build(tempDir, "idx-late", "data-1.parquet", "data-2.parquet");
    MutableClock clock = new MutableClock(BASE);
    IndexResidencyCoordinator coordinator = new IndexResidencyCoordinator(clock);
    IndexCacheDriverControl control = new IndexCacheDriverControl(coordinator);
    control.receive(node());

    // The plan is accepted now, but the call began five minutes ago: that is what planning cost.
    long entry = BASE - Duration.ofMinutes(5).toNanos();
    Thread executor = slowExecutor(control, coordinator, 100L);

    IndexCacheProtocol.Result result =
        IndexCacheController.load(control, built.table(), built.metadata(), "30m", entry);

    executor.interrupt();
    assertThat(result.targetFiles()).hasSize(indexFiles(built).size());
    assertThat(result.status()).isEqualTo(IndexCacheProtocol.Status.COMPLETE);
    assertThat(result.uncovered()).isEmpty();
  }

  @Test
  void aPlanThatPassesItsDeadlineIsFencedInsteadOfStayingPending() {
    TestIndexFixtures.BuiltIndex built =
        TestIndexFixtures.build(tempDir, "idx-deadline", "data-1.parquet", "data-2.parquet");
    MutableClock clock = new MutableClock(BASE);
    IndexResidencyCoordinator coordinator = new IndexResidencyCoordinator(clock);
    IndexCacheDriverControl control = new IndexCacheDriverControl(coordinator);
    control.receive(node());
    String planId =
        control.acceptLoad(
            IndexCacheController.target(built.table(), built.metadata()),
            IndexResidencyPolicy.parse("30m"),
            clock.nowNanos());
    assertThat(control.result(planId).status()).isEqualTo(IndexCacheProtocol.Status.PENDING);
    // the space the plan promised is what the next load would count as taken
    assertThat(coordinator.state("executor-1", "executor-1#1").reservedByCommand()).isNotEmpty();

    clock.advance(Duration.ofMinutes(1).toNanos());
    IndexCacheProtocol.Result result = IndexCacheController.await(control, planId, 0L);

    // A deadline that ends the wait must also end the plan: the executor never answered, so the
    // files are not resident and their reservations may not outlive the caller's patience.
    assertThat(result.status()).isEqualTo(IndexCacheProtocol.Status.FAILED);
    assertThat(result.uncovered()).hasSize(indexFiles(built).size());
    assertThat(result.uncovered().values()).containsOnly("TIMED_OUT");
    assertThat(coordinator.state("executor-1", "executor-1#1").reservedByCommand()).isEmpty();
    assertThat(coordinator.state("executor-1", "executor-1#1").reservedByPlan()).isEmpty();
  }

  @Test
  void aDeadExecutorDoesNotKeepAPlanInFlightPastItsDeadline() {
    // refresh() only decides a node is gone once nothing is queued for it, so a node that never
    // answers its command leaves the plan waiting; the deadline is the only end of that wait.
    TestIndexFixtures.BuiltIndex built =
        TestIndexFixtures.build(tempDir, "idx-dead", "data-1.parquet");
    MutableClock clock = new MutableClock(BASE);
    IndexResidencyCoordinator coordinator = new IndexResidencyCoordinator(clock);
    IndexCacheDriverControl control = new IndexCacheDriverControl(coordinator);
    control.receive(node());
    String planId =
        control.acceptLoad(
            IndexCacheController.target(built.table(), built.metadata()),
            IndexResidencyPolicy.parse("30m"),
            clock.nowNanos());

    clock.advance(IndexResidencyCoordinator.STALE_AFTER_NANOS + Duration.ofSeconds(1).toNanos());
    assertThat(control.hasLiveExecutors(clock.nowNanos())).isFalse();
    IndexCacheProtocol.Result result = IndexCacheController.await(control, planId, 0L);

    assertThat(result.status()).isEqualTo(IndexCacheProtocol.Status.FAILED);
    assertThat(result.uncovered().values()).containsOnly("TIMED_OUT");
    assertThat(coordinator.state("executor-1", "executor-1#1").reservedByCommand()).isEmpty();
    assertThat(coordinator.state("executor-1", "executor-1#1").reservedByPlan()).isEmpty();
  }

  @Test
  void fileRowsCarryThePlannedBytes() {
    TestIndexFixtures.BuiltIndex built =
        TestIndexFixtures.build(tempDir, "idx-1", "data-1.parquet");
    IndexCacheDriverControl control =
        new IndexCacheDriverControl(new IndexResidencyCoordinator(() -> BASE));
    control.receive(node());
    IndexCacheProtocol.Result protocol =
        IndexCacheController.load(control, built.table(), built.metadata(), "30m", BASE, 0L);

    IndexCacheControl.Result result = IndexCacheControl.Result.from(control, protocol);

    assertThat(result.files()).allSatisfy(row -> assertThat(row.bytes()).isPositive());
  }

  @Test
  void anIndexOfAnotherTableIsRejected() {
    TestIndexFixtures.BuiltIndex built =
        TestIndexFixtures.build(tempDir, "idx-1", "data-1.parquet");
    TestIndexFixtures.BuiltIndex other =
        TestIndexFixtures.build(new File(tempDir, "other"), "idx-2", "data-1.parquet");
    IndexCacheDriverControl control =
        new IndexCacheDriverControl(new IndexResidencyCoordinator(() -> BASE));
    control.receive(node());

    assertThatThrownBy(
            () ->
                IndexCacheController.load(control, other.table(), built.metadata(), "0", BASE, 0L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("belongs to table");
  }

  private static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @Test
  void aSearchKeyIsTheSameStringThePlanUses() {
    // The search path leases with this key, so it has to be the key the plan stored. Both now build
    // it through the same method; this compares the two strings rather than a constructor.
    TestIndexFixtures.BuiltIndex built =
        TestIndexFixtures.build(tempDir, "idx-1", "data-1.parquet");
    IndexCacheDriverControl control =
        new IndexCacheDriverControl(new IndexResidencyCoordinator(() -> BASE));
    control.receive(node());
    IndexCacheProtocol.Result load =
        IndexCacheController.load(control, built.table(), built.metadata(), "forever", BASE, 0L);

    String searchKey =
        DistributedIndexSearch.cacheKey(
            built.table().location(),
            TestIndexFixtures.adapterKeyFor(built.metadata()),
            load.targetFiles().get(0));

    String planKey =
        IndexCacheController.cacheKey(built.table(), built.metadata(), load.targetFiles().get(0));
    assertThat(searchKey).isEqualTo(planKey);
    assertThat(searchKey).isEqualTo(planKey);
  }

  @Test
  void aFileBeingReleasedSaysSoInThePublicRows() {
    // RELEASING has no uncovered reason of its own; without this fallback the row a caller sees
    // would say nothing about the file it is still waiting for.
    TestIndexFixtures.BuiltIndex built =
        TestIndexFixtures.build(tempDir, "idx-1", "data-1.parquet");
    IndexResidencyCoordinator coordinator = new IndexResidencyCoordinator(() -> BASE);
    IndexCacheDriverControl control = new IndexCacheDriverControl(coordinator);
    control.receive(node());
    IndexCacheProtocol.Result load = control.result(plan(control, built, "forever"));
    IndexCacheProtocol.Command command =
        coordinator.commands("executor-1", "executor-1#1").stream()
            .filter(c -> c.type() == IndexCacheProtocol.CommandType.LOAD)
            .findFirst()
            .orElseThrow();
    control.receive(
        ack(
            load.planId(),
            command,
            IndexCacheProtocol.AckStatus.LOADED,
            command.file().estimatedBytes()));

    IndexCacheProtocol.Result offload =
        IndexCacheController.offload(control, built.table(), built.metadata(), BASE, 0L);
    IndexCacheControl.Result rows = IndexCacheControl.Result.from(control, offload);

    assertThat(rows.status()).isEqualTo("RELEASING");
    assertThat(rows.files()).isNotEmpty();
    assertThat(rows.files())
        .allSatisfy(row -> assertThat(row.reason()).isEqualTo("RELEASE_PENDING"));
  }

  @Test
  void offloadNamesTheFilesItIsReleasingUntilTheyAnswer() {
    TestIndexFixtures.BuiltIndex built =
        TestIndexFixtures.build(tempDir, "idx-1", "data-1.parquet");
    IndexResidencyCoordinator coordinator = new IndexResidencyCoordinator(() -> BASE);
    IndexCacheDriverControl control = new IndexCacheDriverControl(coordinator);
    control.receive(node());
    IndexCacheProtocol.Result load = control.result(plan(control, built, "forever"));
    IndexCacheProtocol.Command command = coordinator.commands("executor-1", "executor-1#1").get(0);
    control.receive(
        IndexCacheProtocol.Ack.builder()
            .session("application-1")
            .planId(load.planId())
            .commandId(command.commandId())
            .executorId("executor-1")
            .incarnation("executor-1#1")
            .epoch(command.epoch())
            .sequence(command.sequence())
            .status(IndexCacheProtocol.AckStatus.LOADED)
            .bytes(command.file().estimatedBytes())
            .build());

    IndexCacheProtocol.Result offload =
        IndexCacheController.offload(control, built.table(), built.metadata(), BASE, 0L);

    // A copy was acknowledged, so the release has to be confirmed before this is done.
    assertThat(offload.status()).isEqualTo(IndexCacheProtocol.Status.RELEASING);
    assertThat(offload.targetFiles()).isNotEmpty();
    assertThat(offload.releasePending().values()).containsOnly("RELEASE_PENDING");

    IndexCacheProtocol.Command release =
        coordinator.commands("executor-1", "executor-1#1").stream()
            .filter(c -> c.type() == IndexCacheProtocol.CommandType.RELEASE)
            .findFirst()
            .orElseThrow();
    control.receive(
        IndexCacheProtocol.Ack.builder()
            .session("application-1")
            .planId(offload.planId())
            .commandId(release.commandId())
            .executorId("executor-1")
            .incarnation("executor-1#1")
            .epoch(release.epoch())
            .sequence(release.sequence())
            .status(IndexCacheProtocol.AckStatus.RELEASED)
            .bytes(0)
            .build());

    assertThat(control.result(offload.planId()).status())
        .isEqualTo(IndexCacheProtocol.Status.OFFLOADED);
  }

  @Test
  void indexStatusReportsProtectionAndAccessAge() {
    TestIndexFixtures.BuiltIndex built =
        TestIndexFixtures.build(tempDir, "idx-1", "data-1.parquet");
    IndexCacheDriverControl control =
        new IndexCacheDriverControl(new IndexResidencyCoordinator(() -> BASE));
    control.receive(node());
    IndexCacheController.load(control, built.table(), built.metadata(), "forever", BASE, 0L);
    control.recordAccess("idx-1", BASE + 5_000_000L);

    List<IndexCacheControl.IndexStatus> rows =
        IndexCacheControl.indexStatusOf(control, BASE + 5_000_000L);

    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).indexUuid()).isEqualTo("idx-1");
    assertThat(rows.get(0).permanent()).isTrue();
    assertThat(rows.get(0).accessAgeMillis()).isZero();
  }

  @Test
  void anIndexNoQueryHasUsedReportsNoAccessAge() {
    TestIndexFixtures.BuiltIndex built =
        TestIndexFixtures.build(tempDir, "idx-1", "data-1.parquet");
    IndexCacheDriverControl control =
        new IndexCacheDriverControl(new IndexResidencyCoordinator(() -> BASE));
    control.receive(node());
    IndexCacheController.load(control, built.table(), built.metadata(), "forever", BASE, 0L);

    List<IndexCacheControl.IndexStatus> rows = IndexCacheControl.indexStatusOf(control, BASE);

    // -1 means "never used"; 0 would claim a query used it on this very clock tick.
    assertThat(rows.get(0).accessAgeMillis()).isEqualTo(-1L);
  }

  @Test
  void missingControlChannelNamesThePlugin() {
    assertThatThrownBy(() -> IndexCacheDriverRegistry.require(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("spark.plugins");
  }

  @Test
  void loadRejectsATableWithoutACurrentSnapshot() {
    File emptyDir = new File(tempDir, "empty");
    org.apache.iceberg.TestTables.TestTable table =
        org.apache.iceberg.TestTables.create(
            emptyDir,
            "empty" + System.nanoTime(),
            TestIndexFixtures.SCHEMA,
            org.apache.iceberg.PartitionSpec.unpartitioned(),
            2);
    IndexCacheDriverControl control =
        new IndexCacheDriverControl(new IndexResidencyCoordinator(() -> BASE));
    control.receive(node());

    assertThatThrownBy(
            () ->
                IndexCacheController.load(
                    control, table, TestIndexFixtures.metadataFor(table, "idx-empty"), "0", BASE))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("snapshot");
  }

  @Test
  void readContextRefusesIncompatibleMemoryBeforePayload() {
    TestIndexFixtures.BuiltIndex built =
        TestIndexFixtures.build(tempDir, "profile-check", "data.parquet");
    AtomicInteger reads = new AtomicInteger();
    FileIO guarded =
        new FileIO() {
          @Override
          public InputFile newInputFile(String path) {
            reads.incrementAndGet();
            throw new AssertionError("Incompatible memory must be refused before file IO");
          }

          @Override
          public OutputFile newOutputFile(String path) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void deleteFile(String path) {
            throw new UnsupportedOperationException();
          }
        };
    IndexFileMemory memory =
        IndexFileMemory.builder()
            .payloadBytes(1)
            .preparedBytes(80)
            .modelVersion(2)
            .readerId("old-reader")
            .runtimeProfile("old-runtime")
            .build();
    IndexCacheProtocol.Command command =
        IndexCacheProtocol.Command.builder()
            .session("session")
            .executorId("executor")
            .incarnation("incarnation")
            .indexUuid("profile-check")
            .epoch(1)
            .planId("plan")
            .commandId("command")
            .type(IndexCacheProtocol.CommandType.LOAD)
            .file(
                IndexCacheProtocol.File.builder()
                    .location(indexFiles(built).get(0))
                    .cacheKey("key")
                    .estimatedBytes(IndexCacheMemory.residentBytes(memory))
                    .memory(memory)
                    .build())
            .definition(built.metadata().definition())
            .io(guarded)
            .build();
    assertThatThrownBy(() -> new IndexCacheReadContext().load(command))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("memory");
    assertThat(reads).hasValue(0);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 99})
  void targetReestimatesIncompatibleManifestMemory(int model) throws IOException {
    TestIndexFixtures.BuiltIndex built =
        TestIndexFixtures.build(tempDir, "stale-memory", "data.parquet");
    String listLocation = built.metadata().snapshots().get(0).indexManifestList();
    String manifestLocation =
        IndexManifestList.read(built.table().io().newInputFile(listLocation))
            .manifests()
            .get(0)
            .location();
    List<IndexFile> rewritten = new ArrayList<>();
    for (IndexFile file :
        IndexManifest.read(built.table().io().newInputFile(manifestLocation)).indexFiles()) {
      rewritten.add(
          IndexFile.builder()
              .location(file.location())
              .kind(file.kind())
              .blobType(file.blobType())
              .entryCount(file.entryCount())
              .coveredDataFiles(file.coveredDataFiles())
              .memory(
                  file.memory().toBuilder()
                      .preparedBytes(1)
                      .modelVersion(model)
                      .readerId("old-reader")
                      .runtimeProfile("old-runtime")
                      .build())
              .build());
    }
    built.table().io().deleteFile(manifestLocation);
    IndexManifest.write(built.table().io().newOutputFile(manifestLocation), rewritten);

    VectorIndexAdapter adapter =
        new VectorIndexAdapter((VectorIndexDefinition) built.metadata().definition());
    DistributedIndexSearch.LoadedFile loaded =
        DistributedIndexSearch.loadFileState(
            built.table().io(), rewritten.get(0).location(), adapter);
    IndexCacheProtocol.File planned =
        IndexCacheController.target(built.table(), built.metadata()).files().get(0);
    assertThat(planned.estimatedBytes()).isEqualTo(loaded.file().bytes());
    assertThat(planned.memory()).isNotNull();
    assertThat(planned.memory().modelVersion()).isEqualTo(2);
    assertThat(planned.memory().preparedBytes()).isEqualTo(loaded.preparedBytes());
    assertThat(planned.memory().readerId()).isNotEqualTo("old-reader");
    assertThat(planned.memory().runtimeProfile()).isNotEqualTo("old-runtime");
  }

  private static List<String> indexFiles(TestIndexFixtures.BuiltIndex built) {
    return new org.apache.iceberg.index.IndexReader(built.metadata(), built.table().io())
            .indexFiles(
                org.apache.iceberg.index.IndexQuery.of(
                    built.table().currentSnapshot().snapshotId(),
                    java.nio.ByteBuffer.allocate(0),
                    1))
            .stream()
            .map(org.apache.iceberg.index.IndexFile::location)
            .toList();
  }

  /**
   * Plans a load without waiting for it, so a test can inspect what the plan issued. Waiting is a
   * separate concern: a plan whose deadline passes is ended by it.
   */
  private static String plan(
      IndexCacheDriverControl control, TestIndexFixtures.BuiltIndex built, String retainFor) {
    return control.acceptLoad(
        IndexCacheController.target(built.table(), built.metadata()),
        IndexResidencyPolicy.parse(retainFor),
        BASE);
  }

  private static IndexCacheProtocol.Ack ack(
      String planId,
      IndexCacheProtocol.Command command,
      IndexCacheProtocol.AckStatus status,
      long bytes) {
    return ack(command.executorId(), command.incarnation(), planId, command, status, bytes);
  }

  private static IndexCacheProtocol.Ack ack(
      String executorId,
      String incarnation,
      String planId,
      IndexCacheProtocol.Command command,
      IndexCacheProtocol.AckStatus status) {
    return ack(executorId, incarnation, planId, command, status, 0L);
  }

  private static IndexCacheProtocol.Ack ack(
      String executorId,
      String incarnation,
      String planId,
      IndexCacheProtocol.Command command,
      IndexCacheProtocol.AckStatus status,
      long bytes) {
    IndexCacheProtocol.Ack.Builder ack =
        IndexCacheProtocol.Ack.builder()
            .session("application-1")
            .planId(planId)
            .commandId(command.commandId())
            .executorId(executorId)
            .incarnation(incarnation)
            .epoch(command.epoch())
            .sequence(command.sequence())
            .status(status)
            .bytes(bytes);
    if (status == IndexCacheProtocol.AckStatus.LOADED) {
      // A loaded copy states the counters of the executor's capacity after it, not the state.
      ExecutorIndexCache.Shape charged =
          ExecutorIndexCache.Shape.empty()
              .project(command.indexUuid(), command.epoch(), List.of(command.file()));
      ack.chargeVersion(charged.charge().version()).controlBytes(charged.charge().controlBytes());
    } else if (status == IndexCacheProtocol.AckStatus.RELEASED) {
      ExecutorIndexCache.Shape withdrawn =
          ExecutorIndexCache.Shape.empty()
              .project(command.indexUuid(), command.epoch(), List.of(command.file()))
              .releaseKeys(command.indexUuid(), Set.of(command.file().cacheKey()));
      ack.chargeVersion(withdrawn.charge().version())
          .controlBytes(withdrawn.charge().controlBytes());
    }
    return ack.build();
  }

  /** Answers pending load commands one at a time, spaced out, the way a slow executor does. */
  private static Thread slowExecutor(
      IndexCacheDriverControl control, IndexResidencyCoordinator coordinator, long gapMillis) {
    Thread thread =
        new Thread(
            () -> {
              Set<String> answered = new HashSet<>();
              try {
                while (!Thread.currentThread().isInterrupted()) {
                  List<IndexCacheProtocol.Command> pending = new ArrayList<>();
                  for (IndexCacheProtocol.Command command :
                      coordinator.commands("executor-1", "executor-1#1")) {
                    if (command.type() == IndexCacheProtocol.CommandType.LOAD
                        && answered.add(command.commandId())) {
                      pending.add(command);
                    }
                  }

                  for (IndexCacheProtocol.Command command : pending) {
                    Thread.sleep(gapMillis);
                    control.receive(
                        ack(
                            command.planId(),
                            command,
                            IndexCacheProtocol.AckStatus.LOADED,
                            command.file().estimatedBytes()));
                  }

                  Thread.sleep(10L);
                }
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            });
    thread.setDaemon(true);
    thread.start();
    return thread;
  }

  /** A clock the test moves by hand, so a deadline passes without waiting for it. */
  private static final class MutableClock implements IndexResidencyCoordinator.Clock {
    private long now;

    private MutableClock(long now) {
      this.now = now;
    }

    private void advance(long nanos) {
      this.now += nanos;
    }

    @Override
    public long nowNanos() {
      return now;
    }
  }

  private static IndexCacheProtocol.NodeReport nodeFor(String executorId, long sequence) {
    return IndexCacheProtocol.NodeReport.builder()
        .session("application-1")
        .executorId(executorId)
        .incarnation(executorId + "#1")
        .sequence(sequence)
        .budgetBytes(1_000_000L)
        .usedBytes(0L)
        .metadataShape(ExecutorIndexCache.Shape.empty())
        .build();
  }

  private static IndexCacheProtocol.NodeReport node() {
    return node(1L);
  }

  private static IndexCacheProtocol.NodeReport node(long sequence) {
    return IndexCacheProtocol.NodeReport.builder()
        .session("application-1")
        .executorId("executor-1")
        .incarnation("executor-1#1")
        .sequence(sequence)
        .budgetBytes(1_000_000L)
        .usedBytes(0L)
        .metadataShape(ExecutorIndexCache.Shape.empty())
        .build();
  }
}
