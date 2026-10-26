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
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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

    IndexCacheProtocol.Result result =
        IndexCacheController.load(control, built.table(), built.metadata(), "30m", BASE, BASE);

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
        IndexCacheController.load(control, built.table(), built.metadata(), "30m", BASE, BASE);
    IndexCacheProtocol.Result repeated =
        IndexCacheController.load(control, built.table(), built.metadata(), null, BASE, BASE);
    assertThat(repeated.protection().deadlineNanos()).isEqualTo(first.protection().deadlineNanos());

    IndexCacheProtocol.Result zero =
        IndexCacheController.load(control, built.table(), built.metadata(), "0", BASE, BASE);
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
        IndexCacheController.load(control, built.table(), built.metadata(), "forever", BASE, BASE);

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
    IndexCacheController.load(control, built.table(), built.metadata(), "forever", BASE, BASE);

    IndexCacheProtocol.Result first =
        IndexCacheController.offload(control, built.table(), built.metadata(), BASE);
    IndexCacheProtocol.Result second =
        IndexCacheController.offload(control, built.table(), built.metadata(), BASE);

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
        IndexCacheController.load(control, built.table(), built.metadata(), "30m", BASE, BASE);

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
    IndexCacheController.load(control, built.table(), built.metadata(), "0", BASE, BASE);

    assertThat(coordinator.protectedIndexes()).containsKey("idx-1");
  }

  @Test
  void statusReportsEachExecutorsOccupancyAndManagedIndexes() {
    TestIndexFixtures.BuiltIndex built =
        TestIndexFixtures.build(tempDir, "idx-1", "data-1.parquet");
    IndexResidencyCoordinator coordinator = new IndexResidencyCoordinator(() -> BASE);
    IndexCacheDriverControl control = new IndexCacheDriverControl(coordinator);
    control.receive(node());
    IndexCacheController.load(control, built.table(), built.metadata(), "30m", BASE, BASE);

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
  void fileRowsCarryThePlannedBytes() {
    TestIndexFixtures.BuiltIndex built =
        TestIndexFixtures.build(tempDir, "idx-1", "data-1.parquet");
    IndexCacheDriverControl control =
        new IndexCacheDriverControl(new IndexResidencyCoordinator(() -> BASE));
    control.receive(node());
    IndexCacheProtocol.Result protocol =
        IndexCacheController.load(control, built.table(), built.metadata(), "30m", BASE, BASE);

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
                IndexCacheController.load(
                    control, other.table(), built.metadata(), "0", BASE, BASE))
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
  void missingControlChannelNamesThePlugin() {
    assertThatThrownBy(() -> IndexCacheDriverRegistry.require("no-such-application"))
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
        .build();
  }
}
