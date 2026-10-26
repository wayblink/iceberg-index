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

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iceberg.TestTables.LocalFileIO;
import org.apache.iceberg.index.spark.IndexBuildStatus.State;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * What a build of an index reports about itself, and how that report survives being asked later.
 *
 * <p>The report is a file beside the index rather than a table property, so two things rest on the
 * file alone: what the last report said, and what a reader gets when it arrives in the middle of a
 * write. Neither may fail a build, which is why the reporter swallows its own write failures.
 */
class TestIndexBuildStatus {

  private static final String INDEX_UUID = "11111111-2222-3333-4444-555555555555";
  private static final String COLUMN = "embedding";
  private static final long SOURCE_SNAPSHOT = 42L;
  private static final long STALE_AFTER = 5L * 60L * 1000L;

  @TempDir Path temp;

  private FileIO io;
  private String indexLocation;
  private String statusDir;

  @BeforeEach
  void before() {
    io = new LocalFileIO();
    indexLocation = temp.toUri().toString() + "/indexes/index-1";
    statusDir = IndexBuildStatus.statusDirFor(indexLocation);
  }

  @AfterEach
  void after() throws Exception {
    io.close();
  }

  @Test
  void thereIsNoStatusBeforeABuildReportsOne() {
    assertThat(IndexBuildStatus.read(io, statusDir)).isNull();
  }

  @Test
  void theLatestReportIsTheOneTheReaderGets() {
    IndexBuildStatus running = running(3L, 10L, 0L, 1000L);
    IndexBuildStatus.write(io, statusDir, running);
    assertThat(IndexBuildStatus.read(io, statusDir)).isEqualTo(running);

    IndexBuildStatus succeeded = running.succeeded(3L, 120L, 2000L);
    IndexBuildStatus.write(io, statusDir, succeeded);
    assertThat(IndexBuildStatus.read(io, statusDir)).isEqualTo(succeeded);
  }

  @Test
  void aReportThatCannotBeParsedIsIgnoredRatherThanFatal() throws Exception {
    IndexBuildStatus running = running(3L, 10L, 0L, 1000L);
    IndexBuildStatus.write(io, statusDir, running);

    // both slots cut off mid-write, which is what a killed driver leaves behind
    for (int slot = 0; slot < 2; slot += 1) {
      try (PositionOutputStream out = io.newOutputFile(slotPath(slot)).createOrOverwrite()) {
        out.write("{\"state\": \"RUN".getBytes(StandardCharsets.UTF_8));
      }
    }

    assertThat(IndexBuildStatus.read(io, statusDir)).isNull();

    // and the next build's report is readable again
    IndexBuildStatus next = running(1L, 4L, 0L, 5000L);
    IndexBuildStatus.write(io, statusDir, next);
    assertThat(IndexBuildStatus.read(io, statusDir)).isEqualTo(next);
  }

  @Test
  void aFailedBuildKeepsTheReasonItFailed() {
    IndexBuildStatus failed =
        IndexBuildStatus.builder()
            .state(State.FAILED)
            .indexUuid(INDEX_UUID)
            .column(COLUMN)
            .sourceSnapshotId(SOURCE_SNAPSHOT)
            .startedAtMillis(1000L)
            .updatedAtMillis(2000L)
            .filesBuilt(4L)
            .filesTotal(10L)
            .filesFailed(1L)
            .rows(900L)
            .error("java.lang.IllegalStateException: the kernel rejected the payload")
            .build();

    IndexBuildStatus.write(io, statusDir, failed);
    IndexBuildStatus read = IndexBuildStatus.read(io, statusDir);

    assertThat(read.state()).isEqualTo(State.FAILED);
    assertThat(read.error()).contains("the kernel rejected the payload");
    assertThat(read.filesFailed()).isEqualTo(1L);
    assertThat(read.filesBuilt()).isEqualTo(4L);
    assertThat(read.filesTotal()).isEqualTo(10L);
    assertThat(read.rows()).isEqualTo(900L);
  }

  @Test
  void aLongFailureReasonIsTruncated() {
    StringBuilder longMessage = new StringBuilder();
    for (int i = 0; i < 2000; i += 1) {
      longMessage.append("the kernel rejected the payload; ");
    }

    IndexBuildStatus failed = running(0L, 1L, 0L, 1000L).failed(longMessage.toString(), 2000L);

    assertThat(failed.error()).hasSizeLessThanOrEqualTo(IndexBuildStatus.MAX_ERROR_LENGTH);
    assertThat(failed.error()).isNotEmpty();
    assertThat(failed.state()).isEqualTo(State.FAILED);
  }

  @Test
  void aRunningReportThatStoppedUpdatingIsReportedAbandoned() {
    IndexBuildStatus running = running(1L, 10L, 0L, 1_000_000L);

    assertThat(running.stateAt(1_000_000L + 60_000L, STALE_AFTER)).isEqualTo(State.RUNNING);
    assertThat(running.stateAt(1_000_000L + 6L * 60_000L, STALE_AFTER))
        .as("a run whose report stopped moving is not still going")
        .isEqualTo(State.ABANDONED);
  }

  @Test
  void aFinishedBuildIsNeverAbandoned() {
    IndexBuildStatus succeeded = running(10L, 10L, 0L, 1_000_000L).succeeded(10L, 100L, 2_000_000L);

    assertThat(succeeded.stateAt(3_000_000L, STALE_AFTER)).isEqualTo(State.SUCCEEDED);
    assertThat(succeeded.failed("boom", 2_000_000L).stateAt(9_000_000L, STALE_AFTER))
        .isEqualTo(State.FAILED);
  }

  @Test
  void theReporterPublishesAsSoonAsItStarts() {
    AtomicLong built = new AtomicLong(0L);
    try (IndexBuildReporter reporter =
        IndexBuildReporter.start(
            io,
            statusDir,
            INDEX_UUID,
            COLUMN,
            SOURCE_SNAPSHOT,
            () -> 10L,
            built::get,
            () -> 0L,
            () -> 0L,
            Long.MAX_VALUE)) {
      IndexBuildStatus read = IndexBuildStatus.read(io, statusDir);
      assertThat(read).isNotNull();
      assertThat(read.state()).isEqualTo(State.RUNNING);
      assertThat(read.column()).isEqualTo(COLUMN);
      assertThat(read.sourceSnapshotId()).isEqualTo(SOURCE_SNAPSHOT);
      assertThat(read.filesTotal()).isEqualTo(10L);
      assertThat(read.filesBuilt()).isZero();
      assertThat(read.updatedAtMillis()).isGreaterThanOrEqualTo(read.startedAtMillis());
    }
  }

  @Test
  void theReporterPublishesTheProgressItIsShownAndThenTheOutcome() {
    AtomicLong built = new AtomicLong(0L);
    try (IndexBuildReporter reporter =
        IndexBuildReporter.start(
            io,
            statusDir,
            INDEX_UUID,
            COLUMN,
            SOURCE_SNAPSHOT,
            () -> 10L,
            built::get,
            () -> 0L,
            () -> 0L,
            Long.MAX_VALUE)) {
      built.set(7L);
      reporter.publishProgress();

      IndexBuildStatus midway = IndexBuildStatus.read(io, statusDir);
      assertThat(midway.state()).isEqualTo(State.RUNNING);
      assertThat(midway.filesBuilt()).isEqualTo(7L);
      assertThat(midway.filesTotal()).isEqualTo(10L);

      built.set(10L);
      reporter.succeeded();

      IndexBuildStatus finished = IndexBuildStatus.read(io, statusDir);
      assertThat(finished.state()).isEqualTo(State.SUCCEEDED);
      assertThat(finished.filesBuilt()).isEqualTo(10L);
    }
  }

  @Test
  void theReporterRecordsWhyABuildFailedAndDoesNotThrow() {
    try (IndexBuildReporter reporter =
        IndexBuildReporter.start(
            io,
            statusDir,
            INDEX_UUID,
            COLUMN,
            SOURCE_SNAPSHOT,
            () -> 4L,
            () -> 2L,
            () -> 1L,
            () -> 30L,
            Long.MAX_VALUE)) {
      reporter.failed(new IllegalArgumentException("dimension does not match the index"));
    }

    IndexBuildStatus read = IndexBuildStatus.read(io, statusDir);
    assertThat(read.state()).isEqualTo(State.FAILED);
    assertThat(read.error()).contains("dimension does not match the index");
    assertThat(read.filesBuilt()).isEqualTo(2L);
    assertThat(read.filesFailed()).isEqualTo(1L);
    assertThat(read.rows()).isEqualTo(30L);
  }

  @Test
  void aReporterThatCannotWriteDoesNotFailTheBuild() {
    FileIO broken =
        new FileIO() {
          @Override
          public InputFile newInputFile(String path) {
            return new Missing(path);
          }

          @Override
          public OutputFile newOutputFile(String path) {
            throw new IllegalStateException("no storage");
          }

          @Override
          public void deleteFile(String path) {
            throw new IllegalStateException("no storage");
          }
        };

    try (IndexBuildReporter reporter =
        IndexBuildReporter.start(
            broken,
            statusDir,
            INDEX_UUID,
            COLUMN,
            SOURCE_SNAPSHOT,
            () -> 1L,
            () -> 1L,
            () -> 0L,
            () -> 1L,
            Long.MAX_VALUE)) {
      reporter.publishProgress();
      reporter.succeeded();
    }

    assertThat(IndexBuildStatus.read(broken, statusDir)).isNull();
  }

  private IndexBuildStatus running(long built, long total, long failed, long startedAt) {
    return IndexBuildStatus.builder()
        .state(State.RUNNING)
        .indexUuid(INDEX_UUID)
        .column(COLUMN)
        .sourceSnapshotId(SOURCE_SNAPSHOT)
        .startedAtMillis(startedAt)
        .updatedAtMillis(startedAt)
        .filesBuilt(built)
        .filesTotal(total)
        .filesFailed(failed)
        .rows(0L)
        .build();
  }

  private String slotPath(int slot) {
    return IndexBuildStatus.slotPath(statusDir, slot);
  }

  /** An input file that reports itself missing, which is what a first build sees. */
  private static final class Missing implements InputFile {
    private final String path;

    private Missing(String path) {
      this.path = path;
    }

    @Override
    public long getLength() {
      return 0L;
    }

    @Override
    public org.apache.iceberg.io.SeekableInputStream newStream() {
      throw new org.apache.iceberg.exceptions.NotFoundException("Not found: %s", path);
    }

    @Override
    public String location() {
      return path;
    }

    @Override
    public boolean exists() {
      return false;
    }
  }
}
