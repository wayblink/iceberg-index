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

import java.io.Closeable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Publishes how far a build of one index has got, for a caller that asks while it runs.
 *
 * <p>The counters a build keeps are driver-side accumulators, readable at any moment, so the report
 * is a periodic read of them rather than an event from the executors. The thread that publishes is
 * a daemon: a build that is killed leaves the last report behind, which is what makes its age the
 * way to tell a build that stopped from one that is still going.
 *
 * <p>Writing a report can never fail the build. A build of a large index is expensive and the
 * report is an observation; a full disk or a storage hiccup must not turn a build that would have
 * succeeded into one that failed, so every failure here is logged and dropped.
 */
public final class IndexBuildReporter implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(IndexBuildReporter.class);

  /** How often a running build says it is still running. */
  public static final long DEFAULT_INTERVAL_MILLIS = 30L * 1000L;

  private final org.apache.iceberg.io.FileIO io;
  private final String statusDir;
  private final String indexUuid;
  private final String column;
  private final long sourceSnapshotId;
  private final long startedAtMillis;
  private final LongSupplier filesTotal;
  private final LongSupplier filesBuilt;
  private final LongSupplier filesFailed;
  private final LongSupplier rows;
  private final ScheduledExecutorService publisher;
  private volatile boolean reporting;
  private volatile boolean finished = false;

  /**
   * Starts reporting the build of {@code indexUuid} into {@code statusDir}.
   *
   * <p>The first report is written before this returns, so that a build already running is visible
   * to the next caller rather than only after the first interval has passed.
   */
  public static IndexBuildReporter start(
      org.apache.iceberg.io.FileIO io,
      String statusDir,
      String indexUuid,
      String column,
      long sourceSnapshotId,
      LongSupplier filesTotal,
      LongSupplier filesBuilt,
      LongSupplier filesFailed,
      LongSupplier rows,
      long intervalMillis) {
    IndexBuildReporter reporter =
        new IndexBuildReporter(
            io,
            statusDir,
            indexUuid,
            column,
            sourceSnapshotId,
            filesTotal,
            filesBuilt,
            filesFailed,
            rows);
    reporter.reporting = reporter.publish(IndexBuildStatus.State.RUNNING, null, reporter.now());
    reporter.schedule(intervalMillis);
    return reporter;
  }

  /** Starts reporting at the default interval. */
  public static IndexBuildReporter start(
      org.apache.iceberg.io.FileIO io,
      String statusDir,
      String indexUuid,
      String column,
      long sourceSnapshotId,
      LongSupplier filesTotal,
      LongSupplier filesBuilt,
      LongSupplier filesFailed,
      LongSupplier rows) {
    return start(
        io,
        statusDir,
        indexUuid,
        column,
        sourceSnapshotId,
        filesTotal,
        filesBuilt,
        filesFailed,
        rows,
        DEFAULT_INTERVAL_MILLIS);
  }

  private IndexBuildReporter(
      org.apache.iceberg.io.FileIO io,
      String statusDir,
      String indexUuid,
      String column,
      long sourceSnapshotId,
      LongSupplier filesTotal,
      LongSupplier filesBuilt,
      LongSupplier filesFailed,
      LongSupplier rows) {
    Preconditions.checkArgument(io != null, "Invalid file IO: null");
    Preconditions.checkArgument(
        statusDir != null && !statusDir.isEmpty(), "Invalid status directory: null or empty");
    this.io = io;
    this.statusDir = statusDir;
    this.indexUuid = indexUuid;
    this.column = column;
    this.sourceSnapshotId = sourceSnapshotId;
    this.startedAtMillis = System.currentTimeMillis();
    this.filesTotal = filesTotal;
    this.filesBuilt = filesBuilt;
    this.filesFailed = filesFailed;
    this.rows = rows;
    this.publisher =
        Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("index-build-status-%d")
                .build());
  }

  private void schedule(long intervalMillis) {
    if (intervalMillis <= 0L || intervalMillis == Long.MAX_VALUE) {
      return;
    }

    publisher.scheduleWithFixedDelay(
        () -> {
          if (!finished) {
            publishProgress();
          }
        },
        intervalMillis,
        intervalMillis,
        TimeUnit.MILLISECONDS);
  }

  /** Writes the counters as they stand, as a report that the build is still running. */
  void publishProgress() {
    if (reporting) {
      publish(IndexBuildStatus.State.RUNNING, null, now());
    }
  }

  /** Writes the final report of a build that finished, and stops reporting. */
  public void succeeded() {
    finishWith(IndexBuildStatus.State.SUCCEEDED, null);
  }

  /** Writes the final report of a build that failed, and stops reporting. */
  public void failed(Throwable failure) {
    finishWith(IndexBuildStatus.State.FAILED, IndexBuildStatus.describeFailure(failure));
  }

  private void finishWith(IndexBuildStatus.State state, String error) {
    if (finished) {
      return;
    }

    finished = true;
    if (reporting) {
      publish(state, error, now());
    }
  }

  /**
   * Writes one report, and says whether it could.
   *
   * <p>The counters are read here rather than kept: the total is unknown when a build starts, and a
   * report that froze it at zero would describe the attempt wrongly for its whole duration.
   */
  private boolean publish(IndexBuildStatus.State state, String error, long atMillis) {
    Preconditions.checkArgument(
        indexUuid != null && !indexUuid.isEmpty(), "Invalid index uuid: null or empty");
    IndexBuildStatus status =
        IndexBuildStatus.builder()
            .state(state)
            .indexUuid(indexUuid)
            .column(column)
            .sourceSnapshotId(sourceSnapshotId)
            .startedAtMillis(startedAtMillis)
            .updatedAtMillis(atMillis)
            .filesBuilt(filesBuilt.getAsLong())
            .filesTotal(filesTotal.getAsLong())
            .filesFailed(filesFailed.getAsLong())
            .rows(rows.getAsLong())
            .error(error)
            .build();
    try {
      IndexBuildStatus.write(io, statusDir, status);
      return true;
    } catch (RuntimeException e) {
      // nothing about the report may cost a build: it is an observation of work that is expensive
      // to repeat, so a storage problem is logged and the build carries on unobserved
      LOG.warn("Failed to record the state of the index build at {}", statusDir, e);
      return false;
    }
  }

  private long now() {
    return System.currentTimeMillis();
  }

  @Override
  public void close() {
    finished = true;
    publisher.shutdownNow();
  }
}
