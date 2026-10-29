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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Table;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.spark.sql.SparkSession;

/**
 * Explicit control of index residency: load an index onto executors, offload it again, and read
 * what is resident. Every method needs the application to run {@code
 * spark.plugins=org.apache.iceberg.index.spark.IndexCachePlugin}; without it the call fails with a
 * message that names the plugin instead of pretending to succeed.
 */
public final class IndexCacheControl {
  private IndexCacheControl() {}

  /** Requests one planning round for this index; a repeated call replaces the previous target. */
  public static Result load(
      SparkSession spark, Table table, IndexMetadata metadata, String retainFor) {
    IndexCacheDriverControl control = controlOf(spark);
    IndexCacheProtocol.Result protocol =
        IndexCacheController.load(control, table, metadata, retainFor, System.nanoTime());
    return Result.from(control, protocol);
  }

  /** Pauses admission for this index and releases its copies; repeating it is a no-op. */
  public static Result offload(SparkSession spark, Table table, IndexMetadata metadata) {
    IndexCacheDriverControl control = controlOf(spark);
    IndexCacheProtocol.Result protocol =
        IndexCacheController.offload(control, table, metadata, System.nanoTime());
    return Result.from(control, protocol);
  }

  /**
   * Records that a query used this index, which is what the index LRU orders by. A no-op when the
   * plugin is not enabled, so a query never fails because of residency bookkeeping.
   */
  public static void recordAccess(SparkSession spark, String indexUuid) {
    IndexCacheDriverControl control = IndexCacheDriverRegistry.find(spark.sparkContext());
    if (control != null) {
      control.recordAccess(indexUuid, System.nanoTime());
    }
  }

  /**
   * What each executor last reported about residency.
   *
   * <p>This reads the last report; it cannot force a new one, because the control channel polls on
   * its own schedule. A caller that needs the present state after an idle period asks {@link
   * IndexCacheStatus#refresh}, which runs a job to collect it.
   */
  public static List<Status> status(SparkSession spark) {
    return statusOf(controlOf(spark), System.nanoTime());
  }

  /** The outcome of a plan a caller already started, without re-planning it. */
  public static Result result(SparkSession spark, String planId) {
    IndexCacheDriverControl control = controlOf(spark);
    return Result.from(control, control.result(planId));
  }

  /** Residency rows built from the last reports; no executor is contacted. */
  static List<Status> statusOf(IndexCacheDriverControl control, long nowNanos) {
    Map<String, Long> ages = control.reportAges(nowNanos);
    List<String> managed = new ArrayList<>(control.protectedIndexes().keySet());
    managed.sort(String::compareTo);

    List<Status> rows = new ArrayList<>();
    for (IndexCacheProtocol.NodeReport report : control.nodeReports()) {
      long ageNanos = ages.getOrDefault(report.executorId(), 0L);
      rows.add(
          new Status(
              report.executorId(),
              report.session(),
              report.incarnation(),
              report.sequence(),
              report.budgetBytes(),
              report.usedBytes(),
              report.reservedBytes(),
              report.releasingBytes(),
              report.holders().size(),
              ageNanos / 1_000_000L,
              ageNanos > STALE_AFTER_NANOS,
              managed));
    }

    return rows;
  }

  /** A report older than this no longer describes the present. */
  static final long STALE_AFTER_NANOS = java.time.Duration.ofSeconds(10).toNanos();

  /** The protection the index currently has, or an empty list when nothing is managed. */
  public static List<String> managedIndexes(SparkSession spark) {
    return new ArrayList<>(controlOf(spark).protectedIndexes().keySet());
  }

  private static IndexCacheDriverControl controlOf(SparkSession spark) {
    return IndexCacheDriverRegistry.require(spark.sparkContext());
  }

  /** One managed index: its protection and how long ago a query last used it. */
  public static final class IndexStatus {
    private final String indexUuid;
    private final String protection;
    private final long deadlineNanos;
    private final boolean permanent;
    private final long accessAgeMillis;

    private IndexStatus(
        String indexUuid,
        String protection,
        long deadlineNanos,
        boolean permanent,
        long accessAgeMillis) {
      this.indexUuid = indexUuid;
      this.protection = protection;
      this.deadlineNanos = deadlineNanos;
      this.permanent = permanent;
      this.accessAgeMillis = accessAgeMillis;
    }

    public String indexUuid() {
      return indexUuid;
    }

    public String protection() {
      return protection;
    }

    public long deadlineNanos() {
      return deadlineNanos;
    }

    public boolean permanent() {
      return permanent;
    }

    /** Age of the last recorded search access; -1 when the index was never used. */
    public long accessAgeMillis() {
      return accessAgeMillis;
    }
  }

  /** The protection and access age of every managed index. */
  public static List<IndexStatus> indexes(SparkSession spark) {
    return indexStatusOf(controlOf(spark), System.nanoTime());
  }

  static List<IndexStatus> indexStatusOf(IndexCacheDriverControl control, long nowNanos) {
    Map<String, Long> ages = control.accessAges(nowNanos);
    List<IndexStatus> rows = new ArrayList<>();
    for (Map.Entry<String, IndexCacheProtocol.Protection> entry :
        control.protectedIndexes().entrySet()) {
      IndexCacheProtocol.Protection protection = entry.getValue();
      Long observed = ages.get(entry.getKey());
      long age = observed == null ? -1L : observed;
      rows.add(
          new IndexStatus(
              entry.getKey(),
              protection.text(),
              protection.deadlineNanos(),
              protection.permanent(),
              age < 0 ? -1L : age / 1_000_000L));
    }

    rows.sort(java.util.Comparator.comparing(IndexStatus::indexUuid));
    return rows;
  }

  /** What one executor last reported: its budget, occupancy, and the indexes the app manages. */
  public static final class Status {
    private final String executorId;
    private final String session;
    private final String incarnation;
    private final long sequence;
    private final long budgetBytes;
    private final long usedBytes;
    private final long reservedBytes;
    private final long releasingBytes;
    private final int holders;
    private final long ageMillis;
    private final boolean stale;
    private final List<String> managedIndexes;

    private Status(
        String executorId,
        String session,
        String incarnation,
        long sequence,
        long budgetBytes,
        long usedBytes,
        long reservedBytes,
        long releasingBytes,
        int holders,
        long ageMillis,
        boolean stale,
        List<String> managedIndexes) {
      this.executorId = executorId;
      this.session = session;
      this.incarnation = incarnation;
      this.sequence = sequence;
      this.budgetBytes = budgetBytes;
      this.usedBytes = usedBytes;
      this.reservedBytes = reservedBytes;
      this.releasingBytes = releasingBytes;
      this.holders = holders;
      this.ageMillis = ageMillis;
      this.stale = stale;
      this.managedIndexes = List.copyOf(managedIndexes);
    }

    public String executorId() {
      return executorId;
    }

    public String session() {
      return session;
    }

    public String incarnation() {
      return incarnation;
    }

    public long sequence() {
      return sequence;
    }

    public long budgetBytes() {
      return budgetBytes;
    }

    public long usedBytes() {
      return usedBytes;
    }

    public long reservedBytes() {
      return reservedBytes;
    }

    public long releasingBytes() {
      return releasingBytes;
    }

    /** How many files this executor reports as resident; a count, not a readiness claim. */
    public int holders() {
      return holders;
    }

    public long ageMillis() {
      return ageMillis;
    }

    public boolean stale() {
      return stale;
    }

    /** Indexes the application manages; the report does not attribute files to them. */
    public List<String> managedIndexes() {
      return managedIndexes;
    }
  }

  private static String reasonFor(IndexCacheProtocol.Result result, String location) {
    String reason = result.uncovered().get(location);
    return reason != null ? reason : result.releasePending().get(location);
  }

  /** One file of a plan: where it was sent and what happened to it. */
  public static final class FileResult {
    private final String fileLocation;
    private final String executorId;
    private final Long bytes;
    private final String reason;

    private FileResult(String fileLocation, String executorId, Long bytes, String reason) {
      this.fileLocation = fileLocation;
      this.executorId = executorId;
      this.bytes = bytes;
      this.reason = reason;
    }

    public String fileLocation() {
      return fileLocation;
    }

    public String executorId() {
      return executorId;
    }

    /** The planned retained bytes, or null when the plan has no estimate for this file. */
    public Long bytes() {
      return bytes;
    }

    /** Why the file is not covered, or null when it is planned or confirmed. */
    public String reason() {
      return reason;
    }
  }

  /** The outcome of one load or offload, as a caller can see it. */
  public static final class Result {
    private final String status;
    private final String planId;
    private final String indexUuid;
    private final long snapshotId;
    private final String protection;
    private final long deadlineNanos;
    private final boolean permanent;
    private final long residentBytes;
    private final List<FileResult> files;
    private final Map<String, String> uncovered;
    private final List<String> evictedIndexes;
    private final List<String> reusedFiles;
    private final Map<String, String> migrations;
    private final Map<String, String> releasePending;

    private Result(
        String status,
        String planId,
        String indexUuid,
        long snapshotId,
        String protection,
        long deadlineNanos,
        boolean permanent,
        long residentBytes,
        List<FileResult> files,
        Map<String, String> uncovered,
        List<String> evictedIndexes,
        List<String> reusedFiles,
        Map<String, String> migrations,
        Map<String, String> releasePending) {
      this.status = status;
      this.planId = planId;
      this.indexUuid = indexUuid;
      this.snapshotId = snapshotId;
      this.protection = protection;
      this.deadlineNanos = deadlineNanos;
      this.permanent = permanent;
      this.residentBytes = residentBytes;
      this.files = List.copyOf(files);
      this.uncovered = Map.copyOf(uncovered);
      this.evictedIndexes = List.copyOf(evictedIndexes);
      this.reusedFiles = List.copyOf(reusedFiles);
      this.migrations = Map.copyOf(migrations);
      this.releasePending = Map.copyOf(releasePending);
    }

    static Result from(IndexCacheDriverControl control, IndexCacheProtocol.Result result) {
      Map<String, String> placements = control.placements(result.planId());
      Map<String, Long> plannedBytes = control.plannedBytes(result.planId());
      IndexCacheProtocol.Protection protection = result.protection();
      List<FileResult> files = new ArrayList<>();
      for (String location : result.targetFiles()) {
        files.add(
            new FileResult(
                location,
                placements.get(location),
                plannedBytes.get(location),
                reasonFor(result, location)));
      }

      return new Result(
          result.status().name(),
          result.planId(),
          result.indexUuid(),
          result.snapshotId(),
          protection == null ? null : protection.text(),
          protection == null ? 0L : protection.deadlineNanos(),
          protection != null && protection.permanent(),
          result.residentBytes(),
          files,
          result.uncovered(),
          result.evictedIndexes(),
          result.reusedFiles(),
          result.migrations(),
          result.releasePending());
    }

    public String status() {
      return status;
    }

    public String planId() {
      return planId;
    }

    public String indexUuid() {
      return indexUuid;
    }

    public long snapshotId() {
      return snapshotId;
    }

    public String protection() {
      return protection;
    }

    public long deadlineNanos() {
      return deadlineNanos;
    }

    public boolean permanent() {
      return permanent;
    }

    public long residentBytes() {
      return residentBytes;
    }

    public List<FileResult> files() {
      return files;
    }

    public Map<String, String> uncovered() {
      return uncovered;
    }

    /** Indexes this plan reclaimed by LRU, so a caller can see what it displaced. */
    public List<String> evictedIndexes() {
      return evictedIndexes;
    }

    /** Files a plan reused from an existing copy instead of loading again. */
    public List<String> reusedFiles() {
      return reusedFiles;
    }

    /** Files this plan moves to another executor first, then releases on the old one. */
    public Map<String, String> migrations() {
      return migrations;
    }

    /** Copies an offload asked an executor to release and has not seen acknowledged. */
    public Map<String, String> releasePending() {
      return releasePending;
    }
  }
}
