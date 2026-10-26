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

  /** The protection the index currently has, or an empty list when nothing is managed. */
  public static List<String> managedIndexes(SparkSession spark) {
    return new ArrayList<>(controlOf(spark).coordinator().protectedIndexes().keySet());
  }

  private static IndexCacheDriverControl controlOf(SparkSession spark) {
    return IndexCacheDriverRegistry.require(spark.sparkContext().applicationId());
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
        Map<String, String> uncovered) {
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
    }

    static Result from(IndexCacheDriverControl control, IndexCacheProtocol.Result result) {
      Map<String, String> placements = control.coordinator().placements(result.planId());
      IndexCacheProtocol.Protection protection = result.protection();
      List<FileResult> files = new ArrayList<>();
      for (String location : result.targetFiles()) {
        files.add(
            new FileResult(
                location, placements.get(location), null, result.uncovered().get(location)));
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
          result.uncovered());
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
  }
}
