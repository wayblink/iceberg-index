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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Table;
import org.apache.iceberg.index.IndexAdapter;
import org.apache.iceberg.index.IndexFile;
import org.apache.iceberg.index.IndexFileMemory;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.IndexQuery;
import org.apache.iceberg.index.IndexReader;
import org.apache.iceberg.index.IndexTypes;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Turns one table and index into the target the coordinator plans: the files of the index snapshot
 * that covers the table's current snapshot, keyed the same way the search path keys them.
 */
final class IndexCacheController {
  private IndexCacheController() {}

  /** How long a control call waits without progress for its files to reach a terminal state. */
  static final long DEFAULT_WAIT_NANOS = java.time.Duration.ofSeconds(60).toNanos();

  /**
   * Waits until the plan is settled or the wait can no longer end well, then reports its state.
   *
   * <p>The window measures a stall, not the whole load, and it starts when the plan is accepted:
   * resolving the target (listing a large index and pricing each of its files) happens on the
   * driver before the plan exists, and charging that time to the window leaves the plan fenced the
   * moment it is accepted. Every confirmation restarts the window, so a large index whose files
   * arrive minutes apart is still waited on while it is moving; a plan that stops moving is what
   * the window ends. It also ends when no executor is recent enough to answer, because then nothing
   * can still arrive. A plan that did not cover every file is ended there rather than merely
   * abandoned, so an executor that never answered does not keep holding the space it promised.
   */
  static IndexCacheProtocol.Result await(
      IndexCacheDriverControl control, String planId, long waitNanos) {
    IndexCacheProtocol.Result result = control.result(planId);
    long waiting = result.uncovered().size();
    long deadlineNanos = control.nowNanos() + waitNanos;
    while (!settled(result.status())) {
      long now = control.nowNanos();
      if (now >= deadlineNanos || !control.hasLiveExecutors(now)) {
        break;
      }

      try {
        Thread.sleep(50L);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(
            "Interrupted while waiting for index residency plan " + planId, e);
      }

      result = control.result(planId);
      long stillWaiting = result.uncovered().size();
      if (stillWaiting < waiting) {
        // A file reached a terminal state, so the plan is moving: the window is about the stall.
        waiting = stillWaiting;
        deadlineNanos = control.nowNanos() + waitNanos;
      }
    }

    if (result.status() == IndexCacheProtocol.Status.COMPLETE) {
      return result;
    }

    // The caller is no longer waiting, so the unfinished files are failures now: ending the plan
    // here is what keeps its reservations and unanswered promises from outliving the wait.
    return control.expire(planId, control.nowNanos());
  }

  /**
   * Statuses no further acknowledgement can change: a plan that loaded some files is reported as
   * {@link IndexCacheProtocol.Status#PARTIAL} while the rest are still in flight, so it is not one
   * of them.
   */
  private static boolean settled(IndexCacheProtocol.Status status) {
    return status == IndexCacheProtocol.Status.COMPLETE
        || status == IndexCacheProtocol.Status.FAILED
        || status == IndexCacheProtocol.Status.CANCELLED
        || status == IndexCacheProtocol.Status.OFFLOADED;
  }

  static IndexCacheProtocol.Result load(
      IndexCacheDriverControl control,
      Table table,
      IndexMetadata metadata,
      String retainFor,
      long nowNanos) {
    return load(control, table, metadata, retainFor, nowNanos, DEFAULT_WAIT_NANOS);
  }

  static IndexCacheProtocol.Result load(
      IndexCacheDriverControl control,
      Table table,
      IndexMetadata metadata,
      String retainFor,
      long nowNanos,
      long waitNanos) {
    IndexCacheProtocol.Target target = target(table, metadata);
    IndexResidencyPolicy policy = IndexResidencyPolicy.parse(retainFor);
    String planId = control.acceptLoad(target, policy, nowNanos);
    return await(control, planId, waitNanos);
  }

  static IndexCacheProtocol.Result offload(
      IndexCacheDriverControl control, Table table, IndexMetadata metadata, long nowNanos) {
    Preconditions.checkArgument(
        table.uuid().toString().equals(metadata.tableUuid()),
        "Index %s belongs to table %s, not to %s",
        metadata.indexUuid(),
        metadata.tableUuid(),
        table.uuid());

    // No location check here: releasing an index only needs its identity, and a moved table must
    // still be able to offload what it loaded.
    return offload(control, table, metadata, nowNanos, DEFAULT_WAIT_NANOS);
  }

  static IndexCacheProtocol.Result offload(
      IndexCacheDriverControl control,
      Table table,
      IndexMetadata metadata,
      long nowNanos,
      long waitNanos) {
    String planId = control.acceptOffload(metadata.indexUuid(), nowNanos);
    return await(control, planId, waitNanos);
  }

  /** The plan target for the index snapshot covering the table's current snapshot. */
  static IndexCacheProtocol.Target target(Table table, IndexMetadata metadata) {
    Preconditions.checkArgument(metadata != null, "Index metadata is missing");
    Preconditions.checkArgument(
        table.uuid().toString().equals(metadata.tableUuid()),
        "Index %s belongs to table %s, not to %s",
        metadata.indexUuid(),
        metadata.tableUuid(),
        table.uuid());
    Preconditions.checkArgument(
        table.currentSnapshot() != null,
        "Table %s has no current snapshot; the index cannot cover a snapshot that does not exist",
        table.name());

    long snapshotId = table.currentSnapshot().snapshotId();
    IndexReader reader = new IndexReader(metadata, table.io());
    // The payload and top-k are unused here: this call only resolves the snapshot and lists files.
    List<IndexFile> files = reader.indexFiles(IndexQuery.of(snapshotId, ByteBuffer.allocate(0), 1));

    IndexAdapter adapter =
        IndexTypes.fromType(metadata.indexType()).createAdapter(metadata.definition());
    List<IndexCacheProtocol.File> planned = new ArrayList<>(files.size());
    IndexCacheMemory.Estimates estimates = new IndexCacheMemory.Estimates();
    for (IndexFile file : files) {
      IndexFileMemory memory = estimates.resolve(table.io(), file, adapter);
      planned.add(
          IndexCacheProtocol.File.builder()
              .location(file.location())
              .cacheKey(cacheKey(table, metadata, file.location()))
              .owner(table.location())
              .kind(file.kind().name())
              .blobType(file.blobType())
              .estimatedBytes(memory == null ? null : IndexCacheMemory.residentBytes(memory))
              .memory(memory)
              .build());
    }
    estimates.log("load plan");

    return IndexCacheProtocol.Target.builder()
        .indexUuid(metadata.indexUuid())
        .owner(table.location())
        .snapshotId(snapshotId)
        .files(planned)
        .definition(metadata.definition())
        .io(table.io())
        .build();
  }

  static String cacheKey(Table table, IndexMetadata metadata, String location) {
    // The owner segment must be the table location the search path uses, not the copy recorded in
    // the index metadata: the two can differ after a table is moved.
    return DistributedIndexSearch.cacheKey(table.location(), adapterKey(metadata), location);
  }

  private static String adapterKey(IndexMetadata metadata) {
    return DistributedIndexSearch.adapterKey(
        IndexTypes.fromType(metadata.indexType()).createAdapter(metadata.definition()));
  }

  static Map<String, String> placements(IndexCacheDriverControl control, String planId) {
    return control.placements(planId);
  }
}
