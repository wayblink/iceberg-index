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
import org.apache.iceberg.index.IndexFile;
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

  /** How long a control call waits for its files to reach a terminal state. */
  static final long DEFAULT_WAIT_NANOS = java.time.Duration.ofSeconds(60).toNanos();

  /**
   * Waits until every file of the plan is terminal or the deadline passes. A cluster with no
   * executor cannot answer, so that case returns at once instead of blocking the caller.
   */
  static IndexCacheProtocol.Result await(
      IndexCacheDriverControl control, String planId, long deadlineNanos) {
    IndexCacheProtocol.Result result = control.result(planId);
    while (!terminal(result.status()) && control.hasLiveExecutors(control.nowNanos())) {
      if (control.nowNanos() >= deadlineNanos) {
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
    }

    return result;
  }

  private static boolean terminal(IndexCacheProtocol.Status status) {
    return status == IndexCacheProtocol.Status.COMPLETE
        || status == IndexCacheProtocol.Status.PARTIAL
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
    return load(control, table, metadata, retainFor, nowNanos, nowNanos + DEFAULT_WAIT_NANOS);
  }

  static IndexCacheProtocol.Result load(
      IndexCacheDriverControl control,
      Table table,
      IndexMetadata metadata,
      String retainFor,
      long nowNanos,
      long deadlineNanos) {
    IndexCacheProtocol.Target target = target(table, metadata);
    IndexResidencyPolicy policy = IndexResidencyPolicy.parse(retainFor);
    String planId = control.acceptLoad(target, policy, nowNanos);
    return await(control, planId, deadlineNanos);
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
    return offload(control, table, metadata, nowNanos, nowNanos + DEFAULT_WAIT_NANOS);
  }

  static IndexCacheProtocol.Result offload(
      IndexCacheDriverControl control,
      Table table,
      IndexMetadata metadata,
      long nowNanos,
      long deadlineNanos) {
    String planId = control.acceptOffload(metadata.indexUuid(), nowNanos);
    return await(control, planId, deadlineNanos);
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

    List<IndexCacheProtocol.File> planned = new ArrayList<>(files.size());
    for (IndexFile file : files) {
      planned.add(
          IndexCacheProtocol.File.builder()
              .location(file.location())
              .cacheKey(cacheKey(table, metadata, file.location()))
              .kind(file.kind().name())
              .blobType(file.blobType())
              .estimatedBytes(estimate(file))
              .memory(file.memory())
              .build());
    }

    return IndexCacheProtocol.Target.builder()
        .indexUuid(metadata.indexUuid())
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

  private static Long estimate(IndexFile file) {
    if (file.memory() == null || file.memory().fileBytes() == null) {
      // Unknown size stays unknown: the planner must not treat it as zero.
      return null;
    }

    return IndexCacheMemory.residentBytes(file.memory());
  }

  static Map<String, String> placements(IndexCacheDriverControl control, String planId) {
    return control.placements(planId);
  }
}
