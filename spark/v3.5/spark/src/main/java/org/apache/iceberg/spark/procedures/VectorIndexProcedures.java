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
package org.apache.iceberg.spark.procedures;

import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.IndexSnapshot;
import org.apache.iceberg.index.IndexSnapshotWriter;
import org.apache.iceberg.index.SidecarIndexCatalog;
import org.apache.iceberg.index.spark.VectorIndexes;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Shared helpers for the vector index SQL procedures. */
final class VectorIndexProcedures {
  private static final Logger LOG = LoggerFactory.getLogger(VectorIndexProcedures.class);

  /**
   * The default entry budget that groups data files into index files. Large enough that a data file
   * of a usual size keeps an index file of its own, so a table is not fragmented by default.
   */
  static final long DEFAULT_MAX_ENTRIES_PER_INDEX_FILE = 1_000_000L;

  private VectorIndexProcedures() {}

  /** Registers the vector index type with the core index type registry. */
  static void registerVectorIndexType() {
    VectorIndexes.registerVectorIndexType();
  }

  /** Registers the text index type with the core index type registry. */
  static void registerTextIndexType() {
    VectorIndexes.registerTextIndexType();
  }

  /** Returns the sidecar index catalog for the given table. */
  static SidecarIndexCatalog indexCatalog(Table table) {
    return VectorIndexes.indexCatalog(table);
  }

  /**
   * Publishes a built snapshot on top of the index metadata as it stands.
   *
   * <p>The snapshot is appended to whatever the catalog holds when the publication lands, not to
   * the metadata this build started from: two publishers that read the same index (a scheduled
   * refresh and a rebuild, or two refreshes) would otherwise each write a document containing only
   * their own snapshot, and the one that landed first would be lost without anyone noticing.
   *
   * <p>If another publisher covered the same source snapshot, keep its result. Distinct refreshes
   * can have the same provisional index snapshot ID, so the published ID is assigned from the
   * current metadata while preserving the parent used to build the snapshot. An older refresh that
   * finishes after its descendant is skipped so it cannot become the latest index.
   */
  static IndexSnapshot publishSnapshot(
      SidecarIndexCatalog catalog, Table table, String indexUuid, IndexSnapshot snapshot) {
    long source = snapshot.sourceTableSnapshotId();
    IndexMetadata result;
    try {
      result =
          catalog.updateIndex(
              table.location(),
              indexUuid,
              current -> {
                if (current.snapshotForSourceTableSnapshot(source) != null) {
                  return current;
                }

                IndexSnapshot latest = current.latestSnapshot();
                table.refresh();
                if (latest != null
                    && SnapshotUtil.isAncestorOf(table, latest.sourceTableSnapshotId(), source)) {
                  return current;
                }

                long nextId = latest == null ? 1L : Math.addExact(latest.snapshotId(), 1L);
                return current.withSnapshot(snapshot.toBuilder().snapshotId(nextId).build());
              });
    } catch (NotFoundException e) {
      // the index was dropped while it was being built, so no publication will ever land and no
      // metadata will ever name the files this attempt wrote: they are removed with the attempt
      // instead of being left behind as an orphan index. A conflict is not handled here, because
      // the publication can be retried and still needs the files it built.
      try {
        IndexSnapshotWriter.abortPublication(table.io(), snapshot.indexManifestList());
      } catch (RuntimeException cleanup) {
        LOG.warn(
            "Failed to remove the files of the unpublished index build {} of table {}",
            indexUuid,
            table.location(),
            cleanup);
      }

      throw e;
    }

    try {
      IndexSnapshotWriter.finishPublication(
          table.io(), result.location(), snapshot.indexManifestList());
    } catch (RuntimeException e) {
      LOG.warn("Failed to release the build ledger for index {}", indexUuid, e);
    }

    return result.snapshotForSourceTableSnapshot(source);
  }

  /** Resolves the field ID of the vector column, failing if the column does not exist. */
  /**
   * Reads the entry budget that groups data files into index files from a build procedure's input.
   *
   * <p>The budget is an upper bound on a group and never splits a data file, so a table whose data
   * files are larger than the budget keeps one index file per data file, and only a table with many
   * small data files sees them merged. The default is large enough that a data file of a usual size
   * is left alone; lower it to trade parallelism for a larger HNSW graph, which recalls better.
   */
  static long maxEntriesPerIndexFile(ProcedureInput input, ProcedureParameter parameter) {
    return input.asLong(parameter, DEFAULT_MAX_ENTRIES_PER_INDEX_FILE);
  }

  static int fieldId(Table table, String column) {
    return VectorIndexes.fieldId(table, column);
  }

  /** Resolves the column name of a field ID. */
  static String columnName(Table table, int fieldId) {
    return VectorIndexes.columnName(table, fieldId);
  }

  /** Counts the index files and entries of an index snapshot. */
  static long[] counts(
      org.apache.iceberg.io.FileIO fileIO, org.apache.iceberg.index.IndexSnapshot snapshot) {
    return VectorIndexes.counts(fileIO, snapshot);
  }

  /** Returns the positional index of a procedure parameter, matching Spark's argument order. */
  static int ordinal(ProcedureParameter[] parameters, ProcedureParameter parameter) {
    for (int ordinal = 0; ordinal < parameters.length; ordinal++) {
      if (parameters[ordinal] == parameter) {
        return ordinal;
      }
    }

    throw new IllegalArgumentException("Unknown parameter " + parameter.name());
  }
}
