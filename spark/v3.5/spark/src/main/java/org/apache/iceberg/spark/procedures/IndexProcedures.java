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
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.spark.IndexBuildStatus;
import org.apache.iceberg.index.spark.IndexSelection;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The arguments and rules every index procedure shares.
 *
 * <p>An index is built with a {@code name} and searched with an {@code index}, whatever its type,
 * so the parameters are declared once and the procedure passes its type to the shared rules. A new
 * index type declares no parameter and carries no copy of the conflict or selection logic.
 */
final class IndexProcedures {
  private static final Logger LOG = LoggerFactory.getLogger(IndexProcedures.class);

  /** Names an index as it is built, so several can live on one column. */
  static final ProcedureParameter NAME_PARAM =
      ProcedureParameter.optional("name", DataTypes.StringType);

  /** Selects an index by name or UUID in a statement that reads one. */
  static final ProcedureParameter INDEX_PARAM =
      ProcedureParameter.optional("index", DataTypes.StringType);

  private IndexProcedures() {}

  /** The name the caller gave the index being built, or null when it named none. */
  static String name(ProcedureInput input) {
    return input.asString(NAME_PARAM, null);
  }

  /** The index the caller selected, by name or UUID, or null when it selected none. */
  static String selection(ProcedureInput input) {
    return input.asString(INDEX_PARAM, null);
  }

  /** Resolves the index a procedure should read: the named one, or the only one of the type. */
  static IndexMetadata resolveIndex(
      Table table, String column, String indexType, ProcedureInput input) {
    return IndexSelection.resolve(table, column, indexType, selection(input));
  }

  /** Refuses a build that would make the column ambiguous, unless the caller named the index. */
  static void checkCanBuild(
      Table table, String column, String indexType, String tableName, String name) {
    IndexSelection.checkCanBuild(table, column, indexType, tableName, name);
  }

  /**
   * Keeps the report of a failed attempt where the rollback does not delete it.
   *
   * <p>The report a build writes about itself lives inside the index tree, and a failed build drops
   * that tree, so the run that ended is the one nobody could look up afterwards. This copies what
   * the failed attempt said to a location keyed by the column it was for, which is what a later
   * status call reads when the index it describes is gone.
   */
  static void keepFailedAttempt(
      Table table,
      int fieldId,
      String column,
      String indexUuid,
      IndexBuildStatus report,
      Throwable failure) {
    IndexBuildStatus failed =
        report == null
            ? IndexBuildStatus.builder()
                .state(IndexBuildStatus.State.FAILED)
                .indexUuid(indexUuid)
                .column(column)
                .sourceSnapshotId(
                    table.currentSnapshot() == null ? -1L : table.currentSnapshot().snapshotId())
                .startedAtMillis(System.currentTimeMillis())
                .error(IndexBuildStatus.describeFailure(failure))
                .build()
            : report.failed(IndexBuildStatus.describeFailure(failure), System.currentTimeMillis());

    try {
      IndexBuildStatus.write(
          table.io(), IndexBuildStatus.rolledBackStatusDir(table.location(), fieldId), failed);
    } catch (RuntimeException e) {
      // the build has already failed, and the report is an observation: reporting that the report
      // could not be written would replace the failure that matters with one that does not
      LOG.warn("Failed to record the failed index build of {}", column, e);
    }
  }

  /** Drops the report of a failed attempt once a later attempt of the same column succeeds. */
  static void forgetFailedAttempt(Table table, int fieldId) {
    try {
      IndexBuildStatus.delete(
          table.io(), IndexBuildStatus.rolledBackStatusDir(table.location(), fieldId));
    } catch (RuntimeException e) {
      // a stale failure in the report is a smaller problem than a build that worked reporting
      // that it did not
      LOG.warn("Failed to clear the failed index build report of column {}", fieldId, e);
    }
  }
}
