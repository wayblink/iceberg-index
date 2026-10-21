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
import org.apache.iceberg.index.SidecarIndexCatalog;
import org.apache.iceberg.index.spark.VectorIndexes;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;

/** Shared helpers for the vector index SQL procedures. */
final class VectorIndexProcedures {

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

  /**
   * The index a search should use: the one the caller named, or the only one the column has.
   *
   * <p>When a column carries several indexes and the caller named none, the message lists them
   * rather than picking one, because a query that silently used the other index would look like an
   * accuracy problem instead of a missing argument.
   */
  static org.apache.iceberg.index.IndexMetadata resolveIndex(
      Table table,
      String column,
      org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter indexParam,
      ProcedureInput input) {
    String nameOrUuid = input.asString(indexParam, null);
    if (nameOrUuid != null) {
      return org.apache.iceberg.index.spark.VectorIndexes.findIndex(
          table, column, "vector", nameOrUuid);
    }

    java.util.List<org.apache.iceberg.index.IndexMetadata> indexes =
        org.apache.iceberg.index.spark.VectorIndexes.indexesFor(table, column, "vector");
    if (indexes.size() > 1) {
      java.util.List<String> names = new java.util.ArrayList<>();
      for (org.apache.iceberg.index.IndexMetadata metadata : indexes) {
        names.add(metadata.name() != null ? metadata.name() : metadata.indexUuid());
      }

      throw new IllegalArgumentException(
          "Column "
              + column
              + " of table "
              + table.name()
              + " has "
              + indexes.size()
              + " vector indexes "
              + names
              + "; pass index => '...' to say which one to search");
    }

    return org.apache.iceberg.index.spark.VectorIndexes.findIndex(table, column, "vector");
  }

  static int fieldId(Table table, String column) {
    return VectorIndexes.fieldId(table, column);
  }

  /** Resolves the column name of a field ID. */
  static String columnName(Table table, int fieldId) {
    return VectorIndexes.columnName(table, fieldId);
  }

  /** Finds the index of a type for a table by the indexed column, failing if none exists. */
  static IndexMetadata findIndex(Table table, String column, String indexType) {
    return VectorIndexes.findIndex(table, column, indexType);
  }

  /** Finds any index of any type for a table by the indexed column, or null if none exists. */
  static IndexMetadata findAnyIndexOrNull(Table table, String column) {
    return VectorIndexes.findAnyIndexOrNull(table, column);
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
