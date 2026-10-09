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
import org.apache.iceberg.spark.index.VectorIndexes;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;

/** Shared helpers for the vector index SQL procedures. */
final class VectorIndexProcedures {

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
