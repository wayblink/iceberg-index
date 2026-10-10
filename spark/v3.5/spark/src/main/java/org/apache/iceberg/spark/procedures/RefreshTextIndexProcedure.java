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

import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.SidecarIndexCatalog;
import org.apache.iceberg.index.spark.SparkIndexBuilder;
import org.apache.iceberg.spark.procedures.SparkProcedures.ProcedureBuilder;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Refreshes an existing text index incrementally against the current table snapshot.
 *
 * <p>Like vector refresh: index files of unchanged data files are carried over, files of removed or
 * rewritten data files are dropped and new data files are indexed. When the current snapshot is
 * already covered, no new index snapshot is created and zero files and entries are reported.
 *
 * <pre>{@code
 * CALL spark_catalog.system.refresh_text_index(
 *   table => 'db.documents', column => 'body');
 * }</pre>
 */
class RefreshTextIndexProcedure extends BaseProcedure {
  private static final ProcedureParameter TABLE_PARAM =
      requiredInParameter("table", DataTypes.StringType);
  private static final ProcedureParameter COLUMN_PARAM =
      requiredInParameter("column", DataTypes.StringType);
  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {TABLE_PARAM, COLUMN_PARAM};

  private static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField("index_uuid", DataTypes.StringType, false, Metadata.empty()),
            new StructField("source_snapshot_id", DataTypes.LongType, false, Metadata.empty()),
            new StructField("index_files", DataTypes.LongType, false, Metadata.empty()),
            new StructField("entries", DataTypes.LongType, false, Metadata.empty())
          });

  public static ProcedureBuilder builder() {
    return new BaseProcedure.Builder<RefreshTextIndexProcedure>() {
      @Override
      public RefreshTextIndexProcedure doBuild() {
        return new RefreshTextIndexProcedure(tableCatalog());
      }
    };
  }

  private RefreshTextIndexProcedure(TableCatalog tableCatalog) {
    super(tableCatalog);
  }

  @Override
  public ProcedureParameter[] parameters() {
    return PARAMETERS;
  }

  @Override
  public StructType outputType() {
    return OUTPUT_TYPE;
  }

  @Override
  public InternalRow[] call(InternalRow args) {
    VectorIndexProcedures.registerTextIndexType();
    ProcedureInput input = new ProcedureInput(spark(), tableCatalog(), PARAMETERS, args);
    String tableName = input.asString(TABLE_PARAM);
    String column = input.asString(COLUMN_PARAM);

    return modifyIcebergTable(
        input.ident(TABLE_PARAM),
        table -> {
          if (table.currentSnapshot() == null) {
            throw new IllegalArgumentException(
                "Table " + tableName + " has no data: cannot refresh an index");
          }

          IndexMetadata metadata = VectorIndexProcedures.findIndex(table, column, "text");
          SidecarIndexCatalog catalog = VectorIndexProcedures.indexCatalog(table);
          long sourceSnapshotId = table.currentSnapshot().snapshotId();

          IndexMetadata updated =
              SparkIndexBuilder.refresh(
                  spark(), table, metadata, sourceSnapshotId, column, System.currentTimeMillis());
          if (updated.snapshots().size() == metadata.snapshots().size()) {
            // nothing changed; the current snapshot was already covered
            InternalRow row =
                newInternalRow(
                    UTF8String.fromString(metadata.indexUuid()), sourceSnapshotId, 0L, 0L);
            return new InternalRow[] {row};
          }

          catalog.updateIndex(updated);
          long[] counts = VectorIndexProcedures.counts(table.io(), updated.latestSnapshot());
          InternalRow row =
              newInternalRow(
                  UTF8String.fromString(metadata.indexUuid()),
                  sourceSnapshotId,
                  counts[0],
                  counts[1]);
          return new InternalRow[] {row};
        });
  }

  @Override
  public String description() {
    return "RefreshTextIndexProcedure";
  }
}
