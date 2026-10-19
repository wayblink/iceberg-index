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

import java.util.List;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.IndexMetadataParser;
import org.apache.iceberg.index.IndexSnapshot;
import org.apache.iceberg.index.spark.VectorIndexes;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
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
 * Lists the indexes registered for a table, of any index type. Type-specific parameters are in the
 * {@code definition} column as JSON.
 *
 * <pre>{@code
 * CALL spark_catalog.system.list_indexes(table => 'db.documents');
 * }</pre>
 */
class ListIndexesProcedure extends BaseProcedure {
  private static final ProcedureParameter TABLE_PARAM =
      requiredInParameter("table", DataTypes.StringType);
  private static final ProcedureParameter[] PARAMETERS = new ProcedureParameter[] {TABLE_PARAM};

  private static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField("index_uuid", DataTypes.StringType, false, Metadata.empty()),
            new StructField("column", DataTypes.StringType, true, Metadata.empty()),
            new StructField("index_type", DataTypes.StringType, false, Metadata.empty()),
            new StructField("source_snapshot_id", DataTypes.LongType, true, Metadata.empty()),
            new StructField("status", DataTypes.StringType, true, Metadata.empty()),
            new StructField("index_files", DataTypes.LongType, false, Metadata.empty()),
            new StructField("entries", DataTypes.LongType, false, Metadata.empty()),
            new StructField("definition", DataTypes.StringType, true, Metadata.empty())
          });

  public static ProcedureBuilder builder() {
    return new BaseProcedure.Builder<ListIndexesProcedure>() {
      @Override
      public ListIndexesProcedure doBuild() {
        return new ListIndexesProcedure(tableCatalog());
      }
    };
  }

  private ListIndexesProcedure(TableCatalog tableCatalog) {
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
    VectorIndexProcedures.registerVectorIndexType();
    ProcedureInput input = new ProcedureInput(spark(), tableCatalog(), PARAMETERS, args);

    return modifyIcebergTable(
        input.ident(TABLE_PARAM),
        table -> {
          // only the indexes of this table instance: a location outlives the table, so a table
          // created again there still has the indexes of the one before it on disk
          List<IndexMetadata> indexes = VectorIndexes.indexesOf(table);
          List<InternalRow> rows = Lists.newArrayListWithCapacity(indexes.size());
          for (IndexMetadata metadata : indexes) {
            IndexSnapshot snapshot = metadata.latestSnapshot();
            long[] counts = VectorIndexProcedures.counts(table.io(), snapshot);
            String column =
                VectorIndexProcedures.columnName(table, metadata.definition().fieldId());
            String status = snapshot == null ? null : snapshot.status().name();
            rows.add(
                newInternalRow(
                    UTF8String.fromString(metadata.indexUuid()),
                    column == null ? null : UTF8String.fromString(column),
                    UTF8String.fromString(metadata.indexType()),
                    snapshot == null ? null : snapshot.sourceTableSnapshotId(),
                    status == null ? null : UTF8String.fromString(status),
                    counts[0],
                    counts[1],
                    UTF8String.fromString(IndexMetadataParser.toJson(metadata.definition()))));
          }

          return rows.toArray(new InternalRow[0]);
        });
  }

  @Override
  public String description() {
    return "ListIndexesProcedure";
  }
}
