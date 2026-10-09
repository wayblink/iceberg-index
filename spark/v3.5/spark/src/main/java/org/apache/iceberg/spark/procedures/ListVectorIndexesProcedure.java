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
import org.apache.iceberg.index.IndexSnapshot;
import org.apache.iceberg.index.vector.VectorIndexDefinition;
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
 * Lists the vector indexes registered for a table.
 *
 * <pre>{@code
 * CALL spark_catalog.system.list_vector_indexes(table => 'db.documents');
 * }</pre>
 */
class ListVectorIndexesProcedure extends BaseProcedure {
  private static final ProcedureParameter TABLE_PARAM =
      requiredInParameter("table", DataTypes.StringType);
  private static final ProcedureParameter[] PARAMETERS = new ProcedureParameter[] {TABLE_PARAM};

  private static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField("index_uuid", DataTypes.StringType, false, Metadata.empty()),
            new StructField("column", DataTypes.StringType, true, Metadata.empty()),
            new StructField("index_type", DataTypes.StringType, false, Metadata.empty()),
            new StructField("mode", DataTypes.StringType, true, Metadata.empty()),
            new StructField("metric", DataTypes.StringType, true, Metadata.empty()),
            new StructField("dimension", DataTypes.IntegerType, true, Metadata.empty()),
            new StructField("source_snapshot_id", DataTypes.LongType, true, Metadata.empty()),
            new StructField("status", DataTypes.StringType, true, Metadata.empty()),
            new StructField("index_files", DataTypes.LongType, false, Metadata.empty()),
            new StructField("entries", DataTypes.LongType, false, Metadata.empty())
          });

  public static ProcedureBuilder builder() {
    return new BaseProcedure.Builder<ListVectorIndexesProcedure>() {
      @Override
      public ListVectorIndexesProcedure doBuild() {
        return new ListVectorIndexesProcedure(tableCatalog());
      }
    };
  }

  private ListVectorIndexesProcedure(TableCatalog tableCatalog) {
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
          List<IndexMetadata> indexes =
              VectorIndexProcedures.indexCatalog(table).listIndexes(table.location());
          List<InternalRow> rows = Lists.newArrayListWithCapacity(indexes.size());
          for (IndexMetadata metadata : indexes) {
            VectorIndexDefinition definition = (VectorIndexDefinition) metadata.definition();
            IndexSnapshot snapshot = metadata.latestSnapshot();
            long[] counts = VectorIndexProcedures.counts(table.io(), snapshot);
            String column = VectorIndexProcedures.columnName(table, definition.fieldId());
            String status = snapshot == null ? null : snapshot.status().name();
            rows.add(
                newInternalRow(
                    UTF8String.fromString(metadata.indexUuid()),
                    column == null ? null : UTF8String.fromString(column),
                    UTF8String.fromString(metadata.indexType()),
                    UTF8String.fromString(definition.mode().name()),
                    UTF8String.fromString(definition.metric().name()),
                    definition.dimension(),
                    snapshot == null ? null : snapshot.sourceTableSnapshotId(),
                    status == null ? null : UTF8String.fromString(status),
                    counts[0],
                    counts[1]));
          }

          return rows.toArray(new InternalRow[0]);
        });
  }

  @Override
  public String description() {
    return "ListVectorIndexesProcedure";
  }
}
