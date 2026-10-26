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
import org.apache.iceberg.index.spark.IndexBuildStatus;
import org.apache.iceberg.spark.procedures.SparkProcedures.ProcedureBuilder;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Drops the vector index of a column.
 *
 * <p>An index is stored in a sidecar directory beside the table's data rather than inside the
 * table's metadata, so dropping the table does not remove it. This is what removes it, whether the
 * table is still there or has been created again at the same location.
 *
 * <pre>{@code
 * CALL spark_catalog.system.drop_vector_index(
 *   table => 'db.documents', column => 'embedding', purge => true);
 * }</pre>
 */
class DropVectorIndexProcedure extends BaseProcedure {
  private static final Logger LOG = LoggerFactory.getLogger(DropVectorIndexProcedure.class);

  private static final ProcedureParameter TABLE_PARAM =
      requiredInParameter("table", DataTypes.StringType);
  private static final ProcedureParameter COLUMN_PARAM =
      requiredInParameter("column", DataTypes.StringType);
  private static final ProcedureParameter INDEX_PARAM =
      optionalInParameter("index", DataTypes.StringType);

  private static final ProcedureParameter PURGE_PARAM =
      optionalInParameter("purge", DataTypes.BooleanType);
  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {TABLE_PARAM, COLUMN_PARAM, INDEX_PARAM, PURGE_PARAM};

  private static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField("index_uuid", DataTypes.StringType, false, Metadata.empty()),
            new StructField("purged", DataTypes.BooleanType, false, Metadata.empty())
          });

  public static ProcedureBuilder builder() {
    return new BaseProcedure.Builder<DropVectorIndexProcedure>() {
      @Override
      public DropVectorIndexProcedure doBuild() {
        return new DropVectorIndexProcedure(tableCatalog());
      }
    };
  }

  private DropVectorIndexProcedure(TableCatalog tableCatalog) {
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
    String column = input.asString(COLUMN_PARAM);
    // deleting the index files by default: an index that has been dropped is derived data that
    // nothing can read again, and leaving it behind is what makes the next table at this location
    // inherit it
    boolean purge = input.asBoolean(PURGE_PARAM, Boolean.TRUE);

    return modifyIcebergTable(
        input.ident(TABLE_PARAM),
        table -> {
          IndexMetadata metadata =
              VectorIndexProcedures.resolveIndex(table, column, INDEX_PARAM, input);
          SidecarIndexCatalog catalog = VectorIndexProcedures.indexCatalog(table);
          boolean dropped = catalog.dropIndex(table.location(), metadata.indexUuid(), purge);
          if (dropped && purge) {
            // the report of the builds of this index lives beside the tree the drop purges, so it
            // is cleared here rather than left behind with nothing to describe
            try {
              IndexBuildStatus.delete(
                  table.io(), IndexBuildStatus.statusDirFor(metadata.location()));
            } catch (RuntimeException e) {
              LOG.warn(
                  "Failed to clear the build reports of the dropped index {}",
                  metadata.indexUuid(),
                  e);
            }
          }
          InternalRow row = newInternalRow(UTF8String.fromString(metadata.indexUuid()), dropped);
          return new InternalRow[] {row};
        });
  }
}
