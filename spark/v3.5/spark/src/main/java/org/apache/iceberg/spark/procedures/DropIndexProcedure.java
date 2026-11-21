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

import static org.apache.iceberg.spark.procedures.SparkProcedures.ProcedureBuilder;

import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.Table;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.spark.VectorIndexes;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Removes one index of a table, whatever its type, so a column can be indexed again with another
 * layout, analyzer or parameters.
 *
 * <pre>{@code
 * CALL spark_catalog.system.drop_index(table => 'db.documents', column => 'content');
 * CALL spark_catalog.system.drop_index(
 *   table => 'db.documents', column => 'content', index => 'idx_content', purge => false);
 * }</pre>
 *
 * <p>The column usually has one index. When it has several — a text index and a vector index on
 * different columns never collide, but two of the same kind can exist on one column — the caller
 * names the one to remove with {@code index}, by name or by uuid, and the error lists the choices.
 *
 * <p>This is the general form of {@code drop_vector_index}, which stays as the spelling for a
 * vector index alone.
 */
class DropIndexProcedure extends BaseProcedure {
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
            new StructField("index_type", DataTypes.StringType, false, Metadata.empty()),
            new StructField("purged", DataTypes.BooleanType, false, Metadata.empty())
          });

  public static ProcedureBuilder builder() {
    return new BaseProcedure.Builder<DropIndexProcedure>() {
      @Override
      public DropIndexProcedure doBuild() {
        return new DropIndexProcedure(tableCatalog());
      }
    };
  }

  private DropIndexProcedure(TableCatalog tableCatalog) {
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
    String nameOrUuid = input.asString(INDEX_PARAM, null);
    boolean purge = input.asBoolean(PURGE_PARAM, Boolean.TRUE);

    return modifyIcebergTable(
        input.ident(TABLE_PARAM),
        table -> {
          IndexMetadata metadata = resolve(table, column, nameOrUuid);
          IndexDrop.Result dropped = IndexDrop.drop(table, metadata, purge);
          return new InternalRow[] {
            newInternalRow(
                UTF8String.fromString(dropped.indexUuid()),
                UTF8String.fromString(metadata.indexType()),
                dropped.purged())
          };
        });
  }

  /**
   * Resolves the index to remove: the only one on the column, or the named one when several exist.
   */
  private static IndexMetadata resolve(Table table, String column, String nameOrUuid) {
    int fieldId = VectorIndexes.fieldId(table, column);
    List<IndexMetadata> found = new ArrayList<>();
    for (IndexMetadata metadata : VectorIndexes.indexesOf(table)) {
      if (metadata.definition().fieldId() == fieldId) {
        found.add(metadata);
      }
    }

    if (nameOrUuid != null) {
      for (IndexMetadata metadata : found) {
        if (nameOrUuid.equals(metadata.name()) || nameOrUuid.equals(metadata.indexUuid())) {
          return metadata;
        }
      }

      throw new IllegalArgumentException(
          "No index named "
              + nameOrUuid
              + " exists for column "
              + column
              + " of table "
              + table.name()
              + (found.isEmpty() ? "" : "; existing indexes: " + describe(found)));
    }

    if (found.isEmpty()) {
      throw new IllegalArgumentException(
          "No index exists for column " + column + " of table " + table.name());
    }

    if (found.size() > 1) {
      throw new IllegalArgumentException(
          "Column "
              + column
              + " of table "
              + table.name()
              + " has "
              + found.size()
              + " indexes; pass index => <name or uuid> to pick one: "
              + describe(found));
    }

    return found.get(0);
  }

  /** Lists the resolvable indexes as {@code type:name(uuid)} so an error can name them. */
  private static String describe(List<IndexMetadata> indexes) {
    List<String> described = new ArrayList<>(indexes.size());
    for (IndexMetadata metadata : indexes) {
      String name = metadata.name() != null ? metadata.name() : metadata.indexUuid();
      described.add(metadata.indexType() + ":" + name + "(" + metadata.indexUuid() + ")");
    }

    return String.join(", ", described);
  }
}
