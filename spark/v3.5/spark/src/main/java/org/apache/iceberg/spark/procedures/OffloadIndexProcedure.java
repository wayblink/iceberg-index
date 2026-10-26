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
import org.apache.iceberg.index.spark.IndexCacheControl;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.procedures.SparkProcedures.ProcedureBuilder;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.types.DataTypes;

/** Pauses admission for one index and releases its copies; repeating it is a no-op. */
class OffloadIndexProcedure extends IndexResidencyProcedure {
  private static final ProcedureParameter TABLE_PARAM =
      requiredInParameter("table", DataTypes.StringType);
  private static final ProcedureParameter INDEX_PARAM =
      requiredInParameter("index", DataTypes.StringType);

  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {TABLE_PARAM, INDEX_PARAM};

  public static ProcedureBuilder builder() {
    return new BaseProcedure.Builder<OffloadIndexProcedure>() {
      @Override
      public OffloadIndexProcedure doBuild() {
        return new OffloadIndexProcedure(tableCatalog());
      }
    };
  }

  private OffloadIndexProcedure(TableCatalog catalog) {
    super(catalog);
  }

  @Override
  public ProcedureParameter[] parameters() {
    return PARAMETERS;
  }

  @Override
  public InternalRow[] call(InternalRow args) {
    ProcedureInput input = new ProcedureInput(spark(), tableCatalog(), PARAMETERS, args);
    Table table = table(input.asString(TABLE_PARAM));
    IndexMetadata metadata = resolveIndex(table, input.asString(INDEX_PARAM));
    return rows(IndexCacheControl.offload(spark(), table, metadata));
  }

  /** Loads the table the caller named, turning parser and lookup failures into clear messages. */
  private Table table(String tableName) {
    try {
      return Spark3Util.loadIcebergTable(spark(), tableName);
    } catch (org.apache.spark.sql.catalyst.parser.ParseException e) {
      throw new IllegalArgumentException("Not a table identifier: " + tableName, e);
    } catch (org.apache.spark.sql.catalyst.analysis.NoSuchTableException e) {
      throw new IllegalArgumentException("No such table: " + tableName, e);
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("Cannot load table " + tableName, e);
    }
  }
}
