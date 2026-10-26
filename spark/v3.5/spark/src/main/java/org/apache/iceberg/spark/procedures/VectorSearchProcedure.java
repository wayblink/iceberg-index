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
 * Searches a vector index and resolves the matched rows.
 *
 * <p>Returns the row reference ({@code _file} + {@code _pos}) of each hit with its score, plus the
 * matched row itself as a JSON string. Rows are ordered by score, descending.
 *
 * <pre>{@code
 * CALL spark_catalog.system.vector_search(
 *   table => 'db.documents', column => 'embedding',
 *   query => array(0.95d, 0.05d), k => 10);
 * }</pre>
 */
class VectorSearchProcedure extends BaseProcedure {
  private static final ProcedureParameter TABLE_PARAM =
      requiredInParameter("table", DataTypes.StringType);
  private static final ProcedureParameter COLUMN_PARAM =
      requiredInParameter("column", DataTypes.StringType);
  private static final ProcedureParameter QUERY_PARAM =
      requiredInParameter("query", DataTypes.createArrayType(DataTypes.DoubleType));
  private static final ProcedureParameter K_PARAM = requiredInParameter("k", DataTypes.IntegerType);

  private static final ProcedureParameter INDEX_PARAM =
      optionalInParameter("index", DataTypes.StringType);
  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {TABLE_PARAM, COLUMN_PARAM, QUERY_PARAM, K_PARAM, INDEX_PARAM};

  private static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField("data_file", DataTypes.StringType, false, Metadata.empty()),
            new StructField("pos", DataTypes.LongType, false, Metadata.empty()),
            new StructField("score", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("row", DataTypes.StringType, true, Metadata.empty())
          });

  public static ProcedureBuilder builder() {
    return new BaseProcedure.Builder<VectorSearchProcedure>() {
      @Override
      public VectorSearchProcedure doBuild() {
        return new VectorSearchProcedure(tableCatalog());
      }
    };
  }

  private VectorSearchProcedure(TableCatalog tableCatalog) {
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
    int topK = input.asInt(K_PARAM);
    double[] queryDoubles =
        args.getArray(VectorIndexProcedures.ordinal(PARAMETERS, QUERY_PARAM)).toDoubleArray();

    return modifyIcebergTable(
        input.ident(TABLE_PARAM),
        table -> {
          if (topK <= 0) {
            return new InternalRow[0];
          }

          // the same planning and parameter decisions the other vector entry points use: an
          // explicit setting is kept, and only an unset multiplier is decided here
          String indexArgument = input.isProvided(INDEX_PARAM) ? input.asString(INDEX_PARAM) : null;
          org.apache.iceberg.spark.index.VectorSearchHitsPlan plan =
              org.apache.iceberg.spark.index.VectorSearchCalls.plan(
                  spark(),
                  table,
                  column,
                  indexArgument,
                  List.of(toFloats(queryDoubles)),
                  topK,
                  false);
          List<org.apache.spark.sql.Row> hits =
              org.apache.iceberg.spark.index.VectorSearchCalls.search(plan, spark());

          List<InternalRow> rows = Lists.newArrayListWithCapacity(hits.size());
          for (org.apache.spark.sql.Row hit : hits) {
            // the single-query form of the hit schema has no query id column
            rows.add(
                newInternalRow(
                    UTF8String.fromString(hit.getString(0)),
                    hit.getLong(1),
                    hit.getDouble(2),
                    hit.isNullAt(3) ? null : UTF8String.fromString(hit.getString(3))));
          }

          return rows.toArray(new InternalRow[0]);
        });
  }

  private static float[] toFloats(double[] values) {
    float[] floats = new float[values.length];
    for (int i = 0; i < values.length; i++) {
      floats[i] = (float) values[i];
    }
    return floats;
  }

  @Override
  public String description() {
    return "VectorSearchProcedure";
  }
}
