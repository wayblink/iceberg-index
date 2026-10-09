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
import org.apache.iceberg.spark.index.VectorIndexSearch;
import org.apache.iceberg.spark.procedures.SparkProcedures.ProcedureBuilder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Searches a vector index for many queries at once and resolves the matched rows.
 *
 * <p>Like {@link VectorSearchProcedure}, each hit carries its row reference ({@code _file} + {@code
 * _pos}), score and the matched row as a JSON string, plus the index of the query it belongs to.
 * Rows are ordered by query and then by score. Unlike repeated single searches, the matched rows of
 * all queries are resolved by reading each hit data file once, so a batch of N queries costs one
 * read per distinct hit data file.
 *
 * <p>Queries are passed as an array of strings, one comma-separated list of numbers per query, for
 * example {@code array('0.95,0.05', '0.0,0.95')} for two 2-dimensional queries.
 *
 * <pre>{@code
 * CALL spark_catalog.system.vector_search_batch(
 *   table => 'db.documents', column => 'embedding',
 *   queries => array('0.95,0.05', '0.0,0.95'), k => 10);
 * }</pre>
 */
class VectorSearchBatchProcedure extends BaseProcedure {
  private static final ProcedureParameter TABLE_PARAM =
      requiredInParameter("table", DataTypes.StringType);
  private static final ProcedureParameter COLUMN_PARAM =
      requiredInParameter("column", DataTypes.StringType);
  private static final ProcedureParameter QUERIES_PARAM =
      requiredInParameter("queries", STRING_ARRAY);
  private static final ProcedureParameter K_PARAM = requiredInParameter("k", DataTypes.IntegerType);
  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {TABLE_PARAM, COLUMN_PARAM, QUERIES_PARAM, K_PARAM};

  private static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField("query_id", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("data_file", DataTypes.StringType, false, Metadata.empty()),
            new StructField("pos", DataTypes.LongType, false, Metadata.empty()),
            new StructField("score", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("row", DataTypes.StringType, true, Metadata.empty())
          });

  public static ProcedureBuilder builder() {
    return new BaseProcedure.Builder<VectorSearchBatchProcedure>() {
      @Override
      public VectorSearchBatchProcedure doBuild() {
        return new VectorSearchBatchProcedure(tableCatalog());
      }
    };
  }

  private VectorSearchBatchProcedure(TableCatalog tableCatalog) {
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

    List<float[]> queryVectors = Lists.newArrayList();
    for (String encodedQuery : input.asStringArray(QUERIES_PARAM)) {
      queryVectors.add(parseQuery(encodedQuery));
    }

    return modifyIcebergTable(
        input.ident(TABLE_PARAM),
        table -> {
          if (topK <= 0 || queryVectors.isEmpty()) {
            return new InternalRow[0];
          }

          List<Row> hits =
              VectorIndexSearch.search(spark(), table, column, queryVectors, topK).collectAsList();

          List<InternalRow> rows = Lists.newArrayListWithCapacity(hits.size());
          for (Row hit : hits) {
            rows.add(
                newInternalRow(
                    hit.getInt(0),
                    UTF8String.fromString(hit.getString(1)),
                    hit.getLong(2),
                    hit.getDouble(3),
                    hit.isNullAt(4) ? null : UTF8String.fromString(hit.getString(4))));
          }

          return rows.toArray(new InternalRow[0]);
        });
  }

  /** Parses one query vector from its comma-separated number list. */
  private static float[] parseQuery(String encodedQuery) {
    String[] values = encodedQuery.split(",");
    float[] query = new float[values.length];
    for (int i = 0; i < values.length; i++) {
      try {
        query[i] = Float.parseFloat(values[i].trim());
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            "Query '" + encodedQuery + "' is not a comma-separated list of numbers", e);
      }
    }
    return query;
  }

  @Override
  public String description() {
    return "VectorSearchBatchProcedure";
  }
}
