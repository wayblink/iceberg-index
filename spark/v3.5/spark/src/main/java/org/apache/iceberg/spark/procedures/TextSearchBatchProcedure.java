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

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.spark.IndexSearch;
import org.apache.iceberg.index.text.TextIndexAdapter;
import org.apache.iceberg.index.text.TextIndexDefinition;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.procedures.SparkProcedures.ProcedureBuilder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Searches a BM25 text index for many queries at once and resolves the matched rows.
 *
 * <p>Every hit carries the index of the query it belongs to ({@code query_id}), its row reference,
 * score and the matched row as a JSON string. Rows are ordered by query and then by score.
 *
 * <p>Unlike repeated single searches, the matched rows of all queries are resolved by reading each
 * hit data file once, so a batch of N queries costs one read per distinct hit data file instead of
 * one read per query per hit data file.
 *
 * <pre>{@code
 * CALL spark_catalog.system.text_search_batch(
 *   table => 'db.documents', column => 'body',
 *   queries => array('iceberg table', 'vector search'), k => 10);
 * }</pre>
 */
class TextSearchBatchProcedure extends BaseProcedure {
  private static final ProcedureParameter TABLE_PARAM =
      requiredInParameter("table", DataTypes.StringType);
  private static final ProcedureParameter COLUMN_PARAM =
      requiredInParameter("column", DataTypes.StringType);
  private static final ProcedureParameter QUERIES_PARAM =
      requiredInParameter("queries", DataTypes.createArrayType(DataTypes.StringType));
  private static final ProcedureParameter K_PARAM = requiredInParameter("k", DataTypes.IntegerType);
  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {TABLE_PARAM, COLUMN_PARAM, QUERIES_PARAM, K_PARAM};

  private static final StructType OUTPUT_TYPE = IndexSearch.RESULT_SCHEMA;

  public static ProcedureBuilder builder() {
    return new BaseProcedure.Builder<TextSearchBatchProcedure>() {
      @Override
      public TextSearchBatchProcedure doBuild() {
        return new TextSearchBatchProcedure(tableCatalog());
      }
    };
  }

  private TextSearchBatchProcedure(TableCatalog tableCatalog) {
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
  public org.apache.spark.sql.catalyst.InternalRow[] call(
      org.apache.spark.sql.catalyst.InternalRow args) {
    VectorIndexProcedures.registerTextIndexType();
    ProcedureInput input = new ProcedureInput(spark(), tableCatalog(), PARAMETERS, args);
    String column = input.asString(COLUMN_PARAM);
    int topK = input.asInt(K_PARAM);

    return modifyIcebergTable(
        input.ident(TABLE_PARAM),
        table -> {
          if (topK <= 0) {
            return new org.apache.spark.sql.catalyst.InternalRow[0];
          }

          IndexMetadata metadata = VectorIndexProcedures.findIndex(table, column, "text");
          TextIndexDefinition definition = (TextIndexDefinition) metadata.definition();
          TextIndexAdapter adapter = new TextIndexAdapter(definition);

          List<ByteBuffer> payloads = Lists.newArrayList();
          for (String query : input.asStringArray(QUERIES_PARAM, new String[0])) {
            payloads.add(adapter.encodeQuery(query));
          }

          List<Row> hits =
              IndexSearch.search(spark(), table, column, "text", payloads, topK).collectAsList();

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

  @Override
  public String description() {
    return "TextSearchBatchProcedure";
  }
}
