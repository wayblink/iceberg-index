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
import org.apache.iceberg.index.IndexQuery;
import org.apache.iceberg.index.IndexReader;
import org.apache.iceberg.index.IndexSearchResult;
import org.apache.iceberg.index.IndexSnapshot;
import org.apache.iceberg.index.text.TextIndexAdapter;
import org.apache.iceberg.index.text.TextIndexDefinition;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.procedures.SparkProcedures.ProcedureBuilder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import scala.Tuple2;

/**
 * Searches a BM25 text index and resolves the matched rows.
 *
 * <p>Returns the row reference ({@code _file} + {@code _pos}) of each hit with its score, plus the
 * matched row itself as a JSON string. Rows are ordered by score, descending.
 *
 * <pre>{@code
 * CALL spark_catalog.system.text_search(
 *   table => 'db.documents', column => 'body', query => 'iceberg vector search', k => 10);
 * }</pre>
 */
class TextSearchProcedure extends BaseProcedure {
  private static final ProcedureParameter TABLE_PARAM =
      requiredInParameter("table", DataTypes.StringType);
  private static final ProcedureParameter COLUMN_PARAM =
      requiredInParameter("column", DataTypes.StringType);
  private static final ProcedureParameter QUERY_PARAM =
      requiredInParameter("query", DataTypes.StringType);
  private static final ProcedureParameter K_PARAM = requiredInParameter("k", DataTypes.IntegerType);
  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {TABLE_PARAM, COLUMN_PARAM, QUERY_PARAM, K_PARAM};

  private static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField("data_file", DataTypes.StringType, false, Metadata.empty()),
            new StructField("pos", DataTypes.LongType, false, Metadata.empty()),
            new StructField("score", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("row", DataTypes.StringType, true, Metadata.empty())
          });

  public static ProcedureBuilder builder() {
    return new BaseProcedure.Builder<TextSearchProcedure>() {
      @Override
      public TextSearchProcedure doBuild() {
        return new TextSearchProcedure(tableCatalog());
      }
    };
  }

  private TextSearchProcedure(TableCatalog tableCatalog) {
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
    String query = input.asString(QUERY_PARAM);
    int topK = input.asInt(K_PARAM);

    return modifyIcebergTable(
        input.ident(TABLE_PARAM),
        table -> {
          if (topK <= 0) {
            return new InternalRow[0];
          }

          IndexMetadata metadata = VectorIndexProcedures.findIndex(table, column, "text");
          IndexSnapshot snapshot = metadata.latestSnapshot();
          if (snapshot == null) {
            throw new IllegalArgumentException(
                "Index for column " + column + " of table " + tableName + " has no snapshot");
          }

          TextIndexDefinition definition = (TextIndexDefinition) metadata.definition();
          TextIndexAdapter adapter = new TextIndexAdapter(definition);
          List<IndexSearchResult> hits =
              new IndexReader(metadata, table.io())
                  .search(
                      IndexQuery.of(
                          snapshot.sourceTableSnapshotId(), adapter.encodeQuery(query), topK));

          List<Row> resolved = resolveRows(table, hits);
          List<InternalRow> rows = Lists.newArrayListWithCapacity(resolved.size());
          for (int i = 0; i < hits.size(); i++) {
            IndexSearchResult hit = hits.get(i);
            String row = resolved.get(i) == null ? null : resolved.get(i).getString(0);
            rows.add(
                newInternalRow(
                    UTF8String.fromString(hit.dataFile()),
                    hit.rowPosition(),
                    hit.score(),
                    row == null ? null : UTF8String.fromString(row)));
          }

          return rows.toArray(new InternalRow[0]);
        });
  }

  /** Reads the matched rows of each hit data file, serialized as JSON, in hit order. */
  private List<Row> resolveRows(org.apache.iceberg.Table table, List<IndexSearchResult> hits) {
    java.util.Map<String, java.util.Set<Long>> positionsByFile = new java.util.LinkedHashMap<>();
    for (IndexSearchResult hit : hits) {
      positionsByFile
          .computeIfAbsent(hit.dataFile(), key -> new java.util.HashSet<>())
          .add(hit.rowPosition());
    }

    java.util.Map<String, java.util.Map<Long, String>> rowsByFile = new java.util.HashMap<>();
    for (java.util.Map.Entry<String, java.util.Set<Long>> entry : positionsByFile.entrySet()) {
      String dataFile = entry.getKey();
      java.util.Set<Long> positions = entry.getValue();
      List<Tuple2<String, Long>> zipped =
          spark()
              .read()
              .parquet(dataFile)
              .toJSON()
              .toJavaRDD()
              .zipWithIndex()
              .filter(tuple -> positions.contains(tuple._2()))
              .collect();
      java.util.Map<Long, String> rows = new java.util.HashMap<>();
      for (Tuple2<String, Long> tuple : zipped) {
        rows.put(tuple._2(), tuple._1());
      }
      rowsByFile.put(dataFile, rows);
    }

    List<Row> resolved = Lists.newArrayListWithCapacity(hits.size());
    for (IndexSearchResult hit : hits) {
      resolved.add(
          RowFactory.create(
              rowsByFile
                  .getOrDefault(hit.dataFile(), new java.util.HashMap<>())
                  .get(hit.rowPosition())));
    }
    return resolved;
  }

  @Override
  public String description() {
    return "TextSearchProcedure";
  }
}
