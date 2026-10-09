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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.IndexQuery;
import org.apache.iceberg.index.IndexReader;
import org.apache.iceberg.index.IndexSearchResult;
import org.apache.iceberg.index.IndexSnapshot;
import org.apache.iceberg.index.vector.VectorIndexAdapter;
import org.apache.iceberg.index.vector.VectorIndexDefinition;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.procedures.SparkProcedures.ProcedureBuilder;
import org.apache.spark.api.java.JavaPairRDD;
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
 * Searches a vector index and resolves the matched rows.
 *
 * <p>Returns the row reference ({@code _file} + {@code _pos}) of each hit with its score, plus the
 * matched row itself as a JSON string. Rows are ordered by score, descending.
 *
 * <pre>{@code
 * CALL spark_catalog.system.vector_search(
 *   table => 'db.documents', column => 'embedding',
 *   query => array(0.95, 0.05), k => 10);
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
    String tableName = input.asString(TABLE_PARAM);
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

          IndexMetadata metadata = VectorIndexProcedures.findIndex(table, column);
          IndexSnapshot snapshot = metadata.latestSnapshot();
          if (snapshot == null) {
            throw new IllegalArgumentException(
                "Index for column " + column + " of table " + tableName + " has no snapshot");
          }

          VectorIndexDefinition definition = (VectorIndexDefinition) metadata.definition();
          VectorIndexAdapter adapter = new VectorIndexAdapter(definition);
          float[] query = toFloats(queryDoubles);
          if (query.length != definition.dimension()) {
            throw new IllegalArgumentException(
                "Query dimension "
                    + query.length
                    + " does not match index dimension "
                    + definition.dimension());
          }

          List<IndexSearchResult> results =
              Lists.newArrayList(
                  new IndexReader(metadata, table.io())
                      .search(
                          IndexQuery.of(
                              snapshot.sourceTableSnapshotId(), adapter.encodeQuery(query), topK)));
          // the reader sorts with the metric-aware comparator (distance ascending, similarity
          // descending); re-sort defensively with the same comparator
          results.sort(adapter.resultComparator());

          Map<String, Map<Long, String>> rowsByFile = resolveRows(results);
          List<InternalRow> rows = Lists.newArrayListWithCapacity(results.size());
          for (IndexSearchResult result : results) {
            String row =
                rowsByFile
                    .getOrDefault(result.dataFile(), Maps.newHashMap())
                    .get(result.rowPosition());
            rows.add(
                newInternalRow(
                    UTF8String.fromString(result.dataFile()),
                    result.rowPosition(),
                    result.score(),
                    row == null ? null : UTF8String.fromString(row)));
          }

          return rows.toArray(new InternalRow[0]);
        });
  }

  /**
   * Reads the matched rows of each hit data file and serializes them as JSON, keyed by data file
   * and row position.
   */
  private Map<String, Map<Long, String>> resolveRows(List<IndexSearchResult> results) {
    Map<String, Set<Long>> positionsByFile = new LinkedHashMap<>();
    for (IndexSearchResult result : results) {
      positionsByFile
          .computeIfAbsent(result.dataFile(), key -> Sets.newHashSet())
          .add(result.rowPosition());
    }

    Map<String, Map<Long, String>> rowsByFile = Maps.newHashMap();
    for (Map.Entry<String, Set<Long>> entry : positionsByFile.entrySet()) {
      String dataFile = entry.getKey();
      Set<Long> positions = entry.getValue();
      JavaPairRDD<String, Long> zipped =
          spark()
              .read()
              .parquet(dataFile)
              .toJSON()
              .toJavaRDD()
              .zipWithIndex()
              .filter(tuple -> positions.contains(tuple._2()));
      Map<Long, String> rows = Maps.newHashMap();
      for (Tuple2<String, Long> tuple : zipped.collect()) {
        rows.put(tuple._2(), tuple._1());
      }
      rowsByFile.put(dataFile, rows);
    }

    return rowsByFile;
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
