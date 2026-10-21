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
package org.apache.iceberg.index.spark;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.IndexQuery;
import org.apache.iceberg.index.IndexSearchResult;
import org.apache.iceberg.index.IndexSnapshot;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Searches an index of any type for many queries at once and resolves the matched rows.
 *
 * <p>Queries arrive as encoded payloads, one per query, produced by the {@link
 * org.apache.iceberg.index.IndexAdapter} of the index type: the vector adapter encodes a query
 * vector, the text adapter a query string. The search reads and scores the index files on the
 * executors, one index file per task; only the row resolution and the merge of the per-file results
 * stay on the driver, and the row resolution reads each hit data file once for all queries instead
 * of once per query.
 */
public final class IndexSearch {

  /** Column of the index of the query a hit belongs to. */
  public static final String QUERY_ID_COLUMN = "query_id";

  /** Column of the data file of a matched row. */
  public static final String DATA_FILE_COLUMN = "data_file";

  /** Column of the row position of a matched row. */
  public static final String POSITION_COLUMN = "pos";

  /** Column of the score of a hit. */
  public static final String SCORE_COLUMN = "score";

  /** Column of the matched row as a JSON object. */
  public static final String ROW_COLUMN = "row";

  /** Schema of one hit row of a single query, without the query id column. */
  public static final StructType SINGLE_RESULT_SCHEMA =
      new StructType(
          new StructField[] {
            new StructField(DATA_FILE_COLUMN, DataTypes.StringType, false, Metadata.empty()),
            new StructField(POSITION_COLUMN, DataTypes.LongType, false, Metadata.empty()),
            new StructField(SCORE_COLUMN, DataTypes.DoubleType, false, Metadata.empty()),
            new StructField(ROW_COLUMN, DataTypes.StringType, true, Metadata.empty())
          });

  /** Schema of one hit row. */
  public static final StructType RESULT_SCHEMA =
      new StructType(
          new StructField[] {
            new StructField(QUERY_ID_COLUMN, DataTypes.IntegerType, false, Metadata.empty()),
            new StructField(DATA_FILE_COLUMN, DataTypes.StringType, false, Metadata.empty()),
            new StructField(POSITION_COLUMN, DataTypes.LongType, false, Metadata.empty()),
            new StructField(SCORE_COLUMN, DataTypes.DoubleType, false, Metadata.empty()),
            new StructField(ROW_COLUMN, DataTypes.StringType, true, Metadata.empty())
          });

  private IndexSearch() {}

  /**
   * Searches the index of the given column for all queries and resolves the matched rows.
   *
   * @param spark the Spark session
   * @param table the table whose index is searched
   * @param column the indexed column
   * @param indexType the index type to search, for example {@code vector} or {@code text}
   * @param queries the encoded query payloads, one per query
   * @param topK the number of results per query
   * @return one row per hit: query id, data file, position, score and the matched row as JSON
   */
  public static Dataset<Row> search(
      SparkSession spark,
      org.apache.iceberg.Table table,
      String column,
      String indexType,
      List<ByteBuffer> queries,
      int topK) {
    return search(
        spark, table, column, VectorIndexes.findIndex(table, column, indexType), queries, topK);
  }

  /** The same search, over an index a caller already resolved (by name, or as the only one). */
  public static Dataset<Row> search(
      SparkSession spark,
      org.apache.iceberg.Table table,
      String column,
      IndexMetadata metadata,
      List<ByteBuffer> queries,
      int topK) {
    if (topK <= 0 || queries.isEmpty()) {
      return spark.createDataFrame(Lists.newArrayList(), RESULT_SCHEMA);
    }

    IndexSnapshot snapshot = metadata.latestSnapshot();
    if (snapshot == null) {
      throw new IllegalArgumentException(
          "Index for column " + column + " of table " + table.name() + " has no snapshot");
    }

    org.apache.iceberg.index.IndexAdapter adapter =
        org.apache.iceberg.index.IndexTypes.fromType(metadata.indexType())
            .createAdapter(metadata.definition());
    // every query is answered from the same snapshot, so one pass over the index files serves the
    // whole batch: each file is read and prepared once and searched once per query
    List<IndexQuery> indexQueries = Lists.newArrayListWithCapacity(queries.size());
    for (ByteBuffer query : queries) {
      indexQueries.add(IndexQuery.of(snapshot.sourceTableSnapshotId(), query.duplicate(), topK));
    }
    List<List<IndexSearchResult>> hitsPerQuery =
        DistributedIndexSearch.searchBatch(spark, metadata, table, indexQueries);

    // the hits are positions in the files the index was built from, but the rows are read from the
    // table as it is now: a row deleted since the index was built must not be returned, and a file
    // that is no longer part of the table leaves its hits without a row
    long resolveSnapshotId =
        table.currentSnapshot() != null
            ? table.currentSnapshot().snapshotId()
            : snapshot.sourceTableSnapshotId();
    Map<String, Map<Long, String>> rowsByFile =
        resolveRows(spark, table, resolveSnapshotId, hitsPerQuery);

    List<Row> rows = Lists.newArrayList();
    for (int queryId = 0; queryId < hitsPerQuery.size(); queryId++) {
      for (IndexSearchResult hit : hitsPerQuery.get(queryId)) {
        String row =
            rowsByFile.getOrDefault(hit.dataFile(), Maps.newHashMap()).get(hit.rowPosition());
        rows.add(RowFactory.create(queryId, hit.dataFile(), hit.rowPosition(), hit.score(), row));
      }
    }

    return spark.createDataFrame(rows, RESULT_SCHEMA);
  }

  /**
   * Searches the index of the given column for one query and resolves the matched rows, without the
   * query id column. The rows are the same the batch search returns, with the query id dropped.
   *
   * @param spark the Spark session
   * @param table the table whose index is searched
   * @param column the indexed column
   * @param indexType the index type to search, for example {@code vector} or {@code text}
   * @param query the encoded query payload
   * @param topK the number of results
   * @return one row per hit: data file, position, score and the matched row as JSON
   */
  public static Dataset<Row> search(
      SparkSession spark,
      org.apache.iceberg.Table table,
      String column,
      String indexType,
      ByteBuffer query,
      int topK) {
    return search(spark, table, column, indexType, List.of(query), topK).drop(QUERY_ID_COLUMN);
  }

  /**
   * Reads the matched rows of every hit data file once and serializes them as JSON, keyed by data
   * file and row position. Positions are the row ordinals of the data file, which is what the index
   * records.
   *
   * <p>The rows are read through the same delete-aware reader the index is built with, so a row
   * removed after the index snapshot was built is not returned. Its position is then absent from
   * the result and the hit is reported without a row.
   */
  private static Map<String, Map<Long, String>> resolveRows(
      SparkSession spark,
      org.apache.iceberg.Table table,
      long snapshotId,
      List<List<IndexSearchResult>> hitsPerQuery) {
    Map<String, Set<Long>> positionsByFile = new LinkedHashMap<>();
    for (List<IndexSearchResult> hits : hitsPerQuery) {
      for (IndexSearchResult hit : hits) {
        positionsByFile
            .computeIfAbsent(hit.dataFile(), key -> Sets.newHashSet())
            .add(hit.rowPosition());
      }
    }

    List<String> columns = Lists.newArrayList();
    for (Types.NestedField field : table.schema().columns()) {
      columns.add(field.name());
    }

    Map<String, Map<Long, Row>> rowsByFile =
        IndexEntries.rowsAt(spark, table, snapshotId, positionsByFile, columns, null);

    Map<String, Map<Long, String>> jsonByFile = Maps.newHashMap();
    for (Map.Entry<String, Map<Long, Row>> entry : rowsByFile.entrySet()) {
      Map<Long, Row> rows = entry.getValue();
      Map<Long, String> json = Maps.newLinkedHashMap();
      if (!rows.isEmpty()) {
        List<Row> rowList = Lists.newArrayList(rows.values());
        List<String> serialized =
            spark.createDataFrame(rowList, rowList.get(0).schema()).toJSON().collectAsList();
        int index = 0;
        for (Long position : rows.keySet()) {
          json.put(position, serialized.get(index));
          index += 1;
        }
      }
      jsonByFile.put(entry.getKey(), json);
    }

    return jsonByFile;
  }
}
