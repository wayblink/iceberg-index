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
package org.apache.iceberg.spark.index;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.IndexQuery;
import org.apache.iceberg.index.IndexReader;
import org.apache.iceberg.index.IndexSearchResult;
import org.apache.iceberg.index.IndexSnapshot;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

/**
 * Searches an index of any type for many queries at once and resolves the matched rows.
 *
 * <p>Queries arrive as encoded payloads, one per query, produced by the {@link
 * org.apache.iceberg.index.IndexAdapter} of the index type: the vector adapter encodes a query
 * vector, the text adapter a query string. The index search itself runs on the driver, because
 * index files are small and there is nothing to distribute; only the row resolution is distributed,
 * and it reads each hit data file once for all queries instead of once per query.
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
    if (topK <= 0 || queries.isEmpty()) {
      return spark.createDataFrame(Lists.newArrayList(), RESULT_SCHEMA);
    }

    IndexMetadata metadata = VectorIndexes.findIndex(table, column, indexType);
    IndexSnapshot snapshot = metadata.latestSnapshot();
    if (snapshot == null) {
      throw new IllegalArgumentException(
          "Index for column " + column + " of table " + table.name() + " has no snapshot");
    }

    org.apache.iceberg.index.IndexAdapter adapter =
        org.apache.iceberg.index.IndexTypes.fromType(metadata.indexType())
            .createAdapter(metadata.definition());
    IndexReader reader = new IndexReader(metadata, table.io());

    List<List<IndexSearchResult>> hitsPerQuery = Lists.newArrayList();
    for (ByteBuffer query : queries) {
      List<IndexSearchResult> hits =
          Lists.newArrayList(
              reader.search(
                  IndexQuery.of(snapshot.sourceTableSnapshotId(), query.duplicate(), topK)));
      hits.sort(adapter.resultComparator());
      hitsPerQuery.add(hits);
    }

    Map<String, Map<Long, String>> rowsByFile = resolveRows(spark, hitsPerQuery);

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
   * Reads the matched rows of every hit data file once and serializes them as JSON, keyed by data
   * file and row position. Positions are the row ordinals of the data file, which is what the index
   * records.
   */
  private static Map<String, Map<Long, String>> resolveRows(
      SparkSession spark, List<List<IndexSearchResult>> hitsPerQuery) {
    Map<String, Set<Long>> positionsByFile = new LinkedHashMap<>();
    for (List<IndexSearchResult> hits : hitsPerQuery) {
      for (IndexSearchResult hit : hits) {
        positionsByFile
            .computeIfAbsent(hit.dataFile(), key -> Sets.newHashSet())
            .add(hit.rowPosition());
      }
    }

    Map<String, Map<Long, String>> rowsByFile = Maps.newHashMap();
    for (Map.Entry<String, Set<Long>> entry : positionsByFile.entrySet()) {
      String dataFile = entry.getKey();
      Set<Long> positions = entry.getValue();
      JavaPairRDD<String, Long> zipped =
          spark
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
}
