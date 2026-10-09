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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.Table;
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
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

/**
 * Searches a vector index for many queries at once and resolves the matched rows.
 *
 * <p>The index itself is searched on the driver: it reads the small index files and scores the
 * candidates, which needs no cluster. Row resolution is the expensive part, so it is amortized
 * across queries: every data file that any query hits is read exactly once, and the positions
 * needed by all queries are looked up in that single pass. Searching N queries therefore costs one
 * read per distinct hit data file instead of one read per query per hit data file.
 *
 * <p>The returned rows carry the query index, the row reference ({@code _file} + {@code _pos}) with
 * its score, and the matched row itself as a JSON string, ordered by query and then by score.
 */
public final class VectorIndexSearch {

  public static final String QUERY_ID_COLUMN = "query_id";
  public static final String DATA_FILE_COLUMN = "data_file";
  public static final String POSITION_COLUMN = "pos";
  public static final String SCORE_COLUMN = "score";
  public static final String ROW_COLUMN = "row";

  public static final StructType RESULT_SCHEMA =
      new StructType()
          .add(QUERY_ID_COLUMN, DataTypes.IntegerType, false)
          .add(DATA_FILE_COLUMN, DataTypes.StringType, false)
          .add(POSITION_COLUMN, DataTypes.LongType, false)
          .add(SCORE_COLUMN, DataTypes.DoubleType, false)
          .add(ROW_COLUMN, DataTypes.StringType, true);

  private VectorIndexSearch() {}

  /**
   * Searches the index of the given column for all queries and resolves the matched rows.
   *
   * @param spark the Spark session
   * @param table the table whose index is searched
   * @param vectorColumn the indexed vector column
   * @param queries the query vectors, all of the index dimension
   * @param topK the number of results per query
   * @return one row per hit: query id, data file, position, score and the matched row as JSON
   */
  public static Dataset<Row> search(
      SparkSession spark, Table table, String vectorColumn, List<float[]> queries, int topK) {
    VectorIndexes.registerVectorIndexType();
    if (topK <= 0 || queries.isEmpty()) {
      return spark.createDataFrame(Lists.newArrayList(), RESULT_SCHEMA);
    }

    IndexMetadata metadata = VectorIndexes.findIndex(table, vectorColumn, "vector");
    IndexSnapshot snapshot = metadata.latestSnapshot();
    if (snapshot == null) {
      throw new IllegalArgumentException(
          "Index for column " + vectorColumn + " of table " + table.name() + " has no snapshot");
    }

    VectorIndexDefinition definition = (VectorIndexDefinition) metadata.definition();
    VectorIndexAdapter adapter = new VectorIndexAdapter(definition);
    IndexReader reader = new IndexReader(metadata, table.io());

    // the index search itself runs on the driver: small index files, no cluster work
    List<List<IndexSearchResult>> hitsPerQuery = Lists.newArrayList();
    for (float[] query : queries) {
      if (query.length != definition.dimension()) {
        throw new IllegalArgumentException(
            "Query dimension "
                + query.length
                + " does not match index dimension "
                + definition.dimension());
      }

      List<IndexSearchResult> hits =
          Lists.newArrayList(
              reader.search(
                  IndexQuery.of(
                      snapshot.sourceTableSnapshotId(), adapter.encodeQuery(query), topK)));
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
