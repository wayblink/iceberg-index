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
import java.util.List;
import org.apache.iceberg.Table;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.vector.VectorIndexAdapter;
import org.apache.iceberg.index.vector.VectorIndexDefinition;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

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
 *
 * <p>This is the vector entry point of {@link IndexSearch}, which holds the search and row
 * resolution for any index type; here the query vectors are encoded and checked against the index
 * dimension.
 */
public final class VectorIndexSearch {

  /** Column of the index of the query a hit belongs to. */
  public static final String QUERY_ID_COLUMN = IndexSearch.QUERY_ID_COLUMN;

  /** Column of the data file of a matched row. */
  public static final String DATA_FILE_COLUMN = IndexSearch.DATA_FILE_COLUMN;

  /** Column of the row position of a matched row. */
  public static final String POSITION_COLUMN = IndexSearch.POSITION_COLUMN;

  /** Column of the score of a hit. */
  public static final String SCORE_COLUMN = IndexSearch.SCORE_COLUMN;

  /** Column of the matched row as a JSON object. */
  public static final String ROW_COLUMN = IndexSearch.ROW_COLUMN;

  /** Schema of one hit row. */
  public static final StructType RESULT_SCHEMA = IndexSearch.RESULT_SCHEMA;

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
    return search(
        spark,
        table,
        vectorColumn,
        VectorIndexes.findIndex(table, vectorColumn, "vector"),
        queries,
        topK);
  }

  /** The same search over an index the caller resolved, which is how a named index is searched. */
  public static Dataset<Row> search(
      SparkSession spark,
      Table table,
      String vectorColumn,
      IndexMetadata metadata,
      List<float[]> queries,
      int topK) {
    VectorIndexes.registerVectorIndexType();
    VectorIndexDefinition definition = (VectorIndexDefinition) metadata.definition();
    VectorIndexAdapter adapter = new VectorIndexAdapter(definition);

    List<ByteBuffer> payloads = Lists.newArrayList();
    for (float[] query : queries) {
      if (query.length != definition.dimension()) {
        throw new IllegalArgumentException(
            "Query dimension "
                + query.length
                + " does not match index dimension "
                + definition.dimension());
      }
      payloads.add(adapter.encodeQuery(query));
    }

    return IndexSearch.search(spark, table, vectorColumn, metadata, payloads, topK);
  }
}
