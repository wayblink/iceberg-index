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
package org.apache.iceberg.spark.extensions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.index.vector.VectorIndexTypes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Verifies batch vector search: all queries are answered in one call, each hit carries its query
 * id, and the results are identical to running the queries one by one.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class TestVectorIndexSearchBatch extends ExtensionsTestBase {

  @BeforeAll
  static void registerVectorIndexType() {
    VectorIndexTypes.register();
  }

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName("vectors_batch_match_" + catalogName));
    sql("DROP TABLE IF EXISTS %s", tableName("vectors_batch_from_table_" + catalogName));
    sql("DROP TABLE IF EXISTS %s", tableName("vectors_batch_queries_" + catalogName));
    sql("DROP TABLE IF EXISTS %s", tableName("vectors_batch_empty_" + catalogName));
  }

  @TestTemplate
  public void testBatchSearchMatchesSingleSearches() {
    String tableName = tableName("vectors_batch_match_" + catalogName);
    sql("CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg", tableName);
    sql(
        "INSERT INTO %s SELECT id, array(CAST(id AS double), CAST(10 - id AS double)) "
            + "FROM range(0, 20)",
        tableName);
    sql(
        "CALL %s.system.build_vector_index("
            + "table => '%s', column => 'embedding', dimension => 2, metric => 'euclidean')",
        catalogName, tableName);

    // three queries: closest to (0,10), (10,0) and (5,5)
    List<Object[]> batch =
        sql(
            "CALL %s.system.vector_search_batch(table => '%s', column => 'embedding', "
                + "queries => array('0.0,10.0', '10.0,0.0', '5.0,5.0'), k => 3)",
            catalogName, tableName);

    // every query returns its hits, grouped and ordered by query id
    assertThat(batch).hasSize(9);
    List<Integer> queryIds =
        batch.stream().map(row -> ((Number) row[0]).intValue()).collect(Collectors.toList());
    assertThat(queryIds).containsExactly(0, 0, 0, 1, 1, 1, 2, 2, 2);

    // query 0 ranks by ascending distance to (0, 10): ids 0, 1, 2
    assertThat(batch.subList(0, 3).stream().map(row -> parseId((String) row[4])))
        .containsExactly(0L, 1L, 2L);
    // query 1 ranks by ascending distance to (10, 0): id 10, then ids 9 and 11 (equal distance,
    // broken by row position), then id 8
    assertThat(batch.subList(3, 6).stream().map(row -> parseId((String) row[4])))
        .containsExactly(10L, 9L, 11L);
    // query 2 ranks by ascending distance to (5, 5): id 5, then ids 4 and 6 (equal distance,
    // broken by row position)
    assertThat(batch.subList(6, 9).stream().map(row -> parseId((String) row[4])))
        .containsExactly(5L, 4L, 6L);

    // scores ascend within each query (euclidean distance)
    for (int start : new int[] {0, 3, 6}) {
      double first = ((Number) batch.get(start)[3]).doubleValue();
      double last = ((Number) batch.get(start + 2)[3]).doubleValue();
      assertThat(first).isLessThanOrEqualTo(last);
    }

    // the same queries one by one return the same rows in the same order
    for (int queryId = 0; queryId < 3; queryId++) {
      List<Object[]> single =
          sql(
              "CALL %s.system.vector_search(table => '%s', column => 'embedding', "
                  + "query => array(%s), k => 3)",
              catalogName, tableName, queryLiteral(queryId));
      List<Object[]> batchForQuery = batch.subList(queryId * 3, queryId * 3 + 3);
      for (int i = 0; i < 3; i++) {
        assertThat(batchForQuery.get(i)[1]).isEqualTo(single.get(i)[0]);
        assertThat(batchForQuery.get(i)[2]).isEqualTo(single.get(i)[1]);
        assertThat(batchForQuery.get(i)[3]).isEqualTo(single.get(i)[2]);
        assertThat(batchForQuery.get(i)[4]).isEqualTo(single.get(i)[3]);
      }
    }
  }

  @TestTemplate
  public void testBatchSearchRejectsMalformedQuery() {
    String tableName = tableName("vectors_batch_empty_" + catalogName);
    sql("CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg", tableName);
    sql("INSERT INTO %s SELECT id, array(CAST(id AS double), 0.0) FROM range(0, 5)", tableName);
    sql(
        "CALL %s.system.build_vector_index("
            + "table => '%s', column => 'embedding', dimension => 2, metric => 'euclidean')",
        catalogName, tableName);

    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.vector_search_batch(table => '%s', column => 'embedding', "
                        + "queries => array('not-a-vector'), k => 3)",
                    catalogName, tableName))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("comma-separated list of numbers");
  }

  private static String queryLiteral(int queryId) {
    switch (queryId) {
      case 0:
        return "0.0d, 10.0d";
      case 1:
        return "10.0d, 0.0d";
      default:
        return "5.0d, 5.0d";
    }
  }

  private static long parseId(String rowJson) {
    int start = rowJson.indexOf("\"id\":") + 5;
    int end = rowJson.indexOf(',', start);
    return Long.parseLong(rowJson.substring(start, end == -1 ? rowJson.length() - 1 : end));
  }

  @TestTemplate
  public void testBatchSearchFromAQueryTable() {
    String tableName = tableName("vectors_batch_from_table_" + catalogName);
    String queriesName = tableName("vectors_batch_queries_" + catalogName);
    sql("DROP TABLE IF EXISTS %s", queriesName);
    sql("CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg", tableName);
    sql(
        "INSERT INTO %s SELECT id, array(CAST(id AS double), CAST(10 - id AS double)) "
            + "FROM range(0, 20)",
        tableName);
    sql(
        "CALL %s.system.build_vector_index("
            + "table => '%s', column => 'embedding', dimension => 2, metric => 'euclidean')",
        catalogName, tableName);

    sql("CREATE TABLE %s (query_id bigint, q array<double>) USING iceberg", queriesName);
    sql(
        "INSERT INTO %s VALUES (7, array(0.0, 10.0)), (3, array(10.0, 0.0)), (5, array(5.0, 5.0))",
        queriesName);

    // the queries come from a table and are answered in query_id order
    List<Object[]> fromTable =
        sql(
            "CALL %s.system.vector_search_batch(table => '%s', column => 'embedding', "
                + "query_table => '%s', query_id_column => 'query_id', vector_column => 'q', k => 3)",
            catalogName, tableName, queriesName);

    List<Object[]> inline =
        sql(
            "CALL %s.system.vector_search_batch(table => '%s', column => 'embedding', "
                + "queries => array('10.0,0.0', '5.0,5.0', '0.0,10.0'), k => 3)",
            catalogName, tableName);

    // same three queries, ordered by query id, so the rows must agree query for query
    assertThat(fromTable).hasSize(inline.size());
    for (int i = 0; i < inline.size(); i++) {
      assertThat(parseId((String) fromTable.get(i)[4]))
          .as("row %s", i)
          .isEqualTo(parseId((String) inline.get(i)[4]));
    }
    sql("DROP TABLE IF EXISTS %s", queriesName);
  }
}
