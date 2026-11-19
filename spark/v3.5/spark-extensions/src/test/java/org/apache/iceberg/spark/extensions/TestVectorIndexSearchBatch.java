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

import java.util.ArrayList;
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

  private static final String CACHE_ENABLED = "spark.iceberg.index.cache.enabled";

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
    sql("DROP TABLE IF EXISTS %s", tableName("vectors_batch_cached_" + catalogName));
    sql("DROP TABLE IF EXISTS %s", tableName("vectors_batch_cached_deletes_" + catalogName));
    sql("DROP TABLE IF EXISTS %s", tableName("vectors_batch_located_" + catalogName));
    sql("DROP TABLE IF EXISTS %s", tableName("vectors_batch_order_" + catalogName));
    sql("DROP TABLE IF EXISTS %s", tableName("vectors_batch_blocks_" + catalogName));
    sql("DROP TABLE IF EXISTS %s", tableName("vectors_batch_uncovered_" + catalogName));
    sql("DROP TABLE IF EXISTS %s", tableName("vectors_batch_override_" + catalogName));
    sql("DROP TABLE IF EXISTS %s", tableName("vectors_batch_schema_" + catalogName));
    sql("DROP TABLE IF EXISTS %s", tableName("vectors_batch_jobs_" + catalogName));
    sql("DROP TABLE IF EXISTS %s", tableName("vectors_batch_address_" + catalogName));
  }

  @TestTemplate
  public void cachedBatchSearchReturnsTheSameHits() {
    String tableName = tableName("vectors_batch_cached_" + catalogName);
    sql("CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg", tableName);
    sql(
        "INSERT INTO %s SELECT id, array(CAST(id AS double), CAST(10 - id AS double)) "
            + "FROM range(0, 20)",
        tableName);
    sql(
        "CALL %s.system.build_vector_index("
            + "table => '%s', column => 'embedding', dimension => 2, metric => 'l2')",
        catalogName, tableName);

    String search =
        "CALL %s.system.vector_search_batch(table => '%s', column => 'embedding', "
            + "queries => array('0.0,10.0', '10.0,0.0', '5.0,5.0'), k => 3)";

    List<Object[]> uncached = sql(search, catalogName, tableName);

    spark.conf().set(CACHE_ENABLED, "true");
    try {
      // cold: every index file is read and prepared for this batch, the first one with the cache on
      List<Object[]> cold = sql(search, catalogName, tableName);
      // warm: the payloads and their prepared structures come from the executor's cache, so the
      // hits have to be the same rows in the same order
      List<Object[]> warm = sql(search, catalogName, tableName);
      // the executor is this JVM here, so the second batch has to have come from the cache
      assertThat(org.apache.iceberg.index.spark.ExecutorIndexCacheProbe.stats().hits())
          .isPositive();
      assertThat(warm).hasSameSizeAs(cold);
      for (int i = 0; i < cold.size(); i++) {
        assertThat(rowValues(warm.get(i))).isEqualTo(rowValues(cold.get(i)));
      }
      for (int i = 0; i < uncached.size(); i++) {
        assertThat(rowValues(warm.get(i))).isEqualTo(rowValues(uncached.get(i)));
      }
    } finally {
      spark.conf().unset(CACHE_ENABLED);
    }
  }

  @TestTemplate
  public void equalDistancesAreOrderedByTheirPositionNotByTheCache() {
    String tableName = tableName("vectors_batch_order_" + catalogName);
    sql("CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg", tableName);
    // four rows, each in a file of its own, all exactly one unit from the query: the answer orders
    // them by where they are, not by the order the tasks happened to produce
    sql("INSERT INTO %s VALUES (1, array(1.0d, 0.0d))", tableName);
    sql("INSERT INTO %s VALUES (2, array(-1.0d, 0.0d))", tableName);
    sql("INSERT INTO %s VALUES (3, array(0.0d, 1.0d))", tableName);
    sql("INSERT INTO %s VALUES (4, array(0.0d, -1.0d))", tableName);
    sql(
        "CALL %s.system.build_vector_index("
            + "table => '%s', column => 'embedding', dimension => 2, metric => 'l2')",
        catalogName, tableName);

    String search =
        "CALL %s.system.vector_search_batch(table => '%s', column => 'embedding', "
            + "queries => array('0.0,0.0'), k => 4)";

    List<Object[]> uncached = sql(search, catalogName, tableName);
    assertThat(uncached).hasSize(4);
    assertOrderedByPosition(uncached);

    spark.conf().set(CACHE_ENABLED, "true");
    try {
      List<Object[]> cold = sql(search, catalogName, tableName);
      List<Object[]> warm = sql(search, catalogName, tableName);
      assertOrderedByPosition(cold);
      assertOrderedByPosition(warm);
      for (List<Object[]> cached : java.util.Arrays.asList(cold, warm)) {
        assertThat(cached).hasSameSizeAs(uncached);
        for (int i = 0; i < uncached.size(); i++) {
          assertThat(rowValues(cached.get(i))).isEqualTo(rowValues(uncached.get(i)));
        }
      }
    } finally {
      spark.conf().unset(CACHE_ENABLED);
    }
  }

  /** Two hits at the same distance come back ordered by data file and then by row position. */
  private static void assertOrderedByPosition(List<Object[]> rows) {
    for (int i = 1; i < rows.size(); i += 1) {
      Object[] previous = rows.get(i - 1);
      Object[] current = rows.get(i);
      double previousDistance = ((Number) previous[3]).doubleValue();
      double currentDistance = ((Number) current[3]).doubleValue();
      if (previousDistance == currentDistance) {
        int byFile = ((String) previous[1]).compareTo((String) current[1]);
        assertThat(byFile <= 0)
            .as("row %s ties with %s and has to come first by data file", i - 1, i)
            .isTrue();
        if (byFile == 0) {
          assertThat(((Number) previous[2]).longValue())
              .as(
                  "row %s ties with %s in the same file and has to come first by position",
                  i - 1, i)
              .isLessThan(((Number) current[2]).longValue());
        }
      }
    }
  }

  @TestTemplate
  public void cachedBatchSearchAppliesDeletes() {
    String tableName = tableName("vectors_batch_cached_deletes_" + catalogName);
    sql("CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg", tableName);
    sql(
        "INSERT INTO %s SELECT id, array(CAST(id AS double), CAST(10 - id AS double)) "
            + "FROM range(0, 20)",
        tableName);
    sql(
        "CALL %s.system.build_vector_index("
            + "table => '%s', column => 'embedding', dimension => 2, metric => 'l2')",
        catalogName, tableName);

    String search =
        "CALL %s.system.vector_search_batch(table => '%s', column => 'embedding', "
            + "queries => array('0.0,10.0', '10.0,0.0'), k => 5)";

    sql("DELETE FROM %s WHERE id IN (0, 1, 10)", tableName);

    List<Object[]> uncached = sql(search, catalogName, tableName);

    spark.conf().set(CACHE_ENABLED, "true");
    try {
      // cold and warm: the same payloads, read once and then remembered, have to give the same
      // rows, and neither may return a deleted one
      List<Object[]> cold = sql(search, catalogName, tableName);
      List<Object[]> warm = sql(search, catalogName, tableName);
      for (List<Object[]> cached : java.util.Arrays.asList(cold, warm)) {
        assertThat(cached).hasSameSizeAs(uncached);
        for (int i = 0; i < uncached.size(); i++) {
          assertThat(rowValues(cached.get(i))).isEqualTo(rowValues(uncached.get(i)));
        }
      }

      // a position the delete removed has no row left to return, so the hit comes back without one
      // rather than with the deleted row's values
      List<Long> ids =
          warm.stream()
              .filter(row -> row[4] != null)
              .map(row -> parseId((String) row[4]))
              .collect(Collectors.toList());
      assertThat(ids).doesNotContain(0L, 1L, 10L);
    } finally {
      spark.conf().unset(CACHE_ENABLED);
    }
  }

  /** A batch answers each query the way that query's own call would, deletes included. */
  @TestTemplate
  public void aBatchAfterDeletesAnswersEachQueryLikeItsSingleSearch() {
    String tableName = tableName("vectors_batch_delete_equivalence_" + catalogName);
    sql("CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg", tableName);
    sql(
        "INSERT INTO %s SELECT id, array(CAST(id AS double), CAST(10 - id AS double)) "
            + "FROM range(0, 20)",
        tableName);
    sql(
        "CALL %s.system.build_vector_index("
            + "table => '%s', column => 'embedding', dimension => 2, metric => 'l2')",
        catalogName, tableName);
    sql("DELETE FROM %s WHERE id IN (0, 1, 10)", tableName);

    String[] queries = {"0.0,10.0", "10.0,0.0"};
    String[] doubles = {"0.0d, 10.0d", "10.0d, 0.0d"};
    List<Object[]> batched =
        sql(
            "CALL %s.system.vector_search_batch(table => '%s', column => 'embedding', "
                + "queries => array('%s', '%s'), k => 5)",
            catalogName, tableName, queries[0], queries[1]);

    for (int query = 0; query < queries.length; query += 1) {
      List<Object[]> single =
          sql(
              "CALL %s.system.vector_search(table => '%s', column => 'embedding', "
                  + "query => array(%s), k => 5)",
              catalogName, tableName, doubles[query]);
      int queried = query;
      List<Object[]> fromBatch =
          batched.stream()
              .filter(row -> ((Number) row[0]).intValue() == queried)
              .collect(Collectors.toList());
      assertThat(fromBatch).as("query %s", queries[query]).hasSameSizeAs(single);
      for (int i = 0; i < single.size(); i += 1) {
        // the batch row is (query_id, data file, position, score, json) and the single row is the
        // same without the query id
        assertThat(java.util.Arrays.copyOfRange(fromBatch.get(i), 1, 5))
            .as("query %s row %s", queries[query], i)
            .isEqualTo(single.get(i));
      }
    }
  }

  /**
   * The positions mode answers what the index found and nothing more: the same ranking and the same
   * estimate as the hit view, with the row left null. Nothing is read at the data files and the
   * hits are not judged against the snapshot, so a row deleted after the build still appears.
   */
  @TestTemplate
  public void positionsCoverageLeavesTheRowToTheCaller() {
    String tableName = tableName("vectors_batch_positions_" + catalogName);
    sql("CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg", tableName);
    sql(
        "INSERT INTO %s SELECT id, array(CAST(id AS double), CAST(10 - id AS double)) "
            + "FROM range(0, 20)",
        tableName);
    sql(
        "CALL %s.system.build_vector_index("
            + "table => '%s', column => 'embedding', dimension => 2, metric => 'l2')",
        catalogName, tableName);
    sql("DELETE FROM %s WHERE id IN (0, 1)", tableName);

    List<Object[]> hits =
        sql(
            "CALL %s.system.vector_search_batch(table => '%s', column => 'embedding', "
                + "queries => array('0.0,10.0', '10.0,0.0'), k => 5, coverage => 'index_hits')",
            catalogName, tableName);
    List<Object[]> positions =
        sql(
            "CALL %s.system.vector_search_batch(table => '%s', column => 'embedding', "
                + "queries => array('0.0,10.0', '10.0,0.0'), k => 5, coverage => 'positions')",
            catalogName, tableName);

    assertThat(positions).hasSameSizeAs(hits);
    for (int i = 0; i < hits.size(); i += 1) {
      // the query id, the file, the position and the score are the index's own, in its own order
      assertThat(java.util.Arrays.copyOfRange(positions.get(i), 0, 4))
          .as("row %s", i)
          .isEqualTo(java.util.Arrays.copyOfRange(hits.get(i), 0, 4));
      assertThat(positions.get(i)[4]).as("no row is resolved").isNull();
    }
  }

  /**
   * The address table function answers with the index's own hits. A hit whose row was deleted after
   * the build is still reported, with a null row, so the answer is the {@code index_hits} mode of a
   * procedure rather than the complete-table one; and the index reads its latest snapshot, so the
   * deleted rows do not have to be removed from the index for the answer to be the index's own.
   */
  @TestTemplate
  public void addressTableFunctionAnswersTheIndexHits() {
    String tableName = tableName("vectors_batch_address_" + catalogName);
    sql("CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg", tableName);
    sql(
        "INSERT INTO %s SELECT id, array(CAST(id AS double), CAST(10 - id AS double)) "
            + "FROM range(0, 20)",
        tableName);
    sql(
        "CALL %s.system.build_vector_index("
            + "table => '%s', column => 'embedding', dimension => 2, metric => 'l2')",
        catalogName, tableName);
    // the index is not refreshed after the delete, so the deleted rows are still what it ranks
    sql("DELETE FROM %s WHERE id IN (0, 1, 10)", tableName);

    List<Object[]> fromFunction =
        sql(
            "SELECT * FROM vector_index_search('%s', 'embedding', array(0.0d, 10.0d), 5)",
            tableName);
    List<Object[]> fromIndexHits =
        sql(
            "CALL %s.system.vector_search(table => '%s', column => 'embedding', "
                + "query => array(0.0d, 10.0d), k => 5, coverage => 'index_hits')",
            catalogName, tableName);
    List<Object[]> fromTable =
        sql(
            "CALL %s.system.vector_search(table => '%s', column => 'embedding', "
                + "query => array(0.0d, 10.0d), k => 5, coverage => 'table')",
            catalogName, tableName);

    // the function and the procedure's index-hits mode ask the same question
    assertThat(fromIndexHits).hasSize(5);
    assertThat(fromTable).hasSize(5);
    for (int i = 0; i < fromIndexHits.size(); i += 1) {
      assertThat(java.util.Arrays.asList(fromFunction.get(i)))
          .as("row %s", i)
          .isEqualTo(java.util.Arrays.asList(fromIndexHits.get(i)));
    }

    // the deleted rows are still hits of the index, kept without a row
    assertThat(fromFunction.stream().filter(row -> row[3] == null)).isNotEmpty();
    // while the complete-table mode answers with live rows only, which is a different answer
    assertThat(fromTable.stream().filter(row -> row[3] == null)).isEmpty();
    assertThat(fromFunction.stream().map(row -> row[3]).collect(Collectors.toList()))
        .as("the address function does not answer for the whole table")
        .isNotEqualTo(fromTable.stream().map(row -> row[3]).collect(Collectors.toList()));
  }

  /**
   * A block starts every query at the same window, but only the queries that came up short may take
   * another round: one query's nearest rows are gone, the other's are all there, and both still
   * answer with k rows that match their own single query.
   */
  @TestTemplate
  public void aBlockWidensOnlyTheQueriesThatFallShort() {
    String tableName = tableName("vectors_batch_mixed_rounds_" + catalogName);
    sql("CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg", tableName);
    sql(
        "INSERT INTO %s SELECT id, array(CAST(id AS double), CAST(10 - id AS double)) "
            + "FROM range(0, 30)",
        tableName);
    sql(
        "CALL %s.system.build_vector_index("
            + "table => '%s', column => 'embedding', dimension => 2, metric => 'l2')",
        catalogName, tableName);
    // four of the first six hits of one query are removed, so its first window cannot fill; the
    // other query's window is intact
    sql("DELETE FROM %s WHERE id IN (0, 1, 2, 3)", tableName);

    String[] queries = {"0.0,10.0", "10.0,0.0"};
    String[] doubles = {"0.0d, 10.0d", "10.0d, 0.0d"};
    List<Object[]> batched =
        sql(
            "CALL %s.system.vector_search_batch(table => '%s', column => 'embedding', "
                + "queries => array('%s', '%s'), k => 5)",
            catalogName, tableName, queries[0], queries[1]);

    for (int query = 0; query < queries.length; query += 1) {
      List<Object[]> single =
          sql(
              "CALL %s.system.vector_search(table => '%s', column => 'embedding', "
                  + "query => array(%s), k => 5)",
              catalogName, tableName, doubles[query]);
      int queried = query;
      List<Object[]> fromBatch =
          batched.stream()
              .filter(row -> ((Number) row[0]).intValue() == queried)
              .collect(Collectors.toList());
      assertThat(fromBatch).as("query %s", queries[query]).hasSameSizeAs(single);
      for (int i = 0; i < single.size(); i += 1) {
        assertThat(java.util.Arrays.copyOfRange(fromBatch.get(i), 1, 5))
            .as("query %s row %s", queries[query], i)
            .isEqualTo(single.get(i));
      }
    }

    // the widened query answered k live rows, and some of them came from beyond the window it
    // started with, which is what says a later round was taken for it and not for the other query
    List<Long> widened =
        batched.stream()
            .filter(row -> ((Number) row[0]).intValue() == 0 && row[4] != null)
            .map(row -> parseId((String) row[4]))
            .collect(Collectors.toList());
    assertThat(widened).hasSize(5).doesNotContain(0L, 1L, 2L, 3L);
    assertThat(widened)
        .as("the short query had to ask its index file for more than its first window")
        .anySatisfy(id -> assertThat(id).isGreaterThanOrEqualTo(6L));
  }

  @TestTemplate
  public void cachedSearchRecordsWhereItIsCached() {
    String tableName = tableName("vectors_batch_located_" + catalogName);
    sql("CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg", tableName);
    sql(
        "INSERT INTO %s SELECT id, array(CAST(id AS double), CAST(10 - id AS double)) "
            + "FROM range(0, 20)",
        tableName);
    sql(
        "CALL %s.system.build_vector_index("
            + "table => '%s', column => 'embedding', dimension => 2, metric => 'l2')",
        catalogName, tableName);

    String search =
        "CALL %s.system.vector_search_batch(table => '%s', column => 'embedding', "
            + "queries => array('0.0,10.0'), k => 3)";

    spark.conf().set(CACHE_ENABLED, "true");
    try {
      // the executor reports what it holds at the end of the task, so after a query the driver
      // knows where the files are; a repeated query then finds them there
      sql(search, catalogName, tableName);
      sql(search, catalogName, tableName);

      assertThat(org.apache.iceberg.index.spark.ExecutorIndexCacheProbe.locatedExecutors(spark))
          .isPositive();
    } finally {
      spark.conf().unset(CACHE_ENABLED);
    }
  }

  private static List<Object> rowValues(Object[] row) {
    return java.util.Arrays.asList(row);
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
            + "table => '%s', column => 'embedding', dimension => 2, metric => 'l2')",
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
            + "table => '%s', column => 'embedding', dimension => 2, metric => 'l2')",
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
            + "table => '%s', column => 'embedding', dimension => 2, metric => 'l2')",
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

  @TestTemplate
  public void batchSearchAnswersMoreQueriesThanOneBlock() {
    String tableName = tableName("vectors_batch_blocks_" + catalogName);
    sql("CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg", tableName);
    sql(
        "INSERT INTO %s SELECT id, array(CAST(id AS double), CAST(10 - id AS double)) "
            + "FROM range(0, 20)",
        tableName);
    sql(
        "CALL %s.system.build_vector_index("
            + "table => '%s', column => 'embedding', dimension => 2, metric => 'l2')",
        catalogName, tableName);

    // more queries than one block answers, so the block boundary is part of the answer
    List<String> queries = new ArrayList<>();
    for (int i = 0; i < 130; i += 1) {
      int id = i % 20;
      queries.add("'" + id + ".0," + (10 - id) + ".0'");
    }

    List<Object[]> batch =
        sql(
            "CALL %s.system.vector_search_batch(table => '%s', column => 'embedding', "
                + "queries => array(%s), k => 3)",
            catalogName, tableName, String.join(",", queries));

    // every query answered, in query order, with its own three rows
    assertThat(batch).hasSize(130 * 3);
    for (int i = 0; i < batch.size(); i += 1) {
      assertThat(((Number) batch.get(i)[0]).intValue()).as("row %s", i).isEqualTo(i / 3);
    }

    // the queries on both sides of the first block boundary answer what a single search does
    for (int queryId : new int[] {0, 64, 127, 128, 129}) {
      int id = queryId % 20;
      List<Object[]> single =
          sql(
              "CALL %s.system.vector_search(table => '%s', column => 'embedding', "
                  + "query => array(%s.0d, %s.0d), k => 3)",
              catalogName, tableName, id, 10 - id);
      assertThat(single).hasSize(3);
      for (int i = 0; i < single.size(); i += 1) {
        Object[] batchRow = batch.get(queryId * 3 + i);
        assertThat(batchRow[1]).isEqualTo(single.get(i)[0]);
        assertThat(batchRow[2]).isEqualTo(single.get(i)[1]);
        assertThat(batchRow[3]).isEqualTo(single.get(i)[2]);
        assertThat(batchRow[4]).isEqualTo(single.get(i)[3]);
      }
    }
  }

  @TestTemplate
  public void batchSearchAnswersRowsTheIndexDoesNotCover() {
    String tableName = tableName("vectors_batch_uncovered_" + catalogName);
    sql("CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg", tableName);
    sql(
        "INSERT INTO %s SELECT id, array(CAST(id AS double), CAST(10 - id AS double)) "
            + "FROM range(0, 5)",
        tableName);
    sql(
        "CALL %s.system.build_vector_index("
            + "table => '%s', column => 'embedding', dimension => 2, metric => 'l2')",
        catalogName, tableName);

    // rows written after the index was built are not covered by it; the table mode scans them and
    // has to prefer them over the covered rows when they are nearer
    sql("INSERT INTO %s VALUES (5, array(5.0d, 5.0d)), (6, array(6.0d, 4.0d))", tableName);

    List<Object[]> batch =
        sql(
            "CALL %s.system.vector_search_batch(table => '%s', column => 'embedding', "
                + "queries => array('5.0,5.0', '4.0,6.0'), k => 1)",
            catalogName, tableName);

    assertThat(batch).hasSize(2);
    assertThat(parseId((String) batch.get(0)[4])).isEqualTo(5L);
    assertThat(parseId((String) batch.get(1)[4])).isEqualTo(4L);
  }

  @TestTemplate
  public void batchSearchBlockSizeOverrideKeepsTheAnswer() {
    String tableName = tableName("vectors_batch_override_" + catalogName);
    sql("CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg", tableName);
    sql(
        "INSERT INTO %s SELECT id, array(CAST(id AS double), CAST(10 - id AS double)) "
            + "FROM range(0, 20)",
        tableName);
    sql(
        "CALL %s.system.build_vector_index("
            + "table => '%s', column => 'embedding', dimension => 2, metric => 'l2')",
        catalogName, tableName);

    String search =
        "CALL %s.system.vector_search_batch(table => '%s', column => 'embedding', "
            + "queries => array('0.0,10.0', '10.0,0.0', '5.0,5.0', '3.0,7.0'), k => 3)";
    List<Object[]> expected = sql(search, catalogName, tableName);

    // one block per two queries: the answer is the same as the one the default block size gives
    System.setProperty("iceberg.index.batch.queries-per-block", "2");
    try {
      List<Object[]> smallBlocks = sql(search, catalogName, tableName);
      assertThat(smallBlocks).hasSameSizeAs(expected);
      for (int i = 0; i < expected.size(); i += 1) {
        assertThat(java.util.Arrays.asList(smallBlocks.get(i)))
            .as("row %s", i)
            .isEqualTo(java.util.Arrays.asList(expected.get(i)));
      }

      System.setProperty("iceberg.index.batch.queries-per-block", "not-a-number");
      assertThatThrownBy(() -> sql(search, catalogName, tableName))
          .hasStackTraceContaining("Invalid iceberg.index.batch.queries-per-block");
    } finally {
      System.clearProperty("iceberg.index.batch.queries-per-block");
    }
  }

  @TestTemplate
  public void batchSearchUsesOneExecutionPerBlock() throws Exception {
    String tableName = tableName("vectors_batch_jobs_" + catalogName);
    sql("CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg", tableName);
    sql(
        "INSERT INTO %s SELECT id, array(CAST(id AS double), CAST(10 - id AS double)) "
            + "FROM range(0, 20)",
        tableName);
    sql(
        "CALL %s.system.build_vector_index("
            + "table => '%s', column => 'embedding', dimension => 2, metric => 'l2')",
        catalogName, tableName);

    String search =
        "CALL %s.system.vector_search_batch(table => '%s', column => 'embedding', "
            + "queries => array(%s), k => 3)";

    long oneBlock = countJobs(() -> sql(search, catalogName, tableName, queries(20)));
    long twoBlocks = countJobs(() -> sql(search, catalogName, tableName, queries(130)));

    // two blocks, not 130 queries: the jobs follow the blocks, they do not follow the queries
    assertThat(twoBlocks).as("jobs for 130 queries").isLessThan(130L);
    assertThat(twoBlocks)
        .as("jobs for one block against jobs for two")
        .isLessThanOrEqualTo(oneBlock + 12L);

    // one query per block costs more executions than one block for all of them
    System.setProperty("iceberg.index.batch.queries-per-block", "1");
    try {
      long oneQueryPerBlock = countJobs(() -> sql(search, catalogName, tableName, queries(20)));
      assertThat(oneQueryPerBlock)
          .as("a smaller block costs more executions")
          .isGreaterThan(oneBlock);
    } finally {
      System.clearProperty("iceberg.index.batch.queries-per-block");
    }
  }

  /** The queries of the batch tests, as the comma-separated form the procedure takes. */
  private static String queries(int count) {
    List<String> queries = new ArrayList<>();
    for (int i = 0; i < count; i += 1) {
      int id = i % 20;
      queries.add("'" + id + ".0," + (10 - id) + ".0'");
    }
    return String.join(",", queries);
  }

  /** Runs one action and counts the Spark jobs it started. */
  private long countJobs(Runnable action) throws Exception {
    String group = "vector-batch-jobs-" + System.nanoTime();
    spark.sparkContext().setJobGroup(group, "vector batch jobs", false);
    try {
      action.run();
      spark.sparkContext().listenerBus().waitUntilEmpty();
      return spark.sparkContext().statusTracker().getJobIdsForGroup(group).length;
    } finally {
      spark.sparkContext().clearJobGroup();
    }
  }

  @TestTemplate
  public void batchSearchAnswersAfterAColumnWasAdded() {
    String tableName = tableName("vectors_batch_schema_" + catalogName);
    sql("CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg", tableName);
    sql(
        "INSERT INTO %s SELECT id, array(CAST(id AS double), CAST(10 - id AS double)) "
            + "FROM range(0, 5)",
        tableName);
    sql(
        "CALL %s.system.build_vector_index("
            + "table => '%s', column => 'embedding', dimension => 2, metric => 'l2')",
        catalogName, tableName);

    // the index was built before the column existed, so the row JSON has to carry the column as a
    // null rather than to fail or to shift the columns it does have
    sql("ALTER TABLE %s ADD COLUMN label string", tableName);
    spark.conf().set("spark.sql.jsonGenerator.ignoreNullFields", "false");
    try {
      List<Object[]> batch =
          sql(
              "CALL %s.system.vector_search_batch(table => '%s', column => 'embedding', "
                  + "queries => array('5.0,5.0', '2.0,8.0'), k => 2)",
              catalogName, tableName);
      assertThat(batch).hasSize(4);
      for (Object[] row : batch) {
        assertThat((String) row[4]).contains("\"id\":").contains("\"label\":null");
      }
    } finally {
      spark.conf().unset("spark.sql.jsonGenerator.ignoreNullFields");
    }
  }
}
