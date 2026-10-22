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

import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.TestTemplate;

/**
 * The cache and the refresh path together.
 *
 * <p>The cache keeps the payload of an index file, and a refresh publishes different index files.
 * The two are only safe together if a refresh is visible immediately and the entries of the old
 * files cannot answer for the new ones: an index that is one snapshot behind is a correctness
 * problem that looks like a cache problem.
 */
public class TestIndexCacheAndRefresh extends ExtensionsTestBase {

  private static final String CACHE_ENABLED = "spark.iceberg.index.cache.enabled";

  @TestTemplate
  public void aRefreshIsVisibleToTheNextQuery() {
    String table = tableName("cache_refresh_" + catalogName);
    sql("CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg", table);
    sql("INSERT INTO %s VALUES (1, array(1.0, 0.0))", table);
    sql(
        "CALL %s.system.build_vector_index(table => '%s', column => 'embedding', dimension => 2)",
        catalogName, table);

    spark.conf().set(CACHE_ENABLED, "true");
    try {
      // warm the cache on the index as it is
      assertThat(searchIds(table, "1.0d, 0.0d")).contains(1L).doesNotContain(2L);

      // new data, then a refresh: the next query has to answer from the new index, not from what
      // the executor remembered about the old one
      sql("INSERT INTO %s VALUES (2, array(1.0, 0.1))", table);
      sql(
          "CALL %s.system.refresh_vector_index(table => '%s', column => 'embedding')",
          catalogName, table);

      List<Long> ids = searchIds(table, "1.0d, 0.0d");
      assertThat(ids)
          .as("the refreshed index sees the appended row")
          .containsExactlyInAnyOrder(1L, 2L);
    } finally {
      spark.conf().unset(CACHE_ENABLED);
    }
  }

  @TestTemplate
  public void aRefreshedIndexDoesNotAnswerWithStaleRows() {
    String table = tableName("cache_stale_" + catalogName);
    sql("CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg", table);
    sql("INSERT INTO %s VALUES (1, array(1.0, 0.0)), (2, array(0.0, 1.0))", table);
    sql(
        "CALL %s.system.build_vector_index(table => '%s', column => 'embedding', dimension => 2)",
        catalogName, table);

    spark.conf().set(CACHE_ENABLED, "true");
    try {
      assertThat(searchIds(table, "1.0d, 0.0d")).contains(1L, 2L);

      // delete the row the query used to return, then refresh
      sql("DELETE FROM %s WHERE id = 1", table);
      sql(
          "CALL %s.system.refresh_vector_index(table => '%s', column => 'embedding')",
          catalogName, table);

      assertThat(searchIds(table, "1.0d, 0.0d"))
          .as("a row deleted before the refresh cannot come back from a cached payload")
          .doesNotContain(1L)
          .containsExactly(2L);
    } finally {
      spark.conf().unset(CACHE_ENABLED);
    }
  }

  @TestTemplate
  public void theStatusShowsTheNewIndexFiles() {
    String table = tableName("cache_status_refresh_" + catalogName);
    sql("CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg", table);
    sql("INSERT INTO %s VALUES (1, array(1.0, 0.0))", table);
    sql(
        "CALL %s.system.build_vector_index(table => '%s', column => 'embedding', dimension => 2)",
        catalogName, table);

    spark.conf().set(CACHE_ENABLED, "true");
    try {
      searchIds(table, "1.0d, 0.0d");
      // in a local session the only executor is the driver, so every row is an executor row
      List<Object[]> before = sql("CALL %s.system.index_cache_status()", catalogName);
      long entriesBefore = before.stream().mapToLong(row -> ((Number) row[4]).longValue()).sum();

      sql("INSERT INTO %s VALUES (2, array(0.0, 1.0))", table);
      sql(
          "CALL %s.system.refresh_vector_index(table => '%s', column => 'embedding')",
          catalogName, table);
      searchIds(table, "1.0d, 0.0d");

      List<Object[]> after = sql("CALL %s.system.index_cache_status()", catalogName);
      long entriesAfter = after.stream().mapToLong(row -> ((Number) row[4]).longValue()).sum();

      // the refresh publishes new files, so the executor holds more of them than before; the old
      // ones stay until they age out, which is why this is "at least" rather than "exactly one
      // more"
      assertThat(entriesAfter)
          .as("the new index file is cached after the refresh")
          .isGreaterThan(entriesBefore);
    } finally {
      spark.conf().unset(CACHE_ENABLED);
    }
  }

  private List<Long> searchIds(String table, String query) {
    return sql(
            "CALL %s.system.vector_search(table => '%s', column => 'embedding', query => array(%s), k => 5)",
            catalogName, table, query)
        .stream()
        .map(row -> Long.parseLong(((String) row[3]).replaceAll(".*\"id\":(\\d+).*", "$1")))
        .collect(Collectors.toList());
  }
}
