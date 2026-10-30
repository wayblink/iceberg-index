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
import org.junit.jupiter.api.TestTemplate;

/**
 * Several indexes over one column, told apart by name.
 *
 * <p>Comparing two parameter sets used to mean two copies of the table; a name lets both indexes
 * live on the same data and lets a query say which one it means.
 */
public class TestNamedIndexes extends ExtensionsTestBase {

  @TestTemplate
  public void twoNamedIndexesOverOneColumnCoexist() {
    String table = tableName("named_indexes_" + catalogName);
    createIndexedTable(table);

    // a second, named index over the same column is allowed; a second unnamed one is not
    sql(
        "CALL %s.system.build_vector_index(table => '%s', column => 'embedding', dimension => 2, "
            + "name => 'flat_bruteforce', structure => 'FLAT')",
        catalogName, table);

    List<Object[]> indexes = sql("CALL %s.system.list_indexes(table => '%s')", catalogName, table);
    assertThat(indexes).hasSize(2);
    List<Object> names = indexes.stream().map(row -> row[8]).collect(Collectors.toList());
    assertThat(names).containsExactlyInAnyOrder(null, "flat_bruteforce");
  }

  @TestTemplate
  public void aQueryCanChooseBetweenNamedIndexes() {
    String table = tableName("named_search_" + catalogName);
    createIndexedTable(table);
    sql(
        "CALL %s.system.build_vector_index(table => '%s', column => 'embedding', dimension => 2, "
            + "name => 'flat_bruteforce', structure => 'FLAT')",
        catalogName, table);

    // both indexes answer the same query with the same rows, since they hold the same vectors
    List<Object[]> byName =
        sql(
            "CALL %s.system.vector_search(table => '%s', column => 'embedding', "
                + "query => array(0.0d, 10.0d), k => 3, index => 'flat_bruteforce')",
            catalogName, table);
    List<Object[]> byUuid =
        sql(
            "CALL %s.system.vector_search(table => '%s', column => 'embedding', "
                + "query => array(0.0d, 10.0d), k => 3, index => '%s')",
            catalogName, table, uuidOf(table, "flat_bruteforce"));

    assertThat(byName).hasSize(3);
    // an Object[] compares by identity, so the rows are compared as text
    assertThat(byName.stream().map(java.util.Arrays::toString).collect(Collectors.toList()))
        .isEqualTo(byUuid.stream().map(java.util.Arrays::toString).collect(Collectors.toList()));

    // naming an index that does not exist says what the column does have
    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.vector_search(table => '%s', column => 'embedding', "
                        + "query => array(0.0d, 10.0d), k => 3, index => 'nope')",
                    catalogName, table))
        .hasMessageContaining("flat_bruteforce");
  }

  @TestTemplate
  public void aQueryWithoutANameRefusesToPickOne() {
    String table = tableName("named_ambiguous_" + catalogName);
    createIndexedTable(table);
    sql(
        "CALL %s.system.build_vector_index(table => '%s', column => 'embedding', dimension => 2, "
            + "name => 'flat_bruteforce', structure => 'FLAT')",
        catalogName, table);

    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.vector_search(table => '%s', column => 'embedding', "
                        + "query => array(0.0d, 10.0d), k => 3)",
                    catalogName, table))
        .hasMessageContaining("several indexes exist")
        .hasMessageContaining("flat_bruteforce");
  }

  @TestTemplate
  public void droppingOneNamedIndexLeavesTheOtherSearchable() {
    String table = tableName("named_drop_" + catalogName);
    createIndexedTable(table);
    sql(
        "CALL %s.system.build_vector_index(table => '%s', column => 'embedding', dimension => 2, "
            + "name => 'flat_bruteforce', structure => 'FLAT')",
        catalogName, table);

    sql(
        "CALL %s.system.drop_vector_index(table => '%s', column => 'embedding', "
            + "index => 'flat_bruteforce')",
        catalogName, table);

    assertThat(sql("CALL %s.system.list_indexes(table => '%s')", catalogName, table)).hasSize(1);
    // with one index left, an unnamed query works again
    List<Object[]> hits =
        sql(
            "CALL %s.system.vector_search(table => '%s', column => 'embedding', "
                + "query => array(0.0d, 10.0d), k => 3)",
            catalogName, table);
    assertThat(hits).hasSize(3);
  }

  @TestTemplate
  public void refreshingOneNamedIndexLeavesTheOtherWhereItWas() {
    String table = tableName("named_refresh_" + catalogName);
    createIndexedTable(table);
    sql(
        "CALL %s.system.build_vector_index(table => '%s', column => 'embedding', dimension => 2, "
            + "name => 'flat_bruteforce', structure => 'FLAT')",
        catalogName, table);

    long namedBefore = snapshotOf(table, "flat_bruteforce");
    long unnamedBefore = snapshotOf(table, null);
    sql("INSERT INTO %s VALUES (100, array(100.0, 100.0))", table);

    // refresh only the named arm
    sql(
        "CALL %s.system.refresh_vector_index(table => '%s', column => 'embedding', "
            + "index => 'flat_bruteforce')",
        catalogName, table);

    // snapshot ids are random, not ordered: a refresh shows as a different id, not a larger one
    assertThat(snapshotOf(table, "flat_bruteforce")).isNotEqualTo(namedBefore);
    // the unnamed index was not touched: its source snapshot still points at the build
    assertThat(snapshotOf(table, null)).isEqualTo(unnamedBefore);
  }

  @TestTemplate
  public void anUnnamedRefreshRefusesToPickOne() {
    String table = tableName("named_refresh_ambiguous_" + catalogName);
    createIndexedTable(table);
    sql(
        "CALL %s.system.build_vector_index(table => '%s', column => 'embedding', dimension => 2, "
            + "name => 'flat_bruteforce', structure => 'FLAT')",
        catalogName, table);

    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.refresh_vector_index(table => '%s', column => 'embedding')",
                    catalogName, table))
        .hasMessageContaining("has 2 vector indexes");
  }

  @TestTemplate
  public void aSecondUnnamedIndexIsStillRefused() {
    String table = tableName("named_unnamed_" + catalogName);
    createIndexedTable(table);

    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.build_vector_index(table => '%s', column => 'embedding', "
                        + "dimension => 2)",
                    catalogName, table))
        .hasMessageContaining("already exists")
        .hasMessageContaining("name =>");
  }

  @TestTemplate
  public void aRewriteUsesTheIndexItsQueryNames() {
    String table = tableName("named_rewrite_" + catalogName);
    createIndexedTable(table);
    sql(
        "CALL %s.system.build_vector_index(table => '%s', column => 'embedding', dimension => 2, "
            + "name => 'flat_bruteforce', structure => 'FLAT')",
        catalogName, table);
    sql(
        "CALL %s.system.build_vector_index(table => '%s', column => 'embedding', dimension => 2, "
            + "name => 'ivf', structure => 'IVF', options => map('nprobe', '4'))",
        catalogName, table);

    String query = "SELECT id FROM %s ORDER BY vec_distance(embedding, array(0.0d, 10.0d)) LIMIT 3";
    String ivf = uuidOf(table, "ivf");
    String flat = uuidOf(table, "flat_bruteforce");
    try {
      // the ivf index is approximate, so it answers only once the caller has widened the candidates
      spark.conf().set("spark.iceberg.index.vector.over-sample", "10");
      // either arm answers when it is named, and the plan says which index was resolved
      spark.conf().set("spark.iceberg.index.vector.name", "ivf");
      assertThat(planOf(query, table)).contains("VectorSearchSource").contains("index=" + ivf);
      assertThat(sql(query, table)).hasSize(3);

      spark.conf().set("spark.iceberg.index.vector.name", "flat_bruteforce");
      assertThat(planOf(query, table)).contains("VectorSearchSource").contains("index=" + flat);

      // two indexes and no name is not a guess: the query scans, and still answers
      spark.conf().unset("spark.iceberg.index.vector.name");
      assertThat(planOf(query, table)).doesNotContain("VectorSearchSource");
      assertThat(sql(query, table)).hasSize(3);
    } finally {
      spark.conf().unset("spark.iceberg.index.vector.name");
      spark.conf().unset("spark.iceberg.index.vector.over-sample");
    }
  }

  private String planOf(String query, String table) {
    return spark.sql(String.format(query, table)).queryExecution().optimizedPlan().toString();
  }

  private void createIndexedTable(String table) {
    sql("CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg", table);
    sql(
        "INSERT INTO %s SELECT id, array(CAST(id AS double), CAST(10 - id AS double)) "
            + "FROM range(0, 20)",
        table);
    sql(
        "CALL %s.system.build_vector_index("
            + "table => '%s', column => 'embedding', dimension => 2, metric => 'euclidean')",
        catalogName, table);
  }

  /** The source table snapshot an index was last built from, found by name (null for unnamed). */
  private long snapshotOf(String table, String indexName) {
    for (Object[] row : sql("CALL %s.system.list_indexes(table => '%s')", catalogName, table)) {
      boolean matches = indexName == null ? row[8] == null : indexName.equals(row[8]);
      if (matches) {
        return ((Number) row[3]).longValue();
      }
    }

    throw new IllegalStateException("No index named " + indexName);
  }

  private String uuidOf(String table, String indexName) {
    for (Object[] row : sql("CALL %s.system.list_indexes(table => '%s')", catalogName, table)) {
      if (indexName.equals(row[8])) {
        return (String) row[0];
      }
    }

    throw new IllegalStateException("No index named " + indexName);
  }
}
