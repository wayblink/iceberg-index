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
import org.apache.iceberg.index.text.TextIndexTypes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * End-to-end tests of the BM25 text index through the SQL procedures: build, list, search and
 * refresh, including delete semantics.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class TestTextIndexProcedures extends ExtensionsTestBase {

  @BeforeAll
  static void registerTextIndexType() {
    TextIndexTypes.register();
  }

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName("text_build_" + catalogName));
    sql("DROP TABLE IF EXISTS %s", tableName("text_deletes_" + catalogName));
    sql("DROP TABLE IF EXISTS %s", tableName("text_refresh_" + catalogName));
    sql("DROP TABLE IF EXISTS %s", tableName("text_errors_" + catalogName));
  }

  @TestTemplate
  public void testBuildListAndSearchTextIndex() {
    String tableName = tableName("text_build_" + catalogName);
    sql("CREATE TABLE %s (id bigint, body string) USING iceberg", tableName);
    sql(
        "INSERT INTO %s VALUES "
            + "(1, 'iceberg table format for large datasets'), "
            + "(2, 'vector search over embeddings'), "
            + "(3, 'iceberg snapshot isolation and manifests'), "
            + "(4, 'parquet columnar storage engine'), "
            + "(5, 'iceberg vector index with hnsw graphs')",
        tableName);

    List<Object[]> buildOutput =
        sql(
            "CALL %s.system.build_text_index(table => '%s', column => 'body')",
            catalogName, tableName);
    assertThat(buildOutput).hasSize(1);
    String indexUuid = buildOutput.get(0)[0].toString();
    assertThat(((Number) buildOutput.get(0)[3]).longValue()).isEqualTo(5L);

    List<Object[]> listOutput =
        sql("CALL %s.system.list_indexes(table => '%s')", catalogName, tableName);
    assertThat(listOutput).hasSize(1);
    assertThat(listOutput.get(0)[0]).isEqualTo(indexUuid);
    assertThat(listOutput.get(0)[1]).isEqualTo("body");
    assertThat(listOutput.get(0)[2]).isEqualTo("text");
    assertThat(((Number) listOutput.get(0)[6]).longValue()).isEqualTo(5L);
    assertThat(listOutput.get(0)[7].toString()).contains("\"k1\"");

    // a query about iceberg tables must rank the iceberg-heavy documents first
    List<Object[]> searchOutput =
        sql(
            "CALL %s.system.text_search(table => '%s', column => 'body', "
                + "query => 'iceberg table', k => 2)",
            catalogName, tableName);
    assertThat(searchOutput).hasSize(2);
    assertThat(searchIds(searchOutput)).containsExactly(1L, 3L);

    // scores descend and the resolved rows carry the matched documents
    double first = ((Number) searchOutput.get(0)[2]).doubleValue();
    double second = ((Number) searchOutput.get(1)[2]).doubleValue();
    assertThat(first).isGreaterThan(second);
    assertThat(searchOutput.get(0)[3].toString()).contains("iceberg table format");
  }

  @TestTemplate
  public void testTextSearchRespectsDeletesAndRefresh() {
    String tableName = tableName("text_deletes_" + catalogName);
    sql("CREATE TABLE %s (id bigint, body string) USING iceberg", tableName);
    sql(
        "INSERT INTO %s VALUES "
            + "(1, 'iceberg table format'), "
            + "(2, 'vector search embeddings'), "
            + "(3, 'iceberg snapshot manifests')",
        tableName);
    sql("CALL %s.system.build_text_index(table => '%s', column => 'body')", catalogName, tableName);

    // delete one of the iceberg documents: it must disappear from the index after a refresh
    sql("DELETE FROM %s WHERE id = 1", tableName);
    sql(
        "CALL %s.system.refresh_text_index(table => '%s', column => 'body')",
        catalogName, tableName);

    List<Object[]> searchOutput =
        sql(
            "CALL %s.system.text_search(table => '%s', column => 'body', "
                + "query => 'iceberg', k => 5)",
            catalogName, tableName);
    assertThat(searchIds(searchOutput)).containsExactly(3L);
    assertThat(((Number) searchOutput.get(0)[2]).doubleValue()).isPositive();
  }

  @TestTemplate
  public void testRefreshTextIndexPicksUpNewDocuments() {
    String tableName = tableName("text_refresh_" + catalogName);
    sql("CREATE TABLE %s (id bigint, body string) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, 'iceberg table format')", tableName);
    sql("CALL %s.system.build_text_index(table => '%s', column => 'body')", catalogName, tableName);

    // refreshing without changes reports zero files and entries
    List<Object[]> noopOutput =
        sql(
            "CALL %s.system.refresh_text_index(table => '%s', column => 'body')",
            catalogName, tableName);
    assertThat(((Number) noopOutput.get(0)[2]).longValue()).isZero();
    assertThat(((Number) noopOutput.get(0)[3]).longValue()).isZero();

    sql("INSERT INTO %s VALUES (2, 'iceberg vector index')", tableName);
    sql(
        "CALL %s.system.refresh_text_index(table => '%s', column => 'body')",
        catalogName, tableName);

    List<Object[]> listOutput =
        sql("CALL %s.system.list_indexes(table => '%s')", catalogName, tableName);
    assertThat(((Number) listOutput.get(0)[6]).longValue()).isEqualTo(2L);

    // the new document is searchable
    List<Object[]> searchOutput =
        sql(
            "CALL %s.system.text_search(table => '%s', column => 'body', "
                + "query => 'vector index', k => 1)",
            catalogName, tableName);
    assertThat(searchIds(searchOutput)).containsExactly(2L);
  }

  @TestTemplate
  public void testTextIndexErrorPaths() {
    String tableName = tableName("text_errors_" + catalogName);
    sql("CREATE TABLE %s (id bigint, body string) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, 'iceberg table format')", tableName);

    // searching without an index fails
    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.text_search(table => '%s', column => 'body', "
                        + "query => 'iceberg', k => 1)",
                    catalogName, tableName))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("No text index exists");

    sql("CALL %s.system.build_text_index(table => '%s', column => 'body')", catalogName, tableName);

    // building twice fails
    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.build_text_index(table => '%s', column => 'body')",
                    catalogName, tableName))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("already exists");

    // an unknown query token matches nothing
    List<Object[]> searchOutput =
        sql(
            "CALL %s.system.text_search(table => '%s', column => 'body', "
                + "query => 'zzzznotpresent', k => 5)",
            catalogName, tableName);
    assertThat(searchOutput).isEmpty();
  }

  private static List<Long> searchIds(List<Object[]> output) {
    return output.stream().map(row -> parseId((String) row[3])).collect(Collectors.toList());
  }

  private static long parseId(String rowJson) {
    int start = rowJson.indexOf("\"id\":") + 5;
    int end = rowJson.indexOf(',', start);
    return Long.parseLong(rowJson.substring(start, end == -1 ? rowJson.length() - 1 : end));
  }
}
