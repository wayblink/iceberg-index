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
 * Tests that one table can carry several indexes of several types at the same time: two vector
 * columns each with their own vector index, plus a text column with a BM25 index. Every index has
 * its own uuid, its own snapshot chain and its own files, and each procedure addresses an index by
 * its column (and type), so the indexes are independent of each other.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class TestMultipleIndexesPerTable extends ExtensionsTestBase {

  @BeforeAll
  static void registerTextIndexType() {
    TextIndexTypes.register();
  }

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName("multi_index_coexist_" + catalogName));
    sql("DROP TABLE IF EXISTS %s", tableName("multi_index_dup_" + catalogName));
    sql("DROP TABLE IF EXISTS %s", tableName("multi_index_group_" + catalogName));
    sql("DROP TABLE IF EXISTS %s", tableName("multi_index_group2_" + catalogName));
    sql("DROP TABLE IF EXISTS %s", tableName("multi_index_group3_" + catalogName));
    sql("DROP TABLE IF EXISTS %s", tableName("multi_index_group_refresh_" + catalogName));
  }

  @TestTemplate
  public void testVectorAndTextIndexesCoexist() {
    String tableName = tableName("multi_index_coexist_" + catalogName);
    sql(
        "CREATE TABLE %s (id bigint, title string, "
            + "title_vec array<double>, body string, body_vec array<double>) USING iceberg",
        tableName);
    sql(
        "INSERT INTO %s VALUES "
            + "(1, 'iceberg', array(1.0, 0.0), 'iceberg table format', array(1.0, 0.0)), "
            + "(2, 'spark', array(0.0, 1.0), 'spark sql engine', array(0.0, 1.0)), "
            + "(3, 'trino', array(0.7, 0.7), 'trino query engine', array(0.7, 0.7))",
        tableName);

    sql(
        "CALL %s.system.build_vector_index(table => '%s', column => 'title_vec', dimension => 2, mode => 'HNSW')",
        catalogName, tableName);
    sql(
        "CALL %s.system.build_vector_index(table => '%s', column => 'body_vec', dimension => 2, mode => 'EXACT')",
        catalogName, tableName);
    sql("CALL %s.system.build_text_index(table => '%s', column => 'body')", catalogName, tableName);

    // all three indexes are visible, one row per index, each on its own column
    List<Object[]> listOutput =
        sql("CALL %s.system.list_indexes(table => '%s')", catalogName, tableName);
    assertThat(listOutput).hasSize(3);
    assertThat(listOutput.stream().map(row -> row[1]).collect(Collectors.toList()))
        .containsExactlyInAnyOrder("title_vec", "body_vec", "body");
    assertThat(listOutput.stream().map(row -> row[2]).collect(Collectors.toList()))
        .containsExactlyInAnyOrder("vector", "vector", "text");
    // index uuids are distinct: each index is a separate object with its own snapshot chain
    assertThat(listOutput.stream().map(row -> row[0].toString()).distinct()).hasSize(3);
    // all three were built from the same table snapshot, so they share that source snapshot id
    assertThat(listOutput.stream().map(row -> row[3].toString()).distinct()).hasSize(1);
    java.util.Map<String, Long> sourceSnapshots = new java.util.HashMap<>();
    for (Object[] row : listOutput) {
      sourceSnapshots.put(row[1].toString(), ((Number) row[3]).longValue());
    }

    // searching one index does not see the others: each vector column has its own index
    List<Object[]> titleHits =
        sql(
            "CALL %s.system.vector_search(table => '%s', column => 'title_vec', "
                + "query => array(1.0d, 0.0d), k => 1)",
            catalogName, tableName);
    assertThat(hitIds(titleHits)).containsExactly(1L);

    List<Object[]> bodyHits =
        sql(
            "CALL %s.system.vector_search(table => '%s', column => 'body_vec', "
                + "query => array(0.0d, 1.0d), k => 1)",
            catalogName, tableName);
    assertThat(hitIds(bodyHits)).containsExactly(2L);

    // the text index on another column works next to the two vector indexes
    List<Object[]> textHits =
        sql(
            "CALL %s.system.text_search(table => '%s', column => 'body', "
                + "query => 'iceberg', k => 1)",
            catalogName, tableName);
    assertThat(hitIds(textHits)).containsExactly(1L);

    // refreshing one index leaves the others untouched: new data reaches only the refreshed one
    sql(
        "INSERT INTO %s VALUES (4, 'flink', array(1.0, 1.0), 'flink streaming', array(1.0, 1.0))",
        tableName);
    sql(
        "CALL %s.system.refresh_text_index(table => '%s', column => 'body')",
        catalogName, tableName);

    List<Object[]> afterRefresh =
        sql("CALL %s.system.list_indexes(table => '%s')", catalogName, tableName);
    assertThat(afterRefresh).hasSize(3);
    for (Object[] row : afterRefresh) {
      String column = row[1].toString();
      long sourceSnapshotId = ((Number) row[3]).longValue();
      if ("body".equals(column)) {
        assertThat(sourceSnapshotId).isNotEqualTo(sourceSnapshots.get(column));
      } else {
        assertThat(sourceSnapshotId).isEqualTo(sourceSnapshots.get(column));
      }
    }

    List<Object[]> textHitsAfterRefresh =
        sql(
            "CALL %s.system.text_search(table => '%s', column => 'body', "
                + "query => 'flink', k => 1)",
            catalogName, tableName);
    assertThat(hitIds(textHitsAfterRefresh)).containsExactly(4L);

    // an index on a different column is not affected by the new row
    List<Object[]> titleHitsAfterRefresh =
        sql(
            "CALL %s.system.vector_search(table => '%s', column => 'title_vec', "
                + "query => array(1.0d, 1.0d), k => 1)",
            catalogName, tableName);
    assertThat(hitIds(titleHitsAfterRefresh)).containsExactly(3L);
  }

  @TestTemplate
  public void testGroupsDataFilesIntoOneIndexFile() {
    // one row per statement, so each statement writes exactly one data file
    String tableName = tableName("multi_index_group_" + catalogName);
    sql("CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg", tableName);
    for (int id = 0; id < 4; id++) {
      sql("INSERT INTO %s VALUES (%d, array(%d.0d, 1.0d))", tableName, id, id);
    }

    // without a budget every data file gets an index file of its own
    sql(
        "CALL %s.system.build_vector_index(table => '%s', column => 'embedding', dimension => 2)",
        catalogName, tableName);
    List<Object[]> listOutput =
        sql("CALL %s.system.list_indexes(table => '%s')", catalogName, tableName);
    assertThat(((Number) listOutput.get(0)[5]).longValue()).isEqualTo(4L);
    assertThat(((Number) listOutput.get(0)[6]).longValue()).isEqualTo(4L);

    // a budget of two entries packs two data files into one index file
    String grouped = tableName("multi_index_group2_" + catalogName);
    sql("CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg", grouped);
    for (int id = 0; id < 4; id++) {
      sql("INSERT INTO %s VALUES (%d, array(%d.0d, 1.0d))", grouped, id, id);
    }
    sql(
        "CALL %s.system.build_vector_index(table => '%s', column => 'embedding', dimension => 2, "
            + "entries_per_index_file => 2)",
        catalogName, grouped);

    List<Object[]> groupedOutput =
        sql("CALL %s.system.list_indexes(table => '%s')", catalogName, grouped);
    assertThat(((Number) groupedOutput.get(0)[5]).longValue()).isEqualTo(2L);
    assertThat(((Number) groupedOutput.get(0)[6]).longValue()).isEqualTo(4L);

    // a budget of one keeps one index file per data file
    String single = tableName("multi_index_group3_" + catalogName);
    sql("CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg", single);
    for (int id = 0; id < 4; id++) {
      sql("INSERT INTO %s VALUES (%d, array(%d.0d, 1.0d))", single, id, id);
    }
    sql(
        "CALL %s.system.build_vector_index(table => '%s', column => 'embedding', dimension => 2, "
            + "entries_per_index_file => 1)",
        catalogName, single);
    List<Object[]> singleOutput =
        sql("CALL %s.system.list_indexes(table => '%s')", catalogName, single);
    assertThat(((Number) singleOutput.get(0)[5]).longValue()).isEqualTo(4L);

    // the grouped index answers queries the same way: every row is still reachable
    List<Object[]> hits =
        sql(
            "CALL %s.system.vector_search(table => '%s', column => 'embedding', "
                + "query => array(2.0d, 1.0d), k => 1)",
            catalogName, grouped);
    assertThat(hitIds(hits)).containsExactly(2L);
  }

  @TestTemplate
  public void testRefreshRewritesTheWholeGroup() {
    String tableName = tableName("multi_index_group_refresh_" + catalogName);
    sql("CREATE TABLE %s (id bigint, body string) USING iceberg", tableName);
    // one row per statement, so each statement writes exactly one data file
    for (int id = 0; id < 4; id++) {
      sql("INSERT INTO %s VALUES (%d, 'iceberg document word%d')", tableName, id, id);
    }
    // two data files per index file
    sql(
        "CALL %s.system.build_text_index(table => '%s', column => 'body', "
            + "entries_per_index_file => 2)",
        catalogName, tableName);

    List<Object[]> before =
        sql("CALL %s.system.list_indexes(table => '%s')", catalogName, tableName);
    assertThat(((Number) before.get(0)[5]).longValue()).isEqualTo(2L);
    assertThat(((Number) before.get(0)[6]).longValue()).isEqualTo(4L);

    // deleting a row rewrites its data file, and with it the whole index file that covers the
    // group that data file belongs to
    sql("DELETE FROM %s WHERE id = 1", tableName);
    sql(
        "CALL %s.system.refresh_text_index(table => '%s', column => 'body')",
        catalogName, tableName);

    List<Object[]> after =
        sql("CALL %s.system.list_indexes(table => '%s')", catalogName, tableName);
    // the groups are repacked, because the rewritten data file sorts differently: the file count
    // may move, but every remaining row is still indexed and no group is left half empty
    long indexFiles = ((Number) after.get(0)[5]).longValue();
    assertThat(indexFiles).isBetween(1L, 2L);
    assertThat(((Number) after.get(0)[6]).longValue()).isEqualTo(3L);

    // the deleted row is gone from the index and the others are still found
    List<Object[]> deleted =
        sql(
            "CALL %s.system.text_search(table => '%s', column => 'body', "
                + "query => 'word1', k => 5)",
            catalogName, tableName);
    assertThat(hitIds(deleted)).isEmpty();

    List<Object[]> kept =
        sql(
            "CALL %s.system.text_search(table => '%s', column => 'body', "
                + "query => 'word3', k => 1)",
            catalogName, tableName);
    assertThat(hitIds(kept)).containsExactly(3L);
  }

  @TestTemplate
  public void testSecondIndexOnSameColumnIsRejected() {
    String tableName = tableName("multi_index_dup_" + catalogName);
    sql("CREATE TABLE %s (id bigint, vec array<double>) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, array(1.0, 0.0))", tableName);

    sql(
        "CALL %s.system.build_vector_index(table => '%s', column => 'vec', dimension => 2)",
        catalogName, tableName);

    // one index per column: a second index on the same column is rejected, whatever its type or
    // parameters, so that search and refresh stay unambiguous
    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.build_vector_index(table => '%s', column => 'vec', dimension => 2)",
                    catalogName, tableName))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("already exists");

    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.build_vector_index(table => '%s', column => 'vec', "
                        + "dimension => 2, mode => 'HNSW', metric => 'EUCLIDEAN')",
                    catalogName, tableName))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("already exists");
  }

  private static List<Long> hitIds(List<Object[]> output) {
    return output.stream().map(row -> parseId((String) row[3])).collect(Collectors.toList());
  }

  private static long parseId(String rowJson) {
    int start = rowJson.indexOf("\"id\":") + 5;
    int end = rowJson.indexOf(',', start);
    return Long.parseLong(rowJson.substring(start, end == -1 ? rowJson.length() - 1 : end));
  }
}
