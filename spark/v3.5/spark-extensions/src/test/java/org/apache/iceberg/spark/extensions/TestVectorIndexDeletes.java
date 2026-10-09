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
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Verifies the delete-semantics contract of vector indexes: an index snapshot is equivalent to the
 * table snapshot it is bound to, so rows deleted by position delete files must not be indexed, and
 * a refresh must rebuild the entries of data files whose delete files changed.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class TestVectorIndexDeletes extends ExtensionsTestBase {

  @BeforeAll
  static void registerVectorIndexType() {
    VectorIndexTypes.register();
  }

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName("vectors_deleted_rows_" + catalogName));
    sql("DROP TABLE IF EXISTS %s", tableName("vectors_new_deletes_" + catalogName));
    sql("DROP TABLE IF EXISTS %s", tableName("vectors_new_data_" + catalogName));
    sql("DROP TABLE IF EXISTS %s", tableName("vectors_rollback_" + catalogName));
  }

  @TestTemplate
  public void testDeletedRowsAreNotIndexed() {
    String tableName = tableName("vectors_deleted_rows_" + catalogName);
    sql("CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg", tableName);
    sql(
        "INSERT INTO %s SELECT id, array(CAST(id AS double), CAST(10 - id AS double)) "
            + "FROM range(0, 10)",
        tableName);

    // merge-on-read deletes: position delete files are added, data files stay
    sql("DELETE FROM %s WHERE id IN (3, 7)", tableName);

    sql(
        "CALL %s.system.build_vector_index("
            + "table => '%s', column => 'embedding', dimension => 2, metric => 'euclidean')",
        catalogName, tableName);

    // only the 8 live rows are indexed
    assertThat(indexEntries(tableName)).isEqualTo(8L);

    // the query close to (0, 10) ranks rows by ascending distance: 0, 1, 2, 4, 5, 6, 8, 9
    assertThat(searchIds(tableName, 10)).containsExactly(0L, 1L, 2L, 4L, 5L, 6L, 8L, 9L);

    // the indexed positions must be the raw row ordinals of the surviving rows
    assertPositionsMatchRawRowOrdinals(tableName);
  }

  @TestTemplate
  public void testRefreshPicksUpNewDeleteFiles() {
    String tableName = tableName("vectors_new_deletes_" + catalogName);
    sql("CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg", tableName);
    sql(
        "INSERT INTO %s SELECT id, array(CAST(id AS double), CAST(10 - id AS double)) "
            + "FROM range(0, 10)",
        tableName);
    sql("DELETE FROM %s WHERE id IN (3, 7)", tableName);
    sql(
        "CALL %s.system.build_vector_index("
            + "table => '%s', column => 'embedding', dimension => 2, metric => 'euclidean')",
        catalogName, tableName);
    assertThat(indexEntries(tableName)).isEqualTo(8L);

    // only a delete file is added; no data file changes, so the delete-set diff must trigger the
    // rebuild of the affected data file
    sql("DELETE FROM %s WHERE id = 5", tableName);
    sql(
        "CALL %s.system.refresh_vector_index(table => '%s', column => 'embedding')",
        catalogName, tableName);

    assertThat(indexEntries(tableName)).isEqualTo(7L);
    assertThat(searchIds(tableName, 10)).containsExactly(0L, 1L, 2L, 4L, 6L, 8L, 9L);
  }

  @TestTemplate
  public void testRefreshPicksUpNewDataAndDeletes() {
    String tableName = tableName("vectors_new_data_" + catalogName);
    sql("CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg", tableName);
    sql(
        "INSERT INTO %s SELECT id, array(CAST(id AS double), CAST(10 - id AS double)) "
            + "FROM range(0, 10)",
        tableName);
    sql("DELETE FROM %s WHERE id IN (3, 7)", tableName);
    sql(
        "CALL %s.system.build_vector_index("
            + "table => '%s', column => 'embedding', dimension => 2, metric => 'euclidean')",
        catalogName, tableName);

    // new data files and a new delete file at the same time
    sql(
        "INSERT INTO %s SELECT id, array(CAST(id AS double), CAST(10 - id AS double)) "
            + "FROM range(10, 15)",
        tableName);
    sql("DELETE FROM %s WHERE id = 11", tableName);
    sql(
        "CALL %s.system.refresh_vector_index(table => '%s', column => 'embedding')",
        catalogName, tableName);

    assertThat(indexEntries(tableName)).isEqualTo(12L);
    assertThat(searchIds(tableName, 12))
        .containsExactly(0L, 1L, 2L, 4L, 5L, 6L, 8L, 9L, 10L, 12L, 13L, 14L);
  }

  @TestTemplate
  public void testRefreshRejectsSourceSnapshotOutsideLineage() {
    String tableName = tableName("vectors_rollback_" + catalogName);
    sql("CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg", tableName);
    sql("INSERT INTO %s SELECT id, array(CAST(id AS double), 0.0) FROM range(0, 10)", tableName);
    sql(
        "CALL %s.system.build_vector_index("
            + "table => '%s', column => 'embedding', dimension => 2, metric => 'euclidean')",
        catalogName, tableName);

    // a newer table snapshot, then the index refreshed onto it
    sql("INSERT INTO %s SELECT id, array(CAST(id AS double), 0.0) FROM range(10, 20)", tableName);
    sql(
        "CALL %s.system.refresh_vector_index(table => '%s', column => 'embedding')",
        catalogName, tableName);
    assertThat(indexEntries(tableName)).isEqualTo(20L);

    // roll the table back to the first snapshot: refreshing from it would silently drop the newer
    // entries, so it must be rejected
    long firstSnapshotId =
        spark
            .sql(
                "SELECT snapshot_id FROM "
                    + tableName
                    + ".snapshots ORDER BY committed_at ASC LIMIT 1")
            .collectAsList()
            .get(0)
            .getLong(0);
    sql("CALL %s.system.rollback_to_snapshot('%s', %d)", catalogName, tableName, firstSnapshotId);

    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.refresh_vector_index(table => '%s', column => 'embedding')",
                    catalogName, tableName))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("not a descendant");

    // the index still covers the snapshot it was built from and stays searchable
    assertThat(indexEntries(tableName)).isEqualTo(20L);
    assertThat(searchIds(tableName, 2)).containsExactly(0L, 1L);
  }

  /** Runs a search and returns the ids parsed from the resolved row JSON, in result order. */
  private List<Long> searchIds(String tableName, int topK) {
    return spark
        .sql(
            "CALL "
                + catalogName
                + ".system.vector_search(table => '"
                + tableName
                + "', column => 'embedding', query => array(0.0d, 10.0d), k => "
                + topK
                + ")")
        .collectAsList()
        .stream()
        .map(row -> parseId(row.getString(3)))
        .collect(Collectors.toList());
  }

  private long indexEntries(String tableName) {
    List<Row> indexes =
        spark
            .sql("CALL " + catalogName + ".system.list_indexes(table => '" + tableName + "')")
            .collectAsList();
    assertThat(indexes).hasSize(1);
    return ((Number) indexes.get(0).get(6)).longValue();
  }

  /**
   * The positions recorded by the index are row ordinals of the data file, which is what the row
   * resolution of {@code vector_search} relies on: for every hit, the raw parquet row at that
   * ordinal must be the row returned in the result.
   */
  private void assertPositionsMatchRawRowOrdinals(String tableName) {
    List<Row> hits =
        spark
            .sql(
                "CALL "
                    + catalogName
                    + ".system.vector_search(table => '"
                    + tableName
                    + "', column => 'embedding', query => array(0.0d, 10.0d), k => 3)")
            .collectAsList();

    for (Row hit : hits) {
      String dataFile = hit.getString(0);
      long position = hit.getLong(1);
      long resolvedId = parseId(hit.getString(3));

      List<Row> rawRows =
          spark
              .read()
              .parquet(dataFile)
              .select("id")
              .toJavaRDD()
              .zipWithIndex()
              .filter(tuple -> tuple._2() == position)
              .map(tuple -> tuple._1())
              .collect();
      assertThat(rawRows).hasSize(1);
      assertThat(rawRows.get(0).getLong(0)).isEqualTo(resolvedId);
    }
  }

  private static long parseId(String rowJson) {
    int start = rowJson.indexOf("\"id\":") + 5;
    int end = rowJson.indexOf(',', start);
    return Long.parseLong(rowJson.substring(start, end == -1 ? rowJson.length() - 1 : end));
  }
}
