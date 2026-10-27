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

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.index.vector.VectorIndexTypes;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A row without a vector is not a row a vector query can answer.
 *
 * <p>An index stores a vector per entry, so a null vector has nothing to store, and a nearest
 * neighbour query is about the rows that have a vector at all. Such a row is therefore skipped when
 * the index is built, and both branches of a query agree on what a candidate is: the index returns
 * the rows it holds, and a scan of the files the index does not cover drops the rows a vector query
 * could not have matched. Skipping one row must not cost the rest of its data file, and a malformed
 * vector is still an error, not a row to leave out.
 */
class TestVectorIndexNullVectors {
  private static SparkSession spark;

  @BeforeAll
  static void startSpark(@TempDir Path warehouse) {
    VectorIndexTypes.register();
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("vector-null-vectors")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", warehouse.toUri().toString())
            .getOrCreate();
  }

  @AfterAll
  static void stopSpark() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  void buildsAnIndexOverATableThatHoldsANullVector() {
    String table = tableWithNullVector("null_build");

    // the build finishes, and the row without a vector is not an entry: three rows have one
    assertThat(entries(table)).isEqualTo(3L);
    assertThat(indexFiles(table)).isPositive();
  }

  @Test
  void aQueryReturnsOnlyTheRowsThatHaveAVector() {
    String table = tableWithNullVector("null_query");

    // the scan of the same table without an index returns the null row first, because a null
    // distance sorts first; a vector query is about the rows it could have matched, so the null row
    // is not one of them
    List<Long> ids = nearest(table, 10);
    assertThat(ids).containsExactlyInAnyOrder(1L, 2L, 3L);
    assertThat(nearest(table, 2)).hasSize(2);
  }

  @Test
  void theRowsThatHaveAVectorAreStillAnsweredFromTheIndex() {
    String table = tableWithNullVector("null_indexed");

    // the null row does not make its data file unindexed: the other rows of that same file are
    // found by the index, which is what the plan shows
    assertThat(spark.sql(nearestSql(table, 2)).queryExecution().optimizedPlan().toString())
        .contains("VectorSearchSource");

    assertThat(nearest(table, 3)).containsExactly(1L, 2L, 3L);
  }

  @Test
  void aNullAppendedAfterTheBuildIsNotAQueryResultEither() {
    String table = tableWithNullVector("null_appended");
    spark.sql("INSERT INTO local.db." + table + " VALUES (4, null)");

    // the appended row sits in a file the index does not cover, and the scan of that file answers
    // the same question as the index does: no vector, no candidate
    assertThat(nearest(table, 10)).containsExactlyInAnyOrder(1L, 2L, 3L);
  }

  @Test
  void aQueryThatRulesNullOutItselfAgreesWithAScan() {
    String table = tableWithNullVector("null_guarded");
    String guarded =
        "SELECT id FROM local.db."
            + table
            + " WHERE embedding IS NOT NULL "
            + "ORDER BY vec_distance(embedding, array(0.0d, 0.0d)) LIMIT 3";

    assertThat(
            spark.sql(guarded).collectAsList().stream()
                .map(row -> row.getLong(0))
                .collect(Collectors.toList()))
        .containsExactly(1L, 2L, 3L);
  }

  @Test
  void aVectorOfTheWrongDimensionIsStillAnError() {
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
    spark.sql(
        "CREATE TABLE local.db.null_wrong_dimension (id bigint, embedding array<double>) "
            + "USING iceberg");
    spark.sql("INSERT INTO local.db.null_wrong_dimension VALUES (1, array(1.0d, 0.0d)), (2, null)");

    // a row with no vector is a row to leave out; a row whose vector is the wrong shape is a
    // definition that does not match the data, and saying so is the whole point of the build
    assertThatThrownBy(
            () ->
                spark.sql(
                    "CALL local.system.build_vector_index(table => 'db.null_wrong_dimension', "
                        + "column => 'embedding', dimension => 3)"))
        .hasMessageContaining("dimension");
  }

  private static String nearestSql(String table, int limit) {
    return "SELECT id FROM local.db."
        + table
        + " ORDER BY vec_distance(embedding, array(0.0d, 0.0d)) LIMIT "
        + limit;
  }

  private static List<Long> nearest(String table, int limit) {
    return spark.sql(nearestSql(table, limit)).collectAsList().stream()
        .map(row -> row.getLong(0))
        .collect(Collectors.toList());
  }

  private static long entries(String table) {
    Row index = spark.sql("CALL local.system.list_indexes(table => 'db." + table + "')").first();
    return index.getLong(6);
  }

  private static long indexFiles(String table) {
    Row index = spark.sql("CALL local.system.list_indexes(table => 'db." + table + "')").first();
    return index.getLong(5);
  }

  /** Three rows with a vector and one without, in one data file. */
  private static String tableWithNullVector(String name) {
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
    spark.sql(
        "CREATE TABLE local.db." + name + " (id bigint, embedding array<double>) USING iceberg");
    spark.sql(
        "INSERT INTO local.db."
            + name
            + " VALUES (1, array(1.0d, 0.0d)), (2, array(0.0d, 2.0d)), "
            + "(3, array(3.0d, 3.0d)), (4, null)");
    spark.sql(
        "CALL local.system.build_vector_index(table => 'db."
            + name
            + "', column => 'embedding', dimension => 2, metric => 'euclidean', "
            + "structure => 'flat')");
    return name;
  }
}
