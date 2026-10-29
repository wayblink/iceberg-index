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
import static org.assertj.core.api.Assertions.within;

import java.nio.file.Path;
import java.util.List;
import org.apache.iceberg.index.vector.VectorIndexTypes;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests the vector_search table function: the nearest rows of a table as the table's own columns
 * plus the distance that chose them.
 *
 * <p>The function builds an ordinary projection over a sort over a limit, so these tests also hold
 * the rewrite to its contract: the same rows as an ORDER BY query, from the index when one matches
 * and from a scan when one does not.
 */
class TestVectorSearchTableFunction {

  private static final String COLUMNS =
      "(id bigint, body string, embedding array<double>) USING iceberg";
  private static final String VECTORS =
      "(1, 'alpha', array(1.0d, 0.0d)), "
          + "(2, 'bravo', array(0.0d, 1.0d)), "
          + "(3, 'charlie', array(0.7071d, 0.7071d)), "
          + "(4, 'delta', array(0.0d, -1.0d))";

  private static SparkSession spark;

  @BeforeAll
  static void startSpark(@TempDir Path warehouse) {
    VectorIndexTypes.register();
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("vector-search-table-function")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "4")
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
  public void returnsTheGivenColumnsAndTheDistance() {
    String table = tableWithIndex("vs_typed", "hnsw");

    List<Row> hits =
        spark
            .sql(
                String.format(
                    "SELECT id, body, _distance FROM vector_search("
                        + "table => 'local.%s', query_vector => array(0.9d, 0.1d), "
                        + "vector_column => 'embedding', k => 2)",
                    table))
            .collectAsList();

    assertThat(hits).hasSize(2);
    assertThat(hits.get(0).getLong(0)).isEqualTo(1L);
    assertThat(hits.get(0).getString(1)).isEqualTo("alpha");
    // the distance is the one that ordered the rows, so the first row is the closest
    assertThat(hits.get(0).getDouble(2)).isLessThan(hits.get(1).getDouble(2));

    // and the rows and their order are the ones the equivalent ORDER BY query returns
    List<Row> ordered =
        spark
            .sql(
                String.format(
                    "SELECT id, body FROM local.%s "
                        + "ORDER BY vec_distance(embedding, array(0.9d, 0.1d)) LIMIT 2",
                    table))
            .collectAsList();
    assertThat(hits)
        .extracting(row -> row.getLong(0))
        .containsExactly(ordered.get(0).getLong(0), ordered.get(1).getLong(0));
  }

  @Test
  public void projectsTheColumnsThatWereAskedFor() {
    String table = tableWithIndex("vs_columns", "hnsw");

    Row row =
        spark
            .sql(
                String.format(
                    "SELECT * FROM vector_search("
                        + "table => 'local.%s', query_vector => array(1.0d, 0.0d), "
                        + "vector_column => 'embedding', k => 1, columns => array('id'))",
                    table))
            .head();

    // only the columns that were named, plus the distance
    assertThat(row.size()).isEqualTo(2);
    assertThat(row.getLong(0)).isEqualTo(1L);
  }

  @Test
  public void answersWithAScanWhenTheIndexUsesAnotherMetric() {
    // the index is cosine; asking for euclidean is a different question, so the rows come from a
    // scan and the distance is the euclidean one, not a re-labelled cosine
    String table = tableWithIndex("vs_metric", "hnsw");

    String query =
        String.format(
            "SELECT id, _distance FROM vector_search("
                + "table => 'local.%s', query_vector => array(0.9d, 0.1d), "
                + "vector_column => 'embedding', k => 1, distance_type => 'euclidean')",
            table);

    Row row = spark.sql(query).head();
    assertThat(row.getLong(0)).isEqualTo(1L);
    assertThat(row.getDouble(1)).isCloseTo(Math.sqrt(0.02d), within(1e-6d));

    // the cosine question is answered from the index, so the two metric paths really do differ
    String cosine =
        String.format(
            "SELECT id FROM vector_search("
                + "table => 'local.%s', query_vector => array(0.9d, 0.1d), "
                + "vector_column => 'embedding', k => 1, distance_type => 'cosine')",
            table);
    assertThat(planOf(cosine)).contains("VectorSearchSource");
    assertThat(planOf(query)).doesNotContain("VectorSearchSource");
  }

  @Test
  public void refusesWhatItCannotAnswer() {
    String table = tableWithIndex("vs_refusals", "hnsw");

    assertThatThrownBy(
            () ->
                spark
                    .sql(
                        String.format(
                            "SELECT * FROM vector_search(table => 'local.%s', "
                                + "query_vector => array(1.0d, 0.0d), vector_column => 'embedding', "
                                + "k => 1, index_name => 'x')",
                            table))
                    .collectAsList())
        .hasMessageContaining("index_name");

    assertThatThrownBy(
            () ->
                spark
                    .sql(
                        String.format(
                            "SELECT * FROM vector_search(table => 'local.%s', "
                                + "query_vector => array(1.0d, 0.0d), vector_column => 'missing', "
                                + "k => 1)",
                            table))
                    .collectAsList())
        .hasMessageContaining("missing");

    assertThatThrownBy(
            () ->
                spark
                    .sql(
                        String.format(
                            "SELECT * FROM vector_search(table => 'local.%s', "
                                + "query_vector => array(1.0d, 0.0d), vector_column => 'embedding', "
                                + "k => 1, distance_type => 'manhattan')",
                            table))
                    .collectAsList())
        .hasMessageContaining("manhattan");
  }

  @Test
  public void refusesToOverwriteAColumnCalledDistance() {
    String table = "db.vs_distance_column";
    spark.sql(
        String.format("CREATE TABLE local.%s (id bigint, _distance double) USING iceberg", table));
    spark.sql(String.format("INSERT INTO local.%s VALUES (1, 0.5d)", table));

    assertThatThrownBy(
            () ->
                spark
                    .sql(
                        String.format(
                            "SELECT * FROM vector_search(table => 'local.%s', "
                                + "query_vector => array(1.0d, 0.0d), vector_column => '_distance', "
                                + "k => 1)",
                            table))
                    .collectAsList())
        .hasMessageContaining("_distance");
  }

  private static String planOf(String query) {
    var execution = spark.sql(query).queryExecution();
    return execution.optimizedPlan().toString() + execution.executedPlan().toString();
  }

  /** Creates the table and builds a vector index over its embedding column. */
  private static String tableWithIndex(String name, String mode) {
    String table = "db." + name;
    spark.sql(String.format("CREATE TABLE local.%s %s", table, COLUMNS));
    spark.sql(String.format("INSERT INTO local.%s VALUES %s", table, VECTORS));
    spark.sql(
        String.format(
            "CALL local.system.build_vector_index("
                + "table => '%s', column => 'embedding', dimension => 2, mode => '%s')",
            table, mode));
    return table;
  }
}
