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
import org.apache.iceberg.index.text.TextIndexTypes;
import org.apache.iceberg.index.vector.VectorIndexTypes;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests the index table-valued functions: they must return exactly what the corresponding CALL
 * procedure returns, and their result must be usable like any other table, which is the point of
 * having them.
 */
class TestIndexTableFunctions {

  @BeforeAll
  static void registerIndexTypes() {
    VectorIndexTypes.register();
    TextIndexTypes.register();
  }

  private static SparkSession spark;
  private static Path warehouseDir;

  @BeforeAll
  static void startSpark(@TempDir Path warehouse) {
    warehouseDir = warehouse;
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("index-table-functions")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "4")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", warehouse.toUri().toString())
            .getOrCreate();

    spark.sql(
        "CREATE TABLE local.db.docs (id bigint, body string, embedding array<double>) USING iceberg");
    spark.sql(
        "INSERT INTO local.db.docs VALUES "
            + "(1, 'iceberg table format', array(1.0d, 0.0d)), "
            + "(2, 'vector search over embeddings', array(0.0d, 1.0d)), "
            + "(3, 'iceberg snapshot isolation', array(0.7d, 0.7d)), "
            + "(4, 'parquet columnar storage', array(0.9d, 0.1d))");
    spark.sql(
        "CALL local.system.build_vector_index(table => 'db.docs', column => 'embedding', dimension => 2)");
    spark.sql("CALL local.system.build_text_index(table => 'db.docs', column => 'body')");
  }

  @AfterAll
  static void stopSpark() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  public void vectorIndexSearchMatchesTheProcedure() {
    List<Row> fromProcedure =
        spark
            .sql(
                "CALL local.system.vector_search(table => 'db.docs', column => 'embedding', "
                    + "query => array(0.95d, 0.05d), k => 3)")
            .collectAsList();

    List<Row> fromFunction =
        spark
            .sql(
                "SELECT * FROM vector_index_search('local.db.docs', 'embedding', array(0.95d, 0.05d), 3)")
            .collectAsList();

    assertThat(fromFunction).hasSameSizeAs(fromProcedure);
    assertThat(fromFunction).containsExactlyElementsOf(fromProcedure);
  }

  @Test
  public void vectorIndexSearchBatchMatchesTheProcedure() {
    List<Row> fromProcedure =
        spark
            .sql(
                "CALL local.system.vector_search_batch(table => 'db.docs', column => 'embedding', "
                    + "queries => array('0.95,0.05', '0.1,0.9'), k => 2)")
            .collectAsList();

    List<Row> fromFunction =
        spark
            .sql(
                "SELECT * FROM vector_index_search_batch("
                    + "'local.db.docs', 'embedding', array('0.95,0.05', '0.1,0.9'), 2)")
            .collectAsList();

    assertThat(fromFunction).hasSameSizeAs(fromProcedure);
    assertThat(fromFunction).containsExactlyElementsOf(fromProcedure);
  }

  @Test
  public void textIndexSearchMatchesTheProcedure() {
    List<Row> fromProcedure =
        spark
            .sql(
                "CALL local.system.text_search(table => 'db.docs', column => 'body', "
                    + "query => 'iceberg table', k => 2)")
            .collectAsList();

    List<Row> fromFunction =
        spark
            .sql("SELECT * FROM text_index_search('local.db.docs', 'body', 'iceberg table', 2)")
            .collectAsList();

    assertThat(fromFunction).hasSameSizeAs(fromProcedure);
    assertThat(fromFunction).containsExactlyElementsOf(fromProcedure);
  }

  @Test
  public void resultsComposeWithTheRestOfTheQuery() {
    // filter, order, limit and projection all work, which is what the procedure could not do
    List<Row> hits =
        spark
            .sql(
                "SELECT get_json_object(row, '$.id') AS id, score FROM "
                    + "vector_index_search('local.db.docs', 'embedding', array(1.0d, 0.0d), 10) "
                    + "WHERE score > 0.5 ORDER BY score DESC LIMIT 2")
            .collectAsList();

    assertThat(hits).hasSize(2);
    assertThat(hits.get(0).getString(0)).isEqualTo("1");
    // scores descend
    assertThat(hits.get(0).getDouble(1)).isGreaterThanOrEqualTo(hits.get(1).getDouble(1));

    // the function can be joined against the table
    List<Row> joined =
        spark
            .sql(
                "SELECT count(*) AS n FROM "
                    + "text_index_search('local.db.docs', 'body', 'iceberg', 10) h "
                    + "JOIN local.db.docs d "
                    + "ON get_json_object(h.row, '$.id') = CAST(d.id AS string)")
            .collectAsList();
    assertThat(joined.get(0).getLong(0)).isEqualTo(2L);
  }

  @Test
  public void plansOfQueriesWithoutTheFunctionAreUnchanged() {
    // a query that does not use a table-valued function must plan exactly as before
    String explain =
        spark
            .sql("SELECT id FROM local.db.docs WHERE id > 1 ORDER BY id")
            .queryExecution()
            .executedPlan()
            .toString();
    assertThat(explain).contains("Scan");
    assertThat(explain).doesNotContain("vector_index_search");
  }

  /**
   * A table with columns named like the ones a search returns is searched like any other: a
   * resolved row is serialised from the schema it was read with, so no column name of the row is
   * parsed as a SQL reference and none of them collides with the columns of the answer.
   */
  @Test
  public void searchResolvesColumnsNamedLikeTheOutputColumns() {
    spark.sql("DROP TABLE IF EXISTS local.db.clashing");
    spark.sql(
        "CREATE TABLE local.db.clashing (id bigint, pos bigint, data_file string,"
            + " embedding array<double>) USING iceberg");
    spark.sql(
        "INSERT INTO local.db.clashing VALUES"
            + " (1, 10, 'mine-a', array(1.0d, 0.0d)),"
            + " (2, 20, 'mine-b', array(0.0d, 1.0d)),"
            + " (3, 30, 'mine-c', array(0.7d, 0.7d))");
    spark.sql(
        "CALL local.system.build_vector_index(table => 'db.clashing', column => 'embedding',"
            + " dimension => 2)");

    List<String> rows =
        spark
            .sql(
                "CALL local.system.vector_search(table => 'db.clashing', column => 'embedding',"
                    + " query => array(1.0d, 0.0d), k => 3)")
            .collectAsList()
            .stream()
            .map(hit -> hit.getString(3))
            .collect(Collectors.toList());

    assertThat(rows).hasSize(3);
    assertThat(rows)
        .as("the columns of the row come back under their own names")
        .anySatisfy(
            json ->
                assertThat(json)
                    .contains("\"id\":1")
                    .contains("\"pos\":10")
                    .contains("\"data_file\":\"mine-a\""));
  }

  @Test
  public void rejectsNonConstantArguments() {
    // a column reference cannot be folded, so the function refuses to plan
    assertThatThrownBy(
            () ->
                spark
                    .sql(
                        "SELECT * FROM vector_index_search('local.db.docs', 'embedding', embedding, 3)")
                    .collectAsList())
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("embedding");
  }

  private static List<Long> ids(List<Row> rows) {
    return rows.stream()
        .map(row -> Long.parseLong(row.getString(3).replaceAll("[^0-9]", "")))
        .collect(Collectors.toList());
  }
}
