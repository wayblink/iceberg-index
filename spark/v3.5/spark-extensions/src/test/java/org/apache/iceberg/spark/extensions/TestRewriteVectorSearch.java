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
 * Tests the automatic rewrite of a nearest-neighbour query into a vector index lookup: with an
 * exact index bound to the snapshot being read, the query must return what a full scan returns, and
 * in every other case the plan must be the one Spark would produce anyway.
 *
 * <p>Every test uses a table of its own, because an index belongs to a column and a table, so two
 * tests cannot share one.
 */
class TestRewriteVectorSearch {

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
            .appName("rewrite-vector-search")
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
  public void rewritesANearestNeighbourQueryIntoAnIndexLookup() {
    String table = tableWithIndex("docs_exact", "exact");

    List<Row> hits =
        spark
            .sql(
                String.format(
                    "SELECT id, body FROM local.%s "
                        + "ORDER BY vec_distance(embedding, array(0.9d, 0.1d)) LIMIT 2",
                    table))
            .collectAsList();

    // the two closest to (0.9, 0.1) are alpha and charlie
    assertThat(hits).hasSize(2);
    assertThat(hits.get(0).getLong(0)).isEqualTo(1L);
    assertThat(hits.get(0).getString(1)).isEqualTo("alpha");
    assertThat(hits.get(1).getLong(0)).isEqualTo(3L);

    // and the plan went through the index, which is what the rewrite is for
    assertThat(
            planOf(
                table,
                "SELECT id FROM local.%s ORDER BY vec_distance(embedding, array(0.9d, 0.1d)) LIMIT 2"))
        .contains("LocalRelation");
  }

  @Test
  public void matchesAFullScanWithoutTheIndex() {
    String query =
        "SELECT id FROM local.%s ORDER BY vec_distance(embedding, array(0.0d, 0.95d)) LIMIT 3";

    // without an index the query is answered by a scan and the expression
    String plain = table("docs_scan_plain");
    List<Row> withoutIndex = spark.sql(String.format(query, plain)).collectAsList();

    // with an index the same query must give the same rows in the same order
    String indexed = tableWithIndex("docs_scan_indexed", "exact");
    List<Row> withIndex = spark.sql(String.format(query, indexed)).collectAsList();

    assertThat(withIndex).containsExactlyElementsOf(withoutIndex);
    assertThat(planOf(indexed, String.format(query, indexed))).contains("LocalRelation");
  }

  @Test
  public void leavesThePlanAloneWithoutAnIndex() {
    String table = table("docs_no_index");
    assertThat(
            planOf(
                table,
                String.format(
                    "SELECT id FROM local.%s ORDER BY vec_distance(embedding, array(0.9d, 0.1d)) LIMIT 2",
                    table)))
        .doesNotContain("LocalRelation");
  }

  @Test
  public void leavesThePlanAloneForAnApproximateIndex() {
    String table = tableWithIndex("docs_hnsw", "hnsw");

    // HNSW candidates have to be re-scored first, which is not part of this stage
    assertThat(
            planOf(
                table,
                String.format(
                    "SELECT id FROM local.%s ORDER BY vec_distance(embedding, array(0.9d, 0.1d)) LIMIT 2",
                    table)))
        .doesNotContain("LocalRelation");

    // the query still works, through the expression itself
    List<Row> hits =
        spark
            .sql(
                String.format(
                    "SELECT id FROM local.%s ORDER BY vec_distance(embedding, array(0.9d, 0.1d)) LIMIT 1",
                    table))
            .collectAsList();
    assertThat(hits.get(0).getLong(0)).isEqualTo(1L);
  }

  @Test
  public void leavesThePlanAloneForAStaleIndex() {
    String table = tableWithIndex("docs_stale", "exact");
    // a row added after the index was built leaves the index behind the current snapshot
    spark.sql(String.format("INSERT INTO local.%s VALUES (5, 'echo', array(2.0d, 1.0d))", table));

    assertThat(
            planOf(
                table,
                String.format(
                    "SELECT id FROM local.%s ORDER BY vec_distance(embedding, array(0.9d, 0.1d)) LIMIT 2",
                    table)))
        .doesNotContain("LocalRelation");

    // and the results still include the new row
    List<Row> hits =
        spark
            .sql(
                String.format(
                    "SELECT id FROM local.%s ORDER BY vec_distance(embedding, array(0.9d, 0.4d)) LIMIT 1",
                    table))
            .collectAsList();
    assertThat(hits.get(0).getLong(0)).isEqualTo(5L);
  }

  @Test
  public void leavesThePlanAloneWhenTheQueryIsNotANearestNeighbourQuery() {
    String table = tableWithIndex("docs_shape", "exact");

    // descending order asks for the farthest rows, which the index does not answer
    assertThat(
            planOf(
                table,
                String.format(
                    "SELECT id FROM local.%s ORDER BY vec_distance(embedding, array(0.9d, 0.1d)) DESC LIMIT 2",
                    table)))
        .doesNotContain("LocalRelation");

    // a filter without an order and limit is not the shape either
    assertThat(planOf(table, String.format("SELECT id FROM local.%s WHERE id > 1", table)))
        .doesNotContain("LocalRelation");
  }

  /** Creates the table without an index. */
  private static String table(String name) {
    String table = "db." + name;
    spark.sql(String.format("CREATE TABLE local.%s %s", table, COLUMNS));
    spark.sql(String.format("INSERT INTO local.%s VALUES %s", table, VECTORS));
    return table;
  }

  /** Creates the table and builds a vector index over its embedding column. */
  private static String tableWithIndex(String name, String mode) {
    String table = table(name);
    spark.sql(
        String.format(
            "CALL local.system.build_vector_index("
                + "table => '%s', column => 'embedding', dimension => 2, mode => '%s')",
            table, mode));
    return table;
  }

  private static String planOf(String table, String query) {
    return spark.sql(String.format(query, table)).queryExecution().optimizedPlan().toString();
  }
}
