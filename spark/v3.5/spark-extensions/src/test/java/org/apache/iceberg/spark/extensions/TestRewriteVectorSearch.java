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
    String table = tableWithIndex("docs_exact", "hnsw");

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
    String indexed = tableWithIndex("docs_scan_indexed", "hnsw");
    List<Row> withIndex = spark.sql(String.format(query, indexed)).collectAsList();

    assertThat(withIndex).containsExactlyElementsOf(withoutIndex);
    assertThat(planOf(indexed, String.format(query, indexed))).contains("LocalRelation");
  }

  @Test
  public void askingForAWiderCandidateSetDoesNotChangeTheAnswer() {
    String query =
        "SELECT id FROM local.%s ORDER BY vec_distance(embedding, array(0.0d, 0.95d)) LIMIT 3";
    String indexed = tableWithIndex("docs_oversample", "hnsw");

    List<Row> widenedOnce;
    try {
      // one is what an exactly scoring kernel needs: merging each file's exact top-k is already the
      // exact global top-k, so asking for more would only fetch more rows for the same answer
      spark.conf().set("spark.iceberg.index.vector.over-sample", "1");
      widenedOnce = spark.sql(String.format(query, indexed)).collectAsList();

      for (String overSample : new String[] {"5", "50"}) {
        spark.conf().set("spark.iceberg.index.vector.over-sample", overSample);
        assertThat(spark.sql(String.format(query, indexed)).collectAsList())
            .as("the same rows in the same order, over-sampling by %s", overSample)
            .containsExactlyElementsOf(widenedOnce);
      }

      // a value that means nothing falls back to asking for what the query wants, rather than
      // failing a query over a configuration
      for (String nonsense : new String[] {"0", "-3"}) {
        spark.conf().set("spark.iceberg.index.vector.over-sample", nonsense);
        assertThat(spark.sql(String.format(query, indexed)).collectAsList())
            .as("a usable answer for the configured value %s", nonsense)
            .containsExactlyElementsOf(widenedOnce);
      }
    } finally {
      spark.conf().unset("spark.iceberg.index.vector.over-sample");
    }
  }

  @Test
  public void leavesAnExactIndexAloneAndScans() {
    // an exact index repacks the vector column with a row reference and is searched by walking
    // every entry, which is the work a scan does over a payload larger than the column itself
    String table = tableWithIndex("docs_exact_only", "exact");

    assertThat(
            planOf(
                table,
                String.format(
                    "SELECT id FROM local.%s ORDER BY vec_distance(embedding, array(0.9d, 0.1d)) LIMIT 2",
                    table)))
        .doesNotContain("LocalRelation");

    // the answers are still right, they just come from a scan
    List<Row> hits = search(table, 2);
    assertThat(hits).hasSize(2);
    assertThat(hits.get(0).getLong(0)).isEqualTo(1L);
    assertThat(hits.get(1).getLong(0)).isEqualTo(3L);
  }

  @Test
  public void acceptsAQueryVectorOfDecimalsAndAFloatColumn() {
    // array(0.9, 0.1) is an array of decimals in Spark rather than an array of doubles, and a float
    // column is backed by floats: neither survives Spark's own conversion of an array to doubles
    String table = "db.docs_float";
    spark.sql(
        String.format(
            "CREATE TABLE local.%s (id bigint, body string, embedding array<float>) USING iceberg",
            table));
    spark.sql(
        String.format(
            "INSERT INTO local.%s VALUES (1, 'alpha', array(1.0F, 0.0F)), "
                + "(2, 'bravo', array(0.0F, 1.0F)), (3, 'charlie', array(0.7071F, 0.7071F))",
            table));
    spark.sql(
        String.format(
            "CALL local.system.build_vector_index("
                + "table => '%s', column => 'embedding', dimension => 2, mode => 'hnsw')",
            table));

    String query =
        String.format(
            "SELECT id FROM local.%s ORDER BY vec_distance(embedding, array(0.9, 0.1)) LIMIT 2",
            table);

    // the two closest to (0.9, 0.1) are alpha and charlie, whichever numeric type carried them
    List<Row> hits = spark.sql(query).collectAsList();
    assertThat(hits).hasSize(2);
    assertThat(hits.get(0).getLong(0)).isEqualTo(1L);
    assertThat(hits.get(1).getLong(0)).isEqualTo(3L);
    assertThat(planOf(table, query)).contains("LocalRelation");
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
  public void usesAnApproximateIndexAndReRanksExactly() {
    String table = tableWithIndex("docs_hnsw", "hnsw");

    // an approximate index is used too: its candidates are re-scored exactly before the limit
    assertThat(planOf(table, nearestNeighbour(table, 2))).contains("LocalRelation");

    // the table is small enough for the graph to hold every vector, so the re-ranked candidates are
    // the ones a full scan gives
    assertThat(search(table, 2)).isEqualTo(fullScan(table, 2));
    assertThat(search(table, 3)).isEqualTo(fullScan(table, 3));
    assertThat(search(table, 4)).isEqualTo(fullScan(table, 4));
  }

  @Test
  public void usesAnIndexThatIsBehindAndScansTheDifference() {
    String table = tableWithIndex("docs_behind", "hnsw");
    // a row added after the index was built leaves the index behind the current snapshot
    spark.sql(String.format("INSERT INTO local.%s VALUES (5, 'echo', array(2.0d, 1.0d))", table));

    // the index is still used, and the data file it does not cover is scanned on the side
    assertThat(planOf(table, nearestNeighbour(table, 2))).contains("LocalRelation");

    // the indexed row is the closest one, and the new row comes from the scan of the data file the
    // index does not cover
    List<Row> hits = search(table, 2);
    assertThat(hits.stream().map(row -> row.getLong(0))).containsExactly(1L, 5L);
    assertThat(hits).isEqualTo(fullScan(table, 2));
  }

  @Test
  public void doesNotReturnRowsDeletedAfterTheIndexWasBuilt() {
    String table = tableWithIndex("docs_deleted", "hnsw");
    spark.sql(String.format("DELETE FROM local.%s WHERE id = 1", table));

    assertThat(planOf(table, nearestNeighbour(table, 3))).contains("LocalRelation");

    // the deleted row is the closest one to the query, so it leaves a hole the next hit fills
    List<Row> hits = search(table, 3);
    assertThat(hits.stream().map(row -> row.getLong(0))).containsExactly(3L, 2L, 4L);
    assertThat(hits).isEqualTo(fullScan(table, 3));
  }

  @Test
  public void leavesThePlanAloneWhenTheIndexKnowsNothingAboutTheSnapshot() {
    String table = table("docs_rolled_back");
    spark.sql(
        String.format(
            "INSERT INTO local.%s VALUES (5, 'echo', array(2.0d, 1.0d)), (6, 'foxtrot', array(-2.0d, -1.0d))",
            table));
    spark.sql(
        String.format(
            "CALL local.system.build_vector_index(table => '%s', column => 'embedding', dimension => 2, mode => 'hnsw')",
            table));

    // the index was built on the second snapshot, so it has nothing to say about the first one
    List<Row> snapshots =
        spark
            .sql(
                String.format(
                    "SELECT snapshot_id FROM local.%s.snapshots ORDER BY committed_at", table))
            .collectAsList();
    spark.sql(
        String.format(
            "CALL local.system.rollback_to_snapshot(table => '%s', snapshot_id => %s)",
            table, snapshots.get(0).getLong(0)));

    assertThat(planOf(table, nearestNeighbour(table, 2))).doesNotContain("LocalRelation");
    assertThat(search(table, 2)).isEqualTo(fullScan(table, 2));
  }

  @Test
  public void leavesThePlanAloneWhenTheQueryIsNotANearestNeighbourQuery() {
    String table = tableWithIndex("docs_shape", "hnsw");

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

  @Test
  public void rewritesAQueryOnAnIdentityPartitionedTable() {
    String table = "db.docs_partitioned";
    spark.sql(
        String.format(
            "CREATE TABLE local.%s (id bigint, body string, embedding array<double>) "
                + "USING iceberg PARTITIONED BY (id)",
            table));
    spark.sql(
        String.format(
            "INSERT INTO local.%s VALUES %s", table, VECTORS.replace("array(", "array(")));
    spark.sql(
        String.format(
            "CALL local.system.build_vector_index("
                + "table => '%s', column => 'embedding', dimension => 2, mode => 'hnsw')",
            table));

    // the partition column is not in the data file, its value comes from the path
    assertThat(planOf(table, nearestNeighbour(table, 2))).contains("LocalRelation");
    assertThat(partitionRows(table)).isEqualTo(partitionScan(table));
  }

  @Test
  public void leavesThePlanAloneForATransformedPartition() {
    String table = "db.docs_bucketed";
    spark.sql(
        String.format(
            "CREATE TABLE local.%s (id bigint, body string, embedding array<double>) "
                + "USING iceberg PARTITIONED BY (bucket(4, id))",
            table));
    spark.sql(String.format("INSERT INTO local.%s VALUES %s", table, VECTORS));
    spark.sql(
        String.format(
            "CALL local.system.build_vector_index("
                + "table => '%s', column => 'embedding', dimension => 2, mode => 'hnsw')",
            table));

    // the bucket number is in the path but the column value is not, so the rows cannot be read back
    assertThat(planOf(table, nearestNeighbour(table, 2))).doesNotContain("LocalRelation");
  }

  private static List<Row> partitionRows(String table) {
    return spark
        .sql(
            String.format(
                "SELECT id, vec_distance(embedding, array(0.9d, 0.1d)) AS d FROM local.%s "
                    + "ORDER BY d LIMIT 3",
                table))
        .collectAsList();
  }

  private static List<Row> partitionScan(String table) {
    return spark
        .sql(
            String.format(
                "SELECT id, vec_distance(embedding, array(0.9d, 0.1d)) + 0.0d AS d FROM local.%s "
                    + "ORDER BY d LIMIT 3",
                table))
        .collectAsList();
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

  private static String nearestNeighbour(String table, int limit) {
    return String.format(
        "SELECT id FROM local.%s ORDER BY vec_distance(embedding, array(0.9d, 0.1d)) LIMIT %d",
        table, limit);
  }

  private static List<Row> search(String table, int limit) {
    return spark.sql(nearestNeighbour(table, limit)).collectAsList();
  }

  private static List<Row> fullScan(String table, int limit) {
    // the same ordering, wrapped in an addition the rewrite does not recognize, so the whole table
    // is scanned instead
    return spark
        .sql(
            String.format(
                "SELECT id FROM local.%s ORDER BY vec_distance(embedding, array(0.9d, 0.1d)) + 0.0d LIMIT %d",
                table, limit))
        .collectAsList();
  }
}
