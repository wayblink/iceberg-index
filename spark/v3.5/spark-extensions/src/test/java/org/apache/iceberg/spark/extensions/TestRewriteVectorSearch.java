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
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import org.apache.iceberg.index.vector.VectorIndexTypes;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
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
        .contains("VectorSearchSource");
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
    assertThat(planOf(indexed, String.format(query, indexed))).contains("VectorSearchSource");
  }

  @Test
  public void overSamplingReachesTheReScoringInsteadOfBeingTruncated() {
    // an approximate kernel ranks its candidates by an estimate, so the query asks for more of them
    // than it needs and an exact re-scoring picks the nearest. That only works when every candidate
    // reaches the re-scoring: keeping the kernel's own best `limit` throws the rest away, and the
    // answer is then whatever the estimate said, however wide the candidate set was asked to be
    int rows = 2000;
    int dimension = 128;
    java.util.Random random = new java.util.Random(20261021L);
    java.util.List<Row> data = Lists.newArrayList();
    float[][] vectors = new float[rows][dimension];
    for (int id = 0; id < rows; id++) {
      List<Float> values = Lists.newArrayList();
      for (int d = 0; d < dimension; d++) {
        vectors[id][d] = random.nextInt(256);
        values.add(vectors[id][d]);
      }
      data.add(RowFactory.create((long) id, "row" + id, values));
    }

    String table = "docs_oversample_reaches";
    spark
        .createDataFrame(
            data,
            new StructType(
                new StructField[] {
                  new StructField("id", DataTypes.LongType, false, Metadata.empty()),
                  new StructField("body", DataTypes.StringType, false, Metadata.empty()),
                  new StructField(
                      "embedding",
                      DataTypes.createArrayType(DataTypes.FloatType),
                      false,
                      Metadata.empty())
                }))
        .writeTo("local.db." + table)
        .createOrReplace();
    spark.sql(
        String.format(
            "CALL local.system.build_vector_index("
                + "table => 'db.%s', column => 'embedding', dimension => 128, "
                + "metric => 'euclidean', structure => 'IVF', quantizer => 'RABITQ', "
                + "options => map('nlist', '20', 'nprobe', '20', 'rabitq-bits', '3'), "
                + "max_entries_per_index_file => 100000)",
            table));

    float[] query = new float[dimension];
    for (int d = 0; d < dimension; d++) {
      query[d] = random.nextInt(256);
    }

    // the whole table is asked for as candidates (200 x 10 rows), so the re-scoring has every row
    // to choose from and its answer is the exact one
    try {
      spark.conf().set("spark.iceberg.index.vector.over-sample", "200");
      List<Long> returned =
          spark
              .sql(
                  String.format(
                      "SELECT id FROM local.db.%s "
                          + "ORDER BY vec_distance(embedding, %s) LIMIT 10",
                      table, toSqlArray(query)))
              .collectAsList()
              .stream()
              .map(row -> row.getLong(0))
              .collect(Collectors.toList());
      assertThat(returned)
          .as("the exact ten nearest, out of the whole table as candidates")
          .containsExactlyElementsOf(exactNearest(vectors, query, 10));
    } finally {
      spark.conf().unset("spark.iceberg.index.vector.over-sample");
    }
  }

  @Test
  public void overSamplingDoesNotChangeTheAnswerOnATableSmallerThanTheCandidateSet() {
    String query =
        "SELECT id FROM local.%s ORDER BY vec_distance(embedding, array(0.0d, 0.95d)) LIMIT 3";
    String indexed = tableWithIndex("docs_oversample", "hnsw");

    List<Row> widenedOnce;
    try {
      // this table is four rows, so every over-sample already holds the whole table and there is
      // nothing for a wider candidate set to add: the answer cannot change
      spark.conf().set("spark.iceberg.index.vector.over-sample", "1");
      widenedOnce = spark.sql(String.format(query, indexed)).collectAsList();

      for (String overSample : new String[] {"5", "50"}) {
        spark.conf().set("spark.iceberg.index.vector.over-sample", overSample);
        assertThat(spark.sql(String.format(query, indexed)).collectAsList())
            .as("the same rows in the same order, over-sampling by %s", overSample)
            .containsExactlyElementsOf(widenedOnce);
      }

      // a value that cannot be a multiplier is reported rather than quietly replaced: a setting
      // the query asked for is not something the rule may rewrite, and turning 0 into 1 would hide
      // the mistake instead of naming it
      for (String nonsense : new String[] {"0", "-3"}) {
        spark.conf().set("spark.iceberg.index.vector.over-sample", nonsense);
        assertThatThrownBy(() -> spark.sql(String.format(query, indexed)).collectAsList())
            .as("the configured value %s is rejected", nonsense)
            .hasMessageContaining("spark.iceberg.index.vector.over-sample");
      }
    } finally {
      spark.conf().unset("spark.iceberg.index.vector.over-sample");
    }
  }

  @Test
  public void overSamplingKeepsTheNearestCandidatesRatherThanTheFarthest() {
    // an approximate index is asked for more candidates than the query wants, and the re-scoring
    // that follows has to keep the nearest of them: keeping the farthest answers with rows that are
    // nowhere near the query, which is invisible on a table small enough to be one candidate set
    String table = "db.docs_oversample_order";
    spark.sql(String.format("CREATE TABLE local.%s %s", table, COLUMNS));
    StringBuilder values = new StringBuilder();
    for (int id = 0; id < 400; id++) {
      values
          .append(id == 0 ? "" : ", ")
          .append("(")
          .append(id)
          .append(", 'row")
          .append(id)
          .append("', array(")
          .append(id + 1)
          .append(".0d, 0.0d))");
    }
    spark.sql(String.format("INSERT INTO local.%s VALUES %s", table, values));
    spark.sql(
        String.format(
            "CALL local.system.build_vector_index("
                + "table => '%s', column => 'embedding', dimension => 2, metric => 'euclidean', "
                + "structure => 'IVF', "
                + "options => map('nlist', '4', 'nprobe', '4'))",
            table));

    String query =
        String.format(
            "SELECT id FROM local.%s ORDER BY vec_distance(embedding, array(1.1d, 0.0d)) LIMIT 10",
            table);

    try {
      spark.conf().set("spark.iceberg.index.vector.over-sample", "10");
      List<Row> hits = spark.sql(query).collectAsList();
      // the vectors are 1.0, 2.0, ... 400.0 on a line and the query is 1.1, so the ten nearest
      // are rows 0 to 9
      assertThat(hits.stream().map(row -> row.getLong(0)))
          .as("the ten rows closest to the query")
          .containsExactly(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L);
    } finally {
      spark.conf().unset("spark.iceberg.index.vector.over-sample");
    }
  }

  @Test
  public void usesAnExactIndexInsteadOfScanning() {
    // a flat index is the vector column repacked with a row reference; the old rule stood aside
    // because searching it walks every entry, which is a cost decision rather than a correctness
    // one, and a cost decision is not what decides whether a query may use an index
    String table = tableWithIndex("docs_exact_only", "exact");

    assertThat(
            planOf(
                table,
                String.format(
                    "SELECT id FROM local.%s ORDER BY vec_distance(embedding, array(0.9d, 0.1d)) LIMIT 2",
                    table)))
        .contains("VectorSearchSource");

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
    assertThat(planOf(table, query)).contains("VectorSearchSource");
  }

  @Test
  public void followsTheMetricOfTheIndexThatAnswersIt() {
    // cosine and euclidean disagree about which row is nearest: a far row on the query's own ray
    // has cosine 1, while a close row off the ray has the smaller euclidean distance. Ordering by
    // the metric the function starts with would answer a euclidean query with the cosine winner
    String table = "db.docs_metric";
    spark.sql(String.format("CREATE TABLE local.%s %s", table, COLUMNS));
    spark.sql(
        String.format(
            "INSERT INTO local.%s VALUES "
                + "(1, 'far-on-the-ray', array(10.0d, 0.0d)), "
                + "(2, 'near-off-the-ray', array(1.0d, 0.1d))",
            table));
    spark.sql(
        String.format(
            "CALL local.system.build_vector_index("
                + "table => '%s', column => 'embedding', dimension => 2, metric => 'euclidean', "
                + "structure => 'flat')",
            table));

    List<Row> hits =
        spark
            .sql(
                String.format(
                    "SELECT id FROM local.%s "
                        + "ORDER BY vec_distance(embedding, array(1.0d, 0.0d)) LIMIT 1",
                    table))
            .collectAsList();
    assertThat(hits).hasSize(1);
    assertThat(hits.get(0).getLong(0)).isEqualTo(2L);
    assertThat(
            planOf(
                table,
                "SELECT id FROM local.%s "
                    + "ORDER BY vec_distance(embedding, array(1.0d, 0.0d)) LIMIT 1"))
        .contains("vec_distance(EUCLIDEAN");
  }

  @Test
  public void ordersByTheColumnItNamesRatherThanADistanceOfTheSameName() {
    // a distance aliased to a column's own name is not the column: ordering by the qualified column
    // asks for that column's order, and answering it with the distance would be a different query
    String table = "db.docs_shadow";
    spark.sql(
        String.format(
            "CREATE TABLE local.%s (id bigint, rank double, embedding array<double>) USING iceberg",
            table));
    // ordering by rank puts 3 first, ordering by the distance puts 2 first
    spark.sql(
        String.format(
            "INSERT INTO local.%s VALUES "
                + "(1, 2.0d, array(10.0d, 0.0d)), "
                + "(2, 3.0d, array(1.0d, 0.1d)), "
                + "(3, 1.0d, array(2.0d, 1.0d))",
            table));
    spark.sql(
        String.format(
            "CALL local.system.build_vector_index("
                + "table => '%s', column => 'embedding', dimension => 2, metric => 'euclidean', "
                + "structure => 'flat')",
            table));

    String query =
        String.format(
            "SELECT id, vec_distance(embedding, array(1.0d, 0.0d)) AS rank FROM local.%s "
                + "ORDER BY local.%s.rank LIMIT 1",
            table, table);

    assertThat(planOf(table, query))
        .as("the ordering names a column, so there is nothing to answer from an index")
        .doesNotContain("VectorSearchSource");
    assertThat(spark.sql(query).collectAsList())
        .as("the smallest rank, which is not the nearest row")
        .extracting(row -> row.getLong(0))
        .containsExactly(3L);
  }

  @Test
  public void rewritesAQueryThatOrdersByAProjectedDistance() {
    // selecting the distance under a name and ordering by that name is the natural way to show it:
    // Spark then leaves an attribute in the sort, with the distance in the projection below it
    String table = tableWithIndex("docs_aliased", "hnsw");
    String query =
        String.format(
            "SELECT id, vec_distance(embedding, array(0.9d, 0.1d)) AS d FROM local.%s "
                + "ORDER BY d LIMIT 2",
            table);

    assertThat(planOf(table, query)).contains("VectorSearchSource");

    List<Row> hits = spark.sql(query).collectAsList();
    assertThat(hits).hasSize(2);
    assertThat(hits).extracting(row -> row.getLong(0)).containsExactly(1L, 3L);
    // the distance is still the one the caller asked for, so it is reported with the row
    assertThat(hits.get(0).getDouble(1)).isCloseTo(0.0053, within(0.001));
  }

  @Test
  public void aProjectedDistanceFollowsTheMetricOfTheIndexThatAnswersIt() {
    // the metric is bound where the distance is computed, which for a projected distance is the
    // projection: ordering by the alias must not answer with the metric the function starts with
    String table = "db.docs_metric_aliased";
    spark.sql(String.format("CREATE TABLE local.%s %s", table, COLUMNS));
    spark.sql(
        String.format(
            "INSERT INTO local.%s VALUES "
                + "(1, 'far-on-the-ray', array(10.0d, 0.0d)), "
                + "(2, 'near-off-the-ray', array(1.0d, 0.1d))",
            table));
    spark.sql(
        String.format(
            "CALL local.system.build_vector_index("
                + "table => '%s', column => 'embedding', dimension => 2, metric => 'euclidean', "
                + "structure => 'flat')",
            table));

    String query =
        String.format(
            "SELECT id, vec_distance(embedding, array(1.0d, 0.0d)) AS d FROM local.%s "
                + "ORDER BY d LIMIT 1",
            table);

    List<Row> hits = spark.sql(query).collectAsList();
    assertThat(hits).hasSize(1);
    assertThat(hits.get(0).getLong(0)).isEqualTo(2L);
    assertThat(planOf(table, query)).contains("vec_distance(EUCLIDEAN");
  }

  @Test
  public void aProjectedDistanceOfAnotherColumnIsLeftAlone() {
    // the alias names a distance over a column without an index, so there is nothing to answer with
    String table = "db.docs_aliased_other";
    spark.sql(String.format("CREATE TABLE local.%s %s", table, COLUMNS));
    spark.sql(String.format("INSERT INTO local.%s VALUES %s", table, VECTORS));
    spark.sql(
        String.format(
            "CALL local.system.build_vector_index("
                + "table => '%s', column => 'embedding', dimension => 2, mode => 'hnsw')",
            table));

    String query =
        String.format("SELECT id FROM local.%s ORDER BY vec_distance(body, 'zz') LIMIT 2", table);

    assertThat(planOf(table, query)).doesNotContain("VectorSearchSource");
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
        .doesNotContain("VectorSearchSource");
  }

  @Test
  public void usesAnApproximateIndexAndReRanksExactly() {
    String table = tableWithIndex("docs_hnsw", "hnsw");

    // an approximate index is used too: its candidates are re-scored exactly before the limit
    assertThat(planOf(table, nearestNeighbour(table, 2))).contains("VectorSearchSource");

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
    assertThat(planOf(table, nearestNeighbour(table, 2))).contains("VectorSearchSource");

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

    assertThat(planOf(table, nearestNeighbour(table, 3))).contains("VectorSearchSource");

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

    assertThat(planOf(table, nearestNeighbour(table, 2))).doesNotContain("VectorSearchSource");
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
        .doesNotContain("VectorSearchSource");

    // a filter without an order and limit is not the shape either
    assertThat(planOf(table, String.format("SELECT id FROM local.%s WHERE id > 1", table)))
        .doesNotContain("VectorSearchSource");
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
    assertThat(planOf(table, nearestNeighbour(table, 2))).contains("VectorSearchSource");
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
    assertThat(planOf(table, nearestNeighbour(table, 2))).doesNotContain("VectorSearchSource");
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
    var execution = spark.sql(String.format(query, table)).queryExecution();
    // both plans are inspected: a rewrite shows up in the optimization, and the physical plan is
    // what actually runs the search
    return execution.optimizedPlan().toString() + execution.executedPlan().toString();
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

  /** The ids of the {@code count} nearest vectors, closest first, ties by id. */
  private static List<Long> exactNearest(float[][] vectors, float[] query, int count) {
    List<Long> ids = Lists.newArrayList();
    for (int id = 0; id < vectors.length; id++) {
      ids.add((long) id);
    }
    ids.sort(
        Comparator.<Long>comparingDouble(id -> distance(query, vectors[id.intValue()]))
            .thenComparingLong(id -> id));
    return ids.subList(0, count);
  }

  private static double distance(float[] left, float[] right) {
    double sum = 0.0;
    for (int i = 0; i < left.length; i++) {
      double difference = (double) left[i] - right[i];
      sum += difference * difference;
    }
    return sum;
  }

  private static String toSqlArray(float[] vector) {
    StringBuilder sql = new StringBuilder("array(");
    for (int i = 0; i < vector.length; i++) {
      if (i > 0) {
        sql.append(", ");
      }
      sql.append(String.format(Locale.ROOT, "%sd", vector[i]));
    }
    return sql.append(')').toString();
  }
}
