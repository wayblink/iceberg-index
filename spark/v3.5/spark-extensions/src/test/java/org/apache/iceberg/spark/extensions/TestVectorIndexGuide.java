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
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.Table;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.spark.IndexBuildStatus;
import org.apache.iceberg.index.spark.VectorIndexes;
import org.apache.iceberg.index.vector.VectorIndexTypes;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Runs the statements of {@code vector-index-guide.md} and checks what the manual claims about
 * them.
 *
 * <p>The manual is the user-facing description of the feature, so a statement in it that the engine
 * does not accept, and a claim about what a statement does that is not true, are both defects of
 * the documentation. Every test here is one section of it, written the way a reader would copy it.
 */
class TestVectorIndexGuide {

  private static SparkSession spark;

  @BeforeAll
  static void startSpark(@TempDir Path warehouse) {
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("vector-index-guide")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "4")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", warehouse.toString())
            .getOrCreate();
    VectorIndexTypes.register();
  }

  @AfterAll
  static void stopSpark() {
    if (spark != null) {
      spark.stop();
    }
  }

  @BeforeEach
  void resetSettings() {
    spark.conf().unset("spark.iceberg.index.vector.over-sample");
    spark.conf().unset("spark.iceberg.index.vector.name");
  }

  /** §1 · the statements of the quick start, copied as written. */
  @Test
  void quickStartReturnsTheNearestRow() {
    spark.sql(
        "CREATE TABLE local.db.qs (id bigint, title string, embedding array<double>) USING iceberg");
    spark.sql(
        "INSERT INTO local.db.qs VALUES "
            + "(1, 'iceberg tables', array(1.0, 0.0)), "
            + "(2, 'vector search',  array(0.9, 0.1)), "
            + "(3, 'parquet files',  array(0.0, 1.0))");

    spark.sql(
        "CALL local.system.build_vector_index("
            + "table => 'db.qs', column => 'embedding', dimension => 2, metric => 'cosine')");

    List<Row> rows =
        spark
            .sql(
                "SELECT id, title FROM local.db.qs "
                    + "ORDER BY vec_distance(embedding, array(0.95d, 0.05d)) LIMIT 3")
            .collectAsList();
    assertThat(rows).hasSize(3);
    assertThat(rows.get(0).getLong(0)).as("the nearest row of the quick start").isEqualTo(1L);

    List<Row> status =
        spark.sql("CALL local.system.index_build_status(table => 'db.qs')").collectAsList();
    assertThat(status).hasSize(1);
    assertThat(status.get(0).getString(5)).isEqualTo("SUCCEEDED");
  }

  /**
   * §2 · a dimension that does not match the column fails the build, and the failure is readable.
   */
  @Test
  void dimensionMismatchFailsTheBuild() {
    createTable("dim", 8);

    assertThatThrownBy(
            () ->
                spark.sql(
                    "CALL local.system.build_vector_index("
                        + "table => 'db.dim', column => 'embedding', dimension => 768)"))
        .isInstanceOf(RuntimeException.class);

    List<Row> status =
        spark.sql("CALL local.system.index_build_status(table => 'db.dim')").collectAsList();
    assertThat(status).hasSize(1);
    assertThat(status.get(0).getString(5)).isEqualTo("FAILED");
    assertThat(status.get(0).getString(14)).isNotNull();
  }

  /** §3 · the full parameter list of the build, as the manual spells it. */
  @Test
  void theDocumentedBuildParametersAreAccepted() {
    createTable("ivf", 64);

    spark.sql(
        "CALL local.system.build_vector_index("
            + "table => 'db.ivf', column => 'embedding', dimension => 2, metric => 'cosine', "
            + "structure => 'IVF', quantizer => 'RABITQ', "
            + "options => map('nlist', '0', 'nprobe', '32', 'rabitq-bits', '1'), "
            + "name => 'arm_ivf_1bit', max_entries_per_index_file => 1000000)");

    List<Row> indexes =
        spark.sql("CALL local.system.list_indexes(table => 'db.ivf')").collectAsList();
    assertThat(indexes).hasSize(1);
    Row index = indexes.get(0);
    assertThat(index.getString(2)).isEqualTo("vector");
    assertThat(index.getString(8)).isEqualTo("arm_ivf_1bit");
    assertThat(index.getString(7)).contains("RABITQ").contains("nprobe");

    List<Row> hits =
        spark
            .sql(
                "CALL local.system.vector_search(table => 'db.ivf', column => 'embedding', "
                    + "query => array(0.95d, 0.05d), k => 3, index => 'arm_ivf_1bit')")
            .collectAsList();
    assertThat(hits).isNotEmpty();
    assertThat(hits.stream().map(row -> row.getString(3))).allMatch(row -> row.contains("\"id\":"));
  }

  /** §3.2 · the second index of a column needs a name, and an unnamed search refuses to guess. */
  @Test
  void twoIndexesOnOneColumnNeedNames() {
    createTable("two", 16);
    spark.sql(
        "CALL local.system.build_vector_index("
            + "table => 'db.two', column => 'embedding', dimension => 2, structure => 'IVF', "
            + "quantizer => 'RABITQ')");

    assertThatThrownBy(
            () ->
                spark.sql(
                    "CALL local.system.build_vector_index("
                        + "table => 'db.two', column => 'embedding', dimension => 2)"))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("already exists");

    spark.sql(
        "CALL local.system.build_vector_index("
            + "table => 'db.two', column => 'embedding', dimension => 2, name => 'flat_second', "
            + "structure => 'FLAT')");

    assertThatThrownBy(
            () ->
                spark.sql(
                    "CALL local.system.vector_search(table => 'db.two', column => 'embedding', "
                        + "query => array(0.95d, 0.05d), k => 2)"))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("several indexes exist")
        .hasMessageContaining("flat_second");
  }

  /** §3.1 and §4.1 · which structures the automatic rewrite uses. */
  @Test
  void theRewriteUsesWhateverStructureTheIndexHas() {
    createTable("flat_idx", 8);
    spark.sql(
        "CALL local.system.build_vector_index("
            + "table => 'db.flat_idx', column => 'embedding', dimension => 2)");
    // cost is not what decides the plan: the column has a usable index, so it answers
    assertThat(planOf("db.flat_idx"))
        .as("an exact FLAT index answers too, it just scores every entry")
        .contains("VectorSearchSource");

    createTable("ivf_idx", 8);
    spark.sql(
        "CALL local.system.build_vector_index("
            + "table => 'db.ivf_idx', column => 'embedding', dimension => 2, structure => 'IVF', "
            + "quantizer => 'RABITQ')");
    assertThat(planOf("db.ivf_idx"))
        .as("an approximate kernel asked for k candidates cannot find the true top-k")
        .doesNotContain("VectorSearchSource");

    // §4.1 · the manual's own statement: the multiplier is what lets an approximate index answer
    try {
      spark.sql("SET `spark.iceberg.index.vector.over-sample`=10");
      assertThat(planOf("db.ivf_idx"))
          .as("an approximate index is used once the candidates are widened")
          .contains("VectorSearchSource");
    } finally {
      spark.conf().unset("spark.iceberg.index.vector.over-sample");
    }
  }

  /**
   * §4.1 · a distance the caller projected under a name is ordered by that name, and that is the
   * same query.
   */
  @Test
  void aDistanceOrderedByTheNameItWasProjectedUnderIsRewritten() {
    createTable("aliased", 8);
    spark.sql(
        "CALL local.system.build_vector_index("
            + "table => 'db.aliased', column => 'embedding', dimension => 2)");

    String query =
        "SELECT id, vec_distance(embedding, array(0.95d, 0.05d)) AS d FROM local.db.aliased "
            + "ORDER BY d LIMIT 3";

    assertThat(spark.sql(query).queryExecution().optimizedPlan().toString())
        .as("ordering by the projected name reaches the index too")
        .contains("VectorSearchSource");
    // the same query with a shape the rewrite does not recognise is answered from a full scan, and
    // the index has to answer what that scan answers
    String scanned = query.replace("ORDER BY d LIMIT 3", "ORDER BY d + 0.0d LIMIT 3");
    assertThat(spark.sql(query).collectAsList())
        .as("the index answers what a scan answers")
        .isEqualTo(spark.sql(scanned).collectAsList());
  }

  /** §2 · only identity partitioning is rewritten. */
  @Test
  void aTransformedPartitionIsNotRewritten() {
    spark.sql(
        "CREATE TABLE local.db.bucketed (id bigint, embedding array<double>) USING iceberg "
            + "PARTITIONED BY (bucket(4, id))");
    spark.sql(
        "INSERT INTO local.db.bucketed SELECT id, array(CAST(id AS double), CAST(10 - id AS double)) "
            + "FROM range(0, 20)");
    spark.sql(
        "CALL local.system.build_vector_index("
            + "table => 'db.bucketed', column => 'embedding', dimension => 2, structure => 'IVF', "
            + "quantizer => 'RABITQ')");

    assertThat(planOf("db.bucketed"))
        .as("a bucket partition has no value in the path to recover")
        .doesNotContain("VectorSearchSource");

    // the explicit entry point does not care about partitioning
    assertThat(
            spark
                .sql(
                    "CALL local.system.vector_search(table => 'db.bucketed', column => 'embedding', "
                        + "query => array(9.0d, 1.0d), k => 3)")
                .collectAsList())
        .isNotEmpty();
  }

  /** §4.1 · widening the candidates is what makes an approximate index find the true neighbour. */
  @Test
  void overSampleIsASessionSetting() {
    createTable("over", 32);
    spark.sql(
        "CALL local.system.build_vector_index("
            + "table => 'db.over', column => 'embedding', dimension => 2, structure => 'IVF', "
            + "quantizer => 'RABITQ', options => map('nprobe', '32', 'rabitq-bits', '4'))");

    // the same ordering with an addition the rewrite does not recognize, so the table is scanned
    // and the answer is exact
    List<Row> exact =
        spark
            .sql(
                "SELECT id FROM local.db.over "
                    + "ORDER BY vec_distance(embedding, array(0.95d, 0.05d)) + 0.0d LIMIT 5")
            .collectAsList();

    // a kernel that scores approximately cannot be given one candidate per hit: the widened set is
    // what puts the true neighbours in it
    spark.sql("SET `spark.iceberg.index.vector.over-sample`=10");
    List<Row> widened = spark.sql(nearest("db.over", 5)).collectAsList();

    assertThat(widened.get(0).getLong(0))
        .as("with the candidates widened, the rewritten query finds the true nearest row")
        .isEqualTo(exact.get(0).getLong(0));
  }

  /** §4.2 and §9 · a deleted row is gone from the live answer and null in the index hits. */
  @Test
  void aRowDeletedAfterTheBuildComesBackWithoutItsRow() {
    createTable("deleted", 8);
    spark.sql(
        "CALL local.system.build_vector_index("
            + "table => 'db.deleted', column => 'embedding', dimension => 2, metric => 'euclidean', "
            + "structure => 'IVF', quantizer => 'RABITQ', options => map('nprobe', '64'))");

    spark.sql("DELETE FROM local.db.deleted WHERE id = 1");

    // the default coverage is `table`: the procedure answers with live rows, so a deleted row is
    // not
    // among them and nothing is returned with a null row
    List<Row> live =
        spark
            .sql(
                "CALL local.system.vector_search(table => 'db.deleted', column => 'embedding', "
                    + "query => array(1.0d, 0.0d), k => 3)")
            .collectAsList();
    assertThat(live).as("the procedure answers with the rows the table still holds").isNotEmpty();
    assertThat(live.stream().map(row -> row.isNullAt(3)))
        .as("a live row is never reported as a missing one")
        .doesNotContain(true);
    assertThat(live.stream().map(row -> row.getString(3)))
        .as("and the row that was deleted is not one of them")
        .noneMatch(json -> json.contains("\"id\":1,"));

    // asking for coverage => 'index' returns the hits themselves: the deleted row keeps its place
    // in
    // the index and loses only its row, which is what a caller inspecting the index sees
    List<Row> hits =
        spark
            .sql(
                "CALL local.system.vector_search(table => 'db.deleted', column => 'embedding', "
                    + "query => array(1.0d, 0.0d), k => 3, coverage => 'index_hits')")
            .collectAsList();
    assertThat(hits).as("the index returns the candidates it was asked for").hasSize(3);
    assertThat(hits.stream().map(row -> row.isNullAt(3)))
        .as("a row deleted since the build has no row to return")
        .contains(true);

    // the rewrite path fills the hole from the next candidate instead, which is what asking an
    // approximate index for more candidates than hits buys: without the multiplier the query would
    // be scanned, and a plain scan needs no widening at all
    List<Row> rewritten;
    try {
      spark.sql("SET `spark.iceberg.index.vector.over-sample`=10");
      rewritten =
          spark
              .sql(
                  "SELECT id FROM local.db.deleted "
                      + "ORDER BY vec_distance(embedding, array(1.0d, 0.0d)) LIMIT 3")
              .collectAsList();
    } finally {
      spark.conf().unset("spark.iceberg.index.vector.over-sample");
    }
    assertThat(rewritten).hasSize(3);
    assertThat(rewritten).noneMatch(row -> row.getLong(0) == 1L);
  }

  /** §4.2 · the batch entry point takes the string forms and the query table the manual shows. */
  @Test
  void theBatchEntryPointTakesStringsAndAQueryTable() {
    createTable("batch", 16);
    buildIndex("db.batch");

    List<Row> hits =
        spark
            .sql(
                "CALL local.system.vector_search_batch(table => 'db.batch', column => 'embedding', "
                    + "queries => array('0.95,0.05', '0.0,0.95'), k => 2)")
            .collectAsList();
    assertThat(hits).isNotEmpty();
    assertThat(hits.stream().map(row -> row.getInt(0)).distinct().count()).isEqualTo(2);

    spark.sql("CREATE TABLE local.db.batch_queries (qid int, qvec array<double>) USING iceberg");
    spark.sql("INSERT INTO local.db.batch_queries VALUES (10, array(0.95d, 0.05d))");
    List<Row> fromTable =
        spark
            .sql(
                "CALL local.system.vector_search_batch(table => 'db.batch', column => 'embedding', "
                    + "k => 2, query_table => 'db.batch_queries', query_id_column => 'qid', "
                    + "vector_column => 'qvec')")
            .collectAsList();
    assertThat(fromTable).isNotEmpty();
    // the ids are the positions of the query rows in the order the call read them, not the values
    // of query_id_column, which only decides that order
    assertThat(fromTable.get(0).getInt(0)).isZero();
  }

  /** §4.3 · the table-valued function can be filtered and joined, as the manual shows. */
  @Test
  void theTableFunctionCanBeFiltered() {
    createTable("tvf", 16);
    buildIndex("db.tvf");

    List<Row> rows =
        spark
            .sql(
                "SELECT get_json_object(row, '$.id') AS id, score "
                    + "FROM vector_index_search('local.db.tvf', 'embedding', array(0.95d, 0.05d), 100) "
                    + "WHERE score > 0.5 ORDER BY score DESC LIMIT 5")
            .collectAsList();
    assertThat(rows).isNotEmpty();

    // the kernel's own order, not an exact one: these rows are not re-scored
    List<Double> scores = rows.stream().map(row -> row.getDouble(1)).collect(Collectors.toList());
    assertThat(scores).isSortedAccordingTo(Comparator.reverseOrder());
    assertThat(Integer.parseInt(rows.get(0).getString(0))).isBetween(0, 15);
  }

  /** §6 · the columns of the four status calls match what the manual tabulates. */
  @Test
  void theStatusCallsHaveTheDocumentedColumns() {
    createTable("status", 8);
    spark.sql(
        "CALL local.system.build_vector_index("
            + "table => 'db.status', column => 'embedding', dimension => 2)");

    assertThat(spark.sql("CALL local.system.list_indexes(table => 'db.status')").columns())
        .containsExactly(
            "index_uuid",
            "column",
            "index_type",
            "source_snapshot_id",
            "status",
            "index_files",
            "entries",
            "definition",
            "index_name",
            "index_snapshot_id");

    // The index snapshot is the build, the source snapshot is the data it covers: both are
    // reported, and neither can stand in for the other.
    Row listed =
        spark.sql("CALL local.system.list_indexes(table => 'db.status')").collectAsList().get(0);
    Long indexSnapshot = listed.getAs("index_snapshot_id");
    Long sourceSnapshot = listed.getAs("source_snapshot_id");
    assertThat(indexSnapshot).isNotNull();
    assertThat(indexSnapshot.longValue()).isPositive();
    assertThat(sourceSnapshot).isNotNull();

    assertThat(spark.sql("CALL local.system.check_indexes(table => 'db.status')").columns())
        .containsExactly(
            "table",
            "column",
            "index_uuid",
            "index_name",
            "index_type",
            "writer",
            "writer_version",
            "reader_version",
            "readable",
            "source_snapshot_id",
            "stale",
            "uncovered_data_files",
            "suggestion");

    assertThat(spark.sql("CALL local.system.index_build_status(table => 'db.status')").columns())
        .containsExactly(
            "table",
            "column",
            "index_uuid",
            "index_name",
            "index_type",
            "state",
            "started_at",
            "updated_at",
            "age_ms",
            "stale",
            "files_built",
            "files_total",
            "files_failed",
            "rows",
            "error");

    assertThat(spark.sql("CALL local.system.index_cache_status()").columns())
        .containsExactly(
            "executor_id",
            "host",
            "epoch",
            "mode",
            "entries",
            "bytes",
            "hits",
            "misses",
            "loads",
            "evictions",
            "coverage",
            "working_set_bytes",
            "reported_at",
            "age_ms",
            "stale");
  }

  /** §5 · an index that lags the table is still used, and the uncovered files are scanned. */
  @Test
  void aStaleIndexStillReturnsTheNewRows() {
    createTable("stale", 16);
    buildIndex("db.stale");

    // the closest row of the whole table is one the index has never seen
    spark.sql("INSERT INTO local.db.stale VALUES (999, array(0.95d, 0.05d))");

    List<Row> rows = spark.sql(nearest("db.stale", 1)).collectAsList();
    assertThat(rows.get(0).getLong(0))
        .as("the residual scan covers what the index does not")
        .isEqualTo(999L);

    spark.sql("CALL local.system.refresh_vector_index(table => 'db.stale', column => 'embedding')");
    List<Row> refreshed = spark.sql(nearest("db.stale", 1)).collectAsList();
    assertThat(refreshed.get(0).getLong(0)).isEqualTo(999L);
  }

  /**
   * §8 · `cleanup_index_files` keeps a healthy index readable.
   *
   * <p>The sweep is verified on a table of its own: running it immediately before a drop on the
   * same table fails today for a reason of its own, which is tracked as a defect of the sweep
   * rather than of the manual.
   */
  @Test
  void cleanupKeepsTheIndexReadable() {
    createTable("cleanup", 16);
    buildIndex("db.cleanup");

    List<Row> before = spark.sql(nearest("db.cleanup", 3)).collectAsList();
    List<Row> cleaned =
        spark.sql("CALL local.system.cleanup_index_files(table => 'db.cleanup')").collectAsList();

    assertThat(cleaned).as("one row per index").hasSize(1);
    assertThat(cleaned.get(0).getLong(2)).as("deleted_files").isZero();
    assertThat(spark.sql(nearest("db.cleanup", 3)).collectAsList()).isEqualTo(before);
  }

  /** §8 · dropping purges the registration, the index tree and the report of its builds. */
  @Test
  void dropPurgesTheRegistrationAndTheTree() throws Exception {
    createTable("dropped", 8);
    spark.sql(
        "CALL local.system.build_vector_index("
            + "table => 'db.dropped', column => 'embedding', dimension => 2)");

    Table table = tableOf("db.dropped");
    IndexMetadata index = VectorIndexes.findIndexOrNull(table, "embedding", "vector");
    String statusFile = IndexBuildStatus.statusDirFor(index.location()) + "/build-status-1.json";
    assertThat(table.io().newInputFile(statusFile).exists()).isTrue();

    List<Row> dropped =
        spark
            .sql(
                "CALL local.system.drop_vector_index(table => 'db.dropped', column => 'embedding', "
                    + "purge => true)")
            .collectAsList();
    assertThat(dropped).hasSize(1);
    assertThat(dropped.get(0).getBoolean(1)).as("purge => true").isTrue();
    assertThat(spark.sql("CALL local.system.list_indexes(table => 'db.dropped')").collectAsList())
        .isEmpty();
    assertThat(table.io().newInputFile(statusFile).exists())
        .as("purge removes the index tree, the status report with it")
        .isFalse();

    assertThatThrownBy(
            () ->
                spark.sql(
                    "CALL local.system.vector_search(table => 'db.dropped', column => 'embedding', "
                        + "query => array(1.0d, 0.0d), k => 1)"))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("No vector index exists");
  }

  /** §6 · the status calls resolve the table through the catalog the call names. */
  @Test
  void theStatusCallsUseTheCatalogOfTheCall() {
    createTable("cat", 8);
    spark.sql(
        "CALL local.system.build_vector_index("
            + "table => 'db.cat', column => 'embedding', dimension => 2)");
    assertThat(spark.conf().get("spark.sql.defaultCatalog", "spark_catalog"))
        .as("the session's current catalog is not the one the calls name")
        .isNotEqualTo("local");

    assertThat(spark.sql("CALL local.system.index_build_status(table => 'db.cat')").collectAsList())
        .hasSize(1);
    assertThat(spark.sql("CALL local.system.check_indexes(table => 'db.cat')").collectAsList())
        .hasSize(1);
    assertThat(spark.sql("CALL local.system.list_indexes(table => 'db.cat')").collectAsList())
        .hasSize(1);
  }

  /** A modest IVF index, which is what the manual reaches for when a table is not tiny. */
  private static void buildIndex(String table) {
    spark.sql(
        String.format(
            "CALL local.system.build_vector_index(table => '%s', column => 'embedding', "
                + "dimension => 2, structure => 'IVF', quantizer => 'RABITQ', "
                + "options => map('nprobe', '64'))",
            table));
  }

  private static String nearest(String table, int limit) {
    return String.format(
        "SELECT id FROM local.%s ORDER BY vec_distance(embedding, array(0.95d, 0.05d)) LIMIT %d",
        table, limit);
  }

  private static String planOf(String table) {
    return spark.sql(nearest(table, 3)).queryExecution().optimizedPlan().toString();
  }

  private static Table tableOf(String name) throws Exception {
    return org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark, "local." + name);
  }

  /**
   * A table whose vector is {@code (id, rows - id)}, so id 0 is nearest to the query of the tests.
   */
  private static void createTable(String name, int rows) {
    spark.sql(
        String.format(
            "CREATE TABLE local.db.%s (id bigint, embedding array<double>) USING iceberg", name));
    spark.sql(
        String.format(
            "INSERT INTO local.db.%s SELECT id, array(CAST(id AS double), CAST(%d - id AS double)) "
                + "FROM range(0, %d)",
            name, rows, rows));
  }
}
