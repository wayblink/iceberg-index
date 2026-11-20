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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.Table;
import org.apache.iceberg.index.IndexFile;
import org.apache.iceberg.index.IndexManifest;
import org.apache.iceberg.index.IndexManifestList;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.IndexReader;
import org.apache.iceberg.index.IndexSnapshot;
import org.apache.iceberg.index.IndexSnapshotWriter;
import org.apache.iceberg.index.SidecarIndexCatalog;
import org.apache.iceberg.index.spark.IndexEntries;
import org.apache.iceberg.index.spark.VectorIndexes;
import org.apache.iceberg.index.vector.VectorIndexTypes;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Safety checks for an index that covers only part of a table snapshot.
 *
 * <p>The risk this feature carries is not that a build fails — that is visible. It is that a build
 * publishes an index covering some files while every reader believes it covers the ones it does
 * not, and then answers a query with fewer rows than the table holds. So the checks here are about
 * the answer, not about the counters: a partly covered index must give the same rows, in the same
 * order, as a full scan of the table, and a file it could not index must never be recorded as
 * covered.
 */
class TestPartialBuildSmoke {

  private static final String COLUMNS = "(id bigint, embedding array<double>) USING iceberg";

  /** Query the tests search from: the origin, so ordering does not depend on tie breaking. */
  private static final double[] QUERY = {0.0d, 0.0d};

  private static SparkSession spark;

  @BeforeAll
  static void startSpark(@TempDir Path warehouse) {
    VectorIndexTypes.register();
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("partial-build-smoke")
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

  /**
   * The answer that matters: a gap in the index must not become a gap in the answer.
   *
   * <p>The table did not move, and the index covers only one of its two data files. The rows of the
   * other file are live and nearest to the query, so an index that answers alone would return the
   * wrong rows, not merely fewer.
   */
  @Test
  void aGapInTheIndexIsNotAGapInTheAnswer() throws Exception {
    String table = "gap_answer";
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
    spark.sql("CREATE TABLE local.db.t_" + table + " " + COLUMNS);
    // two files, distinct distances: 1 is nearest, 4 is farthest
    spark.sql("INSERT INTO local.db.t_" + table + " VALUES (3, array(1.0d, 1.0d))");
    spark.sql("INSERT INTO local.db.t_" + table + " VALUES (1, array(0.0d, 0.0d))");
    spark.sql("INSERT INTO local.db.t_" + table + " VALUES (4, array(2.0d, 2.0d))");
    build(table);

    // drop one index file from the published build, as a build that stopped early would
    leaveOnlyOneIndexFile(table);

    List<Long> byScan = nearestByScan(table, 3);
    List<Row> answered = search(table, 3, "table");

    assertThat(idsOf(answered))
        .as("the nearest rows of the whole table, whatever the index covers")
        .containsExactlyElementsOf(byScan);
    assertThat(scoresOf(answered))
        .as("and the distances of the rows the rewrite sorts by")
        .containsExactlyElementsOf(distancesOf(table, byScan));
  }

  /**
   * The rows of the file the index does not cover are still answered, and the ones it does cover
   * are answered from the index.
   *
   * <p>This is the check that separates "the index has a gap" from "the query has a gap": the
   * uncovered file's row must come back, and it must come back from the scan, not from the index.
   */
  @Test
  void theRowsOfAnUncoveredFileAreFilledInFromTheScan() throws Exception {
    String table = "gap_rows";
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
    spark.sql("CREATE TABLE local.db.t_" + table + " " + COLUMNS);
    spark.sql("INSERT INTO local.db.t_" + table + " VALUES (1, array(0.0d, 0.0d))");
    spark.sql("INSERT INTO local.db.t_" + table + " VALUES (2, array(3.0d, 3.0d))");
    build(table);

    Table indexed = Spark3Util.loadIcebergTable(spark, "local.db.t_" + table);
    long snapshot = indexed.currentSnapshot().snapshotId();
    leaveOnlyOneIndexFile(table);

    IndexMetadata metadata = VectorIndexes.findIndex(indexed, "embedding", "vector", null);
    Set<String> uncovered =
        new IndexReader(metadata, indexed)
            .uncoveredDataFiles(snapshot, IndexEntries.dataFiles(indexed, snapshot));
    assertThat(uncovered).as("one data file is outside the index").hasSize(1);

    // the uncovered file's row is answered, and the k nearest of the whole table is what comes back
    List<Row> answered = search(table, 2, "table");
    assertThat(idsOf(answered)).containsExactlyElementsOf(nearestByScan(table, 2));

    // the indexed mode is the one that may be short, and it must not answer with a row it cannot
    // resolve rather than with a row that is missing
    List<Row> indexedOnly = search(table, 2, "indexed");
    assertThat(indexedOnly).isNotEmpty();
    assertThat(idsOf(indexedOnly))
        .as("everything the indexed mode answers is a live row of the table")
        .isSubsetOf(idsByScan(table));
  }

  /**
   * A file a build could not index is never recorded as covered.
   *
   * <p>This is the invariant that keeps the gap visible: a data file listed as covered but holding
   * no entries is a file every query skips, and the rows it holds disappear without an error.
   */
  @Test
  void aFileThatCouldNotBeIndexedIsNotRecordedAsCovered() throws Exception {
    String table = "never_covered";
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
    spark.sql("CREATE TABLE local.db.t_" + table + " " + COLUMNS);
    spark.sql("INSERT INTO local.db.t_" + table + " VALUES (1, array(1.0d, 0.0d))");
    // a vector of the wrong width: this group cannot be indexed, and the other one can
    spark.sql("INSERT INTO local.db.t_" + table + " VALUES (2, array(0.0d, 1.0d, 2.0d))");

    spark
        .sql(
            "CALL local.system.build_vector_index(table => 'db.t_"
                + table
                + "', column => 'embedding', dimension => 2, max_entries_per_index_file => 1)")
        .collectAsList();

    Table indexed = Spark3Util.loadIcebergTable(spark, "local.db.t_" + table);
    long snapshot = indexed.currentSnapshot().snapshotId();
    IndexMetadata metadata = VectorIndexes.findIndex(indexed, "embedding", "vector", null);
    IndexSnapshot published = metadata.snapshotForSourceTableSnapshot(snapshot);
    assertThat(published).as("the build published for the snapshot it was asked about").isNotNull();

    List<String> covered = new ArrayList<>();
    for (IndexFile file : indexFilesOf(indexed, published)) {
      covered.addAll(file.coveredDataFiles());
    }

    List<String> dataFiles = IndexEntries.dataFiles(indexed, snapshot);
    assertThat(dataFiles).as("two data files, one of which cannot be indexed").hasSize(2);
    assertThat(covered)
        .as("the file that could not be indexed is not listed as covered by any index file")
        .hasSize(1)
        .isSubsetOf(dataFiles);

    // and the reader agrees: exactly the file the build left out is the one reported as uncovered
    Set<String> uncovered =
        new IndexReader(metadata, indexed).uncoveredDataFiles(snapshot, dataFiles);
    assertThat(uncovered)
        .as("the uncovered set is the complement of what was published, not empty")
        .hasSize(1);
    assertThat(covered).doesNotContainAnyElementsOf(uncovered);
  }

  /**
   * Cleaning up the index tree after a partial build leaves the published index usable.
   *
   * <p>The files of the attempt are referenced by the published snapshot, so the sweep must keep
   * them and delete only what nothing points at. A sweep that took the partial build for garbage
   * would break every query that uses the index.
   */
  @Test
  void cleaningUpAfterAPartialBuildKeepsThePublishedFiles() throws Exception {
    String table = "cleanup_after_partial";
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
    spark.sql("CREATE TABLE local.db.t_" + table + " " + COLUMNS);
    spark.sql("INSERT INTO local.db.t_" + table + " VALUES (1, array(0.5d, 0.5d))");
    spark.sql("INSERT INTO local.db.t_" + table + " VALUES (2, array(0.0d, 0.5d, 0.5d))");
    spark
        .sql(
            "CALL local.system.build_vector_index(table => 'db.t_"
                + table
                + "', column => 'embedding', dimension => 2, max_entries_per_index_file => 1)")
        .collectAsList();

    Table indexed = Spark3Util.loadIcebergTable(spark, "local.db.t_" + table);
    IndexMetadata before = VectorIndexes.findIndex(indexed, "embedding", "vector", null);
    List<IndexFile> published = indexFilesOf(indexed, before.latestSnapshot());
    assertThat(published).as("the partial build published one index file").hasSize(1);

    spark.sql("CALL local.system.cleanup_index_files(table => 'db.t_" + table + "')");

    Table after = Spark3Util.loadIcebergTable(spark, "local.db.t_" + table);
    IndexMetadata stillPublished = VectorIndexes.findIndex(after, "embedding", "vector", null);
    assertThat(stillPublished.latestSnapshot().indexManifestList())
        .as("the published snapshot is still there")
        .isEqualTo(before.latestSnapshot().indexManifestList());
    assertThat(indexFilesOf(after, stillPublished.latestSnapshot()))
        .as("and the index file it names was not swept away")
        .hasSize(1)
        .allSatisfy(
            file ->
                assertThat(after.io().newInputFile(file.location()).exists())
                    .as("index file %s still exists", file.location())
                    .isTrue());
  }

  /**
   * A refresh that cannot index every file it was asked to still fails, so a caller that did not
   * ask for a partial index never silently gets one.
   */
  @Test
  void aStrictRefreshStillFailsOnDataItCannotIndex() throws Exception {
    String table = "strict_refresh";
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
    spark.sql("CREATE TABLE local.db.t_" + table + " " + COLUMNS);
    spark.sql("INSERT INTO local.db.t_" + table + " VALUES (1, array(0.0d, 0.0d))");
    build(table);
    spark.sql("INSERT INTO local.db.t_" + table + " VALUES (2, array(0.0d, 0.0d, 1.0d))");

    assertThatThrownBy(
            () ->
                spark.sql(
                    "CALL local.system.refresh_vector_index(table => 'db.t_"
                        + table
                        + "', column => 'embedding')"))
        .as("the refresh reports the failure instead of publishing a gap")
        .hasMessageContaining("dimensions");

    // and it published nothing: the index still answers for the snapshot it was built for
    Table after = Spark3Util.loadIcebergTable(spark, "local.db.t_" + table);
    IndexMetadata metadata = VectorIndexes.findIndex(after, "embedding", "vector", null);
    assertThat(metadata.snapshots()).as("a failed refresh adds no snapshot").hasSize(1);
    assertThat(metadata.snapshotForSourceTableSnapshot(after.currentSnapshot().snapshotId()))
        .as("and the current table snapshot is still not answered for")
        .isNull();
  }

  /**
   * The entry point people actually use: the rewrite of {@code ORDER BY vec_distance ... LIMIT k}.
   *
   * <p>It binds the index and fills the gap with a scan in one plan, which is a different path from
   * the procedure, so the gap has to be checked here as well.
   */
  @Test
  void theSqlRewriteAnswersTheSameAsAFullScanOverAGap() throws Exception {
    String table = "rewrite_gap";
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
    spark.sql("CREATE TABLE local.db.t_" + table + " " + COLUMNS);
    spark.sql("INSERT INTO local.db.t_" + table + " VALUES (3, array(1.0d, 1.0d))");
    spark.sql("INSERT INTO local.db.t_" + table + " VALUES (1, array(0.0d, 0.0d))");
    spark.sql("INSERT INTO local.db.t_" + table + " VALUES (4, array(2.0d, 2.0d))");
    build(table);
    leaveOnlyOneIndexFile(table);

    List<Long> rewritten =
        spark
            .sql(
                "SELECT id FROM local.db.t_"
                    + table
                    + " ORDER BY vec_distance(embedding, array(0.0d, 0.0d)) LIMIT 3")
            .collectAsList()
            .stream()
            .map(row -> row.getLong(0))
            .collect(Collectors.toList());

    assertThat(rewritten)
        .as("the rewritten query answers the same rows in the same order as a full scan")
        .containsExactlyElementsOf(nearestByScan(table, 3));
  }

  /**
   * A gap and an append at the same time: the index covers part of its snapshot, and the table has
   * moved past that snapshot since.
   *
   * <p>The gap and the new file are two different reasons for a data file not to be in the index,
   * and an answer that has to fill both is the state a table spends most of its life in.
   */
  @Test
  void aGapAndAnAppendTogetherStillAnswerTheWholeTable() throws Exception {
    String table = "gap_and_append";
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
    spark.sql("CREATE TABLE local.db.t_" + table + " " + COLUMNS);
    spark.sql("INSERT INTO local.db.t_" + table + " VALUES (1, array(0.0d, 0.0d))");
    spark.sql("INSERT INTO local.db.t_" + table + " VALUES (6, array(3.0d, 3.0d))");
    build(table);
    leaveOnlyOneIndexFile(table);

    // a file that arrives after the index was built, nearest of all
    spark.sql("INSERT INTO local.db.t_" + table + " VALUES (5, array(0.0d, 0.01d))");

    List<Row> answered = search(table, 3, "table");
    assertThat(idsOf(answered))
        .as("the appended row, the covered row and the uncovered row are all answered")
        .containsExactlyElementsOf(nearestByScan(table, 3));
    assertThat(idsOf(answered)).contains(5L);
  }

  /**
   * A batch over a table with a gap answers each query the way the single-query path does.
   *
   * <p>The batch shares one index load and one gap-filling scan across its queries, so a gap that
   * is handled correctly one query at a time is still worth checking here: the two paths do not
   * share the code that decides what is uncovered.
   */
  @Test
  void aBatchOverAGapAnswersEachQueryLikeTheSingleQueryPath() throws Exception {
    String table = "batch_gap";
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
    spark.sql("CREATE TABLE local.db.t_" + table + " " + COLUMNS);
    spark.sql("INSERT INTO local.db.t_" + table + " VALUES (1, array(0.0d, 0.0d))");
    spark.sql("INSERT INTO local.db.t_" + table + " VALUES (2, array(1.0d, 0.0d))");
    spark.sql("INSERT INTO local.db.t_" + table + " VALUES (3, array(3.0d, 3.0d))");
    spark.sql("INSERT INTO local.db.t_" + table + " VALUES (4, array(4.0d, 4.0d))");
    build(table);
    leaveOnlyOneIndexFile(table);

    List<String> queries = List.of("0.0,0.0", "3.0,3.0", "4.0,4.0");
    List<Row> batch =
        spark
            .sql(
                "CALL local.system.vector_search_batch(table => 'db.t_"
                    + table
                    + "', column => 'embedding', queries => array("
                    + queries.stream().map(q -> "'" + q + "'").collect(Collectors.joining(", "))
                    + "), k => 2)")
            .collectAsList();

    assertThat(batch).as("two hits per query, as asked").hasSize(6);
    for (int query = 0; query < queries.size(); query++) {
      int queryId = query;
      double[] values = parseQuery(queries.get(query));
      List<Long> expected = nearestByScan(table, values, 2);
      List<Long> answered =
          batch.stream()
              .filter(row -> row.getInt(0) == queryId)
              .map(row -> parseId(row.getString(4)))
              .collect(Collectors.toList());
      assertThat(answered)
          .as("query %s answers the rows a full scan of the table would", queries.get(query))
          .containsExactlyElementsOf(expected);
    }
  }

  private void build(String table) {
    spark
        .sql(
            "CALL local.system.build_vector_index(table => 'db.t_"
                + table
                + "', column => 'embedding', dimension => 2, metric => 'l2', "
                + "structure => 'flat', max_entries_per_index_file => 1)")
        .collectAsList();
  }

  /**
   * Rewrites the published snapshot so that it names one index file of the build instead of all of
   * them, which is what a build that stopped after one group would have published.
   */
  private void leaveOnlyOneIndexFile(String table) throws Exception {
    Table indexed = Spark3Util.loadIcebergTable(spark, "local.db.t_" + table);
    IndexMetadata metadata = VectorIndexes.findIndex(indexed, "embedding", "vector", null);
    IndexSnapshot latest = metadata.latestSnapshot();
    List<IndexFile> files = indexFilesOf(indexed, latest);
    assertThat(files).as("the build wrote one index file per group").hasSizeGreaterThan(1);

    IndexSnapshotWriter writer = new IndexSnapshotWriter(indexed.io());
    String manifestList =
        writer.writeManifestList(metadata.location(), latest.snapshotId(), List.of(files.get(0)));
    IndexSnapshot partial = latest.toBuilder().indexManifestList(manifestList).build();
    new SidecarIndexCatalog(indexed.io(), indexed)
        .updateIndex(
            indexed.location(), metadata.indexUuid(), current -> current.replaceSnapshot(partial));
    indexed.refresh();
  }

  private List<IndexFile> indexFilesOf(Table table, IndexSnapshot snapshot) {
    try {
      IndexManifestList manifestList =
          IndexManifestList.read(table.io().newInputFile(snapshot.indexManifestList()));
      List<IndexFile> files = new ArrayList<>();
      for (IndexManifestList.ManifestRef ref : manifestList.manifests()) {
        files.addAll(IndexManifest.read(table.io().newInputFile(ref.location())).indexFiles());
      }

      return files;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private List<Row> search(String table, int k, String coverage) {
    String sql =
        "CALL local.system.vector_search(table => 'db.t_"
            + table
            + "', column => 'embedding', query => array(0.0d, 0.0d), k => "
            + k
            + (coverage == null ? "" : ", coverage => '" + coverage + "'")
            + ")";
    return spark.sql(sql).collectAsList();
  }

  private List<Long> nearestByScan(String table, double[] query, int k) {
    return idsByScan(table).stream()
        .sorted(Comparator.comparingDouble(id -> distanceOf(table, query, id)))
        .limit(k)
        .collect(Collectors.toList());
  }

  /** The k nearest rows, computed from the table alone rather than from any index. */
  private List<Long> nearestByScan(String table, int k) {
    List<Long> ids = idsByScan(table);
    return ids.stream()
        .sorted(Comparator.comparingDouble(id -> distanceOf(table, id)))
        .limit(k)
        .collect(Collectors.toList());
  }

  private List<Long> idsByScan(String table) {
    return spark.sql("SELECT id FROM local.db.t_" + table).collectAsList().stream()
        .map(row -> row.getLong(0))
        .collect(Collectors.toList());
  }

  private double distanceOf(String table, double[] query, long id) {
    Row row = spark.sql("SELECT embedding FROM local.db.t_" + table + " WHERE id = " + id).first();
    List<?> values = row.getList(0);
    double sum = 0.0d;
    for (int i = 0; i < query.length; i++) {
      double delta = ((Number) values.get(i)).doubleValue() - query[i];
      sum += delta * delta;
    }

    return Math.sqrt(sum);
  }

  private static double[] parseQuery(String query) {
    String[] parts = query.split(",");
    double[] values = new double[parts.length];
    for (int i = 0; i < parts.length; i++) {
      values[i] = Double.parseDouble(parts[i]);
    }

    return values;
  }

  private double distanceOf(String table, long id) {
    Row row = spark.sql("SELECT embedding FROM local.db.t_" + table + " WHERE id = " + id).first();
    List<?> values = row.getList(0);
    double sum = 0.0d;
    for (int i = 0; i < QUERY.length; i++) {
      double delta = ((Number) values.get(i)).doubleValue() - QUERY[i];
      sum += delta * delta;
    }

    return Math.sqrt(sum);
  }

  private List<Double> distancesOf(String table, List<Long> ids) {
    return ids.stream().map(id -> round(distanceOf(table, id))).collect(Collectors.toList());
  }

  private static List<Long> idsOf(List<Row> hits) {
    return hits.stream().map(hit -> parseId(hit.getString(3))).collect(Collectors.toList());
  }

  private static List<Double> scoresOf(List<Row> hits) {
    return hits.stream().map(hit -> round(hit.getDouble(2))).collect(Collectors.toList());
  }

  private static double round(double value) {
    return Math.round(value * 1_000_000d) / 1_000_000d;
  }

  private static long parseId(String rowJson) {
    int start = rowJson.indexOf("\"id\":") + 5;
    int end = rowJson.indexOf(',', start);
    return Long.parseLong(rowJson.substring(start, end == -1 ? rowJson.indexOf('}', start) : end));
  }
}
