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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
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
 * The partial build at a size where its costs show: a table of many data files where the index has
 * a gap covering a real share of the rows.
 *
 * <p>The small-table checks prove the semantics. This one proves the semantics survive contact with
 * the shapes a large table has: many index files in several manifests, a gap that holds most of the
 * rows, and an answer that has to merge what the index returned with what the scan returned.
 *
 * <p>It is not a benchmark and must not be read as one: the rows are generated, the machine is a
 * laptop, and nothing here measures a cost.
 *
 * <p>What this class adds over the small-table checks is the number of index files, not the number
 * of rows: ten groups means the answer merges ten index files across several manifests. The rows
 * are kept small on purpose. A data file of a hundred thousand rows makes the parquet fetch-back
 * return rows under the wrong positions, which is a defect of the read path, independent of a
 * partial build; a bigger table here would fail for a reason this class does not test.
 */
class TestPartialBuildAtScale {

  /** Rows per data file. Ten files, so the index has ten of them to lose or keep. */
  private static final int ROWS_PER_FILE = 2_000;

  private static final int FILES = 10;

  private static final int ROWS = ROWS_PER_FILE * FILES;

  private static final int DIMENSION = 32;

  private static SparkSession spark;

  @BeforeAll
  static void startSpark(@TempDir Path warehouse) {
    VectorIndexTypes.register();
    spark =
        SparkSession.builder()
            .master("local[4]")
            .appName("partial-build-at-scale")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "8")
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
   * A gap over most of the table still answers the nearest rows correctly.
   *
   * <p>The index keeps one data file of ten, so nine tenths of the rows are only reachable through
   * the scan that fills the gap. If the plan believed the index covered them, the answer would be
   * the nearest row of one file rather than of the table.
   */
  @Test
  void aGapOverMostOfTheTableStillAnswersTheNearestRows() throws Exception {
    String table = "scale_gap";
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
    spark.sql(
        "CREATE TABLE local.db.t_" + table + " (id bigint, embedding array<double>) USING iceberg");

    // ten files of generated vectors, each file written by its own insert
    for (int file = 0; file < FILES; file++) {
      spark.sql(
          String.format(
              // a deterministic pseudo-random vector: rand() cannot be seeded per row, and the
              // rows have to be the same across runs for the scan and the index to be compared
              "INSERT INTO local.db.t_%s SELECT /*+ REPARTITION(1) */ global, "
                  + "transform(sequence(1, %d), x -> "
                  + "CAST(pmod(hash(global, x), 10007) AS double) / 10007.0) "
                  + "FROM (SELECT id, id + %d AS global FROM range(%d))",
              table, DIMENSION, (long) file * ROWS_PER_FILE, ROWS_PER_FILE));
    }

    // a copy without an index: once this table has one, the same query is rewritten into a search,
    // and a baseline that is rewritten too would agree with the index whatever it answered
    spark.sql(
        "CREATE TABLE local.db.t_"
            + table
            + "_ref USING iceberg AS SELECT * FROM local.db.t_"
            + table);
    double[] query = queryVector();
    List<Long> expected = nearestByScan(table + "_ref", query, 10);
    assertThat(expected).as("the table has the rows the test is about").hasSize(10);

    spark
        .sql(
            "CALL local.system.build_vector_index(table => 'db.t_"
                + table
                + "', column => 'embedding', dimension => "
                + DIMENSION
                + ", metric => 'l2', structure => 'flat', max_entries_per_index_file => 1)")
        .collectAsList();

    Table indexed = Spark3Util.loadIcebergTable(spark, "local.db.t_" + table);
    IndexMetadata metadata = VectorIndexes.findIndex(indexed, "embedding", "vector", null);
    assertThat(indexFilesOf(indexed, metadata.latestSnapshot()))
        .as("one index file per data file")
        .hasSize(FILES);

    // keep one index file of ten: nine tenths of the rows leave the index
    IndexSnapshot latest = metadata.latestSnapshot();
    IndexFile kept = indexFilesOf(indexed, latest).get(0);
    IndexSnapshotWriter writer = new IndexSnapshotWriter(indexed.io());
    String manifestList =
        writer.writeManifestList(metadata.location(), latest.snapshotId(), List.of(kept));
    IndexSnapshot partial = latest.toBuilder().indexManifestList(manifestList).build();
    new SidecarIndexCatalog(indexed.io(), indexed)
        .updateIndex(
            indexed.location(), metadata.indexUuid(), current -> current.replaceSnapshot(partial));
    indexed.refresh();

    long snapshot = indexed.currentSnapshot().snapshotId();
    Set<String> uncovered =
        new IndexReader(VectorIndexes.findIndex(indexed, "embedding", "vector", null), indexed)
            .uncoveredDataFiles(snapshot, IndexEntries.dataFiles(indexed, snapshot));
    assertThat(uncovered).as("nine of the ten data files are outside the index").hasSize(FILES - 1);

    List<Long> answered = idsOf(search(table, query, 10));
    assertThat(answered)
        .as("the nearest rows of the table, not of the one file the index kept")
        .containsExactlyElementsOf(expected);

    // and the gap is fillable: a refresh rebuilds the nine files and replaces the build
    spark.sql(
        "CALL local.system.refresh_vector_index(table => 'db.t_"
            + table
            + "', "
            + "column => 'embedding')");
    Table refreshed = Spark3Util.loadIcebergTable(spark, "local.db.t_" + table);
    IndexMetadata after = VectorIndexes.findIndex(refreshed, "embedding", "vector", null);
    assertThat(after.snapshots()).as("the gap-filling build replaced the partial one").hasSize(1);
    assertThat(
            new IndexReader(after, refreshed)
                .uncoveredDataFiles(
                    refreshed.currentSnapshot().snapshotId(),
                    IndexEntries.dataFiles(refreshed, refreshed.currentSnapshot().snapshotId())))
        .as("every data file is covered once the gap is filled")
        .isEmpty();
    assertThat(idsOf(search(table, query, 10)))
        .as("a completed index answers what the scan answers")
        .containsExactlyElementsOf(expected);
  }

  private double[] queryVector() {
    double[] query = new double[DIMENSION];
    for (int i = 0; i < DIMENSION; i++) {
      query[i] = 0.5d;
    }

    return query;
  }

  /**
   * The k nearest ids, computed from the table alone.
   *
   * <p>The distance comes from {@code vec_distance} rather than from an expression of the test's
   * own: the indexed answer is scored with it, and a test that computes the baseline a different
   * way compares two different questions. The call is made before an index exists, so nothing
   * rewrites it into a search.
   */
  private List<Long> nearestByScan(String table, double[] query, int k) {
    List<Row> rows =
        spark
            .sql(
                "SELECT id FROM local.db.t_"
                    + table
                    + " ORDER BY vec_distance(embedding, "
                    + queryArray(query)
                    + ") LIMIT "
                    + k)
            .collectAsList();
    return rows.stream().map(row -> row.getLong(0)).collect(Collectors.toList());
  }

  private static String queryArray(double[] query) {
    return java.util.Arrays.stream(toBoxed(query))
        .map(value -> value + "d")
        .collect(Collectors.joining(", ", "array(", ")"));
  }

  private static Double[] toBoxed(double[] values) {
    Double[] boxed = new Double[values.length];
    for (int i = 0; i < values.length; i++) {
      boxed[i] = values[i];
    }

    return boxed;
  }

  private List<Row> search(String table, double[] query, int k) {
    String values = queryArray(query);
    return spark
        .sql(
            "CALL local.system.vector_search(table => 'db.t_"
                + table
                + "', column => 'embedding', query => "
                + values
                + ", k => "
                + k
                + ")")
        .collectAsList();
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

  private static List<Long> idsOf(List<Row> hits) {
    return hits.stream().map(hit -> parseId(hit.getString(3))).collect(Collectors.toList());
  }

  private static long parseId(String rowJson) {
    int start = rowJson.indexOf("\"id\":") + 5;
    int end = rowJson.indexOf(',', start);
    return Long.parseLong(rowJson.substring(start, end == -1 ? rowJson.indexOf('}', start) : end));
  }
}
