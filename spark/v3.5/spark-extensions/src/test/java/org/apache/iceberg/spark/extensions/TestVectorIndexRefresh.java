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
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.Table;
import org.apache.iceberg.index.IndexFile;
import org.apache.iceberg.index.IndexManifest;
import org.apache.iceberg.index.IndexManifestList;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.IndexReader;
import org.apache.iceberg.index.IndexSnapshot;
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
 * Tests what a refresh does to an existing index snapshot.
 *
 * <p>A refresh carries every segment whose data files are still present and unchanged, builds
 * segments for the data files the parent does not cover, and rebuilds the segments of data files
 * whose contents changed — a delete file appeared for one, or a rewrite replaced it. Re-indexing
 * those is what keeps a query from answering with rows the table no longer holds; carrying the rest
 * unchanged is what keeps a refresh cheap. Merging segments and redistributing them are not part of
 * it.
 */
class TestVectorIndexRefresh {

  private static final String COLUMNS = "(id bigint, embedding array<double>) USING iceberg";

  private static SparkSession spark;

  @BeforeAll
  static void startSpark(@TempDir Path warehouse) {
    VectorIndexTypes.register();
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("refresh-appends")
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
  public void appendsSegmentsForNewFilesAndKeepsTheExistingOnes() {
    String table = "db.refresh_append";
    spark.sql(String.format("CREATE TABLE local.%s %s", table, COLUMNS));
    spark.sql(String.format("INSERT INTO local.%s VALUES (1, array(1.0d, 0.0d))", table));
    buildIndex(table);

    List<Object[]> before = listIndexes(table);
    long filesBefore = ((Number) before.get(0)[5]).longValue();
    long entriesBefore = ((Number) before.get(0)[6]).longValue();

    spark.sql(String.format("INSERT INTO local.%s VALUES (2, array(0.0d, 1.0d))", table));
    List<Object[]> refreshed = refresh(table);

    // the appended row is the only new entry: the refresh adds a segment for the new data file and
    // carries the one it already had. The counts below are the totals of the new snapshot.
    long refreshedFiles = ((Number) refreshed.get(0)[2]).longValue();
    long refreshedEntries = ((Number) refreshed.get(0)[3]).longValue();
    assertThat(refreshedEntries).isEqualTo(entriesBefore + 1L);
    assertThat(refreshedFiles).isEqualTo(filesBefore + 1L);

    List<Object[]> after = listIndexes(table);
    assertThat(((Number) after.get(0)[6]).longValue()).isEqualTo(refreshedEntries);
    assertThat(((Number) after.get(0)[5]).longValue()).isEqualTo(refreshedFiles);
    assertThat(((Number) after.get(0)[3]).longValue())
        .isNotEqualTo(((Number) before.get(0)[3]).longValue());

    // and the refreshed index finds the new row
    List<Row> hits =
        spark
            .sql(
                String.format(
                    "SELECT id FROM local.%s "
                        + "ORDER BY vec_distance(embedding, array(0.0d, 0.95d)) LIMIT 1",
                    table))
            .collectAsList();
    assertThat(hits.get(0).getLong(0)).isEqualTo(2L);
  }

  @Test
  public void rebuildsTheGroupWhoseDataFileGainedDeletes() throws Exception {
    String table = "db.refresh_deletes";
    // merge-on-read keeps the data file a delete touches, so this is the case a rewrite does not
    // produce: the file is still in the snapshot and its entries changed
    spark.sql(
        String.format(
            "CREATE TABLE local.%s %s TBLPROPERTIES "
                + "('format-version' = '2', 'write.delete.mode' = 'merge-on-read')",
            table, COLUMNS));
    spark.sql(
        String.format(
            "INSERT INTO local.%s VALUES (1, array(1.0d, 0.0d)), (2, array(0.0d, 1.0d))", table));
    buildIndex(table);

    List<Object[]> before = listIndexes(table);
    long filesBefore = ((Number) before.get(0)[5]).longValue();
    long entriesBefore = ((Number) before.get(0)[6]).longValue();

    // a delete changes the entries of a file the index already covers
    spark.sql(String.format("DELETE FROM local.%s WHERE id = 2", table));

    List<Object[]> refreshed = refresh(table);
    // the group that covers the changed data file is rebuilt, and only that group: the new
    // segment holds the rows the table still has, so the entry count drops and the file
    // count stays where it was
    assertThat(((Number) refreshed.get(0)[2]).longValue()).isEqualTo(filesBefore);
    assertThat(((Number) refreshed.get(0)[3]).longValue()).isEqualTo(entriesBefore - 1L);

    List<Object[]> after = listIndexes(table);
    assertThat(((Number) after.get(0)[6]).longValue()).isEqualTo(entriesBefore - 1L);
    assertThat(((Number) after.get(0)[5]).longValue()).isEqualTo(filesBefore);

    // the rebuilt segment does not answer with the deleted row any more, and the snapshot binds
    Table icebergTable = Spark3Util.loadIcebergTable(spark, "local." + table);
    long snapshot = icebergTable.currentSnapshot().snapshotId();
    assertThat(
            new IndexReader(
                    VectorIndexes.findIndex(icebergTable, "embedding", "vector", null),
                    icebergTable)
                .uncoveredDataFiles(snapshot, IndexEntries.dataFiles(icebergTable, snapshot)))
        .isEmpty();

    List<Row> hits =
        spark
            .sql(
                String.format(
                    "SELECT id FROM local.%s "
                        + "ORDER BY vec_distance(embedding, array(0.0d, 0.95d)) LIMIT 2",
                    table))
            .collectAsList();
    assertThat(hits).extracting(row -> row.getLong(0)).doesNotContain(2L);
  }

  @Test
  public void carriesUnchangedSegmentsThroughAnAppend() throws Exception {
    String table = "db.refresh_carry";
    spark.sql(String.format("CREATE TABLE local.%s %s", table, COLUMNS));
    spark.sql(String.format("INSERT INTO local.%s VALUES (1, array(1.0d, 0.0d))", table));
    buildIndex(table);

    Table icebergTable = Spark3Util.loadIcebergTable(spark, "local." + table);
    Set<String> before = indexFileLocations(icebergTable);

    spark.sql(String.format("INSERT INTO local.%s VALUES (2, array(0.0d, 1.0d))", table));
    refresh(table);

    Set<String> after = indexFileLocations(Spark3Util.loadIcebergTable(spark, "local." + table));
    // an append must not rewrite anything: every index file the parent had is still the file the
    // refreshed snapshot points at
    assertThat(after).containsAll(before);
  }

  private static Set<String> indexFileLocations(Table table) {
    IndexMetadata metadata = VectorIndexes.findIndex(table, "embedding", "vector", null);
    IndexSnapshot snapshot = metadata.latestSnapshot();
    Set<String> locations = new LinkedHashSet<>();
    try {
      IndexManifestList manifestList =
          IndexManifestList.read(table.io().newInputFile(snapshot.indexManifestList()));
      for (IndexManifestList.ManifestRef manifestRef : manifestList.manifests()) {
        for (IndexFile indexFile :
            IndexManifest.read(table.io().newInputFile(manifestRef.location())).indexFiles()) {
          locations.add(indexFile.location());
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    return locations;
  }

  private static void buildIndex(String table) {
    spark.sql(
        String.format(
            "CALL local.system.build_vector_index("
                + "table => '%s', column => 'embedding', dimension => 2)",
            table));
  }

  private static List<Object[]> refresh(String table) {
    return rows(
        spark.sql(
            String.format(
                "CALL local.system.refresh_vector_index(table => '%s', column => 'embedding')",
                table)));
  }

  private static List<Object[]> listIndexes(String table) {
    return rows(spark.sql(String.format("CALL local.system.list_indexes(table => '%s')", table)));
  }

  private static List<Object[]> rows(org.apache.spark.sql.Dataset<Row> dataset) {
    List<Object[]> rows = new java.util.ArrayList<>();
    for (Row row : dataset.collectAsList()) {
      Object[] values = new Object[row.size()];
      Arrays.setAll(values, row::get);
      rows.add(values);
    }

    return rows;
  }
}
