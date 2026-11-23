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
import org.apache.iceberg.Table;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.IndexReader;
import org.apache.iceberg.index.spark.IndexEntries;
import org.apache.iceberg.index.spark.VectorIndexes;
import org.apache.iceberg.index.vector.VectorIndexTypes;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests what a build does when it can index some of the data files and not others.
 *
 * <p>A build of a large table is expensive, and one data file that cannot be indexed used to cost
 * the whole run: the job failed, every index file the other groups wrote was discarded, and the
 * next attempt started from nothing. The build now publishes what the other groups wrote and names
 * the files it could not index, so the index answers for the table it covers and a refresh fills
 * the gap.
 */
class TestPartialIndexBuild {

  private static final String COLUMNS = "(id bigint, embedding array<double>) USING iceberg";

  private static SparkSession spark;

  @BeforeAll
  static void startSpark(@TempDir Path warehouse) {
    VectorIndexTypes.register();
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("partial-index-build")
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
    spark.stop();
  }

  @Test
  void aBuildThatCannotIndexEveryFilePublishesTheRest() throws Exception {
    String table = "db.partial_build";
    spark.sql(String.format("CREATE TABLE local.%s %s", table, COLUMNS));
    // one data file whose vectors match the requested dimension, and one whose vectors do not:
    // the second fails to encode, which is the failure this build has to survive
    spark.sql(String.format("INSERT INTO local.%s VALUES (1, array(1.0d, 0.0d))", table));
    spark.sql(String.format("INSERT INTO local.%s VALUES (2, array(1.0d, 0.0d, 0.0d))", table));

    // the build reports success: what it could index is published, and the rest is reported
    Row built =
        spark
            .sql(
                String.format(
                    "CALL local.system.build_vector_index("
                        + "table => '%s', column => 'embedding', dimension => 2, "
                        + "max_entries_per_index_file => 1)",
                    table))
            .head();
    assertThat(longValue(built, "index_files"))
        .as("one of the two data files is covered by the published index")
        .isEqualTo(1L);

    Table indexed = Spark3Util.loadIcebergTable(spark, "local." + table);
    long snapshot = indexed.currentSnapshot().snapshotId();
    IndexMetadata metadata = VectorIndexes.findIndex(indexed, "embedding", "vector", null);
    assertThat(
            new IndexReader(metadata, indexed)
                .uncoveredDataFiles(snapshot, IndexEntries.dataFiles(indexed, snapshot)))
        .as("the data file that could not be indexed is reported as not covered")
        .hasSize(1);

    // the report says PARTIAL rather than SUCCEEDED or FAILED: the index answers for part of the
    // table, and either of the other two would be wrong
    Row status = buildStatus(table);
    assertThat(stringValue(status, "state")).isEqualTo("PARTIAL");
    assertThat(longValue(status, "files_failed")).isEqualTo(1L);

    // check_indexes counts the same gap, which is what an operator looks at next
    Row checked =
        spark.sql(String.format("CALL local.system.check_indexes(table => '%s')", table)).head();
    assertThat(longValue(checked, "uncovered_data_files")).isEqualTo(1L);

    // A partial build leaves the table readable: the gap is a gap in the index, not damage to the
    // table. It does not make bad data answerable either — the file the index could not index is
    // scanned by a query, and that scan reads the vectors the index rejected, so the row still has
    // to be fixed or removed. The build buys back the hours, not the data.
    assertThat(spark.sql(String.format("SELECT count(*) FROM local.%s", table)).head().getLong(0))
        .as("the table the index only partly covers is still readable")
        .isEqualTo(2L);

    // a refresh is not the way to repair a gap whose cause is still there: the file that cannot be
    // indexed is still in the snapshot, so the strict path fails instead of quietly dropping it
    assertThatThrownBy(
            () ->
                spark.sql(
                    String.format(
                        "CALL local.system.refresh_vector_index("
                            + "table => '%s', column => 'embedding')",
                        table)))
        .as("a refresh that cannot index every file it was asked to fails, as it always did")
        .hasMessageContaining("3 dimensions");

    // removing the file that could not be indexed and refreshing covers the table completely
    spark.sql(String.format("DELETE FROM local.%s WHERE id = 2", table));
    spark.sql(
        String.format(
            "CALL local.system.refresh_vector_index(table => '%s', column => 'embedding')", table));

    Table refreshed = Spark3Util.loadIcebergTable(spark, "local." + table);
    long refreshedSnapshot = refreshed.currentSnapshot().snapshotId();
    assertThat(
            new IndexReader(
                    VectorIndexes.findIndex(refreshed, "embedding", "vector", null), refreshed)
                .uncoveredDataFiles(
                    refreshedSnapshot, IndexEntries.dataFiles(refreshed, refreshedSnapshot)))
        .as("the refresh fills the gap the partial build left")
        .isEmpty();
  }

  @Test
  void aFailedGroupCostsEveryFileItCovers() throws Exception {
    String table = "db.partial_group";
    spark.sql(String.format("CREATE TABLE local.%s %s", table, COLUMNS));
    // four data files in two groups of two: the second group holds one indexable file and one that
    // is not, and the failed group takes both with it
    spark.sql(String.format("INSERT INTO local.%s VALUES (1, array(1.0d, 0.0d))", table));
    spark.sql(String.format("INSERT INTO local.%s VALUES (2, array(0.0d, 1.0d))", table));
    spark.sql(String.format("INSERT INTO local.%s VALUES (3, array(1.0d, 1.0d))", table));
    spark.sql(String.format("INSERT INTO local.%s VALUES (4, array(0.0d, 0.0d, 1.0d))", table));

    Row built =
        spark
            .sql(
                String.format(
                    "CALL local.system.build_vector_index("
                        + "table => '%s', column => 'embedding', dimension => 2, "
                        + "max_entries_per_index_file => 2)",
                    table))
            .head();
    assertThat(longValue(built, "index_files"))
        .as("only the group that could be indexed is published")
        .isEqualTo(1L);

    Row status = buildStatus(table);
    assertThat(stringValue(status, "state")).isEqualTo("PARTIAL");
    assertThat(longValue(status, "files_failed"))
        .as("files_failed counts the groups that failed, not the data files they held")
        .isEqualTo(1L);

    Table indexed = Spark3Util.loadIcebergTable(spark, "local." + table);
    long snapshot = indexed.currentSnapshot().snapshotId();
    assertThat(
            new IndexReader(VectorIndexes.findIndex(indexed, "embedding", "vector", null), indexed)
                .uncoveredDataFiles(snapshot, IndexEntries.dataFiles(indexed, snapshot)))
        .as("the data files the failed group covered are the ones left uncovered")
        .hasSize(2);
  }

  @Test
  void aBuildThatIndexesNothingIsNotPublished() throws Exception {
    String table = "db.nothing_built";
    spark.sql(String.format("CREATE TABLE local.%s %s", table, COLUMNS));
    spark.sql(String.format("INSERT INTO local.%s VALUES (1, array(1.0d, 0.0d, 0.0d))", table));

    assertThatThrownBy(
            () ->
                spark.sql(
                    String.format(
                        "CALL local.system.build_vector_index("
                            + "table => '%s', column => 'embedding', dimension => 2)",
                        table)))
        .hasMessageContaining("could not be indexed");

    // an index with no snapshot would look like an index over an empty build, so the registration
    // is rolled back and the failure is what the column keeps
    assertThat(
            spark
                .sql(String.format("CALL local.system.list_indexes(table => '%s')", table))
                .collectAsList())
        .as("no index is registered for a build that wrote nothing")
        .isEmpty();
    assertThat(stringValue(buildStatus(table), "state")).isEqualTo("FAILED");
  }

  private static Row buildStatus(String table) {
    Dataset<Row> status =
        spark.sql(String.format("CALL local.system.index_build_status(table => '%s')", table));
    assertThat(status.collectAsList()).as("one report per index of the table").hasSize(1);
    return status.head();
  }

  private static long longValue(Row row, String column) {
    return row.getLong(row.fieldIndex(column));
  }

  private static String stringValue(Row row, String column) {
    return row.getString(row.fieldIndex(column));
  }
}
