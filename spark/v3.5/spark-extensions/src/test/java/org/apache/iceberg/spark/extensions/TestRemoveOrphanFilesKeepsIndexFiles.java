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
import java.nio.file.Path;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.iceberg.Table;
import org.apache.iceberg.index.vector.VectorIndexTypes;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Orphan file cleanup on a table whose location names its scheme.
 *
 * <p>Index files live in a sidecar tree under the table location and are referenced by index
 * metadata rather than table metadata, so a cleanup that reads only table metadata sees every one
 * of them as an orphan. Excluding them compares the table location against the paths a directory
 * listing hands back, and those are written differently: the location carries the scheme the user
 * wrote, the listing returns what the filesystem reports. A comparison that does not put the two in
 * the same form excludes nothing, and then the cleanup deletes a live index.
 */
class TestRemoveOrphanFilesKeepsIndexFiles {
  private static SparkSession spark;

  @BeforeAll
  static void startSpark(@TempDir Path warehouse) {
    // the warehouse is a URI, which is what puts the scheme into the table location
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("remove-orphan-files-index")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", warehouse.toUri().toString())
            .getOrCreate();
    VectorIndexTypes.register();
  }

  @AfterAll
  static void stopSpark() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  void removeOrphanFilesKeepsTheFilesOfTheLiveIndex()
      throws IOException, ParseException, NoSuchTableException {
    spark.sql(
        "CREATE TABLE local.db.uri_orphans (id bigint, embedding array<double>) USING iceberg");
    spark.sql(
        "INSERT INTO local.db.uri_orphans "
            + "SELECT id, array(CAST(id AS double), CAST(10 - id AS double)) FROM range(0, 10)");
    spark.sql(
        "CALL local.system.build_vector_index(table => 'db.uri_orphans', column => 'embedding', "
            + "dimension => 2, metric => 'euclidean', structure => 'IVF', quantizer => 'RABITQ')");

    List<Row> before = nearest();

    // the procedure refuses an interval under 24 hours, so the index files are aged instead: they
    // are then the only files the cleanup has to decide about, which is what this test is about
    Table table = Spark3Util.loadIcebergTable(spark, "local.db.uri_orphans");
    age(table.location());

    List<Row> deleted =
        spark
            .sql(
                "CALL local.system.remove_orphan_files(table => 'db.uri_orphans', "
                    + "older_than => TIMESTAMP '2021-01-01 00:00:00')")
            .collectAsList();

    assertThat(deleted)
        .as("files of the live index reported as orphans: %s", deleted)
        .noneMatch(row -> row.getString(0).contains("/indexes/"));

    // the index is still whole: its metadata can be listed and it still answers the query
    assertThat(
            spark.sql("CALL local.system.list_indexes(table => 'db.uri_orphans')").collectAsList())
        .hasSize(1);
    assertThat(nearest()).isEqualTo(before);
  }

  /** Makes every file of the index tree look old enough for the cleanup to consider it. */
  private static void age(String tableLocation) throws IOException {
    org.apache.hadoop.fs.Path indexes = new org.apache.hadoop.fs.Path(tableLocation + "/indexes");
    FileSystem fs = indexes.getFileSystem(new Configuration());
    RemoteIterator<LocatedFileStatus> files = fs.listFiles(indexes, true);
    while (files.hasNext()) {
      fs.setTimes(files.next().getPath(), 1_600_000_000_000L, -1L);
    }
  }

  private static List<Row> nearest() {
    return spark
        .sql(
            "SELECT id FROM local.db.uri_orphans "
                + "ORDER BY vec_distance(embedding, array(0.0d, 10.0d)) LIMIT 3")
        .collectAsList();
  }
}
