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
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end verification of a Lance-backed Iceberg table through Spark SQL.
 *
 * <p>Proves that Lance can act as an Iceberg data file format while Iceberg keeps ownership of the
 * catalog, snapshots and delete/update semantics. Delete files stay Parquet, so the position delete
 * path is Iceberg's, not Lance's.
 *
 * <p>Run with {@code -DsparkVersions=3.5 -DscalaVersion=2.12}.
 */
class TestLanceFormatTable {

  private static SparkSession spark;

  @BeforeAll
  static void startSpark(@TempDir Path warehouse) {
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("lance-format-table")
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
  public void writesAndReadsALanceBackedIcebergTable() {
    String table = "local.db.lance_basic";
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
    spark.sql("DROP TABLE IF EXISTS " + table);
    spark.sql(
        "CREATE TABLE "
            + table
            + " (id bigint, name string, score double) USING iceberg "
            + "TBLPROPERTIES ('write.format.default' = 'lance')");
    spark.sql(
        "INSERT INTO "
            + table
            + " VALUES (1, 'alice', 95.5), (2, 'bob', 87.3), (3, 'charlie', 92.1)");

    assertThat(spark.sql("SELECT count(*) FROM " + table).head().getLong(0)).isEqualTo(3L);
    assertThat(spark.sql("SELECT id, name FROM " + table + " WHERE score > 90 ORDER BY id").collectAsList())
        .extracting(row -> row.getLong(0) + ":" + row.getString(1))
        .containsExactly("1:alice", "3:charlie");

    // the data file really is Lance, and Iceberg tracked it in its own manifests
    List<Row> files =
        spark.sql("SELECT file_path, record_count FROM " + table + ".files").collectAsList();
    assertThat(files).isNotEmpty();
    assertThat(files).allSatisfy(row -> assertThat(row.getString(0)).endsWith(".lance"));
    assertThat(files.stream().mapToLong(row -> row.getLong(1)).sum()).isEqualTo(3L);
  }

  @Test
  public void deleteUpdateAndMergeKeepIcebergSemantics() {
    String table = "local.db.lance_mutations";
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
    spark.sql("DROP TABLE IF EXISTS " + table);
    spark.sql(
        "CREATE TABLE "
            + table
            + " (id bigint, name string, score double) USING iceberg "
            + "TBLPROPERTIES ('write.format.default' = 'lance')");
    spark.sql(
        "INSERT INTO "
            + table
            + " VALUES (1, 'alice', 95.5), (2, 'bob', 87.3), (3, 'charlie', 92.1), (4, 'dave', 70.0)");

    // position delete written by Iceberg (parquet delete file), read through a Lance data file
    spark.sql("DELETE FROM " + table + " WHERE id = 2");
    assertThat(spark.sql("SELECT id FROM " + table + " ORDER BY id").collectAsList())
        .extracting(row -> row.getLong(0))
        .containsExactly(1L, 3L, 4L);

    spark.sql("UPDATE " + table + " SET score = 99.9 WHERE id = 3");
    assertThat(spark.sql("SELECT score FROM " + table + " WHERE id = 3").head().getDouble(0))
        .isEqualTo(99.9d);

    spark.sql(
        "MERGE INTO "
            + table
            + " t USING (SELECT 4L id, 'dave2' name, 71.0 score) s ON t.id = s.id"
            + " WHEN MATCHED THEN UPDATE SET t.name = s.name, t.score = s.score"
            + " WHEN NOT MATCHED THEN INSERT *");
    assertThat(spark.sql("SELECT name FROM " + table + " WHERE id = 4").head().getString(0))
        .isEqualTo("dave2");

    // copy-on-write deletes rewrite the data file, so it is still a Lance file owned by Iceberg
    List<Row> dataFiles =
        spark.sql("SELECT file_path FROM " + table + ".all_files WHERE content = 0").collectAsList();
    assertThat(dataFiles).isNotEmpty();
    assertThat(dataFiles).allSatisfy(row -> assertThat(row.getString(0)).endsWith(".lance"));
  }

  @Test
  public void mergeOnReadWritesIcebergDeleteFilesNotLanceFiles() {
    String table = "local.db.lance_mor";
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
    spark.sql("DROP TABLE IF EXISTS " + table);
    spark.sql(
        "CREATE TABLE "
            + table
            + " (id bigint, name string) USING iceberg TBLPROPERTIES ("
            + "'write.format.default' = 'lance', 'write.delete.mode' = 'merge-on-read')");
    spark.sql("INSERT INTO " + table + " VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    spark.sql("DELETE FROM " + table + " WHERE id = 2");

    assertThat(spark.sql("SELECT id FROM " + table + " ORDER BY id").collectAsList())
        .extracting(row -> row.getLong(0))
        .containsExactly(1L, 3L);

    // merge-on-read keeps Iceberg position delete files. Iceberg's write.delete.format.default
    // falls back to the table's data format, so the delete file is Lance here.
    List<Row> deletes =
        spark
            .sql("SELECT file_path FROM " + table + ".all_files WHERE content != 0")
            .collectAsList();
    assertThat(deletes).isNotEmpty();
    assertThat(deletes).allSatisfy(row -> assertThat(row.getString(0)).endsWith(".lance"));
  }

  @Test
  public void deleteFileFormatCanStayOnIcebergsDefaultWhileDataIsLance() {
    String table = "local.db.lance_mor_parquet_deletes";
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
    spark.sql("DROP TABLE IF EXISTS " + table);
    spark.sql(
        "CREATE TABLE "
            + table
            + " (id bigint, name string) USING iceberg TBLPROPERTIES ("
            + "'write.format.default' = 'lance',"
            + " 'write.delete.mode' = 'merge-on-read',"
            + " 'write.delete.format.default' = 'parquet')");
    spark.sql("INSERT INTO " + table + " VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    spark.sql("DELETE FROM " + table + " WHERE id = 2");

    assertThat(spark.sql("SELECT id FROM " + table + " ORDER BY id").collectAsList())
        .extracting(row -> row.getLong(0))
        .containsExactly(1L, 3L);

    List<Row> deletes =
        spark
            .sql("SELECT file_path FROM " + table + ".all_files WHERE content != 0")
            .collectAsList();
    assertThat(deletes).isNotEmpty();
    assertThat(deletes).allSatisfy(row -> assertThat(row.getString(0)).endsWith(".parquet"));
  }

  @Test
  public void timeTravelStillSeesTheOldSnapshot() {
    String table = "local.db.lance_time_travel";
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
    spark.sql("DROP TABLE IF EXISTS " + table);
    spark.sql(
        "CREATE TABLE "
            + table
            + " (id bigint) USING iceberg TBLPROPERTIES ('write.format.default' = 'lance')");
    spark.sql("INSERT INTO " + table + " VALUES (1), (2)");
    long firstSnapshot = spark.sql("SELECT snapshot_id FROM " + table + ".snapshots").head().getLong(0);
    spark.sql("INSERT INTO " + table + " VALUES (3)");

    assertThat(spark.sql("SELECT count(*) FROM " + table + " VERSION AS OF " + firstSnapshot).head().getLong(0))
        .isEqualTo(2L);
    assertThat(spark.sql("SELECT count(*) FROM " + table).head().getLong(0)).isEqualTo(3L);
  }

  @Test
  public void writesAndReadsAListColumn() {
    String table = "local.db.lance_list";
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
    spark.sql("DROP TABLE IF EXISTS " + table);
    spark.sql(
        "CREATE TABLE "
            + table
            + " (id bigint, vec array<float>) USING iceberg "
            + "TBLPROPERTIES ('write.format.default' = 'lance')");

    // enough rows to cross several Lance batches, with a vector-shaped list column
    spark.sql(
        "INSERT INTO "
            + table
            + " SELECT id, array(cast(id as float), cast(-id as float), 0.5f, 1.5f) FROM range(3000)");

    assertThat(spark.sql("SELECT count(*) FROM " + table).head().getLong(0)).isEqualTo(3000L);
    assertThat(spark.sql("SELECT sum(size(vec)) FROM " + table).head().getLong(0))
        .isEqualTo(12000L);
    assertThat(spark.sql("SELECT sum(vec[0]) FROM " + table).head().getDouble(0))
        .isEqualTo((2999L * 3000L) / 2.0);

    java.util.List<Row> rows =
        spark.sql("SELECT id, vec FROM " + table + " WHERE id < 3 ORDER BY id").collectAsList();
    assertThat(rows).hasSize(3);
    assertThat(rows.get(2).getList(1))
        .containsExactly(2.0f, -2.0f, 0.5f, 1.5f);

    // the list column round-trips through Lance, not a side channel
    List<Row> files =
        spark.sql("SELECT file_path FROM " + table + ".files").collectAsList();
    assertThat(files).allSatisfy(row -> assertThat(row.getString(0)).endsWith(".lance"));
  }
}
