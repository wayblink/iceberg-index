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
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.index.spark.IndexEntries;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A lookup that names rows of a data file gets the rows it named, each paired with its position.
 *
 * <p>The fetch-back asks the format for the positions it hit and pairs what comes back with them.
 * The rows here carry their position in the id column and a vector derived from it, so a row whose
 * values come from two rows shows up as an id that does not match its own vector. That is what a
 * positional read that decodes the pages holding a wanted row does: the pages of the narrow column
 * and of the wide one do not start and end on the same rows, so one column's values come back with
 * another column's rows, in the right number, and nothing fails.
 */
class TestFetchBackPositions {
  private static final long ROWS = 100_000L;
  private static final int DIMENSION = 32;
  // positions of the same file, sparse and few: the shape a top-k lookup of a large file asks for
  private static final List<Long> POSITIONS = ImmutableList.of(16_316L, 32_232L, 75_669L);
  private static final List<String> COLUMNS = ImmutableList.of("id", "embedding");

  private static SparkSession spark;

  @BeforeAll
  static void startSpark(@TempDir Path warehouse) {
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("fetch-back-positions")
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
      spark = null;
    }
  }

  @Test
  void aSparseLookupOfALargeParquetFileReadsTheRowsItNamed() throws Exception {
    String tableName = "local.db.fetch_back_positions";
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
    spark.sql(
        String.format(
            "CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg", tableName));

    // one file, so every row asked for comes from the same reader, and the id of a row is its
    // position in that file
    Dataset<Row> rows =
        spark
            .range(0, ROWS)
            .coalesce(1)
            .selectExpr(
                "id",
                "transform(sequence(1, "
                    + DIMENSION
                    + "), d -> cast((id + d - 1) % 97 as double)) as embedding");
    rows.writeTo(tableName).append();

    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    List<FileScanTask> tasks = IndexEntries.scanTasks(table, table.currentSnapshot().snapshotId());
    assertThat(tasks).as("the data files of the table the lookup reads").hasSize(1);
    String dataFile = tasks.get(0).file().location();

    Map<String, Collection<Long>> asked = ImmutableMap.of(dataFile, POSITIONS);
    Map<String, Map<Long, Row>> read =
        IndexEntries.rowsAt(
            spark,
            table,
            table.currentSnapshot().snapshotId(),
            asked,
            COLUMNS,
            IndexEntries.sparkSchema(table, COLUMNS));

    assertThat(read.get(dataFile))
        .as("the positions the lookup asked for")
        .hasSize(POSITIONS.size());
    for (long position : POSITIONS) {
      Row row = read.get(dataFile).get(position);
      assertThat(row).as("the row at position %d", position).isNotNull();
      assertThat(row.<Long>getAs("id"))
          .as("the id of the row read for position %d", position)
          .isEqualTo(position);
      assertThat(row.<Double>getList(COLUMNS.indexOf("embedding")))
          .as("the vector of the row read for position %d", position)
          .containsExactly(vectorOf(position));
    }
  }

  /** The vector the fixture writes for a row, so a row read can be checked against its position. */
  private static Double[] vectorOf(long position) {
    Double[] vector = new Double[DIMENSION];
    for (int d = 0; d < DIMENSION; d++) {
      vector[d] = (double) ((position + d) % 97);
    }

    return vector;
  }

  /**
   * A lookup that names its columns in another order gets them under those names.
   *
   * <p>The two columns hold values a swap cannot be caught by: they have the same type, and the
   * numbers are different, so a row that answers one column with the other's value reads back as a
   * plain wrong number rather than as a type error.
   */
  @Test
  void aLookupThatNamesItsColumnsInAnotherOrderGetsThemUnderThoseNames() throws Exception {
    String tableName = "local.db.fetch_back_column_order";
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
    spark.sql(
        String.format(
            "CREATE TABLE %s (a bigint, b bigint, body string) USING iceberg", tableName));

    spark
        .range(0, 100)
        .coalesce(1)
        .selectExpr("id as a", "id * 10 as b", "concat('row-', id) as body")
        .writeTo(tableName)
        .append();

    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    List<FileScanTask> tasks = IndexEntries.scanTasks(table, table.currentSnapshot().snapshotId());
    String dataFile = tasks.get(0).file().location();
    List<Long> asked = ImmutableList.of(3L, 7L, 41L);

    // the order the caller asks for, which is not the order the table declares
    List<String> columns = ImmutableList.of("b", "a", "body");
    Map<String, Map<Long, Row>> read =
        IndexEntries.rowsAt(
            spark,
            table,
            table.currentSnapshot().snapshotId(),
            ImmutableMap.of(dataFile, asked),
            columns,
            IndexEntries.sparkSchema(table, columns));

    for (long position : asked) {
      Row row = read.get(dataFile).get(position);
      assertThat(row).as("the row at position %d", position).isNotNull();
      assertThat(row.<Long>getAs("b")).as("b of position %d", position).isEqualTo(position * 10);
      assertThat(row.<Long>getAs("a")).as("a of position %d", position).isEqualTo(position);
      assertThat(row.<String>getAs("body"))
          .as("body of position %d", position)
          .isEqualTo("row-" + position);
    }
  }

  /**
   * A lookup whose columns omit the equality-delete key still drops the rows that key removes.
   *
   * <p>The read has to carry the key to judge a row, so the row it builds has more columns than the
   * caller asked for; the caller's projection is applied by name afterwards. A key the caller did
   * not name is still a column of the read, and the row's schema has to describe it.
   */
  @Test
  void aLookupWithoutTheEqualityDeleteKeyDropsTheRowsThatKeyRemoves() throws Exception {
    String tableName = "local.db.fetch_back_delete_key";
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
    spark.sql(String.format("CREATE TABLE %s (id bigint, body string) USING iceberg", tableName));

    spark
        .range(0, 20)
        .coalesce(1)
        .selectExpr("id", "concat('row-', id) as body")
        .writeTo(tableName)
        .append();

    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    // row 5 is removed by a key the lookup below does not ask for
    table.newRowDelta().addDeletes(writeEqualityDelete(table, 5L)).commit();
    table.refresh();

    List<FileScanTask> tasks = IndexEntries.scanTasks(table, table.currentSnapshot().snapshotId());
    String dataFile = tasks.get(0).file().location();
    List<Long> asked = ImmutableList.of(4L, 5L, 6L);
    List<String> columns = ImmutableList.of("body");

    Map<String, Map<Long, Row>> read =
        IndexEntries.rowsAt(
            spark,
            table,
            table.currentSnapshot().snapshotId(),
            ImmutableMap.of(dataFile, asked),
            columns,
            IndexEntries.sparkSchema(table, columns));

    assertThat(read.get(dataFile))
        .as("the live rows among the ones the lookup asked for")
        .containsOnlyKeys(4L, 6L);
    assertThat(read.get(dataFile).get(4L).<String>getAs("body")).isEqualTo("row-4");
    assertThat(read.get(dataFile).get(6L).<String>getAs("body")).isEqualTo("row-6");
  }

  /**
   * A lookup that asks for positions of a file and gets none of them answers with a failure, not
   * with nothing.
   *
   * <p>Every row the lookup asked for can be gone (deleted after the index named it), and a
   * fetch-back that only answers with the rows it found cannot tell "the file is not part of the
   * answer" from "the file answered nothing": a caller that needs its rows has to be told the index
   * is stale.
   */
  @Test
  void aFetchBackThatFindsNoneOfTheRowsItWasAskedForFailsWhenTheCallerNeedsThem() throws Exception {
    String tableName = "local.db.fetch_back_all_gone";
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
    spark.sql(String.format("CREATE TABLE %s (id bigint, body string) USING iceberg", tableName));
    spark
        .range(0, 3)
        .coalesce(1)
        .selectExpr("id", "concat('row-', id) as body")
        .writeTo(tableName)
        .append();

    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    // every row of the one file is removed by equality deletes, so the fetch-back finds nothing
    org.apache.iceberg.RowDelta delta = table.newRowDelta();
    for (long id = 0; id < 3; id++) {
      delta.addDeletes(writeEqualityDelete(table, id));
    }

    delta.commit();
    table.refresh();

    List<FileScanTask> tasks = IndexEntries.scanTasks(table, table.currentSnapshot().snapshotId());
    String dataFile = tasks.get(0).file().location();
    Map<String, Collection<Long>> asked =
        ImmutableMap.<String, Collection<Long>>of(dataFile, ImmutableList.of(0L, 1L, 2L));

    // without the requirement the rows are gone and the answer is empty, which is the behaviour a
    // caller that can tolerate a lost row uses
    assertThat(
            IndexEntries.rowsAtRdd(
                    spark,
                    table,
                    asked,
                    ImmutableList.of("body"),
                    IndexEntries.sparkSchema(table, ImmutableList.of("body")),
                    tasks,
                    null)
                .count())
        .as("rows found for three positions whose rows are all gone")
        .isZero();

    // a caller whose answer is the rows has to see the failure
    org.apache.spark.api.java.JavaPairRDD<String, scala.Tuple2<Long, Row>> rows =
        IndexEntries.rowsAtRdd(
            spark,
            table,
            asked,
            ImmutableList.of("body"),
            IndexEntries.sparkSchema(table, ImmutableList.of("body")),
            tasks,
            "The index refers to a row the snapshot does not have");
    // a task failure reaches the caller wrapped, so the failure is matched by what it says
    assertThatThrownBy(rows::count)
        .hasStackTraceContaining("The index refers to a row the snapshot does not have")
        .hasStackTraceContaining(dataFile);
  }

  /** An equality delete file for one row, written the way another engine writes it. */
  private static DeleteFile writeEqualityDelete(Table table, long id) throws IOException {
    Schema keySchema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    EncryptedOutputFile out =
        EncryptedFiles.plainAsEncryptedOutput(
            table
                .io()
                .newOutputFile(table.location() + "/data/equality-delete-" + id + ".parquet"));
    EqualityDeleteWriter<Record> writer =
        FormatModelRegistry.equalityDeleteWriteBuilder(FileFormat.PARQUET, Record.class, out)
            .schema(keySchema)
            .spec(PartitionSpec.unpartitioned())
            .equalityFieldIds(keySchema.columns().get(0).fieldId())
            .build();
    try (writer) {
      writer.write(GenericRecord.create(keySchema).copy("id", id));
    }

    return writer.toDeleteFile();
  }
}
