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
package org.apache.iceberg.index.spark;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A fetch-back turns its rows into JSON on the executors, and crosses the driver barrier once.
 *
 * <p>The rows a query resolves are the whole record, with every column the caller asked for, so
 * collecting them and then parallelising them again to serialise them crosses that barrier twice.
 * The JSON of a row is what a caller sees, so it has to stay byte for byte what the row-to-JSON
 * conversion produces: the field order, the nested values, the treatment of a null, and the
 * timestamp, decimal and binary renderings are all part of the contract.
 */
class TestRowsAtJson {
  private static SparkSession spark;
  private static final String RESOLVED = "rows_at_json";

  @BeforeAll
  static void startSpark(@TempDir Path warehouse) {
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("rows-at-json")
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

  @BeforeEach
  void createTable() {
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
    spark.sql("DROP TABLE IF EXISTS local.db." + RESOLVED);
    spark.sql(
        "CREATE TABLE local.db."
            + RESOLVED
            + " (id bigint, name string, amount decimal(9, 2), ts timestamp, payload binary,"
            + " tags array<string>, attrs map<string, int>, loc struct<city: string, zip: int>)"
            + " USING iceberg TBLPROPERTIES ('format-version' = '2',"
            + " 'write.delete.mode' = 'merge-on-read')");
    // one file with a single row, and one with a row that carries nulls next to a row a delete
    // removes: the absent row, the null values and the nested values are all part of the answer
    spark.sql(
        "INSERT INTO local.db."
            + RESOLVED
            + " SELECT /*+ COALESCE(1) */ 1L, 'a', CAST(1.25 AS decimal(9, 2)),"
            + " TIMESTAMP '2026-10-27 10:11:12.345678', CAST('abc' AS binary),"
            + " array('x', 'y'), map('k', 7), named_struct('city', 'Shanghai', 'zip', 200000)");
    spark.sql(
        "INSERT INTO local.db."
            + RESOLVED
            + " SELECT /*+ COALESCE(1) */ v.id, v.name, v.amount, v.ts, v.payload, v.tags, v.attrs,"
            + " v.loc FROM VALUES (2L, CAST('b' AS string), CAST(2.50 AS decimal(9, 2)),"
            + " TIMESTAMP '2026-01-02 03:04:05.123456', CAST('de' AS binary), array('z'),"
            + " map('m', 8), named_struct('city', 'Beijing', 'zip', 100000)),"
            + " (3L, CAST(NULL AS string), CAST(NULL AS decimal(9, 2)), CAST(NULL AS timestamp),"
            + " CAST(NULL AS binary), CAST(NULL AS array<string>), CAST(NULL AS map<string, int>),"
            + " CAST(NULL AS struct<city: string, zip: int>)) AS v(id, name, amount, ts, payload,"
            + " tags, attrs, loc)");
    spark.sql("DELETE FROM local.db." + RESOLVED + " WHERE id = 2");
  }

  @Test
  void theJsonOfAResolvedRowIsWhatTheRowToJsonConversionProduces() throws Exception {
    Table table = Spark3Util.loadIcebergTable(spark, "local.db." + RESOLVED);
    List<String> columns = allColumns();
    StructType readSchema =
        SparkSchemaUtil.convert(table.schema().select(columns.toArray(new String[0])));
    List<FileScanTask> tasks = IndexEntries.scanTasks(table, table.currentSnapshot().snapshotId());
    Map<String, java.util.Collection<Long>> positions = positionsOfDeleted(tasks);

    Map<String, Map<Long, String>> json =
        IndexEntries.rowsAtJson(spark, table, positions, columns, readSchema, tasks);
    Map<String, Map<Long, String>> expected =
        legacyJson(
            IndexEntries.rowsAt(spark, table, positions, columns, readSchema, tasks), readSchema);

    assertThat(json.keySet()).as("the files that answered").isEqualTo(expected.keySet());
    for (String dataFile : expected.keySet()) {
      assertThat(json.get(dataFile))
          .as("the JSON of every resolved row of %s", dataFile)
          .isEqualTo(expected.get(dataFile));
    }

    // the delete removed one row of the file that held two, so that file answers only the row
    // after it, which is the one whose other columns are null: the comparison above therefore
    // covers the null rendering and not only the populated columns. Which file that is, and where
    // inside it the surviving row sits, is read from the file rather than from the order the
    // answers come back in: another file also answers a single row.
    String partiallyDeletedFile = null;
    for (FileScanTask task : tasks) {
      if (task.file().recordCount() == 2) {
        partiallyDeletedFile = task.file().location().toString();
      }
    }

    assertThat(partiallyDeletedFile).as("the file that held two rows").isNotNull();
    Map<Long, String> partiallyDeleted = json.get(partiallyDeletedFile);
    assertThat(partiallyDeleted).as("the rows the delete left").hasSize(1);
    // the null columns are dropped exactly as the row-to-JSON conversion drops them, rather than
    // rendered as a null literal
    assertThat(partiallyDeleted.values())
        .as("the row the delete left")
        .allSatisfy(value -> assertThat(value).contains("\"id\":3").doesNotContain("\"name\""));
  }

  /**
   * The null policy of the JSON is the session's, not a choice this path makes: a query that keeps
   * null fields gets them from both conversions.
   */
  @Test
  void theJsonFollowsTheSessionsNullPolicy() throws Exception {
    Table table = Spark3Util.loadIcebergTable(spark, "local.db." + RESOLVED);
    List<String> columns = allColumns();
    StructType readSchema =
        SparkSchemaUtil.convert(table.schema().select(columns.toArray(new String[0])));
    List<FileScanTask> tasks = IndexEntries.scanTasks(table, table.currentSnapshot().snapshotId());
    Map<String, java.util.Collection<Long>> positions = positionsOfDeleted(tasks);

    boolean previous =
        Boolean.parseBoolean(spark.conf().get("spark.sql.jsonGenerator.ignoreNullFields", "true"));
    spark.conf().set("spark.sql.jsonGenerator.ignoreNullFields", "false");
    try {
      Map<String, Map<Long, String>> json =
          IndexEntries.rowsAtJson(spark, table, positions, columns, readSchema, tasks);
      Map<String, Map<Long, String>> expected =
          legacyJson(
              IndexEntries.rowsAt(spark, table, positions, columns, readSchema, tasks), readSchema);

      assertThat(json).as("the JSON with null fields kept").isEqualTo(expected);
      assertThat(json.values().stream().flatMap(byPosition -> byPosition.values().stream()))
          .as("a null column travels as a null literal")
          .anySatisfy(value -> assertThat(value).contains("\"name\":null"));
    } finally {
      spark.conf().set("spark.sql.jsonGenerator.ignoreNullFields", Boolean.toString(previous));
    }
  }

  /**
   * A table may have a column of its own named like the two the fetch-back carries next to the
   * row. Those travel outside the row, so the caller's column keeps its name and its value.
   */
  @Test
  void aColumnNamedLikeAnAuxiliaryColumnStillSerialises() throws Exception {
    spark.sql("DROP TABLE IF EXISTS local.db.auxiliary_names");
    spark.sql(
        "CREATE TABLE local.db.auxiliary_names (id bigint, pos bigint, data_file string)"
            + " USING iceberg");
    spark.sql(
        "INSERT INTO local.db.auxiliary_names SELECT /*+ COALESCE(1) */ 1L, 42L, 'mine'");

    assertSameJsonAsTheRowToJsonConversion("local.db.auxiliary_names");
  }

  /**
   * A column whose name contains a dot is a legal column. Serialising a resolved row reads the
   * schema, so the name is a JSON key rather than a path through nested values, and it is not
   * resolved by the query parser.
   */
  @Test
  void aColumnWhoseNameContainsADotStillSerialises() throws Exception {
    spark.sql("DROP TABLE IF EXISTS local.db.dotted_names");
    spark.sql(
        "CREATE TABLE local.db.dotted_names (`id` bigint, `a.b` string, `c d` string)"
            + " USING iceberg");
    spark.sql("INSERT INTO local.db.dotted_names SELECT /*+ COALESCE(1) */ 1L, 'dotted', 'spaced'");

    // the value of such a column is not what this test is about: a parquet writer escapes a dot in
    // a column name to `a_x2Eb`, so the fetch-back cannot match the escaped name in the file and
    // reads a null there. What the test pins is that serialising the row no longer goes through the
    // query parser, which is what used to reject the name outright.
    assertSameJsonAsTheRowToJsonConversion("local.db.dotted_names");
  }

  @Test
  void aFetchBackCrossesTheDriverBarrierOnce() throws Exception {
    Table table = Spark3Util.loadIcebergTable(spark, "local.db." + RESOLVED);
    List<String> columns = allColumns();
    StructType readSchema =
        SparkSchemaUtil.convert(table.schema().select(columns.toArray(new String[0])));
    List<FileScanTask> tasks = IndexEntries.scanTasks(table, table.currentSnapshot().snapshotId());
    Map<String, java.util.Collection<Long>> positions = positionsOfDeleted(tasks);

    int fetchBackJobs =
        jobsWhile(() -> IndexEntries.rowsAt(spark, table, positions, columns, readSchema, tasks));
    int jsonJobs =
        jobsWhile(
            () -> IndexEntries.rowsAtJson(spark, table, positions, columns, readSchema, tasks));
    int legacyJobs =
        jobsWhile(
            () -> {
              Map<String, Map<Long, Row>> rows =
                  IndexEntries.rowsAt(spark, table, positions, columns, readSchema, tasks);
              legacyJson(rows, readSchema);
            });

    // serialising the rows costs no pass of its own: it happens in the task that read them, so the
    // answer comes back as JSON and nothing is parallelised a second time. Doing it the other way
    // round costs one more job, which is the extra crossing this removes.
    assertThat(jsonJobs)
        .as("jobs to resolve and serialise the rows (fetch-back alone: %d)", fetchBackJobs)
        .isEqualTo(fetchBackJobs);
    assertThat(legacyJobs)
        .as(
            "jobs to collect the rows and then serialise them (%d for the fetch-back alone)",
            fetchBackJobs)
        .isGreaterThan(fetchBackJobs);
  }

  /** Both conversions of every row of a table, checked to agree. */
  private void assertSameJsonAsTheRowToJsonConversion(String tableName) throws Exception {
    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    List<String> columns = Lists.newArrayList();
    for (Types.NestedField field : table.schema().columns()) {
      columns.add(field.name());
    }

    StructType readSchema = SparkSchemaUtil.convert(table.schema());
    List<FileScanTask> tasks = IndexEntries.scanTasks(table, table.currentSnapshot().snapshotId());
    Map<String, java.util.Collection<Long>> positions = positionsOfDeleted(tasks);

    Map<String, Map<Long, String>> json =
        IndexEntries.rowsAtJson(spark, table, positions, columns, readSchema, tasks);
    Map<String, Map<Long, String>> expected =
        legacyJson(
            IndexEntries.rowsAt(spark, table, positions, columns, readSchema, tasks), readSchema);

    assertThat(json).as("the JSON of every row of %s", tableName).isEqualTo(expected);
    assertThat(json.values().stream().flatMap(byPosition -> byPosition.values().stream()))
        .as("the rows %s answered", tableName)
        .isNotEmpty();
  }

  private static List<String> allColumns() {
    return Lists.newArrayList("id", "name", "amount", "ts", "payload", "tags", "attrs", "loc");
  }

  /** What the row-to-JSON conversion produces, per file and then per position. */
  private static Map<String, Map<Long, String>> legacyJson(
      Map<String, Map<Long, Row>> rowsByFile, StructType readSchema) {
    Map<String, Map<Long, String>> jsonByFile = new HashMap<>();
    for (Map.Entry<String, Map<Long, Row>> entry : rowsByFile.entrySet()) {
      List<Map.Entry<Long, Row>> ordered = new ArrayList<>(entry.getValue().entrySet());
      ordered.sort(Map.Entry.comparingByKey());
      List<Row> rows = new ArrayList<>();
      for (Map.Entry<Long, Row> row : ordered) {
        rows.add(row.getValue());
      }

      List<String> json = spark.createDataFrame(rows, readSchema).toJSON().collectAsList();
      Map<Long, String> byPosition = new HashMap<>();
      for (int i = 0; i < ordered.size(); i++) {
        byPosition.put(ordered.get(i).getKey(), json.get(i));
      }

      jsonByFile.put(entry.getKey(), byPosition);
    }

    return jsonByFile;
  }

  /**
   * Every position of every data file, so the fetch-back answers every row of the fixture.
   *
   * <p>The positions come from the files rather than from the order the tasks are listed in: which
   * insert produced which file is not something a scan promises, and a position that belongs to
   * another file would ask for a row no file has.
   */
  private static Map<String, java.util.Collection<Long>> positionsOfDeleted(
      List<FileScanTask> tasks) {
    Map<String, java.util.Collection<Long>> positions = new HashMap<>();
    for (FileScanTask task : tasks) {
      List<Long> filePositions = new ArrayList<>();
      for (long position = 0; position < task.file().recordCount(); position++) {
        filePositions.add(position);
      }

      positions.put(task.file().location().toString(), filePositions);
    }

    return positions;
  }

  /** Counts the Spark jobs a piece of work starts. */
  private static int jobsWhile(Runnable work) {
    AtomicInteger jobs = new AtomicInteger();
    SparkListener listener =
        new SparkListener() {
          @Override
          public void onJobStart(SparkListenerJobStart jobStart) {
            jobs.incrementAndGet();
          }
        };

    spark.sparkContext().addSparkListener(listener);
    try {
      work.run();
    } finally {
      spark.sparkContext().removeSparkListener(listener);
    }

    return jobs.get();
  }
}
