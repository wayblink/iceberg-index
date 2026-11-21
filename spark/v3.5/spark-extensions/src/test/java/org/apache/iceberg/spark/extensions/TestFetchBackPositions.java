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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.index.spark.IndexEntries;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.Spark3Util;
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
}
