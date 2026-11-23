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
import java.util.List;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.SerializableConfiguration;
import org.apache.iceberg.io.StorageProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import scala.Tuple2;

/**
 * A fetch-back of a file that a table imported reads its columns through the table's name mapping.
 *
 * <p>add_files imports a parquet file that another engine wrote, so the file carries no Iceberg
 * field ids and its columns can only be found by name, through the {@code
 * schema.name-mapping.default} the procedure stores on the table. A lookup resolves a subset of a
 * row's columns, and a reader that ignores the mapping binds the columns of the file by their
 * position instead: the requested column then answers another column's values.
 */
class TestFetchBackNameMapping {
  private static SparkSession spark;

  @BeforeAll
  static void startSpark(@TempDir Path warehouse) {
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("fetch-back-name-mapping")
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
  void anImportedFileAnswersTheColumnsTheLookupAskedFor() throws Exception {
    String tableName = "local.db.fetch_back_imported";
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
    spark.sql("DROP TABLE IF EXISTS " + tableName);
    spark.sql(String.format("CREATE TABLE %s (id bigint, body string) USING iceberg", tableName));

    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    String imported = table.location() + "/imported";

    // a plain parquet file: the columns of the table, none of its field ids, and in another
    // order than the table declares them, which is what makes the mapping the only way to find
    // them
    spark
        .range(0, 100)
        .coalesce(1)
        .selectExpr("concat('row-', id) as body", "id")
        .write()
        .parquet(imported);

    spark.sql(
        String.format(
            "CALL local.system.add_files(table => '%s', source_table => '`parquet`.`%s`')",
            tableName, imported));
    table.refresh();

    // the mapping the fetch-back has to read the imported file with
    assertThat(table.properties())
        .as("the name mapping add_files stored for the imported file")
        .containsKey(TableProperties.DEFAULT_NAME_MAPPING);

    List<FileScanTask> tasks = IndexEntries.scanTasks(table, table.currentSnapshot().snapshotId());
    assertThat(tasks).as("the data files of the table the lookup reads").hasSize(1);
    FileScanTask task = tasks.get(0);
    assertThat(task.file().format()).isEqualTo(FileFormat.PARQUET);
    assertThat(task.file().recordCount()).as("rows of the imported file").isEqualTo(100L);

    List<Long> asked = ImmutableList.of(3L, 7L, 41L);
    // one column of the row is asked for: without the mapping, a reader binds the columns of the
    // file by their ordinal against the field ids of the table, and the file's second column holds
    // id rather than body
    Schema projection = table.schema().select("body");
    SerializableConfiguration conf =
        new SerializableConfiguration(spark.sessionState().newHadoopConf());

    List<Tuple2<Row, Long>> rows =
        IndexEntries.liveRowsAt(
            table.io(),
            task,
            IndexEntries.SnapshotDeletes.none(),
            asked,
            projection,
            null,
            conf,
            StorageProperties.resolve(table.io(), table.properties(), conf.get()));

    List<Long> readPositions = Lists.newArrayList();
    for (Tuple2<Row, Long> entry : rows) {
      long position = entry._2();
      readPositions.add(position);
      assertThat(entry._1().<String>getAs("body"))
          .as("the body of the row read for position %d", position)
          .isEqualTo("row-" + position);
    }

    assertThat(readPositions)
        .as("the positions the lookup asked for")
        .containsExactlyElementsOf(asked);
  }
}
