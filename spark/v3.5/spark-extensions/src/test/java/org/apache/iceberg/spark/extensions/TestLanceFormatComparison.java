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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Measures what a Lance data file costs Iceberg's split-based scan planning: Lance files are not
 * byte-range splittable, so Iceberg plans one task per file where Parquet contributes one task per
 * row group. Also records the storage footprint of both encodings for the same rows.
 *
 * <p>The numbers are written to build/lance-poc-comparison.txt for the POC report.
 */
class TestLanceFormatComparison {

  private static final int ROWS = 200_000;

  private static SparkSession spark;
  private static Path report;

  @BeforeAll
  static void startSpark(@TempDir Path warehouse) {
    spark =
        SparkSession.builder()
            .master("local[4]")
            .appName("lance-format-comparison")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "4")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", warehouse.toUri().toString())
            .getOrCreate();
    report = Path.of("build", "lance-poc-comparison.txt");
  }

  @AfterAll
  static void stopSpark() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  public void compareLanceAndParquetScanPlanning() throws Exception {
    StringBuilder out = new StringBuilder();
    out.append("rows=").append(ROWS).append(" dim=").append(0).append('\n');

    for (String format : new String[] {"lance", "parquet"}) {
      String table = "local.db.compare_" + format;
      spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
      spark.sql("DROP TABLE IF EXISTS " + table);
      spark.sql(
          "CREATE TABLE "
              + table
              + " (id bigint, payload string, score double, flag boolean) USING iceberg TBLPROPERTIES ("
              + "'write.format.default' = '"
              + format
              + "', 'write.parquet.row-group-size-bytes' = '262144')");

      spark.sql(
          "INSERT INTO "
              + table
              + " SELECT id, concat('payload-', id, '-"
              + "x".repeat(40)
              + "'), cast(id as double) / 3.0, id % 2 = 0 FROM range("
              + ROWS
              + ")");

      org.apache.iceberg.Table icebergTable =
          org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark, table);
      long filesInScan = 0;
      try (org.apache.iceberg.io.CloseableIterable<org.apache.iceberg.FileScanTask> files =
          icebergTable.newScan().planFiles()) {
        for (org.apache.iceberg.FileScanTask ignored : files) {
          filesInScan++;
        }
      }
      long tasksInScan = 0;
      try (org.apache.iceberg.io.CloseableIterable<org.apache.iceberg.CombinedScanTask> tasks =
          icebergTable.newScan().planTasks()) {
        for (org.apache.iceberg.CombinedScanTask task : tasks) {
          tasksInScan += task.files().size();
        }
      }
      long countTasks =
          spark
              .sql("SELECT * FROM " + table)
              .queryExecution()
              .executedPlan()
              .execute()
              .getNumPartitions();

      long bytes =
          spark.sql("SELECT sum(file_size_in_bytes) FROM " + table + ".files").head().getLong(0);
      long files = spark.sql("SELECT count(*) FROM " + table + ".files").head().getLong(0);

      long countMs = timeMillis("SELECT count(*) FROM " + table);
      long fullScanMs =
          timeMillis("SELECT sum(id) FROM " + table + " WHERE payload LIKE 'payload-1%'");

      out.append(format)
          .append(": files=")
          .append(files)
          .append(" dataBytes=")
          .append(bytes)
          .append(" dataFiles=")
          .append(filesInScan)
          .append(" plannedTasks=")
          .append(tasksInScan)
          .append(" sparkScanPartitions=")
          .append(countTasks)
          .append(" countMs=")
          .append(countMs)
          .append(" fullScanMs=")
          .append(fullScanMs)
          .append('\n');

      long rowCount = spark.sql("SELECT count(*) FROM " + table).head().getLong(0);
      out.append(format).append(" rowCount=").append(rowCount).append('\n');
      if (rowCount != ROWS) {
        throw new IllegalStateException(
            format + " row count mismatch: " + rowCount + " != " + ROWS);
      }
    }

    List<Row> sizes =
        spark
            .sql(
                "SELECT count(*) FROM local.db.compare_lance.files f"
                    + " WHERE f.file_path LIKE '%.lance'")
            .collectAsList();
    out.append("lanceFileRows=").append(sizes.get(0).getLong(0)).append('\n');

    Files.createDirectories(report.getParent());
    Files.writeString(
        report, out.toString(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    System.out.println("[LANCE-POC]\n" + out);
  }

  private long timeMillis(String sql) {
    long best = Long.MAX_VALUE;
    for (int i = 0; i < 3; i++) {
      long start = System.nanoTime();
      spark.sql(sql).collectAsList();
      best = Math.min(best, (System.nanoTime() - start) / 1_000_000);
    }
    return best;
  }
}
