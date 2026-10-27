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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A fetch-back reads the delete files of a snapshot once, however many data files it reads.
 *
 * <p>The delete files belong to the fetch-back, not to the data file being read: the same set is
 * applied to every file of the snapshot, so reading them inside the loop over hit files makes a
 * query pay once per hit. That multiplier is invisible on a table without deletes, which is why it
 * survived a benchmark suite built on one; the count below is what makes it visible.
 */
class TestRowsAtReadsDeletesOnce {
  private static SparkSession spark;
  private static final String COUNTED = "rows_at_deletes";

  @BeforeAll
  static void startSpark(@TempDir Path warehouse) {
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("rows-at-deletes")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            // every read of a file goes through this, so the count below is the real number of
            // times the delete files were opened
            .config("spark.sql.catalog.local.io-impl", CountingFileIO.class.getName())
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
  void resetCounts() {
    CountingFileIO.reset();
  }

  @Test
  void aFetchBackOverThreeFilesReadsTheDeleteFileOnce() throws Exception {
    Table table = tableWithOnePositionDelete();

    // three data files are asked for, so a per-file read of the deletes opens the same file three
    // times
    List<FileScanTask> hits = new ArrayList<>();
    List<DeleteFile> deletes = new ArrayList<>();
    for (FileScanTask task : IndexEntries.scanTasks(table, table.currentSnapshot().snapshotId())) {
      if (hits.size() < 3) {
        hits.add(task);
      }
      deletes.addAll(task.deletes());
    }
    assertThat(hits).as("the fixture needs three data files with a hit each").hasSize(3);
    assertThat(deletes).as("the fixture needs one position delete file").hasSize(1);

    Map<String, java.util.Collection<Long>> positions = new java.util.HashMap<>();
    for (FileScanTask task : hits) {
      positions.put(task.file().location().toString(), List.of(0L));
    }

    Map<String, Map<Long, Row>> rows =
        IndexEntries.rowsAt(
            spark,
            table,
            positions,
            List.of("id", "embedding"),
            org.apache.iceberg.spark.SparkSchemaUtil.convert(
                table.schema().select("id", "embedding")),
            IndexEntries.scanTasks(table, table.currentSnapshot().snapshotId()));

    String deleteLocation = Iterables.getOnlyElement(deletes).location().toString();
    assertThat(CountingFileIO.opens(deleteLocation))
        .as("the delete file is read once for the whole fetch-back")
        .isEqualTo(1);

    // and the row it removes is still absent from the answer
    long absent = positions.keySet().stream().filter(file -> !rows.containsKey(file)).count();
    assertThat(absent).as("the deleted row's file still answered its other rows").isZero();
  }

  /**
   * A table whose rows live in separate data files, one of which a position delete removes a row
   * from.
   */
  private static Table tableWithOnePositionDelete() throws Exception {
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
    spark.sql(
        "CREATE TABLE local.db."
            + COUNTED
            + " (id bigint, embedding array<double>) USING iceberg "
            + "TBLPROPERTIES ('format-version' = '2', 'write.delete.mode' = 'merge-on-read', "
            + "'write.update.mode' = 'merge-on-read', 'write.merge.mode' = 'merge-on-read')");
    // two rows per data file, written as one file: deleting the only row of a file drops the file
    // instead of leaving a delete file, which is what this test counts. COALESCE(1) is what keeps
    // the two rows of an insert in one file on a two-core local session.
    for (int id = 1; id <= 8; id += 2) {
      spark.sql(
          "INSERT INTO local.db."
              + COUNTED
              + " SELECT /*+ COALESCE(1) */ v.id, array(CAST(v.id AS double), 0.0d) "
              + "FROM VALUES ("
              + id
              + "), ("
              + (id + 1)
              + ") AS v(id)");
    }
    spark.sql("DELETE FROM local.db." + COUNTED + " WHERE id = 2");
    return Spark3Util.loadIcebergTable(spark, "local.db." + COUNTED);
  }

  /** Counts how often each file is opened, so a repeated read can be told from a single one. */
  public static class CountingFileIO extends HadoopFileIO {
    private static final Map<String, AtomicInteger> OPENED = new ConcurrentHashMap<>();

    static void reset() {
      OPENED.clear();
    }

    static int opens(String location) {
      AtomicInteger count = OPENED.get(location);
      return count == null ? 0 : count.get();
    }

    @Override
    public InputFile newInputFile(String path) {
      OPENED.computeIfAbsent(path, key -> new AtomicInteger()).incrementAndGet();
      return super.newInputFile(path);
    }
  }
}
