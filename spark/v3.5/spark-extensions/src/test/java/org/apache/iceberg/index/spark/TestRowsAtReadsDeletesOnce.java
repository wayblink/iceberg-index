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
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.InputFile;
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

    // the file that carries the delete has to be one of the three asked for: three files that
    // carry no delete read no delete file at all, and then the count below is zero whether the
    // deletes are read once or once per hit file. Which file the delete landed on is not something
    // a scan promises, so it is picked by looking for it rather than by taking the first tasks.
    List<FileScanTask> withDeletes = new ArrayList<>();
    List<FileScanTask> withoutDeletes = new ArrayList<>();
    for (FileScanTask task : IndexEntries.scanTasks(table, table.currentSnapshot().snapshotId())) {
      if (task.deletes().isEmpty()) {
        withoutDeletes.add(task);
      } else {
        withDeletes.add(task);
      }
    }
    assertThat(withDeletes).as("the fixture needs one data file carrying a delete").hasSize(1);
    assertThat(withoutDeletes)
        .as("the fixture needs two more data files that carry none")
        .hasSizeGreaterThanOrEqualTo(2);

    List<FileScanTask> hits = new ArrayList<>();
    hits.add(withDeletes.get(0));
    hits.addAll(withoutDeletes.subList(0, 2));

    // every position of every hit file is asked for, so the answer shows both that the deleted row
    // is gone and that its file still answers the row next to it
    Map<String, java.util.Collection<Long>> positions = new java.util.HashMap<>();
    for (FileScanTask task : hits) {
      List<Long> filePositions = new ArrayList<>();
      for (long position = 0; position < task.file().recordCount(); position++) {
        filePositions.add(position);
      }
      positions.put(task.file().location().toString(), filePositions);
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

    String deleteFileLocation = withDeletes.get(0).deletes().get(0).location();
    String deletedDataFile = withDeletes.get(0).file().location().toString();
    assertThat(CountingFileIO.opens(deleteFileLocation))
        .as("the delete file is read once for the whole fetch-back")
        .isEqualTo(1);

    // three files were asked for and three answered: the fetch-back did not drop one because it
    // carries a delete
    assertThat(rows.keySet()).as("the files that answered").isEqualTo(positions.keySet());
    // the file that carries the delete held two rows and answers one, which is the row the delete
    // did not remove
    assertThat(rows.get(deletedDataFile)).as("the rows the deleted file answered").hasSize(1);
    assertThat(rows.get(deletedDataFile).values())
        .as("the row the delete left")
        .allSatisfy(row -> assertThat(row.getLong(0)).isNotEqualTo(2L));
    // and the files without deletes answer both of their rows
    for (FileScanTask task : withoutDeletes.subList(0, 2)) {
      assertThat(rows.get(task.file().location().toString()))
          .as("the rows of a file without deletes")
          .hasSize(2);
    }
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
