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

import java.net.InetAddress;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.apache.spark.Dependency;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import scala.Tuple2;

/**
 * A fetch-back reads the files a lookup hit as one RDD, split into the number of tasks it asks for.
 *
 * <p>A lookup of a large table hits thousands of data files, and the planner is a single thread: a
 * partition built on the driver for every hit file, unioned one at a time, costs that thread a pass
 * over every file before a single row is read. The files are therefore the data of the RDD rather
 * than one RDD each, and the number of tasks comes from the fetch-back's own setting.
 *
 * <p>The shape is asserted without running a job: partitions and lineage are what the driver
 * builds.
 */
class TestFetchBackReadPlan {
  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
  private static final List<String> COLUMNS = Lists.newArrayList("id");
  // TestTables keeps one static metadata map per table name, so each fixture needs its own name.
  private static final AtomicInteger SEQUENCE = new AtomicInteger();

  private static SparkSession spark;

  @TempDir private Path temp;

  @BeforeAll
  static void startSpark() {
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("test-fetch-back-read-plan")
            .config("spark.driver.host", InetAddress.getLoopbackAddress().getHostAddress())
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
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
  void aFetchBackOfManyFilesIsOneRddWithOnePartitionPerFile() {
    Fixture fixture = fixture(8);

    JavaPairRDD<String, Tuple2<Long, Row>> rows = fetchBack(fixture);

    assertThat(rows.getNumPartitions())
        .as("tasks of a fetch-back that asks for no packing of %d files", fixture.tasks.size())
        .isEqualTo(fixture.tasks.size());
    assertThat(shape(rows.rdd()))
        .as("the most parents an RDD of the fetch-back has, and the number of RDDs it built")
        .isEqualTo(new Shape(1, 2));
  }

  @Test
  void theNumberOfTasksIsWhatTheFetchBackAsksFor() {
    Fixture fixture = fixture(6);

    String previous = System.getProperty("iceberg.index.fetch-back.tasks");
    try {
      System.setProperty("iceberg.index.fetch-back.tasks", "2");
      assertThat(fetchBack(fixture).getNumPartitions())
          .as("tasks of a fetch-back of %d files that asks for two", fixture.tasks.size())
          .isEqualTo(2);

      System.setProperty("iceberg.index.fetch-back.tasks", "64");
      assertThat(fetchBack(fixture).getNumPartitions())
          .as("tasks of a fetch-back that asks for more tasks than it has files")
          .isEqualTo(fixture.tasks.size());
    } finally {
      if (previous == null) {
        System.clearProperty("iceberg.index.fetch-back.tasks");
      } else {
        System.setProperty("iceberg.index.fetch-back.tasks", previous);
      }
    }
  }

  @Test
  void aFetchBackOfOneFileIsOneTask() {
    Fixture fixture = fixture(1);

    assertThat(fetchBack(fixture).getNumPartitions()).isEqualTo(1);
  }

  private JavaPairRDD<String, Tuple2<Long, Row>> fetchBack(Fixture fixture) {
    StructType readSchema = IndexEntries.sparkSchema(fixture.table, COLUMNS);
    return IndexEntries.liveRowsRdd(
        spark, fixture.table, fixture.positions, COLUMNS, readSchema, fixture.tasks);
  }

  /**
   * A table of the given number of data files, and rows asked for from each of them.
   *
   * <p>The files are Lance files: a fetch-back of them plans its tasks without opening anything, so
   * the shape it builds can be asserted without a table that holds data.
   */
  private Fixture fixture(int files) {
    String tableName = "fetch-back-plan-" + SEQUENCE.incrementAndGet();
    TestTables.TestTable table =
        TestTables.create(
            temp.toFile(),
            tableName,
            SCHEMA,
            PartitionSpec.unpartitioned(),
            SortOrder.unsorted(),
            2,
            new TestTables.TestTableOperations(
                tableName, temp.toFile(), new TestTables.LocalFileIO()));
    for (int i = 0; i < files; i++) {
      table
          .newAppend()
          .appendFile(
              DataFiles.builder(PartitionSpec.unpartitioned())
                  .withPath(temp.resolve("data-" + i + ".lance").toString())
                  .withFormat(FileFormat.LANCE)
                  .withFileSizeInBytes(10L)
                  .withRecordCount(4L)
                  .build())
          .commit();
    }

    List<FileScanTask> tasks = IndexEntries.scanTasks(table, table.currentSnapshot().snapshotId());
    // a caller can hand over a collection view, and the rows it wants come in whatever order it
    // collected them in
    Map<String, Set<Long>> positions = Maps.newLinkedHashMap();
    for (FileScanTask task : tasks) {
      positions.put(task.file().location().toString(), Sets.newLinkedHashSet(List.of(3L, 0L)));
    }

    return new Fixture(table, tasks, positions);
  }

  /**
   * The lineage of an RDD: the largest number of parents any of its RDDs has, which is one per
   * unioned branch, and how many RDDs the driver built at all.
   */
  private static Shape shape(RDD<?> rdd) {
    int largest = 0;
    int built = 0;
    Set<Integer> seen = Sets.newHashSet();
    Deque<RDD<?>> pending = new ArrayDeque<>();
    pending.add(rdd);
    while (!pending.isEmpty()) {
      RDD<?> next = pending.poll();
      if (!seen.add(next.id())) {
        continue;
      }

      built++;
      largest = Math.max(largest, next.dependencies().size());
      scala.collection.Iterator<Dependency<?>> parents = next.dependencies().iterator();
      while (parents.hasNext()) {
        pending.add(parents.next().rdd());
      }
    }

    return new Shape(largest, built);
  }

  private record Shape(int largestParentCount, int rddCount) {}

  private static final class Fixture {
    private final TestTables.TestTable table;
    private final List<FileScanTask> tasks;
    private final Map<String, Set<Long>> positions;

    private Fixture(
        TestTables.TestTable table, List<FileScanTask> tasks, Map<String, Set<Long>> positions) {
      this.table = table;
      this.tasks = tasks;
      this.positions = positions;
    }
  }
}
