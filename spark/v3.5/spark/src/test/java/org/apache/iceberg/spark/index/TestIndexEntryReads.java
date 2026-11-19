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
package org.apache.iceberg.spark.index;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.BooleanSupplier;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptingFileIO;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.index.spark.IndexEntries;
import org.apache.iceberg.index.vector.VectorIndexAdapter;
import org.apache.iceberg.index.vector.VectorIndexDefinition;
import org.apache.iceberg.index.vector.VectorIndexTypes;
import org.apache.iceberg.index.vector.VectorKernels;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.types.Types;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.util.LongAccumulator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

/**
 * The rows of an index group are read on a few streams at once, and still reach the index file in
 * data file order and, within a file, in position order, with the snapshot's deletes applied.
 *
 * <p>A group of avro or other non-parquet data files is read by the task that writes its index
 * file, on several streams at once; a group that holds a parquet file keeps the per-file read
 * through Spark's reader, because a task cannot start that reader for a file it was handed. The
 * tests that cover the multi-stream read therefore write avro tables, and the parquet tests cover
 * the fallback path.
 */
class TestIndexEntryReads extends TestBase {

  private static final String TABLE_NAME = "default.index_entry_reads";
  private static final int EMBEDDING_FIELD_ID = 2;

  @BeforeAll
  static void registerVectorIndex() {
    VectorIndexTypes.register();
    VectorKernels.registerDefaults();
  }

  @BeforeAll
  static void configureSparkCatalog() {
    spark
        .conf()
        .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog");
    spark.conf().set("spark.sql.catalog.spark_catalog.type", "hive");
  }

  @BeforeEach
  void setupHmsUri() {
    spark.sessionState().catalogManager().reset();
    spark.conf().set("spark.hadoop." + METASTOREURIS.varname, hiveConf.get(METASTOREURIS.varname));
    spark.conf().set("spark.sql.catalog.spark_catalog.uri", hiveConf.get(METASTOREURIS.varname));
  }

  @AfterEach
  void dropTable() {
    sql("DROP TABLE IF EXISTS %s", TABLE_NAME);
  }

  @Test
  void keepsTheDataFileAndPositionOrderOfTheRows() {
    sql("CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg", TABLE_NAME);
    sql("INSERT INTO %s VALUES (0, array(0.0, 1.0)), (1, array(1.0, 0.0))", TABLE_NAME);
    sql(
        "INSERT INTO %s VALUES (2, array(0.0, 0.5)), (3, array(0.5, 0.0)), (4, array(1.0, 1.0))",
        TABLE_NAME);
    sql(
        "INSERT INTO %s VALUES (5, array(0.25, 0.5)), (6, array(0.75, 0.25)), "
            + "(7, array(0.5, 0.5)), (8, array(0.1, 0.9))",
        TABLE_NAME);

    Table table = catalog.loadTable(TableIdentifier.parse(TABLE_NAME));
    List<FileScanTask> tasks = IndexEntries.scanTasks(table, table.currentSnapshot().snapshotId());
    assertThat(tasks).hasSizeGreaterThanOrEqualTo(3);

    List<Row> rows = entryRows(table, tasks);

    assertThat(rows).hasSize(9);
    assertThat(references(rows)).containsExactlyElementsOf(everyPosition(tasks));

    // the entry carries the vector of the row at that position, read the way a scan reads it
    VectorIndexAdapter adapter = adapter();
    for (Row row : rows) {
      assertThat(adapter.decodeVector(row.getAs(2)))
          .containsExactly(floats(vectorsByPosition(row.getString(0)).get(row.getLong(1))));
    }
  }

  @Test
  void readsTheFilesOfAGroupInPathOrderWhateverOrderTheScanPlannedThemIn() {
    sql(
        "CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg "
            + "TBLPROPERTIES ('write.format.default'='avro')",
        TABLE_NAME);
    sql("INSERT INTO %s VALUES (0, array(0.0, 1.0)), (1, array(1.0, 0.0))", TABLE_NAME);
    sql("INSERT INTO %s VALUES (2, array(0.5, 0.5)), (3, array(0.1, 0.9))", TABLE_NAME);
    sql("INSERT INTO %s VALUES (4, array(0.2, 0.8)), (5, array(0.9, 0.1))", TABLE_NAME);

    Table table = catalog.loadTable(TableIdentifier.parse(TABLE_NAME));
    List<FileScanTask> tasks = IndexEntries.scanTasks(table, table.currentSnapshot().snapshotId());
    assertThat(tasks).hasSizeGreaterThanOrEqualTo(3);

    List<FileScanTask> sorted = inBuildOrder(tasks);
    List<FileScanTask> reversed = Lists.newArrayList(sorted);
    Collections.reverse(reversed);

    List<Tuple2<String, Long>> expected = everyPosition(sorted);
    assertThat(references(entryRows(table, sorted)))
        .as("the rows of a group are read in data file path order")
        .isEqualTo(expected);
    assertThat(references(entryRows(table, reversed)))
        .as(
            "the order the scan planned the files in does not move the rows: the training sample is"
                + " every k-th row, so it would build another index")
        .isEqualTo(expected);
  }

  @Test
  void keepsTheOrderOfTheRowsOfAnotherFormat() {
    sql(
        "CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg "
            + "TBLPROPERTIES ('write.format.default'='avro')",
        TABLE_NAME);
    sql("INSERT INTO %s VALUES (0, array(0.0, 1.0)), (1, array(1.0, 0.0))", TABLE_NAME);
    sql("INSERT INTO %s VALUES (2, array(0.5, 0.5)), (3, array(0.1, 0.9))", TABLE_NAME);

    Table table = catalog.loadTable(TableIdentifier.parse(TABLE_NAME));
    List<FileScanTask> tasks = IndexEntries.scanTasks(table, table.currentSnapshot().snapshotId());
    assertThat(tasks).hasSizeGreaterThanOrEqualTo(2);

    assertEntriesMatchFiles(table, tasks, entryRows(table, tasks), Sets.newHashSet());
  }

  @Test
  void readsEveryChunkOfEveryFileOfAGroupInOrder() {
    sql(
        "CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg "
            + "TBLPROPERTIES ('write.format.default'='avro')",
        TABLE_NAME);
    // each file holds more than one chunk of the read (128 rows), so the read has to keep the order
    // of the chunks of a file and of the files of the group, not just of the first chunk of each
    appendRows(150, 0L);
    appendRows(200, 1000L);
    appendRows(130, 2000L);

    Table table = catalog.loadTable(TableIdentifier.parse(TABLE_NAME));
    List<FileScanTask> tasks = IndexEntries.scanTasks(table, table.currentSnapshot().snapshotId());
    assertThat(tasks).hasSize(3);
    for (FileScanTask task : tasks) {
      assertThat(task.file().recordCount())
          .as("the file %s spans several read chunks", task.file().location())
          .isGreaterThan(128L);
    }

    assertEntriesMatchFiles(table, tasks, entryRows(table, tasks), Sets.newHashSet());
  }

  @Test
  void stopsTheReadsOfAGroupWhenTheTaskThatConsumesThemFails() {
    sql(
        "CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg "
            + "TBLPROPERTIES ('write.format.default'='avro')",
        TABLE_NAME);
    appendRows(150, 0L);
    appendRows(200, 1000L);
    appendRows(130, 2000L);

    Table table = catalog.loadTable(TableIdentifier.parse(TABLE_NAME));
    List<FileScanTask> tasks = IndexEntries.scanTasks(table, table.currentSnapshot().snapshotId());
    assertThat(tasks).hasSize(3);

    // a read that a previous test drained has already stopped its readers: starting from no reader
    // thread makes the assertion below be about this task
    awaitTrue(() -> readerThreads().isEmpty(), 5_000L);

    // the task that consumes the rows fails before the read reaches its end, as an encoding failure
    // or a cancelled task does; the streams it started have to be stopped with the task rather than
    // left waiting on a queue nobody drains again
    JavaRDD<Row> entries = IndexEntries.entryRows(spark, table, tasks, "embedding", adapter());
    Throwable failure =
        catchThrowable(
            () ->
                entries.foreach(
                    (VoidFunction<Row>)
                        row -> {
                          throw new IllegalStateException("the consumer failed on its first row");
                        }));
    assertThat(failure).as("the failing task fails the job").isNotNull();
    assertThat(messages(failure)).anyMatch(message -> message.contains("the consumer failed"));

    awaitTrue(() -> readerThreads().isEmpty(), 5_000L);
    assertThat(readerThreads()).as("no reader thread outlives the failed task").isEmpty();
  }

  @Test
  void keepsTheDataFileOfTheRowsWhenAFileInTheMiddleHasNoLiveRows() {
    sql(
        "CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg "
            + "TBLPROPERTIES ('write.format.default'='avro', 'write.delete.mode'='merge-on-read')",
        TABLE_NAME);
    appendRows(150, 0L);
    appendRows(200, 1000L);
    appendRows(130, 2000L);

    // every row of the second file is deleted, so its stream is empty while the files after it
    // still have rows: the entries of those rows have to carry their own data file
    sql("DELETE FROM %s WHERE id >= 1000 AND id < 1200", TABLE_NAME);

    Table table = catalog.loadTable(TableIdentifier.parse(TABLE_NAME));
    List<FileScanTask> tasks = IndexEntries.scanTasks(table, table.currentSnapshot().snapshotId());
    assertThat(tasks).hasSize(3);

    String emptyFile = dataFileWithId(table, tasks, 1000L);
    List<FileScanTask> remaining = Lists.newArrayList();
    for (FileScanTask task : tasks) {
      if (!task.file().location().toString().equals(emptyFile)) {
        remaining.add(task);
      }
    }

    assertThat(remaining).hasSize(2);
    List<Row> rows = entryRows(table, tasks);
    List<Tuple2<String, Long>> expected = everyPosition(remaining);
    assertThat(rows).hasSize(expected.size());
    assertThat(references(rows)).containsExactlyElementsOf(expected);
    assertEntriesMatchFiles(table, tasks, rows, ids(1000L, 1200L));
  }

  @Test
  void leavesOutTheRowsOfAFileWrittenBeforeTheIndexedColumnWasAdded() {
    sql(
        "CREATE TABLE %s (id bigint) USING iceberg TBLPROPERTIES ('write.format.default'='avro')",
        TABLE_NAME);
    sql("INSERT INTO %s VALUES (0), (1), (2)", TABLE_NAME);
    // the file above has no column to index, so the read has to produce rows with a null value for
    // it rather than fail, and the entry loop leaves those rows out and counts them
    sql("ALTER TABLE %s ADD COLUMN embedding array<double>", TABLE_NAME);
    sql("INSERT INTO %s VALUES (3, array(0.1, 0.2))", TABLE_NAME);

    Table table = catalog.loadTable(TableIdentifier.parse(TABLE_NAME));
    List<FileScanTask> tasks = IndexEntries.scanTasks(table, table.currentSnapshot().snapshotId());
    LongAccumulator skipped = spark.sparkContext().longAccumulator();
    List<Row> rows =
        IndexEntries.entryRows(spark, table, tasks, "embedding", adapter(), skipped).collect();

    assertThat(skipped.value()).isEqualTo(3);
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).getLong(1)).isZero();
    assertThat(adapter().decodeVector(rows.get(0).getAs(2))).containsExactly(0.1f, 0.2f);
  }

  @Test
  void appliesTheDeleteFilesOfTheSnapshot() throws IOException {
    sql(
        "CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg "
            + "TBLPROPERTIES ('write.format.default'='avro', 'write.delete.mode'='merge-on-read')",
        TABLE_NAME);
    sql(
        "INSERT INTO %s VALUES (0, array(0.0, 1.0)), (1, array(1.0, 0.0)), (2, array(0.5, 0.5))",
        TABLE_NAME);
    sql("INSERT INTO %s VALUES (3, array(0.1, 0.2)), (4, array(0.2, 0.1))", TABLE_NAME);

    // one row is removed by a position delete file and another by an equality delete file
    sql("DELETE FROM %s WHERE id = 1", TABLE_NAME);
    Table table = catalog.loadTable(TableIdentifier.parse(TABLE_NAME));
    addEqualityDelete(table, 4L);
    table.refresh();

    List<FileScanTask> tasks = IndexEntries.scanTasks(table, table.currentSnapshot().snapshotId());
    assertThat(tasks).hasSizeGreaterThanOrEqualTo(2);
    assertThat(deleteKinds(tasks))
        .containsExactlyInAnyOrder(FileContent.POSITION_DELETES, FileContent.EQUALITY_DELETES);

    // a row a delete file removes leaves its position behind: rows after it are not renumbered,
    // because an entry answers with the row's place in the file
    assertEntriesMatchFiles(table, tasks, entryRows(table, tasks), Sets.newHashSet(1L, 4L));
  }

  @Test
  void leavesOutAndCountsTheRowsWithoutAValue() {
    sql(
        "CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg "
            + "TBLPROPERTIES ('write.format.default'='avro')",
        TABLE_NAME);
    sql(
        "INSERT INTO %s VALUES (0, array(0.0, 1.0)), (1, cast(null as array<double>)), "
            + "(2, array(0.5, 0.5))",
        TABLE_NAME);

    Table table = catalog.loadTable(TableIdentifier.parse(TABLE_NAME));
    List<FileScanTask> tasks = IndexEntries.scanTasks(table, table.currentSnapshot().snapshotId());
    LongAccumulator skipped = spark.sparkContext().longAccumulator();
    List<Row> rows =
        IndexEntries.entryRows(spark, table, tasks, "embedding", adapter(), skipped).collect();

    assertThat(skipped.value()).isEqualTo(1);
    assertEntriesMatchFiles(table, tasks, rows, Sets.newHashSet());
  }

  @Test
  void failsTheBuildWhenADataFileCannotBeRead() {
    sql(
        "CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg "
            + "TBLPROPERTIES ('write.format.default'='avro')",
        TABLE_NAME);
    sql("INSERT INTO %s VALUES (0, array(0.0, 1.0)), (1, array(1.0, 0.0))", TABLE_NAME);

    Table table = catalog.loadTable(TableIdentifier.parse(TABLE_NAME));
    List<FileScanTask> tasks = IndexEntries.scanTasks(table, table.currentSnapshot().snapshotId());
    assertThat(tasks).isNotEmpty();

    // a data file that is gone has to fail the read rather than produce a short group silently
    List<String> dataFiles = IndexEntries.dataFiles(table, table.currentSnapshot().snapshotId());
    assertThat(dataFiles).isNotEmpty();
    for (String dataFile : dataFiles) {
      table.io().deleteFile(dataFile);
    }

    // the files are read on several streams at once, so the failure that surfaces names one of the
    // missing files rather than in particular the first one
    Throwable failure = catchThrowable(() -> entryRows(table, tasks));
    assertThat(failure).as("reading a group whose data file is gone fails").isNotNull();
    assertThat(messages(failure))
        .as("the failure names a file it could not read")
        .anyMatch(message -> dataFiles.stream().anyMatch(message::contains));
  }

  private static List<String> messages(Throwable failure) {
    List<String> messages = Lists.newArrayList();
    for (Throwable current = failure; current != null; current = current.getCause()) {
      if (current.getMessage() != null) {
        messages.add(current.getMessage());
      }

      if (current.getCause() == current) {
        break;
      }
    }

    return messages;
  }

  /** The order the build reads a group's files in: by data file location. */
  private static List<FileScanTask> inBuildOrder(List<FileScanTask> tasks) {
    List<FileScanTask> ordered = Lists.newArrayList(tasks);
    ordered.sort(Comparator.comparing(task -> task.file().location().toString()));
    return ordered;
  }

  private List<Row> entryRows(Table table, List<FileScanTask> tasks) {
    LongAccumulator skipped = spark.sparkContext().longAccumulator();
    List<Row> rows =
        IndexEntries.entryRows(spark, table, tasks, "embedding", adapter(), skipped).collect();
    assertThat(skipped.value()).isZero();
    return rows;
  }

  /**
   * Appends rows whose ids start at the given id, forced to a single task so the append writes one
   * data file.
   */
  private void appendRows(int rows, long firstId) {
    sql(
        "INSERT INTO %s SELECT /*+ REPARTITION(1) */ cast(id + %d as bigint) AS id, "
            + "array(cast(id + %d as double), cast(id as double)) AS embedding FROM range(0, %d)",
        TABLE_NAME, firstId, firstId, rows);
  }

  /** The data file whose rows carry the given id. */
  private static String dataFileWithId(Table table, List<FileScanTask> tasks, long id) {
    for (FileScanTask task : tasks) {
      String dataFile = task.file().location().toString();
      for (Record record : records(table, dataFile, task.file().format())) {
        if ((Long) record.getField("id") == id) {
          return dataFile;
        }
      }
    }

    throw new IllegalArgumentException("No data file holds the row " + id);
  }

  private static Set<Long> ids(long from, long toExclusive) {
    Set<Long> ids = Sets.newHashSet();
    for (long id = from; id < toExclusive; id++) {
      ids.add(id);
    }

    return ids;
  }

  /** The reader threads of the group reads that are still running. */
  private static List<String> readerThreads() {
    List<String> names = Lists.newArrayList();
    for (Thread thread : Thread.getAllStackTraces().keySet()) {
      if (thread.getName().startsWith("index-group-read")) {
        names.add(thread.getName());
      }
    }

    return names;
  }

  private static void awaitTrue(BooleanSupplier condition, long waitMillis) {
    long deadline = System.currentTimeMillis() + waitMillis;
    while (!condition.getAsBoolean() && System.currentTimeMillis() < deadline) {
      try {
        Thread.sleep(10L);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  /**
   * The entries are the live rows of the data files, in file order and then in position order, and
   * each carries the vector of the row at that position. The rows of the given ids are the ones the
   * test removed.
   */
  private void assertEntriesMatchFiles(
      Table table, List<FileScanTask> tasks, List<Row> rows, Set<Long> removedIds) {
    VectorIndexAdapter adapter = adapter();
    int index = 0;
    for (FileScanTask task : inBuildOrder(tasks)) {
      String dataFile = task.file().location().toString();
      List<Record> records = records(table, dataFile, task.file().format());
      for (int position = 0; position < records.size(); position++) {
        Record record = records.get(position);
        if (removedIds.contains((Long) record.getField("id"))
            || record.getField("embedding") == null) {
          continue;
        }

        assertThat(index)
            .as("entries reached the row %s of %s", position, dataFile)
            .isLessThan(rows.size());
        Row row = rows.get(index);
        index += 1;
        assertThat(row.getString(0)).isEqualTo(dataFile);
        assertThat(row.getLong(1)).isEqualTo(position);
        assertThat(adapter.decodeVector(row.getAs(2)))
            .containsExactly(floats((List<?>) record.getField("embedding")));
      }
    }

    assertThat(rows).hasSize(index);
  }

  private static VectorIndexAdapter adapter() {
    return new VectorIndexAdapter(
        VectorIndexDefinition.builder()
            .fieldId(EMBEDDING_FIELD_ID)
            .dimension(2)
            .metric(VectorIndexDefinition.Metric.COSINE)
            .build());
  }

  private static List<Tuple2<String, Long>> everyPosition(List<FileScanTask> tasks) {
    List<Tuple2<String, Long>> references = Lists.newArrayList();
    for (FileScanTask task : inBuildOrder(tasks)) {
      String dataFile = task.file().location().toString();
      for (long position = 0; position < task.file().recordCount(); position++) {
        references.add(new Tuple2<>(dataFile, position));
      }
    }

    return references;
  }

  private static List<Tuple2<String, Long>> references(List<Row> rows) {
    List<Tuple2<String, Long>> references = Lists.newArrayListWithCapacity(rows.size());
    for (Row row : rows) {
      references.add(new Tuple2<>(row.getString(0), row.getLong(1)));
    }

    return references;
  }

  private static Set<FileContent> deleteKinds(List<FileScanTask> tasks) {
    Set<FileContent> kinds = Sets.newLinkedHashSet();
    for (FileScanTask task : tasks) {
      for (DeleteFile deleteFile : task.deletes()) {
        kinds.add(deleteFile.content());
      }
    }

    return kinds;
  }

  /**
   * The rows of a data file in file order, read on their own rather than through the entry code.
   */
  private static List<Record> records(Table table, String dataFile, FileFormat format) {
    List<Record> records = Lists.newArrayList();
    try (CloseableIterable<Record> rows =
        FormatModelRegistry.readBuilder(format, Record.class, table.io().newInputFile(dataFile))
            .project(table.schema())
            .build()) {
      for (Record record : rows) {
        records.add(record);
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read " + dataFile, e);
    }

    return records;
  }

  /** The vector of every row of a data file, by its position in the file. */
  private Map<Long, List<Double>> vectorsByPosition(String dataFile) {
    Map<Long, List<Double>> vectors = Maps.newHashMap();
    long position = 0;
    for (Row row : spark.read().parquet(dataFile).select("embedding").collectAsList()) {
      vectors.put(position, row.getList(0));
      position += 1;
    }

    return vectors;
  }

  private static float[] floats(List<?> values) {
    float[] floats = new float[values.size()];
    for (int i = 0; i < floats.length; i++) {
      floats[i] = ((Number) values.get(i)).floatValue();
    }

    return floats;
  }

  /**
   * Removes the row with the given id with an equality delete file, the kind a writer that cannot
   * name a row's position produces.
   */
  private static void addEqualityDelete(Table table, long id) throws IOException {
    Schema deleteRowSchema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    FileWriterFactory<Record> writers =
        new GenericFileWriterFactory.Builder(table)
            .equalityDeleteRowSchema(deleteRowSchema)
            .equalityFieldIds(new int[] {1})
            .build();

    String format =
        table
            .properties()
            .getOrDefault(
                TableProperties.DELETE_DEFAULT_FILE_FORMAT,
                table
                    .properties()
                    .getOrDefault(
                        TableProperties.DEFAULT_FILE_FORMAT,
                        TableProperties.DEFAULT_FILE_FORMAT_DEFAULT));
    EncryptedOutputFile output =
        EncryptingFileIO.combine(table.io(), table.encryption())
            .newEncryptingOutputFile(
                table.location()
                    + "/data/equality-delete-"
                    + UUID.randomUUID()
                    + "."
                    + format.toLowerCase(java.util.Locale.ROOT));

    EqualityDeleteWriter<Record> writer =
        writers.newEqualityDeleteWriter(output, table.spec(), null);
    try {
      writer.write(GenericRecord.create(deleteRowSchema).copy(Map.of("id", id)));
    } finally {
      writer.close();
    }

    table.newRowDelta().addDeletes(writer.toDeleteFile()).commit();
  }
}
