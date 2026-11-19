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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
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
    for (String dataFile : dataFiles) {
      table.io().deleteFile(dataFile);
    }

    String gone = dataFiles.get(0).substring(dataFiles.get(0).lastIndexOf('/') + 1);
    Throwable failure = catchThrowable(() -> entryRows(table, tasks));
    assertThat(failure).as("reading a group whose data file is gone fails").isNotNull();
    assertThat(messages(failure))
        .as("the failure names the file it could not read")
        .anyMatch(message -> message.contains(gone));
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

  private List<Row> entryRows(Table table, List<FileScanTask> tasks) {
    LongAccumulator skipped = spark.sparkContext().longAccumulator();
    List<Row> rows =
        IndexEntries.entryRows(spark, table, tasks, "embedding", adapter(), skipped).collect();
    assertThat(skipped.value()).isZero();
    return rows;
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
    for (FileScanTask task : tasks) {
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
    for (FileScanTask task : tasks) {
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
