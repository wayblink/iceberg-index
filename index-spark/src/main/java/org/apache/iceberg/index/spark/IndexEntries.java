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

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.data.IdentityPartitionConverters;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.formats.ReadBuilder;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hadoop.SerializableConfiguration;
import org.apache.iceberg.index.IndexAdapter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.StorageProperties;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PartitionUtil;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.LongAccumulator;
import org.apache.spark.util.TaskCompletionListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/**
 * Reads the vector entries of a table snapshot: one entry per live row, carrying its data file, its
 * position within that file and the encoded vector.
 *
 * <p>Data files are read directly as parquet and the snapshot's delete files are applied here: rows
 * removed by position delete files and rows matching equality delete files are not indexed. The
 * recorded position is the row's ordinal in its data file, which is what row resolution relies on.
 * Together this makes an index snapshot equivalent to the table snapshot it is bound to, deletes
 * included.
 *
 * <p>The pipeline is index-type neutral: the column value is handed to {@link
 * IndexAdapter#encodeEntryValue(Object)}, so any index type works without engine changes.
 */
public final class IndexEntries {

  private static final Logger LOG = LoggerFactory.getLogger(IndexEntries.class);

  public static final String DATA_FILE_COLUMN = "data_file";
  public static final String POSITION_COLUMN = "pos";
  public static final String VECTOR_COLUMN = "vector";

  private static final String JSON_COLUMN = "row_json";
  private static final String ROW_COLUMN = "resolved_row";

  public static final StructType SCHEMA =
      new StructType()
          .add(DATA_FILE_COLUMN, DataTypes.StringType, false)
          .add(POSITION_COLUMN, DataTypes.LongType, false)
          .add(VECTOR_COLUMN, DataTypes.BinaryType, false);

  private static final String DELETE_FILE_PATH_COLUMN = "file_path";
  private static final String DELETE_POSITION_COLUMN = "pos";
  private static final char NULL_MARKER = '\u0000';
  private static final char VALUE_SEPARATOR = '\u0001';

  // How many data files of one group are read at once. Four streams took the read of a group of 13
  // Lance files from 54.5 to 122.7 MiB/s on the machine the P4 spike ran on; eight streams added
  // nothing while holding twice the connections and memory, so four is the number to read with.
  private static final int READ_STREAMS = 4;
  // The rows read ahead of the entry loop, per stream: a queue of eight chunks of 128 rows bounds
  // what a group read holds at once to four streams of that, rather than a whole file of each.
  private static final int READ_CHUNKS_PER_STREAM = 8;
  private static final int READ_CHUNK_ROWS = 128;

  private IndexEntries() {}

  /**
   * Builds the entry dataset for the given source table snapshot.
   *
   * @param spark the Spark session
   * @param table the Iceberg table to index
   * @param snapshotId the source table snapshot to read; its delete files are applied
   * @param column the name of the indexed column
   * @param adapter the adapter that encodes column values into entry payloads
   */
  public static Dataset<Row> entries(
      SparkSession spark, Table table, long snapshotId, String column, IndexAdapter adapter) {
    JavaRDD<Row> rows = entryRows(spark, table, scanTasks(table, snapshotId), column, adapter);
    return rows == null
        ? spark.createDataFrame(Lists.newArrayList(), SCHEMA)
        : spark.createDataFrame(rows, SCHEMA);
  }

  /**
   * The index entries of the given data files, as rows of the entry schema.
   *
   * <p>Unlike {@link #entries} this does not union a whole snapshot into one dataset without saying
   * how: the rows of one group of data files are kept together, so the caller's task reads the
   * group whose index file it writes and the vectors of that group never cross the cluster. Within
   * the group the rows are in data file order and, within a file, in position order. Returns null
   * when there are no data files to read.
   */
  public static JavaRDD<Row> entryRows(
      SparkSession spark,
      Table table,
      java.util.Collection<FileScanTask> tasks,
      String column,
      IndexAdapter adapter) {
    return entryRows(spark, table, tasks, column, adapter, null);
  }

  /**
   * The same, reporting the rows it left out.
   *
   * <p>A row the adapter cannot hold is left out rather than failing the build: a vector index has
   * nothing to store for a null vector, so such a row is skipped and counted, while an index type
   * that records an absence still gets the row. A value that is present but cannot be indexed, a
   * wrong dimension for instance, is still an error about the data.
   *
   * @param skipped counts the rows left out because their value is null, which may be null
   */
  public static JavaRDD<Row> entryRows(
      SparkSession spark,
      Table table,
      java.util.Collection<FileScanTask> tasks,
      String column,
      IndexAdapter adapter,
      LongAccumulator skipped) {
    Schema tableSchema = table.schema();
    List<FileScanTask> taskList = Lists.newArrayList(tasks);
    if (taskList.isEmpty()) {
      return null;
    }

    // The rows of a group are read in data file order, and that order has to be the same on every
    // build: the training sample is every k-th row, so a scan that planned its files in another
    // order would sample other vectors from the same table and write another index. Grouping
    // already sorts the files it packs into a group, so sorting here is what makes the read match.
    taskList.sort(Comparator.comparing(task -> task.file().location().toString()));

    SnapshotDeletes deletes = SnapshotDeletes.read(spark, table, taskList, tableSchema);
    // one configuration for the whole group: each stream of the read would otherwise carry its own
    // copy of the Hadoop configuration into the stage's task binary
    SerializableConfiguration conf =
        new SerializableConfiguration(spark.sessionState().newHadoopConf());
    // the settings a format that opens the file itself needs, resolved once for the whole group:
    // the table's properties win over the catalog's, which win over the Hadoop configuration's
    Map<String, String> storageProperties =
        StorageProperties.resolve(table.io(), table.properties(), conf.get());

    // the data read needs the indexed column plus the columns referenced by equality deletes
    Set<String> columnsToRead = Sets.newLinkedHashSet();
    columnsToRead.add(column);
    columnsToRead.addAll(deletes.keyColumns());
    List<String> readColumns = Lists.newArrayList(columnsToRead);

    // One partition per group, with the rows read by the task that consumes them: a group is one
    // index file, so its rows have to reach one task, and a task is one thread. Handing each data
    // file a partition of its own and merging them with coalesce(1) instead left that one thread
    // reading the group's files one after another, waiting on the slow request of each file in turn
    // rather than overlapping them.
    if (readsInTask(taskList)) {
      // the read carries the columns in the order the entries index them, so a row's value can be
      // found by its position in the row: selecting the columns from the table schema instead puts
      // them in the table's order, which differs as soon as the indexed column is not the first of
      // the columns a group is read with (equality deletes add their keys to the read)
      Schema projection = projectionOf(tableSchema, readColumns);
      StructType rowSchema = toSparkSchema(projection);
      return JavaSparkContext.fromSparkContext(spark.sparkContext())
          .parallelize(Collections.singletonList(taskList), 1)
          .flatMap(
              (FlatMapFunction<List<FileScanTask>, Row>)
                  files -> {
                    CloseableIterator<Row> rows =
                        entryRows(
                            table.io(),
                            files,
                            readColumns,
                            projection,
                            rowSchema,
                            readColumns.indexOf(column),
                            adapter.skipsNullValues(),
                            adapter,
                            deletes,
                            storageProperties,
                            skipped);
                    // a task that is cancelled or that fails stops consuming the rows before the
                    // read reaches its end, and the reader threads it started would then wait on a
                    // queue nobody drains again. The listener runs when the task ends, however it
                    // ends, so the streams are closed even though the caller never sees the end of
                    // the rows; a read that does reach its end has already closed itself.
                    TaskContext taskContext = TaskContext.get();
                    if (taskContext != null) {
                      taskContext.addTaskCompletionListener(
                          (TaskCompletionListener) context -> closeQuietly(rows));
                    }

                    return rows;
                  });
    }

    return sparkReadRows(
        spark,
        table,
        taskList,
        column,
        readColumns,
        adapter,
        deletes,
        conf,
        storageProperties,
        skipped);
  }

  /** The projection of the given columns, in the order they are read in. */
  private static Schema projectionOf(Schema tableSchema, List<String> columns) {
    List<Types.NestedField> fields = Lists.newArrayListWithCapacity(columns.size());
    for (String column : columns) {
      Types.NestedField field = tableSchema.findField(column);
      Preconditions.checkArgument(field != null, "Column is not in the table schema: %s", column);
      fields.add(field);
    }

    return new Schema(fields);
  }

  /**
   * Whether one task can read the files of the group itself.
   *
   * <p>Every format but parquet is already read through the Iceberg format model, in the task, so
   * reading several files of a group at once reads exactly what the per-file read read. Parquet
   * keeps Spark's own reader: a task cannot start one for a data file it was handed, and moving
   * parquet to the format model to gain the concurrency would change the reader a build reads its
   * data with for a speedup that was measured on Lance files. A group that holds a parquet file
   * keeps the per-file read.
   */
  private static boolean readsInTask(List<FileScanTask> tasks) {
    for (FileScanTask task : tasks) {
      if (task.file().format() == FileFormat.PARQUET) {
        return false;
      }
    }

    return true;
  }

  /**
   * The entry rows of a group whose files are read by Spark's own readers.
   *
   * <p>One partition per data file, merged by the caller: a parquet file is read by the engine's
   * reader, which a task has to be given a partition to run on.
   */
  private static JavaRDD<Row> sparkReadRows(
      SparkSession spark,
      Table table,
      List<FileScanTask> tasks,
      String column,
      List<String> readColumns,
      IndexAdapter adapter,
      SnapshotDeletes deletes,
      SerializableConfiguration conf,
      Map<String, String> storageProperties,
      LongAccumulator skipped) {
    Schema tableSchema = table.schema();
    StructType readSchema = sparkSchema(table, readColumns);
    int columnIndex = readColumns.indexOf(column);
    // only an index type that cannot represent a null asks for the row to be left out: a text index
    // records a null document as a document with no tokens, and dropping the row would change the
    // corpus statistics its scores are computed from
    boolean skipNulls = adapter.skipsNullValues();

    JavaRDD<Row> rows = null;
    for (FileScanTask task : tasks) {
      String dataFile = task.file().location().toString();
      JavaRDD<Row> fileRows =
          liveRowsOf(
                  spark,
                  tableSchema,
                  task,
                  readColumns,
                  null,
                  readSchema,
                  deletes,
                  conf,
                  storageProperties)
              .filter(
                  (Function<Tuple2<Long, Row>, Boolean>)
                      entry -> !skipNulls || hasValue(entry._2(), columnIndex, skipped))
              .mapToPair(
                  (PairFunction<Tuple2<Long, Row>, Long, Row>)
                      entry ->
                          new Tuple2<>(
                              entry._1(),
                              RowFactory.create(
                                  dataFile,
                                  entry._1(),
                                  adapter.encodeEntryValue(columnValue(entry._2(), columnIndex)))))
              .values();

      rows = rows == null ? fileRows : rows.union(fileRows);
    }

    return rows;
  }

  /**
   * The entry rows of the data files of one group, read here on a few streams at once.
   *
   * <p>The rows come back in data file order and, within a file, in position order, which is what
   * the index entries and the list of files an index file covers are written in.
   *
   * <p>The value of the indexed column is encoded on the calling thread rather than on the reading
   * threads: an adapter is handed the rows of one stream at a time, as it was when the files of a
   * group were read one after another.
   */
  private static CloseableIterator<Row> entryRows(
      FileIO io,
      List<FileScanTask> files,
      List<String> readColumns,
      Schema projection,
      StructType rowSchema,
      int columnIndex,
      boolean skipNulls,
      IndexAdapter adapter,
      SnapshotDeletes deletes,
      Map<String, String> storageProperties,
      LongAccumulator skipped) {
    List<String> dataFiles = Lists.newArrayListWithCapacity(files.size());
    List<OrderedParallelReads.Stream<Tuple2<Long, Row>>> streams =
        Lists.newArrayListWithCapacity(files.size());
    for (FileScanTask task : files) {
      String dataFile = task.file().location().toString();
      dataFiles.add(dataFile);
      // the deletes belong to the snapshot rather than to the file being read, so the arrays are
      // looked up once here and closed over by the stream of that file
      long[] deletedPositions = deletes.deletedPositions(dataFile);
      List<EqualityKeySet> keySets = deletes.keySets();
      streams.add(
          () ->
              liveRowsOfFile(
                  io, task, projection, rowSchema, deletedPositions, keySets, storageProperties));
    }

    OrderedParallelReads<Tuple2<Long, Row>> rows =
        OrderedParallelReads.of(streams, READ_STREAMS, READ_CHUNKS_PER_STREAM, READ_CHUNK_ROWS);
    return new EntryRows(rows, dataFiles, columnIndex, skipNulls, adapter, skipped);
  }

  /** Closes a group read that is no longer wanted; the caller is already on its way out. */
  private static void closeQuietly(Closeable closeable) {
    if (closeable != null) {
      try {
        closeable.close();
      } catch (IOException e) {
        // the read is abandoned at the end of a task: a close that fails there cannot be reported
        // without masking the failure that ended the task
      }
    }
  }

  /**
   * The live rows of one data file, in file order, each with its position in the file.
   *
   * <p>The rows are read as they are asked for: a group read on several streams holds a chunk of
   * each file it is reading rather than a whole file of each. The read carries the columns the
   * entries need — the indexed column and the equality-delete keys — so no projection follows.
   */
  private static CloseableIterator<Tuple2<Long, Row>> liveRowsOfFile(
      FileIO io,
      FileScanTask task,
      Schema projection,
      StructType rowSchema,
      long[] deletedPositions,
      List<EqualityKeySet> keySets,
      Map<String, String> storageProperties) {
    String dataFile = task.file().location().toString();
    try {
      return new LiveRows(
          openRowsWithFormat(
              io.newInputFile(task.file()),
              task.file().format(),
              projection,
              rowSchema,
              storageProperties),
          deletedPositions,
          keySets);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read " + dataFile, e);
    }
  }

  /**
   * The rows of one group's data files as index entries.
   *
   * <p>The rows are pulled from the streams in file order and encoded here, one at a time: the
   * reading threads only read. A failure stops the streams before it is rethrown, because they are
   * waiting on a queue that nobody would drain again, and {@link #close()} stops them when the
   * caller stops asking for rows before the end, as a task that is cancelled does.
   */
  private static final class EntryRows implements CloseableIterator<Row> {
    private final OrderedParallelReads<Tuple2<Long, Row>> rows;
    private final List<String> dataFiles;
    private final int columnIndex;
    private final boolean skipNulls;
    private final IndexAdapter adapter;
    private final LongAccumulator skipped;
    private Row next;
    private boolean ready;

    private EntryRows(
        OrderedParallelReads<Tuple2<Long, Row>> rows,
        List<String> dataFiles,
        int columnIndex,
        boolean skipNulls,
        IndexAdapter adapter,
        LongAccumulator skipped) {
      this.rows = rows;
      this.dataFiles = dataFiles;
      this.columnIndex = columnIndex;
      this.skipNulls = skipNulls;
      this.adapter = adapter;
      this.skipped = skipped;
    }

    @Override
    public boolean hasNext() {
      return ready || advance();
    }

    @Override
    public Row next() {
      if (!hasNext()) {
        throw new NoSuchElementException("No more entries to read");
      }

      ready = false;
      Row row = next;
      next = null;
      return row;
    }

    @Override
    public void close() {
      rows.close();
    }

    private boolean advance() {
      try {
        while (rows.hasNext()) {
          Tuple2<Long, Row> entry = rows.next();
          Row row = entry._2();
          // only an index type that cannot represent a null asks for the row to be left out: a text
          // index records a null document as a document with no tokens, and dropping the row would
          // change the corpus statistics its scores are computed from
          if (skipNulls && !hasValue(row, columnIndex, skipped)) {
            continue;
          }

          next =
              RowFactory.create(
                  dataFiles.get(rows.currentStream()),
                  entry._1(),
                  adapter.encodeEntryValue(columnValue(row, columnIndex)));
          ready = true;
          return true;
        }

        return false;
      } catch (RuntimeException | Error e) {
        rows.close();
        throw e;
      }
    }
  }

  /**
   * The rows of one data file that its delete files leave live, each with its position.
   *
   * <p>The position is the row's ordinal in the file, counted as the rows stream past, so the rows
   * a delete file removes still take up their position.
   */
  private static final class LiveRows implements CloseableIterator<Tuple2<Long, Row>> {
    private final CloseableIterator<Tuple2<Long, Row>> rows;
    private final long[] deletedPositions;
    private final List<EqualityKeySet> keySets;
    private Tuple2<Long, Row> next;

    private LiveRows(
        CloseableIterator<Tuple2<Long, Row>> rows,
        long[] deletedPositions,
        List<EqualityKeySet> keySets) {
      this.rows = rows;
      this.deletedPositions = deletedPositions;
      this.keySets = keySets;
    }

    @Override
    public boolean hasNext() {
      if (next != null) {
        return true;
      }

      while (rows.hasNext()) {
        Tuple2<Long, Row> entry = rows.next();
        if (isLive(entry._1(), entry._2(), null, deletedPositions, keySets)) {
          next = entry;
          return true;
        }
      }

      return false;
    }

    @Override
    public Tuple2<Long, Row> next() {
      if (!hasNext()) {
        throw new NoSuchElementException("No more live rows to read");
      }

      Tuple2<Long, Row> row = next;
      next = null;
      return row;
    }

    @Override
    public void close() throws IOException {
      rows.close();
    }
  }

  /** The delete files of the tasks being read, read once. */
  private static SnapshotDeletes deletesFor(
      SparkSession spark, List<FileScanTask> tasks, Table table) {
    return SnapshotDeletes.read(spark, table, tasks, table.schema());
  }

  /**
   * Reads the live rows of a table snapshot at the given positions, keyed by data file and then by
   * row position. A data file that is not in the map is not read at all; the delete files of the
   * snapshot are applied, so positions of removed rows are absent from the result.
   *
   * @param spark the Spark session
   * @param table the Iceberg table to read
   * @param snapshotId the table snapshot to read; its delete files are applied
   * @param positionsByFile the row positions to read, per data file
   * @param columns the columns to return, in this order
   * @param readSchema the schema to read the data files with, or null to infer it from the files
   */
  public static Map<String, Map<Long, Row>> rowsAt(
      SparkSession spark,
      Table table,
      long snapshotId,
      Map<String, ? extends Collection<Long>> positionsByFile,
      List<String> columns,
      StructType readSchema) {
    return rowsAt(spark, table, positionsByFile, columns, readSchema, scanTasks(table, snapshotId));
  }

  /**
   * Reads the live rows of a snapshot at the given positions, from scan tasks that were already
   * planned. Planning a snapshot reads its manifests, so a query that already planned it should
   * pass the tasks on instead of paying for a second pass.
   */
  public static Map<String, Map<Long, Row>> rowsAt(
      SparkSession spark,
      Table table,
      Map<String, ? extends Collection<Long>> positionsByFile,
      List<String> columns,
      StructType readSchema,
      List<FileScanTask> scanTasks) {
    List<FileScanTask> tasks = tasksAt(positionsByFile, scanTasks);
    Map<String, Map<Long, Row>> rowsByFile = Maps.newLinkedHashMap();
    JavaPairRDD<String, Tuple2<Long, Row>> rows =
        liveRowsRdd(spark, table, positionsByFile, columns, readSchema, tasks);
    if (rows == null) {
      return rowsByFile;
    }

    for (Tuple2<String, Tuple2<Long, Row>> entry : rows.collect()) {
      rowsByFile
          .computeIfAbsent(entry._1(), key -> Maps.newLinkedHashMap())
          .put(entry._2()._1(), entry._2()._2());
    }

    return rowsByFile;
  }

  /**
   * Reads the live rows of a snapshot at the given positions as JSON, keyed by data file and then
   * by row position.
   *
   * <p>This is the same fetch-back as {@link #rowsAt}, with the JSON of a row produced on the
   * executor that read it. A lookup resolves a whole record per hit, so collecting the records and
   * then parallelising them again to serialise them moves the same records across the driver
   * barrier twice; serialising them where they are read moves them once.
   */
  public static Map<String, Map<Long, String>> rowsAtJson(
      SparkSession spark,
      Table table,
      Map<String, ? extends Collection<Long>> positionsByFile,
      List<String> columns,
      StructType readSchema,
      List<FileScanTask> scanTasks) {
    List<FileScanTask> tasks = tasksAt(positionsByFile, scanTasks);
    Map<String, Map<Long, String>> jsonByFile = Maps.newLinkedHashMap();
    JavaPairRDD<String, Tuple2<Long, Row>> rows =
        liveRowsRdd(spark, table, positionsByFile, columns, readSchema, tasks);
    if (rows == null) {
      return jsonByFile;
    }

    // the row travels as one value of its own type rather than as one column per name: its JSON
    // then comes from the schema the read was given, so no column name of the row is resolved by
    // the query parser and none of them collides with the file or the position. The position
    // travels with the row, so nothing depends on the order of the answers.
    Dataset<Row> json =
        spark
            .createDataFrame(
                rows.map(
                    (Function<Tuple2<String, Tuple2<Long, Row>>, Row>)
                        entry -> RowFactory.create(entry._1(), entry._2()._1(), entry._2()._2())),
                schemaOf(columns, readSchema))
            .select(
                functions.col(DATA_FILE_COLUMN),
                functions.col(POSITION_COLUMN),
                functions.to_json(functions.col(ROW_COLUMN)).as(JSON_COLUMN));

    for (Row row : json.collectAsList()) {
      jsonByFile
          .computeIfAbsent(row.getString(0), key -> Maps.newLinkedHashMap())
          .put(row.getLong(1), row.getString(2));
    }

    return jsonByFile;
  }

  /** The scan tasks a fetch-back reads: the ones whose file is asked for a row of. */
  private static List<FileScanTask> tasksAt(
      Map<String, ? extends Collection<Long>> positionsByFile, List<FileScanTask> scanTasks) {
    List<FileScanTask> tasks = Lists.newArrayList();
    for (FileScanTask task : scanTasks) {
      if (positionsByFile.containsKey(task.file().location().toString())) {
        tasks.add(task);
      }
    }

    return tasks;
  }

  /**
   * The live rows of the given files, paired with their data file and position, as one RDD. Null
   * when there is nothing to read.
   *
   * <p>The files are the data of the RDD rather than one RDD each. A lookup of a large table hits
   * thousands of data files, and the driver is a single thread: building a partition for every hit
   * file there and unioning them costs that thread a pass over every file before a single row is
   * read, and every unioned branch carries its own copy of the settings the read needs. Here the
   * driver hands over the list of what to read, the settings every file is read with are broadcast
   * once, and each task reads the files it was given.
   */
  static JavaPairRDD<String, Tuple2<Long, Row>> liveRowsRdd(
      SparkSession spark,
      Table table,
      Map<String, ? extends Collection<Long>> positionsByFile,
      List<String> columns,
      StructType readSchema,
      List<FileScanTask> tasks) {
    if (tasks.isEmpty()) {
      return null;
    }

    JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());

    // the configuration, the delete files and the storage settings belong to the fetch-back
    // rather than to a file: broadcasting them once keeps a task binary from carrying its own copy
    // of them, which across thousands of tasks is megabytes
    Broadcast<FetchPlan> plan = jsc.broadcast(fetchPlan(spark, table, tasks, columns, readSchema));

    // a caller may hand over a collection view, such as a map's key set, which Spark cannot ship to
    // an executor; copying it here keeps that from surfacing as a task serialization failure
    List<FileFetch> files = Lists.newArrayListWithCapacity(tasks.size());
    for (FileScanTask task : tasks) {
      String dataFile = task.file().location().toString();
      Collection<Long> positions = positionsByFile.get(dataFile);
      files.add(
          new FileFetch(
              dataFile,
              task.file().format(),
              positions == null ? null : Lists.newArrayList(positions)));
    }

    // how many tasks the fetch-back uses packs the files it hit into that many tasks; it does not
    // decide how often a file is opened, which is once per file that is read
    return jsc.parallelize(files, readTaskCount(files.size()))
        .flatMapToPair(
            (PairFlatMapFunction<FileFetch, String, Tuple2<Long, Row>>)
                fetch -> {
                  List<Tuple2<String, Tuple2<Long, Row>>> rows = Lists.newArrayList();
                  for (Tuple2<Long, Row> row : fetch.liveRows(plan.value())) {
                    rows.add(new Tuple2<>(fetch.dataFile, row));
                  }

                  return rows.iterator();
                });
  }

  /**
   * What every file of one fetch-back is read with, built once and broadcast once.
   *
   * <p>The Hadoop configuration alone carries tens of thousands of entries, and the delete files
   * are read before the fetch-back starts: neither is per-file state, so neither travels as part of
   * a task.
   */
  private static final class FetchPlan implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    private final Schema projection;
    private final List<String> readColumns;
    private final List<String> columns;
    private final StructType rowSchema;
    private final SerializableConfiguration conf;
    private final SnapshotDeletes deletes;
    private final Map<String, String> storageProperties;

    private FetchPlan(
        Schema projection,
        List<String> readColumns,
        List<String> columns,
        StructType rowSchema,
        SerializableConfiguration conf,
        SnapshotDeletes deletes,
        Map<String, String> storageProperties) {
      this.projection = projection;
      this.readColumns = readColumns;
      this.columns = columns;
      this.rowSchema = rowSchema;
      this.conf = conf;
      this.deletes = deletes;
      this.storageProperties = storageProperties;
    }
  }

  /** The plan of a fetch-back that reads the given files into the given columns. */
  private static FetchPlan fetchPlan(
      SparkSession spark,
      Table table,
      List<FileScanTask> tasks,
      List<String> columns,
      StructType readSchema) {
    // one serializable configuration for the whole fetch-back: building it copies the session's
    // Hadoop configuration, which is not something to do once per data file
    SerializableConfiguration conf =
        new SerializableConfiguration(spark.sessionState().newHadoopConf());

    // the deletes belong to the fetch-back, not to the file being read: the same delete files apply
    // to every file of the snapshot
    SnapshotDeletes deletes = deletesFor(spark, tasks, table);

    // the equality keys have to be read to judge a row, and a key the query did not ask for still
    // has to be read for that judgement to be possible
    Set<String> readColumns = Sets.newLinkedHashSet();
    readColumns.addAll(columns);
    readColumns.addAll(deletes.keyColumns());
    List<String> read = Lists.newArrayList(readColumns);

    Schema projection = table.schema().select(read.toArray(new String[0]));
    // the schema the caller made describes its projection; the read also carries the
    // equality-delete keys, so the row a reader builds has to be described by what was read
    StructType rowSchema =
        readSchema == null ? toSparkSchema(projection) : schemaFor(read, readSchema);

    return new FetchPlan(
        projection,
        read,
        columns,
        rowSchema,
        conf,
        deletes,
        StorageProperties.resolve(table.io(), table.properties(), conf.get()));
  }

  /**
   * One data file of a fetch-back: which file to read and which of its rows the lookup asked for.
   *
   * <p>It travels as the data of the fetch-back's RDD, so it carries nothing every task also has:
   * the configuration, the delete files and the storage settings come from the {@link FetchPlan}
   * the task was handed.
   */
  private static final class FileFetch implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    private final String dataFile;
    private final FileFormat format;
    // null means every row of the file, which is how a caller that has no positions reads it
    private final List<Long> positions;

    private FileFetch(String dataFile, FileFormat format, List<Long> positions) {
      this.dataFile = dataFile;
      this.format = format;
      this.positions = positions;
    }

    /** The live rows of this file that the lookup asked for, each paired with its position. */
    private List<Tuple2<Long, Row>> liveRows(FetchPlan plan) {
      // the delete files are read on the driver, and every file of the fetch-back asks the same set
      // for its own rows, so the lookup here is a map read rather than a file read
      long[] deletedPositions = plan.deletes.deletedPositions(dataFile);
      List<EqualityKeySet> keySets = plan.deletes.keySets();

      Iterator<Tuple2<Row, Long>> read;
      try {
        read =
            readFileWithFormat(
                dataFile,
                format,
                plan.projection,
                plan.rowSchema,
                positions,
                plan.conf,
                plan.storageProperties);
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to read " + dataFile, e);
      }

      List<Tuple2<Long, Row>> rows = Lists.newArrayList();
      while (read.hasNext()) {
        Tuple2<Row, Long> entry = read.next();
        // a format that ignored the positions read the whole file, so the rows it returned are
        // numbered but not narrowed: only the ones that were asked for are part of the answer
        if (!isLive(entry._2(), entry._1(), positions, deletedPositions, keySets)) {
          continue;
        }

        rows.add(
            new Tuple2<>(
                entry._2(),
                new GenericRowWithSchema(
                    asValues(entry._1(), plan.readColumns, plan.columns),
                    schemaFor(plan.columns, entry._1().schema()))));
      }

      return rows;
    }
  }

  /**
   * Reads all live rows of the given data files of a table snapshot, with the snapshot's delete
   * files applied. Used to scan the data files an index does not cover yet.
   *
   * @param spark the Spark session
   * @param table the Iceberg table to read
   * @param snapshotId the table snapshot to read; its delete files are applied
   * @param dataFiles the data files to read
   * @param columns the columns to return, in this order
   * @param readSchema the schema to read the data files with
   */
  public static Dataset<Row> liveRows(
      SparkSession spark,
      Table table,
      long snapshotId,
      Collection<String> dataFiles,
      List<String> columns,
      StructType readSchema) {
    return liveRows(spark, table, dataFiles, columns, readSchema, scanTasks(table, snapshotId));
  }

  /**
   * The live rows of the given data files, each with the file and the position it came from.
   *
   * <p>The dataset form of {@link #liveRows} drops the reference of a row: it is the row that a
   * scan returns, and a scan has no need for where the row sits. A caller that has to report which
   * row it picked, or that scores rows itself and wants to answer with their references, needs the
   * pair, and reading the files again to recover it would double the work.
   */
  public static JavaPairRDD<String, Tuple2<Long, Row>> liveRowsWithPositions(
      SparkSession spark,
      Table table,
      Collection<String> dataFiles,
      List<String> columns,
      StructType readSchema,
      List<FileScanTask> scanTasks) {
    List<FileScanTask> tasks = Lists.newArrayList();
    for (FileScanTask task : scanTasks) {
      if (dataFiles.contains(task.file().location().toString())) {
        tasks.add(task);
      }
    }

    // no positions for any file, which this path reads as "every row of the file"
    Map<String, Collection<Long>> everyRow = Maps.newHashMap();
    return liveRowsRdd(spark, table, everyRow, columns, readSchema, tasks);
  }

  /** Reads all live rows of the given data files, from scan tasks that were already planned. */
  public static Dataset<Row> liveRows(
      SparkSession spark,
      Table table,
      Collection<String> dataFiles,
      List<String> columns,
      StructType readSchema,
      List<FileScanTask> scanTasks) {
    List<FileScanTask> tasks = Lists.newArrayList();
    for (FileScanTask task : scanTasks) {
      if (dataFiles.contains(task.file().location().toString())) {
        tasks.add(task);
      }
    }

    SnapshotDeletes deletes = SnapshotDeletes.read(spark, table, tasks, table.schema());
    Map<String, String> storageProperties =
        StorageProperties.resolve(
            table.io(), table.properties(), spark.sessionState().newHadoopConf());
    List<JavaRDD<Row>> perFile = Lists.newArrayList();
    for (FileScanTask task : tasks) {
      perFile.add(
          liveRowsOf(
                  spark,
                  table.schema(),
                  task,
                  columns,
                  null,
                  readSchema,
                  deletes,
                  null,
                  storageProperties)
              .values());
    }

    if (perFile.isEmpty()) {
      return spark.createDataFrame(new ArrayList<Row>(), schemaFor(columns, readSchema));
    }

    JavaRDD<Row> rows = perFile.get(0);
    for (int i = 1; i < perFile.size(); i++) {
      rows = rows.union(perFile.get(i));
    }

    return spark.createDataFrame(rows, schemaFor(columns, readSchema));
  }

  /**
   * Reads the live rows of one data file, paired with their position, which is the row ordinal of
   * the data file. Rows removed by the snapshot's delete files are left out. An empty position
   * collection means every row of the file.
   */
  private static JavaPairRDD<Long, Row> liveRowsOf(
      SparkSession spark,
      Schema tableSchema,
      FileScanTask task,
      List<String> columns,
      Collection<Long> positions,
      StructType readSchema,
      SnapshotDeletes deletes,
      SerializableConfiguration conf,
      Map<String, String> storageProperties) {
    String dataFile = task.file().location().toString();

    // delete files are read on the driver so the executor-side filter is a simple lookup
    long[] deletedPositions = deletes.deletedPositions(dataFile);
    List<EqualityKeySet> keySets = deletes.keySets();
    List<String> keyColumns = deletes.keyColumns();

    Set<String> columnsToRead = Sets.newLinkedHashSet();
    columnsToRead.addAll(columns);
    columnsToRead.addAll(keyColumns);
    List<String> readColumns = Lists.newArrayList(columnsToRead);

    StructType readWith = readSchema == null ? null : schemaFor(readColumns, readSchema);
    // the projection to the requested columns happens below, by name, so the read itself can take
    // every column the file has
    JavaPairRDD<Row, Long> data =
        positionedRows(
            spark, tableSchema, task, readColumns, positions, readWith, conf, storageProperties);

    List<String> capturedReadColumns = readColumns;
    List<String> capturedColumns = columns;
    // a caller may hand over a collection view, such as a map's key set, which Spark cannot ship to
    // an executor; copying it here keeps that from surfacing as a task serialization failure
    Collection<Long> wantedPositions = positions == null ? null : Lists.newArrayList(positions);
    return data.filter(
            (Function<Tuple2<Row, Long>, Boolean>)
                tuple -> isLive(tuple, wantedPositions, deletedPositions, keySets))
        .mapToPair(
            (PairFunction<Tuple2<Row, Long>, Long, Row>)
                tuple ->
                    new Tuple2<>(
                        tuple._2(),
                        new GenericRowWithSchema(
                            asValues(tuple._1(), capturedReadColumns, capturedColumns),
                            schemaFor(capturedColumns, tuple._1().schema()))));
  }

  /** Whether a row of a data file survives its position and equality delete files. */
  private static boolean isLive(
      Tuple2<Row, Long> tuple,
      Collection<Long> positions,
      long[] deletedPositions,
      List<EqualityKeySet> keySets) {
    return isLive(tuple._2(), tuple._1(), positions, deletedPositions, keySets);
  }

  /** Whether the row at the given position of a data file survives its delete files. */
  private static boolean isLive(
      long position,
      Row row,
      Collection<Long> positions,
      long[] deletedPositions,
      List<EqualityKeySet> keySets) {
    if (positions != null && !positions.isEmpty() && !positions.contains(position)) {
      return false;
    }

    if (Arrays.binarySearch(deletedPositions, position) >= 0) {
      return false;
    }

    // a snapshot can hold equality deletes keyed on different columns, and each of them removes its
    // own rows: one key per row would only be able to match one of them
    for (EqualityKeySet keySet : keySets) {
      if (keySet.keys().contains(equalityKey(row, keySet.fields()))) {
        return false;
      }
    }

    return true;
  }

  /**
   * Reads one data file as (position, row) pairs, whatever format it is stored in.
   *
   * <p>Parquet keeps its native Spark read. Every other format is read through the Iceberg format
   * model for that file's format, which keeps the index layer format-agnostic: a Lance data file is
   * read by the Lance format model rather than being handed to a parquet reader.
   */
  private static JavaPairRDD<Row, Long> positionedRows(
      SparkSession spark,
      Schema tableSchema,
      FileScanTask task,
      List<String> columns,
      Collection<Long> positions,
      StructType readSchema,
      SerializableConfiguration conf,
      Map<String, String> storageProperties) {
    String dataFile = task.file().location().toString();
    FileFormat format = task.file().format();

    if (format == FileFormat.PARQUET) {
      // The position of a row is its ordinal in the file, so it has to be counted as the rows
      // stream past. zipWithIndex would give the same answer, but it runs a job of its own to size
      // the partitions first, so a lookup that touches ten data files pays for ten jobs and reads
      // each file twice. Merging to one partition first costs nothing when the file was going to
      // be read by a single task anyway, and it is how every other format is read below.
      return readParquet(spark, dataFile, columns, tableSchema, readSchema, conf)
          .toJavaRDD()
          .coalesce(1)
          .mapPartitionsToPair(
              (PairFlatMapFunction<Iterator<Row>, Row, Long>) rows -> new NumberedRows(rows), true);
    }

    Schema projection = tableSchema.select(columns.toArray(new String[0]));
    StructType sparkSchema = toSparkSchema(projection);
    SerializableConfiguration readConf =
        conf != null ? conf : new SerializableConfiguration(spark.sessionState().newHadoopConf());

    return JavaSparkContext.fromSparkContext(spark.sparkContext())
        .parallelize(Collections.singletonList(dataFile), 1)
        .flatMapToPair(
            (PairFlatMapFunction<String, Row, Long>)
                file ->
                    readFileWithFormat(
                        file,
                        format,
                        projection,
                        sparkSchema,
                        positions,
                        readConf,
                        storageProperties));
  }

  /**
   * Reads a parquet data file into the given columns, in that order.
   *
   * <p>A parquet writer escapes a column name it cannot store verbatim: `a.b` is written as
   * `a_x2Eb` and `c d` as `c_x20d`. Asking for such a column by its table name finds nothing in the
   * file, and the read answers a null rather than failing. The file still carries the field ids, so
   * the name each column is stored under is read from the footer and the read asks for that, then
   * renames the columns back to the table's names. Only a table that has such a name pays for the
   * footer read; every other table matches every column by name, which is how the file stores it.
   */
  private static Dataset<Row> readParquet(
      SparkSession spark,
      String dataFile,
      List<String> columns,
      Schema tableSchema,
      StructType readSchema,
      SerializableConfiguration conf) {
    if (readSchema == null) {
      return spark.read().parquet(dataFile);
    }

    if (!hasEscapableName(columns)) {
      return spark.read().schema(readSchema).parquet(dataFile);
    }

    Map<Integer, String> stored = storedNames(dataFile, conf);
    StructField[] storedFields = new StructField[columns.size()];
    boolean renamed = false;
    for (int i = 0; i < columns.size(); i++) {
      StructField field = readSchema.apply(columns.get(i));
      Types.NestedField column = columnOf(tableSchema, columns.get(i));
      String storedName = column == null ? null : stored.get(column.fieldId());
      boolean differs = storedName != null && !storedName.equals(field.name());
      renamed |= differs;
      storedFields[i] =
          differs
              ? new StructField(storedName, field.dataType(), field.nullable(), field.metadata())
              : field;
    }

    Dataset<Row> data = spark.read().schema(new StructType(storedFields)).parquet(dataFile);
    return renamed ? data.toDF(columns.toArray(new String[0])) : data;
  }

  /**
   * The name each column of a parquet file is stored under, keyed by field id, empty when the file
   * carries no ids.
   */
  private static Map<Integer, String> storedNames(String dataFile, SerializableConfiguration conf) {
    Map<Integer, String> names = Maps.newHashMap();
    try (org.apache.parquet.hadoop.ParquetFileReader reader =
        org.apache.parquet.hadoop.ParquetFileReader.open(
            org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(
                new org.apache.hadoop.fs.Path(dataFile), taskConf(conf)))) {
      org.apache.parquet.schema.MessageType schema =
          reader.getFooter().getFileMetaData().getSchema();
      for (int i = 0; i < schema.getFieldCount(); i++) {
        org.apache.parquet.schema.Type field = schema.getType(i);
        if (field.getId() != null) {
          names.put(field.getId().intValue(), schema.getFieldName(i));
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(String.format("Failed to read the footer of %s", dataFile), e);
    }

    return names;
  }

  /**
   * Whether a parquet writer would store one of the column names escaped, which it does for a name
   * with a character outside the plain identifier set. Such a table is the only one whose columns
   * cannot be found in the file by name.
   */
  private static boolean hasEscapableName(List<String> columns) {
    for (String column : columns) {
      for (int i = 0; i < column.length(); i++) {
        char c = column.charAt(i);
        boolean plain =
            (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_';
        if (!plain) {
          return true;
        }
      }
    }

    return false;
  }

  /** The table column of a given name, or null when the table does not have it. */
  private static Types.NestedField columnOf(Schema tableSchema, String name) {
    for (Types.NestedField column : tableSchema.columns()) {
      if (column.name().equals(name)) {
        return column;
      }
    }

    return null;
  }

  /**
   * The deserialized Hadoop configuration for this task.
   *
   * <p>Deserializing it costs more than a file open, and a task can resolve several files, so it is
   * kept per thread and rebuilt only when the configuration handed down changes.
   */
  private static org.apache.hadoop.conf.Configuration taskConf(SerializableConfiguration conf) {
    CachedConf cached = TASK_CONF.get();
    if (cached == null || cached.source != conf) {
      cached = new CachedConf(conf, conf.get());
      TASK_CONF.set(cached);
    }
    return cached.conf;
  }

  private static final class CachedConf {
    private final SerializableConfiguration source;
    private final org.apache.hadoop.conf.Configuration conf;

    private CachedConf(
        SerializableConfiguration source, org.apache.hadoop.conf.Configuration conf) {
      this.source = source;
      this.conf = conf;
    }
  }

  private static final ThreadLocal<CachedConf> TASK_CONF = new ThreadLocal<>();

  private static long ms(long nanos) {
    return java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(nanos);
  }

  /**
   * How many tasks the fetch-back uses for the given number of hit files.
   *
   * <p>Defaults to one task per file, which is what a fetch-back of a handful of large files wants;
   * {@code iceberg.index.fetch-back.tasks} lowers it, which is what a fetch-back of many small
   * reads wants because the per-task cost then dominates.
   */
  private static int readTaskCount(int files) {
    int wanted = Integer.getInteger("iceberg.index.fetch-back.tasks", files);
    return Math.max(1, Math.min(files, wanted));
  }

  /** Pairs each row with its position, counting rows as they pass. */
  private static final class NumberedRows implements Iterator<Tuple2<Row, Long>> {
    private final Iterator<Row> rows;
    private long position = 0;

    NumberedRows(Iterator<Row> rows) {
      this.rows = rows;
    }

    @Override
    public boolean hasNext() {
      return rows.hasNext();
    }

    @Override
    public Tuple2<Row, Long> next() {
      return new Tuple2<>(rows.next(), position++);
    }
  }

  /**
   * The live rows at the given positions of one data file, as this task reads them.
   *
   * <p>A search names the rows it found by file and position, and this reads those rows where the
   * search happened: the format model opens the file once and reads only the positions asked for,
   * for every format a table can be written in, and the snapshot's deletes are applied before a row
   * is returned. Reading them here rather than collecting the positions and fetching the rows in
   * another job is what keeps the fetch-back from costing a job of its own.
   *
   * @param io the table's file IO, so the file is read the way the table is read
   * @param task the scan task of the file, of the snapshot being answered
   * @param deletes the delete files of that snapshot
   * @param positions the positions to read, in ascending order
   * @param projection the columns to return, in this order
   * @param expected the schema to read the file with, or null to infer it from the file
   * @param conf the Hadoop configuration to read the file with, which is the session's own and is
   *     deserialized once per task rather than per file
   * @return the live rows, in ascending position order, each paired with its position
   */
  static List<Tuple2<Row, Long>> liveRowsAt(
      FileIO io,
      FileScanTask task,
      SnapshotDeletes deletes,
      List<Long> positions,
      Schema projection,
      org.apache.spark.sql.types.StructType expected,
      SerializableConfiguration conf,
      Map<String, String> storageProperties) {
    if (positions.isEmpty()) {
      return Lists.newArrayList();
    }

    String dataFile = task.file().location().toString();
    long[] deleted = deletes.deletedPositions(dataFile);
    List<EqualityKeySet> keySets = deletes.keySets();
    List<String> keyColumns = deletes.keyColumns();

    List<Long> wanted = Lists.newArrayListWithCapacity(positions.size());
    for (long position : positions) {
      if (java.util.Arrays.binarySearch(deleted, position) < 0) {
        wanted.add(position);
      }
    }

    if (wanted.isEmpty()) {
      return Lists.newArrayList();
    }

    // the equality keys have to be read to judge a row, and a key the query did not ask for still
    // has to be read for that judgement to be possible
    List<String> readColumns = Lists.newArrayList();
    for (Types.NestedField field : projection.columns()) {
      readColumns.add(field.name());
    }
    for (String key : keyColumns) {
      if (!readColumns.contains(key)) {
        readColumns.add(key);
      }
    }

    Schema readSchema = projection.select(readColumns.toArray(new String[0]));
    // the schema the caller made describes its projection; the read also carries the
    // equality-delete
    // keys, so the row a reader builds has to be described by what was actually read
    StructType rowSchema =
        expected != null && expected.size() == readSchema.columns().size()
            ? expected
            : toSparkSchema(readSchema);
    java.util.Iterator<Tuple2<Row, Long>> read;
    try {
      read =
          readFileWithFormat(
              dataFile,
              task.file().format(),
              readSchema,
              rowSchema,
              wanted,
              conf,
              storageProperties);
    } catch (java.io.IOException e) {
      throw new java.io.UncheckedIOException("Failed to read " + dataFile, e);
    }

    List<Tuple2<Row, Long>> rows = Lists.newArrayListWithCapacity(wanted.size());
    while (read.hasNext()) {
      Tuple2<Row, Long> entry = read.next();
      // a format that ignored the positions read the whole file, so the rows it returned are
      // numbered but not narrowed: only the ones that were asked for are part of the answer
      if (java.util.Collections.binarySearch(wanted, entry._2()) < 0) {
        continue;
      }

      if (!survivesEqualityDeletes(entry._1(), keySets)) {
        continue;
      }

      rows.add(new Tuple2<>(projectTo(entry._1(), readColumns, projection), entry._2()));
    }

    return rows;
  }

  /**
   * The live rows of one data file that a filter admits, as the positions a search names them by.
   *
   * <p>A task that restricts its search to the admitted rows has to know them, and reading them
   * here is what makes that a local decision: it opens the file the task covers and reads the
   * columns the filter names, so the price of the filter is the columns it needs rather than the
   * whole row, and every task pays it in parallel with the others rather than in one pass over the
   * table. The snapshot's deletes are applied as the rows stream past, so a row a delete file
   * removed never becomes a candidate.
   *
   * @param io the table's file IO, so the file is read the way the table is read
   * @param task the scan task of the file, of the snapshot being answered
   * @param deletes the delete files of that snapshot
   * @param filter the filter a row has to pass, bound to the columns of {@code filterColumns}
   * @param tableSchema the table's schema, which the columns are selected from
   * @param filterColumns the columns the filter names
   * @param expected the schema to read the file with, or null to infer it from the file
   * @param conf the Hadoop configuration to read the file with
   * @return the admitted positions, in ascending order
   */
  static long[] positionsMatching(
      FileIO io,
      FileScanTask task,
      SnapshotDeletes deletes,
      IndexRowFilter filter,
      Schema tableSchema,
      List<String> filterColumns,
      org.apache.spark.sql.types.StructType expected,
      SerializableConfiguration conf,
      Map<String, String> storageProperties) {
    String dataFile = task.file().location().toString();
    long[] deleted = deletes.deletedPositions(dataFile);
    List<EqualityKeySet> keySets = deletes.keySets();

    // the equality keys have to be read to judge a row, and a key the filter does not name still
    // has to be read for that judgement to be possible
    List<String> readColumns = Lists.newArrayList(filterColumns);
    for (String key : deletes.keyColumns()) {
      if (!readColumns.contains(key)) {
        readColumns.add(key);
      }
    }

    Schema readSchema = tableSchema.select(readColumns.toArray(new String[0]));
    Schema filterSchema = tableSchema.select(filterColumns.toArray(new String[0]));
    java.util.Iterator<Tuple2<Row, Long>> read;
    try {
      // no positions: every row of the file is read in file order, which is what makes the filter
      // see the rows it has to judge rather than the rows a search happened to find
      StructType rowSchema =
          expected != null && expected.size() == readSchema.columns().size()
              ? expected
              : toSparkSchema(readSchema);
      read =
          readFileWithFormat(
              dataFile, task.file().format(), readSchema, rowSchema, null, conf, storageProperties);
    } catch (java.io.IOException e) {
      throw new java.io.UncheckedIOException("Failed to read " + dataFile, e);
    }

    long[] admitted = new long[64];
    int count = 0;
    while (read.hasNext()) {
      Tuple2<Row, Long> entry = read.next();
      if (java.util.Arrays.binarySearch(deleted, entry._2()) >= 0) {
        continue;
      }

      if (!survivesEqualityDeletes(entry._1(), keySets)) {
        continue;
      }

      if (!filter.matches(projectTo(entry._1(), readColumns, filterSchema))) {
        continue;
      }

      if (count == admitted.length) {
        admitted = java.util.Arrays.copyOf(admitted, admitted.length * 2);
      }

      admitted[count++] = entry._2();
    }

    return java.util.Arrays.copyOf(admitted, count);
  }

  /**
   * Reads the positions admitted by a text predicate. Visibility comes from the postings of the
   * exact index snapshot, so only the columns that the predicate needs are read here.
   */
  static long[] textFilterPositions(
      FileIO io,
      FileScanTask task,
      Schema projection,
      Predicate<Row> predicate,
      Map<String, String> properties) {
    InputFile input = io.newInputFile(task.file());
    ReadBuilder<Record, ?> reader =
        FormatModelRegistry.readBuilder(task.file().format(), Record.class, input)
            .project(projection)
            .setAll(properties)
            .idToConstant(
                PartitionUtil.constantsMap(task, IdentityPartitionConverters::convertConstant));
    String mapping = properties.get(TableProperties.DEFAULT_NAME_MAPPING);
    if (mapping != null) {
      reader.withNameMapping(NameMappingParser.fromJson(mapping));
    }

    List<Types.NestedField> fields = projection.columns();
    long[] admitted = new long[64];
    int count = 0;
    long position = 0;
    try (CloseableIterable<Record> records = reader.build();
        CloseableIterator<Record> iterator = records.iterator()) {
      while (iterator.hasNext()) {
        Record record = iterator.next();
        Object[] values = new Object[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
          Types.NestedField field = fields.get(i);
          values[i] = toSparkValue(field.type(), record.get(i));
        }

        if (predicate.test(RowFactory.create(values))) {
          if (count == admitted.length) {
            admitted = Arrays.copyOf(admitted, Math.multiplyExact(admitted.length, 2));
          }
          admitted[count++] = position;
        }

        position++;
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to filter " + input.location(), e);
    }

    return Arrays.copyOf(admitted, count);
  }

  /** Whether a row survives the equality deletes of the snapshot. */
  private static boolean survivesEqualityDeletes(Row row, List<EqualityKeySet> keySets) {
    // a snapshot can hold equality deletes keyed on different columns, and each removes its own
    // rows, so the row is held against every key set rather than against one key
    for (EqualityKeySet keySet : keySets) {
      if (keySet.keys().contains(equalityKey(row, keySet.fields()))) {
        return false;
      }
    }

    return true;
  }

  /** The row narrowed to the columns the query asked for, when the read carried more than that. */
  private static Row projectTo(Row row, List<String> readColumns, Schema projection) {
    if (readColumns.size() == projection.columns().size()) {
      return row;
    }

    Object[] values = new Object[projection.columns().size()];
    for (int i = 0; i < values.length; i++) {
      values[i] = row.get(readColumns.indexOf(projection.columns().get(i).name()));
    }

    return RowFactory.create(values);
  }

  /**
   * Reads every row of one data file in file order, one at a time.
   *
   * <p>Unlike {@link #readFileWithFormat} this does not collect the file before returning: a group
   * whose files are read at once would otherwise hold every file it is reading as rows, which is
   * both more memory and more work than holding the chunk of each that is being consumed.
   */
  private static CloseableIterator<Tuple2<Long, Row>> openRowsWithFormat(
      InputFile inputFile,
      FileFormat format,
      Schema projection,
      StructType sparkSchema,
      Map<String, String> storageProperties)
      throws IOException {
    ReadBuilder<Record, Void> builder =
        FormatModelRegistry.readBuilder(format, Record.class, inputFile);
    // a format that opens the file itself does not read through the FileIO, so the storage settings
    // this table is read with have to be handed to it
    builder.setAll(storageProperties);
    return new RowsWithPosition(builder.project(projection).build(), projection, sparkSchema);
  }

  /** The rows of one data file, each numbered by its position in the file. */
  private static final class RowsWithPosition implements CloseableIterator<Tuple2<Long, Row>> {
    private final CloseableIterable<Record> records;
    private final CloseableIterator<Record> rows;
    private final Schema projection;
    private final StructType sparkSchema;
    private long position = 0;

    private RowsWithPosition(
        CloseableIterable<Record> records, Schema projection, StructType sparkSchema) {
      this.records = records;
      this.rows = records.iterator();
      this.projection = projection;
      this.sparkSchema = sparkSchema;
    }

    @Override
    public boolean hasNext() {
      return rows.hasNext();
    }

    @Override
    public Tuple2<Long, Row> next() {
      return new Tuple2<>(position++, rowOf(rows.next(), projection, sparkSchema));
    }

    @Override
    public void close() throws IOException {
      records.close();
    }
  }

  /** One record read with the given projection, as the Spark row the index layer works with. */
  private static Row rowOf(Record record, Schema projection, StructType sparkSchema) {
    List<Types.NestedField> fields = projection.columns();
    Object[] values = new Object[fields.size()];
    for (int i = 0; i < values.length; i++) {
      Types.NestedField field = fields.get(i);
      values[i] = toSparkValue(field.type(), record.getField(field.name()));
    }

    return new GenericRowWithSchema(values, sparkSchema);
  }

  /** Reads every row of one data file with the format model for its format, in file order. */
  private static Iterator<Tuple2<Row, Long>> readFileWithFormat(
      String location,
      FileFormat format,
      Schema projection,
      StructType sparkSchema,
      Collection<Long> positions,
      SerializableConfiguration conf,
      Map<String, String> storageProperties)
      throws IOException {
    long tStart = System.nanoTime();
    InputFile input = HadoopInputFile.fromLocation(location, taskConf(conf));
    long tInput = System.nanoTime();
    ReadBuilder<Record, Void> builder =
        FormatModelRegistry.readBuilder(format, Record.class, input);
    // a format that opens the file itself does not read through the Hadoop file system, so the
    // storage settings this table is read with have to be handed to it
    builder.setAll(storageProperties);
    long tBuilder = System.nanoTime();
    boolean timings = Boolean.getBoolean("iceberg.index.fetch-back.timings");

    // a lookup knows the rows it wants: ask the format to read only those, which is what lets a
    // page-oriented format read a handful of pages instead of the whole file
    List<Long> wanted = null;
    if (positions != null && !positions.isEmpty()) {
      wanted = Lists.newArrayList(positions);
      java.util.Collections.sort(wanted);
      builder.positions(wanted);
    }

    List<Tuple2<Row, Long>> rows = Lists.newArrayList();
    long nextPosition = 0;
    long tOpen;
    try (CloseableIterable<Record> records = builder.project(projection).build()) {
      tOpen = System.nanoTime();
      for (Record record : records) {
        // the row is numbered as it streams past, which is its position in the file. A format that
        // honoured the positions returned exactly the rows asked for, and those are relabelled
        // below; until the read ends there is no way to tell the two cases apart
        rows.add(new Tuple2<>(rowOf(record, projection, sparkSchema), nextPosition));
        nextPosition++;
      }
    }

    if (timings) {
      long tRead = System.nanoTime();
      System.err.println(
          "[FETCH-BACK] "
              + location.substring(location.lastIndexOf('/') + 1)
              + " conf+input="
              + ms(tInput - tStart)
              + "ms builder="
              + ms(tBuilder - tInput)
              + "ms open="
              + ms(tOpen - tBuilder)
              + "ms read="
              + ms(tRead - tOpen)
              + "ms rows="
              + rows.size());
    }

    if (wanted != null && rows.size() == wanted.size()) {
      // the format honoured the positions: it returned exactly those rows, in ascending order, so
      // the numbering they streamed with is the order asked for rather than the row's position
      for (int i = 0; i < rows.size(); i++) {
        rows.set(i, new Tuple2<>(rows.get(i)._1(), wanted.get(i)));
      }
    }

    // otherwise the format ignored the positions and read the file, which is allowed: the rows are
    // numbered in file order, and the caller's position filter picks the ones it asked for
    return rows.iterator();
  }

  /** The Spark schema of an Iceberg projection, for the rows the format model returns. */
  private static StructType toSparkSchema(Schema schema) {
    return toSparkSchema(schema.asStruct());
  }

  private static StructType toSparkSchema(Types.StructType struct) {
    StructField[] fields = new StructField[struct.fields().size()];
    for (int i = 0; i < fields.length; i++) {
      Types.NestedField field = struct.fields().get(i);
      fields[i] =
          new StructField(
              field.name(), toSparkType(field.type()), field.isOptional(), Metadata.empty());
    }
    return new StructType(fields);
  }

  private static DataType toSparkType(org.apache.iceberg.types.Type type) {
    switch (type.typeId()) {
      case BOOLEAN:
        return DataTypes.BooleanType;
      case INTEGER:
        return DataTypes.IntegerType;
      case DATE:
        return DataTypes.DateType;
      case LONG:
      case TIME:
        return DataTypes.LongType;
      case TIMESTAMP:
        return ((Types.TimestampType) type).shouldAdjustToUTC()
            ? DataTypes.TimestampType
            : DataTypes.TimestampNTZType;
      case TIMESTAMP_NANO:
        return ((Types.TimestampNanoType) type).shouldAdjustToUTC()
            ? DataTypes.TimestampType
            : DataTypes.TimestampNTZType;
      case FLOAT:
        return DataTypes.FloatType;
      case DOUBLE:
        return DataTypes.DoubleType;
      case STRING:
        return DataTypes.StringType;
      case BINARY:
      case FIXED:
      case UUID:
        return DataTypes.BinaryType;
      case DECIMAL:
        Types.DecimalType decimal = (Types.DecimalType) type;
        return DataTypes.createDecimalType(decimal.precision(), decimal.scale());
      case STRUCT:
        return toSparkSchema(type.asStructType());
      case LIST:
        Types.ListType list = type.asListType();
        return DataTypes.createArrayType(toSparkType(list.elementType()), list.isElementOptional());
      case MAP:
        Types.MapType map = type.asMapType();
        return DataTypes.createMapType(
            toSparkType(map.keyType()), toSparkType(map.valueType()), map.isValueOptional());
      default:
        throw new UnsupportedOperationException(
            "Unsupported column type for the index layer: " + type);
    }
  }

  private static boolean adjustsToUtc(org.apache.iceberg.types.Type type) {
    return type.typeId() == org.apache.iceberg.types.Type.TypeID.TIMESTAMP_NANO
        ? ((Types.TimestampNanoType) type).shouldAdjustToUTC()
        : ((Types.TimestampType) type).shouldAdjustToUTC();
  }

  private static java.time.Instant instantOf(Object value) {
    if (value instanceof java.time.Instant) {
      return (java.time.Instant) value;
    } else if (value instanceof java.time.OffsetDateTime) {
      return ((java.time.OffsetDateTime) value).toInstant();
    } else if (value instanceof java.time.LocalDateTime) {
      return ((java.time.LocalDateTime) value).toInstant(java.time.ZoneOffset.UTC);
    }

    return ((java.sql.Timestamp) value).toInstant();
  }

  /** The wall clock a zone-less timestamp holds, whichever way the format handed it back. */
  private static java.time.LocalDateTime localDateTimeOf(Object value) {
    if (value instanceof java.time.LocalDateTime) {
      return (java.time.LocalDateTime) value;
    } else if (value instanceof java.time.OffsetDateTime) {
      return ((java.time.OffsetDateTime) value)
          .withOffsetSameInstant(java.time.ZoneOffset.UTC)
          .toLocalDateTime();
    }

    return java.time.LocalDateTime.ofInstant(instantOf(value), java.time.ZoneOffset.UTC);
  }

  private static Object toSparkValue(org.apache.iceberg.types.Type type, Object value) {
    if (value == null) {
      return null;
    }

    switch (type.typeId()) {
      case STRUCT:
        Record record = (Record) value;
        List<Types.NestedField> fields = type.asStructType().fields();
        Object[] values = new Object[fields.size()];
        for (int i = 0; i < values.length; i++) {
          values[i] = toSparkValue(fields.get(i).type(), record.get(i));
        }
        return RowFactory.create(values);
      case LIST:
        {
          // Spark hands array columns to external rows as Scala sequences, which is what callers of
          // Row.getList expect back
          List<?> list = (List<?>) value;
          List<Object> elements = Lists.newArrayListWithCapacity(list.size());
          for (Object element : list) {
            elements.add(toSparkValue(type.asListType().elementType(), element));
          }
          return scala.collection.JavaConverters.asScalaBuffer(elements);
        }
      case MAP:
        Types.MapType mapType = type.asMapType();
        Map<Object, Object> entries = Maps.newLinkedHashMap();
        ((Map<?, ?>) value)
            .forEach(
                (key, item) ->
                    entries.put(
                        toSparkValue(mapType.keyType(), key),
                        toSparkValue(mapType.valueType(), item)));
        return scala.collection.JavaConverters.mapAsScalaMap(entries);
      case STRING:
        return value.toString();
      case DATE:
        return java.sql.Date.valueOf((java.time.LocalDate) value);
      case TIME:
        return ((java.time.LocalTime) value).toNanoOfDay() / 1000L;
      case TIMESTAMP:
      case TIMESTAMP_NANO:
        // Spark's external type for a zone-aware timestamp is java.sql.Timestamp and for a
        // zone-less one a LocalDateTime: the value has to arrive as that type, not as the count of
        // micros the row holds internally, or every later encode of the row fails on the column.
        // A format may hand back either an instant or a wall clock for the zone-less case, so the
        // declared type decides and not the Java class the value happens to have.
        return adjustsToUtc(type)
            ? java.sql.Timestamp.from(instantOf(value))
            : localDateTimeOf(value);
      case BINARY:
      case FIXED:
        // Spark hands a binary column to an external row as a byte array, while the format model
        // reads the bytes into a buffer
        return value instanceof java.nio.ByteBuffer ? bytesOf((java.nio.ByteBuffer) value) : value;
      default:
        return value;
    }
  }

  private static byte[] bytesOf(java.nio.ByteBuffer buffer) {
    byte[] bytes = new byte[buffer.remaining()];
    buffer.duplicate().get(bytes);
    return bytes;
  }

  private static Object[] asValues(Row row, List<String> readColumns, List<String> columns) {
    Object[] values = new Object[columns.size()];
    for (int i = 0; i < values.length; i++) {
      values[i] = row.get(row.fieldIndex(columns.get(i)));
    }
    return values;
  }

  /**
   * The schema a fetch-back hands to Spark: the data file and the position a row answers, then one
   * value of the row's own type carrying the requested columns.
   */
  private static StructType schemaOf(List<String> columns, StructType readSchema) {
    // the requested columns are nested under one field: a top-level field per column would have to
    // share the space with the file and the position, and a caller's column of the same name would
    // make every reference to it ambiguous
    List<StructField> fields = Lists.newArrayListWithCapacity(3);
    fields.add(new StructField(DATA_FILE_COLUMN, DataTypes.StringType, false, Metadata.empty()));
    fields.add(new StructField(POSITION_COLUMN, DataTypes.LongType, false, Metadata.empty()));
    fields.add(
        new StructField(ROW_COLUMN, schemaFor(columns, readSchema), false, Metadata.empty()));

    return new StructType(fields.toArray(new StructField[0]));
  }

  /** The schema of the given columns, taken from the schema they are read with. */
  private static StructType schemaFor(List<String> columns, StructType readSchema) {
    StructField[] fields = new StructField[columns.size()];
    for (int i = 0; i < fields.length; i++) {
      StructField field = readSchema.apply(columns.get(i));
      fields[i] =
          new StructField(field.name(), field.dataType(), field.nullable(), field.metadata());
    }
    return new StructType(fields);
  }

  /**
   * The Spark schema of the given columns as the table declares them.
   *
   * <p>A fetch-back reads a data file that may predate a column, so the read has to be told which
   * columns were asked for: a column the file does not carry then comes back as null, which is what
   * the build already treats as a row without a value, instead of failing on a field the file never
   * wrote.
   */
  public static StructType sparkSchema(Table table, List<String> columns) {
    StructField[] fields = new StructField[columns.size()];
    for (int i = 0; i < fields.length; i++) {
      Types.NestedField column = table.schema().findField(columns.get(i));
      Preconditions.checkArgument(
          column != null, "Column is not in the table schema: %s", columns.get(i));
      fields[i] =
          new StructField(
              column.name(), toSparkType(column.type()), column.isOptional(), Metadata.empty());
    }

    return new StructType(fields);
  }

  /** Lists the data file locations of the given table snapshot, on the driver. */
  public static List<String> dataFiles(Table table, long snapshotId) {
    List<String> dataFiles = Lists.newArrayList();
    for (FileScanTask task : scanTasks(table, snapshotId)) {
      dataFiles.add(task.file().location().toString());
    }
    return dataFiles;
  }

  /**
   * Maps each data file of a snapshot to the locations of the delete files that apply to it.
   *
   * <p>Used by refresh to detect data files whose delete set changed: their index entries must be
   * rebuilt from the live rows of the newer snapshot.
   */
  public static Map<String, Set<String>> deleteFilesByDataFile(Table table, long snapshotId) {
    Map<String, Set<String>> deletesByFile = Maps.newHashMap();
    for (FileScanTask task : scanTasks(table, snapshotId)) {
      Set<String> deleteLocations = Sets.newHashSet();
      for (DeleteFile deleteFile : task.deletes()) {
        deleteLocations.add(deleteFile.location().toString());
      }
      deletesByFile.put(task.file().location().toString(), deleteLocations);
    }
    return deletesByFile;
  }

  /**
   * Plans the data files of a table snapshot, with the delete files that apply to each of them.
   *
   * <p>This is the one pass over the manifests a query needs, and it is not free: it reads the
   * manifest list and the manifests of the snapshot. A caller that needs both the file list and the
   * rows of some of those files should plan once and pass the result on, rather than planning
   * again.
   */
  public static List<FileScanTask> scanTasks(Table table, long snapshotId) {
    return scanTasks(table, snapshotId, List.of());
  }

  /** Plans the snapshot's files and retains column statistics for the requested columns. */
  public static List<FileScanTask> scanTasks(
      Table table, long snapshotId, Collection<String> statsColumns) {
    TableScan scan = table.newScan().useSnapshot(snapshotId);
    if (!statsColumns.isEmpty()) {
      scan = scan.includeColumnStats(statsColumns);
    }

    List<FileScanTask> tasks = Lists.newArrayList();
    try (CloseableIterable<FileScanTask> planned = scan.planFiles()) {
      for (FileScanTask task : planned) {
        tasks.add(task);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(
          "Failed to plan files for snapshot " + snapshotId + " of table " + table.name(), e);
    }
    return tasks;
  }

  private static List<DeleteFile> positionDeletes(List<DeleteFile> deleteFiles) {
    return byContent(deleteFiles, FileContent.POSITION_DELETES);
  }

  private static List<DeleteFile> equalityDeletes(List<DeleteFile> deleteFiles) {
    return byContent(deleteFiles, FileContent.EQUALITY_DELETES);
  }

  private static List<DeleteFile> byContent(List<DeleteFile> deleteFiles, FileContent content) {
    List<DeleteFile> selected = Lists.newArrayList();
    for (DeleteFile deleteFile : deleteFiles) {
      if (deleteFile.content() == content) {
        selected.add(deleteFile);
      }
    }
    return selected;
  }

  /**
   * The rows a snapshot's delete files remove, read once for every data file being read.
   *
   * <p>Reading them per data file is what makes row resolution expensive: each file resolved would
   * read the delete files that apply to it again, and a delete file that applies to many data files
   * would be read once per file. Equality keys are the same for every data file, so they are read
   * once and broadcast rather than carried into every task.
   */
  /** The keys one equality delete file set removes a row by. */
  private static final class EqualityKeySet implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    private final List<Types.NestedField> fields;
    private final Broadcast<Set<String>> keys;

    private EqualityKeySet(List<Types.NestedField> fields, Broadcast<Set<String>> keys) {
      this.fields = fields;
      this.keys = keys;
    }

    List<Types.NestedField> fields() {
      return fields;
    }

    Set<String> keys() {
      return keys.getValue();
    }
  }

  static final class SnapshotDeletes implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    private static final long[] NO_POSITIONS = new long[0];

    private final Map<String, long[]> deletedPositionsByFile;
    private final List<EqualityKeySet> keySets;

    private SnapshotDeletes(
        Map<String, long[]> deletedPositionsByFile, List<EqualityKeySet> keySets) {
      this.deletedPositionsByFile = deletedPositionsByFile;
      this.keySets = keySets;
    }

    /** A snapshot without delete files, which is the common case. */
    static SnapshotDeletes none() {
      return new SnapshotDeletes(ImmutableMap.of(), ImmutableList.of());
    }

    /** Reads the delete files that apply to the given tasks, in at most two jobs. */
    static SnapshotDeletes read(
        SparkSession spark, Table table, List<FileScanTask> tasks, Schema tableSchema) {
      Set<DeleteFile> positionDeletes = Sets.newLinkedHashSet();
      Set<DeleteFile> equalityDeletes = Sets.newLinkedHashSet();
      for (FileScanTask task : tasks) {
        for (DeleteFile deleteFile : task.deletes()) {
          if (deleteFile.content() == FileContent.POSITION_DELETES) {
            positionDeletes.add(deleteFile);
          } else if (deleteFile.content() == FileContent.EQUALITY_DELETES) {
            equalityDeletes.add(deleteFile);
          }
        }
      }

      if (positionDeletes.isEmpty() && equalityDeletes.isEmpty()) {
        return none();
      }

      Map<String, long[]> deletedByFile =
          positionDeletes.isEmpty()
              ? ImmutableMap.of()
              : readDeletedPositions(table.io(), positionDeletes);

      // equality deletes are grouped by the columns they key on: a snapshot can hold several such
      // files with different keys, and each removes its own rows
      Map<List<Integer>, List<DeleteFile>> byFieldIds = Maps.newLinkedHashMap();
      for (DeleteFile deleteFile : equalityDeletes) {
        byFieldIds
            .computeIfAbsent(deleteFile.equalityFieldIds(), ids -> Lists.newArrayList())
            .add(deleteFile);
      }

      List<EqualityKeySet> keySets = Lists.newArrayList();
      for (Map.Entry<List<Integer>, List<DeleteFile>> entry : byFieldIds.entrySet()) {
        List<org.apache.iceberg.types.Types.NestedField> keyFields =
            fieldsOf(tableSchema, entry.getKey());
        if (keyFields.contains(null)) {
          // a delete file can name a column the table no longer has. A row cannot carry a value for
          // it, so the delete matches nothing. Planning a scan already fails on such a file (the
          // delete index cannot resolve the field), so this is a net rather than a path: dropping
          // the column from the key would compare the wrong values, which is worse than saying so
          LOG.warn(
              "Ignoring equality deletes {}: they name a column the table no longer has, so they "
                  + "remove nothing",
              entry.getValue().stream()
                  .map(deleteFile -> deleteFile.location().toString())
                  .collect(java.util.stream.Collectors.toList()));
          continue;
        }

        refuseNanosKeys(keyFields, entry.getValue());
        Set<String> keys = readEqualityKeys(table.io(), entry.getValue(), keyFields);
        keySets.add(
            new EqualityKeySet(
                keyFields,
                JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(keys)));
      }

      return new SnapshotDeletes(deletedByFile, keySets);
    }

    /** The positions removed from a data file, sorted so a lookup is a binary search. */
    long[] deletedPositions(String dataFile) {
      long[] positions = deletedPositionsByFile.get(dataFile);
      return positions == null ? NO_POSITIONS : positions;
    }

    List<EqualityKeySet> keySets() {
      return keySets;
    }

    /** Every column the equality deletes key on, which is what the read has to project. */
    List<String> keyColumns() {
      List<String> names = Lists.newArrayList();
      for (EqualityKeySet keySet : keySets) {
        for (org.apache.iceberg.types.Types.NestedField field : keySet.fields()) {
          if (!names.contains(field.name())) {
            names.add(field.name());
          }
        }
      }

      return names;
    }
  }

  /**
   * Reads the positions removed by the given position delete files, grouped by the data file they
   * point at.
   *
   * <p>The delete file is read in whatever format it was written in, which for a table that writes
   * Lance is a Lance file: a parquet reader only knows parquet, and the delete files of a table
   * follow the table's data format. The Iceberg format model reads both, so the filter does not
   * depend on the table being written in parquet.
   */
  private static Map<String, long[]> readDeletedPositions(
      FileIO io, Collection<DeleteFile> positionDeletes) {
    Map<String, List<Long>> byFile = Maps.newHashMap();
    for (DeleteFile deleteFile : positionDeletes) {
      try (CloseableIterable<Record> rows =
          openDeletes(io, deleteFile, DeleteSchemaUtil.pathPosSchema())) {
        for (Record row : rows) {
          byFile
              .computeIfAbsent(
                  String.valueOf(row.getField(DELETE_FILE_PATH_COLUMN)),
                  key -> Lists.newArrayList())
              .add(((Number) row.getField(DELETE_POSITION_COLUMN)).longValue());
        }
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to read delete file " + deleteFile.location(), e);
      }
    }

    Map<String, long[]> positionsByFile = Maps.newHashMap();
    for (Map.Entry<String, List<Long>> entry : byFile.entrySet()) {
      long[] positions = new long[entry.getValue().size()];
      int index = 0;
      for (long position : entry.getValue()) {
        positions[index++] = position;
      }
      Arrays.sort(positions);
      positionsByFile.put(entry.getKey(), positions);
    }

    return positionsByFile;
  }

  /**
   * Reads the equality keys removed by the given equality delete files, one key per delete row. The
   * key is the values of the equality columns in field id order, with a null marker for nulls.
   *
   * <p>Equality delete files contain one column per equality field id, in field id order. The file
   * is read through a projection built from the table's own fields, so the record carries the
   * table's names for those ids whatever the file spells them: the values are then read by name. As
   * with position deletes, the file is read in its own format rather than as parquet.
   */
  private static Set<String> readEqualityKeys(
      FileIO io, Collection<DeleteFile> equalityDeletes, List<Types.NestedField> keyFields) {
    Schema keySchema = new Schema(keyFields);
    Set<String> keys = Sets.newHashSet();
    for (DeleteFile deleteFile : equalityDeletes) {
      try (CloseableIterable<Record> rows = openDeletes(io, deleteFile, keySchema)) {
        for (Record row : rows) {
          StringBuilder key = new StringBuilder();
          // a delete file holds its key columns in the order the delete file was written with,
          // which
          // is the order of the field ids; the value is encoded by the column's type, because the
          // two sides read it through different readers and only the type is common to both
          for (Types.NestedField field : keyFields) {
            key.append(encodedValue(field.type(), row.getField(field.name())))
                .append(VALUE_SEPARATOR);
          }
          keys.add(key.toString());
        }
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to read delete file " + deleteFile.location(), e);
      }
    }

    return keys;
  }

  /** Opens one delete file of any format with the given projection. */
  private static CloseableIterable<Record> openDeletes(
      FileIO io, DeleteFile deleteFile, Schema projection) {
    InputFile inputFile = io.newInputFile(deleteFile.location());
    return FormatModelRegistry.readBuilder(deleteFile.format(), Record.class, inputFile)
        .project(projection)
        .setAll(StorageProperties.fromFileIO(io))
        .reuseContainers()
        .build();
  }

  /**
   * The file system settings a format that opens files itself needs, in the spelling the engine
   * already carried them in.
   */
  static Map<String, String> storageProperties(FileIO io) {
    return StorageProperties.fromFileIO(io);
  }

  /** The same, for a path that only has the Hadoop configuration. */
  static Map<String, String> storageProperties(org.apache.hadoop.conf.Configuration conf) {
    return StorageProperties.fromConf(conf);
  }

  /** The table columns of one equality key set, in field id order; null where a column is gone. */
  private static List<Types.NestedField> fieldsOf(Schema schema, List<Integer> fieldIds) {
    List<Types.NestedField> fields = Lists.newArrayListWithCapacity(fieldIds.size());
    for (int fieldId : fieldIds) {
      fields.add(schema.findField(fieldId));
    }

    return fields;
  }

  /**
   * Refuses a key on a nanosecond timestamp.
   *
   * <p>The index path reads a row into a Spark row before it compares keys, and Spark carries a
   * timestamp in microseconds, so two rows that differ only below the microsecond would key the
   * same and a delete would remove both. Refusing the query says so; guessing would remove a live
   * row and say nothing.
   */
  static void refuseNanosKeys(
      List<Types.NestedField> keyFields, Collection<DeleteFile> deleteFiles) {
    for (Types.NestedField field : keyFields) {
      if (field.type().typeId() == Type.TypeID.TIMESTAMP_NANO) {
        Preconditions.checkArgument(
            false,
            "Equality deletes key on the nanosecond timestamp column %s, which the index path "
                + "cannot match: a row is read as microseconds before its key is compared, so a "
                + "delete would remove every row within the same microsecond. Use a different key "
                + "column, or query without the index. Delete files: %s",
            field.name(),
            deleteFiles.stream()
                .map(deleteFile -> deleteFile.location().toString())
                .collect(java.util.stream.Collectors.toList()));
      }
    }
  }

  /**
   * Builds the lookup key of a data row for equality delete matching, in the same encoding as the
   * keys read from the equality delete files.
   */
  private static String equalityKey(Row row, List<Types.NestedField> keyFields) {
    StringBuilder key = new StringBuilder();
    for (Types.NestedField field : keyFields) {
      Object value = row.get(row.fieldIndex(field.name()));
      key.append(encodedValue(field.type(), value)).append(VALUE_SEPARATOR);
    }
    return key.toString();
  }

  /**
   * One key column's value as text, decided by its type rather than by its Java class.
   *
   * <p>The data rows and the delete rows reach this point through different readers: a data row is
   * converted to the Spark representation ({@code toSparkValue}), while a delete row is read by
   * Spark itself, which hands back {@code java.sql.Timestamp} for a timestamp and {@code
   * java.math.BigDecimal} for a decimal. Encoding the class name, as this did before, made the two
   * sides produce different keys for anything but the primitive types, so a delete matched nothing
   * and the row it removed stayed visible through the index.
   */
  static String encodedValue(Type type, Object value) {
    if (value == null) {
      return String.valueOf(NULL_MARKER);
    }

    // the canonical form of each type, which both sides can produce: the day number, the micros,
    // the plain text, and so on
    switch (type.typeId()) {
      case STRING:
        return "string:" + value;
      case DATE:
        return "date:" + epochDay(value);
      case TIME:
        return "time:"
            + (value instanceof Number
                ? ((Number) value).longValue()
                : ((java.time.LocalTime) value).toNanoOfDay() / 1000L);
      case TIMESTAMP:
        return "timestamp:" + micros(value);
      case TIMESTAMP_NANO:
        // the index path reads a nanos timestamp as the micros Spark hands back, so the key carries
        // the same precision on both sides
        return "timestamp_nano:" + micros(value);
      case INTEGER:
        return "int:" + ((Number) value).intValue();
      case LONG:
        return "long:" + ((Number) value).longValue();
      case FLOAT:
        return "float:" + ((Number) value).floatValue();
      case DOUBLE:
        return "double:" + ((Number) value).doubleValue();
      case BOOLEAN:
        return "boolean:" + value;
      case UUID:
        return "uuid:" + value;
      case DECIMAL:
        // the same number can arrive with a different scale (2.50 and 2.5), and the scale of a
        // decimal is not part of its value: strip it before comparing
        return "decimal:" + ((java.math.BigDecimal) value).stripTrailingZeros().toPlainString();
      case FIXED:
      case BINARY:
        return type.typeId() + ":" + java.util.Base64.getEncoder().encodeToString(bytes(value));
      default:
        return type.typeId() + ":" + value;
    }
  }

  /**
   * The day number of a date key, whatever Java type carries it.
   *
   * <p>A row of a data file is read as {@code LocalDate}, a delete written through Spark carries
   * the day as a number, and a reader predating {@code LocalDate} hands back {@code java.sql.Date}.
   * All three name one day, so all three have to produce one key: admitting only one of them turns
   * the others into a class-cast failure when the index is built.
   */
  private static long epochDay(Object value) {
    if (value instanceof Number) {
      return ((Number) value).intValue();
    } else if (value instanceof java.time.LocalDate) {
      return ((java.time.LocalDate) value).toEpochDay();
    } else if (value instanceof java.sql.Date) {
      return ((java.sql.Date) value).toLocalDate().toEpochDay();
    }
    throw new IllegalArgumentException("Unsupported value for a date key: " + value);
  }

  /** The bytes of a binary value, which the data and delete readers hand back differently. */
  private static byte[] bytes(Object value) {
    if (value instanceof byte[]) {
      return (byte[]) value;
    } else if (value instanceof java.nio.ByteBuffer) {
      java.nio.ByteBuffer buffer = ((java.nio.ByteBuffer) value).duplicate();
      byte[] out = new byte[buffer.remaining()];
      buffer.get(out);
      return out;
    }

    throw new IllegalArgumentException("Cannot read binary from a " + value.getClass());
  }

  /** Microseconds since the epoch, from either a count of them or a timestamp object. */
  private static long micros(Object value) {
    if (value instanceof Number) {
      return ((Number) value).longValue();
    } else if (value instanceof java.sql.Timestamp) {
      java.sql.Timestamp timestamp = (java.sql.Timestamp) value;
      return timestamp.getTime() * 1000L + (timestamp.getNanos() / 1000L) % 1000L;
    } else if (value instanceof java.time.Instant) {
      java.time.Instant instant = (java.time.Instant) value;
      return instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1000L;
    } else if (value instanceof java.time.OffsetDateTime) {
      return micros(((java.time.OffsetDateTime) value).toInstant());
    } else if (value instanceof java.time.LocalDateTime) {
      return micros(((java.time.LocalDateTime) value).toInstant(java.time.ZoneOffset.UTC));
    }

    throw new IllegalArgumentException("Cannot read a timestamp from a " + value.getClass());
  }

  /**
   * Extracts the indexed column's value in a Java representation: array columns come back from
   * Spark rows as Scala sequences, which are converted to Java lists here so adapters do not need
   * Scala.
   */
  /** Whether a row has a value the index can hold, counting it when the row is left out. */
  private static boolean hasValue(Row row, int columnIndex, LongAccumulator skipped) {
    if (columnValue(row, columnIndex) == null) {
      if (skipped != null) {
        skipped.add(1L);
      }
      return false;
    }
    return true;
  }

  private static Object columnValue(Row row, int columnIndex) {
    Object value = row.get(columnIndex);
    if (value instanceof scala.collection.Seq) {
      return scala.collection.JavaConverters.seqAsJavaList((scala.collection.Seq<?>) value);
    }
    return value;
  }

  private static float[] toFloats(List<Object> values) {
    float[] vector = new float[values.size()];
    for (int i = 0; i < values.size(); i++) {
      vector[i] = ((Number) values.get(i)).floatValue();
    }
    return vector;
  }
}
