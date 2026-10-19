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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.formats.ReadBuilder;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hadoop.SerializableConfiguration;
import org.apache.iceberg.index.IndexAdapter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
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

  public static final String DATA_FILE_COLUMN = "data_file";
  public static final String POSITION_COLUMN = "pos";
  public static final String VECTOR_COLUMN = "vector";

  public static final StructType SCHEMA =
      new StructType()
          .add(DATA_FILE_COLUMN, DataTypes.StringType, false)
          .add(POSITION_COLUMN, DataTypes.LongType, false)
          .add(VECTOR_COLUMN, DataTypes.BinaryType, false);

  private static final String DELETE_FILE_PATH_COLUMN = "file_path";
  private static final String DELETE_POSITION_COLUMN = "pos";
  private static final char NULL_MARKER = '\u0000';
  private static final char VALUE_SEPARATOR = '\u0001';

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
   * how: every data file's rows stay in their own partition. A caller that writes one index file
   * per group of data files can then read the files of one group into a single task, so the vectors
   * of that group never cross the cluster. Returns null when there are no data files to read.
   */
  public static JavaRDD<Row> entryRows(
      SparkSession spark,
      Table table,
      java.util.Collection<FileScanTask> tasks,
      String column,
      IndexAdapter adapter) {
    Schema tableSchema = table.schema();
    List<FileScanTask> taskList = Lists.newArrayList(tasks);
    if (taskList.isEmpty()) {
      return null;
    }

    SnapshotDeletes deletes = SnapshotDeletes.read(spark, taskList, tableSchema);

    // the data read needs the indexed column plus the columns referenced by equality deletes
    Set<String> columnsToRead = Sets.newLinkedHashSet();
    columnsToRead.add(column);
    columnsToRead.addAll(deletes.keyColumns());
    List<String> readColumns = Lists.newArrayList(columnsToRead);

    JavaRDD<Row> rows = null;
    for (FileScanTask task : taskList) {
      String dataFile = task.file().location().toString();

      JavaRDD<Row> fileRows =
          liveRowsOf(spark, tableSchema, task, readColumns, null, null, deletes, null)
              .mapToPair(
                  (PairFunction<Tuple2<Long, Row>, Long, Row>)
                      entry ->
                          new Tuple2<>(
                              entry._1(),
                              RowFactory.create(
                                  dataFile,
                                  entry._1(),
                                  adapter.encodeEntryValue(
                                      columnValue(entry._2(), readColumns.indexOf(column))))))
              .values();

      rows = rows == null ? fileRows : rows.union(fileRows);
    }

    return rows;
  }

  /** The delete files of the tasks being read, read once. */
  private static SnapshotDeletes deletesFor(
      SparkSession spark, List<FileScanTask> tasks, Table table) {
    return SnapshotDeletes.read(spark, tasks, table.schema());
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
    List<FileScanTask> tasks = Lists.newArrayList();
    for (FileScanTask task : scanTasks) {
      if (positionsByFile.containsKey(task.file().location().toString())) {
        tasks.add(task);
      }
    }

    // the rows of every file are unioned and read by one job, which only holds because the
    // positions are counted as the rows stream past rather than asked of Spark per file
    // one serializable configuration for the whole fetch-back: building it copies the session's
    // Hadoop configuration, which is not something to do once per data file
    SerializableConfiguration conf =
        new SerializableConfiguration(spark.sessionState().newHadoopConf());

    JavaPairRDD<String, Tuple2<Long, Row>> rows = null;
    for (FileScanTask task : tasks) {
      String dataFile = task.file().location().toString();
      Collection<Long> positions = positionsByFile.get(dataFile);
      JavaPairRDD<String, Tuple2<Long, Row>> fileRows =
          liveRowsOf(
                  spark,
                  table.schema(),
                  task,
                  columns,
                  positions,
                  readSchema,
                  deletesFor(spark, tasks, table),
                  conf)
              .mapToPair(
                  (PairFunction<Tuple2<Long, Row>, String, Tuple2<Long, Row>>)
                      entry -> new Tuple2<>(dataFile, entry));
      rows = rows == null ? fileRows : rows.union(fileRows);
    }

    Map<String, Map<Long, Row>> rowsByFile = Maps.newLinkedHashMap();
    if (rows == null) {
      return rowsByFile;
    }

    // a lookup reads a few rows from each hit file, so what the fetch-back pays is opening the
    // files and starting the tasks rather than moving bytes. Packing the files into fewer tasks
    // keeps that fixed cost paid a handful of times instead of once per file; the number is a
    // property so the trade-off against per-file parallelism can be measured.
    int readTasks = readTaskCount(tasks.size());
    for (Tuple2<String, Tuple2<Long, Row>> entry : rows.coalesce(readTasks).collect()) {
      rowsByFile
          .computeIfAbsent(entry._1(), key -> Maps.newLinkedHashMap())
          .put(entry._2()._1(), entry._2()._2());
    }

    return rowsByFile;
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

    SnapshotDeletes deletes = SnapshotDeletes.read(spark, tasks, table.schema());
    List<JavaRDD<Row>> perFile = Lists.newArrayList();
    for (FileScanTask task : tasks) {
      perFile.add(
          liveRowsOf(spark, table.schema(), task, columns, null, readSchema, deletes, null)
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
      SerializableConfiguration conf) {
    String dataFile = task.file().location().toString();

    // delete files are read on the driver so the executor-side filter is a simple lookup
    long[] deletedPositions = deletes.deletedPositions(dataFile);
    List<String> keyColumns = deletes.keyColumns();
    Set<String> equalityKeys = deletes.equalityKeys();

    Set<String> columnsToRead = Sets.newLinkedHashSet();
    columnsToRead.addAll(columns);
    columnsToRead.addAll(keyColumns);
    List<String> readColumns = Lists.newArrayList(columnsToRead);

    StructType readWith = readSchema == null ? null : schemaFor(readColumns, readSchema);
    // the projection to the requested columns happens below, by name, so the read itself can take
    // every column the file has
    JavaPairRDD<Row, Long> data =
        positionedRows(spark, tableSchema, task, readColumns, positions, readWith, conf);

    List<String> capturedReadColumns = readColumns;
    List<String> capturedColumns = columns;
    // a caller may hand over a collection view, such as a map's key set, which Spark cannot ship to
    // an executor; copying it here keeps that from surfacing as a task serialization failure
    Collection<Long> wantedPositions = positions == null ? null : Lists.newArrayList(positions);
    return data.filter(
            (Function<Tuple2<Row, Long>, Boolean>)
                tuple -> isLive(tuple, wantedPositions, deletedPositions, equalityKeys, keyColumns))
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
      Set<String> equalityKeys,
      List<String> keyColumns) {
    long position = tuple._2();
    if (positions != null && !positions.isEmpty() && !positions.contains(position)) {
      return false;
    }

    if (Arrays.binarySearch(deletedPositions, position) >= 0) {
      return false;
    }

    return equalityKeys.isEmpty() || !equalityKeys.contains(equalityKey(tuple._1(), keyColumns));
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
      SerializableConfiguration conf) {
    String dataFile = task.file().location().toString();
    FileFormat format = task.file().format();

    if (format == FileFormat.PARQUET) {
      Dataset<Row> data =
          readSchema == null
              ? spark.read().parquet(dataFile)
              : spark.read().schema(readSchema).parquet(dataFile);
      // The position of a row is its ordinal in the file, so it has to be counted as the rows
      // stream past. zipWithIndex would give the same answer, but it runs a job of its own to size
      // the partitions first, so a lookup that touches ten data files pays for ten jobs and reads
      // each file twice. Merging to one partition first costs nothing when the file was going to
      // be read by a single task anyway, and it is how every other format is read below.
      return data.toJavaRDD()
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
                    readFileWithFormat(file, format, projection, sparkSchema, positions, readConf));
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

  /** Reads every row of one data file with the format model for its format, in file order. */
  private static Iterator<Tuple2<Row, Long>> readFileWithFormat(
      String location,
      FileFormat format,
      Schema projection,
      StructType sparkSchema,
      Collection<Long> positions,
      SerializableConfiguration conf)
      throws IOException {
    long tStart = System.nanoTime();
    InputFile input = HadoopInputFile.fromLocation(location, taskConf(conf));
    long tInput = System.nanoTime();
    ReadBuilder<Record, Void> builder =
        FormatModelRegistry.readBuilder(format, Record.class, input);
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
        Object[] values = new Object[projection.columns().size()];
        for (int i = 0; i < values.length; i++) {
          Types.NestedField field = projection.columns().get(i);
          values[i] = toSparkValue(field.type(), record.getField(field.name()));
        }
        // a format that honoured the positions returned exactly those rows, in ascending order
        long position = wanted == null ? nextPosition : wanted.get((int) nextPosition);
        rows.add(new Tuple2<>(new GenericRowWithSchema(values, sparkSchema), position));
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

    if (wanted != null && rows.size() != wanted.size()) {
      // the format ignored the positions and read the file, which is allowed: number the rows the
      // way an unfiltered read would, so the caller's position filter still picks the hit rows
      List<Tuple2<Row, Long>> numbered = Lists.newArrayListWithCapacity(rows.size());
      for (int i = 0; i < rows.size(); i++) {
        numbered.add(new Tuple2<>(rows.get(i)._1(), (long) i));
      }
      return numbered.iterator();
    }
    return rows.iterator();
  }

  /** The Spark schema of an Iceberg projection, for the rows the format model returns. */
  private static StructType toSparkSchema(Schema schema) {
    StructField[] fields = new StructField[schema.columns().size()];
    for (int i = 0; i < fields.length; i++) {
      Types.NestedField field = schema.columns().get(i);
      fields[i] =
          new StructField(
              field.name(),
              toSparkType(field.type()),
              field.isOptional(),
              org.apache.spark.sql.types.Metadata.empty());
    }
    return new StructType(fields);
  }

  private static DataType toSparkType(org.apache.iceberg.types.Type type) {
    switch (type.typeId()) {
      case BOOLEAN:
        return DataTypes.BooleanType;
      case INTEGER:
      case DATE:
        return DataTypes.IntegerType;
      case LONG:
      case TIME:
      case TIMESTAMP:
      case TIMESTAMP_NANO:
        return DataTypes.LongType;
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
      case LIST:
        return DataTypes.createArrayType(toSparkType(type.asListType().elementType()));
      default:
        throw new UnsupportedOperationException(
            "Unsupported column type for the index layer: " + type);
    }
  }

  /** Converts an Iceberg value into the value a Spark row holds for it. */
  private static Object toSparkValue(org.apache.iceberg.types.Type type, Object value) {
    if (value == null) {
      return null;
    }

    switch (type.typeId()) {
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
      case DATE:
        return (int) ((java.time.LocalDate) value).toEpochDay();
      case TIME:
        return ((java.time.LocalTime) value).toNanoOfDay() / 1000L;
      case TIMESTAMP:
        return microsToSpark(((java.time.OffsetDateTime) value).toInstant());
      case TIMESTAMP_NANO:
        return ((java.time.Instant) value).getEpochSecond() * 1_000_000L
            + ((java.time.Instant) value).getNano() / 1000L;
      default:
        return value;
    }
  }

  private static long microsToSpark(java.time.Instant instant) {
    return instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1000L;
  }

  private static Object[] asValues(Row row, List<String> readColumns, List<String> columns) {
    Object[] values = new Object[columns.size()];
    for (int i = 0; i < values.length; i++) {
      values[i] = row.get(row.fieldIndex(columns.get(i)));
    }
    return values;
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
    List<FileScanTask> tasks = Lists.newArrayList();
    try (CloseableIterable<FileScanTask> planned =
        table.newScan().useSnapshot(snapshotId).planFiles()) {
      tasks.addAll(Lists.newArrayList(planned));
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
  private static final class SnapshotDeletes {

    private static final long[] NO_POSITIONS = new long[0];

    private final Map<String, long[]> deletedPositionsByFile;
    private final Broadcast<Set<String>> equalityKeys;
    private final List<String> keyColumns;

    private SnapshotDeletes(
        Map<String, long[]> deletedPositionsByFile,
        Broadcast<Set<String>> equalityKeys,
        List<String> keyColumns) {
      this.deletedPositionsByFile = deletedPositionsByFile;
      this.equalityKeys = equalityKeys;
      this.keyColumns = keyColumns;
    }

    /** A snapshot without delete files, which is the common case. */
    static SnapshotDeletes none() {
      return new SnapshotDeletes(ImmutableMap.of(), null, ImmutableList.of());
    }

    /** Reads the delete files that apply to the given tasks, in at most two jobs. */
    static SnapshotDeletes read(SparkSession spark, List<FileScanTask> tasks, Schema tableSchema) {
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

      List<String> keyColumns = equalityColumns(tableSchema, ImmutableList.copyOf(equalityDeletes));
      Map<String, long[]> deletedByFile =
          positionDeletes.isEmpty()
              ? ImmutableMap.of()
              : readDeletedPositions(spark, positionDeletes);

      Broadcast<Set<String>> keys = null;
      if (!equalityDeletes.isEmpty()) {
        keys =
            JavaSparkContext.fromSparkContext(spark.sparkContext())
                .broadcast(readEqualityKeys(spark, equalityDeletes, keyColumns));
      }

      return new SnapshotDeletes(deletedByFile, keys, keyColumns);
    }

    /** The positions removed from a data file, sorted so a lookup is a binary search. */
    long[] deletedPositions(String dataFile) {
      long[] positions = deletedPositionsByFile.get(dataFile);
      return positions == null ? NO_POSITIONS : positions;
    }

    Set<String> equalityKeys() {
      return equalityKeys == null ? ImmutableSet.of() : equalityKeys.getValue();
    }

    List<String> keyColumns() {
      return keyColumns;
    }
  }

  /**
   * Reads the positions removed by the given position delete files, grouped by the data file they
   * point at. Position delete files are parquet files with {@code file_path} and {@code pos}
   * columns, and all of them are read in one job.
   */
  private static Map<String, long[]> readDeletedPositions(
      SparkSession spark, Collection<DeleteFile> positionDeletes) {
    Dataset<Row> deletes = null;
    for (DeleteFile deleteFile : positionDeletes) {
      Dataset<Row> file =
          spark
              .read()
              .parquet(deleteFile.location().toString())
              .select(DELETE_FILE_PATH_COLUMN, DELETE_POSITION_COLUMN);
      deletes = deletes == null ? file : deletes.union(file);
    }

    Map<String, List<Long>> byFile = Maps.newHashMap();
    for (Row row : deletes.collectAsList()) {
      byFile.computeIfAbsent(row.getString(0), key -> Lists.newArrayList()).add(row.getLong(1));
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
   * <p>Equality delete files contain one column per equality field id, in field id order; the
   * column name falls back to the field id when the name is not the table's field name.
   */
  private static Set<String> readEqualityKeys(
      SparkSession spark, Collection<DeleteFile> equalityDeletes, List<String> keyColumns) {
    Dataset<Row> deletes = null;
    for (DeleteFile deleteFile : equalityDeletes) {
      Dataset<Row> file = spark.read().parquet(deleteFile.location().toString());
      deletes = deletes == null ? file : deletes.union(file);
    }

    Set<String> keys = Sets.newHashSet();
    for (Row row : deletes.collectAsList()) {
      StringBuilder key = new StringBuilder();
      for (int i = 0; i < keyColumns.size() && i < row.size(); i++) {
        Object value = row.get(i);
        key.append(encodedValue(value)).append(VALUE_SEPARATOR);
      }
      keys.add(key.toString());
    }
    return keys;
  }

  /** The table columns of the equality fields, in field id order. */
  private static List<String> equalityColumns(
      Schema schema, Collection<DeleteFile> equalityDeletes) {
    if (equalityDeletes.isEmpty()) {
      return Lists.newArrayList();
    }

    List<String> columns = Lists.newArrayList();
    for (int fieldId : equalityDeletes.iterator().next().equalityFieldIds()) {
      Types.NestedField field = schema.findField(fieldId);
      columns.add(field == null ? String.valueOf(fieldId) : field.name());
    }
    return columns;
  }

  /**
   * Builds the lookup key of a data row for equality delete matching, in the same encoding as the
   * keys read from the equality delete files.
   */
  private static String equalityKey(Row row, List<String> keyColumns) {
    StringBuilder key = new StringBuilder();
    for (String keyColumn : keyColumns) {
      key.append(encodedValue(row.get(row.fieldIndex(keyColumn)))).append(VALUE_SEPARATOR);
    }
    return key.toString();
  }

  private static String encodedValue(Object value) {
    return value == null ? String.valueOf(NULL_MARKER) : value.getClass().getName() + ":" + value;
  }

  /**
   * Extracts the indexed column's value in a Java representation: array columns come back from
   * Spark rows as Scala sequences, which are converted to Java lists here so adapters do not need
   * Scala.
   */
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
