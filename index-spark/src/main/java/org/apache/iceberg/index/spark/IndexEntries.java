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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.index.IndexAdapter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
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
    Schema tableSchema = table.schema();

    Dataset<Row> entries = null;
    for (FileScanTask task : scanTasks(table, snapshotId)) {
      String dataFile = task.file().location().toString();
      List<DeleteFile> deleteFiles = Lists.newArrayList(task.deletes());

      // delete files are read on the driver so the executor-side filter is a simple lookup
      long[] deletedPositions = readDeletedPositions(spark, positionDeletes(deleteFiles), dataFile);
      List<String> keyColumns = equalityColumns(tableSchema, equalityDeletes(deleteFiles));
      Set<String> equalityKeys = readEqualityKeys(spark, equalityDeletes(deleteFiles), keyColumns);

      // the data read needs the indexed column plus the columns referenced by equality deletes
      Set<String> columnsToRead = Sets.newLinkedHashSet();
      columnsToRead.add(column);
      columnsToRead.addAll(keyColumns);
      List<String> readColumns = Lists.newArrayList(columnsToRead);
      List<String> capturedKeyColumns = keyColumns;
      long[] capturedDeletedPositions = deletedPositions;
      Set<String> capturedEqualityKeys = equalityKeys;

      Dataset<Row> data = spark.read().parquet(dataFile);
      for (String readColumn : readColumns) {
        data = data.select(readColumn);
      }

      JavaRDD<Row> entryRows =
          data.toJavaRDD()
              .zipWithIndex()
              .mapPartitions(
                  (FlatMapFunction<Iterator<Tuple2<Row, Long>>, Row>)
                      iterator -> {
                        List<Row> fileEntries = Lists.newArrayList();
                        while (iterator.hasNext()) {
                          Tuple2<Row, Long> tuple = iterator.next();
                          long position = tuple._2();
                          if (containsPosition(capturedDeletedPositions, position)) {
                            continue;
                          }

                          Row row = tuple._1();
                          if (!capturedEqualityKeys.isEmpty()
                              && capturedEqualityKeys.contains(
                                  equalityKey(row, capturedKeyColumns))) {
                            continue;
                          }

                          fileEntries.add(
                              RowFactory.create(
                                  dataFile,
                                  position,
                                  adapter.encodeEntryValue(columnValue(row, column))));
                        }
                        return fileEntries.iterator();
                      });

      Dataset<Row> fileEntries = spark.createDataFrame(entryRows, SCHEMA);
      entries = entries == null ? fileEntries : entries.union(fileEntries);
    }

    return entries == null ? spark.createDataFrame(Lists.newArrayList(), SCHEMA) : entries;
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

  private static List<FileScanTask> scanTasks(Table table, long snapshotId) {
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
   * Reads the positions removed from a data file by the given position delete files. Position
   * delete files are parquet files with {@code file_path} and {@code pos} columns; only rows
   * pointing at the given data file are collected.
   */
  private static long[] readDeletedPositions(
      SparkSession spark, List<DeleteFile> positionDeletes, String dataFile) {
    List<Long> positions = Lists.newArrayList();
    for (DeleteFile deleteFile : positionDeletes) {
      List<Row> rows =
          spark
              .read()
              .parquet(deleteFile.location().toString())
              .select(DELETE_FILE_PATH_COLUMN, DELETE_POSITION_COLUMN)
              .collectAsList();
      for (Row row : rows) {
        if (dataFile.equals(row.getString(0))) {
          positions.add(row.getLong(1));
        }
      }
    }

    long[] result = new long[positions.size()];
    for (int i = 0; i < result.length; i++) {
      result[i] = positions.get(i);
    }
    return result;
  }

  /**
   * Reads the equality keys removed by the given equality delete files, one key per delete row. The
   * key is the values of the equality columns in field id order, with a null marker for nulls.
   *
   * <p>Equality delete files contain one column per equality field id, in field id order; the
   * column name falls back to the field id when the name is not the table's field name.
   */
  private static Set<String> readEqualityKeys(
      SparkSession spark, List<DeleteFile> equalityDeletes, List<String> keyColumns) {
    Set<String> keys = Sets.newHashSet();
    for (DeleteFile deleteFile : equalityDeletes) {
      List<Row> rows = spark.read().parquet(deleteFile.location().toString()).collectAsList();
      for (Row row : rows) {
        StringBuilder key = new StringBuilder();
        for (int i = 0; i < keyColumns.size() && i < row.size(); i++) {
          Object value = row.get(i);
          key.append(encodedValue(value)).append(VALUE_SEPARATOR);
        }
        keys.add(key.toString());
      }
    }
    return keys;
  }

  /** The table columns of the equality fields, in field id order. */
  private static List<String> equalityColumns(Schema schema, List<DeleteFile> equalityDeletes) {
    if (equalityDeletes.isEmpty()) {
      return Lists.newArrayList();
    }

    List<String> columns = Lists.newArrayList();
    for (int fieldId : equalityDeletes.get(0).equalityFieldIds()) {
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
  private static Object columnValue(Row row, String column) {
    Object value = row.get(row.fieldIndex(column));
    if (value instanceof scala.collection.Seq) {
      return scala.collection.JavaConverters.seqAsJavaList((scala.collection.Seq<?>) value);
    }
    return value;
  }

  private static boolean containsPosition(long[] positions, long position) {
    for (long candidate : positions) {
      if (candidate == position) {
        return true;
      }
    }
    return false;
  }

  private static float[] toFloats(List<Object> values) {
    float[] vector = new float[values.size()];
    for (int i = 0; i < values.size(); i++) {
      vector[i] = ((Number) values.get(i)).floatValue();
    }
    return vector;
  }
}
