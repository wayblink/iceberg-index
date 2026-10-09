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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.index.vector.VectorIndexAdapter;
import org.apache.iceberg.index.vector.VectorIndexDefinition;
import org.apache.iceberg.index.vector.VectorIndexDefinition.Metric;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
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
 * Reads index entries for a vector column from an Iceberg table snapshot.
 *
 * <p>Row positions are computed per data file with {@code zipWithIndex}, so every entry carries the
 * {@code _file + _pos} anchor the index layer uses to resolve search hits back to table rows. The
 * vector value is encoded into the entry payload by the core vector adapter, which accepts any
 * numeric array column.
 *
 * <p>The returned DataFrame has columns {@code data_file} (string), {@code pos} (long) and {@code
 * payload} (binary), ready to be written by {@link SparkIndexBuilder}.
 */
public final class VectorIndexEntries {

  /** Schema of the entries DataFrame produced by {@link #entries}. */
  public static final StructType SCHEMA =
      new StructType()
          .add("data_file", DataTypes.StringType, false)
          .add("pos", DataTypes.LongType, false)
          .add("payload", DataTypes.BinaryType, false);

  /** Name of the data file column in {@link #SCHEMA}. */
  public static final String DATA_FILE_COLUMN = "data_file";

  private VectorIndexEntries() {}

  /**
   * Reads index entries for the vector column from the given table snapshot.
   *
   * @param spark the Spark session
   * @param table the Iceberg table
   * @param snapshotId the source table snapshot to read
   * @param vectorColumn the name of the numeric array column to index
   * @param definition the vector index definition
   */
  public static Dataset<Row> entries(
      SparkSession spark,
      Table table,
      long snapshotId,
      String vectorColumn,
      VectorIndexDefinition definition) {
    int fieldId = definition.fieldId();
    int dimension = definition.dimension();
    Metric metric = definition.metric();

    Dataset<Row> entries = null;
    for (String dataFile : dataFiles(table, snapshotId)) {
      JavaRDD<Row> fileEntries =
          spark
              .read()
              .parquet(dataFile)
              .select(vectorColumn)
              .toJavaRDD()
              .zipWithIndex()
              .mapPartitions(
                  (FlatMapFunction<Iterator<Tuple2<Row, Long>>, Row>)
                      iterator -> {
                        VectorIndexAdapter adapter =
                            new VectorIndexAdapter(
                                VectorIndexDefinition.builder()
                                    .fieldId(fieldId)
                                    .dimension(dimension)
                                    .metric(metric)
                                    .build());
                        List<Row> rows = Lists.newArrayList();
                        while (iterator.hasNext()) {
                          Tuple2<Row, Long> tuple = iterator.next();
                          rows.add(
                              RowFactory.create(
                                  dataFile,
                                  tuple._2(),
                                  adapter.encodeVector(toFloats(tuple._1(), vectorColumn))));
                        }
                        return rows.iterator();
                      });
      Dataset<Row> fileDataset = spark.createDataFrame(fileEntries, SCHEMA);
      entries = entries == null ? fileDataset : entries.union(fileDataset);
    }

    return entries == null ? spark.createDataFrame(Lists.newArrayList(), SCHEMA) : entries;
  }

  /** Lists the data file locations of the given table snapshot, on the driver. */
  public static List<String> dataFiles(Table table, long snapshotId) {
    try (CloseableIterable<FileScanTask> tasks =
        table.newScan().useSnapshot(snapshotId).planFiles()) {
      List<String> dataFiles = Lists.newArrayList();
      for (FileScanTask task : tasks) {
        dataFiles.add(task.file().location().toString());
      }
      return dataFiles;
    } catch (IOException e) {
      throw new UncheckedIOException(
          "Failed to list data files for snapshot " + snapshotId + " of table " + table.name(), e);
    }
  }

  private static float[] toFloats(Row row, String vectorColumn) {
    List<Object> values = row.getList(row.fieldIndex(vectorColumn));
    float[] vector = new float[values.size()];
    for (int i = 0; i < values.size(); i++) {
      vector[i] = ((Number) values.get(i)).floatValue();
    }
    return vector;
  }
}
