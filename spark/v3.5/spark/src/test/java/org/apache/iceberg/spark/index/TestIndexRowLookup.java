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

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.index.spark.IndexEntries;
import org.apache.iceberg.index.vector.VectorIndexAdapter;
import org.apache.iceberg.index.vector.VectorIndexDefinition;
import org.apache.iceberg.index.vector.VectorIndexTypes;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.functions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * What a row lookup costs.
 *
 * <p>A query resolves the hits the index returned by reading the rows they point at, and the
 * position of a row is its ordinal in its data file. Counting the rows as they stream past gives
 * that ordinal in the same pass, which is how every format but parquet is read. Asking Spark for it
 * with {@code zipWithIndex} does not: it runs a job of its own first to size every partition. On a
 * table whose hits land in ten data files that was ten extra jobs and each of those files read
 * twice, which on a million-vector table cost more than the index search it was meant to make
 * cheap. This counts the jobs so it cannot come back unnoticed.
 */
class TestIndexRowLookup {

  private static final int DATA_FILES = 8;
  private static final int ROWS_PER_FILE = 4;

  private static SparkSession spark;
  private static Path warehouseDir;

  @BeforeAll
  static void registerVectorIndexType() {
    VectorIndexTypes.register();
  }

  @BeforeAll
  static void startSpark(@TempDir Path warehouse) {
    warehouseDir = warehouse;
    spark =
        SparkSession.builder()
            .master("local[4]")
            .appName("index-row-lookup")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", warehouse.toUri().toString())
            .getOrCreate();
  }

  @AfterAll
  static void stopSpark() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  public void readsTheRowsOfEveryHitFileInOneJob() throws NoSuchTableException {
    spark.sql("CREATE TABLE local.db.lookup (id bigint, embedding array<double>) USING iceberg");

    // one write per file, each forced to a single task, so a lookup of every file has DATA_FILES
    // files to resolve and each of them has the rows the positions below point at
    for (int file = 0; file < DATA_FILES; file++) {
      spark
          .range(ROWS_PER_FILE)
          .select(
              functions.col("id").plus(file * ROWS_PER_FILE).as("id"),
              functions.array(functions.lit(1.0d), functions.lit(2.0d)).as("embedding"))
          .repartition(1)
          .writeTo("local.db.lookup")
          .append();
    }

    Table table =
        new HadoopTables(spark.sessionState().newHadoopConf())
            .load(warehouseDir.resolve("db/lookup").toUri().toString());
    long snapshotId = table.currentSnapshot().snapshotId();

    List<String> dataFiles = IndexEntries.dataFiles(table, snapshotId);
    assertThat(dataFiles).hasSize(DATA_FILES);

    // positions 1 and 3 out of the four rows every file holds
    Map<String, Collection<Long>> positionsByFile = Maps.newLinkedHashMap();
    for (String dataFile : dataFiles) {
      positionsByFile.put(dataFile, Lists.newArrayList(1L, 3L));
    }

    CountingJobs jobs = new CountingJobs();
    spark.sparkContext().addSparkListener(jobs);
    Map<String, Map<Long, Row>> rows;
    try {
      rows =
          IndexEntries.rowsAt(
              spark, table, snapshotId, positionsByFile, Lists.newArrayList("id"), null);
    } finally {
      spark.sparkContext().removeSparkListener(jobs);
    }

    assertThat(rows).hasSize(DATA_FILES);
    for (String dataFile : dataFiles) {
      // the positions are ordinals in the file, not indexes into the file's partitions
      assertThat(rows.get(dataFile)).containsOnlyKeys(1L, 3L);
    }

    // one job reads the rows of every file. A per-file job would be DATA_FILES of them, which is
    // what a lookup paid before the positions were counted in the same pass as the rows
    // one job reads the rows of every file. The rest are Spark listing each file to build the
    // read, which is metadata only, around thirty milliseconds each here. Counting the positions
    // per file instead used to add a job per file that read the whole file a second time, which is
    // what this bound is here to catch
    assertThat(jobs.started).as("jobs: %s", jobs.calls).isLessThanOrEqualTo(DATA_FILES + 1);
  }

  /**
   * The positions the index records are the row ordinals in the data file, which is what a position
   * delete refers to and what Iceberg's own {@code _pos} metadata column reports. If the two ever
   * disagree, the index points at the wrong rows, so this compares them row for row rather than
   * trusting that a lookup happens to return something plausible.
   */
  @Test
  public void numbersTheRowsTheWayIcebergDoes() throws NoSuchTableException {
    spark.sql(
        "CREATE TABLE local.db.positioned (id bigint, embedding array<double>) USING iceberg");
    for (int file = 0; file < 4; file++) {
      spark
          .range(ROWS_PER_FILE)
          .select(
              functions.col("id").plus(file * ROWS_PER_FILE).as("id"),
              functions.array(functions.lit(1.0d), functions.lit(2.0d)).as("embedding"))
          .repartition(1)
          .writeTo("local.db.positioned")
          .append();
    }

    Table table =
        new HadoopTables(spark.sessionState().newHadoopConf())
            .load(warehouseDir.resolve("db/positioned").toUri().toString());
    long snapshotId = table.currentSnapshot().snapshotId();

    Map<String, Map<Long, Long>> fromIceberg = Maps.newLinkedHashMap();
    for (Row row : spark.sql("SELECT _file, _pos, id FROM local.db.positioned").collectAsList()) {
      fromIceberg
          .computeIfAbsent(fileName(row.getString(0)), key -> Maps.newLinkedHashMap())
          .put(row.getLong(1), row.getLong(2));
    }

    VectorIndexAdapter adapter =
        new VectorIndexAdapter(
            VectorIndexDefinition.builder()
                .fieldId(2)
                .dimension(2)
                .metric(VectorIndexDefinition.Metric.EUCLIDEAN)
                .build());

    Map<String, Map<Long, Long>> fromEntries = Maps.newLinkedHashMap();
    Map<String, Collection<Long>> allPositions = Maps.newLinkedHashMap();
    for (Row row :
        IndexEntries.entries(spark, table, snapshotId, "embedding", adapter).collectAsList()) {
      fromEntries.computeIfAbsent(fileName(row.getString(0)), key -> Maps.newLinkedHashMap());
      allPositions
          .computeIfAbsent(fileName(row.getString(0)), key -> Lists.newArrayList())
          .add(row.getLong(1));
    }

    assertThat(fromIceberg).isNotEmpty();
    assertThat(fromEntries.keySet()).isEqualTo(fromIceberg.keySet());

    // resolving each position through the lookup has to produce the row Iceberg says is there
    Map<String, Collection<Long>> positionsByFile = Maps.newLinkedHashMap();
    for (String file : fromIceberg.keySet()) {
      positionsByFile.put(fileName(file), fromIceberg.get(file).keySet());
    }
    Map<String, Map<Long, Row>> resolved = Maps.newLinkedHashMap();
    for (Map.Entry<String, Map<Long, Row>> entry :
        IndexEntries.rowsAt(
                spark,
                table,
                snapshotId,
                positionsByFileByName(IndexEntries.dataFiles(table, snapshotId), positionsByFile),
                Lists.newArrayList("id"),
                null)
            .entrySet()) {
      resolved.put(fileName(entry.getKey()), entry.getValue());
    }

    for (String file : fromIceberg.keySet()) {
      assertThat(resolved.get(file)).as("the rows of %s", file).isNotNull();
      for (Map.Entry<Long, Long> position : fromIceberg.get(file).entrySet()) {
        Row row = resolved.get(file).get(position.getKey());
        assertThat(row).as("row at %s of %s", position.getKey(), file).isNotNull();
        assertThat(row.getLong(0))
            .as("id at position %s of %s", position.getKey(), file)
            .isEqualTo(position.getValue());
      }
    }
  }

  private static Map<String, Collection<Long>> positionsByFileByName(
      List<String> dataFiles, Map<String, Collection<Long>> positionsByFileName) {
    Map<String, Collection<Long>> byPath = Maps.newLinkedHashMap();
    for (String dataFile : dataFiles) {
      Collection<Long> positions = positionsByFileName.get(fileName(dataFile));
      if (positions != null) {
        byPath.put(dataFile, positions);
      }
    }
    return byPath;
  }

  private static String fileName(String location) {
    return location.substring(location.lastIndexOf('/') + 1);
  }

  private static class CountingJobs extends SparkListener {
    private int started = 0;
    private final java.util.List<String> calls = Lists.newArrayList();
    private final java.util.List<Long> millis = Lists.newArrayList();

    private final java.util.Map<Integer, Long> startedAt = new java.util.HashMap<>();

    @Override
    public void onJobEnd(SparkListenerJobEnd jobEnd) {
      Long began = startedAt.get(jobEnd.jobId());
      if (began != null) {
        millis.add(System.currentTimeMillis() - began);
      }
    }

    @Override
    public void onJobStart(SparkListenerJobStart jobStart) {
      started += 1;
      StringBuilder sb = new StringBuilder();
      for (Object stage : scala.collection.JavaConverters.seqAsJavaList(jobStart.stageInfos())) {
        sb.append(((org.apache.spark.scheduler.StageInfo) stage).name());
      }
      calls.add(sb.toString());
      startedAt.put(jobStart.jobId(), System.currentTimeMillis());
    }
  }
}
