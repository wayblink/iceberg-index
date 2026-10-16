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
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.index.CreateIndexRequest;
import org.apache.iceberg.index.IndexCatalog;
import org.apache.iceberg.index.IndexFile;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.IndexQuery;
import org.apache.iceberg.index.IndexReader;
import org.apache.iceberg.index.SidecarIndexCatalog;
import org.apache.iceberg.index.spark.SparkIndexBuilder;
import org.apache.iceberg.index.vector.VectorIndexAdapter;
import org.apache.iceberg.index.vector.VectorIndexDefinition;
import org.apache.iceberg.index.vector.VectorIndexDefinition.Metric;
import org.apache.iceberg.index.vector.VectorIndexTypes;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.functions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * What a build does not do: move the vectors.
 *
 * <p>One index file is built per group of data files, and the graph of a group is built by the task
 * that reads that group, so the vectors of a group never have to travel. Grouping the rows by index
 * file with a shuffle, which is the obvious way to write it and the way it was written, moved every
 * vector in the table across the cluster to reach the task that was to build its graph: five
 * gigabytes on a ten million vector table, and a lost executor then cost the whole stage rather
 * than one partition of work.
 *
 * <p>This counts the bytes written to shuffle during a build, so the movement cannot come back
 * without the test noticing.
 */
class TestIndexBuildShuffle {

  private static final int DATA_FILES = 4;
  private static final int ROWS_PER_FILE = 8;

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
            .appName("index-build-shuffle")
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
  public void buildsWithoutRedistributingTheVectors() throws NoSuchTableException {
    spark.sql("CREATE TABLE local.db.build (id bigint, embedding array<double>) USING iceberg");

    // one write per file, each forced to a single task, so the build has several groups whose
    // vectors would have to be shuffled into place if it grouped them the obvious way
    for (int file = 0; file < DATA_FILES; file++) {
      spark
          .range(ROWS_PER_FILE)
          .select(
              functions.col("id").plus(file * ROWS_PER_FILE).as("id"),
              functions
                  .array(functions.col("id").cast("double"), functions.lit((double) file))
                  .as("embedding"))
          .repartition(1)
          .writeTo("local.db.build")
          .append();
    }

    Table table =
        new HadoopTables(spark.sessionState().newHadoopConf())
            .load(warehouseDir.resolve("db/build").toUri().toString());
    long snapshotId = table.currentSnapshot().snapshotId();

    IndexCatalog indexCatalog = new SidecarIndexCatalog(table.io());
    IndexMetadata metadata =
        indexCatalog.createIndex(
            CreateIndexRequest.builder()
                .tableLocation(table.location())
                .tableUuid(table.uuid().toString())
                .indexUuid("build-vectors")
                .definition(
                    VectorIndexDefinition.builder()
                        .fieldId(2)
                        .dimension(2)
                        .metric(Metric.EUCLIDEAN)
                        .build())
                .build());

    AtomicLong shuffled = new AtomicLong();
    SparkListener listener =
        new SparkListener() {
          @Override
          public void onTaskEnd(SparkListenerTaskEnd end) {
            if (end.taskMetrics() != null) {
              shuffled.addAndGet(end.taskMetrics().shuffleWriteMetrics().bytesWritten());
            }
          }
        };

    IndexMetadata built;
    spark.sparkContext().addSparkListener(listener);
    try {
      built = SparkIndexBuilder.build(spark, table, metadata, snapshotId, "embedding", 0L);
    } finally {
      spark.sparkContext().removeSparkListener(listener);
    }

    assertThat(built.snapshots()).hasSize(1);

    // one index file per data file, which is the 1:1 layout the default budget gives
    VectorIndexAdapter adapter = new VectorIndexAdapter((VectorIndexDefinition) built.definition());
    IndexReader reader = new IndexReader(built, table.io());
    List<IndexFile> indexFiles =
        reader.indexFiles(
            IndexQuery.of(snapshotId, adapter.encodeQuery(new float[] {0.0f, 0.0f}), 1));
    assertThat(indexFiles).hasSize(DATA_FILES);
    assertThat(shuffled.get())
        .as("bytes written to shuffle while building, which should be none")
        .isZero();
  }
}
