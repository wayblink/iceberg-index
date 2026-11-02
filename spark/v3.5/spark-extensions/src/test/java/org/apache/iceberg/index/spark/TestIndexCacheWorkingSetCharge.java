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

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import org.apache.iceberg.Table;
import org.apache.iceberg.index.IndexFile;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.IndexQuery;
import org.apache.iceberg.index.IndexReader;
import org.apache.iceberg.index.vector.VectorIndexAdapter;
import org.apache.iceberg.index.vector.VectorIndexDefinition;
import org.apache.iceberg.index.vector.VectorIndexTypes;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * What a query's budget is charged for, and that both sides of the charge agree.
 *
 * <p>A budget is only a budget if the number it is derived from is the number admission charges:
 * deriving it from the size of the files on disk while charging what they cost once prepared leaves
 * the budget short by the difference between the two, which is what made a derived budget hold only
 * part of the files a query reads. The two numbers are computed on different paths here on purpose,
 * so a change that moves one and not the other shows up as a failure rather than as a cache that
 * quietly stops working.
 */
class TestIndexCacheWorkingSetCharge {
  private static SparkSession spark;

  @BeforeAll
  static void startSpark(@TempDir Path warehouse) {
    VectorIndexTypes.register();
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("index-cache-working-set")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
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
  void aQueryIsChargedForWhatTheFilesCostPrepared() throws Exception {
    String name = "working_set_charge";
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
    spark.sql(
        "CREATE TABLE local.db." + name + " (id bigint, embedding array<double>) USING iceberg");
    for (int file = 0; file < 6; file++) {
      spark.sql(
          "INSERT INTO local.db."
              + name
              + " VALUES ("
              + file
              + ", array(CAST("
              + file
              + " AS double), CAST("
              + (6 - file)
              + " AS double)))");
    }
    spark.sql(
        "CALL local.system.build_vector_index(table => 'db."
            + name
            + "', column => 'embedding', dimension => 2, metric => 'euclidean', "
            + "structure => 'IVF', quantizer => 'RABITQ', options => map('nlist', '2', "
            + "'nprobe', '2'), max_entries_per_index_file => 2)");

    Table table = Spark3Util.loadIcebergTable(spark, "local.db." + name);
    IndexMetadata metadata = VectorIndexes.findIndex(table, "embedding", "vector");
    List<IndexFile> files =
        new IndexReader(metadata, table)
            .indexFiles(
                IndexQuery.of(
                    metadata.latestSnapshot().sourceTableSnapshotId(), ByteBuffer.allocate(0), 0));
    VectorIndexAdapter adapter =
        new VectorIndexAdapter((VectorIndexDefinition) metadata.definition());
    assertThat(files).as("the fixture needs more than one index file").hasSizeGreaterThan(1);

    long onDisk = 0L;
    long charged = 0L;
    for (IndexFile file : files) {
      onDisk += read(table, file.location()).length;
      // the admitted size, read the way a task reads it
      charged += DistributedIndexSearch.loadFile(table.io(), file.location(), adapter).bytes();
    }

    // the budget a query derives is only defined for an owner: owned cache metadata is what the
    // projection charges, so without one the size is reported as unknown rather than guessed
    assertThat(DistributedIndexSearch.workingSetBytes(table.io(), files, adapter))
        .as("an unowned working set is unknown")
        .isEqualTo(-1);

    long budget =
        DistributedIndexSearch.workingSetBytes(table.io(), files, adapter, table.location());
    // a prepared structure costs more than the bytes it was read from, so the budget has to exceed
    // both the container on disk and the payload the reader admitted
    assertThat(budget)
        .as("a prepared structure costs more than the bytes it was read from: disk=%s", onDisk)
        .isGreaterThan(onDisk);
    assertThat(budget)
        .as("the budget covers at least the bytes admission read: charged=%s", charged)
        .isGreaterThanOrEqualTo(charged);

    // and it is the charge admission arrives at, not a separate estimate
    List<IndexCacheProtocol.File> planned = Lists.newArrayList();
    for (IndexFile file : files) {
      planned.add(
          IndexCacheProtocol.File.builder()
              .location(file.location())
              .owner(table.location())
              .cacheKey(
                  DistributedIndexSearch.cacheKey(
                      table.location(),
                      DistributedIndexSearch.adapterKey(adapter),
                      file.location()))
              .memory(file.memory())
              .estimatedBytes(IndexCacheMemory.residentBytes(file.memory()))
              .build());
    }

    assertThat(budget)
        .as("the budget is derived from the same charge admission uses")
        .isEqualTo(ExecutorIndexCache.Shape.empty().project(null, 0, planned).charge().usedBytes());
  }

  private static byte[] read(Table table, String location) {
    try (org.apache.iceberg.io.SeekableInputStream in =
        table.io().newInputFile(location).newStream()) {
      return org.apache.iceberg.relocated.com.google.common.io.ByteStreams.toByteArray(in);
    } catch (java.io.IOException e) {
      throw new java.io.UncheckedIOException("Cannot read " + location, e);
    }
  }
}
