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
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * How many tasks reading index files as binary files produces, which is what decides the
 * parallelism of a distributed index search.
 */
class TestBinaryFilePartitioning {

  private static SparkSession spark;

  @BeforeAll
  static void startSpark(@TempDir Path tempDir) {
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("binary-file-partitioning")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "4")
            // without this, Spark raises the partition count to the default parallelism, which
            // hides
            // how the files themselves are packed
            .config("spark.sql.files.minPartitionNum", "1")
            .getOrCreate();
  }

  @AfterAll
  static void stopSpark() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  public void howFilesBecomePartitions(@TempDir Path tempDir) throws Exception {
    List<String> paths = writeFiles(tempDir, 8, 1024);

    // eight files of 1 KB, packed up to the default 128 MB, land in one partition
    assertThat(partitions(paths)).isEqualTo(1);

    // a budget below the size of a file puts every file in a partition of its own
    spark.conf().set("spark.sql.files.maxPartitionBytes", "1");
    assertThat(partitions(paths)).isEqualTo(8);
    spark.conf().unset("spark.sql.files.maxPartitionBytes");

    // a minimum partition count above what the packing produced splits the packed groups further,
    // so the parallelism of the cluster is used even when the files are small
    spark.conf().set("spark.sql.files.minPartitionNum", "3");
    assertThat(partitions(paths)).isEqualTo(3);
    spark.conf().set("spark.sql.files.minPartitionNum", "8");
    assertThat(partitions(paths)).isEqualTo(8);
    spark.conf().unset("spark.sql.files.minPartitionNum");

    // a file never lands in two partitions, so a count above the number of files is not reached
    spark.conf().set("spark.sql.files.minPartitionNum", "16");
    assertThat(partitions(paths)).isEqualTo(8);
    spark.conf().unset("spark.sql.files.minPartitionNum");

    // a file counts as at least the open cost towards the budget, not as its own size: eight 1 KB
    // files against a 2 MB budget are split, because each of them counts as the 4 MB open cost
    spark.conf().set("spark.sql.files.maxPartitionBytes", String.valueOf(2 * 1024 * 1024));
    assertThat(partitions(paths)).isEqualTo(8);
    spark.conf().unset("spark.sql.files.maxPartitionBytes");
  }

  private int partitions(List<String> paths) {
    return spark
        .read()
        .format("binaryFile")
        .load(paths.toArray(new String[0]))
        .rdd()
        .getNumPartitions();
  }

  private static List<String> writeFiles(Path dir, int count, int size) throws Exception {
    List<String> paths = new java.util.ArrayList<>();
    for (int i = 0; i < count; i++) {
      java.io.File file = dir.resolve("index-file-" + i).toFile();
      try (java.io.OutputStream out = new java.io.FileOutputStream(file)) {
        out.write(new byte[size]);
      }
      paths.add(file.toURI().toString());
    }
    return paths;
  }
}
