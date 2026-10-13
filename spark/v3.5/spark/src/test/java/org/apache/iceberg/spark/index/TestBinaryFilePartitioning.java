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
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
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
  public void smallFilesArePackedIntoSinglePartitionsByDefault(@TempDir Path tempDir)
      throws Exception {
    List<String> paths = writeFiles(tempDir, 8, 1024);

    Dataset<Row> blobs = spark.read().format("binaryFile").load(paths.toArray(new String[0]));

    // without a partition budget the files are packed up to the default 128 MB, so eight small
    // files
    // end up in one task and a distributed index search would not be distributed at all
    assertThat(blobs.rdd().getNumPartitions()).isEqualTo(1);
  }

  @Test
  public void aPartitionBudgetOfOneByteGivesOnePartitionPerFile(@TempDir Path tempDir)
      throws Exception {
    List<String> paths = writeFiles(tempDir, 8, 1024);

    spark.conf().set("spark.sql.files.maxPartitionBytes", "1");
    Dataset<Row> blobs = spark.read().format("binaryFile").load(paths.toArray(new String[0]));

    // a file is never split, so a budget below the size of a file puts every file in a partition of
    // its own, which is the unit of work an index search wants
    assertThat(blobs.rdd().getNumPartitions()).isEqualTo(8);
    spark.conf().unset("spark.sql.files.maxPartitionBytes");
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
