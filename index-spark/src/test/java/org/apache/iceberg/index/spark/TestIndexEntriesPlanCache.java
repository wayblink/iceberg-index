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

import java.nio.file.Path;
import java.util.List;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A snapshot's plan is cached on the driver unless the caller turns that off.
 *
 * <p>The cache is what makes the second plan of a snapshot a lookup rather than a pass over the
 * object store; the switch that turns the driver-side caches off has to take it out of both sides,
 * so a caller that asks for no caching plans the snapshot again and records nothing.
 */
class TestIndexEntriesPlanCache {
  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

  @Test
  void cachingThePlanCanBeDisabledPerCall(@TempDir Path tempDir) {
    TestTables.TestTable table =
        TestTables.create(tempDir.toFile(), "plan-cache", SCHEMA, PartitionSpec.unpartitioned(), 2);
    table
        .newAppend()
        .appendFile(
            DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath(tempDir.resolve("data-0.parquet").toString())
                .withFileSizeInBytes(10L)
                .withRecordCount(1L)
                .build())
        .commit();
    long snapshotId = table.currentSnapshot().snapshotId();

    List<FileScanTask> firstUncached = IndexEntries.scanTasks(table, snapshotId, false);
    List<FileScanTask> secondUncached = IndexEntries.scanTasks(table, snapshotId, false);
    assertThat(secondUncached)
        .as("a call that may not cache plans the snapshot again")
        .isNotSameAs(firstUncached);

    List<FileScanTask> firstCached = IndexEntries.scanTasks(table, snapshotId);
    List<FileScanTask> secondCached = IndexEntries.scanTasks(table, snapshotId);
    assertThat(secondCached)
        .as("a repeated plan of the same snapshot is the cached one")
        .isSameAs(firstCached);
    assertThat(firstCached)
        .as("the plan of an uncached call is not what a caching call is handed")
        .isNotSameAs(secondUncached);
  }
}
