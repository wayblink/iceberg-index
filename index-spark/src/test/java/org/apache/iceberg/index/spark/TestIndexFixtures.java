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

import java.io.File;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.index.IndexBuildRequest;
import org.apache.iceberg.index.IndexBuilder;
import org.apache.iceberg.index.IndexEntry;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.vector.VectorIndexAdapter;
import org.apache.iceberg.index.vector.VectorIndexDefinition;
import org.apache.iceberg.index.vector.VectorIndexTypes;
import org.apache.iceberg.index.vector.VectorKernels;
import org.apache.iceberg.types.Types;

/** Builds a real table, a real index, and the files a control command has to plan. */
final class TestIndexFixtures {
  // TestTables keeps one static metadata map per table name, so each fixture needs its own name.
  private static final AtomicInteger SEQUENCE = new AtomicInteger();

  static {
    VectorIndexTypes.register();
    VectorKernels.registerDefaults();
  }

  private TestIndexFixtures() {}

  static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

  record BuiltIndex(TestTables.TestTable table, IndexMetadata metadata) {}

  static BuiltIndex build(File tempDir, String indexUuid, String... dataFiles) {
    TestTables.TestTable table =
        TestTables.create(
            tempDir, "t" + SEQUENCE.incrementAndGet(), SCHEMA, PartitionSpec.unpartitioned(), 2);
    for (String dataFile : dataFiles) {
      table
          .newAppend()
          .appendFile(
              DataFiles.builder(PartitionSpec.unpartitioned())
                  .withPath(tempDir.toPath().resolve(dataFile).toString())
                  .withFileSizeInBytes(10L)
                  .withRecordCount(1L)
                  .build())
          .commit();
    }

    VectorIndexDefinition definition =
        VectorIndexDefinition.builder()
            .fieldId(1)
            .dimension(2)
            .metric(VectorIndexDefinition.Metric.COSINE)
            .structure(VectorIndexDefinition.VectorStructure.FLAT)
            .build();
    IndexMetadata metadata =
        IndexMetadata.builder()
            .indexUuid(indexUuid)
            .tableUuid(table.uuid().toString())
            .tableLocation(table.location())
            .location(tempDir.toPath().resolve("indexes/" + indexUuid).toString())
            .definition(definition)
            .build();

    VectorIndexAdapter adapter = new VectorIndexAdapter(definition);
    IndexMetadata built =
        new IndexBuilder(table.io())
            .build(
                IndexBuildRequest.builder()
                    .currentMetadata(metadata)
                    .sourceTableSnapshotId(table.currentSnapshot().snapshotId())
                    .entries(
                        List.of(
                            IndexEntry.of(
                                dataFiles.length == 0 ? "data-1.parquet" : dataFiles[0],
                                0,
                                adapter.encodeVector(new float[] {1.0f, 0.0f})),
                            IndexEntry.of(
                                dataFiles.length < 2 ? "data-2.parquet" : dataFiles[1],
                                1,
                                adapter.encodeVector(new float[] {0.0f, 1.0f}))))
                    .timestampMillis(30L)
                    .build());
    return new BuiltIndex(table, built);
  }

  /** Index metadata bound to a table but with no snapshot, for negative cases. */
  static IndexMetadata metadataFor(org.apache.iceberg.Table table, String indexUuid) {
    VectorIndexDefinition definition =
        VectorIndexDefinition.builder()
            .fieldId(1)
            .dimension(2)
            .metric(VectorIndexDefinition.Metric.COSINE)
            .structure(VectorIndexDefinition.VectorStructure.FLAT)
            .build();
    return IndexMetadata.builder()
        .indexUuid(indexUuid)
        .tableUuid(table.uuid().toString())
        .tableLocation(table.location())
        .location(table.location() + "/indexes/" + indexUuid)
        .definition(definition)
        .build();
  }

  /** The adapter segment of the cache key: adapter class and the blob type it writes. */
  static String adapterKeyFor(IndexMetadata metadata) {
    String[] parts = cacheKeyFor(metadata, "").split("\\|");
    return parts[1] + "|" + parts[2];
  }

  /** The cache key the search path builds for this index: table|adapter|blobType|file. */
  static String cacheKeyFor(IndexMetadata metadata, String fileLocation) {
    VectorIndexAdapter adapter =
        new VectorIndexAdapter((VectorIndexDefinition) metadata.definition());
    String adapterKey = DistributedIndexSearch.adapterKey(adapter);
    return metadata.tableLocation() + "|" + adapterKey + "|" + fileLocation;
  }
}
