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
package org.apache.iceberg.index;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import org.apache.iceberg.TestTables.LocalFileIO;
import org.apache.iceberg.index.IndexFile.Kind;
import org.apache.iceberg.index.IndexManifestList.ManifestRef;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestIndexReader {

  @BeforeAll
  static void registerTestingIndexType() {
    IndexTypes.register(new TestingIndexType());
  }

  @Test
  public void searchesSnapshotAndReportsUncoveredFiles(@TempDir Path tempDir) throws IOException {
    LocalFileIO fileIO = new LocalFileIO();
    TestingIndexAdapter adapter = new TestingIndexAdapter(TestingIndexDefinition.of(4));
    IndexFileContainer container = new PuffinIndexFileContainer();

    String firstFile = tempDir.resolve("index-file-1.puffin").toString();
    String secondFile = tempDir.resolve("index-file-2.puffin").toString();
    String manifestLocation = tempDir.resolve("index-manifest-1.imn").toString();
    String listLocation = tempDir.resolve("index-manifest-list-1.iml").toString();

    container.write(
        fileIO.newOutputFile(firstFile),
        List.of(
            IndexBlob.of(
                adapter.entryBlobType(),
                adapter
                    .encodeEntries(
                        List.of(
                            IndexEntry.of(
                                "data-1.parquet", 0, TestingIndexAdapter.valuePayload(10)),
                            IndexEntry.of(
                                "data-2.parquet", 1, TestingIndexAdapter.valuePayload(1))))
                    .array())));
    container.write(
        fileIO.newOutputFile(secondFile),
        List.of(
            IndexBlob.of(
                adapter.entryBlobType(),
                adapter
                    .encodeEntries(
                        List.of(
                            IndexEntry.of(
                                "data-3.parquet", 3, TestingIndexAdapter.valuePayload(8))))
                    .array())));

    IndexManifest.write(
        fileIO.newOutputFile(manifestLocation),
        List.of(
            IndexFile.builder()
                .location(firstFile)
                .kind(Kind.ENTRY_BEARING)
                .blobType(adapter.entryBlobType())
                .entryCount(2)
                .coveredDataFiles(List.of("data-1.parquet", "data-2.parquet"))
                .build(),
            IndexFile.builder()
                .location(secondFile)
                .kind(Kind.ENTRY_BEARING)
                .blobType(adapter.entryBlobType())
                .entryCount(1)
                .coveredDataFiles(List.of("data-3.parquet"))
                .build()));
    IndexManifestList.write(
        fileIO.newOutputFile(listLocation), List.of(ManifestRef.of(manifestLocation, 2, 3)));

    IndexMetadata metadata =
        IndexMetadata.builder()
            .indexUuid("index-uuid")
            .tableUuid("table-uuid")
            .location(tempDir.toString())
            .definition(TestingIndexDefinition.of(4))
            .addSnapshot(
                IndexSnapshot.builder()
                    .snapshotId(10)
                    .sourceTableSnapshotId(20)
                    .timestampMillis(30)
                    .indexManifestList(listLocation)
                    .build())
            .build();

    IndexReader reader = new IndexReader(metadata, fileIO);
    assertThat(reader.search(IndexQuery.of(20, query(5), 2)))
        .extracting(IndexSearchResult::dataFile)
        .containsExactly("data-1.parquet", "data-3.parquet");
    assertThat(reader.search(IndexQuery.of(20, query(5), 0))).isEmpty();
    assertThatThrownBy(() -> reader.search(IndexQuery.of(999, query(5), 1)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("does not cover");
    assertThat(
            reader.uncoveredDataFiles(
                20,
                List.of("data-1.parquet", "data-2.parquet", "data-3.parquet", "data-4.parquet")))
        .containsExactly("data-4.parquet");
  }

  private static ByteBuffer query(int value) {
    return ByteBuffer.allocate(4).putInt(value).flip();
  }
}
