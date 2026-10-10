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
        .hasMessageContaining("covers source table snapshot 999");
    assertThat(
            reader.uncoveredDataFiles(
                20,
                List.of("data-1.parquet", "data-2.parquet", "data-3.parquet", "data-4.parquet")))
        .containsExactly("data-4.parquet");
  }

  @Test
  public void atOrBeforeAnswersFromTheNewestSnapshotItCanUse(@TempDir Path tempDir)
      throws IOException {
    LocalFileIO fileIO = new LocalFileIO();
    TestingIndexAdapter adapter = new TestingIndexAdapter(TestingIndexDefinition.of(4));
    IndexFileContainer container = new PuffinIndexFileContainer();

    // one index snapshot per source table snapshot, 10 and then 20
    IndexMetadata metadata =
        IndexMetadata.builder()
            .indexUuid("index-uuid")
            .tableUuid("table-uuid")
            .location(tempDir.toString())
            .definition(TestingIndexDefinition.of(4))
            .addSnapshot(snapshot(fileIO, container, adapter, tempDir, 1, 10, "data-1.parquet", 10))
            .addSnapshot(snapshot(fileIO, container, adapter, tempDir, 2, 20, "data-2.parquet", 20))
            .build();

    IndexReader reader = new IndexReader(metadata, fileIO);
    // the exact requirement still picks the snapshot bound to the requested table snapshot
    assertThat(reader.search(IndexQuery.of(20, query(5), 1)).get(0).score()).isEqualTo(15.0);

    // a table snapshot the index has not reached yet is answered from the newest one at or before
    // it, so the results are those of the snapshot bound to table snapshot 20
    IndexQuery stale =
        IndexQuery.of(30, query(5), 1).withConsistency(IndexQuery.Consistency.AT_OR_BEFORE);
    IndexSearchResult hit = reader.search(stale).get(0);
    assertThat(hit.dataFile()).isEqualTo("data-2.parquet");
    assertThat(hit.score()).isEqualTo(15.0);
    // the coverage report is what tells a caller that the index is behind
    assertThat(reader.uncoveredDataFiles(stale, List.of("data-1.parquet", "data-2.parquet")))
        .containsExactly("data-1.parquet");

    // an exact query for that same table snapshot is refused instead of silently going stale
    assertThatThrownBy(() -> reader.search(IndexQuery.of(30, query(5), 1)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("covers source table snapshot 30");
  }

  @Test
  public void atOrBeforeRejectsATableSnapshotOlderThanTheIndex(@TempDir Path tempDir)
      throws IOException {
    LocalFileIO fileIO = new LocalFileIO();
    TestingIndexAdapter adapter = new TestingIndexAdapter(TestingIndexDefinition.of(4));
    IndexFileContainer container = new PuffinIndexFileContainer();
    IndexMetadata metadata =
        IndexMetadata.builder()
            .indexUuid("index-uuid")
            .tableUuid("table-uuid")
            .location(tempDir.toString())
            .definition(TestingIndexDefinition.of(4))
            .addSnapshot(snapshot(fileIO, container, adapter, tempDir, 1, 10, "data-1.parquet", 10))
            .build();

    IndexReader reader = new IndexReader(metadata, fileIO);
    assertThatThrownBy(
            () ->
                reader.search(
                    IndexQuery.of(5, query(5), 1)
                        .withConsistency(IndexQuery.Consistency.AT_OR_BEFORE)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("covers source table snapshot 5");
  }

  /** Writes one index file and its manifest list, and returns a snapshot bound to the source. */
  private static IndexSnapshot snapshot(
      LocalFileIO fileIO,
      IndexFileContainer container,
      TestingIndexAdapter adapter,
      Path tempDir,
      long snapshotId,
      long sourceTableSnapshotId,
      String dataFile,
      int value)
      throws IOException {
    String indexFile = tempDir.resolve("index-file-" + snapshotId + ".puffin").toString();
    container.write(
        fileIO.newOutputFile(indexFile),
        List.of(
            IndexBlob.of(
                adapter.entryBlobType(),
                adapter
                    .encodeEntries(
                        List.of(
                            IndexEntry.of(dataFile, 0, TestingIndexAdapter.valuePayload(value))))
                    .array())));

    String manifest = tempDir.resolve("index-manifest-" + snapshotId + ".imn").toString();
    IndexManifest.write(
        fileIO.newOutputFile(manifest),
        List.of(
            IndexFile.builder()
                .location(indexFile)
                .kind(Kind.ENTRY_BEARING)
                .blobType(adapter.entryBlobType())
                .entryCount(1)
                .coveredDataFiles(List.of(dataFile))
                .build()));

    String list = tempDir.resolve("index-manifest-list-" + snapshotId + ".iml").toString();
    IndexManifestList.write(fileIO.newOutputFile(list), List.of(ManifestRef.of(manifest, 1, 1)));

    return IndexSnapshot.builder()
        .snapshotId(snapshotId)
        .sourceTableSnapshotId(sourceTableSnapshotId)
        .timestampMillis(snapshotId)
        .indexManifestList(list)
        .build();
  }

  private static ByteBuffer query(int value) {
    return ByteBuffer.allocate(4).putInt(value).flip();
  }
}
