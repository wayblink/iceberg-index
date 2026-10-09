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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestIndexBuilder {

  @BeforeAll
  static void registerTestingIndexType() {
    IndexTypes.register(new TestingIndexType());
  }

  @Test
  public void buildsFullSnapshotAndReaderCanSearchIt(@TempDir Path tempDir) throws IOException {
    LocalFileIO fileIO = new LocalFileIO();
    TestingIndexAdapter adapter = new TestingIndexAdapter(TestingIndexDefinition.of(4));
    IndexBuilder builder =
        new IndexBuilder(new IndexSnapshotWriter(fileIO, new PuffinIndexFileContainer(), 2));
    IndexMetadata current = indexMetadata(tempDir, "indexes/index-uuid");

    IndexMetadata updated =
        builder.build(
            IndexBuildRequest.builder()
                .currentMetadata(current)
                .sourceTableSnapshotId(10)
                .entries(entries(adapter))
                .timestampMillis(30)
                .build());

    assertThat(updated.snapshots()).hasSize(1);
    IndexSnapshot snapshot = updated.snapshots().get(0);
    assertThat(snapshot.snapshotId()).isEqualTo(1L);
    assertThat(snapshot.sourceTableSnapshotId()).isEqualTo(10L);
    assertThat(snapshot.parentIndexSnapshotId()).isNull();
    assertThat(snapshot.status()).isEqualTo(IndexSnapshot.Status.PUBLISHED);

    // the snapshot's files describe two entry-bearing index files with the expected coverage
    IndexManifestList manifestList =
        IndexManifestList.read(fileIO.newInputFile(snapshot.indexManifestList()));
    assertThat(manifestList.manifests()).hasSize(1);
    IndexManifest manifest =
        IndexManifest.read(fileIO.newInputFile(manifestList.manifests().get(0).location()));
    assertThat(manifest.indexFiles()).hasSize(2);
    assertThat(manifest.indexFiles()).extracting(IndexFile::kind).containsOnly(Kind.ENTRY_BEARING);
    assertThat(manifest.indexFiles())
        .extracting(indexFile -> indexFile.coveredDataFiles().get(0))
        .containsExactly("data-1.parquet", "data-2.parquet");
    assertThat(manifest.indexFiles()).extracting(IndexFile::entryCount).containsExactly(2L, 1L);
    assertThat(manifest.indexedDataFiles())
        .containsExactlyInAnyOrder("data-1.parquet", "data-2.parquet");

    // the reader can search the built snapshot end to end
    IndexReader reader = new IndexReader(updated, fileIO);
    assertThat(reader.search(IndexQuery.of(10, query(5), 2)))
        .extracting(IndexSearchResult::dataFile)
        .containsExactly("data-1.parquet", "data-2.parquet");
    assertThat(reader.uncoveredDataFiles(10, List.of("data-1.parquet", "data-3.parquet")))
        .containsExactly("data-3.parquet");
  }

  @Test
  public void rejectsAlreadyCoveredSourceSnapshot(@TempDir Path tempDir) {
    LocalFileIO fileIO = new LocalFileIO();
    IndexBuilder builder = new IndexBuilder(fileIO);
    IndexMetadata current = indexMetadata(tempDir, "indexes/index-uuid");

    IndexBuildRequest request =
        IndexBuildRequest.builder()
            .currentMetadata(current)
            .sourceTableSnapshotId(10)
            .entries(entries(new TestingIndexAdapter(TestingIndexDefinition.of(4))))
            .timestampMillis(30)
            .build();
    IndexMetadata updated = builder.build(request);

    IndexBuildRequest second =
        IndexBuildRequest.builder()
            .currentMetadata(updated)
            .sourceTableSnapshotId(10)
            .entries(entries(new TestingIndexAdapter(TestingIndexDefinition.of(4))))
            .timestampMillis(30)
            .build();
    assertThatThrownBy(() -> builder.build(second))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("already covers");
  }

  private static ByteBuffer query(int value) {
    return ByteBuffer.allocate(4).putInt(value).flip();
  }

  private static List<IndexEntry> entries(TestingIndexAdapter adapter) {
    return List.of(
        IndexEntry.of("data-1.parquet", 0, TestingIndexAdapter.valuePayload(10)),
        IndexEntry.of("data-1.parquet", 1, TestingIndexAdapter.valuePayload(20)),
        IndexEntry.of("data-2.parquet", 2, TestingIndexAdapter.valuePayload(15)));
  }

  private static IndexMetadata indexMetadata(Path tempDir, String indexLocation) {
    return IndexMetadata.builder()
        .indexUuid("index-uuid")
        .tableUuid("table-uuid")
        .tableLocation(tempDir.resolve("table").toString())
        .location(tempDir.resolve(indexLocation).toString())
        .definition(TestingIndexDefinition.of(4))
        .build();
  }
}
