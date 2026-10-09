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
import java.util.Set;
import org.apache.iceberg.TestTables.LocalFileIO;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestIndexRefresher {

  @BeforeAll
  static void registerTestingIndexType() {
    IndexTypes.register(new TestingIndexType());
  }

  @Test
  public void reusesUnchangedIndexFiles(@TempDir Path tempDir) throws IOException {
    LocalFileIO fileIO = new LocalFileIO();
    TestingIndexAdapter adapter = new TestingIndexAdapter(TestingIndexDefinition.of(4));
    IndexSnapshotWriter writer = new IndexSnapshotWriter(fileIO, new PuffinIndexFileContainer(), 1);
    IndexBuilder builder = new IndexBuilder(writer);
    IndexRefresher refresher = new IndexRefresher(fileIO, writer);
    IndexMetadata current = indexMetadata(tempDir);

    IndexMetadata first =
        builder.build(
            IndexBuildRequest.builder()
                .currentMetadata(current)
                .sourceTableSnapshotId(10)
                .entries(
                    List.of(
                        IndexEntry.of("data-1.parquet", 0, TestingIndexAdapter.valuePayload(10)),
                        IndexEntry.of("data-2.parquet", 1, TestingIndexAdapter.valuePayload(20))))
                .timestampMillis(30)
                .build());
    IndexSnapshot firstSnapshot = first.snapshots().get(0);
    String reusedLocation = indexFilesOf(fileIO, firstSnapshot).get(1).location();

    // table snapshot 20: data-1 was rewritten (its entry moves to data-3), data-2 unchanged
    IndexMetadata second =
        refresher.refresh(
            IndexBuildRequest.builder()
                .currentMetadata(first)
                .sourceTableSnapshotId(20)
                .entries(
                    List.of(
                        IndexEntry.of("data-3.parquet", 0, TestingIndexAdapter.valuePayload(15))))
                .timestampMillis(40)
                .build(),
            Set.of("data-1.parquet"));

    assertThat(second.snapshots()).hasSize(2);
    IndexSnapshot secondSnapshot = second.snapshots().get(1);
    assertThat(secondSnapshot.snapshotId()).isEqualTo(2L);
    assertThat(secondSnapshot.sourceTableSnapshotId()).isEqualTo(20L);
    assertThat(secondSnapshot.parentIndexSnapshotId()).isEqualTo(1L);

    List<IndexFile> indexFiles = indexFilesOf(fileIO, secondSnapshot);
    assertThat(indexFiles).hasSize(2);
    assertThat(indexFiles.get(0).location()).isEqualTo(reusedLocation);
    assertThat(indexFiles.get(1).coveredDataFiles()).containsExactly("data-3.parquet");
    assertThat(indexFiles.get(1).entryCount()).isEqualTo(1L);

    // the refreshed snapshot is searchable and reports correct coverage
    IndexReader reader = new IndexReader(second, fileIO);
    assertThat(reader.search(IndexQuery.of(20, query(5), 1)))
        .extracting(IndexSearchResult::dataFile)
        .containsExactly("data-2.parquet");
    assertThat(
            reader.uncoveredDataFiles(
                20, List.of("data-2.parquet", "data-3.parquet", "data-4.parquet")))
        .containsExactly("data-4.parquet");
  }

  @Test
  public void rejectsRefreshWithoutSnapshot(@TempDir Path tempDir) {
    IndexRefresher refresher = new IndexRefresher(new LocalFileIO());
    assertThatThrownBy(
            () ->
                refresher.refresh(
                    IndexBuildRequest.builder()
                        .currentMetadata(indexMetadata(tempDir))
                        .sourceTableSnapshotId(10)
                        .entries(List.of())
                        .timestampMillis(30)
                        .build(),
                    Set.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("without an index snapshot");
  }

  private static List<IndexFile> indexFilesOf(LocalFileIO fileIO, IndexSnapshot snapshot)
      throws IOException {
    IndexManifestList manifestList =
        IndexManifestList.read(fileIO.newInputFile(snapshot.indexManifestList()));
    return IndexManifest.read(fileIO.newInputFile(manifestList.manifests().get(0).location()))
        .indexFiles();
  }

  private static ByteBuffer query(int value) {
    return ByteBuffer.allocate(4).putInt(value).flip();
  }

  private static IndexMetadata indexMetadata(Path tempDir) {
    return IndexMetadata.builder()
        .indexUuid("index-uuid")
        .tableUuid("table-uuid")
        .tableLocation(tempDir.resolve("table").toString())
        .location(tempDir.resolve("indexes/index-uuid").toString())
        .definition(TestingIndexDefinition.of(4))
        .build();
  }
}
