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
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.TestTables.LocalFileIO;
import org.apache.iceberg.index.VectorIndexDefinition.Metric;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestIndexRefresher {

  @Test
  public void reusesUnchangedIndexFiles(@TempDir Path tempDir) throws IOException {
    LocalFileIO fileIO = new LocalFileIO();
    VectorIndexAdapter adapter = adapter();
    IndexSnapshotWriter writer = new IndexSnapshotWriter(fileIO, new PuffinIndexFileContainer(), 1);
    IndexBuilder builder = new IndexBuilder(writer);
    IndexRefresher refresher = new IndexRefresher(fileIO, writer);
    IndexMetadata current = indexMetadata(tempDir);

    IndexMetadata first =
        builder.build(
            IndexBuildRequest.builder()
                .currentMetadata(current)
                .sourceTableSnapshotId(10)
                .entries(entries(adapter))
                .timestampMillis(30)
                .build());
    IndexSnapshot firstSnapshot = first.snapshots().get(0);
    String reusedLocation = indexFilesOf(fileIO, firstSnapshot).get(1).location();

    // table snapshot 20: data-1 was rewritten (its entries move to data-3), data-2 unchanged
    IndexMetadata second =
        refresher.refresh(
            IndexBuildRequest.builder()
                .currentMetadata(first)
                .sourceTableSnapshotId(20)
                .entries(
                    List.of(
                        IndexEntry.of(
                            "data-3.parquet", 0, adapter.encodeVector(new float[] {0.6f, 0.8f}))))
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
    assertThat(reader.search(IndexQuery.of(20, adapter.encodeQuery(new float[] {1.0f, 0.0f}), 1)))
        .extracting(IndexSearchResult::dataFile)
        .containsExactly("data-3.parquet");
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

  private static VectorIndexAdapter adapter() {
    return new VectorIndexAdapter(
        VectorIndexDefinition.builder().fieldId(1).dimension(2).metric(Metric.COSINE).build());
  }

  private static List<IndexEntry> entries(VectorIndexAdapter adapter) {
    return List.of(
        IndexEntry.of("data-1.parquet", 0, adapter.encodeVector(new float[] {1.0f, 0.0f})),
        IndexEntry.of("data-2.parquet", 1, adapter.encodeVector(new float[] {0.0f, 1.0f})));
  }

  private static IndexMetadata indexMetadata(Path tempDir) {
    return IndexMetadata.builder()
        .indexUuid("index-uuid")
        .tableUuid("table-uuid")
        .tableLocation(tempDir.resolve("table").toString())
        .location(tempDir.resolve("indexes/index-uuid").toString())
        .definition(
            VectorIndexDefinition.builder().fieldId(1).dimension(2).metric(Metric.COSINE).build())
        .build();
  }
}
