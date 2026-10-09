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

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.apache.iceberg.TestTables.LocalFileIO;
import org.apache.iceberg.index.IndexFile.Kind;
import org.apache.iceberg.index.VectorIndexDefinition.Metric;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestSidecarIndexCatalog {

  @Test
  public void createsLoadsAndListsIndexes(@TempDir Path tempDir) {
    LocalFileIO fileIO = new LocalFileIO();
    IndexCatalog catalog = new SidecarIndexCatalog(fileIO);
    String tableLocation = tempDir.resolve("table").toString();

    CreateIndexRequest request = request(tableLocation, "index-uuid-1");
    IndexMetadata created = catalog.createIndex(request);
    assertThat(created.location()).isEqualTo(tableLocation + "/indexes/index-uuid-1");
    assertThat(created.snapshots()).isEmpty();
    assertThat(catalog.loadIndex(tableLocation, "index-uuid-1")).isEqualTo(created);

    assertThatThrownBy(() -> catalog.createIndex(request))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("already exists");

    IndexMetadata createdSecond = catalog.createIndex(request(tableLocation, "index-uuid-2"));
    assertThat(catalog.listIndexes(tableLocation)).containsExactly(created, createdSecond);

    assertThatThrownBy(() -> catalog.loadIndex(tableLocation, "missing"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("not found");
  }

  @Test
  public void updatesIndexMetadata(@TempDir Path tempDir) throws IOException {
    LocalFileIO fileIO = new LocalFileIO();
    IndexCatalog catalog = new SidecarIndexCatalog(fileIO);
    String tableLocation = tempDir.resolve("table").toString();

    IndexMetadata created = catalog.createIndex(request(tableLocation, "index-uuid"));
    String manifestList = writeSnapshot(fileIO, created);
    IndexMetadata updated = withSnapshot(created, manifestList);

    catalog.updateIndex(updated);
    assertThat(catalog.loadIndex(tableLocation, "index-uuid")).isEqualTo(updated);
  }

  @Test
  public void dropsWithAndWithoutPurge(@TempDir Path tempDir) throws IOException {
    LocalFileIO fileIO = new LocalFileIO();
    IndexCatalog catalog = new SidecarIndexCatalog(fileIO);
    String tableLocation = tempDir.resolve("table").toString();

    IndexMetadata purged = catalog.createIndex(request(tableLocation, "index-purge"));
    String manifestList = writeSnapshot(fileIO, purged);
    catalog.updateIndex(withSnapshot(purged, manifestList));
    String indexFile = purged.location() + "/data/index-file-1.puffin";
    String metadataFile = purged.location() + "/metadata/index-metadata-v1.json";

    IndexMetadata kept = catalog.createIndex(request(tableLocation, "index-keep"));

    assertThat(catalog.dropIndex(tableLocation, "index-purge", true)).isTrue();
    assertThat(new File(manifestList)).doesNotExist();
    assertThat(new File(indexFile)).doesNotExist();
    assertThat(new File(metadataFile)).doesNotExist();
    assertThat(catalog.listIndexes(tableLocation)).containsExactly(kept);

    assertThat(catalog.dropIndex(tableLocation, "index-keep", false)).isTrue();
    assertThat(catalog.listIndexes(tableLocation)).isEmpty();
    assertThat(new File(kept.location() + "/metadata/index-metadata-v1.json")).exists();

    assertThat(catalog.dropIndex(tableLocation, "index-keep", false)).isFalse();
  }

  private static CreateIndexRequest request(String tableLocation, String indexUuid) {
    return CreateIndexRequest.builder()
        .tableLocation(tableLocation)
        .tableUuid("table-uuid")
        .indexUuid(indexUuid)
        .definition(
            VectorIndexDefinition.builder().fieldId(1).dimension(2).metric(Metric.COSINE).build())
        .build();
  }

  /** Writes one entry-bearing index file, its manifest and manifest list under the index tree. */
  private static String writeSnapshot(LocalFileIO fileIO, IndexMetadata metadata)
      throws IOException {
    VectorIndexAdapter adapter =
        new VectorIndexAdapter((VectorIndexDefinition) metadata.definition());
    IndexFileContainer container = new PuffinIndexFileContainer();

    String indexFile = metadata.location() + "/data/index-file-1.puffin";
    container.write(
        fileIO.newOutputFile(indexFile),
        List.of(
            IndexBlob.of(
                adapter.entryBlobType(),
                adapter
                    .encodeEntries(
                        List.of(
                            IndexEntry.of(
                                "data-1.parquet",
                                0,
                                adapter.encodeVector(new float[] {1.0f, 0.0f}))))
                    .array())));

    String manifest = metadata.location() + "/snapshots/index-manifest-1.imn";
    IndexManifest.write(
        fileIO.newOutputFile(manifest),
        List.of(
            IndexFile.builder()
                .location(indexFile)
                .kind(Kind.ENTRY_BEARING)
                .blobType(adapter.entryBlobType())
                .entryCount(1)
                .coveredDataFiles(List.of("data-1.parquet"))
                .build()));

    String manifestList = metadata.location() + "/snapshots/index-manifest-list-1.iml";
    IndexManifestList.write(
        fileIO.newOutputFile(manifestList),
        List.of(IndexManifestList.ManifestRef.of(manifest, 1, 1)));
    return manifestList;
  }

  private static IndexMetadata withSnapshot(IndexMetadata metadata, String manifestList) {
    return IndexMetadata.builder()
        .indexUuid(metadata.indexUuid())
        .tableUuid(metadata.tableUuid())
        .tableLocation(metadata.tableLocation())
        .location(metadata.location())
        .definition(metadata.definition())
        .properties(metadata.properties())
        .addSnapshot(
            IndexSnapshot.builder()
                .snapshotId(1)
                .sourceTableSnapshotId(10)
                .timestampMillis(20)
                .indexManifestList(manifestList)
                .build())
        .build();
  }
}
