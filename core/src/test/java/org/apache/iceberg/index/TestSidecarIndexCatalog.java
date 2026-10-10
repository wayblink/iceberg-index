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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.iceberg.TestTables.LocalFileIO;
import org.apache.iceberg.index.IndexFile.Kind;
import org.apache.iceberg.index.IndexManifestList.ManifestRef;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestSidecarIndexCatalog {

  @BeforeAll
  static void registerTestingIndexType() {
    IndexTypes.register(new TestingIndexType());
  }

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
    File metadataDir = new File(purged.location() + "/metadata");

    IndexMetadata kept = catalog.createIndex(request(tableLocation, "index-keep"));

    assertThat(catalog.dropIndex(tableLocation, "index-purge", true)).isTrue();
    assertThat(new File(manifestList)).doesNotExist();
    assertThat(new File(indexFile)).doesNotExist();
    // every metadata version and both pointer slots are gone
    assertThat(metadataDir.listFiles()).isEmpty();
    assertThat(catalog.listIndexes(tableLocation)).containsExactly(kept);

    assertThat(catalog.dropIndex(tableLocation, "index-keep", false)).isTrue();
    assertThat(catalog.listIndexes(tableLocation)).isEmpty();
    assertThat(new File(kept.location() + "/metadata/index-metadata-v1-g1.json")).exists();

    assertThat(catalog.dropIndex(tableLocation, "index-keep", false)).isFalse();
  }

  @Test
  public void publishesMetadataVersionsAndSwitchesThePointer(@TempDir Path tempDir)
      throws IOException {
    LocalFileIO fileIO = new LocalFileIO();
    IndexCatalog catalog = new SidecarIndexCatalog(fileIO);
    String tableLocation = tempDir.resolve("table").toString();

    IndexMetadata created = catalog.createIndex(request(tableLocation, "index-uuid"));
    File metadataDir = new File(created.location() + "/metadata");
    assertThat(new File(created.location() + "/metadata/index-metadata-v1-g1.json")).exists();
    assertThat(new File(created.location() + "/metadata/index-metadata-pointer-0.json")).exists();
    assertThat(pointerGeneration(metadataDir, 0)).isEqualTo(1);

    // the first update goes to slot 1, the second back to slot 0, so no pointer is ever
    // overwritten in place
    String first = writeSnapshot(fileIO, created);
    catalog.updateIndex(withSnapshot(created, first));
    assertThat(pointerGeneration(metadataDir, 1)).isEqualTo(2);
    assertThat(pointerGeneration(metadataDir, 0)).isEqualTo(1);

    String second = writeSnapshot(fileIO, created, 2);
    IndexMetadata thirdVersion = withSnapshot(withSnapshot(created, first), second);
    catalog.updateIndex(thirdVersion);
    assertThat(pointerGeneration(metadataDir, 0)).isEqualTo(3);
    assertThat(pointerGeneration(metadataDir, 1)).isEqualTo(2);
    assertThat(new File(created.location() + "/metadata/index-metadata-v1-g3.json")).exists();
    assertThat(catalog.loadIndex(tableLocation, "index-uuid")).isEqualTo(thirdVersion);
  }

  @Test
  public void readersNeverSeeAnUnpublishedMetadataVersion(@TempDir Path tempDir)
      throws IOException {
    LocalFileIO fileIO = new LocalFileIO();
    IndexCatalog catalog = new SidecarIndexCatalog(fileIO);
    String tableLocation = tempDir.resolve("table").toString();

    IndexMetadata created = catalog.createIndex(request(tableLocation, "index-uuid"));
    String manifestList = writeSnapshot(fileIO, created);
    IndexMetadata published = withSnapshot(created, manifestList);
    catalog.updateIndex(published);

    // simulate a crash between writing the next metadata version and switching the pointer: the
    // new version exists as a file, but the pointer still names the published one
    IndexMetadata unpublished = withSnapshot(published, manifestList);
    String unpublishedPath = created.location() + "/metadata/index-metadata-v1-g3.json";
    Files.write(Path.of(unpublishedPath), IndexMetadataParser.toJson(unpublished).getBytes(UTF_8));

    assertThat(catalog.loadIndex(tableLocation, "index-uuid")).isEqualTo(published);
    // the unpublished version is simply unreferenced, so it is harmless
    assertThat(new File(unpublishedPath)).exists();
  }

  @Test
  public void fallsBackToTheOlderSlotWhenAPointerIsUnreadable(@TempDir Path tempDir)
      throws IOException {
    LocalFileIO fileIO = new LocalFileIO();
    IndexCatalog catalog = new SidecarIndexCatalog(fileIO);
    String tableLocation = tempDir.resolve("table").toString();

    IndexMetadata created = catalog.createIndex(request(tableLocation, "index-uuid"));
    String manifestList = writeSnapshot(fileIO, created);
    IndexMetadata published = withSnapshot(created, manifestList);
    catalog.updateIndex(published);

    // truncate the newest pointer: the reader falls back to the other slot rather than failing
    File newestPointer = new File(created.location() + "/metadata/index-metadata-pointer-1.json");
    Files.write(newestPointer.toPath(), "{trunc".getBytes(UTF_8));

    assertThat(catalog.loadIndex(tableLocation, "index-uuid")).isEqualTo(created);
  }

  @Test
  public void readsAndUpdatesThePrePointerLayout(@TempDir Path tempDir) throws IOException {
    LocalFileIO fileIO = new LocalFileIO();
    IndexCatalog catalog = new SidecarIndexCatalog(fileIO);
    String tableLocation = tempDir.resolve("table").toString();

    // an index written before the pointer switch: a single metadata file, no pointer
    IndexMetadata legacy =
        IndexMetadata.builder()
            .indexUuid("index-uuid")
            .tableUuid("table-uuid")
            .tableLocation(tableLocation)
            .location(tableLocation + "/indexes/index-uuid")
            .definition(TestingIndexDefinition.of(4))
            .build();
    String legacyPath = legacy.location() + "/metadata/index-metadata-v1.json";
    Files.createDirectories(Path.of(legacyPath).getParent());
    Files.write(Path.of(legacyPath), IndexMetadataParser.toJson(legacy).getBytes(UTF_8));
    Files.write(
        Path.of(tableLocation, "indexes/index-registry.json"),
        ("{\"format-version\":1,\"index-uuids\":[\"index-uuid\"]}").getBytes(UTF_8));

    assertThat(catalog.loadIndex(tableLocation, "index-uuid")).isEqualTo(legacy);

    // updating such an index moves it onto the pointer layout
    String manifestList = writeSnapshot(fileIO, legacy);
    catalog.updateIndex(withSnapshot(legacy, manifestList));
    assertThat(new File(legacy.location() + "/metadata/index-metadata-pointer-0.json")).exists();
    assertThat(new File(legacy.location() + "/metadata/index-metadata-v1-g1.json")).exists();
    assertThat(catalog.loadIndex(tableLocation, "index-uuid").snapshots()).hasSize(1);
  }

  private static long pointerGeneration(File metadataDir, int slot) throws IOException {
    String json =
        new String(
            Files.readAllBytes(
                metadataDir.toPath().resolve("index-metadata-pointer-" + slot + ".json")),
            UTF_8);
    return Long.parseLong(json.replaceAll("[^0-9]", "").substring(0, 1));
  }

  private static CreateIndexRequest request(String tableLocation, String indexUuid) {
    return CreateIndexRequest.builder()
        .tableLocation(tableLocation)
        .tableUuid("table-uuid")
        .indexUuid(indexUuid)
        .definition(TestingIndexDefinition.of(4))
        .build();
  }

  /** Writes one entry-bearing index file, its manifest and manifest list under the index tree. */
  private static String writeSnapshot(LocalFileIO fileIO, IndexMetadata metadata)
      throws IOException {
    return writeSnapshot(fileIO, metadata, 1);
  }

  private static String writeSnapshot(LocalFileIO fileIO, IndexMetadata metadata, int seq)
      throws IOException {
    TestingIndexAdapter adapter =
        new TestingIndexAdapter((TestingIndexDefinition) metadata.definition());
    IndexFileContainer container = new PuffinIndexFileContainer();

    String indexFile = metadata.location() + "/data/index-file-" + seq + ".puffin";
    container.write(
        fileIO.newOutputFile(indexFile),
        List.of(
            IndexBlob.of(
                adapter.entryBlobType(),
                adapter
                    .encodeEntries(
                        List.of(
                            IndexEntry.of(
                                "data-1.parquet", 0, TestingIndexAdapter.valuePayload(10))))
                    .array())));

    String manifest = metadata.location() + "/snapshots/index-manifest-" + seq + ".imn";
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

    String manifestList = metadata.location() + "/snapshots/index-manifest-list-" + seq + ".iml";
    IndexManifestList.write(
        fileIO.newOutputFile(manifestList), List.of(ManifestRef.of(manifest, 1, 1)));
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
