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

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iceberg.TestTables.LocalFileIO;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.index.IndexFile.Kind;
import org.apache.iceberg.index.IndexManifestList.ManifestRef;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
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
  public void publishesAboveTheGenerationAnotherWriterClaimed(@TempDir Path tempDir)
      throws IOException {
    LocalFileIO fileIO = new LocalFileIO();
    IndexCatalog catalog = new SidecarIndexCatalog(fileIO);
    String tableLocation = tempDir.resolve("table").toString();
    IndexMetadata created = catalog.createIndex(request(tableLocation, "index-uuid"));

    // another publisher already claimed generation 2: its slot names its document, which is what a
    // claim is now. This one has to publish above that claim rather than replace a document a
    // reader may already be following
    IndexMetadata claimed = withSnapshot(created, writeSnapshot(fileIO, created, 2), 2, 11);
    File claimedFile = new File(created.location() + "/metadata/index-metadata-v1-g2.json");
    Files.write(claimedFile.toPath(), IndexMetadataParser.toJson(claimed).getBytes(UTF_8));
    Files.write(
        Path.of(created.location() + "/metadata/index-metadata-pointer-1.json"),
        ("{\"generation\":2,\"metadata-file\":\"" + claimedFile + "\"}").getBytes(UTF_8));

    IndexMetadata updated = withSnapshot(created, writeSnapshot(fileIO, created));
    catalog.updateIndex(updated);

    assertThat(pointerGeneration(new File(created.location() + "/metadata"), 0)).isEqualTo(3);
    assertThat(new String(Files.readAllBytes(claimedFile.toPath()), UTF_8))
        .as("the document another writer claimed is left alone")
        .isEqualTo(IndexMetadataParser.toJson(claimed));
    assertThat(
            IndexMetadataParser.fromJson(
                new String(Files.readAllBytes(claimedFile.toPath()), UTF_8)))
        .isEqualTo(claimed);
  }

  @Test
  public void failsWhenAnotherPublisherSupersedesThePointerItself(@TempDir Path tempDir)
      throws IOException {
    // the pointer is a small file overwritten in place, so a publisher that writes the other slot
    // can move the index past a publication that has just landed. Reading the pointer back across
    // both slots turns that into a failure instead of a publication nobody will follow
    LocalFileIO delegate = new LocalFileIO();
    AtomicBoolean armed = new AtomicBoolean();
    AtomicBoolean superseded = new AtomicBoolean();
    FileIO racing =
        new FileIO() {
          @Override
          public InputFile newInputFile(String path) {
            return delegate.newInputFile(path);
          }

          @Override
          public OutputFile newOutputFile(String path) {
            if (armed.get()
                && path.contains("index-metadata-pointer-")
                && superseded.compareAndSet(false, true)) {
              writePointerOfAnotherPublisher(delegate, path);
            }

            return delegate.newOutputFile(path);
          }

          @Override
          public void deleteFile(String path) {
            delegate.deleteFile(path);
          }
        };

    IndexCatalog catalog = new SidecarIndexCatalog(racing);
    String tableLocation = tempDir.resolve("table").toString();
    IndexMetadata created = catalog.createIndex(request(tableLocation, "index-uuid"));
    IndexMetadata updated = withSnapshot(created, writeSnapshot(delegate, created));
    armed.set(true);

    assertThatThrownBy(() -> catalog.updateIndex(updated))
        .isInstanceOf(CommitFailedException.class)
        .hasMessageContaining("pointer");
  }

  @Test
  public void appliesAChangeAgainWhenAnotherPublisherGotThereFirst(@TempDir Path tempDir)
      throws IOException {
    // the change lands a competing publication on its first invocation, which makes the caller's
    // base stale in exactly the way a concurrent publisher does; the change has to be re-applied to
    // the newer document rather than published above a snapshot it never saw
    LocalFileIO fileIO = new LocalFileIO();
    IndexCatalog catalog = new SidecarIndexCatalog(fileIO);
    String tableLocation = tempDir.resolve("table").toString();
    IndexMetadata created = catalog.createIndex(request(tableLocation, "index-uuid"));
    IndexMetadata first = withSnapshot(created, writeSnapshot(fileIO, created, 1), 1, 10);
    IndexMetadata second = withSnapshot(first, writeSnapshot(fileIO, first, 2), 2, 11);
    AtomicBoolean landed = new AtomicBoolean();

    IndexMetadata published =
        catalog.updateIndex(
            tableLocation,
            "index-uuid",
            current -> {
              if (landed.compareAndSet(false, true)) {
                // another publisher appends its own snapshot between this one's read and its
                // publish
                catalog.updateIndex(
                    tableLocation, "index-uuid", seen -> append(seen, first.latestSnapshot()));
              }

              return append(current, second.latestSnapshot());
            });

    assertThat(published.snapshots())
        .extracting(IndexSnapshot::sourceTableSnapshotId)
        .as("the change was applied to the document the competing publisher left behind")
        .containsExactly(10L, 11L);
    assertThat(catalog.loadIndex(tableLocation, "index-uuid").snapshots())
        .extracting(IndexSnapshot::sourceTableSnapshotId)
        .containsExactly(10L, 11L);
  }

  @Test
  public void readsTheGenerationWithTheDocumentItAppliesTo(@TempDir Path tempDir)
      throws IOException {
    // A change is applied to a document, and the generation it is published into has to come from
    // the same read. A publisher that reads the document at generation N and then learns about
    // generation N+1 from a second read of the pointer publishes a document that never saw the
    // snapshot published at N+1, and claims the generation after it. Neither the claim nor the
    // pointer read-back notices: both only ask whether the generation this writer wrote was taken,
    // never whether the document it derived that generation from is still the current one.
    LocalFileIO delegate = new LocalFileIO();
    IndexCatalog bootstrap = new SidecarIndexCatalog(delegate);
    String tableLocation = tempDir.resolve("table").toString();
    IndexMetadata created = bootstrap.createIndex(request(tableLocation, "index-uuid"));
    IndexMetadata first = withSnapshot(created, writeSnapshot(delegate, created, 1), 1, 10);
    IndexMetadata second = withSnapshot(first, writeSnapshot(delegate, first, 2), 2, 11);

    AtomicBoolean raced = new AtomicBoolean();
    IndexCatalog competitor = new SidecarIndexCatalog(delegate);
    FileIO racing =
        new FileIO() {
          @Override
          public InputFile newInputFile(String path) {
            // the document is read after the pointer has been resolved, which is exactly where a
            // second pointer read used to be spliced in
            if (path.matches(".*index-metadata-v1-g\\d+\\.json$")
                && raced.compareAndSet(false, true)) {
              System.out.println("HOOK fires on " + path);
              competitor.updateIndex(
                  tableLocation, "index-uuid", seen -> append(seen, first.latestSnapshot()));
            }

            return delegate.newInputFile(path);
          }

          @Override
          public OutputFile newOutputFile(String path) {
            return delegate.newOutputFile(path);
          }

          @Override
          public void deleteFile(String path) {
            delegate.deleteFile(path);
          }
        };

    IndexCatalog catalog = new SidecarIndexCatalog(racing);
    IndexMetadata published =
        catalog.updateIndex(
            tableLocation, "index-uuid", current -> append(current, second.latestSnapshot()));

    assertThat(pointerGeneration(new File(created.location() + "/metadata"), 0))
        .as("the publication claimed the generation after the document it read")
        .isEqualTo(3);
    assertThat(published.snapshots())
        .extracting(IndexSnapshot::sourceTableSnapshotId)
        .containsExactlyInAnyOrder(10L, 11L);
    assertThat(catalog.loadIndex(tableLocation, "index-uuid").snapshots())
        .extracting(IndexSnapshot::sourceTableSnapshotId)
        .as("neither the competing snapshot nor this one is dropped")
        .containsExactlyInAnyOrder(10L, 11L);
  }

  @Test
  public void twoConcurrentPublishersKeepBothSnapshots(@TempDir Path tempDir) throws Exception {
    LocalFileIO fileIO = new LocalFileIO();
    IndexCatalog catalog = new SidecarIndexCatalog(fileIO);
    String tableLocation = tempDir.resolve("table").toString();
    IndexMetadata created = catalog.createIndex(request(tableLocation, "index-uuid"));
    IndexMetadata first = withSnapshot(created, writeSnapshot(fileIO, created, 1), 1, 10);
    IndexMetadata second = withSnapshot(first, writeSnapshot(fileIO, first, 2), 2, 11);
    IndexSnapshot[] snapshots = {first.latestSnapshot(), second.latestSnapshot()};

    CyclicBarrier start = new CyclicBarrier(2);
    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      List<Future<IndexMetadata>> published = Lists.newArrayList();
      for (IndexSnapshot snapshot : snapshots) {
        published.add(
            pool.submit(
                () -> {
                  start.await();
                  return catalog.updateIndex(
                      tableLocation, "index-uuid", current -> append(current, snapshot));
                }));
      }

      for (Future<IndexMetadata> future : published) {
        future.get(2, TimeUnit.MINUTES);
      }
    } finally {
      pool.shutdownNow();
    }

    assertThat(catalog.loadIndex(tableLocation, "index-uuid").snapshots())
        .extracting(IndexSnapshot::sourceTableSnapshotId)
        .as("neither publisher's snapshot is lost to the other")
        .containsExactlyInAnyOrder(10L, 11L);
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

  /**
   * Writes a pointer that names a later generation, the way a publisher that just finished would.
   *
   * <p>The slot is the other one, so the publication under test lands in its own slot and is only
   * visible as superseded when the two slots are compared. The generation is far above anything a
   * handful of publications reaches, which is what makes it "later" without knowing the value the
   * publication under test picked.
   */
  private static void writePointerOfAnotherPublisher(LocalFileIO fileIO, String pointerPath) {
    int slot = pointerPath.endsWith("pointer-0.json") ? 1 : 0;
    String other = pointerPath.replaceAll("pointer-\\d\\.json$", "pointer-" + slot + ".json");
    // the slot was written by an earlier publication, so this one replaces it the way a publisher
    // that is switching slots would
    try (PositionOutputStream out = fileIO.newOutputFile(other).createOrOverwrite()) {
      out.write(
          "{\"generation\":999999,\"metadata-file\":\"claimed-by-another-publisher\"}"
              .getBytes(UTF_8));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static IndexMetadata withSnapshot(IndexMetadata metadata, String manifestList) {
    return withSnapshot(metadata, manifestList, 1, 10);
  }

  /**
   * Appends a snapshot unless it is already there.
   *
   * <p>A change given to {@code updateIndex(table, uuid, change)} may be applied more than once, so
   * it has to be safe on a document that already carries its result.
   */
  private static IndexMetadata append(IndexMetadata metadata, IndexSnapshot snapshot) {
    for (IndexSnapshot existing : metadata.snapshots()) {
      if (existing.snapshotId() == snapshot.snapshotId()) {
        return metadata;
      }
    }

    return metadata.withSnapshot(snapshot);
  }

  private static IndexMetadata withSnapshot(
      IndexMetadata metadata, String manifestList, long snapshotId, long sourceTableSnapshotId) {
    return IndexMetadata.builder()
        .indexUuid(metadata.indexUuid())
        .tableUuid(metadata.tableUuid())
        .tableLocation(metadata.tableLocation())
        .location(metadata.location())
        .definition(metadata.definition())
        .properties(metadata.properties())
        .addSnapshot(
            IndexSnapshot.builder()
                .snapshotId(snapshotId)
                .sourceTableSnapshotId(sourceTableSnapshotId)
                .timestampMillis(20)
                .indexManifestList(manifestList)
                .build())
        .build();
  }
}
