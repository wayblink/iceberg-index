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
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.TestTables.LocalFileIO;
import org.apache.iceberg.index.IndexManifestList.ManifestRef;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests the attempt ledger: the write-ahead record that lets a failed or killed build be cleaned up
 * without listing directories.
 */
class TestIndexSnapshotWriterAttempts {

  @BeforeAll
  static void registerTestingIndexType() {
    IndexTypes.register(new TestingIndexType());
  }

  @Test
  public void writesFilesUnderTheAttemptLedger(@TempDir Path tempDir) throws IOException {
    LocalFileIO fileIO = new LocalFileIO();
    IndexSnapshotWriter writer =
        new IndexSnapshotWriter(
            fileIO,
            new PuffinIndexFileContainer(),
            2,
            IndexSnapshotWriter.DEFAULT_FILES_PER_MANIFEST,
            "attempt-1");
    String indexLocation = tempDir.resolve("indexes/index-uuid").toString();

    List<IndexFile> indexFiles = writeFiles(writer, indexLocation);
    writer.writeManifestList(indexLocation, 1, indexFiles);

    // every file the attempt wrote is named by a marker, written before the file itself
    assertThat(indexFiles).hasSize(2);
    assertThat(marker(indexLocation, "attempt-1", "f0")).isEqualTo(indexFiles.get(0).location());
    assertThat(marker(indexLocation, "attempt-1", "f1")).isEqualTo(indexFiles.get(1).location());
    assertThat(marker(indexLocation, "attempt-1", "m0"))
        .contains("index-manifest-1-0-")
        .contains("snapshots/");
    assertThat(marker(indexLocation, "attempt-1", "ml")).contains("index-manifest-list-1-");
    assertThat(marker(indexLocation, "attempt-1", "complete")).isNull();
    assertThat(new File(indexFiles.get(0).location())).exists();

    writer.completeAttempt(indexLocation);
    assertThat(new File(markerPath(indexLocation, "attempt-1", "complete"))).exists();

    writer.finishAttempt(indexLocation);
    assertThat(new File(markerPath(indexLocation, "attempt-1", "f0"))).doesNotExist();
    assertThat(new File(markerPath(indexLocation, "attempt-1", "complete"))).doesNotExist();
    // the files themselves survive: they belong to the snapshot now
    assertThat(new File(indexFiles.get(0).location())).exists();
  }

  @Test
  public void abortDeletesEverythingTheAttemptWrote(@TempDir Path tempDir) throws IOException {
    LocalFileIO fileIO = new LocalFileIO();
    IndexSnapshotWriter writer =
        new IndexSnapshotWriter(
            fileIO,
            new PuffinIndexFileContainer(),
            2,
            IndexSnapshotWriter.DEFAULT_FILES_PER_MANIFEST,
            "attempt-1");
    String indexLocation = tempDir.resolve("indexes/index-uuid").toString();

    List<IndexFile> indexFiles = writeFiles(writer, indexLocation);
    String manifestList = writer.writeManifestList(indexLocation, 1, indexFiles);
    String manifest = marker(indexLocation, "attempt-1", "m0");

    writer.abortAttempt(indexLocation);

    for (IndexFile indexFile : indexFiles) {
      assertThat(new File(indexFile.location())).doesNotExist();
    }
    assertThat(new File(manifest)).doesNotExist();
    assertThat(new File(manifestList)).doesNotExist();
    // the ledger is gone too, so a sweep has nothing left to do
    assertThat(new File(markerPath(indexLocation, "attempt-1", "f0"))).doesNotExist();
    assertThat(new File(markerPath(indexLocation, "attempt-1", "complete"))).doesNotExist();

    // aborting twice is safe
    writer.abortAttempt(indexLocation);
  }

  @Test
  public void abortAfterAPartialWriteLeavesNothingBehind(@TempDir Path tempDir) throws IOException {
    LocalFileIO fileIO = new LocalFileIO();
    IndexSnapshotWriter writer =
        new IndexSnapshotWriter(
            fileIO,
            new PuffinIndexFileContainer(),
            2,
            IndexSnapshotWriter.DEFAULT_FILES_PER_MANIFEST,
            "attempt-1");
    String indexLocation = tempDir.resolve("indexes/index-uuid").toString();
    TestingIndexAdapter adapter = new TestingIndexAdapter(TestingIndexDefinition.of(4));

    // write one index file and then fail, as a build that dies mid-way would
    List<IndexFile> indexFiles =
        writer.writeIndexFiles(
            indexLocation,
            1,
            adapter,
            List.of(IndexEntry.of("data-1.parquet", 0, TestingIndexAdapter.valuePayload(10))));
    assertThat(indexFiles).hasSize(1);

    writer.abortAttempt(indexLocation);
    assertThat(new File(indexFiles.get(0).location())).doesNotExist();
    assertThat(new File(markerPath(indexLocation, "attempt-1", "f0"))).doesNotExist();
  }

  @Test
  public void aKilledAttemptLeavesARecoverableLedger(@TempDir Path tempDir) throws IOException {
    LocalFileIO fileIO = new LocalFileIO();
    IndexSnapshotWriter writer =
        new IndexSnapshotWriter(
            fileIO,
            new PuffinIndexFileContainer(),
            2,
            IndexSnapshotWriter.DEFAULT_FILES_PER_MANIFEST,
            "attempt-1");
    String indexLocation = tempDir.resolve("indexes/index-uuid").toString();

    List<IndexFile> indexFiles = writeFiles(writer, indexLocation);
    writer.writeManifestList(indexLocation, 1, indexFiles);

    // a process that dies here never gets to abort: the ledger without the completion marker is
    // what a recovery sweep has to go on
    assertThat(new File(markerPath(indexLocation, "attempt-1", "f0"))).exists();
    assertThat(new File(markerPath(indexLocation, "attempt-1", "complete"))).doesNotExist();

    // the sweep reads the markers and deletes exactly the paths they name
    List<String> paths = Lists.newArrayList();
    for (int seq = 0; seq < 2; seq++) {
      paths.add(marker(indexLocation, "attempt-1", "f" + seq));
    }
    paths.add(marker(indexLocation, "attempt-1", "m0"));
    paths.add(marker(indexLocation, "attempt-1", "ml"));
    for (String path : paths) {
      assertThat(new File(path)).exists();
    }
    assertThat(indexFiles.stream().map(IndexFile::location))
        .containsExactlyElementsOf(paths.subList(0, 2));
  }

  @Test
  public void splitsIndexFilesIntoSeveralManifests(@TempDir Path tempDir) throws IOException {
    LocalFileIO fileIO = new LocalFileIO();
    // a budget of two index files per manifest
    IndexSnapshotWriter writer =
        new IndexSnapshotWriter(fileIO, new PuffinIndexFileContainer(), 1, 2, "attempt-1");
    String indexLocation = tempDir.resolve("indexes/index-uuid").toString();

    List<IndexFile> indexFiles =
        writer.writeIndexFiles(
            indexLocation,
            1,
            new TestingIndexAdapter(TestingIndexDefinition.of(4)),
            List.of(
                IndexEntry.of("data-1.parquet", 0, TestingIndexAdapter.valuePayload(10)),
                IndexEntry.of("data-2.parquet", 1, TestingIndexAdapter.valuePayload(20)),
                IndexEntry.of("data-3.parquet", 2, TestingIndexAdapter.valuePayload(15)),
                IndexEntry.of("data-4.parquet", 3, TestingIndexAdapter.valuePayload(25)),
                IndexEntry.of("data-5.parquet", 4, TestingIndexAdapter.valuePayload(5))));
    String manifestList = writer.writeManifestList(indexLocation, 1, indexFiles);

    IndexManifestList list = IndexManifestList.read(fileIO.newInputFile(manifestList));
    // five index files at two per manifest make three manifests, and the list carries them all
    assertThat(list.manifests()).hasSize(3);
    assertThat(list.manifests().stream().mapToInt(ManifestRef::indexFileCount).sum()).isEqualTo(5);
    assertThat(list.manifests().stream().mapToLong(ManifestRef::entryCount).sum()).isEqualTo(5L);

    // every index file is listed by exactly one manifest, and all of them are readable
    List<String> listed = Lists.newArrayList();
    for (ManifestRef ref : list.manifests()) {
      IndexManifest manifest = IndexManifest.read(fileIO.newInputFile(ref.location()));
      assertThat(manifest.indexFiles().size()).isEqualTo(ref.indexFileCount());
      for (IndexFile indexFile : manifest.indexFiles()) {
        listed.add(indexFile.location());
      }
    }
    assertThat(listed)
        .containsExactlyInAnyOrderElementsOf(
            indexFiles.stream().map(IndexFile::location).collect(Collectors.toList()));
    // the manifests are numbered, so a ledger can name them without listing directories
    assertThat(marker(indexLocation, "attempt-1", "m2"))
        .isEqualTo(list.manifests().get(2).location());

    writer.abortAttempt(indexLocation);
    assertThat(new File(manifestList)).doesNotExist();
    for (ManifestRef ref : list.manifests()) {
      assertThat(new File(ref.location())).doesNotExist();
    }
  }

  @Test
  public void oneManifestHoldsEverythingBelowTheBudget(@TempDir Path tempDir) throws IOException {
    LocalFileIO fileIO = new LocalFileIO();
    IndexSnapshotWriter writer =
        new IndexSnapshotWriter(fileIO, new PuffinIndexFileContainer(), 10, 2, "attempt-1");
    String indexLocation = tempDir.resolve("indexes/index-uuid").toString();

    List<IndexFile> indexFiles = writeFiles(writer, indexLocation);
    String manifestList = writer.writeManifestList(indexLocation, 1, indexFiles);

    IndexManifestList list = IndexManifestList.read(fileIO.newInputFile(manifestList));
    assertThat(list.manifests()).hasSize(1);
    assertThat(list.manifests().get(0).indexFileCount()).isEqualTo(indexFiles.size());
  }

  @Test
  public void attemptIdsKeepRetriesFromColliding(@TempDir Path tempDir) throws IOException {
    LocalFileIO fileIO = new LocalFileIO();
    String indexLocation = tempDir.resolve("indexes/index-uuid").toString();
    IndexSnapshotWriter first = new IndexSnapshotWriter(fileIO, new PuffinIndexFileContainer(), 2);
    IndexSnapshotWriter second = new IndexSnapshotWriter(fileIO, new PuffinIndexFileContainer(), 2);

    assertThat(first.attemptId()).isNotEqualTo(second.attemptId());

    List<IndexFile> firstFiles = writeFiles(first, indexLocation);
    List<IndexFile> secondFiles = writeFiles(second, indexLocation);

    // the second attempt's files do not overwrite the first attempt's, and its ledger is separate
    assertThat(firstFiles.get(0).location()).isNotEqualTo(secondFiles.get(0).location());
    assertThat(markerPath(indexLocation, first.attemptId(), "f0"))
        .isNotEqualTo(markerPath(indexLocation, second.attemptId(), "f0"));
    assertThat(new File(firstFiles.get(0).location())).exists();
    assertThat(new File(secondFiles.get(0).location())).exists();

    // aborting one attempt leaves the other one's files alone
    first.abortAttempt(indexLocation);
    assertThat(new File(firstFiles.get(0).location())).doesNotExist();
    assertThat(new File(secondFiles.get(0).location())).exists();
  }

  @Test
  public void buildAbortsOnFailure(@TempDir Path tempDir) {
    LocalFileIO fileIO = new LocalFileIO();
    IndexSnapshotWriter writer =
        new IndexSnapshotWriter(
            fileIO,
            new PuffinIndexFileContainer(),
            2,
            IndexSnapshotWriter.DEFAULT_FILES_PER_MANIFEST,
            "attempt-1");
    IndexBuilder builder = new IndexBuilder(writer);
    IndexMetadata current = indexMetadata(tempDir, "indexes/index-uuid");

    // an adapter that fails while encoding makes the build die after some files were written
    IndexSnapshotWriter failing =
        new IndexSnapshotWriter(
            fileIO,
            new FailingContainer(),
            2,
            IndexSnapshotWriter.DEFAULT_FILES_PER_MANIFEST,
            "attempt-1");
    IndexBuilder failingBuilder = new IndexBuilder(failing);

    assertThatThrownBy(
            () ->
                failingBuilder.build(
                    IndexBuildRequest.builder()
                        .currentMetadata(current)
                        .sourceTableSnapshotId(10)
                        .entries(
                            List.of(
                                IndexEntry.of(
                                    "data-1.parquet", 0, TestingIndexAdapter.valuePayload(10)),
                                IndexEntry.of(
                                    "data-1.parquet", 1, TestingIndexAdapter.valuePayload(20)),
                                IndexEntry.of(
                                    "data-2.parquet", 2, TestingIndexAdapter.valuePayload(15))))
                        .timestampMillis(30)
                        .build()))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Injected failure");

    // nothing the failed attempt wrote is left behind
    assertThat(new File(current.location() + "/data").listFiles()).isNullOrEmpty();
    // FileIO cannot delete a directory, so the empty staging directory stays; the engine-side
    // sweep removes it
    assertThat(new File(current.location() + "/staging/attempt-1").listFiles()).isEmpty();

    // and the index can still be built afterwards
    IndexMetadata updated =
        builder.build(
            IndexBuildRequest.builder()
                .currentMetadata(current)
                .sourceTableSnapshotId(10)
                .entries(
                    List.of(
                        IndexEntry.of("data-1.parquet", 0, TestingIndexAdapter.valuePayload(10)),
                        IndexEntry.of("data-1.parquet", 1, TestingIndexAdapter.valuePayload(20)),
                        IndexEntry.of("data-2.parquet", 2, TestingIndexAdapter.valuePayload(15))))
                .timestampMillis(30)
                .build());
    assertThat(updated.snapshots()).hasSize(1);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void persistsBuildMemoryWithActualContainerSize(boolean auxiliary, @TempDir Path tempDir)
      throws IOException {
    LocalFileIO fileIO = new LocalFileIO();
    PuffinIndexFileContainer container = new PuffinIndexFileContainer();
    IndexSnapshotWriter writer = new IndexSnapshotWriter(fileIO, container, 10);
    String indexLocation = tempDir.resolve("indexes/index-uuid").toString();
    MemoryAdapter adapter = new MemoryAdapter(auxiliary);
    try {
      List<IndexFile> files =
          writer.writeIndexFiles(
              indexLocation,
              1,
              adapter,
              List.of(IndexEntry.of("data.parquet", 7, TestingIndexAdapter.valuePayload(10))));
      assertThat(files).hasSize(1);
      IndexFile file = files.get(0);
      assertThat(file.memory()).isNotNull();
      assertThat(file.memory())
          .isEqualTo(
              adapter.memory.toBuilder()
                  .fileBytes(fileIO.newInputFile(file.location()).getLength())
                  .build());
      assertThat(file.kind())
          .isEqualTo(auxiliary ? IndexFile.Kind.AUXILIARY : IndexFile.Kind.ENTRY_BEARING);
      assertThat(file.coveredDataFiles()).containsExactly("data.parquet");
      assertThat(file.entryCount()).isEqualTo(1);
      IndexBlob blob = container.read(fileIO.newInputFile(file.location())).get(0);
      assertThat(blob.type()).isEqualTo(file.blobType());
      assertThat(blob.payload().length).isEqualTo(file.memory().payloadBytes());
      List<IndexEntry> decoded = adapter.decodeEntries(ByteBuffer.wrap(blob.payload()));
      assertThat(decoded).extracting(IndexEntry::rowPosition).containsExactly(7L);
      assertThat(TestingIndexAdapter.payloadValue(decoded.get(0).payload())).isEqualTo(10);
      String manifestList = writer.writeManifestList(indexLocation, 1, files);
      ManifestRef manifest =
          IndexManifestList.read(fileIO.newInputFile(manifestList)).manifests().get(0);
      assertThat(IndexManifest.read(fileIO.newInputFile(manifest.location())).indexFiles())
          .containsExactly(file);
    } finally {
      writer.abortAttempt(indexLocation);
    }
  }

  private static final class MemoryAdapter extends TestingIndexAdapter {
    private final boolean auxiliary;
    private IndexFileMemory memory;

    private MemoryAdapter(boolean auxiliary) {
      super(TestingIndexDefinition.of(4));
      this.auxiliary = auxiliary;
    }

    @Override
    public boolean writesAuxiliaryFiles() {
      return auxiliary;
    }

    @Override
    public String auxiliaryBlobType() {
      return "testing-auxiliary";
    }

    @Override
    public ByteBuffer encodeAuxiliaryPayload(List<IndexEntry> entries) {
      return super.encodeEntries(entries);
    }

    @Override
    public IndexBuildResult encodeEntriesWithMemory(List<IndexEntry> entries) {
      ByteBuffer payload = super.encodeEntries(entries).asReadOnlyBuffer();
      this.memory =
          IndexFileMemory.builder()
              .payloadBytes(payload.remaining())
              .preparedBytes(73)
              .modelVersion(1)
              .readerId("testing/1")
              .runtimeProfile("fixture")
              .structure(Map.of("entry-count", (long) entries.size()))
              .build();
      return IndexBuildResult.of(payload, memory);
    }

    @Override
    public IndexBuildResult encodeAuxiliaryPayloadWithMemory(
        List<IndexEntry> entries, int parallelism) {
      return encodeEntriesWithMemory(entries);
    }

    @Override
    public PreparedSearch prepare(ByteBuffer payload) {
      throw new AssertionError("Building must not prepare a second retained structure");
    }

    @Override
    public PreparedSearch prepareAuxiliary(ByteBuffer payload) {
      throw new AssertionError("Building must not prepare a second retained structure");
    }
  }

  private static List<IndexFile> writeFiles(IndexSnapshotWriter writer, String indexLocation)
      throws IOException {
    TestingIndexAdapter adapter = new TestingIndexAdapter(TestingIndexDefinition.of(4));
    List<IndexFile> indexFiles =
        writer.writeIndexFiles(
            indexLocation,
            1,
            adapter,
            List.of(
                IndexEntry.of("data-1.parquet", 0, TestingIndexAdapter.valuePayload(10)),
                IndexEntry.of("data-1.parquet", 1, TestingIndexAdapter.valuePayload(20)),
                IndexEntry.of("data-2.parquet", 2, TestingIndexAdapter.valuePayload(15))));
    return indexFiles;
  }

  private static String markerPath(String indexLocation, String attemptId, String marker) {
    return indexLocation + "/staging/" + attemptId + "/" + marker;
  }

  private static String marker(String indexLocation, String attemptId, String marker)
      throws IOException {
    Path path = Path.of(markerPath(indexLocation, attemptId, marker));
    if (!path.toFile().exists()) {
      return null;
    }
    return new String(java.nio.file.Files.readAllBytes(path)).trim();
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

  /** An index file container that fails while writing, to model a build that dies mid-way. */
  private static class FailingContainer implements IndexFileContainer {
    @Override
    public void write(org.apache.iceberg.io.OutputFile outputFile, List<IndexBlob> blobs) {
      throw new RuntimeException("Injected failure");
    }

    @Override
    public List<IndexBlob> read(org.apache.iceberg.io.InputFile inputFile) {
      throw new UnsupportedOperationException();
    }
  }
}
