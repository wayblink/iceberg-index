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

import static org.apache.iceberg.Files.localInput;
import static org.apache.iceberg.Files.localOutput;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.index.IndexFile.Kind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestIndexManifestFiles {

  @Test
  void writesTheVersionEveryReaderAccepts(@TempDir Path tempDir) throws IOException {
    Path path = tempDir.resolve("current.imn");
    IndexManifest.write(localOutput(path.toFile()), List.of());
    try (DataInputStream in = new DataInputStream(Files.newInputStream(path))) {
      assertThat(in.readNBytes(8))
          .containsExactly(new byte[] {'I', 'C', 'E', 'I', 'M', 'N', '1', 0});
      // bumping this int is what makes an older reader refuse the file outright, so an optional
      // field is written after the file list instead of behind a new version
      assertThat(in.readInt()).isEqualTo(1);
    }
  }

  @Test
  void aManifestWithMemoryIsReadableByAReaderThatOnlyKnowsTheFileList(@TempDir Path tempDir)
      throws IOException {
    Path path = tempDir.resolve("compatible.imn");
    IndexManifest.write(
        localOutput(path.toFile()), List.of(fileWithMemory(memory().build()), fixtureFile()));

    // what a reader that predates the memory section does: read the files, stop, never look at what
    // follows. It has to see exactly the files, and it must not be asked to understand the tail.
    try (DataInputStream in = new DataInputStream(Files.newInputStream(path))) {
      in.readNBytes(8);
      assertThat(in.readInt()).isEqualTo(1);
      int count = in.readInt();
      assertThat(count).isEqualTo(2);
      assertThat(readFileRecord(in)).isEqualTo(fixtureFile());
      assertThat(readFileRecord(in)).isEqualTo(fixtureFile());
      assertThat(in.available()).as("the tail is left unread").isPositive();
    }
  }

  @Test
  void aManifestWrittenWithoutMemoryStillCarriesAnEmptyTail(@TempDir Path tempDir)
      throws IOException {
    Path path = tempDir.resolve("no-memory.imn");
    IndexManifest.write(localOutput(path.toFile()), List.of(fixtureFile()));
    List<IndexFile> files = IndexManifest.read(localInput(path.toFile())).indexFiles();
    assertThat(files).containsExactly(fixtureFile());
    assertThat(files.get(0).memory()).isNull();
  }

  @Test
  void rejectsATailThatDoesNotMatchTheFileList(@TempDir Path tempDir) throws IOException {
    Path path = tempDir.resolve("wrong-tail-count.imn");
    try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(path))) {
      writeHeader(out, 1, 1);
      writeFile(out);
      out.writeInt(2);
      writeMemory(out, 1, List.of("nodes"));
      writeMemory(out, 1, List.of("nodes"));
    }
    assertThatThrownBy(() -> IndexManifest.read(localInput(path.toFile())))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("does not match");
  }

  @Test
  void rejectsATruncatedTail(@TempDir Path tempDir) throws IOException {
    Path path = tempDir.resolve("truncated-tail.imn");
    IndexManifest.write(localOutput(path.toFile()), List.of(fileWithMemory(memory().build())));
    byte[] bytes = Files.readAllBytes(path);
    Files.write(path, Arrays.copyOf(bytes, bytes.length - 1));
    assertThatThrownBy(() -> IndexManifest.read(localInput(path.toFile())))
        .isInstanceOf(UncheckedIOException.class)
        .hasCauseInstanceOf(EOFException.class);
  }

  @Test
  void readsVersionTwoWithoutMemory(@TempDir Path tempDir) throws IOException {
    Path path = tempDir.resolve("without-memory.imn");
    try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(path))) {
      writeHeader(out, 2, 1);
      writeFile(out);
      out.writeBoolean(false);
    }
    assertThat(IndexManifest.read(localInput(path.toFile())).indexFiles())
        .containsExactly(fixtureFile());
  }

  @Test
  void readsLegacyManifest(@TempDir Path tempDir) throws IOException {
    Path path = tempDir.resolve("legacy.imn");
    try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(path))) {
      writeHeader(out, 1, 1);
      writeFile(out);
    }
    assertThat(IndexManifest.read(localInput(path.toFile())).indexFiles())
        .containsExactly(fixtureFile());
  }

  @Test
  void roundTripsMemory(@TempDir Path tempDir) throws IOException {
    Path path = tempDir.resolve("memory.imn");
    IndexFile withMemory = fileWithMemory(memory().build());
    IndexManifest.write(localOutput(path.toFile()), List.of(withMemory, fixtureFile()));
    List<IndexFile> files = IndexManifest.read(localInput(path.toFile())).indexFiles();
    assertThat(files).containsExactly(withMemory, fixtureFile());
    assertThat(files.get(0).memory().fileBytes()).isEqualTo(8192L);
    assertThat(files.get(0).memory().payloadBytes()).isEqualTo(1024L);
    assertThat(files.get(0).memory().preparedBytes()).isEqualTo(4096L);
    assertThat(files.get(0).memory().structure()).containsExactlyEntriesOf(Map.of("nodes", 2L));
    assertThat(files.get(1).memory()).isNull();
  }

  @Test
  void readsMemoryFixture(@TempDir Path tempDir) throws IOException {
    Path path = tempDir.resolve("fixture-memory.imn");
    try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(path))) {
      writeHeader(out, 2, 1);
      writeFile(out);
      writeMemory(out, 1, List.of("nodes"));
    }
    IndexFile file = IndexManifest.read(localInput(path.toFile())).indexFiles().get(0);
    assertThat(file).isEqualTo(fileWithMemory(memory().build()));
  }

  @Test
  void retainsUnknownMemoryModel(@TempDir Path tempDir) throws IOException {
    Path path = tempDir.resolve("unknown-model.imn");
    try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(path))) {
      writeHeader(out, 2, 1);
      writeFile(out);
      writeMemory(out, 99, List.of("nodes"));
    }
    assertThat(
            IndexManifest.read(localInput(path.toFile()))
                .indexFiles()
                .get(0)
                .memory()
                .modelVersion())
        .isEqualTo(99);
  }

  @Test
  void requiresContainerSizeWhenWriting(@TempDir Path tempDir) {
    Path path = tempDir.resolve("missing-file-size.imn");
    IndexFile file = fileWithMemory(memory().fileBytes(null).build());
    assertThatThrownBy(() -> IndexManifest.write(localOutput(path.toFile()), List.of(file)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("file bytes");
    assertThat(path).doesNotExist();
  }

  @Test
  void rejectsDuplicateStructureKeys(@TempDir Path tempDir) throws IOException {
    Path path = tempDir.resolve("duplicate-keys.imn");
    try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(path))) {
      writeHeader(out, 2, 1);
      writeFile(out);
      writeMemory(out, 1, List.of("nodes", "nodes"));
    }
    assertThatThrownBy(() -> IndexManifest.read(localInput(path.toFile())))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Duplicate index memory structure key");
  }

  @Test
  void rejectsTruncatedMemory(@TempDir Path tempDir) throws IOException {
    Path path = tempDir.resolve("truncated.imn");
    try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(path))) {
      writeHeader(out, 2, 1);
      writeFile(out);
      writeMemory(out, 1, List.of("nodes"));
    }
    byte[] bytes = Files.readAllBytes(path);
    Files.write(path, Arrays.copyOf(bytes, bytes.length - 1));
    assertThatThrownBy(() -> IndexManifest.read(localInput(path.toFile())))
        .isInstanceOf(UncheckedIOException.class)
        .hasCauseInstanceOf(EOFException.class);
  }

  @Test
  void rejectsUnknownManifestVersion(@TempDir Path tempDir) throws IOException {
    Path path = tempDir.resolve("unknown-format.imn");
    try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(path))) {
      writeHeader(out, 99, 0);
    }
    assertThatThrownBy(() -> IndexManifest.read(localInput(path.toFile())))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsupported index manifest format version: 99");
  }

  @Test
  void writesStructureInStableOrder(@TempDir Path tempDir) throws IOException {
    Map<String, Long> forward = new LinkedHashMap<>();
    forward.put("nodes", 2L);
    forward.put("dimension", 128L);
    Map<String, Long> reverse = new LinkedHashMap<>();
    reverse.put("dimension", 128L);
    reverse.put("nodes", 2L);
    Path first = tempDir.resolve("first.imn");
    Path second = tempDir.resolve("second.imn");
    IndexManifest.write(
        localOutput(first.toFile()), List.of(fileWithMemory(memory().structure(forward).build())));
    IndexManifest.write(
        localOutput(second.toFile()), List.of(fileWithMemory(memory().structure(reverse).build())));
    assertThat(Files.readAllBytes(first)).containsExactly(Files.readAllBytes(second));
  }

  @Test
  void rejectsNegativeStructureCount(@TempDir Path tempDir) throws IOException {
    Path path = tempDir.resolve("negative-count.imn");
    try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(path))) {
      writeHeader(out, 2, 1);
      writeFile(out);
      writeMemoryHeader(out, 1, 8192, 1024, 4096);
      out.writeInt(-1);
    }
    assertThatThrownBy(() -> IndexManifest.read(localInput(path.toFile())))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("structure count");
  }

  @Test
  void rejectsInvalidMemorySizes(@TempDir Path tempDir) throws IOException {
    Path path = tempDir.resolve("negative-payload.imn");
    try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(path))) {
      writeHeader(out, 2, 1);
      writeFile(out);
      writeMemoryHeader(out, 1, 8192, -1, 4096);
      out.writeInt(0);
    }
    assertThatThrownBy(() -> IndexManifest.read(localInput(path.toFile())))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("payload bytes");
  }

  /** One file record, read the way the oldest reader reads it: no memory, no look-ahead. */
  private static IndexFile readFileRecord(DataInputStream in) throws IOException {
    String location = in.readUTF();
    Kind kind = Kind.valueOf(in.readUTF());
    String blobType = in.readUTF();
    long entryCount = in.readLong();
    int coveredCount = in.readInt();
    List<String> covered = new java.util.ArrayList<>();
    for (int i = 0; i < coveredCount; i++) {
      covered.add(in.readUTF());
    }
    return IndexFile.builder()
        .location(location)
        .kind(kind)
        .blobType(blobType)
        .entryCount(entryCount)
        .coveredDataFiles(covered)
        .build();
  }

  private static void writeMemory(DataOutputStream out, int model, List<String> keys)
      throws IOException {
    writeMemoryHeader(out, model, 8192, 1024, 4096);
    out.writeInt(keys.size());
    for (String key : keys) {
      out.writeUTF(key);
      out.writeLong(2);
    }
  }

  private static void writeMemoryHeader(
      DataOutputStream out, int model, long fileBytes, long payloadBytes, long preparedBytes)
      throws IOException {
    out.writeBoolean(true);
    out.writeLong(fileBytes);
    out.writeLong(payloadBytes);
    out.writeLong(preparedBytes);
    out.writeInt(model);
    out.writeUTF("fixture-reader-v1");
    out.writeUTF("fixture-jvm-profile");
  }

  private static IndexFileMemory.Builder memory() {
    return IndexFileMemory.builder()
        .fileBytes(8192L)
        .payloadBytes(1024)
        .preparedBytes(4096)
        .modelVersion(1)
        .readerId("fixture-reader-v1")
        .runtimeProfile("fixture-jvm-profile")
        .structure(Map.of("nodes", 2L));
  }

  private static IndexFile fileWithMemory(IndexFileMemory memory) {
    return IndexFile.builder()
        .location("fixture.puffin")
        .kind(Kind.ENTRY_BEARING)
        .blobType("fixture-blob")
        .entryCount(2)
        .coveredDataFiles(List.of("fixture.parquet"))
        .memory(memory)
        .build();
  }

  private static void writeHeader(DataOutputStream out, int version, int count) throws IOException {
    out.write(new byte[] {'I', 'C', 'E', 'I', 'M', 'N', '1', 0});
    out.writeInt(version);
    out.writeInt(count);
  }

  private static void writeFile(DataOutputStream out) throws IOException {
    out.writeUTF("fixture.puffin");
    out.writeUTF("ENTRY_BEARING");
    out.writeUTF("fixture-blob");
    out.writeLong(2);
    out.writeInt(1);
    out.writeUTF("fixture.parquet");
  }

  private static IndexFile fixtureFile() {
    return IndexFile.builder()
        .location("fixture.puffin")
        .kind(Kind.ENTRY_BEARING)
        .blobType("fixture-blob")
        .entryCount(2)
        .coveredDataFiles(List.of("fixture.parquet"))
        .build();
  }

  @Test
  public void roundTripsManifestAndList(@TempDir Path tempDir) throws IOException {
    IndexFile entryBearing =
        IndexFile.builder()
            .location("index-file-1.puffin")
            .kind(Kind.ENTRY_BEARING)
            .blobType(TestingIndexAdapter.BLOB_TYPE)
            .entryCount(2)
            .coveredDataFiles(List.of("data-1.parquet", "data-2.parquet"))
            .build();
    IndexFile auxiliary =
        IndexFile.builder()
            .location("index-file-2.puffin")
            .kind(Kind.AUXILIARY)
            .blobType("iceberg-vector-graph-v1")
            .entryCount(0)
            .coveredDataFiles(List.of())
            .build();

    String manifestLocation = tempDir.resolve("index-manifest-1.imn").toString();
    IndexManifest.write(localOutput(manifestLocation), List.of(entryBearing, auxiliary));
    IndexManifest manifest = IndexManifest.read(localInput(manifestLocation));
    assertThat(manifest.indexFiles()).containsExactly(entryBearing, auxiliary);
    assertThat(manifest.indexedDataFiles())
        .containsExactlyInAnyOrder("data-1.parquet", "data-2.parquet");

    String listLocation = tempDir.resolve("index-manifest-list-1.iml").toString();
    IndexManifestList.ManifestRef ref = IndexManifestList.ManifestRef.of(manifestLocation, 2, 2);
    IndexManifestList.write(localOutput(listLocation), List.of(ref));
    IndexManifestList list = IndexManifestList.read(localInput(listLocation));
    assertThat(list.manifests()).containsExactly(ref);
  }
}
