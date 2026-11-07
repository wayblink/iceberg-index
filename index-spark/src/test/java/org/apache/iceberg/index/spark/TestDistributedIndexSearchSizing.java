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
package org.apache.iceberg.index.spark;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import org.apache.iceberg.Files;
import org.apache.iceberg.index.IndexAdapter;
import org.apache.iceberg.index.IndexBlob;
import org.apache.iceberg.index.IndexBuildResult;
import org.apache.iceberg.index.IndexDefinition;
import org.apache.iceberg.index.IndexEntry;
import org.apache.iceberg.index.IndexFile;
import org.apache.iceberg.index.IndexFileMemory;
import org.apache.iceberg.index.IndexSearchResult;
import org.apache.iceberg.index.PuffinIndexFileContainer;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestDistributedIndexSearchSizing {
  // The owner a query plans with is the table location; a working set is only known under one.
  private static final String OWNER = "file:/warehouse";

  @TempDir private Path temp;

  @ParameterizedTest
  @ValueSource(longs = {0, 70})
  void derivedWorkingSetUsesTheSameBytesAsExecutorAdmission(long preparedBytes) throws IOException {
    String location = temp.resolve("index.puffin").toString();
    new PuffinIndexFileContainer()
        .write(Files.localOutput(location), List.of(IndexBlob.of("test-sizing", new byte[10])));
    IndexFile file =
        IndexFile.builder()
            .location(location)
            .kind(IndexFile.Kind.ENTRY_BEARING)
            .blobType("test-sizing")
            .entryCount(0)
            .build();
    IndexAdapter adapter = new SizedAdapter(preparedBytes);
    long expected = admittedBytes(file, adapter);
    assertThat(DistributedIndexSearch.workingSetBytes(new LocalIO(), List.of(file), adapter, OWNER))
        .isEqualTo(expected);
  }

  @Test
  void genericPositiveEstimateIsUnknownOnUnsupportedWrapperLayout(@TempDir Path temp)
      throws Exception {
    Process child =
        new ProcessBuilder(
                System.getProperty("java.home") + "/bin/java",
                "-XX:-UseCompressedOops",
                "-cp",
                System.getProperty("java.class.path"),
                WrapperLayoutProbe.class.getName(),
                temp.toString())
            .redirectErrorStream(true)
            .start();
    try {
      assertThat(child.waitFor(30, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
      String output =
          new String(
              child.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
      assertThat(child.exitValue()).as(output).isZero();
    } finally {
      child.destroyForcibly();
    }
  }

  public static class WrapperLayoutProbe {
    public static void main(String[] args) throws Exception {
      Path location = Path.of(args[0], "unsupported.puffin");
      SizedAdapter adapter = new SizedAdapter(70);
      new PuffinIndexFileContainer()
          .write(
              Files.localOutput(location.toString()),
              List.of(IndexBlob.of("test-sizing", new byte[10])));
      if (adapter.encodeEntriesWithMemory(List.of()).memory().preparedBytes() <= 0) {
        throw new AssertionError("Probe requires a generic positive P");
      }
      if (IndexCacheMemory.resolve(new LocalIO(), file(location.toString()), adapter) != null) {
        throw new AssertionError("Unsupported wrapper layout was declared known");
      }
      if (IndexCacheMemory.compatible(
          adapter.encodeEntriesWithMemory(List.of()).memory(), adapter)) {
        throw new AssertionError("Unsupported wrapper layout passed managed compatibility");
      }
      ExecutorIndexCache.CachedFile loaded =
          DistributedIndexSearch.loadFile(new LocalIO(), location.toString(), adapter);
      if (loaded.hasKnownContext()) {
        throw new AssertionError("Unsupported wrapper layout retained a known context receipt");
      }
      if (loaded.bytes() <= 0
          || loaded.searching(searcher -> searcher.search(ByteBuffer.allocate(0), 1)) == null) {
        throw new AssertionError("Legacy query fallback must stay usable");
      }
    }
  }

  @Test
  void cacheIdentityIncludesReaderDefinition() {
    assertThat(DistributedIndexSearch.adapterKey(new SizedAdapter(70)))
        .isNotEqualTo(DistributedIndexSearch.adapterKey(new SizedAdapter(170)));
  }

  @Test
  void aFileWithoutRecordIsPricedByItsLengthForEveryReader() throws IOException {
    IndexFile file = writeFile("definition.puffin");
    long length = java.nio.file.Files.size(Path.of(file.location()));
    // A record is the only thing that can state a reader-specific prepared size; without one the
    // bound comes from the container, so it cannot differ between readers of the same file.
    for (SizedAdapter adapter : List.of(new SizedAdapter(70), new SizedAdapter(170))) {
      assertThat(IndexCacheMemory.resolveMemory(new LocalIO(), file, adapter).preparedBytes())
          .isEqualTo(IndexCacheMemory.residentBytes(length, 0L))
          .isPositive();
      assertThat(
              DistributedIndexSearch.workingSetBytes(new LocalIO(), List.of(file), adapter, OWNER))
          .isEqualTo(admittedBytes(file, adapter));
    }
  }

  @Test
  void aFileWithNoRecordIsBoundedAndAnUnknownLengthIsNotFree() throws IOException {
    Path path = temp.resolve("retry.puffin");
    IndexFile file = file(path.toString());
    SizedAdapter adapter = new SizedAdapter(70);
    // Nothing exists at the location, so the length is unknown and the size cannot be bounded.
    assertThat(DistributedIndexSearch.workingSetBytes(new LocalIO(), List.of(file), adapter, OWNER))
        .isEqualTo(-1);
    // Writing the file makes it boundable without reading it: only its length is consulted.
    java.nio.file.Files.write(path, new byte[10]);
    assertThat(DistributedIndexSearch.workingSetBytes(new LocalIO(), List.of(file), adapter, OWNER))
        .isEqualTo(admittedBytes(file, adapter))
        .isPositive();
  }

  @Test
  void unknownFileInWorkingSetIsNotFreeCapacity() throws IOException {
    IndexFile valid = writeFile("valid.puffin");
    Path bad = temp.resolve("bad.puffin");
    assertThat(
            DistributedIndexSearch.workingSetBytes(
                new LocalIO(), List.of(valid, file(bad.toString())), new SizedAdapter(70), OWNER))
        .isEqualTo(-1);
  }

  /**
   * The capacity the executor charges for one file of this reader: the admission it computes from
   * the same memory descriptor, measured rather than derived from the estimate formula.
   */
  private static long admittedBytes(IndexFile file, IndexAdapter adapter) {
    IndexFileMemory memory = IndexCacheMemory.resolveMemory(new LocalIO(), file, adapter);
    if (memory == null) {
      return -1;
    }

    IndexCacheProtocol.File prospect =
        IndexCacheProtocol.File.builder()
            .location(file.location())
            .owner(OWNER)
            .cacheKey(
                DistributedIndexSearch.cacheKey(
                    OWNER, DistributedIndexSearch.adapterKey(adapter), file.location()))
            .memory(memory)
            .estimatedBytes(IndexCacheMemory.residentBytes(memory))
            .build();
    return ExecutorIndexCache.Shape.empty()
        .projectAdmission(null, 0, List.of(prospect))
        .charge()
        .residentBytes();
  }

  private IndexFile writeFile(String name) throws IOException {
    String location = temp.resolve(name).toString();
    new PuffinIndexFileContainer()
        .write(Files.localOutput(location), List.of(IndexBlob.of("test-sizing", new byte[10])));
    return file(location);
  }

  private static IndexFile file(String location) {
    return IndexFile.builder()
        .location(location)
        .kind(IndexFile.Kind.ENTRY_BEARING)
        .blobType("test-sizing")
        .entryCount(0)
        .build();
  }

  private record SizedDefinition(long bytes) implements IndexDefinition {
    @Override
    public String type() {
      return "test-sizing";
    }

    @Override
    public int fieldId() {
      return 1;
    }
  }

  private static class LocalIO implements FileIO {
    @Override
    public InputFile newInputFile(String path) {
      return Files.localInput(path);
    }

    @Override
    public OutputFile newOutputFile(String path) {
      return Files.localOutput(path);
    }

    @Override
    public void deleteFile(String path) {
      throw new UnsupportedOperationException();
    }
  }

  private static class SizedAdapter implements IndexAdapter {
    private final long bytes;

    private SizedAdapter(long bytes) {
      this.bytes = bytes;
    }

    @Override
    public PreparedSearch prepare(ByteBuffer payload) {
      return new PreparedSearch() {
        @Override
        public List<IndexSearchResult> search(ByteBuffer query, int topK) {
          return List.of();
        }

        @Override
        public long estimatedBytes() {
          return bytes;
        }
      };
    }

    @Override
    public IndexDefinition definition() {
      return new SizedDefinition(bytes);
    }

    @Override
    public IndexBuildResult encodeEntriesWithMemory(List<IndexEntry> entries) {
      return IndexBuildResult.of(
          ByteBuffer.wrap(new byte[10]),
          IndexFileMemory.builder()
              .payloadBytes(10)
              .preparedBytes(bytes)
              .modelVersion(2)
              .readerId("test-sizing/1")
              .runtimeProfile("fixture")
              .build());
    }

    @Override
    public String entryBlobType() {
      return "test-sizing";
    }

    @Override
    public ByteBuffer encodeEntries(List<IndexEntry> entries) {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<IndexEntry> decodeEntries(ByteBuffer payload) {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<IndexSearchResult> search(ByteBuffer payload, ByteBuffer query, int topK) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Comparator<IndexSearchResult> resultComparator() {
      throw new UnsupportedOperationException();
    }
  }
}
