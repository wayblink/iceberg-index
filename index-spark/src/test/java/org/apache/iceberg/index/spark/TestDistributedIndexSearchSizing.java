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
    byte[] payload = java.nio.file.Files.readAllBytes(temp.resolve("index.puffin"));
    long residentBytes =
        ExecutorIndexCache.loaded(payload, adapter.prepare(ByteBuffer.wrap(payload))).bytes();
    long expected = preparedBytes == 0 ? -1 : residentBytes;
    assertThat(DistributedIndexSearch.workingSetBytes(new LocalIO(), List.of(file), adapter))
        .isEqualTo(expected);
  }

  @Test
  void cacheIdentityIncludesReaderDefinition() {
    assertThat(DistributedIndexSearch.adapterKey(new SizedAdapter(70)))
        .isNotEqualTo(DistributedIndexSearch.adapterKey(new SizedAdapter(170)));
  }

  @Test
  void changingDefinitionAtTheSameLocationInvalidatesEstimate() throws IOException {
    IndexFile file = writeFile("definition.puffin");
    long first =
        DistributedIndexSearch.workingSetBytes(new LocalIO(), List.of(file), new SizedAdapter(70));
    long second =
        DistributedIndexSearch.workingSetBytes(new LocalIO(), List.of(file), new SizedAdapter(170));
    assertThat(first).isEqualTo(182);
    assertThat(second).isEqualTo(282);
  }

  @Test
  void failedEstimationIsUnknownAndCanBeRetried() throws IOException {
    Path path = temp.resolve("retry.puffin");
    java.nio.file.Files.write(path, new byte[10]);
    IndexFile file = file(path.toString());
    SizedAdapter adapter = new SizedAdapter(70);
    assertThat(DistributedIndexSearch.workingSetBytes(new LocalIO(), List.of(file), adapter))
        .isEqualTo(-1);
    java.nio.file.Files.delete(path);
    new PuffinIndexFileContainer()
        .write(
            Files.localOutput(path.toString()), List.of(IndexBlob.of("test-sizing", new byte[10])));
    assertThat(DistributedIndexSearch.workingSetBytes(new LocalIO(), List.of(file), adapter))
        .isEqualTo(182);
  }

  @Test
  void unknownFileInWorkingSetIsNotFreeCapacity() throws IOException {
    IndexFile valid = writeFile("valid.puffin");
    Path bad = temp.resolve("bad.puffin");
    java.nio.file.Files.write(bad, new byte[10]);
    assertThat(
            DistributedIndexSearch.workingSetBytes(
                new LocalIO(), List.of(valid, file(bad.toString())), new SizedAdapter(70)))
        .isEqualTo(-1);
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
