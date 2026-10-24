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
import org.apache.iceberg.index.IndexDefinition;
import org.apache.iceberg.index.IndexEntry;
import org.apache.iceberg.index.IndexFile;
import org.apache.iceberg.index.IndexSearchResult;
import org.apache.iceberg.index.PuffinIndexFileContainer;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
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
    assertThat(DistributedIndexSearch.workingSetBytes(new LocalIO(), List.of(file), adapter))
        .isEqualTo(residentBytes);
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
      throw new UnsupportedOperationException();
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
