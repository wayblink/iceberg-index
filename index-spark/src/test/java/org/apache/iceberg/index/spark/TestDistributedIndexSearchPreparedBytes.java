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
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iceberg.Files;
import org.apache.iceberg.index.IndexAdapter;
import org.apache.iceberg.index.IndexBlob;
import org.apache.iceberg.index.IndexBuildResult;
import org.apache.iceberg.index.IndexDefinition;
import org.apache.iceberg.index.IndexEntry;
import org.apache.iceberg.index.IndexFile;
import org.apache.iceberg.index.IndexFileMemory;
import org.apache.iceberg.index.IndexManifest;
import org.apache.iceberg.index.IndexSearchResult;
import org.apache.iceberg.index.PuffinIndexFileContainer;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The per-file prepared estimate resolves through the unified memory resolver: a manifest record
 * that already matches the current model, reader and runtime is trusted without a read, and an old,
 * missing or mismatched record is re-estimated by reading and preparing the complete file, never
 * silently priced at zero.
 */
class TestDistributedIndexSearchPreparedBytes {

  private static final String READER = "test-trust/1";
  private static final String PROFILE = "fixture";

  @TempDir private java.nio.file.Path temp;

  @Test
  void matchingManifestRecordIsTrustedWithoutReading() throws IOException {
    String location = writePayload("trusted.puffin", "zstd");
    IndexFile file = file(location, memory(123, READER, PROFILE));
    TrustAdapter adapter = new TrustAdapter(123, READER, PROFILE);
    CountingFileIO io = new CountingFileIO();

    assertThat(DistributedIndexSearch.preparedBytesOf(io, file, adapter)).isEqualTo(123);
    assertThat(io.inputFiles()).as("a matching record must not be read again").isZero();
    assertThat(io.bytesRead()).as("a matching record must not be read again").isZero();
    assertThat(adapter.prepareCount()).as("a matching record must not be prepared again").isZero();
  }

  @Test
  void missingManifestRecordIsReestimatedByReading() throws IOException {
    String location = writePayload("missing.puffin", "zstd");
    IndexFile file = file(location, null);
    TrustAdapter adapter = new TrustAdapter(123, READER, PROFILE);
    CountingFileIO io = new CountingFileIO();

    long fileLength = Files.localInput(location).getLength();
    assertThat(DistributedIndexSearch.preparedBytesOf(io, file, adapter)).isEqualTo(123);
    assertThat(io.inputFiles()).isEqualTo(1);
    assertThat(io.bytesRead()).as("the re-estimate reads the complete file").isEqualTo(fileLength);
    assertThat(adapter.prepareCount()).isEqualTo(1);
  }

  @Test
  void oldMemoryModelIsReestimatedNotTrusted() throws IOException {
    String location = writePayload("old-model.puffin", "zstd");
    IndexFileMemory declared = memory(999, READER, PROFILE).toBuilder().modelVersion(1).build();
    IndexFile file = file(location, declared);
    TrustAdapter adapter = new TrustAdapter(123, READER, PROFILE);
    CountingFileIO io = new CountingFileIO();

    assertThat(DistributedIndexSearch.preparedBytesOf(io, file, adapter)).isEqualTo(123);
    assertThat(io.inputFiles()).isEqualTo(1);
    assertThat(adapter.prepareCount()).isEqualTo(1);
  }

  @Test
  void mismatchedReaderIsReestimated() throws IOException {
    String location = writePayload("reader.puffin", "zstd");
    IndexFile file = file(location, memory(999, "test-trust/2", PROFILE));
    TrustAdapter adapter = new TrustAdapter(123, READER, PROFILE);
    CountingFileIO io = new CountingFileIO();

    assertThat(DistributedIndexSearch.preparedBytesOf(io, file, adapter)).isEqualTo(123);
    assertThat(io.inputFiles()).isEqualTo(1);
    assertThat(adapter.prepareCount()).isEqualTo(1);
  }

  @Test
  void mismatchedRuntimeIsReestimated() throws IOException {
    String location = writePayload("runtime.puffin", "zstd");
    IndexFile file = file(location, memory(999, READER, "other-profile"));
    TrustAdapter adapter = new TrustAdapter(123, READER, PROFILE);
    CountingFileIO io = new CountingFileIO();

    assertThat(DistributedIndexSearch.preparedBytesOf(io, file, adapter)).isEqualTo(123);
    assertThat(io.inputFiles()).isEqualTo(1);
    assertThat(adapter.prepareCount()).isEqualTo(1);
  }

  @Test
  void unreadableFileIsUnknownNotZero() throws IOException {
    String location = temp.resolve("garbage.puffin").toString();
    java.nio.file.Files.write(java.nio.file.Path.of(location), new byte[10]);
    IndexFile file = file(location, null);
    TrustAdapter adapter = new TrustAdapter(123, READER, PROFILE);

    assertThat(DistributedIndexSearch.preparedBytesOf(new CountingFileIO(), file, adapter))
        .isEqualTo(-1);
  }

  @Test
  void reestimationFailureIsRetryable() throws IOException {
    String location = temp.resolve("retry.puffin").toString();
    java.nio.file.Files.write(java.nio.file.Path.of(location), new byte[10]);
    IndexFile file = file(location, null);
    TrustAdapter adapter = new TrustAdapter(123, READER, PROFILE);

    assertThat(DistributedIndexSearch.preparedBytesOf(new CountingFileIO(), file, adapter))
        .isEqualTo(-1);
    java.nio.file.Files.delete(java.nio.file.Path.of(location));
    writePayload(location, "zstd");
    assertThat(DistributedIndexSearch.preparedBytesOf(new CountingFileIO(), file, adapter))
        .isEqualTo(123);
  }

  @ParameterizedTest
  @ValueSource(strings = {"none", "zstd"})
  void bothCodecsAreReadableForReestimation(String codec) throws IOException {
    String location = writePayload("codec-" + codec + ".puffin", codec);
    IndexFile file = file(location, null);
    TrustAdapter adapter = new TrustAdapter(123, READER, PROFILE);

    assertThat(DistributedIndexSearch.preparedBytesOf(new CountingFileIO(), file, adapter))
        .isEqualTo(123);
  }

  @Test
  void manifestRoundTripKeepsTheEstimateTrusted() throws IOException {
    IndexFileMemory declared = memory(123, READER, PROFILE).toBuilder().fileBytes(8192L).build();
    String manifestLocation = temp.resolve("memory.imn").toString();
    IndexManifest.write(
        Files.localOutput(manifestLocation),
        List.of(file("file:/warehouse/index.puffin", declared)));
    List<IndexFile> published = IndexManifest.read(Files.localInput(manifestLocation)).indexFiles();
    assertThat(published).hasSize(1);
    assertThat(published.get(0).memory()).isEqualTo(declared);

    TrustAdapter adapter = new TrustAdapter(123, READER, PROFILE);
    CountingFileIO io = new CountingFileIO();
    assertThat(DistributedIndexSearch.preparedBytesOf(io, published.get(0), adapter))
        .isEqualTo(123);
    assertThat(io.inputFiles()).as("a round-tripped matching record stays trusted").isZero();
    assertThat(adapter.prepareCount()).isZero();
  }

  @Test
  void manifestWithoutMemoryIsReestimated() throws IOException {
    String location = writePayload("legacy.puffin", "zstd");
    String manifestLocation = temp.resolve("legacy.imn").toString();
    IndexManifest.write(Files.localOutput(manifestLocation), List.of(file(location, null)));
    List<IndexFile> published = IndexManifest.read(Files.localInput(manifestLocation)).indexFiles();
    assertThat(published.get(0).memory()).isNull();

    TrustAdapter adapter = new TrustAdapter(123, READER, PROFILE);
    CountingFileIO io = new CountingFileIO();
    assertThat(DistributedIndexSearch.preparedBytesOf(io, published.get(0), adapter))
        .isEqualTo(123);
    assertThat(io.inputFiles()).isEqualTo(1);
    assertThat(adapter.prepareCount()).isEqualTo(1);
  }

  private String writePayload(String name, String codec) throws IOException {
    String location = temp.resolve(name).toString();
    String property = PuffinIndexFileContainer.INDEX_BLOB_COMPRESSION_PROPERTY;
    String previous = System.getProperty(property);
    try {
      System.setProperty(property, codec);
      new PuffinIndexFileContainer()
          .write(Files.localOutput(location), List.of(IndexBlob.of("test-trust", new byte[10])));
      return location;
    } finally {
      if (previous == null) {
        System.clearProperty(property);
      } else {
        System.setProperty(property, previous);
      }
    }
  }

  private static IndexFile file(String location, IndexFileMemory memory) {
    return IndexFile.builder()
        .location(location)
        .kind(IndexFile.Kind.ENTRY_BEARING)
        .blobType("test-trust")
        .entryCount(0)
        .memory(memory)
        .build();
  }

  private static IndexFileMemory memory(
      long preparedBytes, String readerId, String runtimeProfile) {
    return IndexFileMemory.builder()
        .fileBytes(null)
        .payloadBytes(10)
        .preparedBytes(preparedBytes)
        .modelVersion(2)
        .readerId(readerId)
        .runtimeProfile(runtimeProfile)
        .build();
  }

  /** Counts the input files opened and the bytes actually read from them. */
  private static final class CountingFileIO implements FileIO {
    private final AtomicInteger inputFiles = new AtomicInteger();
    private final AtomicLong bytesRead = new AtomicLong();

    int inputFiles() {
      return inputFiles.get();
    }

    long bytesRead() {
      return bytesRead.get();
    }

    @Override
    public InputFile newInputFile(String path) {
      inputFiles.incrementAndGet();
      InputFile file = Files.localInput(path);
      return new InputFile() {
        @Override
        public long getLength() {
          return file.getLength();
        }

        @Override
        public SeekableInputStream newStream() {
          return new CountingStream(file.newStream(), bytesRead);
        }

        @Override
        public String location() {
          return file.location();
        }

        @Override
        public boolean exists() {
          return file.exists();
        }
      };
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

  private static final class CountingStream extends SeekableInputStream {
    private final SeekableInputStream delegate;
    private final AtomicLong bytesRead;

    private CountingStream(SeekableInputStream delegate, AtomicLong bytesRead) {
      this.delegate = delegate;
      this.bytesRead = bytesRead;
    }

    @Override
    public long getPos() throws IOException {
      return delegate.getPos();
    }

    @Override
    public void seek(long newPos) throws IOException {
      delegate.seek(newPos);
    }

    @Override
    public int read() throws IOException {
      int value = delegate.read();
      if (value != -1) {
        bytesRead.incrementAndGet();
      }
      return value;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      int read = delegate.read(b, off, len);
      if (read > 0) {
        bytesRead.addAndGet(read);
      }
      return read;
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }
  }

  private record TrustDefinition(long bytes, String readerId, String runtimeProfile)
      implements IndexDefinition {
    @Override
    public String type() {
      return "test-trust";
    }

    @Override
    public int fieldId() {
      return 1;
    }
  }

  private static final class TrustAdapter implements IndexAdapter {
    private final long preparedBytes;
    private final String readerId;
    private final String runtimeProfile;
    private final AtomicInteger prepares = new AtomicInteger();

    private TrustAdapter(long preparedBytes, String readerId, String runtimeProfile) {
      this.preparedBytes = preparedBytes;
      this.readerId = readerId;
      this.runtimeProfile = runtimeProfile;
    }

    int prepareCount() {
      return prepares.get();
    }

    @Override
    public PreparedSearch prepare(ByteBuffer payload) {
      prepares.incrementAndGet();
      return new PreparedSearch() {
        @Override
        public List<IndexSearchResult> search(ByteBuffer query, int topK) {
          return List.of();
        }

        @Override
        public long estimatedBytes() {
          return preparedBytes;
        }
      };
    }

    @Override
    public IndexDefinition definition() {
      return new TrustDefinition(preparedBytes, readerId, runtimeProfile);
    }

    @Override
    public IndexBuildResult encodeEntriesWithMemory(List<IndexEntry> entries) {
      return IndexBuildResult.of(
          ByteBuffer.wrap(new byte[10]), memory(preparedBytes, readerId, runtimeProfile));
    }

    @Override
    public String entryBlobType() {
      return "test-trust";
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
