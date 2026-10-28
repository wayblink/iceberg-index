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
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import org.apache.iceberg.Files;
import org.apache.iceberg.index.IndexAdapter;
import org.apache.iceberg.index.IndexBlob;
import org.apache.iceberg.index.IndexDefinition;
import org.apache.iceberg.index.IndexEntry;
import org.apache.iceberg.index.IndexSearchResult;
import org.apache.iceberg.index.PuffinIndexFileContainer;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestDistributedIndexSearchLoad {
  private static final String BLOB_TYPE = "test-load";
  private static final int PAYLOAD_BYTES = 4 << 20;

  @TempDir private Path temp;

  /**
   * A load reads the file once and hands the adapter its own copy of the blob, so it allocates the
   * file, the blob the container reads from it and that copy; the file is not copied again to read
   * it as a container.
   */
  @Test
  void loadCopiesTheFileOnlyToReadIt() throws IOException {
    byte[] payload = new byte[PAYLOAD_BYTES];
    for (int i = 0; i < payload.length; i++) {
      payload[i] = (byte) (i % 89);
    }

    String location = write("load.puffin", payload);
    long fileBytes = java.nio.file.Files.size(Path.of(location));
    RecordingAdapter adapter = new RecordingAdapter();

    // the first load pays for the classes, the second is what a load allocates
    DistributedIndexSearch.loadFileState(new LocalIO(), location, adapter);
    long allocated =
        allocatedBy(() -> DistributedIndexSearch.loadFileState(new LocalIO(), location, adapter));

    assertThat(bytes(adapter.lastPayload)).isEqualTo(payload);
    // the file, the blob read from it and the adapter's copy, with headroom below a fourth payload
    assertThat(allocated)
        .as(
            "bytes allocated loading a %s byte file holding a %s byte blob",
            fileBytes, payload.length)
        .isLessThan(fileBytes + 2L * payload.length + payload.length / 2);
  }

  @Test
  void eachLoadHandsTheAdapterAnIndependentWholePayload() throws IOException {
    byte[] payload = new byte[4096];
    for (int i = 0; i < payload.length; i++) {
      payload[i] = (byte) (i % 7);
    }

    String location = write("independent.puffin", payload);
    RecordingAdapter adapter = new RecordingAdapter();
    DistributedIndexSearch.loadFileState(new LocalIO(), location, adapter);
    ByteBuffer first = adapter.lastPayload;
    assertThat(first.position()).isZero();
    assertThat(first.remaining()).isEqualTo(payload.length);
    first.put(0, (byte) 99);

    DistributedIndexSearch.loadFileState(new LocalIO(), location, adapter);
    assertThat(bytes(adapter.lastPayload)).isEqualTo(payload);
    assertThat(adapter.lastPayload.get(0)).isEqualTo(payload[0]);
  }

  private String write(String name, byte[] payload) throws IOException {
    String location = temp.resolve(name).toString();
    System.setProperty(PuffinIndexFileContainer.INDEX_BLOB_COMPRESSION_PROPERTY, "none");
    try {
      new PuffinIndexFileContainer()
          .write(
              Files.localOutput(location),
              List.of(IndexBlob.of("other", new byte[16]), IndexBlob.of(BLOB_TYPE, payload)));
    } finally {
      System.clearProperty(PuffinIndexFileContainer.INDEX_BLOB_COMPRESSION_PROPERTY);
    }

    return location;
  }

  private static byte[] bytes(ByteBuffer buffer) {
    byte[] bytes = new byte[buffer.remaining()];
    buffer.duplicate().get(bytes);
    return bytes;
  }

  private static long allocatedBy(Runnable load) {
    ThreadMXBean bean = ManagementFactory.getThreadMXBean();
    assumeThat(bean).isInstanceOf(com.sun.management.ThreadMXBean.class);
    com.sun.management.ThreadMXBean threads = (com.sun.management.ThreadMXBean) bean;
    assumeThat(threads.isThreadAllocatedMemorySupported()).isTrue();
    threads.setThreadAllocatedMemoryEnabled(true);
    long thread = Thread.currentThread().getId();
    long before = threads.getThreadAllocatedBytes(thread);
    load.run();
    return threads.getThreadAllocatedBytes(thread) - before;
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

  private record LoadDefinition() implements IndexDefinition {
    @Override
    public String type() {
      return BLOB_TYPE;
    }

    @Override
    public int fieldId() {
      return 1;
    }
  }

  /** Keeps the last payload it was given, read only by the assertions after a load. */
  private static class RecordingAdapter implements IndexAdapter {
    private ByteBuffer lastPayload;

    @Override
    public PreparedSearch prepare(ByteBuffer payload) {
      this.lastPayload = payload;
      return new PreparedSearch() {
        @Override
        public List<IndexSearchResult> search(ByteBuffer query, int topK) {
          return List.of();
        }

        @Override
        public long estimatedBytes() {
          return 64;
        }
      };
    }

    @Override
    public IndexDefinition definition() {
      return new LoadDefinition();
    }

    @Override
    public String entryBlobType() {
      return BLOB_TYPE;
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
