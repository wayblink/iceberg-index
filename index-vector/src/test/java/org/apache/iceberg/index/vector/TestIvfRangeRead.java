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
package org.apache.iceberg.index.vector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.index.IndexAdapter;
import org.apache.iceberg.index.IndexCandidates;
import org.apache.iceberg.index.IndexEntry;
import org.apache.iceberg.index.PuffinIndexFileContainer;
import org.apache.iceberg.index.vector.VectorIndexDefinition.Metric;
import org.apache.iceberg.index.vector.VectorIndexDefinition.VectorQuantizer;
import org.apache.iceberg.index.vector.VectorIndexDefinition.VectorStructure;
import org.apache.iceberg.inmemory.InMemoryInputFile;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class TestIvfRangeRead {
  private static final int LISTS = 8;

  @BeforeAll
  static void register() {
    VectorIndexTypes.register();
  }

  @ParameterizedTest
  @EnumSource(VectorQuantizer.class)
  void selectedBlocksMatchWholeReads(VectorQuantizer quantizer) throws IOException {
    VectorIndexAdapter vector = adapter(quantizer, true);
    List<IndexEntry> entries = entries(vector);
    ByteBuffer payload = vector.encodeAuxiliaryPayload(entries, 1);
    InMemoryOutputFile file = new InMemoryOutputFile();
    new PuffinIndexFileContainer().write(file, List.of(vector.auxiliaryBlob(payload)));

    CountingFile counted = new CountingFile(file.toByteArray());
    ByteBuffer query = vector.encodeQuery(new float[] {1F, 1F}, 1);
    IndexAdapter.PreparedSearch full = vector.prepareAuxiliary(payload);
    IndexAdapter.PreparedSearch legacy =
        vector.prepareAuxiliary(adapter(quantizer, false).encodeAuxiliaryPayload(entries, 1));
    try (VectorIndexAdapter.RangeSearch search = vector.prepareRange(counted)) {
      assertThat(search).isNotNull();
      long bytesBefore = counted.bytes;
      int readsBefore = counted.reads;
      assertThat(search.search(query, 5)).isEqualTo(full.search(query, 5));
      assertThat(full.search(query, 5)).isEqualTo(legacy.search(query, 5));
      assertThat(counted.reads - readsBefore).isEqualTo(1);
      assertThat(counted.bytes - bytesBefore).isPositive().isLessThan(payload.remaining());

      long bytesAfter = counted.bytes;
      assertThat(search.search(query, 5)).isEqualTo(full.search(query, 5));
      assertThat(counted.bytes).isEqualTo(bytesAfter);
      assertThat(search.listReadMetrics().bytes()).isEqualTo(bytesAfter - bytesBefore);
      assertThat(search.listReadMetrics().requests()).isEqualTo(1);
      assertThat(search.listReadMetrics().nanos()).isPositive();

      IndexCandidates admitted = IndexCandidates.of(Map.of("b", new long[] {62L, 63L}));
      assertThat(search.search(query, 2, admitted))
          .hasSize(2)
          .isEqualTo(full.search(query, 2, admitted));
    }
  }

  @Test
  void adjacentSparseListsShareOneRead() {
    byte[] payload = new byte[8 * Integer.BYTES];
    ByteBuffer buffer = ByteBuffer.wrap(payload);
    int[] starts = new int[8];
    for (int list = 0; list < starts.length; list++) {
      starts[list] = buffer.position();
      buffer.putInt(list);
    }

    CountingFile file = new CountingFile(payload);
    IvfRangeInput input = new IvfRangeInput(file.newStream(), 0, payload.length, file.location());
    try (IvfListRanges ranges = new IvfListRanges(input, starts)) {
      int[] selected = {5, 4, 1};
      ranges.planInReadOrder(selected, null);
      for (int list : selected) {
        assertThat(ranges.read(list).readInt()).isEqualTo(list);
      }

      assertThat(file.reads).isEqualTo(2);
      assertThat(file.bytes).isEqualTo(3 * Integer.BYTES);
    }
  }

  @Test
  void readsEachCoalescedRangeOnceWhenCentroidOrderInterleavesRanges() {
    int blockSize = 3 * 1024 * 1024;
    byte[] payload = new byte[LISTS * blockSize];
    ByteBuffer buffer = ByteBuffer.wrap(payload);
    int[] starts = new int[LISTS];
    for (int list = 0; list < starts.length; list++) {
      starts[list] = list * blockSize;
      buffer.putInt(starts[list], list);
    }

    CountingFile file = new CountingFile(payload);
    IvfRangeInput input = new IvfRangeInput(file.newStream(), 0, payload.length, file.location());
    try (IvfListRanges ranges = new IvfListRanges(input, starts)) {
      int[] selected = {0, 4, 1, 5};
      ranges.planInReadOrder(selected, null);
      for (int list : selected) {
        assertThat(ranges.read(list).readInt()).isEqualTo(list);
      }

      assertThat(file.reads).isEqualTo(2);
      assertThat(file.bytes).isEqualTo(4L * blockSize);
    }
  }

  @Test
  void probingAllListsReadsTheListAreaOnce() throws IOException {
    VectorIndexAdapter vector = adapter(VectorQuantizer.NONE, true);
    ByteBuffer payload = vector.encodeAuxiliaryPayload(entries(vector), 1);
    InMemoryOutputFile file = new InMemoryOutputFile();
    new PuffinIndexFileContainer().write(file, List.of(vector.auxiliaryBlob(payload)));
    CountingFile counted = new CountingFile(file.toByteArray());
    ByteBuffer query = vector.encodeQuery(new float[] {1F, 1F}, LISTS);
    try (VectorIndexAdapter.RangeSearch search = vector.prepareRange(counted)) {
      int readsBefore = counted.reads;
      assertThat(search.search(query, 5))
          .isEqualTo(vector.prepareAuxiliary(payload).search(query, 5));
      assertThat(counted.reads - readsBefore).isEqualTo(1);
    }
  }

  @Test
  void nearbyListsMergeIntoOneReadWhenTheGapIsWithinTheThreshold() {
    int blockSize = 4 * 1024;
    int listCount = 4;
    byte[] payload = new byte[listCount * blockSize];
    ByteBuffer buffer = ByteBuffer.wrap(payload);
    int[] starts = new int[listCount];
    for (int list = 0; list < listCount; list++) {
      starts[list] = list * blockSize;
      buffer.putInt(starts[list], list);
    }

    int[] selected = {0, 2};
    CountingFile joined = new CountingFile(payload);
    IvfRangeInput joinedInput =
        new IvfRangeInput(joined.newStream(), 0, payload.length, joined.location());
    try (IvfListRanges ranges = new IvfListRanges(joinedInput, starts, blockSize)) {
      ranges.planInReadOrder(selected, null);
      for (int list : selected) {
        assertThat(ranges.read(list).readInt()).isEqualTo(list);
      }

      assertThat(joined.reads).as("the gap is read instead of a third request").isEqualTo(1);
      assertThat(joined.bytes).isEqualTo(3L * blockSize);
    }

    // the default keeps the runs apart, so only the selected lists are read
    CountingFile apart = new CountingFile(payload);
    IvfRangeInput apartInput =
        new IvfRangeInput(apart.newStream(), 0, payload.length, apart.location());
    try (IvfListRanges ranges = new IvfListRanges(apartInput, starts)) {
      ranges.planInReadOrder(selected, null);
      for (int list : selected) {
        assertThat(ranges.read(list).readInt()).isEqualTo(list);
      }

      assertThat(apart.reads).isEqualTo(2);
      assertThat(apart.bytes).isEqualTo(2L * blockSize);
    }
  }

  @Test
  void corruptDirectoryFailsBeforeAnyListIsRead() throws IOException {
    VectorIndexAdapter vector = adapter(VectorQuantizer.NONE, true);
    ByteBuffer payload = vector.encodeAuxiliaryPayload(entries(vector), 1);
    int metadataEnd = payload.getInt(2 * Integer.BYTES);
    payload.putInt(metadataEnd - LISTS * 2 * Integer.BYTES, Integer.MAX_VALUE);
    InMemoryOutputFile file = new InMemoryOutputFile();
    new PuffinIndexFileContainer().write(file, List.of(vector.auxiliaryBlob(payload)));
    assertThatThrownBy(() -> vector.prepareRange(file.toInputFile()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("block directory");
  }

  @Test
  void truncatedSelectedBlockFailsDuringSearch() throws IOException {
    VectorIndexAdapter vector = adapter(VectorQuantizer.NONE, true);
    ByteBuffer payload = vector.encodeAuxiliaryPayload(entries(vector), 1);
    InMemoryOutputFile file = new InMemoryOutputFile();
    new PuffinIndexFileContainer().write(file, List.of(vector.auxiliaryBlob(payload)));
    CountingFile counted = new CountingFile(file.toByteArray());
    try (VectorIndexAdapter.RangeSearch search = vector.prepareRange(counted)) {
      counted.truncateReads = true;
      ByteBuffer query = vector.encodeQuery(new float[] {1F, 1F}, 1);
      assertThatThrownBy(() -> search.search(query, 1))
          .isInstanceOf(UncheckedIOException.class)
          .hasMessageContaining("Failed to read IVF range");
    }
  }

  private static VectorIndexAdapter adapter(VectorQuantizer quantizer, boolean blocks) {
    return new VectorIndexAdapter(
        VectorIndexDefinition.builder()
            .fieldId(1)
            .dimension(2)
            .metric(Metric.L2)
            .structure(VectorStructure.IVF)
            .quantizer(quantizer)
            .properties(
                Map.of(
                    "nlist", Integer.toString(LISTS), "ivf-list-blocks", Boolean.toString(blocks)))
            .build());
  }

  private static List<IndexEntry> entries(VectorIndexAdapter vector) {
    List<IndexEntry> entries = new ArrayList<>();
    for (int i = 0; i < 64; i++) {
      entries.add(IndexEntry.of(i < 32 ? "a" : "b", i, vector.encodeVector(new float[] {i, 1F})));
    }

    return entries;
  }

  private static final class CountingFile extends InMemoryInputFile {
    private long bytes;
    private int reads;
    private boolean truncateReads;

    private CountingFile(byte[] contents) {
      super(contents);
    }

    @Override
    public SeekableInputStream newStream() {
      SeekableInputStream delegate = super.newStream();
      return new SeekableInputStream() {
        @Override
        public long getPos() throws IOException {
          return delegate.getPos();
        }

        @Override
        public void seek(long position) throws IOException {
          delegate.seek(position);
        }

        @Override
        public int read() throws IOException {
          int value = delegate.read();
          if (value >= 0) {
            bytes++;
            reads++;
          }

          return value;
        }

        @Override
        public int read(byte[] buffer, int offset, int length) throws IOException {
          int count = truncateReads ? -1 : delegate.read(buffer, offset, length);
          if (count > 0) {
            bytes += count;
            reads++;
          }

          return count;
        }

        @Override
        public void close() throws IOException {
          delegate.close();
        }
      };
    }
  }
}
