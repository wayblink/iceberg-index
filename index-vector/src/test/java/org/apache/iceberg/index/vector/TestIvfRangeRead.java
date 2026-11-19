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
import java.util.Arrays;
import java.util.HashMap;
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

      assertThat(ranges.listsRead()).as("one consumed block per selected list").isEqualTo(3);
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
  void aSparseSelectionFallsBackToTheWholeListAreaAndSaysSo() {
    int blockSize = 1024;
    byte[] payload = new byte[LISTS * blockSize];
    ByteBuffer buffer = ByteBuffer.wrap(payload);
    int[] starts = new int[LISTS];
    for (int list = 0; list < starts.length; list++) {
      starts[list] = list * blockSize;
      buffer.putInt(starts[list], list);
    }

    // seven of eight lists has to read almost the whole list area anyway, so it is read as one
    // range and the fallback says so
    CountingFile fallback = new CountingFile(payload);
    IvfRangeInput input =
        new IvfRangeInput(fallback.newStream(), 0, payload.length, fallback.location());
    try (IvfListRanges ranges = new IvfListRanges(input, starts)) {
      int[] selected = {0, 1, 2, 3, 4, 5, 6};
      ranges.planInReadOrder(selected, null);
      for (int list : selected) {
        assertThat(ranges.read(list).readInt()).isEqualTo(list);
      }

      assertThat(ranges.wholeListAreaReads()).as("the whole-list-area fallback says so").isOne();
      assertThat(fallback.reads).isEqualTo(1);
      assertThat(fallback.bytes).isEqualTo(payload.length);
    }

    // half the lists is below the threshold, so the selection stays sparse and nothing falls back
    CountingFile sparse = new CountingFile(payload);
    IvfRangeInput sparseInput =
        new IvfRangeInput(sparse.newStream(), 0, payload.length, sparse.location());
    try (IvfListRanges ranges = new IvfListRanges(sparseInput, starts)) {
      int[] selected = {0, 1, 2, 3};
      ranges.planInReadOrder(selected, null);
      for (int list : selected) {
        assertThat(ranges.read(list).readInt()).isEqualTo(list);
      }

      assertThat(ranges.wholeListAreaReads()).isZero();
      assertThat(sparse.reads).isEqualTo(1);
      assertThat(sparse.bytes).isEqualTo(4L * blockSize);
    }
  }

  @Test
  void aNegativeMergeGapIsRejected() {
    byte[] payload = new byte[4 * Integer.BYTES];
    int[] starts = new int[4];
    for (int list = 0; list < starts.length; list++) {
      starts[list] = list * Integer.BYTES;
    }

    CountingFile file = new CountingFile(payload);
    IvfRangeInput input = new IvfRangeInput(file.newStream(), 0, payload.length, file.location());
    assertThatThrownBy(() -> new IvfListRanges(input, starts, -1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid merge gap");
  }

  @Test
  void blocksAreSharedAcrossSearches() throws IOException {
    VectorIndexAdapter vector = adapter(VectorQuantizer.RABITQ, true);
    List<IndexEntry> entries = entries(vector);
    ByteBuffer payload = vector.encodeAuxiliaryPayload(entries, 1);
    InMemoryOutputFile file = new InMemoryOutputFile();
    new PuffinIndexFileContainer().write(file, List.of(vector.auxiliaryBlob(payload)));
    CountingFile counted = new CountingFile(file.toByteArray());
    InMemoryListBlocks blocks = new InMemoryListBlocks();
    ByteBuffer query = vector.encodeQuery(new float[] {1F, 1F}, 1);
    IndexAdapter.PreparedSearch full = vector.prepareAuxiliary(payload);

    try (VectorIndexAdapter.RangeSearch first = vector.prepareRange(counted, 0L, blocks)) {
      assertThat(first.search(query, 5)).isEqualTo(full.search(query, 5));
      // the first search reads its lists and leaves them in the shared cache
      assertThat(blocks.stats().loads()).isPositive();
      assertThat(first.blockCacheStats().hits()).isZero();
      assertThat(first.listBlocksRead()).as("one list block consumed").isEqualTo(1);
      assertThat(first.listReadMetrics().bytes()).isPositive();
    }

    try (VectorIndexAdapter.RangeSearch second = vector.prepareRange(counted, 0L, blocks)) {
      assertThat(second.search(query, 5)).isEqualTo(full.search(query, 5));
      // the open still reads the container footer and the description; the lists come from the
      // shared cache, which is what the list metrics and the hit counter say
      assertThat(second.blockCacheStats().hits()).isEqualTo(1);
      assertThat(second.listBlocksRead()).as("a cached list block is still consumed").isEqualTo(1);
      assertThat(second.listReadMetrics().bytes())
          .as("the second search read no list bytes")
          .isZero();
      assertThat(second.listReadMetrics().requests()).isZero();
    }
  }

  @Test
  void aFullyCachedDenseProbeReadsNoStorageAndDoesNotFallBack() {
    int blockSize = 1024;
    int listCount = 8;
    byte[] payload = new byte[listCount * blockSize];
    ByteBuffer buffer = ByteBuffer.wrap(payload);
    int[] starts = new int[listCount];
    for (int list = 0; list < listCount; list++) {
      starts[list] = list * blockSize;
      buffer.putInt(starts[list], list);
    }

    // the blocks are the bytes a previous search of this file would have kept
    CountingFile file = new CountingFile(payload);
    InMemoryListBlocks blocks = new InMemoryListBlocks();
    for (int list = 0; list < listCount; list++) {
      blocks.put(
          file.location(),
          starts[list],
          Arrays.copyOfRange(payload, starts[list], starts[list] + blockSize));
    }

    IvfRangeInput input = new IvfRangeInput(file.newStream(), 0, payload.length, file.location());
    try (IvfListRanges ranges = new IvfListRanges(input, starts, 0, blocks)) {
      int[] dense = {0, 1, 2, 3, 4, 5, 6, 7};
      ranges.planInReadOrder(dense, null);

      assertThat(ranges.wholeListAreaReads())
          .as("a probe whose lists are all cached has no missing bytes to widen")
          .isZero();
      assertThat(blocks.containsCalls)
          .as("the planner asks the shared cache before falling back")
          .isEqualTo(listCount);
      assertThat(file.reads).as("planning a warm probe reads nothing").isZero();

      for (int list : dense) {
        assertThat(ranges.read(list).readInt()).isEqualTo(list);
      }

      assertThat(ranges.listsRead()).isEqualTo(listCount);
      assertThat(file.reads).as("a warm list block is not read from storage").isZero();
    }
  }

  @Test
  void aSearchWithoutASharedCacheReadsEveryTime() throws IOException {
    VectorIndexAdapter vector = adapter(VectorQuantizer.RABITQ, true);
    ByteBuffer payload = vector.encodeAuxiliaryPayload(entries(vector), 1);
    InMemoryOutputFile file = new InMemoryOutputFile();
    new PuffinIndexFileContainer().write(file, List.of(vector.auxiliaryBlob(payload)));
    CountingFile counted = new CountingFile(file.toByteArray());
    ByteBuffer query = vector.encodeQuery(new float[] {1F, 1F}, 1);

    for (int search = 0; search < 2; search++) {
      try (VectorIndexAdapter.RangeSearch opened = vector.prepareRange(counted)) {
        int readsBefore = counted.reads;
        assertThat(opened.search(query, 5))
            .isEqualTo(vector.prepareAuxiliary(payload).search(query, 5));
        assertThat(opened.blockCacheStats().hits()).isZero();
        // with no cache there is no hit to rely on: the list block really comes from storage
        assertThat(opened.listBlocksRead()).isEqualTo(1);
        assertThat(opened.listReadMetrics().requests()).isPositive();
        assertThat(opened.listReadMetrics().bytes()).isPositive();
        assertThat(counted.reads - readsBefore).isPositive();
      }
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

  /** A shared block cache with no budget: a block put stays there for the next search. */
  private static final class InMemoryListBlocks implements ListBlockCache {
    private final Map<String, byte[]> blocks = new HashMap<>();
    private long hits;
    private long misses;
    private long loads;
    private int containsCalls;

    private static String key(String location, int offset, int size) {
      return location + "@" + offset + "+" + size;
    }

    @Override
    public byte[] get(String location, int offset, int size) {
      byte[] block = blocks.get(key(location, offset, size));
      if (block == null) {
        misses++;
        return null;
      }

      hits++;
      return block;
    }

    @Override
    public boolean contains(String location, int offset, int size) {
      // a planner's question must not change the counters a search reports
      containsCalls++;
      return blocks.containsKey(key(location, offset, size));
    }

    @Override
    public void put(String location, int offset, byte[] bytes) {
      blocks.put(key(location, offset, bytes.length), bytes);
      loads++;
    }

    @Override
    public Stats stats() {
      long bytes = 0L;
      for (byte[] block : blocks.values()) {
        bytes += block.length;
      }

      return new Stats(hits, misses, loads, 0L, bytes);
    }
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
