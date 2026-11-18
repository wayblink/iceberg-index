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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.index.IndexAdapter;
import org.apache.iceberg.index.IndexEntry;
import org.apache.iceberg.index.IndexSearchResult;
import org.apache.iceberg.index.PuffinIndexFileContainer;
import org.apache.iceberg.index.vector.VectorIndexDefinition.Metric;
import org.apache.iceberg.index.vector.VectorIndexDefinition.VectorQuantizer;
import org.apache.iceberg.index.vector.VectorIndexDefinition.VectorStructure;
import org.apache.iceberg.inmemory.InMemoryInputFile;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * What opening a blocked IVF file reads, and what a known length saves.
 *
 * <p>The container footer locates the payload, the payload's description holds the format marker,
 * the centroids and the list directory, and the selected lists hold the records. A file whose
 * length is known must be opened without a length probe, and the bytes already read as the
 * description prefix must be part of the description rather than read a second time.
 */
class TestIvfRangeOpenReads {
  private static final int LISTS = 8;
  private static final int DIMENSION = 2;
  private static final int TOP_K = 5;

  @BeforeAll
  static void register() {
    VectorIndexTypes.register();
  }

  @Test
  void aKnownLengthOpensWithoutAProbeAndReadsEachRegionOnce() throws IOException {
    VectorIndexAdapter vector = adapter(VectorQuantizer.NONE, true);
    ByteBuffer payload = vector.encodeAuxiliaryPayload(entries(vector), 1);
    int metadataEnd = payload.getInt(2 * Integer.BYTES);
    ByteBuffer query = vector.encodeQuery(new float[] {1F, 1F}, 1);
    List<IndexSearchResult> expected = vector.prepareAuxiliary(payload).search(query, TOP_K);
    CountingFile counted = new CountingFile(file(vector, payload));

    try (VectorIndexAdapter.RangeSearch search = vector.prepareRange(counted, counted.length())) {
      assertThat(counted.lengthReads).as("a known length is not asked for again").isZero();
      VectorIndexAdapter.OpenReadMetrics opened = search.openReadMetrics();
      assertThat(opened).isNotNull();
      assertThat(opened.opens()).isEqualTo(1);
      assertThat(opened.lengthRequests()).isZero();
      assertThat(opened.descriptionBytes())
          .as("the prefix read to parse the marker is part of the description")
          .isEqualTo(metadataEnd);
      assertThat(opened.descriptionRequests()).isEqualTo(2);
      assertThat(opened.footerBytes()).isPositive();
      assertThat(opened.footerRequests()).isPositive();
      assertThat(opened.listBytes()).isZero();
      assertThat(opened.bytes())
          .as("the footer and the description are all the open has read")
          .isEqualTo(counted.bytes);

      assertThat(search.search(query.duplicate(), TOP_K)).isNotEmpty().isEqualTo(expected);
      VectorIndexAdapter.OpenReadMetrics afterSearch = search.openReadMetrics();
      assertThat(afterSearch.listBytes()).as("the selected list was read once").isPositive();
      assertThat(afterSearch.listRequests()).isEqualTo(1);
      assertThat(afterSearch.bytes())
          .as("every byte the open and the search read is accounted for")
          .isEqualTo(counted.bytes);
      assertThat(afterSearch.requests()).isEqualTo(counted.reads);
      assertThat(afterSearch.footerBytes()).isEqualTo(opened.footerBytes());
      assertThat(afterSearch.descriptionBytes()).isEqualTo(opened.descriptionBytes());
    }
  }

  @Test
  void aRangeReadableStreamIsReadByPositionAndCountsTheSameBytes() throws IOException {
    VectorIndexAdapter vector = adapter(VectorQuantizer.NONE, true);
    ByteBuffer payload = vector.encodeAuxiliaryPayload(entries(vector), 1);
    ByteBuffer query = vector.encodeQuery(new float[] {1F, 1F}, 1);
    List<IndexSearchResult> expected = vector.prepareAuxiliary(payload).search(query, TOP_K);
    CountingFile counted = new CountingFile(file(vector, payload));
    counted.rangeReadable = true;

    try (VectorIndexAdapter.RangeSearch search = vector.prepareRange(counted, counted.length())) {
      assertThat(search.openReadMetrics().lengthRequests()).isZero();
      assertThat(search.search(query.duplicate(), TOP_K)).isEqualTo(expected);
      assertThat(counted.rangeReads)
          .as("a file that supports positional reads is read that way")
          .isPositive();
      assertThat(search.openReadMetrics().bytes())
          .as("every byte is counted through the range path")
          .isEqualTo(counted.bytes);
    }
  }

  @Test
  void aTailReadOfAStreamWithoutPositionalReadsSeeksToTheEnd() throws IOException {
    byte[] contents = new byte[64];
    for (int i = 0; i < contents.length; i += 1) {
      contents[i] = (byte) i;
    }

    SeekableInputStream stream = new InMemoryInputFile(contents).newStream();
    byte[] tail = new byte[8];
    assertThat(VectorIndexAdapter.readTail(stream, contents.length, tail, 0, tail.length))
        .isEqualTo(tail.length);
    assertThat(tail)
        .containsExactly(
            (byte) 56, (byte) 57, (byte) 58, (byte) 59, (byte) 60, (byte) 61, (byte) 62, (byte) 63);
    assertThatThrownBy(() -> VectorIndexAdapter.readTail(stream, contents.length, tail, 0, 65))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid tail read");
  }

  @Test
  void anUnknownLengthIsProbedOnceInsteadOfTwice() throws IOException {
    VectorIndexAdapter vector = adapter(VectorQuantizer.NONE, true);
    ByteBuffer payload = vector.encodeAuxiliaryPayload(entries(vector), 1);
    ByteBuffer query = vector.encodeQuery(new float[] {1F, 1F}, 1);
    List<IndexSearchResult> expected = vector.prepareAuxiliary(payload).search(query, TOP_K);
    CountingFile counted = new CountingFile(file(vector, payload));

    try (VectorIndexAdapter.RangeSearch search = vector.prepareRange(counted)) {
      assertThat(counted.lengthReads)
          .as("one probe locates the footer and bounds the blob")
          .isEqualTo(1);
      assertThat(search.openReadMetrics().lengthRequests()).isEqualTo(1);
      assertThat(search.search(query.duplicate(), TOP_K)).isEqualTo(expected);
    }
  }

  @Test
  void aRangeReadOfTheSamePayloadGivesTheSameHitsAsAWholeRead() throws IOException {
    VectorIndexAdapter vector = adapter(VectorQuantizer.RABITQ, true);
    ByteBuffer payload = vector.encodeAuxiliaryPayload(entries(vector), 1);
    ByteBuffer query = vector.encodeQuery(new float[] {1F, 1F}, 2);
    // the same records read as one payload: what a non-blocked file or a warm cache hands a search
    IndexAdapter.PreparedSearch whole = vector.prepareAuxiliary(payload);
    List<IndexSearchResult> expected = whole.search(query, TOP_K);
    CountingFile counted = new CountingFile(file(vector, payload));

    try (VectorIndexAdapter.RangeSearch search = vector.prepareRange(counted, counted.length())) {
      assertThat(search.search(query.duplicate(), TOP_K)).isEqualTo(expected);
      // a second search of the same open reuses the range it already holds
      long bytesAfterFirst = counted.bytes;
      assertThat(search.search(query.duplicate(), TOP_K)).isEqualTo(expected);
      assertThat(counted.bytes).isEqualTo(bytesAfterFirst);
    }
  }

  @Test
  void aRecordedLengthThatIsNotTheFileFailsTheOpen() throws IOException {
    VectorIndexAdapter vector = adapter(VectorQuantizer.NONE, true);
    ByteBuffer payload = vector.encodeAuxiliaryPayload(entries(vector), 1);
    CountingFile counted = new CountingFile(file(vector, payload));

    // the length is authoritative, so a wrong one is a corrupt file rather than a second probe
    assertThatThrownBy(() -> vector.prepareRange(counted, counted.length() - 1))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void aWholeFileStorageReturnsNoRangeSearch() throws IOException {
    VectorIndexAdapter vector = adapter(VectorQuantizer.NONE, false);
    ByteBuffer payload = vector.encodeAuxiliaryPayload(entries(vector), 1);
    CountingFile counted = new CountingFile(file(vector, payload));

    assertThat(vector.prepareRange(counted, counted.length())).isNull();
  }

  private static byte[] file(VectorIndexAdapter vector, ByteBuffer payload) throws IOException {
    InMemoryOutputFile file = new InMemoryOutputFile();
    new PuffinIndexFileContainer().write(file, List.of(vector.auxiliaryBlob(payload)));
    return file.toByteArray();
  }

  private static VectorIndexAdapter adapter(VectorQuantizer quantizer, boolean blocks) {
    return new VectorIndexAdapter(
        VectorIndexDefinition.builder()
            .fieldId(1)
            .dimension(DIMENSION)
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

  /** An input file that counts length probes and the reads made through its streams. */
  private static final class CountingFile extends InMemoryInputFile {
    private long bytes;
    private int reads;
    private int rangeReads;
    private int lengthReads;
    private boolean rangeReadable;

    private CountingFile(byte[] contents) {
      super(contents);
    }

    private long length() {
      return super.getLength();
    }

    @Override
    public long getLength() {
      this.lengthReads += 1;
      return super.getLength();
    }

    @Override
    public SeekableInputStream newStream() {
      SeekableInputStream delegate = super.newStream();
      if (rangeReadable) {
        return new RangeReadableStream(delegate, length());
      }

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
            bytes += 1;
            reads += 1;
          }

          return value;
        }

        @Override
        public int read(byte[] buffer, int offset, int length) throws IOException {
          int count = delegate.read(buffer, offset, length);
          if (count > 0) {
            bytes += count;
            reads += 1;
          }

          return count;
        }

        @Override
        public void close() throws IOException {
          delegate.close();
        }
      };
    }

    /** A stream that serves the positional reads an object store would serve. */
    private final class RangeReadableStream extends SeekableInputStream implements RangeReadable {
      private final SeekableInputStream delegate;
      private final long fileLength;

      private RangeReadableStream(SeekableInputStream delegate, long fileLength) {
        this.delegate = delegate;
        this.fileLength = fileLength;
      }

      @Override
      public int read() throws IOException {
        int value = delegate.read();
        if (value >= 0) {
          bytes += 1;
          reads += 1;
        }

        return value;
      }

      @Override
      public int read(byte[] buffer, int offset, int length) throws IOException {
        int count = delegate.read(buffer, offset, length);
        if (count > 0) {
          bytes += count;
          reads += 1;
        }

        return count;
      }

      @Override
      public void readFully(long position, byte[] buffer, int offset, int length)
          throws IOException {
        delegate.seek(position);
        IOUtil.readFully(delegate, buffer, offset, length);
        bytes += length;
        reads += 1;
        rangeReads += 1;
      }

      @Override
      public void seek(long position) throws IOException {
        delegate.seek(position);
      }

      @Override
      public long getPos() throws IOException {
        return delegate.getPos();
      }

      @Override
      public int readTail(byte[] buffer, int offset, int length) throws IOException {
        return VectorIndexAdapter.readTail(delegate, fileLength, buffer, offset, length);
      }

      @Override
      public void close() throws IOException {
        delegate.close();
      }
    }
  }
}
