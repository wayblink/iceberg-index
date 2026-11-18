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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Files;
import org.apache.iceberg.index.IndexEntry;
import org.apache.iceberg.index.IndexFile;
import org.apache.iceberg.index.IndexFileMemory;
import org.apache.iceberg.index.IndexSearchResult;
import org.apache.iceberg.index.PuffinIndexFileContainer;
import org.apache.iceberg.index.vector.VectorIndexAdapter;
import org.apache.iceberg.index.vector.VectorIndexDefinition;
import org.apache.iceberg.index.vector.VectorIndexDefinition.Metric;
import org.apache.iceberg.index.vector.VectorIndexDefinition.VectorQuantizer;
import org.apache.iceberg.index.vector.VectorIndexDefinition.VectorStructure;
import org.apache.iceberg.index.vector.VectorIndexTypes;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * What a task asks storage for when it opens an index file whose length the manifest recorded.
 *
 * <p>The length locates the container footer, bounds the payload before it is read and reserves
 * room in the cache. Without it each of those asks storage for the length, and on an object store
 * each ask is a HEAD. These tests count length probes and reads on a file IO that models one.
 */
class TestDistributedIndexSearchColdOpen {
  private static final int LISTS = 8;
  private static final int DIMENSION = 2;
  private static final int TOP_K = 5;

  @TempDir private File tempDir;

  @BeforeAll
  static void register() {
    VectorIndexTypes.register();
  }

  @Test
  void aKnownLengthRangeOpenDoesNotAskStorageForTheLength() throws IOException {
    VectorIndexAdapter vector = adapter(true);
    ByteBuffer payload = vector.encodeAuxiliaryPayload(entries(vector), 1);
    ByteBuffer query = vector.encodeQuery(new float[] {1F, 1F}, 1);
    List<IndexSearchResult> expected = vector.prepareAuxiliary(payload).search(query, TOP_K);
    String location = write(vector, payload);
    long length = new File(location).length();
    CountingIO io = new CountingIO();

    try (DistributedIndexSearch.OpenedSearch opened =
        DistributedIndexSearch.openSearch(
            io,
            location,
            vector,
            ExecutorIndexCache.Config.disabled(),
            readerKey(vector),
            true,
            length)) {
      assertThat(io.lengthProbes()).as("the length is already known").isZero();
      assertThat(opened.rangeSearch()).isNotNull();
      VectorIndexAdapter.OpenReadMetrics metrics = opened.rangeSearch().openReadMetrics();
      assertThat(metrics).isNotNull();
      assertThat(metrics.opens()).isEqualTo(1);
      assertThat(metrics.lengthRequests()).isZero();
      assertThat(metrics.footerBytes()).as("the footer still has to be read").isPositive();
      assertThat(metrics.descriptionBytes()).isPositive();
      assertThat(search(opened, query)).isEqualTo(expected);
    }
  }

  @Test
  void aRangeOpenWithoutARecordedLengthProbesTheLengthOnce() throws IOException {
    VectorIndexAdapter vector = adapter(true);
    ByteBuffer payload = vector.encodeAuxiliaryPayload(entries(vector), 1);
    ByteBuffer query = vector.encodeQuery(new float[] {1F, 1F}, 1);
    List<IndexSearchResult> expected = vector.prepareAuxiliary(payload).search(query, TOP_K);
    String location = write(vector, payload);
    CountingIO io = new CountingIO();

    try (DistributedIndexSearch.OpenedSearch opened =
        DistributedIndexSearch.openSearch(
            io,
            location,
            vector,
            ExecutorIndexCache.Config.disabled(),
            readerKey(vector),
            true,
            0L)) {
      assertThat(io.lengthProbes())
          .as("one probe serves the footer and the blob bounds")
          .isEqualTo(1);
      assertThat(opened.rangeSearch().openReadMetrics().lengthRequests()).isEqualTo(1);
      assertThat(search(opened, query)).isEqualTo(expected);
    }
  }

  @Test
  void aKnownLengthWholeFileOpenDoesNotAskStorageForTheLength() throws IOException {
    VectorIndexAdapter vector = adapter(true);
    ByteBuffer payload = vector.encodeAuxiliaryPayload(entries(vector), 1);
    ByteBuffer query = vector.encodeQuery(new float[] {1F, 1F}, 1);
    List<IndexSearchResult> expected = vector.prepareAuxiliary(payload).search(query, TOP_K);
    String location = write(vector, payload);
    long length = new File(location).length();
    CountingIO io = new CountingIO();

    try (DistributedIndexSearch.OpenedSearch opened =
        DistributedIndexSearch.openSearch(
            io,
            location,
            vector,
            ExecutorIndexCache.Config.disabled(),
            readerKey(vector),
            false,
            length)) {
      assertThat(io.lengthProbes()).as("the length is already known").isZero();
      assertThat(opened.rangeSearch()).isNull();
      assertThat(search(opened, query)).isEqualTo(expected);
    }
  }

  @Test
  void aKnownLengthOpenDoesNotProbeEvenWhenTheFileIoIgnoresTheHint() throws IOException {
    VectorIndexAdapter vector = adapter(true);
    ByteBuffer payload = vector.encodeAuxiliaryPayload(entries(vector), 1);
    ByteBuffer query = vector.encodeQuery(new float[] {1F, 1F}, 1);
    List<IndexSearchResult> expected = vector.prepareAuxiliary(payload).search(query, TOP_K);
    String location = write(vector, payload);
    long length = new File(location).length();
    // a FileIO that ignores the length hint still must not ask storage: the container is given the
    // recorded size, so the footer needs no probe of its own
    CountingIO io = new CountingIO(false);

    try (DistributedIndexSearch.OpenedSearch opened =
        DistributedIndexSearch.openSearch(
            io,
            location,
            vector,
            ExecutorIndexCache.Config.disabled(),
            readerKey(vector),
            false,
            length)) {
      assertThat(io.lengthProbes()).as("the recorded length answers the container too").isZero();
      assertThat(search(opened, query)).isEqualTo(expected);
    }
  }

  @Test
  void aWholeFileOpenWithoutARecordedLengthProbesTheLengthThreeTimes() throws IOException {
    VectorIndexAdapter vector = adapter(true);
    ByteBuffer payload = vector.encodeAuxiliaryPayload(entries(vector), 1);
    ByteBuffer query = vector.encodeQuery(new float[] {1F, 1F}, 1);
    List<IndexSearchResult> expected = vector.prepareAuxiliary(payload).search(query, TOP_K);
    String location = write(vector, payload);
    CountingIO io = new CountingIO();

    try (DistributedIndexSearch.OpenedSearch opened =
        DistributedIndexSearch.openSearch(
            io,
            location,
            vector,
            ExecutorIndexCache.Config.disabled(),
            readerKey(vector),
            false,
            0L)) {
      // the reservation hint, the payload bound and the container footer each ask storage once
      assertThat(io.lengthProbes()).isEqualTo(3);
      assertThat(search(opened, query)).isEqualTo(expected);
    }
  }

  @Test
  void theRecordedLengthIsTheManifestFileBytes() {
    IndexFile recorded =
        IndexFile.builder()
            .location("file:/warehouse/index.puffin")
            .kind(IndexFile.Kind.AUXILIARY)
            .blobType(VectorIndexAdapter.AUXILIARY_BLOB_TYPE)
            .entryCount(0)
            .memory(
                IndexFileMemory.builder()
                    .fileBytes(4_096L)
                    .payloadBytes(10)
                    .preparedBytes(10)
                    .modelVersion(2)
                    .readerId("test/1")
                    .runtimeProfile("test")
                    .build())
            .build();
    assertThat(DistributedIndexSearch.recordedLength(recorded)).isEqualTo(4_096L);

    IndexFile unrecorded =
        IndexFile.builder()
            .location("file:/warehouse/legacy.puffin")
            .kind(IndexFile.Kind.AUXILIARY)
            .blobType(VectorIndexAdapter.AUXILIARY_BLOB_TYPE)
            .entryCount(0)
            .build();
    assertThat(DistributedIndexSearch.recordedLength(unrecorded))
        .as("a manifest without a memory record is opened the way it always was")
        .isZero();
  }

  private static List<IndexSearchResult> search(
      DistributedIndexSearch.OpenedSearch opened, ByteBuffer query) {
    return opened.file().searching(prepared -> prepared.search(query.duplicate(), TOP_K));
  }

  private String write(VectorIndexAdapter vector, ByteBuffer payload) throws IOException {
    String location = tempDir.toPath().resolve("index-file.puffin").toString();
    new PuffinIndexFileContainer()
        .write(Files.localOutput(location), List.of(vector.auxiliaryBlob(payload)));
    return location;
  }

  private static String readerKey(VectorIndexAdapter adapter) {
    return DistributedIndexSearch.adapterKey(adapter);
  }

  private static VectorIndexAdapter adapter(boolean blocks) {
    return new VectorIndexAdapter(
        VectorIndexDefinition.builder()
            .fieldId(1)
            .dimension(DIMENSION)
            .metric(Metric.L2)
            .structure(VectorStructure.IVF)
            .quantizer(VectorQuantizer.NONE)
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

  /** A file IO whose length probes are HEAD requests and whose streams are counted. */
  private static final class CountingIO implements FileIO {
    private final boolean honorsLengthHint;
    private int lengthProbes;

    private CountingIO() {
      this(true);
    }

    private CountingIO(boolean honorsLengthHint) {
      this.honorsLengthHint = honorsLengthHint;
    }

    private int lengthProbes() {
      return lengthProbes;
    }

    @Override
    public InputFile newInputFile(String path) {
      return new CountingInputFile(Files.localInput(path), 0L);
    }

    @Override
    public InputFile newInputFile(String path, long length) {
      return new CountingInputFile(Files.localInput(path), honorsLengthHint ? length : 0L);
    }

    @Override
    public OutputFile newOutputFile(String path) {
      return Files.localOutput(path);
    }

    @Override
    public void deleteFile(String path) {
      throw new UnsupportedOperationException();
    }

    private final class CountingInputFile implements InputFile {
      private final InputFile delegate;
      private final long knownLength;

      private CountingInputFile(InputFile delegate, long knownLength) {
        this.delegate = delegate;
        this.knownLength = knownLength;
      }

      @Override
      public long getLength() {
        if (knownLength > 0) {
          return knownLength;
        }

        lengthProbes += 1;
        return delegate.getLength();
      }

      @Override
      public SeekableInputStream newStream() {
        return delegate.newStream();
      }

      @Override
      public String location() {
        return delegate.location();
      }

      @Override
      public boolean exists() {
        return delegate.exists();
      }
    }
  }
}
