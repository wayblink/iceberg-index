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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.index.IndexAdapter;
import org.apache.iceberg.index.IndexEntry;
import org.apache.iceberg.index.IndexSearchResult;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Searching a prepared bruteforce searcher, which reads entries where they lie.
 *
 * <p>The reference for every answer is a full sort this test performs itself: decode every entry,
 * score it with {@link VectorScoring}, order by the metric's direction, then the data file path as
 * {@code String.compareTo} orders it, then the row, then the order the entries were visited in. The
 * searcher is not allowed to disagree with that on any input, and the score bits are not allowed to
 * differ either, because the arithmetic is the exact scan's own.
 *
 * <p>Two further contracts live here. {@code prepare} validates every entry of the payload, so a
 * corrupt one fails when the searcher is built rather than when a query happens to reach it. And
 * {@code entries} keeps handing back copies, so a caller cannot reach into an indexed payload
 * through the array an entry returns.
 */
class TestFlatInPlaceSearch {

  private static final int DIMENSION = 8;
  private static final int ENTRIES = 60;

  /** The fixed part of an entry: the data file index, the row position and the payload length. */
  private static final int RECORD_PREFIX = 2 * Integer.BYTES + Long.BYTES;

  @BeforeAll
  static void registerVectorIndexType() {
    VectorIndexTypes.register();
  }

  @Test
  void scoresEveryEntryTheWayTheExactScanDoes() {
    for (VectorIndexDefinition.Metric metric : VectorIndexDefinition.Metric.values()) {
      List<IndexEntry> entries = entries(metric);
      VectorIndexAdapter flat = flatAdapter(metric);
      IndexAdapter.PreparedSearch prepared = flat.prepare(flat.encodeEntries(entries));

      for (int q = 0; q < 8; q++) {
        float[] queryVector = vector(q * 5);
        List<IndexSearchResult> expected = reference(entries, queryVector, ENTRIES, metric);
        List<IndexSearchResult> actual = prepared.search(flat.encodeQuery(queryVector), ENTRIES);

        assertThat(actual).as("%s full probe for query %s", metric, q).hasSameSizeAs(expected);
        for (int rank = 0; rank < expected.size(); rank++) {
          assertThat(actual.get(rank).dataFile())
              .as("%s path at rank %s for query %s", metric, rank, q)
              .isEqualTo(expected.get(rank).dataFile());
          assertThat(actual.get(rank).rowPosition())
              .as("%s row at rank %s for query %s", metric, rank, q)
              .isEqualTo(expected.get(rank).rowPosition());
          assertThat(Double.doubleToLongBits(actual.get(rank).score()))
              .as("%s score bits at rank %s for query %s", metric, rank, q)
              .isEqualTo(Double.doubleToLongBits(expected.get(rank).score()));
        }
      }
    }
  }

  @Test
  void matchesTheFullSortWhenScoresTieAndPathsRepeat() {
    // every entry holds the same vector, so every score ties and only the path and the row order
    // the answers; the paths repeat, the rows arrive out of order, and one entry is a duplicate
    VectorIndexAdapter flat = flatAdapter(VectorIndexDefinition.Metric.EUCLIDEAN);
    float[] shared = vector(1);
    long[] rows = {7, 2, 9, 2, 5};
    String[] paths = {
      "data-1.parquet", "data-0.parquet", "data-1.parquet", "data-0.parquet", "data-0.parquet"
    };
    List<IndexEntry> entries = Lists.newArrayList();
    for (int i = 0; i < rows.length; i++) {
      entries.add(IndexEntry.of(paths[i], rows[i], flat.encodeVector(shared)));
    }

    IndexAdapter.PreparedSearch prepared = flat.prepare(flat.encodeEntries(entries));
    for (int topK : new int[] {1, 2, 3, rows.length}) {
      List<IndexSearchResult> expected =
          reference(entries, vector(3), topK, VectorIndexDefinition.Metric.EUCLIDEAN);
      List<IndexSearchResult> actual = prepared.search(flat.encodeQuery(vector(3)), topK);
      assertThat(actual).as("top-k %s", topK).hasSameSizeAs(expected);
      for (int rank = 0; rank < expected.size(); rank++) {
        assertThat(actual.get(rank).dataFile()).isEqualTo(expected.get(rank).dataFile());
        assertThat(actual.get(rank).rowPosition()).isEqualTo(expected.get(rank).rowPosition());
        assertThat(Double.doubleToLongBits(actual.get(rank).score()))
            .isEqualTo(Double.doubleToLongBits(expected.get(rank).score()));
      }
    }
  }

  @Test
  void ordersTiedZeroScoresByPathThenRow() {
    // a zero vector scores exactly zero under every metric, so a payload of them ties completely;
    // a non-finite score cannot arise here, because the values a payload may hold are finite and
    // the scoring guards a zero denominator, so the order of NaN and the infinities is the shared
    // top-k's own contract, which TestVectorTopK holds it to
    for (VectorIndexDefinition.Metric metric : VectorIndexDefinition.Metric.values()) {
      VectorIndexAdapter flat = flatAdapter(metric);
      float[] zero = new float[DIMENSION];
      List<IndexEntry> entries = Lists.newArrayList();
      for (int i = 0; i < 12; i++) {
        entries.add(IndexEntry.of("data-" + (i % 3) + ".parquet", 11 - i, flat.encodeVector(zero)));
      }

      IndexAdapter.PreparedSearch prepared = flat.prepare(flat.encodeEntries(entries));
      List<IndexSearchResult> expected = reference(entries, zero, 12, metric);
      List<IndexSearchResult> actual = prepared.search(flat.encodeQuery(zero), 12);

      assertThat(actual).as("%s tied zero scores", metric).hasSameSizeAs(expected);
      for (int rank = 0; rank < expected.size(); rank++) {
        assertThat(actual.get(rank).dataFile())
            .as("%s path at rank %s", metric, rank)
            .isEqualTo(expected.get(rank).dataFile());
        assertThat(actual.get(rank).rowPosition())
            .as("%s row at rank %s", metric, rank)
            .isEqualTo(expected.get(rank).rowPosition());
        assertThat(Double.doubleToLongBits(actual.get(rank).score()))
            .as("%s score bits at rank %s", metric, rank)
            .isEqualTo(Double.doubleToLongBits(expected.get(rank).score()));
      }
    }
  }

  @Test
  void takesTheSmallerOfTopKAndTheEntriesItHas() {
    List<IndexEntry> entries = entries(VectorIndexDefinition.Metric.EUCLIDEAN);
    VectorIndexAdapter flat = flatAdapter(VectorIndexDefinition.Metric.EUCLIDEAN);
    IndexAdapter.PreparedSearch prepared = flat.prepare(flat.encodeEntries(entries));
    ByteBuffer query = flat.encodeQuery(vector(3));

    assertThat(prepared.search(query, 0)).isEmpty();
    assertThat(prepared.search(query, 1)).hasSize(1);
    // asking for more than the index holds returns what it holds rather than failing or padding,
    // and a top-k no array could hold is clamped to the entries rather than allocated or silently
    // shrunk
    assertThat(prepared.search(query, ENTRIES + 1)).hasSize(ENTRIES);
    assertThat(prepared.search(query, 1_000_000)).hasSize(ENTRIES);
    assertThat(prepared.search(query, Integer.MAX_VALUE)).hasSize(ENTRIES);
  }

  @Test
  void refusesANegativeTopK() {
    List<IndexEntry> entries = entries(VectorIndexDefinition.Metric.EUCLIDEAN);
    VectorIndexAdapter flat = flatAdapter(VectorIndexDefinition.Metric.EUCLIDEAN);
    IndexAdapter.PreparedSearch prepared = flat.prepare(flat.encodeEntries(entries));
    assertThatThrownBy(() -> prepared.search(flat.encodeQuery(vector(3)), -1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Top-k");
  }

  @Test
  void readsVectorsOfAnyDimension() {
    for (int dimension : new int[] {3, 5, 7}) {
      VectorIndexAdapter flat =
          new VectorIndexAdapter(
              VectorIndexDefinition.builder()
                  .fieldId(1)
                  .dimension(dimension)
                  .metric(VectorIndexDefinition.Metric.COSINE)
                  .structure(VectorIndexDefinition.VectorStructure.FLAT)
                  .build());
      List<IndexEntry> entries = Lists.newArrayList();
      for (int i = 0; i < 20; i++) {
        float[] values = new float[dimension];
        for (int d = 0; d < dimension; d++) {
          values[d] = (float) Math.sin(i * 0.7 + d * 1.3) + 0.5f;
        }
        entries.add(IndexEntry.of("data-" + (i / 6) + ".parquet", i, flat.encodeVector(values)));
      }

      IndexAdapter.PreparedSearch prepared = flat.prepare(flat.encodeEntries(entries));
      for (int q = 0; q < 5; q++) {
        float[] queryVector = new float[dimension];
        for (int d = 0; d < dimension; d++) {
          queryVector[d] = (float) Math.cos(q * 0.9 + d * 0.4) + 0.25f;
        }

        List<IndexSearchResult> expected =
            referenceOf(entries, queryVector, dimension, 20, VectorIndexDefinition.Metric.COSINE);
        List<IndexSearchResult> actual = prepared.search(flat.encodeQuery(queryVector), 20);
        assertThat(actual).as("dimension %s query %s", dimension, q).hasSameSizeAs(expected);
        for (int rank = 0; rank < expected.size(); rank++) {
          assertThat(actual.get(rank).rowPosition())
              .as("dimension %s row at rank %s for query %s", dimension, rank, q)
              .isEqualTo(expected.get(rank).rowPosition());
          assertThat(Double.doubleToLongBits(actual.get(rank).score()))
              .as("dimension %s score bits at rank %s for query %s", dimension, rank, q)
              .isEqualTo(Double.doubleToLongBits(expected.get(rank).score()));
        }
      }
    }
  }

  @Test
  void answersTheSameWhateverTheCallerDoesToTheBufferAfterwards() {
    List<IndexEntry> entries = entries(VectorIndexDefinition.Metric.EUCLIDEAN, ENTRIES);
    VectorIndexAdapter flat = flatAdapter(VectorIndexDefinition.Metric.EUCLIDEAN);
    byte[] payload = VectorPayloads.toBytes(flat.encodeEntries(entries));
    ByteBuffer query = flat.encodeQuery(vector(0));

    for (String shape : List.of("heap", "direct", "slice", "read-only")) {
      Source source = source(shape, payload);
      int position = source.buffer.position();
      int limit = source.buffer.limit();

      IndexAdapter.PreparedSearch prepared = flat.prepare(source.buffer);
      assertThat(source.buffer.position())
          .as("%s position after prepare", shape)
          .isEqualTo(position);
      assertThat(source.buffer.limit()).as("%s limit after prepare", shape).isEqualTo(limit);
      List<IndexSearchResult> before = prepared.search(query, 5);

      // the bytes the caller handed in are not the bytes the search reads: overwriting every one of
      // them leaves the answer, and a second search on the same searcher, alone
      source.corrupt();
      assertThat(prepared.search(query, 5))
          .as("%s search after the source was overwritten", shape)
          .isEqualTo(before);
      assertThat(prepared.search(query, 5))
          .as("%s second search on the same searcher", shape)
          .isEqualTo(before);

      // and a searcher prepared from the overwritten bytes fails rather than answering from them
      assertThatThrownBy(() -> flat.prepare(source.buffer))
          .as("%s prepare from overwritten bytes", shape)
          .isInstanceOf(IllegalArgumentException.class);
    }
  }

  @Test
  @Timeout(60)
  void answersTheSameWhenTheSearcherIsSearchedConcurrently() throws Exception {
    List<IndexEntry> entries = entries(VectorIndexDefinition.Metric.EUCLIDEAN, 400);
    VectorIndexAdapter flat = flatAdapter(VectorIndexDefinition.Metric.EUCLIDEAN);
    IndexAdapter.PreparedSearch prepared = flat.prepare(flat.encodeEntries(entries));

    List<ByteBuffer> queries = Lists.newArrayList();
    List<List<IndexSearchResult>> singleThreaded = Lists.newArrayList();
    for (int q = 0; q < 16; q++) {
      ByteBuffer query = flat.encodeQuery(vector(q * 11));
      queries.add(query);
      singleThreaded.add(prepared.search(query, 10));
    }

    ExecutorService pool = Executors.newFixedThreadPool(4);
    try {
      List<Callable<List<List<IndexSearchResult>>>> tasks = Lists.newArrayList();
      for (int thread = 0; thread < 4; thread++) {
        tasks.add(
            () -> {
              List<List<IndexSearchResult>> answers = Lists.newArrayList();
              for (ByteBuffer query : queries) {
                answers.add(prepared.search(query, 10));
              }
              return answers;
            });
      }

      for (Future<List<List<IndexSearchResult>>> future : pool.invokeAll(tasks)) {
        List<List<IndexSearchResult>> answers = future.get(60, TimeUnit.SECONDS);
        for (int q = 0; q < queries.size(); q++) {
          assertThat(answers.get(q))
              .as("concurrent answer for query %s", q)
              .isEqualTo(singleThreaded.get(q));
        }
      }
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  void preparesTheLegacyHeaderFormat() {
    // the header written before kernels were named is a bare format version, and the entries under
    // it are read the same way, so an index built before the header changed still answers
    List<IndexEntry> entries = entries(VectorIndexDefinition.Metric.EUCLIDEAN, 12);
    VectorIndexAdapter flat = flatAdapter(VectorIndexDefinition.Metric.EUCLIDEAN);
    float[] queryVector = vector(4);

    IndexAdapter.PreparedSearch prepared = flat.prepare(ByteBuffer.wrap(legacyPayload(entries)));
    List<IndexSearchResult> expected =
        reference(entries, queryVector, 12, VectorIndexDefinition.Metric.EUCLIDEAN);
    List<IndexSearchResult> actual = prepared.search(flat.encodeQuery(queryVector), 12);
    assertThat(actual).hasSameSizeAs(expected);
    for (int rank = 0; rank < expected.size(); rank++) {
      assertThat(actual.get(rank).dataFile()).isEqualTo(expected.get(rank).dataFile());
      assertThat(actual.get(rank).rowPosition()).isEqualTo(expected.get(rank).rowPosition());
      assertThat(Double.doubleToLongBits(actual.get(rank).score()))
          .isEqualTo(Double.doubleToLongBits(expected.get(rank).score()));
    }
  }

  @Test
  void refusesAPayloadItCannotRead() {
    List<IndexEntry> entries = entries(VectorIndexDefinition.Metric.EUCLIDEAN, 4);
    VectorIndexAdapter flat = flatAdapter(VectorIndexDefinition.Metric.EUCLIDEAN);
    byte[] payload = VectorPayloads.toBytes(flat.encodeEntries(entries));
    byte[] value = value(1, DIMENSION, vector(0));

    // a record cut short: the length it declares runs past the bytes that follow it
    byte[] truncated = Arrays.copyOf(payload, payload.length - 3);
    // an entry that points at a data file the dictionary does not hold
    byte[] badIndex = replaceRecord(payload, 0, record(9, 0, value));
    // a value whose format version this build does not read
    byte[] badVersion = replaceRecord(payload, 0, record(0, 0, value(99, DIMENSION, vector(0))));
    // a value of another dimension, which would be scored against the wrong query
    byte[] badDimension =
        replaceRecord(
            payload, 0, record(0, 0, value(1, 3, 1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f, 7.0f, 8.0f)));
    // a value holding a number no vector may hold
    byte[] notFinite =
        replaceRecord(
            payload,
            0,
            record(
                0,
                0,
                value(1, DIMENSION, Float.NaN, 1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f, 7.0f, 8.0f)));
    // more entries than the payload holds records for
    byte[] tooMany = withEntryCount(payload, entries.size() + 3);
    // a payload another kernel wrote
    byte[] otherKernel = withKernelName(payload, "ivf");

    for (byte[] corrupt :
        new byte[][] {
          truncated, badIndex, badVersion, badDimension, notFinite, tooMany, otherKernel
        }) {
      assertThatThrownBy(() -> flat.prepare(ByteBuffer.wrap(corrupt)))
          .as("prepare of a corrupt payload")
          .isInstanceOf(IllegalArgumentException.class);
    }
  }

  @Test
  void entriesKeepTheirPayloadsImmutable() {
    List<IndexEntry> entries = entries(VectorIndexDefinition.Metric.EUCLIDEAN, 6);
    VectorIndexAdapter flat = flatAdapter(VectorIndexDefinition.Metric.EUCLIDEAN);
    byte[] payload = VectorPayloads.toBytes(flat.encodeEntries(entries));

    List<IndexEntry> decoded = flat.decodeEntries(ByteBuffer.wrap(payload));
    assertThat(decoded).isEqualTo(entries);
    for (int i = 0; i < decoded.size(); i++) {
      assertThat(decoded.get(i).payload()).isEqualTo(entries.get(i).payload());
      // the array an entry hands back is a copy: writing over it changes neither the entry nor the
      // next array the same entry hands back
      byte[] handed = decoded.get(i).payload();
      Arrays.fill(handed, (byte) 0xFF);
      assertThat(decoded.get(i).payload()).isEqualTo(entries.get(i).payload());
    }

    // and the entries are their own: overwriting the buffer they were decoded from leaves them
    Arrays.fill(payload, (byte) 0xFF);
    assertThat(decoded).isEqualTo(entries);
  }

  @Test
  void agreesWithTheIvfSearchOnTheSameEntries() {
    // both kernels answer through the same bounded top-k and the same payload reader, so the exact
    // scan and a partition search that visits every partition return the same records in the same
    // order for the same entries
    for (VectorIndexDefinition.Metric metric : VectorIndexDefinition.Metric.values()) {
      List<IndexEntry> entries = entries(metric, 120);
      VectorIndexAdapter flat = flatAdapter(metric);
      VectorIndexAdapter ivf =
          new VectorIndexAdapter(
              VectorIndexDefinition.builder()
                  .fieldId(1)
                  .dimension(DIMENSION)
                  .metric(metric)
                  .structure(VectorIndexDefinition.VectorStructure.IVF)
                  .property(IvfOptions.NLIST, "4")
                  .property(IvfOptions.NPROBE, "4")
                  .build());

      IndexAdapter.PreparedSearch scan = flat.prepare(flat.encodeEntries(entries));
      IndexAdapter.PreparedSearch partitioned =
          ivf.prepareAuxiliary(ivf.encodeAuxiliaryPayload(entries, 1));
      for (int q = 0; q < 6; q++) {
        ByteBuffer query = flat.encodeQuery(vector(q * 7));
        assertThat(partitioned.search(query, 30))
            .as("%s ivf against the scan for query %s", metric, q)
            .isEqualTo(scan.search(query, 30));
      }
    }
  }

  /** The answer an independent full sort of the same entries gives, best first. */
  private static List<IndexSearchResult> reference(
      List<IndexEntry> entries,
      float[] queryVector,
      int topK,
      VectorIndexDefinition.Metric metric) {
    return referenceOf(entries, queryVector, DIMENSION, topK, metric);
  }

  private static List<IndexSearchResult> referenceOf(
      List<IndexEntry> entries,
      float[] queryVector,
      int dimension,
      int topK,
      VectorIndexDefinition.Metric metric) {
    VectorScoring scoring = new VectorScoring(metric);
    List<IndexSearchResult> results = Lists.newArrayList();
    for (IndexEntry entry : entries) {
      float[] vector = VectorPayloads.decodeVector(entry.payload(), dimension, "Entry");
      results.add(
          IndexSearchResult.of(
              entry.dataFile(), entry.rowPosition(), scoring.score(queryVector, vector)));
    }

    results.sort(scoring.comparator());
    if (results.size() > topK) {
      results = Lists.newArrayList(results.subList(0, topK));
    }

    return results;
  }

  /** A buffer in one of the shapes a caller can hand in, and a way to overwrite its bytes. */
  private static final class Source {
    private final ByteBuffer buffer;
    private final Runnable overwrite;

    private Source(ByteBuffer buffer, Runnable overwrite) {
      this.buffer = buffer;
      this.overwrite = overwrite;
    }

    private void corrupt() {
      overwrite.run();
    }
  }

  private static Source source(String shape, byte[] bytes) {
    switch (shape) {
      case "heap":
        byte[] heap = bytes.clone();
        return new Source(ByteBuffer.wrap(heap), () -> Arrays.fill(heap, (byte) 0xFF));
      case "direct":
        ByteBuffer direct = ByteBuffer.allocateDirect(bytes.length);
        direct.put(bytes);
        direct.flip();
        return new Source(
            direct,
            () -> {
              ByteBuffer copy = direct.duplicate();
              for (int i = copy.position(); i < copy.limit(); i++) {
                copy.put(i, (byte) 0xFF);
              }
            });
      case "slice":
        ByteBuffer larger = ByteBuffer.allocate(bytes.length + 32);
        larger.position(9);
        larger.put(bytes);
        larger.position(9);
        larger.limit(9 + bytes.length);
        return new Source(
            larger.slice(),
            () -> {
              byte[] storage = larger.array();
              Arrays.fill(storage, 9, 9 + bytes.length, (byte) 0xFF);
            });
      case "read-only":
        byte[] hidden = bytes.clone();
        return new Source(
            ByteBuffer.wrap(hidden).asReadOnlyBuffer(), () -> Arrays.fill(hidden, (byte) 0xFF));
      default:
        throw new IllegalArgumentException("Unknown buffer shape: " + shape);
    }
  }

  /** The header written before kernels were named, over the same dictionary and entries. */
  private static byte[] legacyPayload(List<IndexEntry> entries) {
    List<String> dataFiles = VectorPayloads.dataFilesOf(entries);
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(bytes)) {
      out.writeInt(2);
      out.writeInt(dataFiles.size());
      for (String dataFile : dataFiles) {
        out.writeUTF(dataFile);
      }

      out.writeInt(entries.size());
      for (IndexEntry entry : entries) {
        out.writeInt(dataFiles.indexOf(entry.dataFile()));
        out.writeLong(entry.rowPosition());
        byte[] payload = entry.payload();
        out.writeInt(payload.length);
        out.write(payload);
      }

      return bytes.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to write a legacy vector payload", e);
    }
  }

  private static byte[] record(int dataFileIndex, long rowPosition, byte[] value) {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(bytes)) {
      out.writeInt(dataFileIndex);
      out.writeLong(rowPosition);
      out.writeInt(value.length);
      out.write(value);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to write a vector record", e);
    }

    return bytes.toByteArray();
  }

  /** A value with the given header and floats, which need not agree with the header. */
  private static byte[] value(int version, int length, float... values) {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(bytes)) {
      out.writeInt(version);
      out.writeInt(length);
      for (float value : values) {
        out.writeFloat(value);
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to write a vector value", e);
    }

    return bytes.toByteArray();
  }

  /** Rewrites one record of a payload, leaving the rest as it was. */
  private static byte[] replaceRecord(byte[] payload, int record, byte[] replacement) {
    int at = recordOffset(payload, record);
    int oldLength = readIntAt(payload, at + RECORD_PREFIX - Integer.BYTES);
    byte[] rewritten = new byte[payload.length - RECORD_PREFIX - oldLength + replacement.length];
    System.arraycopy(payload, 0, rewritten, 0, at);
    System.arraycopy(replacement, 0, rewritten, at, replacement.length);
    System.arraycopy(
        payload,
        at + RECORD_PREFIX + oldLength,
        rewritten,
        at + replacement.length,
        payload.length - at - RECORD_PREFIX - oldLength);
    return rewritten;
  }

  private static byte[] withEntryCount(byte[] payload, int entryCount) {
    byte[] rewritten = payload.clone();
    writeIntAt(rewritten, firstRecord(payload) - Integer.BYTES, entryCount);
    return rewritten;
  }

  private static byte[] withKernelName(byte[] payload, String kernel) {
    int nameLength = readShortAt(payload, Integer.BYTES);
    byte[] replacement = kernel.getBytes(StandardCharsets.UTF_8);
    byte[] rewritten = new byte[payload.length - nameLength + replacement.length];
    System.arraycopy(payload, 0, rewritten, 0, Integer.BYTES + Short.BYTES);
    writeShortAt(rewritten, Integer.BYTES, replacement.length);
    System.arraycopy(replacement, 0, rewritten, Integer.BYTES + Short.BYTES, replacement.length);
    System.arraycopy(
        payload,
        Integer.BYTES + Short.BYTES + nameLength,
        rewritten,
        Integer.BYTES + Short.BYTES + replacement.length,
        payload.length - Integer.BYTES - Short.BYTES - nameLength);
    return rewritten;
  }

  /** Where the first entry begins: past a named header, the dictionary and the entry count. */
  private static int firstRecord(byte[] payload) {
    int at = Integer.BYTES + Short.BYTES + readShortAt(payload, Integer.BYTES) + Integer.BYTES;
    int dataFileCount = readIntAt(payload, at);
    at += Integer.BYTES;
    for (int i = 0; i < dataFileCount; i++) {
      at += Short.BYTES + readShortAt(payload, at);
    }

    return at + Integer.BYTES;
  }

  /** Where the given entry begins, walking the variable-length records before it. */
  private static int recordOffset(byte[] payload, int record) {
    int at = firstRecord(payload);
    for (int i = 0; i < record; i++) {
      at += RECORD_PREFIX + readIntAt(payload, at + RECORD_PREFIX - Integer.BYTES);
    }

    return at;
  }

  private static int readIntAt(byte[] bytes, int at) {
    return ((bytes[at] & 0xFF) << 24)
        | ((bytes[at + 1] & 0xFF) << 16)
        | ((bytes[at + 2] & 0xFF) << 8)
        | (bytes[at + 3] & 0xFF);
  }

  private static int readShortAt(byte[] bytes, int at) {
    return ((bytes[at] & 0xFF) << 8) | (bytes[at + 1] & 0xFF);
  }

  private static void writeIntAt(byte[] bytes, int at, int value) {
    bytes[at] = (byte) (value >> 24);
    bytes[at + 1] = (byte) (value >> 16);
    bytes[at + 2] = (byte) (value >> 8);
    bytes[at + 3] = (byte) value;
  }

  private static void writeShortAt(byte[] bytes, int at, int value) {
    bytes[at] = (byte) (value >> 8);
    bytes[at + 1] = (byte) value;
  }

  private static List<IndexEntry> entries(VectorIndexDefinition.Metric metric) {
    return entries(metric, ENTRIES);
  }

  private static List<IndexEntry> entries(VectorIndexDefinition.Metric metric, int count) {
    VectorIndexAdapter flat = flatAdapter(metric);
    List<IndexEntry> entries = Lists.newArrayListWithCapacity(count);
    for (int i = 0; i < count; i++) {
      // the paths repeat and the rows do not grow with the position, so a tie can only be broken
      // by the path and the row rather than by the order the entries were written in
      entries.add(
          IndexEntry.of(
              "data-" + (i / 20) + ".parquet", (i * 13) % count, flat.encodeVector(vector(i))));
    }

    return entries;
  }

  private static VectorIndexAdapter flatAdapter(VectorIndexDefinition.Metric metric) {
    return new VectorIndexAdapter(
        VectorIndexDefinition.builder()
            .fieldId(1)
            .dimension(DIMENSION)
            .metric(metric)
            .structure(VectorIndexDefinition.VectorStructure.FLAT)
            .build());
  }

  /** A vector distinct for every entry, so a hit can only be the entry itself. */
  private static float[] vector(int entry) {
    float[] values = new float[DIMENSION];
    for (int d = 0; d < DIMENSION; d++) {
      values[d] = (float) Math.sin(entry * 0.7 + d * 1.3) + 0.5f;
    }

    return values;
  }
}
