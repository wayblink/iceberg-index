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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.index.IndexAdapter.PreparedSearch;
import org.apache.iceberg.index.IndexBuildResult;
import org.apache.iceberg.index.IndexEntry;
import org.apache.iceberg.index.IndexSearchResult;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * Every vector in one list, scanned in full.
 *
 * <p>This is the reference the other structures are measured against: it answers exactly, so a
 * recall figure for an approximate kernel is only meaningful next to it. It is also the simplest
 * kernel, which is what makes it the one to read first when adding another.
 *
 * <p>A search reads each entry where it lies in the payload it was prepared from and keeps a
 * bounded top-k of the scores, so it allocates for the results it was asked for rather than for a
 * result object per entry and a sort over all of them. The payload keeps its entry-bearing form and
 * stays readable by the version that wrote it.
 */
public final class BruteforceKernel implements VectorKernel, RetainedBytes {

  private static final long serialVersionUID = 1L;

  public static final String NAME = "bruteforce";

  private static final int FORMAT_VERSION = 1;

  /** The fixed part of an entry: the data file index, the row position and the payload length. */
  private static final int RECORD_PREFIX = 2 * Integer.BYTES + Long.BYTES;

  /** The fixed part of a value: the format version and the dimension. */
  private static final int VALUE_PREFIX = 2 * Integer.BYTES;

  /** The retained fields of the loaded payload, in the layout the estimate describes. */
  private static final long LOADED_FIELDS =
      2L * PreparedStructureSize.REFERENCE_BYTES + 2L * Integer.BYTES;

  /** The retained fields of the searcher: the payload it reads, the dimension and the scoring. */
  private static final long SEARCHER_FIELDS =
      PreparedStructureSize.REFERENCE_BYTES
          + 2L * Integer.BYTES
          + PreparedStructureSize.REFERENCE_BYTES
          + Long.BYTES;

  private static final long SCORING_FIELDS = PreparedStructureSize.REFERENCE_BYTES;

  private final int dimension;
  private final VectorScoring scoring;

  private BruteforceKernel(int dimension, VectorScoring scoring) {
    this.dimension = dimension;
    this.scoring = scoring;
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public KernelCapabilities capabilities() {
    return new Factory().capabilities();
  }

  @Override
  public int payloadFormatVersion() {
    return FORMAT_VERSION;
  }

  @Override
  public ByteBuffer build(List<IndexEntry> entries, int parallelism) {
    return buildWithMemory(entries, parallelism).payload();
  }

  @Override
  public IndexBuildResult buildWithMemory(List<IndexEntry> entries, int parallelism) {
    // a flat list has nothing to build, so the threads a caller offers go unused; the capability
    // says so rather than letting the hint disappear silently
    Preconditions.checkNotNull(entries, "Index entries are null");
    List<String> dataFiles = VectorPayloads.dataFilesOf(entries);
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(bytes)) {
      VectorPayloads.writeHeader(out, NAME, FORMAT_VERSION);
      // the data files an entry points at are written once, as a dictionary, and an entry refers to
      // one by position: a path is long and every entry of a file repeats it, which is most of the
      // payload once there are many entries per file
      out.writeInt(dataFiles.size());
      for (String dataFile : dataFiles) {
        out.writeUTF(dataFile);
      }

      out.writeInt(entries.size());
      for (IndexEntry entry : entries) {
        VectorPayloads.decodeVector(entry.payload(), dimension, "Entry");
        out.writeInt(dataFiles.indexOf(entry.dataFile()));
        out.writeLong(entry.rowPosition());
        byte[] payload = entry.payload();
        out.writeInt(payload.length);
        out.write(payload);
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to encode vector index entries", e);
    }

    byte[] encoded = bytes.toByteArray();
    return VectorMemoryEstimates.result(
        this,
        ByteBuffer.wrap(encoded),
        preparedBytes(encoded.length, dataFiles),
        Runtime.getRuntime().availableProcessors(),
        Map.of(
            "entry-count", (long) entries.size(),
            "dimension", (long) dimension,
            "data-file-count", (long) dataFiles.size(),
            "data-file-string-bytes", VectorMemoryEstimates.dictionaryBytes(dataFiles)));
  }

  @Override
  public List<IndexEntry> entries(ByteBuffer payload) {
    return ImmutableList.copyOf(decodeEntries(payload).entries);
  }

  /** The fewest bytes one entry can occupy, which bounds how many of them may follow. */
  private long minimumBytesPerEntry() {
    return RECORD_PREFIX + VALUE_PREFIX + (long) Float.BYTES * dimension;
  }

  private DecodedEntries decodeEntries(ByteBuffer payload) {
    Preconditions.checkNotNull(payload, "Entry payload is null");
    byte[] bytes = VectorPayloads.toBytes(payload);
    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
      VectorPayloads.readHeader(in, NAME, FORMAT_VERSION, "entries");
      int dataFileCount = in.readInt();
      Preconditions.checkArgument(
          dataFileCount >= 0, "Invalid vector data file count: %s", dataFileCount);
      String[] dataFiles = new String[dataFileCount];
      for (int i = 0; i < dataFileCount; i++) {
        dataFiles[i] = in.readUTF();
      }

      int entryCount = in.readInt();
      Preconditions.checkArgument(entryCount >= 0, "Invalid vector entry count: %s", entryCount);
      long minimumPerEntry = minimumBytesPerEntry();
      // a count is four bytes wide and the entries it names are at least this large each, so a
      // payload that ends here cannot be claiming them
      int remaining = VectorPayloads.remaining(in);
      Preconditions.checkArgument(
          (long) entryCount * minimumPerEntry <= remaining,
          "Invalid vector entry count %s: the %s-byte payload ends before those entries (each entry"
              + " needs %s bytes, at most %s fit)",
          entryCount,
          remaining,
          minimumPerEntry,
          remaining / minimumPerEntry);
      List<IndexEntry> entries = Lists.newArrayListWithCapacity(entryCount);
      for (int i = 0; i < entryCount; i++) {
        int dataFileIndex = in.readInt();
        Preconditions.checkArgument(
            dataFileIndex >= 0 && dataFileIndex < dataFileCount,
            "Invalid data file index %s of %s",
            dataFileIndex,
            dataFileCount);
        long rowPosition = in.readLong();
        int payloadLength = in.readInt();
        Preconditions.checkArgument(
            payloadLength >= 0, "Invalid vector entry length: %s", payloadLength);
        byte[] entryPayload = new byte[payloadLength];
        in.readFully(entryPayload);
        entries.add(IndexEntry.of(dataFiles[dataFileIndex], rowPosition, entryPayload));
      }

      return new DecodedEntries(entries);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to decode vector index entries", e);
    }
  }

  @Override
  public PreparedSearch prepare(ByteBuffer payload) {
    // the snapshot is this searcher's own array: the caller may reuse or mutate what it passed
    byte[] snapshot = VectorPayloads.toBytes(payload);
    Loaded loaded = load(snapshot);
    return new EntrySearcher(loaded, dimension, scoring, preparedBytes(loaded));
  }

  /**
   * What a prepared search holds: the snapshot its entries are read from, the dictionary of data
   * files they point at, and the small objects that read them. An entry is read where it lies, so
   * it costs the bytes the payload already holds rather than an object of its own.
   */
  private static long preparedBytes(long payloadLength, int dataFileCount, long dictionaryBytes) {
    return PreparedStructureSize.bytes(Math.toIntExact(payloadLength))
        + PreparedStructureSize.references(dataFileCount)
        + dictionaryBytes
        + PreparedStructureSize.object(LOADED_FIELDS)
        + PreparedStructureSize.object(SEARCHER_FIELDS)
        + PreparedStructureSize.object(SCORING_FIELDS);
  }

  private static long preparedBytes(int payloadLength, List<String> dataFiles) {
    return preparedBytes(
        payloadLength, dataFiles.size(), VectorMemoryEstimates.dictionaryBytes(dataFiles));
  }

  /**
   * Prices a payload from the data file count and dictionary size a build recorded, which is
   * everything this prepared form holds besides the snapshot itself.
   */
  @Override
  public long retainedBytes(long payloadBytes, Map<String, Long> structure) {
    int dataFileCount = VectorMemoryEstimates.intCount(structure, "data-file-count");
    long dictionaryBytes = VectorMemoryEstimates.count(structure, "data-file-string-bytes");
    if (dataFileCount < 0 || dictionaryBytes < 0) {
      return 0L;
    }

    return preparedBytes(payloadBytes, dataFileCount, dictionaryBytes);
  }

  private static long preparedBytes(Loaded loaded) {
    return preparedBytes(loaded.snapshot.length, Arrays.asList(loaded.dataFiles));
  }

  /** Validates the payload header and leaves the reader at the first kernel field. */
  private void readHeader(VectorPayloadReader in) {
    VectorPayloadHeader header = VectorPayloadHeader.of(in.payload());
    Preconditions.checkArgument(
        header.readableBy(this),
        "Vector payload was written by %s; this build reads kernel %s version %s",
        header,
        NAME,
        FORMAT_VERSION);
    in.position(Integer.BYTES);
    if (!header.legacy()) {
      int nameLength = in.readUnsignedShort();
      in.skip(nameLength + (long) Integer.BYTES, "the payload header");
    }
  }

  /**
   * Reads the snapshot into the dictionary and verifies every entry it holds.
   *
   * <p>Every entry is checked, so a corrupt payload fails when the searcher is built rather than
   * when a query happens to reach the entry. The counts are bounded by the bytes that are actually
   * present before anything is read from them.
   */
  private Loaded load(byte[] bytes) {
    VectorPayloadReader in = new VectorPayloadReader(bytes);
    readHeader(in);
    String[] dataFiles = readDataFiles(in);
    int entryCount = in.readInt();
    Preconditions.checkArgument(entryCount >= 0, "Invalid vector entry count: %s", entryCount);

    long minimumPerEntry = minimumBytesPerEntry();
    Preconditions.checkArgument(
        (long) entryCount * minimumPerEntry <= in.remaining(),
        "Invalid vector entry count %s: the %s-byte payload ends before those entries (each entry"
            + " needs %s bytes, at most %s fit)",
        entryCount,
        in.remaining(),
        minimumPerEntry,
        in.remaining() / minimumPerEntry);

    int firstEntry = in.position();
    long at = firstEntry;
    for (int entry = 0; entry < entryCount; entry++) {
      Preconditions.checkArgument(
          at + RECORD_PREFIX <= in.limit(),
          "Vector entry %s starts at byte %s, past the %s-byte payload",
          entry,
          at,
          in.limit());
      int dataFileIndex = in.intAt(at);
      Preconditions.checkArgument(
          dataFileIndex >= 0 && dataFileIndex < dataFiles.length,
          "Invalid vector data file index %s at entry %s: the dictionary holds %s data files",
          dataFileIndex,
          entry,
          dataFiles.length);
      int payloadLength = in.intAt(at + RECORD_PREFIX - Integer.BYTES);
      Preconditions.checkArgument(
          payloadLength >= 0, "Invalid vector entry length: %s", payloadLength);
      long entryEnd = at + RECORD_PREFIX + payloadLength;
      Preconditions.checkArgument(
          entryEnd <= in.limit(),
          "Vector entry %s holds %s payload bytes from byte %s, ending at %s after the %s-byte"
              + " payload",
          entry,
          payloadLength,
          at,
          entryEnd,
          in.limit());
      Preconditions.checkArgument(
          payloadLength >= VALUE_PREFIX + (long) Float.BYTES * dimension,
          "Vector entry %s holds %s payload bytes, too few for a %s-dimensional value",
          entry,
          payloadLength,
          dimension);

      long valueAt = at + RECORD_PREFIX;
      int valueVersion = in.intAt(valueAt);
      Preconditions.checkArgument(
          valueVersion == VectorPayloads.VALUE_FORMAT_VERSION,
          "Unsupported vector entry %s value format version: %s",
          entry,
          valueVersion);
      int valueLength = in.intAt(valueAt + Integer.BYTES);
      Preconditions.checkArgument(
          valueLength == dimension,
          "Vector entry %s dimension %s does not match index dimension %s",
          entry,
          valueLength,
          dimension);
      for (int d = 0; d < dimension; d++) {
        float value = in.floatAt(valueAt + VALUE_PREFIX + (long) Float.BYTES * d);
        Preconditions.checkArgument(
            Float.isFinite(value),
            "Vector entry %s dimension %s is not finite: %s",
            (long) entry,
            d,
            value);
      }

      at = entryEnd;
    }

    return new Loaded(bytes, dataFiles, entryCount, firstEntry);
  }

  private static String[] readDataFiles(VectorPayloadReader in) {
    int dataFileCount = in.readInt();
    Preconditions.checkArgument(
        dataFileCount >= 0, "Invalid vector data file count: %s", dataFileCount);
    Preconditions.checkArgument(
        (long) dataFileCount * Short.BYTES <= in.remaining(),
        "Invalid vector data file count %s: the payload holds %s bytes, enough for at most %s",
        dataFileCount,
        in.remaining(),
        in.remaining() / Short.BYTES);
    String[] dataFiles = new String[dataFileCount];
    for (int i = 0; i < dataFileCount; i++) {
      try {
        dataFiles[i] = in.readUTF();
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            "Vector data file dictionary entry " + i + " is truncated: " + e.getMessage(), e);
      }
    }

    return dataFiles;
  }

  /** The loaded payload: its snapshot, the dictionary, and where the entries begin. */
  private static final class Loaded {
    private final byte[] snapshot;
    private final String[] dataFiles;
    private final int entryCount;
    private final int firstEntry;

    private Loaded(byte[] snapshot, String[] dataFiles, int entryCount, int firstEntry) {
      this.snapshot = snapshot;
      this.dataFiles = dataFiles;
      this.entryCount = entryCount;
      this.firstEntry = firstEntry;
    }
  }

  private static final class DecodedEntries {
    private final List<IndexEntry> entries;

    private DecodedEntries(List<IndexEntry> entries) {
      this.entries = entries;
    }
  }

  /** A prepared search that reads entries where they lie and keeps a bounded top-k. */
  private static final class EntrySearcher implements PreparedSearch {
    private final Loaded loaded;
    private final int dimension;
    private final VectorScoring scoring;
    private final long bytes;

    private EntrySearcher(Loaded loaded, int dimension, VectorScoring scoring, long bytes) {
      this.loaded = loaded;
      this.dimension = dimension;
      this.scoring = scoring;
      this.bytes = bytes;
    }

    @Override
    public long estimatedBytes() {
      return VectorMemoryEstimates.knownBytes(bytes);
    }

    @Override
    public List<IndexSearchResult> search(ByteBuffer query, int topK) {
      Preconditions.checkArgument(topK >= 0, "Top-k must be non-negative: %s", topK);
      if (topK == 0 || loaded.entryCount == 0) {
        return ImmutableList.of();
      }

      float[] queryVector =
          VectorPayloads.decodeVector(VectorPayloads.toBytes(query), dimension, "Query");
      VectorTopK top =
          new VectorTopK(topK, loaded.entryCount, scoring, index -> loaded.dataFiles[index]);
      VectorPayloadReader reader = new VectorPayloadReader(loaded.snapshot);
      // one array per search, reused for every entry: the vector is scored where it lies, so the
      // arithmetic and the bits are the ones a decoded array would have given
      float[] vector = new float[dimension];
      long at = loaded.firstEntry;
      for (int entry = 0; entry < loaded.entryCount; entry++) {
        int dataFileIndex = reader.intAt(at);
        long rowPosition = reader.longAt(at + Integer.BYTES);
        int payloadLength = reader.intAt(at + RECORD_PREFIX - Integer.BYTES);
        long valueAt = at + RECORD_PREFIX + VALUE_PREFIX;
        for (int d = 0; d < dimension; d++) {
          vector[d] = reader.floatAt(valueAt + (long) Float.BYTES * d);
        }

        top.offer(dataFileIndex, rowPosition, scoring.score(queryVector, vector));
        at += RECORD_PREFIX + (long) payloadLength;
      }

      return top.results();
    }
  }

  static final class Factory implements VectorKernelFactory {

    @Override
    public String name() {
      return NAME;
    }

    @Override
    public KernelCapabilities capabilities() {
      return KernelCapabilities.builder()
          .structure(VectorIndexDefinition.VectorStructure.FLAT)
          .quantizer(VectorIndexDefinition.VectorQuantizer.NONE)
          .metric(VectorIndexDefinition.Metric.COSINE)
          .metric(VectorIndexDefinition.Metric.DOT_PRODUCT)
          .metric(VectorIndexDefinition.Metric.EUCLIDEAN)
          .nativeLibrary(false)
          // the list could be appended to, but a payload is read back by position, so a rebuild is
          // what a refresh does anyway
          .incrementalAdd(false)
          .build();
    }

    @Override
    public VectorKernel create(VectorIndexDefinition definition) {
      return new BruteforceKernel(definition.dimension(), new VectorScoring(definition.metric()));
    }
  }
}
