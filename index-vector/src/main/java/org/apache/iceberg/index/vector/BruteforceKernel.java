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
import java.util.IdentityHashMap;
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
 */
public final class BruteforceKernel implements VectorKernel {

  private static final long serialVersionUID = 1L;

  public static final String NAME = "bruteforce";

  private static final int FORMAT_VERSION = 1;

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
    long retainedPayloadBytes = 0;
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
        retainedPayloadBytes =
            Math.addExact(retainedPayloadBytes, PreparedStructureSize.bytes(payload.length));
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to encode vector index entries", e);
    }
    long dictionaryBytes = VectorMemoryEstimates.dictionaryBytes(dataFiles);
    return VectorMemoryEstimates.result(
        this,
        ByteBuffer.wrap(bytes.toByteArray()),
        preparedBytes(entries.size(), retainedPayloadBytes, dictionaryBytes),
        Runtime.getRuntime().availableProcessors(),
        Map.of(
            "entry-count", (long) entries.size(),
            "dimension", (long) dimension,
            "data-file-count", (long) dataFiles.size(),
            "data-file-string-bytes", dictionaryBytes,
            "entry-payload-array-bytes", retainedPayloadBytes));
  }

  @Override
  public List<IndexEntry> entries(ByteBuffer payload) {
    return decodeEntries(payload).entries;
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
      List<IndexEntry> entries = Lists.newArrayListWithCapacity(entryCount);
      long retainedPayloadBytes = 0;
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
        retainedPayloadBytes += PreparedStructureSize.bytes(payloadLength);
      }

      return new DecodedEntries(ImmutableList.copyOf(entries), retainedPayloadBytes);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to decode vector index entries", e);
    }
  }

  @Override
  public PreparedSearch prepare(ByteBuffer payload) {
    // the entries are decoded once, so a batch pays the decode per index file instead of per query
    DecodedEntries decoded = decodeEntries(payload);
    return new EntrySearcher(decoded.entries, preparedBytes(decoded));
  }

  /** Estimates retained entries without cloning their immutable payloads to obtain lengths. */
  private static long preparedBytes(DecodedEntries decoded) {
    long dictionaryBytes = 0;
    IdentityHashMap<String, Boolean> seen = new IdentityHashMap<>();
    for (IndexEntry entry : decoded.entries) {
      if (seen.put(entry.dataFile(), Boolean.TRUE) == null) {
        dictionaryBytes += PreparedStructureSize.string(entry.dataFile());
      }
    }

    return preparedBytes(decoded.entries.size(), decoded.payloadBytes, dictionaryBytes);
  }

  private static long preparedBytes(int count, long payloadBytes, long dictionaryBytes) {
    // Both dataFile and payload references are fields, even though the path is shared.
    return PreparedStructureSize.references(count)
        + PreparedStructureSize.object(2L * PreparedStructureSize.REFERENCE_BYTES)
        + PreparedStructureSize.object(2L * PreparedStructureSize.REFERENCE_BYTES + Long.BYTES)
        + (long) count
            * PreparedStructureSize.object(Long.BYTES + 2L * PreparedStructureSize.REFERENCE_BYTES)
        + payloadBytes
        + dictionaryBytes;
  }

  private static final class DecodedEntries {
    private final List<IndexEntry> entries;
    private final long payloadBytes;

    private DecodedEntries(List<IndexEntry> entries, long payloadBytes) {
      this.entries = entries;
      this.payloadBytes = payloadBytes;
    }
  }

  /** A prepared search that reports what it holds. */
  private final class EntrySearcher implements PreparedSearch {
    private final List<IndexEntry> entries;
    private final long bytes;

    private EntrySearcher(List<IndexEntry> entries, long bytes) {
      this.entries = entries;
      this.bytes = bytes;
    }

    @Override
    public List<IndexSearchResult> search(ByteBuffer query, int topK) {
      Preconditions.checkArgument(topK >= 0, "Top-k must be non-negative: %s", topK);
      if (topK == 0) {
        return ImmutableList.of();
      }

      float[] queryVector =
          VectorPayloads.decodeVector(VectorPayloads.toBytes(query), dimension, "Query");
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
      return ImmutableList.copyOf(results);
    }

    @Override
    public long estimatedBytes() {
      return bytes;
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
