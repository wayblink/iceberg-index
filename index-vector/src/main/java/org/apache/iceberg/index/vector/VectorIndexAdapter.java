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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.index.IndexAdapter;
import org.apache.iceberg.index.IndexBlob;
import org.apache.iceberg.index.IndexBuildResult;
import org.apache.iceberg.index.IndexDefinition;
import org.apache.iceberg.index.IndexEntry;
import org.apache.iceberg.index.IndexSearchResult;
import org.apache.iceberg.index.vector.VectorIndexDefinition.VectorStructure;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.puffin.BlobMetadata;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinReader;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;

/**
 * Turns index entries into payloads and searches them, by handing the work to a kernel.
 *
 * <p>What this class does is everything that does not depend on how the vectors are organised: the
 * definition's identity, the blob types an index file is recorded under, the encoding of a single
 * vector, and how results from several index files are ordered. Building a payload and searching
 * one belongs to a {@link VectorKernel}, which is the seam a third-party index library is plugged
 * into.
 *
 * <p>A definition is checked against what a kernel says it can build before anything else happens,
 * so a request for a structure or a quantizer this build does not implement is refused by name
 * rather than producing an index nothing could search.
 */
public final class VectorIndexAdapter implements IndexAdapter, java.io.Serializable {

  private static final long serialVersionUID = 1L;

  public static final String ENTRY_BLOB_TYPE = "iceberg-vector-entries-v1";

  public static final String AUXILIARY_BLOB_TYPE = "iceberg-vector-auxiliary-v1";

  private final VectorIndexDefinition definition;
  private final VectorKernel kernel;
  private final VectorScoring scoring;

  public VectorIndexAdapter(VectorIndexDefinition definition) {
    Preconditions.checkNotNull(definition, "Vector index definition is null");
    this.definition = definition;
    // the kernel is resolved once, so a definition nothing can build fails here rather than in the
    // middle of a build, and the options are parsed as part of making it
    this.kernel = VectorKernels.forDefinition(definition);
    this.scoring = new VectorScoring(definition.metric());
  }

  @Override
  public IndexDefinition definition() {
    return definition;
  }

  @Override
  public String entryBlobType() {
    return ENTRY_BLOB_TYPE;
  }

  @Override
  public String auxiliaryBlobType() {
    return AUXILIARY_BLOB_TYPE;
  }

  @Override
  public Set<String> readableAuxiliaryBlobTypes() {
    // one type per role: the algorithm that wrote a payload is recorded inside it, so it never
    // belongs in the type a reader looks for
    return ImmutableSet.of(AUXILIARY_BLOB_TYPE);
  }

  @Override
  public IndexBlob auxiliaryBlob(ByteBuffer payload) {
    if (kernel instanceof IvfKernel && IvfKernel.isBlocked(payload)) {
      return IndexBlob.uncompressed(auxiliaryBlobType(), VectorPayloads.toBytes(payload));
    }

    return IndexAdapter.super.auxiliaryBlob(payload);
  }

  /** Cumulative data reads for the IVF lists used by a search task. */
  public record RangeReadMetrics(long nanos, long bytes, long requests) {}

  /**
   * What opening one index file read, split by the region of the file each read touched.
   *
   * <p>The container footer locates the payload, the description region holds the format marker,
   * the centroids and the list directory, and the lists hold the encoded records a search scores. A
   * length probe is a request of its own: it is what a known file length avoids.
   *
   * @param opens how many index files this open covers, which is one
   * @param lengthRequests how many times the file length was asked for, which is zero when the
   *     manifest already recorded it
   * @param footerBytes the bytes read to locate the payload in its container
   * @param footerRequests the requests those bytes took
   * @param descriptionBytes the bytes of the payload's own header, centroids and list directory
   * @param descriptionRequests the requests those bytes took
   * @param listBytes the bytes of the selected lists, including any gap read to keep them together
   * @param listRequests the requests those bytes took
   */
  public record OpenReadMetrics(
      long opens,
      long lengthRequests,
      long footerBytes,
      long footerRequests,
      long descriptionBytes,
      long descriptionRequests,
      long listBytes,
      long listRequests) {

    /** Returns the bytes read, across the three regions of the file. */
    public long bytes() {
      return footerBytes + descriptionBytes + listBytes;
    }

    /** Returns the read requests made, not counting the length probes. */
    public long requests() {
      return footerRequests + descriptionRequests + listRequests;
    }
  }

  /** A search that owns the stream for a single task's IVF reads. */
  public interface RangeSearch extends PreparedSearch, AutoCloseable {
    @Override
    default void close() {}

    default RangeReadMetrics listReadMetrics() {
      return new RangeReadMetrics(0L, 0L, 0L);
    }

    /** What opening the file read, by region, or null when the open did not count it. */
    default OpenReadMetrics openReadMetrics() {
      return null;
    }

    /**
     * How many times a search fell back to reading the whole list area instead of its selected
     * ranges.
     *
     * <p>The fallback reads one contiguous region whose bytes are still reported as list bytes, so
     * a caller that compares selected bytes with the list area needs this count to tell a sparse
     * selection from a whole-area read.
     */
    default long wholeListAreaReads() {
      return 0L;
    }
  }

  /** Opens a block-addressable IVF file, or returns null if it uses whole-file storage. */
  public RangeSearch prepareRange(InputFile file) {
    return prepareRange(file, 0L);
  }

  /**
   * Opens a block-addressable IVF file whose length is already known.
   *
   * <p>The length is what locating a Puffin payload otherwise costs a request for: the container
   * footer is found by its distance from the end of the file. Passing the length the index manifest
   * recorded skips that probe, and asking for it exactly once when the manifest has none replaces
   * the two probes the open used to make.
   *
   * @param file the index file
   * @param knownLength the file length the manifest recorded, or zero when it is unknown
   * @return the range search, or null when the file does not store an uncompressed blocked payload
   */
  public RangeSearch prepareRange(InputFile file, long knownLength) {
    if (!(kernel instanceof IvfKernel ivf)) {
      return null;
    }

    CountedInputFile counted = new CountedInputFile(file);
    long length = knownLength > 0 ? knownLength : counted.getLength();
    counted.fileLength(length);
    BlobMetadata blob = null;
    try (PuffinReader reader = Puffin.read(counted).withFileSize(length).build()) {
      for (BlobMetadata candidate : reader.fileMetadata().blobs()) {
        if (readableAuxiliaryBlobTypes().contains(candidate.type())) {
          blob = candidate;
          break;
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read IVF metadata in " + file.location(), e);
    }

    if (blob == null || blob.compressionCodec() != null) {
      return null;
    }

    Preconditions.checkArgument(
        blob.offset() >= 0
            && blob.length() >= 0
            && blob.length() <= length - blob.offset()
            && length <= Integer.MAX_VALUE,
        "Invalid IVF blob range in %s",
        file.location());
    long footerBytes = counted.bytes();
    long footerRequests = counted.requests();
    IvfRangeInput input =
        new IvfRangeInput(counted.newStream(), blob.offset(), (int) blob.length(), file.location());
    RangeSearch search;
    try {
      search =
          ivf.prepareRange(
              input,
              new IvfKernel.OpenCosts(
                  1L, counted.lengthRequests(), footerBytes, footerRequests, 0L, 0L));
    } catch (RuntimeException | Error failure) {
      try {
        input.close();
      } catch (RuntimeException closeFailure) {
        failure.addSuppressed(closeFailure);
      }

      throw failure;
    }

    if (search == null) {
      input.close();
    }

    return search;
  }

  @Override
  public boolean writesAuxiliaryFiles() {
    // a flat scan keeps its entries in the entry-bearing payload; a structure over them needs a
    // payload of its own
    return definition.structure() != VectorStructure.FLAT;
  }

  public byte[] encodeVector(float[] vector) {
    return VectorPayloads.encodeVector(vector, definition.dimension(), "Index");
  }

  public ByteBuffer encodeQuery(float[] query) {
    Preconditions.checkNotNull(query, "Query vector is null");
    return ByteBuffer.wrap(VectorPayloads.encodeVector(query, definition.dimension(), "Query"));
  }

  /** Encodes a query with its IVF probe count. Other structures use the standard query format. */
  public ByteBuffer encodeQuery(float[] query, int nprobe) {
    Preconditions.checkArgument(nprobe > 0, "Invalid IVF nprobe: %s", nprobe);
    ByteBuffer encoded = encodeQuery(query);
    if (definition.structure() != VectorStructure.IVF) {
      return encoded;
    }

    return ByteBuffer.allocate(encoded.remaining() + Integer.BYTES)
        .put(encoded)
        .putInt(nprobe)
        .flip();
  }

  @Override
  public boolean skipsNullValues() {
    return true;
  }

  @Override
  public byte[] encodeEntryValue(Object columnValue) {
    List<Float> values =
        org.apache.iceberg.relocated.com.google.common.collect.Lists.newArrayList();
    for (Object value : (Iterable<?>) columnValue) {
      values.add(((Number) value).floatValue());
    }

    float[] vector = new float[values.size()];
    for (int i = 0; i < vector.length; i++) {
      vector[i] = values.get(i);
    }
    Preconditions.checkArgument(
        vector.length == definition.dimension(),
        "Vector column has %s dimensions, the index was built for %s",
        vector.length,
        definition.dimension());
    return encodeVector(vector);
  }

  /** The vector an entry holds, for a caller that has a payload rather than the row. */
  public float[] decodeVector(byte[] payload) {
    return VectorPayloads.decodeVector(payload, definition.dimension(), "Entry");
  }

  /** The vector a query encodes to. */
  public float[] decodeQuery(ByteBuffer query) {
    return VectorPayloads.decodeVector(
        VectorPayloads.toBytes(query), definition.dimension(), "Query");
  }

  @Override
  public ByteBuffer encodeEntries(List<IndexEntry> entries) {
    return kernel.build(entries, 1);
  }

  @Override
  public IndexBuildResult encodeEntriesWithMemory(List<IndexEntry> entries) {
    return kernel.buildWithMemory(entries, 1);
  }

  @Override
  public List<IndexEntry> decodeEntries(ByteBuffer payload) {
    return kernel.entries(payload);
  }

  @Override
  public PreparedSearch prepare(ByteBuffer payload) {
    return kernel.prepare(payload);
  }

  @Override
  public PreparedSearch prepareOwned(byte[] payload) {
    return kernel.prepareOwned(payload);
  }

  @Override
  public List<IndexSearchResult> search(ByteBuffer payload, ByteBuffer query, int topK) {
    return kernel.search(payload, query, topK);
  }

  @Override
  public ByteBuffer encodeAuxiliaryPayload(List<IndexEntry> entries) {
    return kernel.build(entries, 1);
  }

  @Override
  public ByteBuffer encodeAuxiliaryPayload(List<IndexEntry> entries, int parallelism) {
    return kernel.build(entries, parallelism);
  }

  @Override
  public IndexBuildResult encodeAuxiliaryPayloadWithMemory(
      List<IndexEntry> entries, int parallelism) {
    return kernel.buildWithMemory(entries, parallelism);
  }

  @Override
  public PreparedSearch prepareAuxiliary(ByteBuffer payload) {
    return kernel.prepare(payload);
  }

  @Override
  public PreparedSearch prepareAuxiliaryOwned(byte[] payload) {
    return kernel.prepareOwned(payload);
  }

  @Override
  public List<IndexSearchResult> searchAuxiliary(ByteBuffer payload, ByteBuffer query, int topK) {
    return kernel.search(payload, query, topK);
  }

  /**
   * Prices a payload from the structural counts a build recorded, when this kernel's prepared form
   * follows from them. A kernel whose prepared form those counts do not describe, such as a graph,
   * answers unknown and the caller prices the payload another way.
   */
  @Override
  public Long retainedBytes(long payloadBytes, Map<String, Long> structure) {
    if (payloadBytes <= 0 || structure.isEmpty() || !(kernel instanceof RetainedBytes sizes)) {
      return null;
    }

    long bytes = sizes.retainedBytes(payloadBytes, structure);
    return bytes > 0 ? bytes : null;
  }

  @Override
  public Comparator<IndexSearchResult> resultComparator() {
    return scoring.comparator();
  }

  /** The kernel this definition resolved to, for tests and for a caller that has to report it. */
  public VectorKernel kernel() {
    return kernel;
  }

  /**
   * An input file that counts the length probes and the bytes and requests read through it.
   *
   * <p>The container footer is read through a reader that seeks to the end of the file, so what the
   * open spends to locate the payload is only visible at the stream. The wrapper reports a read
   * request per read call, which is what a positional read on a range-readable stream costs.
   */
  private static final class CountedInputFile implements InputFile {
    private final InputFile delegate;
    private long lengthRequests;
    private long bytes;
    private long requests;
    private long fileLength;

    private CountedInputFile(InputFile delegate) {
      this.delegate = delegate;
    }

    private long lengthRequests() {
      return lengthRequests;
    }

    private long bytes() {
      return bytes;
    }

    private long requests() {
      return requests;
    }

    private void read(long count) {
      if (count > 0) {
        this.bytes += count;
        this.requests += 1;
      }
    }

    @Override
    public long getLength() {
      this.lengthRequests += 1;
      return delegate.getLength();
    }

    private void fileLength(long length) {
      this.fileLength = length;
    }

    @Override
    public SeekableInputStream newStream() {
      return new CountedStream(delegate.newStream(), this, fileLength);
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

  /**
   * Reads the last bytes of a stream that cannot serve a tail read itself.
   *
   * <p>A stream handed to a reader has to answer {@link RangeReadable}, but the file behind it may
   * not support positional reads. The file length locates the tail, so the read is a seek and a
   * full read rather than an unsupported operation.
   *
   * @return the number of bytes read, which is {@code length}
   */
  static int readTail(
      SeekableInputStream stream, long fileLength, byte[] buffer, int offset, int length)
      throws IOException {
    Preconditions.checkArgument(
        length >= 0 && length <= fileLength,
        "Invalid tail read of %s bytes from a file of %s",
        length,
        fileLength);
    stream.seek(fileLength - length);
    IOUtil.readFully(stream, buffer, offset, length);
    return length;
  }

  /** A seekable stream that reports what it reads to the input file it came from. */
  private static final class CountedStream extends SeekableInputStream implements RangeReadable {
    private final SeekableInputStream delegate;
    private final CountedInputFile owner;
    private final long fileLength;

    private CountedStream(SeekableInputStream delegate, CountedInputFile owner, long fileLength) {
      this.delegate = delegate;
      this.owner = owner;
      this.fileLength = fileLength;
    }

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
      owner.read(value < 0 ? 0 : 1);
      return value;
    }

    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException {
      int read = delegate.read(buffer, offset, length);
      owner.read(read);
      return read;
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
      if (delegate instanceof RangeReadable range) {
        range.readFully(position, buffer, offset, length);
      } else {
        delegate.seek(position);
        IOUtil.readFully(delegate, buffer, offset, length);
      }

      owner.read(length);
    }

    @Override
    public int readTail(byte[] buffer, int offset, int length) throws IOException {
      if (delegate instanceof RangeReadable range) {
        int read = range.readTail(buffer, offset, length);
        owner.read(read);
        return read;
      }

      int read = VectorIndexAdapter.readTail(delegate, fileLength, buffer, offset, length);
      owner.read(read);
      return read;
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }
  }
}
