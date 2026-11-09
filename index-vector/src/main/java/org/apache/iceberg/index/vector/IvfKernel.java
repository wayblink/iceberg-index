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

import java.io.ByteArrayOutputStream;
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
 * Vectors partitioned around centroids, searched by visiting the nearest partitions.
 *
 * <p>The centroids are trained on the vectors of one index file, so this is a partition of that
 * file rather than of the table: each index file is independent, which is what the distribution of
 * the search relies on and what keeps a build from having to move vectors between machines. A
 * search visits the {@code nprobe} nearest centroids and scans the vectors under them, so what it
 * misses it misses because those vectors sit in a partition it did not visit.
 *
 * <p>The vectors themselves are kept, which is what makes a score the one the exact scan would
 * give. The approximation is entirely in which partitions are visited; a quantizer is what would
 * make the scores approximate as well, and that is a separate choice.
 *
 * <p>Training is deterministic: the sample is every k-th vector rather than a random draw, the
 * starting centroids are points of that sample, and the iteration count is fixed. Rebuilding the
 * same file therefore reproduces the same partitions, which a test that compares recall can rely
 * on.
 */
public final class IvfKernel implements VectorKernel, RetainedBytes {

  private static final long serialVersionUID = 1L;

  public static final String NAME = "ivf";

  private static final int FORMAT_VERSION = 1;

  /** The payload written when the vectors are quantized. */
  private static final int QUANTIZED_FORMAT_VERSION = 2;

  private static final int MAX_ITERATIONS = 25;

  /**
   * The retained fields of the prepared roots, in the layout the estimate describes: the loaded
   * payload, the searcher and the scoring object.
   */
  private static final long LOADED_FIELDS =
      7L * PreparedStructureSize.REFERENCE_BYTES + Integer.BYTES;

  private static final long SEARCHER_FIELDS =
      2L * PreparedStructureSize.REFERENCE_BYTES + 3L * Integer.BYTES + 2L * Long.BYTES;

  private static final long SCORING_FIELDS = PreparedStructureSize.REFERENCE_BYTES;

  private final int dimension;
  private final IvfOptions options;
  private final VectorScoring scoring;

  /** Present when the vectors are stored as codes rather than as themselves. */
  private final RaBitQQuantizer quantizer;

  private IvfKernel(
      int dimension, IvfOptions options, VectorScoring scoring, RaBitQQuantizer quantizer) {
    this.dimension = dimension;
    this.options = options;
    this.scoring = scoring;
    this.quantizer = quantizer;
  }

  private boolean isQuantized() {
    return quantizer != null;
  }

  private int payloadVersion() {
    // the quantized payload keeps a centre and a code per list instead of the vectors, which a
    // reader of the other form would misread rather than reject
    return isQuantized() ? QUANTIZED_FORMAT_VERSION : FORMAT_VERSION;
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
    return payloadVersion();
  }

  @Override
  public ByteBuffer build(List<IndexEntry> entries, int parallelism) {
    return buildWithMemory(entries, parallelism).payload();
  }

  @Override
  public IndexBuildResult buildWithMemory(List<IndexEntry> entries, int parallelism) {
    Preconditions.checkNotNull(entries, "Index entries are null");
    List<IndexEntry> materialized = Lists.newArrayList(entries);
    int count = materialized.size();

    List<String> dataFiles = VectorPayloads.dataFilesOf(materialized);
    float[][] vectors = decodeAll(materialized);

    int listCount = options.nlistFor(count);
    float[][] centroids = listCount == 0 ? new float[0][] : train(vectors, listCount);
    int[][] assignments = assign(vectors, centroids);
    long quantizerBytes = 0;
    ByteArrayOutputStream payload = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(payload)) {
      VectorPayloads.writeHeader(out, NAME, payloadVersion());
      out.writeInt(dimension);
      if (isQuantized()) {
        out.writeInt(quantizer.bits());
        out.writeLong(quantizer.seed());
      }
      out.writeInt(centroids.length);
      for (float[] centroid : centroids) {
        for (float value : centroid) {
          out.writeFloat(value);
        }
      }

      out.writeInt(dataFiles.size());
      for (String dataFile : dataFiles) {
        out.writeUTF(dataFile);
      }

      out.writeInt(count);
      for (int list = 0; list < centroids.length; list++) {
        int[] members = assignments[list];
        if (isQuantized()) {
          quantizerBytes += writeQuantizedList(out, dataFiles, materialized, vectors, members);
        } else {
          out.writeInt(members.length);
          for (int member : members) {
            out.writeInt(dataFiles.indexOf(materialized.get(member).dataFile()));
            out.writeLong(materialized.get(member).rowPosition());
            for (float value : vectors[member]) {
              out.writeFloat(value);
            }
          }
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to encode the IVF index payload", e);
    }

    byte[] encoded = payload.toByteArray();
    long dictionaryBytes = VectorMemoryEstimates.dictionaryBytes(dataFiles);
    return VectorMemoryEstimates.result(
        this,
        ByteBuffer.wrap(encoded),
        preparedBytes(
            encoded.length,
            centroids.length,
            dataFiles.size(),
            dictionaryBytes,
            dimension,
            isQuantized(),
            quantizerBytes),
        Runtime.getRuntime().availableProcessors(),
        Map.of(
            "entry-count",
            (long) count,
            "dimension",
            (long) dimension,
            "list-count",
            (long) centroids.length,
            "data-file-count",
            (long) dataFiles.size(),
            "data-file-string-bytes",
            dictionaryBytes,
            "payload-bytes",
            (long) encoded.length,
            "list-directory-bytes",
            2L * PreparedStructureSize.ints(centroids.length),
            "quantizer-bytes",
            quantizerBytes,
            "quantizer-bits",
            isQuantized() ? (long) quantizer.bits() : 0L,
            "code-bytes",
            isQuantized() ? (long) quantizer.codeBytes() : 0L));
  }

  /** The vectors of the entries, checked against what this index can store. */
  private float[][] decodeAll(List<IndexEntry> entries) {
    float[][] vectors = new float[entries.size()][];
    for (int i = 0; i < entries.size(); i++) {
      vectors[i] = VectorPayloads.decodeVector(entries.get(i).payload(), dimension, "Entry");
      if (scoring.metric() == VectorIndexDefinition.Metric.COSINE) {
        // a vector with no direction cannot be assigned to a partition: every centroid is equally
        // (dis)similar to it, and the score of anything stored under it would be meaningless
        Preconditions.checkArgument(
            norm(vectors[i]) > 0.0,
            "A zero vector cannot be indexed by cosine: data file %s, row position %s",
            entries.get(i).dataFile(),
            entries.get(i).rowPosition());
      }
    }
    return vectors;
  }

  /**
   * Writes one list with the vectors quantized around the list's own centre.
   *
   * <p>The centre is the mean of the list rather than of the whole file: a quantizer is accurate
   * when the residuals it codes are small, and the residuals of a partition are smaller than those
   * of the file it came from.
   */
  private long writeQuantizedList(
      DataOutputStream out,
      List<String> dataFiles,
      List<IndexEntry> entries,
      float[][] vectors,
      int[] members)
      throws IOException {
    float[][] selected = new float[members.length][];
    for (int i = 0; i < members.length; i++) {
      selected[i] = vectors[members[i]];
    }

    // the centre is the mean of the list rather than of the file: a quantizer is accurate when the
    // residuals it codes are small, and the residuals of a partition are smaller than the file's
    RaBitQQuantizer listQuantizer =
        RaBitQQuantizer.train(selected, dimension, quantizer.seed(), quantizer.bits());
    for (float value : listQuantizer.centerValues()) {
      out.writeFloat(value);
    }
    out.writeInt(members.length);

    byte[] code = new byte[listQuantizer.codeBytes()];
    float[] factors = new float[RaBitQQuantizer.FACTORS];
    for (int member : members) {
      out.writeInt(dataFiles.indexOf(entries.get(member).dataFile()));
      out.writeLong(entries.get(member).rowPosition());
      // the quantizer centres and rotates what it is given, so it takes the vector as it is
      listQuantizer.encode(vectors[member], code, 0, factors);
      out.write(code);
      for (float factor : factors) {
        out.writeFloat(factor);
      }
    }

    return listQuantizer.estimatedBytes();
  }

  @Override
  public List<IndexEntry> entries(ByteBuffer payload) {
    throw new UnsupportedOperationException(
        "The " + NAME + " kernel writes auxiliary payloads, which carry no entries to decode");
  }

  @Override
  public PreparedSearch prepare(ByteBuffer payload) {
    // the snapshot is this searcher's own array: the caller may reuse or mutate what it passed
    byte[] snapshot = VectorPayloads.toBytes(payload);
    Loaded loaded = load(snapshot);
    long stride =
        isQuantized()
            ? IvfLayout.quantizedStride(IvfLayout.codeBytes(quantizer.padded(), quantizer.bits()))
            : IvfLayout.plainStride(dimension);
    return new ListSearcher(
        loaded,
        dimension,
        options.nprobe(),
        scoring.metric(),
        stride,
        isQuantized() ? quantizer.codeBytes() : 0,
        preparedBytes(loaded));
  }

  /**
   * What a prepared searcher actually holds: the snapshot U, the list directory, the trained
   * centroids, the data file dictionary, and a quantizer per list.
   *
   * <p>There is no per-record object any more: a record is read where it lies, so its cost is the
   * bytes the payload already holds. The dictionary is counted by identity, once per retained
   * string, and roots are counted separately from the arrays they point at.
   */
  private static long preparedBytes(
      long payloadBytes,
      int listCount,
      int dataFileCount,
      long dictionaryBytes,
      int dimension,
      boolean quantized,
      long quantizerBytes) {
    long bytes =
        PreparedStructureSize.object(LOADED_FIELDS)
            + PreparedStructureSize.object(SEARCHER_FIELDS)
            + PreparedStructureSize.object(SCORING_FIELDS)
            + PreparedStructureSize.bytes(Math.toIntExact(payloadBytes))
            + PreparedStructureSize.ints(listCount)
            + PreparedStructureSize.ints(listCount)
            + PreparedStructureSize.references(listCount)
            + (long) listCount * PreparedStructureSize.floats(dimension)
            + PreparedStructureSize.references(dataFileCount)
            + dictionaryBytes;
    if (quantized) {
      bytes +=
          PreparedStructureSize.references(listCount)
              + (long) listCount * PreparedStructureSize.floats(dimension)
              + PreparedStructureSize.references(listCount)
              + quantizerBytes;
    }

    return bytes;
  }

  /**
   * Prices a payload from the counts a build recorded: the payload itself, one offset and one count
   * per list, one centroid per list, the data file dictionary, and a quantizer per list when the
   * lists are stored as codes.
   */
  @Override
  public long retainedBytes(long payloadBytes, Map<String, Long> structure) {
    int listCount = VectorMemoryEstimates.intCount(structure, "list-count");
    int dataFileCount = VectorMemoryEstimates.intCount(structure, "data-file-count");
    int dimension = VectorMemoryEstimates.intCount(structure, "dimension");
    long dictionaryBytes = VectorMemoryEstimates.count(structure, "data-file-string-bytes");
    long quantizerBytes =
        isQuantized() ? VectorMemoryEstimates.count(structure, "quantizer-bytes") : 0L;
    if (listCount < 0
        || dataFileCount < 0
        || dimension < 0
        || dictionaryBytes < 0
        || quantizerBytes < 0) {
      return 0L;
    }

    return preparedBytes(
        payloadBytes,
        listCount,
        dataFileCount,
        dictionaryBytes,
        dimension,
        isQuantized(),
        quantizerBytes);
  }

  private static long preparedBytes(Loaded loaded) {
    PreparedStructureSize.Receipt receipt = new PreparedStructureSize.Receipt();
    long quantizerBytes = 0;
    if (loaded.listQuantizers != null) {
      for (RaBitQQuantizer listQuantizer : loaded.listQuantizers) {
        quantizerBytes += listQuantizer.estimatedBytes(receipt);
      }
    }

    return preparedBytes(
        loaded.payload.length,
        loaded.listStarts.length,
        loaded.dataFiles.length,
        VectorMemoryEstimates.dictionaryBytes(Arrays.asList(loaded.dataFiles)),
        loaded.centroids.length == 0 ? 0 : loaded.centroids[0].length,
        loaded.listQuantizers != null,
        quantizerBytes);
  }

  /** Validates the payload header and leaves the reader at the first kernel field. */
  private void readHeader(VectorPayloadReader in) {
    VectorPayloadHeader header = VectorPayloadHeader.of(in.payload());
    Preconditions.checkArgument(
        header.readableBy(this),
        "IVF payload was written by %s; this build reads kernel %s version %s",
        header,
        NAME,
        payloadVersion());
    in.position(Integer.BYTES);
    if (!header.legacy()) {
      int nameLength = in.readUnsignedShort();
      in.skip(nameLength + (long) Integer.BYTES, "the payload header");
    }
  }

  /**
   * Reads the snapshot into a list directory and verifies every record it points at.
   *
   * <p>Every record is checked, including those of lists a search would not visit, so a corrupt
   * payload fails when it is loaded rather than when a query happens to probe the wrong list. The
   * counts are bounded by the bytes that are actually present before anything is allocated from
   * them.
   */
  private Loaded load(byte[] bytes) {
    VectorPayloadReader in = new VectorPayloadReader(bytes);
    readHeader(in);
    int dims = in.readInt();
    Preconditions.checkArgument(
        dims == dimension, "IVF payload has %s dimensions, the index has %s", dims, dimension);
    int bits = 0;
    long seed = 0L;
    if (isQuantized()) {
      bits = in.readInt();
      seed = in.readLong();
      if (bits != quantizer.bits()) {
        throw new IllegalArgumentException(
            "IVF payload was written with "
                + bits
                + " bits per dimension, this index reads "
                + quantizer.bits());
      }
    }

    int listCount = in.readInt();
    Preconditions.checkArgument(listCount >= 0, "Invalid IVF list count: %s", listCount);
    long minimumPerList = minimumBytesPerList();
    Preconditions.checkArgument(
        (long) listCount * minimumPerList <= in.remaining(),
        "Invalid IVF list count %s: the %s-byte payload ends before those centroids and list"
            + " headers (each list needs %s bytes, at most %s fit)",
        listCount,
        in.remaining(),
        minimumPerList,
        in.remaining() / minimumPerList);
    checkStructureFits(listCount);

    long centroidBytes = (long) listCount * dimension * Float.BYTES;
    Preconditions.checkArgument(
        centroidBytes <= in.remaining(),
        "IVF payload ends inside the centroids: %s bytes remain, %s needed",
        in.remaining(),
        centroidBytes);
    float[][] centroids = new float[listCount][dimension];
    for (int list = 0; list < listCount; list++) {
      for (int d = 0; d < dimension; d++) {
        centroids[list][d] = in.readFloat();
      }
    }

    String[] dataFiles = readDataFiles(in);
    int entryCount = in.readInt();
    Preconditions.checkArgument(entryCount >= 0, "Invalid IVF entry count: %s", entryCount);

    long stride =
        isQuantized()
            ? IvfLayout.quantizedStride(IvfLayout.codeBytes(quantizer.padded(), bits))
            : IvfLayout.plainStride(dimension);
    int[] listStarts = new int[listCount];
    int[] listCounts = new int[listCount];
    float[][] listCenters = isQuantized() ? new float[listCount][] : null;
    long records = 0;
    for (int list = 0; list < listCount; list++) {
      if (isQuantized()) {
        float[] center = new float[dimension];
        for (int d = 0; d < dimension; d++) {
          center[d] = in.readFloat();
        }
        listCenters[list] = center;
      }

      int count = in.readInt();
      String where = "IVF list " + list;
      Preconditions.checkArgument(count >= 0, "Invalid %s record count: %s", where, count);
      int recordStart = in.position();
      long end = IvfLayout.recordEnd(recordStart, count, stride, where);
      Preconditions.checkArgument(
          end <= in.limit(),
          "%s has %s records of %s bytes from byte %s, ending at %s after the %s-byte payload",
          where,
          count,
          stride,
          recordStart,
          end,
          in.limit());
      for (int record = 0; record < count; record++) {
        long offset = recordStart + (long) record * stride;
        int dataFileIndex = in.intAt(offset);
        Preconditions.checkArgument(
            dataFileIndex >= 0 && dataFileIndex < dataFiles.length,
            "Invalid IVF data file index %s at %s record %s: the dictionary holds %s data files",
            dataFileIndex,
            where,
            record,
            dataFiles.length);
      }

      in.position((int) end);
      listStarts[list] = recordStart;
      listCounts[list] = count;
      records += count;
    }

    Preconditions.checkArgument(
        records == entryCount,
        "Invalid IVF entry count %s: the lists hold %s record%s",
        entryCount,
        records,
        records == 1 ? "" : "s");

    RaBitQQuantizer[] listQuantizers = null;
    if (isQuantized()) {
      listQuantizers = new RaBitQQuantizer[listCount];
      for (int list = 0; list < listCount; list++) {
        listQuantizers[list] = new RaBitQQuantizer(dimension, seed, bits, listCenters[list]);
      }
    }

    return new Loaded(
        bytes,
        listStarts,
        listCounts,
        centroids,
        listCenters,
        dataFiles,
        listQuantizers,
        entryCount);
  }

  /** The least a list can occupy: its centroid, and its own centre and count when quantized. */
  /** The header every Java array carries, which a per-list allocation pays once. */
  private static final int ARRAY_HEADER_BYTES = 16;

  /** The fields of a {@link RaBitQQuantizer} beyond the arrays it references. */
  private static final int QUANTIZER_BYTES = 48;

  /**
   * The structures an index payload may size, which is the ceiling on the payload itself.
   *
   * <p>A real index spends far less than this on structures: the centres and quantizers of a
   * million-vector file with a thousand lists come to tens of megabytes against hundreds of
   * megabytes of payload. The bound exists for a corrupted count, not for a large index.
   */
  static final long MAX_STRUCTURE_BYTES = Integer.MAX_VALUE;

  private long minimumBytesPerList() {
    return (long) Float.BYTES * dimension + IvfLayout.listHeader(dimension, isQuantized());
  }

  /**
   * The bytes a list costs beyond its own payload bytes.
   *
   * <p>The centre is allocated per list, and a quantized list also builds a {@link RaBitQQuantizer}
   * that clones that centre and carries its own copy of the signs. Those allocations are sized by
   * the list count, which is a four-byte field: without a bound, a payload of a few gigabytes names
   * enough lists to ask for tens of gigabytes of structures.
   */
  long structureBytesPerList() {
    long centre = ARRAY_HEADER_BYTES + (long) Float.BYTES * dimension;
    if (!isQuantized()) {
      return centre;
    }

    int padded = Integer.highestOneBit(Math.max(1, dimension) - 1) * 2;
    long signs = ARRAY_HEADER_BYTES + (long) Float.BYTES * padded;
    return centre + QUANTIZER_BYTES + centre + signs;
  }

  /**
   * Refuses a list count whose structures would not fit in what an index payload may size.
   *
   * <p>Package private so the bound is testable: a test cannot build the two gigabyte payload the
   * check exists for, but it can ask the arithmetic directly.
   */
  void checkStructureFits(int listCount) {
    long structureBytes = (long) listCount * structureBytesPerList();
    Preconditions.checkArgument(
        structureBytes <= MAX_STRUCTURE_BYTES,
        "IVF list count %s needs %s bytes of structures, more than the %s an index payload may size",
        listCount,
        structureBytes,
        MAX_STRUCTURE_BYTES);
  }

  private static String[] readDataFiles(VectorPayloadReader in) {
    int dataFileCount = in.readInt();
    Preconditions.checkArgument(
        dataFileCount >= 0, "Invalid IVF data file count: %s", dataFileCount);
    Preconditions.checkArgument(
        (long) dataFileCount * Short.BYTES <= in.remaining(),
        "Invalid IVF data file count %s: the payload holds %s bytes, enough for at most %s",
        dataFileCount,
        in.remaining(),
        in.remaining() / Short.BYTES);
    String[] dataFiles = new String[dataFileCount];
    for (int i = 0; i < dataFileCount; i++) {
      try {
        dataFiles[i] = in.readUTF();
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            "IVF data file dictionary entry " + i + " is truncated: " + e.getMessage(), e);
      }
    }

    return dataFiles;
  }

  /** The loaded payload: its snapshot, a directory into it, and the metadata a search needs. */
  private static final class Loaded {
    private final byte[] payload;
    private final int[] listStarts;
    private final int[] listCounts;
    private final float[][] centroids;
    private final float[][] listCenters;
    private final String[] dataFiles;
    private final RaBitQQuantizer[] listQuantizers;
    private final int entryCount;

    private Loaded(
        byte[] payload,
        int[] listStarts,
        int[] listCounts,
        float[][] centroids,
        float[][] listCenters,
        String[] dataFiles,
        RaBitQQuantizer[] listQuantizers,
        int entryCount) {
      this.payload = payload;
      this.listStarts = listStarts;
      this.listCounts = listCounts;
      this.centroids = centroids;
      this.listCenters = listCenters;
      this.dataFiles = dataFiles;
      this.listQuantizers = listQuantizers;
      this.entryCount = entryCount;
    }
  }

  /** A prepared search that reads records where they lie and keeps a bounded top-k. */
  private static final class ListSearcher implements PreparedSearch {
    private final Loaded loaded;
    private final int dimension;
    private final int nprobe;
    private final VectorScoring scoring;
    private final long bytes;
    private final long stride;
    private final int codeBytes;

    private ListSearcher(
        Loaded loaded,
        int dimension,
        int nprobe,
        VectorIndexDefinition.Metric metric,
        long stride,
        int codeBytes,
        long bytes) {
      this.loaded = loaded;
      this.dimension = dimension;
      this.nprobe = nprobe;
      this.scoring = new VectorScoring(metric);
      this.stride = stride;
      this.codeBytes = codeBytes;
      this.bytes = bytes;
    }

    @Override
    public long estimatedBytes() {
      return VectorMemoryEstimates.knownBytes(bytes);
    }

    @Override
    public List<IndexSearchResult> search(ByteBuffer query, int topK) {
      Preconditions.checkArgument(topK >= 0, "Top-k must be non-negative: %s", topK);
      if (topK == 0 || loaded.entryCount == 0 || loaded.centroids.length == 0) {
        return ImmutableList.of();
      }

      float[] queryVector =
          VectorPayloads.decodeVector(VectorPayloads.toBytes(query), dimension, "Query");
      // the partitions worth visiting, chosen by the same measure the vectors were partitioned by
      int visited = Math.min(nprobe, loaded.centroids.length);
      int[] lists = nearestCentroids(queryVector, loaded.centroids, visited, scoring);
      long candidates = 0;
      for (int list : lists) {
        candidates = Math.addExact(candidates, loaded.listCounts[list]);
      }

      VectorTopK top = new VectorTopK(topK, candidates, scoring, index -> loaded.dataFiles[index]);
      VectorPayloadReader reader = new VectorPayloadReader(loaded.payload);
      if (loaded.listQuantizers == null) {
        scorePlainLists(queryVector, lists, reader, top);
      } else {
        scoreQuantizedLists(queryVector, lists, reader, top);
      }

      return top.results();
    }

    /**
     * Scores the records of unquantized lists, reading each vector into one search-private array.
     */
    private void scorePlainLists(
        float[] queryVector, int[] lists, VectorPayloadReader reader, VectorTopK top) {
      float[] vector = new float[dimension];
      for (int list : lists) {
        int count = loaded.listCounts[list];
        long offset = loaded.listStarts[list];
        for (int record = 0; record < count; record++, offset += stride) {
          int dataFileIndex = reader.intAt(offset);
          long rowPosition = reader.longAt(offset + Integer.BYTES);
          long vectorBase = offset + Integer.BYTES + Long.BYTES;
          for (int d = 0; d < dimension; d++) {
            vector[d] = reader.floatAt(vectorBase + (long) Float.BYTES * d);
          }

          top.offer(dataFileIndex, rowPosition, scoring.score(queryVector, vector));
        }
      }
    }

    /**
     * Scores the records of quantized lists from their codes in place.
     *
     * <p>The query is centred and rotated once per list, as before; only the code and the three
     * factors are read per record, and the arithmetic is the quantizer's own, in the same order and
     * at the same precision as when the code was a decoded array.
     */
    private void scoreQuantizedLists(
        float[] queryVector, int[] lists, VectorPayloadReader reader, VectorTopK top) {
      float[] factors = new float[RaBitQQuantizer.FACTORS];
      for (int list : lists) {
        RaBitQQuantizer listQuantizer = loaded.listQuantizers[list];
        float[] center = loaded.listCenters[list];
        float[] rotatedQuery = listQuantizer.rotateQuery(queryVector);
        double queryLengthSquared = 0.0;
        for (int d = 0; d < dimension; d++) {
          double difference = queryVector[d] - center[d];
          queryLengthSquared += difference * difference;
        }

        double centerSquared = 0.0;
        double querySquared = 0.0;
        for (int d = 0; d < dimension; d++) {
          centerSquared += (double) center[d] * center[d];
          querySquared += (double) queryVector[d] * queryVector[d];
        }

        int count = loaded.listCounts[list];
        long offset = loaded.listStarts[list];
        for (int record = 0; record < count; record++, offset += stride) {
          int dataFileIndex = reader.intAt(offset);
          long rowPosition = reader.longAt(offset + Integer.BYTES);
          long codeBase = offset + Integer.BYTES + Long.BYTES;
          long factorsBase = codeBase + codeBytes;
          for (int f = 0; f < factors.length; f++) {
            factors[f] = reader.floatAt(factorsBase + (long) Float.BYTES * f);
          }

          double score =
              switch (scoring.metric()) {
                case EUCLIDEAN ->
                    Math.sqrt(
                        listQuantizer.estimatedDistanceSquared(
                            loaded.payload, codeBase, factors, rotatedQuery, queryLengthSquared));
                case DOT_PRODUCT ->
                    listQuantizer.estimatedInnerProduct(
                        loaded.payload, codeBase, factors, rotatedQuery, queryVector);
                case COSINE -> {
                  double inner =
                      listQuantizer.estimatedInnerProduct(
                          loaded.payload, codeBase, factors, rotatedQuery, queryVector);
                  double vectorSquared = RaBitQQuantizer.squaredLength(factors, centerSquared);
                  yield vectorSquared <= 0.0 || querySquared <= 0.0
                      ? 0.0
                      : inner / (Math.sqrt(vectorSquared) * Math.sqrt(querySquared));
                }
              };
          top.offer(dataFileIndex, rowPosition, score);
        }
      }
    }
  }

  /** The indexes of the {@code count} centroids closest to the query, best first. */
  private static int[] nearestCentroids(
      float[] query, float[][] centroids, int count, VectorScoring scoring) {
    double[] scores = new double[centroids.length];
    Integer[] order = new Integer[centroids.length];
    for (int list = 0; list < centroids.length; list++) {
      scores[list] = scoring.score(query, centroids[list]);
      order[list] = list;
    }

    java.util.Arrays.sort(
        order,
        (left, right) -> {
          int byScore = scoring.compareScores(scores[left], scores[right]);
          // partitions that score the same are visited in a fixed order, so that a search is
          // reproducible rather than depending on the sort's treatment of equal elements
          return byScore != 0 ? byScore : Integer.compare(left, right);
        });

    int[] nearest = new int[count];
    for (int i = 0; i < count; i++) {
      nearest[i] = order[i];
    }
    return nearest;
  }

  /**
   * Trains {@code listCount} centroids on a sample of the vectors.
   *
   * <p>The sample is every k-th vector, which spreads it over the file without a random draw, so
   * the same file trains the same centroids every time it is built.
   */
  private float[][] train(float[][] vectors, int listCount) {
    int sampleCount = IvfOptions.samplesFor(vectors.length, listCount);
    int stride = Math.max(1, vectors.length / sampleCount);
    float[][] sample = new float[sampleCount][];
    for (int i = 0; i < sampleCount; i++) {
      sample[i] = vectors[Math.min(vectors.length - 1, i * stride)];
    }

    float[][] centroids = new float[listCount][];
    for (int i = 0; i < listCount; i++) {
      // the starting centroids are points of the sample, spread across it
      centroids[i] = sample[Math.min(sampleCount - 1, (int) ((long) i * sampleCount / listCount))];
    }

    for (int iteration = 0; iteration < MAX_ITERATIONS; iteration++) {
      int[] assignment = nearestCentroid(sample, centroids);
      float[][] sums = new float[listCount][dimension];
      int[] sizes = new int[listCount];
      for (int i = 0; i < sample.length; i++) {
        int list = assignment[i];
        sizes[list] += 1;
        for (int d = 0; d < dimension; d++) {
          sums[list][d] += sample[i][d];
        }
      }

      boolean changed = false;
      for (int list = 0; list < listCount; list++) {
        if (sizes[list] == 0) {
          // a centroid nothing was assigned to keeps its position; the list it stands for is
          // dropped when the payload is written
          continue;
        }

        float[] mean = new float[dimension];
        for (int d = 0; d < dimension; d++) {
          mean[d] = sums[list][d] / sizes[list];
        }
        changed |= !java.util.Arrays.equals(mean, centroids[list]);
        centroids[list] = mean;
      }

      if (!changed) {
        break;
      }
    }

    return centroids;
  }

  /**
   * Puts every vector under its nearest centroid, dropping the lists nothing landed in.
   *
   * <p>A dropped list is a centroid no vector is closest to; keeping it would only add a partition
   * that a search might visit and find empty.
   */
  private int[][] assign(float[][] vectors, float[][] centroids) {
    if (centroids.length == 0) {
      return new int[0][];
    }

    int[] assignment = nearestCentroid(vectors, centroids);
    int[][] members = new int[centroids.length][];
    int[] sizes = new int[centroids.length];
    for (int list : assignment) {
      sizes[list] += 1;
    }

    for (int list = 0; list < centroids.length; list++) {
      members[list] = new int[sizes[list]];
    }

    int[] filled = new int[centroids.length];
    for (int i = 0; i < assignment.length; i++) {
      int list = assignment[i];
      members[list][filled[list]++] = i;
    }

    return members;
  }

  /** The index of the centroid each vector is closest to. */
  private int[] nearestCentroid(float[][] vectors, float[][] centroids) {
    int[] assignment = new int[vectors.length];
    for (int i = 0; i < vectors.length; i++) {
      int best = 0;
      double bestScore = scoring.score(vectors[i], centroids[0]);
      for (int list = 1; list < centroids.length; list++) {
        double score = scoring.score(vectors[i], centroids[list]);
        if (scoring.compareScores(score, bestScore) < 0) {
          best = list;
          bestScore = score;
        }
      }
      assignment[i] = best;
    }
    return assignment;
  }

  private static double norm(float[] vector) {
    double squared = 0.0;
    for (float value : vector) {
      squared += (double) value * value;
    }
    return Math.sqrt(squared);
  }

  /** The same partitioning, with the vectors stored as codes rather than as themselves. */
  static final class QuantizedFactory implements VectorKernelFactory {

    @Override
    public String name() {
      return NAME;
    }

    @Override
    public KernelCapabilities capabilities() {
      return KernelCapabilities.builder()
          .structure(VectorIndexDefinition.VectorStructure.IVF)
          .quantizer(VectorIndexDefinition.VectorQuantizer.RABITQ)
          .metric(VectorIndexDefinition.Metric.COSINE)
          .metric(VectorIndexDefinition.Metric.DOT_PRODUCT)
          .metric(VectorIndexDefinition.Metric.EUCLIDEAN)
          .nativeLibrary(false)
          .incrementalAdd(false)
          .build();
    }

    @Override
    public VectorKernel create(VectorIndexDefinition definition) {
      return new IvfKernel(
          definition.dimension(),
          IvfOptions.from(definition.properties(), definition.quantizer()),
          new VectorScoring(definition.metric()),
          RaBitQQuantizer.blank(
              definition.dimension(), RaBitQQuantizer.bitsFrom(definition.properties())));
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
          .structure(VectorIndexDefinition.VectorStructure.IVF)
          .quantizer(VectorIndexDefinition.VectorQuantizer.NONE)
          .metric(VectorIndexDefinition.Metric.COSINE)
          .metric(VectorIndexDefinition.Metric.DOT_PRODUCT)
          .metric(VectorIndexDefinition.Metric.EUCLIDEAN)
          .nativeLibrary(false)
          // the centroids are trained over the whole file, so growing a payload in place would mean
          // retraining them; a refresh rebuilds the index files of the data files that changed
          .incrementalAdd(false)
          .build();
    }

    @Override
    public VectorKernel create(VectorIndexDefinition definition) {
      return new IvfKernel(
          definition.dimension(),
          IvfOptions.from(definition.properties()),
          new VectorScoring(definition.metric()),
          null);
    }
  }
}
