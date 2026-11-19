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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.iceberg.index.IndexAdapter.PreparedSearch;
import org.apache.iceberg.index.IndexBuildResult;
import org.apache.iceberg.index.IndexCandidates;
import org.apache.iceberg.index.IndexEntry;
import org.apache.iceberg.index.IndexSearchResult;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * starting centroids are points of that sample, and the rounds run until the centroids stop
 * changing. Rebuilding the same file therefore reproduces the same partitions, which a test that
 * compares recall can rely on. A definition may lower the training budget or set a convergence
 * tolerance, in which case the build records the rounds and samples it actually spent.
 */
public final class IvfKernel implements VectorKernel, RetainedBytes {
  static final int BLOCK_MAGIC = 0x49564642;
  static final int BLOCK_VERSION = 1;
  static final int BLOCK_PREFIX_BYTES = 3 * Integer.BYTES;
  private static final int DIRECTORY_ENTRY_BYTES = 2 * Integer.BYTES;

  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(IvfKernel.class);

  public static final String NAME = "ivf";

  private static final int FORMAT_VERSION = 1;

  /** The payload written when the vectors are quantized. */
  private static final int QUANTIZED_FORMAT_VERSION = 2;

  /** Keeps a centroid at the origin from making a small absolute move look unbounded. */
  private static final double DISPLACEMENT_EPSILON = 1e-12;

  /**
   * How many vectors the assignment kernel walks against one centroid row before it moves on.
   *
   * <p>Sixteen rows of 2560 dimensions are 320 KB once converted to double: the block stays in the
   * core's cache while one centroid row is compared with all of it. No row ever changes block, so
   * each row's own list is decided without seeing another row.
   */
  private static final int ASSIGN_VECTOR_BLOCK = 16;

  /**
   * Where the optional vector API assignment kernel is loaded from.
   *
   * <p>The class lives in its own source set because the module it needs is an incubator one; it
   * may be absent, and asking for it then falls back to the exact kernel.
   */
  private static final String SIMD_ASSIGN_KERNEL_CLASS =
      "org.apache.iceberg.index.vector.SimdAssignKernel";

  /**
   * The retained fields of the prepared roots, in the layout the estimate describes: the loaded
   * payload, the searcher and the scoring object.
   */
  private static final long LOADED_FIELDS =
      7L * PreparedStructureSize.REFERENCE_BYTES + Integer.BYTES;

  private static final long SEARCHER_FIELDS =
      3L * PreparedStructureSize.REFERENCE_BYTES + 2L * Integer.BYTES + 2L * Long.BYTES;

  private static final long SCORING_FIELDS = PreparedStructureSize.REFERENCE_BYTES;

  private final int dimension;
  private final IvfOptions options;
  private final VectorScoring scoring;

  /** Present when the vectors are stored as codes rather than as themselves. */
  private final RaBitQQuantizer quantizer;

  /**
   * Whether the build in progress assigned a row with the vector API kernel.
   *
   * <p>It is reset when a build starts and read when the build records what it spent, so a
   * definition that asked for the kernel but ran on a JVM that could not load it says zero.
   */
  private IvfKernel(
      int dimension, IvfOptions options, VectorScoring scoring, RaBitQQuantizer quantizer) {
    this.dimension = dimension;
    this.options = options;
    this.scoring = scoring;
    this.quantizer = quantizer;
  }

  boolean isQuantized() {
    return quantizer != null;
  }

  int dimension() {
    return dimension;
  }

  IvfOptions options() {
    return options;
  }

  VectorScoring scoring() {
    return scoring;
  }

  RaBitQQuantizer quantizer() {
    return quantizer;
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
    // How many threads this build may use. One keeps every loop on the path it always took, so a
    // build that does not ask for threads produces the bytes it always did.
    int threads = Math.max(1, Math.min(parallelism, Math.max(1, count)));

    long buildStartNanos = System.nanoTime();
    float[][] vectors = decodeAll(materialized, threads);
    long decodedNanos = System.nanoTime();

    int listCount = options.nlistFor(count);
    Trained trained = listCount == 0 ? Trained.none() : train(vectors, listCount, threads);
    float[][] centroids = trained.centroids;
    long trainedNanos = System.nanoTime();
    Assigned assigned = assign(vectors, centroids, threads);
    int[][] assignments = assigned.members;
    long assignedNanos = System.nanoTime();
    // every list is encoded on its own: the quantizer of a list is trained on that list's vectors,
    // and the data file dictionary is only read, so the lists can be encoded in parallel and then
    // written in order
    byte[][] encodedLists = encodeLists(materialized, vectors, assignments, dataFiles, threads);
    ByteArrayOutputStream payload = new ByteArrayOutputStream();
    boolean blocks = options.listBlocks();
    int directory = 0;
    int[] starts = blocks ? new int[listCount] : null;
    int metadataEnd = 0;
    try (DataOutputStream out = new DataOutputStream(payload)) {
      if (blocks) {
        out.writeInt(BLOCK_MAGIC);
        out.writeInt(BLOCK_VERSION);
        out.writeInt(0);
      }

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
      if (blocks) {
        directory = payload.size();
        for (int list = 0; list < listCount; list++) {
          out.writeLong(0L);
        }

        metadataEnd = payload.size();
      }

      for (int list = 0; list < centroids.length; list++) {
        if (blocks) {
          starts[list] = payload.size();
        }

        out.write(encodedLists[list]);
        // the bytes are held only until they are written: a payload of hundreds of megabytes must
        // not keep a second copy of itself alive for the rest of the build
        encodedLists[list] = null;
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to encode the IVF index payload", e);
    }

    byte[] encoded = payload.toByteArray();
    if (blocks) {
      ByteBuffer patch = ByteBuffer.wrap(encoded);
      patch.putInt(2 * Integer.BYTES, metadataEnd);
      patch.position(directory);
      for (int list = 0; list < listCount; list++) {
        patch.putInt(starts[list]).putInt(assignments[list].length);
      }
    }

    long encodedNanos = System.nanoTime();
    LOG.info(
        "Encoded an IVF payload of {} entries in {} lists: decode {} ms, train {} ms over {} rounds"
            + " and {} samples, assign {} ms, encode {} ms",
        count,
        centroids.length,
        millis(decodedNanos - buildStartNanos),
        millis(trainedNanos - decodedNanos),
        trained.iterations,
        trained.samples,
        millis(assignedNanos - trainedNanos),
        millis(encodedNanos - assignedNanos));

    long dictionaryBytes = VectorMemoryEstimates.dictionaryBytes(dataFiles);
    long quantizerBytes =
        isQuantized() ? RaBitQQuantizer.estimatedListBytes(dimension, listCount) : 0L;
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
        Map.ofEntries(
            Map.entry("entry-count", (long) count),
            Map.entry("dimension", (long) dimension),
            Map.entry("list-count", (long) centroids.length),
            Map.entry("data-file-count", (long) dataFiles.size()),
            Map.entry("data-file-string-bytes", dictionaryBytes),
            Map.entry("payload-bytes", (long) encoded.length),
            Map.entry("list-directory-bytes", 2L * PreparedStructureSize.ints(centroids.length)),
            Map.entry("quantizer-bytes", quantizerBytes),
            Map.entry("quantizer-bits", isQuantized() ? (long) quantizer.bits() : 0L),
            Map.entry("code-bytes", isQuantized() ? (long) quantizer.codeBytes() : 0L),
            Map.entry("train-samples", (long) trained.samples),
            Map.entry("train-iterations", (long) trained.iterations),
            // the build-time knobs that trade quality against time; zero is what a definition that
            // sets none of them runs, and a definition that set a tolerance records it here
            Map.entry("train-tolerance-ppm", options.tolerancePartsPerMillion()),
            Map.entry("assign-kernel-simd", assigned.simd ? 1L : 0L),
            Map.entry("build-kernel-native", 0L)));
  }

  /** The vectors of the entries, checked against what this index can store. */
  private float[][] decodeAll(List<IndexEntry> entries, int threads) {
    float[][] vectors = new float[entries.size()][];
    runInParallel(
        threads,
        entries.size(),
        start -> {
          for (int i = start[0]; i < start[1]; i++) {
            decodeInto(entries, vectors, i);
          }
        });
    return vectors;
  }

  /** Decodes one entry, and refuses a vector the metric cannot use. */
  private void decodeInto(List<IndexEntry> entries, float[][] vectors, int i) {
    {
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
  }

  /**
   * Writes one list with the vectors quantized around the list's own centre.
   *
   * <p>The centre is the mean of the list rather than of the whole file: a quantizer is accurate
   * when the residuals it codes are small, and the residuals of a partition are smaller than those
   * of the file it came from.
   */
  /** Encodes the bytes of every list, in list order, on up to {@code threads} threads. */
  private byte[][] encodeLists(
      List<IndexEntry> entries,
      float[][] vectors,
      int[][] assignments,
      List<String> dataFiles,
      int threads) {
    byte[][] encoded = new byte[assignments.length][];
    runInParallel(
        threads,
        assignments.length,
        range -> {
          for (int list = range[0]; list < range[1]; list++) {
            encoded[list] =
                isQuantized()
                    ? encodeQuantizedList(dataFiles, entries, vectors, assignments[list])
                    : encodeFullList(dataFiles, entries, vectors, assignments[list]);
          }
        });
    return encoded;
  }

  private byte[] encodeQuantizedList(
      List<String> dataFiles, List<IndexEntry> entries, float[][] vectors, int[] members) {
    float[][] selected = new float[members.length][];
    for (int i = 0; i < members.length; i++) {
      selected[i] = vectors[members[i]];
    }

    // the centre is the mean of the list rather than of the file: a quantizer is accurate when the
    // residuals it codes are small, and the residuals of a partition are smaller than the file's
    RaBitQQuantizer listQuantizer =
        RaBitQQuantizer.train(selected, dimension, quantizer.seed(), quantizer.bits());

    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(bytes)) {
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
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to encode an IVF list", e);
    }

    return bytes.toByteArray();
  }

  private byte[] encodeFullList(
      List<String> dataFiles, List<IndexEntry> entries, float[][] vectors, int[] members) {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(bytes)) {
      out.writeInt(members.length);
      for (int member : members) {
        out.writeInt(dataFiles.indexOf(entries.get(member).dataFile()));
        out.writeLong(entries.get(member).rowPosition());
        for (float value : vectors[member]) {
          out.writeFloat(value);
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to encode an IVF list", e);
    }

    return bytes.toByteArray();
  }

  @Override
  public List<IndexEntry> entries(ByteBuffer payload) {
    throw new UnsupportedOperationException(
        "The " + NAME + " kernel writes auxiliary payloads, which carry no entries to decode");
  }

  @Override
  public PreparedSearch prepare(ByteBuffer payload) {
    // the snapshot is this searcher's own array: the caller may reuse or mutate what it passed
    return prepareOwned(VectorPayloads.toBytes(payload));
  }

  @Override
  public PreparedSearch prepareOwned(byte[] payload) {
    Preconditions.checkArgument(payload != null, "Index payload is null");
    Loaded loaded = load(payload);
    return new ListSearcher(
        loaded,
        dimension,
        scoring.metric(),
        recordStride(),
        isQuantized() ? quantizer.codeBytes() : 0,
        preparedBytes(loaded),
        null,
        null);
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
    if (listCount < 0 || dataFileCount < 0 || dimension <= 0 || dictionaryBytes < 0) {
      return 0L;
    }

    // Historical manifests include a quantizer byte total that charged the rotation per list.
    long quantizerBytes =
        isQuantized() ? RaBitQQuantizer.estimatedListBytes(dimension, listCount) : 0L;
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
    VectorPayloadHeader header =
        VectorPayloadHeader.of(ByteBuffer.wrap(in.payload(), in.position(), in.remaining()));
    Preconditions.checkArgument(
        header.readableBy(this),
        "IVF payload was written by %s; this build reads kernel %s version %s",
        header,
        NAME,
        payloadVersion());
    in.skip(Integer.BYTES, "the payload header version");
    if (!header.legacy()) {
      int nameLength = in.readUnsignedShort();
      in.skip(nameLength + (long) Integer.BYTES, "the payload header");
    }
  }

  private record Metadata(float[][] centroids, String[] files, int entries, long seed, int bits) {}

  private Metadata readMetadata(VectorPayloadReader in, boolean blocks) {
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
    long minimumPerList =
        blocks ? (long) dimension * Float.BYTES + DIRECTORY_ENTRY_BYTES : minimumBytesPerList();
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

    return new Metadata(centroids, dataFiles, entryCount, seed, bits);
  }

  private long recordStride() {
    return isQuantized()
        ? IvfLayout.quantizedStride(quantizer.codeBytes())
        : IvfLayout.plainStride(dimension);
  }

  private int listHeaderBytes() {
    return Integer.BYTES + (isQuantized() ? dimension * Float.BYTES : 0);
  }

  private record Directory(int[] starts, int[] counts) {}

  private Directory readDirectory(VectorPayloadReader in, Metadata metadata, int payloadSize) {
    int lists = metadata.centroids().length;
    int[] starts = new int[lists];
    int[] counts = new int[lists];
    long next = (long) in.position() + (long) lists * DIRECTORY_ENTRY_BYTES;
    long records = 0L;
    for (int list = 0; list < lists; list++) {
      int start = in.readInt();
      int count = in.readInt();
      Preconditions.checkArgument(
          count >= 0 && start == next, "Invalid IVF block directory at list %s", list);
      next =
          IvfLayout.recordEnd(
              (long) start + listHeaderBytes(), count, recordStride(), "IVF list " + list);
      Preconditions.checkArgument(
          next <= payloadSize, "IVF list %s extends outside the blob", list);
      starts[list] = start;
      counts[list] = count;
      records += count;
    }

    Preconditions.checkArgument(
        records == metadata.entries() && next == payloadSize,
        "IVF list directory does not match the entry count or blob length");
    return new Directory(starts, counts);
  }

  private static int readBlockPrefix(VectorPayloadReader in, int length) {
    Preconditions.checkArgument(in.readInt() == BLOCK_MAGIC, "Invalid IVF block marker");
    int version = in.readInt();
    Preconditions.checkArgument(
        version == BLOCK_VERSION, "Unsupported IVF block format: %s", version);
    int metadataEnd = in.readInt();
    Preconditions.checkArgument(
        metadataEnd > BLOCK_PREFIX_BYTES && metadataEnd <= length,
        "Invalid IVF metadata length: %s",
        metadataEnd);
    return metadataEnd;
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
    boolean blocks = isBlocked(ByteBuffer.wrap(bytes));
    int metadataEnd =
        blocks ? readBlockPrefix(new VectorPayloadReader(bytes), bytes.length) : bytes.length;
    VectorPayloadReader metaReader =
        new VectorPayloadReader(bytes, blocks ? BLOCK_PREFIX_BYTES : 0, metadataEnd);
    Metadata metadata = readMetadata(metaReader, blocks);
    Directory directory = blocks ? readDirectory(metaReader, metadata, bytes.length) : null;
    Preconditions.checkArgument(!blocks || metaReader.remaining() == 0, "Invalid IVF metadata end");
    VectorPayloadReader in = new VectorPayloadReader(bytes, metaReader.position());
    int listCount = metadata.centroids().length;
    String[] dataFiles = metadata.files();
    int entryCount = metadata.entries();
    long stride = recordStride();
    int[] listStarts = new int[listCount];
    int[] listCounts = new int[listCount];
    float[][] listCenters = isQuantized() ? new float[listCount][] : null;
    long records = 0;
    for (int list = 0; list < listCount; list++) {
      Preconditions.checkArgument(
          !blocks || in.position() == directory.starts()[list],
          "Invalid IVF list %s block offset",
          list);
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
      Preconditions.checkArgument(
          !blocks || count == directory.counts()[list], "Invalid IVF list %s block count", list);
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
        listQuantizers[list] =
            new RaBitQQuantizer(dimension, metadata.seed(), metadata.bits(), listCenters[list]);
      }
    }

    return new Loaded(
        bytes,
        listStarts,
        listCounts,
        metadata.centroids(),
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
   * <p>A quantized list also builds a {@link RaBitQQuantizer} that clones the centre. The
   * allocation guard conservatively includes room for the shared rotation in each list.
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
        "IVF list count %s needs %s bytes of structures, more than the %s an index payload may"
            + " size",
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

  static boolean isBlocked(ByteBuffer payload) {
    return payload.remaining() >= Integer.BYTES
        && payload.duplicate().getInt(payload.position()) == BLOCK_MAGIC;
  }

  /**
   * What opening the file already cost when the kernel is asked to read the description region.
   *
   * <p>The container footer and the length probe are outside the kernel: they locate the payload.
   * The description region is what this kernel reads first, and the lists are read while searching.
   */
  record OpenCosts(
      long opens,
      long lengthRequests,
      long footerBytes,
      long footerRequests,
      long descriptionBytes,
      long descriptionRequests) {

    OpenCosts withDescription(long bytes, long requests) {
      return new OpenCosts(opens, lengthRequests, footerBytes, footerRequests, bytes, requests);
    }
  }

  VectorIndexAdapter.RangeSearch prepareRange(IvfRangeInput input, OpenCosts costs) {
    return prepareRange(input, costs, null);
  }

  VectorIndexAdapter.RangeSearch prepareRange(
      IvfRangeInput input, OpenCosts costs, ListBlockCache blocks) {
    byte[] prefix = input.read(0, Math.min(input.length(), BLOCK_PREFIX_BYTES));
    if (!isBlocked(ByteBuffer.wrap(prefix))) {
      return null;
    }

    // the bytes already read as the prefix are part of the description, so the rest is appended
    // rather than the same range being read again
    int metadataEnd = readBlockPrefix(new VectorPayloadReader(prefix), input.length());
    VectorPayloadReader in =
        new VectorPayloadReader(description(input, prefix, metadataEnd), BLOCK_PREFIX_BYTES);
    Metadata metadata = readMetadata(in, true);
    Directory directory = readDirectory(in, metadata, input.length());
    Preconditions.checkArgument(in.remaining() == 0, "Invalid IVF metadata end");
    VectorIndexAdapter.RangeReadMetrics descriptionReads = input.metrics();
    Loaded loaded =
        new Loaded(
            null,
            directory.starts(),
            directory.counts(),
            metadata.centroids(),
            null,
            metadata.files(),
            null,
            metadata.entries());
    return new ListSearcher(
        loaded,
        dimension,
        scoring.metric(),
        recordStride(),
        isQuantized() ? quantizer.codeBytes() : 0,
        0L,
        new RangeLists(
            new IvfListRanges(input, directory.starts(), IvfListRanges.MAX_MERGE_GAP_BYTES, blocks),
            metadata.seed(),
            metadata.bits()),
        costs.withDescription(descriptionReads.bytes(), descriptionReads.requests()));
  }

  private static byte[] description(IvfRangeInput input, byte[] prefix, int metadataEnd) {
    // readBlockPrefix has already rejected a description that is not longer than the prefix, so
    // the rest of the description is always read rather than re-reading the prefix
    byte[] description = Arrays.copyOf(prefix, metadataEnd);
    input.read(prefix.length, description, prefix.length, metadataEnd - prefix.length);
    return description;
  }

  private static int queryNprobe(ByteBuffer query, int dimension) {
    long vectorBytes = 2L * Integer.BYTES + (long) dimension * Float.BYTES;
    if (query.remaining() == vectorBytes) {
      return IvfOptions.DEFAULT_NPROBE;
    }

    Preconditions.checkArgument(
        query.remaining() == vectorBytes + Integer.BYTES, "Invalid IVF query length");
    int nprobe = query.duplicate().getInt(query.limit() - Integer.BYTES);
    Preconditions.checkArgument(nprobe > 0, "Invalid IVF nprobe: %s", nprobe);
    return nprobe;
  }

  private record RangeLists(IvfListRanges lists, long seed, int bits) {}

  /** A prepared search that reads records where they lie and keeps a bounded top-k. */
  private static final class ListSearcher implements VectorIndexAdapter.RangeSearch {
    private final Loaded loaded;
    private final int dimension;
    private final VectorScoring scoring;
    private final long bytes;
    private final long stride;
    private final int codeBytes;
    private final RangeLists ranges;
    private final OpenCosts costs;

    private ListSearcher(
        Loaded loaded,
        int dimension,
        VectorIndexDefinition.Metric metric,
        long stride,
        int codeBytes,
        long bytes,
        RangeLists ranges,
        OpenCosts costs) {
      this.loaded = loaded;
      this.dimension = dimension;
      this.scoring = new VectorScoring(metric);
      this.stride = stride;
      this.codeBytes = codeBytes;
      this.bytes = bytes;
      this.ranges = ranges;
      this.costs = costs;
    }

    @Override
    public void close() {
      if (ranges != null) {
        ranges.lists().close();
      }
    }

    @Override
    public VectorIndexAdapter.RangeReadMetrics listReadMetrics() {
      return ranges == null
          ? VectorIndexAdapter.RangeSearch.super.listReadMetrics()
          : ranges.lists().metrics();
    }

    @Override
    public long wholeListAreaReads() {
      return ranges == null ? 0L : ranges.lists().wholeListAreaReads();
    }

    @Override
    public long listBlocksRead() {
      return ranges == null ? 0L : ranges.lists().listsRead();
    }

    @Override
    public ListBlockCache.Stats blockCacheStats() {
      return ranges == null ? ListBlockCache.Stats.EMPTY : ranges.lists().blockStats();
    }

    @Override
    public VectorIndexAdapter.OpenReadMetrics openReadMetrics() {
      if (costs == null) {
        return null;
      }

      VectorIndexAdapter.RangeReadMetrics lists =
          ranges == null
              ? VectorIndexAdapter.RangeSearch.super.listReadMetrics()
              : ranges.lists().metrics();
      return new VectorIndexAdapter.OpenReadMetrics(
          costs.opens(),
          costs.lengthRequests(),
          costs.footerBytes(),
          costs.footerRequests(),
          costs.descriptionBytes(),
          costs.descriptionRequests(),
          lists.bytes(),
          lists.requests());
    }

    private VectorPayloadReader readList(int list, boolean quantized) {
      VectorPayloadReader block = ranges.lists().read(list);
      long countOffset = block.position() + (quantized ? (long) dimension * Float.BYTES : 0L);
      Preconditions.checkArgument(
          block.intAt(countOffset) == loaded.listCounts[list],
          "Invalid IVF list %s block count",
          list);
      long offset = countOffset + Integer.BYTES;
      for (int row = 0; row < loaded.listCounts[list]; row++, offset += stride) {
        int fileID = block.intAt(offset);
        Preconditions.checkArgument(
            fileID >= 0 && fileID < loaded.dataFiles.length,
            "Invalid IVF data file ID %s in list %s",
            fileID,
            list);
      }

      return block;
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

      int nprobe = queryNprobe(query, dimension);
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
      if (ranges != null) {
        ranges.lists().planInReadOrder(lists, null);
      }

      VectorPayloadReader reader = ranges == null ? new VectorPayloadReader(loaded.payload) : null;
      if (codeBytes == 0) {
        scorePlainLists(queryVector, lists, reader, top, null, null);
      } else {
        scoreQuantizedLists(queryVector, lists, reader, top, null, null);
      }

      return top.results();
    }

    @Override
    public boolean joinsCandidateSet() {
      return true;
    }

    @Override
    public List<IndexSearchResult> search(ByteBuffer query, int topK, IndexCandidates candidates) {
      Preconditions.checkArgument(topK >= 0, "Top-k must be non-negative: %s", topK);
      Preconditions.checkArgument(candidates != null, "Candidate set is null");
      if (topK == 0
          || loaded.entryCount == 0
          || loaded.centroids.length == 0
          || candidates.isEmpty()) {
        return ImmutableList.of();
      }

      int nprobe = queryNprobe(query, dimension);
      float[] queryVector =
          VectorPayloads.decodeVector(VectorPayloads.toBytes(query), dimension, "Query");
      IndexCandidates.LongSet[] admitted = admittedRows(candidates);
      if (admitted == null) {
        return ImmutableList.of();
      }

      VectorTopK top =
          new VectorTopK(topK, candidates.count(), scoring, index -> loaded.dataFiles[index]);
      VectorPayloadReader reader = ranges == null ? new VectorPayloadReader(loaded.payload) : null;
      // a filter spreads the rows worth scoring over the whole index in proportion to itself, not
      // in proportion to the query, so the lists nearest the query can hold fewer of them than the
      // window asks for. The search then widens to the next lists instead of returning short, and
      // stops widening once it has visited every list there is.
      boolean[] scored = new boolean[loaded.centroids.length];
      int probed = Math.max(1, Math.min(nprobe, loaded.centroids.length));
      // the order the query visits the lists in is the same whatever number it visits, so a
      // widening round takes the next prefix of one order rather than scoring every centroid again
      int[] nearest =
          nearestCentroids(queryVector, loaded.centroids, loaded.centroids.length, scoring);
      while (true) {
        int[] lists = Arrays.copyOf(nearest, probed);
        if (ranges != null) {
          ranges.lists().planInReadOrder(lists, scored);
        }

        if (codeBytes == 0) {
          scorePlainLists(queryVector, lists, reader, top, admitted, scored);
        } else {
          scoreQuantizedLists(queryVector, lists, reader, top, admitted, scored);
        }

        if (top.size() >= topK || probed == loaded.centroids.length) {
          return top.results();
        }

        probed = (int) Math.min(loaded.centroids.length, (long) probed * 2);
      }
    }

    /**
     * Turns the candidate set into one lookup per data file index, so that scoring a record costs
     * an array read rather than a hash of the file its name identifies.
     *
     * @return the lookups, or null when the set names no file this searcher holds
     */
    private IndexCandidates.LongSet[] admittedRows(IndexCandidates candidates) {
      IndexCandidates.LongSet[] admitted = new IndexCandidates.LongSet[loaded.dataFiles.length];
      boolean any = false;
      for (int index = 0; index < admitted.length; index++) {
        admitted[index] = candidates.forFile(loaded.dataFiles[index]);
        any |= admitted[index] != null;
      }

      return any ? admitted : null;
    }

    /** Whether a record may be scored, which is always true when no filter was given. */
    private static boolean admits(
        IndexCandidates.LongSet[] admitted, int dataFileIndex, long rowPosition) {
      if (admitted == null) {
        return true;
      }

      IndexCandidates.LongSet set = admitted[dataFileIndex];
      return set != null && set.contains(rowPosition);
    }

    /**
     * Scores the records of unquantized lists, reading each vector into one search-private array.
     *
     * <p>A record a candidate set turns down is skipped before its vector is read: reading it is
     * the cost the filter is there to avoid.
     */
    private void scorePlainLists(
        float[] queryVector,
        int[] lists,
        VectorPayloadReader reader,
        VectorTopK top,
        IndexCandidates.LongSet[] admitted,
        boolean[] scored) {
      float[] vector = new float[dimension];
      VectorPayloadReader whole = reader;
      for (int list : lists) {
        if (scored != null) {
          if (scored[list]) {
            continue;
          }

          scored[list] = true;
        }

        int count = loaded.listCounts[list];
        reader = ranges == null ? whole : readList(list, false);
        long offset =
            ranges == null ? loaded.listStarts[list] : (long) reader.position() + Integer.BYTES;
        for (int record = 0; record < count; record++, offset += stride) {
          int dataFileIndex = reader.intAt(offset);
          long rowPosition = reader.longAt(offset + Integer.BYTES);
          if (!admits(admitted, dataFileIndex, rowPosition)) {
            continue;
          }

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
        float[] queryVector,
        int[] lists,
        VectorPayloadReader reader,
        VectorTopK top,
        IndexCandidates.LongSet[] admitted,
        boolean[] scored) {
      float[] factors = new float[RaBitQQuantizer.FACTORS];
      VectorPayloadReader whole = reader;
      for (int list : lists) {
        if (scored != null) {
          if (scored[list]) {
            continue;
          }

          scored[list] = true;
        }

        float[] center;
        RaBitQQuantizer listQuantizer;
        reader = ranges == null ? whole : readList(list, true);
        if (ranges == null) {
          center = loaded.listCenters[list];
          listQuantizer = loaded.listQuantizers[list];
        } else {
          center = new float[dimension];
          for (int d = 0; d < dimension; d++) {
            center[d] = reader.readFloat();
          }

          reader.readInt();
          listQuantizer = new RaBitQQuantizer(dimension, ranges.seed(), ranges.bits(), center);
        }

        // the query's terms of the estimate depend on the list but not on the candidate, so they
        // are derived once here rather than on every call that scores a code
        RaBitQQuantizer.ListQuery preparedQuery = listQuantizer.prepareQuery(queryVector);

        int count = loaded.listCounts[list];
        long offset = ranges == null ? loaded.listStarts[list] : reader.position();
        byte[] listPayload = reader.payload();
        for (int record = 0; record < count; record++, offset += stride) {
          int dataFileIndex = reader.intAt(offset);
          long rowPosition = reader.longAt(offset + Integer.BYTES);
          if (!admits(admitted, dataFileIndex, rowPosition)) {
            continue;
          }

          long codeBase = offset + Integer.BYTES + Long.BYTES;
          long factorsBase = codeBase + codeBytes;
          for (int f = 0; f < factors.length; f++) {
            factors[f] = reader.floatAt(factorsBase + (long) Float.BYTES * f);
          }

          double score =
              switch (scoring.metric()) {
                case L2 ->
                    Math.sqrt(
                        listQuantizer.estimatedDistanceSquared(
                            listPayload, codeBase, factors, preparedQuery));
                case DOT ->
                    listQuantizer.estimatedInnerProduct(
                        listPayload, codeBase, factors, preparedQuery);
                case COSINE -> {
                  double inner =
                      listQuantizer.estimatedInnerProduct(
                          listPayload, codeBase, factors, preparedQuery);
                  double vectorSquared =
                      RaBitQQuantizer.squaredLength(factors, preparedQuery.centerSquared());
                  yield vectorSquared <= 0.0 || preparedQuery.querySquared() <= 0.0
                      ? 0.0
                      : inner
                          / (Math.sqrt(vectorSquared) * Math.sqrt(preparedQuery.querySquared()));
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
  private Trained train(float[][] vectors, int listCount, int threads) {
    int sampleCount = options.samplesFor(vectors.length, listCount);
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

    int maxIterations = options.trainMaxIterations();
    double tolerance = options.trainConvergenceTolerance();
    int iterations = 0;
    for (int iteration = 0; iteration < maxIterations; iteration++) {
      iterations = iteration + 1;
      // the training assignment stays exact: k-means is where the centroids come from, and a
      // float32 assignment there would move every list rather than the rows near a tie. The
      // option accelerates the pass that places the vectors of the file, which is the measured
      // one, and a build that sets it still trains the centres the exact kernel trains.
      int[] assignment = nearestCentroid(sample, centroids, threads, false).assignment;
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
      double displacement = 0.0;
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

        float[] previous = centroids[list];
        centroids[list] = mean;
        if (Arrays.equals(mean, previous)) {
          continue;
        }

        changed = true;
        if (tolerance > 0.0) {
          displacement = Math.max(displacement, relativeDisplacement(mean, previous));
        }
      }

      if (!changed) {
        break;
      }

      // a positive tolerance stops training while the centroids are still settling, once a round
      // moves them by less than the fraction of their own length that was asked for. Zero keeps the
      // bitwise criterion above, so the default path runs the same rounds it always did.
      if (tolerance > 0.0 && displacement < tolerance) {
        break;
      }
    }

    return new Trained(centroids, sampleCount, iterations);
  }

  /**
   * How far a centroid moved relative to where it was: {@code ‖new - old‖ / (‖old‖ + ε)}.
   *
   * <p>The sum is accumulated in double because near convergence the difference of two floats is
   * small and would lose its significant bits if it were squared in float. The epsilon bounds the
   * fraction for a centroid at the origin instead of dividing by zero.
   */
  private static double relativeDisplacement(float[] updated, float[] previous) {
    double moved = 0.0;
    double before = 0.0;
    for (int d = 0; d < updated.length; d++) {
      double delta = (double) updated[d] - previous[d];
      moved += delta * delta;
      before += (double) previous[d] * previous[d];
    }

    return Math.sqrt(moved) / (Math.sqrt(before) + DISPLACEMENT_EPSILON);
  }

  /**
   * The centroids one build trained, and how much it spent: the sample count and the rounds that
   * actually ran. A build that stops early reports fewer rounds than the cap.
   */
  private static final class Trained {
    private final float[][] centroids;
    private final int samples;
    private final int iterations;

    private Trained(float[][] centroids, int samples, int iterations) {
      this.centroids = centroids;
      this.samples = samples;
      this.iterations = iterations;
    }

    private static Trained none() {
      return new Trained(new float[0][], 0, 0);
    }
  }

  private static long millis(long nanos) {
    return nanos / 1_000_000L;
  }

  /**
   * Puts every vector under its nearest centroid, dropping the lists nothing landed in.
   *
   * <p>A dropped list is a centroid no vector is closest to; keeping it would only add a partition
   * that a search might visit and find empty.
   */
  private Assigned assign(float[][] vectors, float[][] centroids, int threads) {
    if (centroids.length == 0) {
      return new Assigned(new int[0][], false);
    }

    Nearest nearest = nearestCentroid(vectors, centroids, threads, options.simdAssignKernel());
    int[] assignment = nearest.assignment;
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

    return new Assigned(members, nearest.simd);
  }

  /** The lists one build assigned, and whether the vectorized kernel produced them. */
  private static final class Assigned {
    private final int[][] members;
    private final boolean simd;

    private Assigned(int[][] members, boolean simd) {
      this.members = members;
      this.simd = simd;
    }
  }

  /** The list each vector chose, and whether the vectorized kernel chose them. */
  private static final class Nearest {
    private final int[] assignment;
    private final boolean simd;

    private Nearest(int[] assignment, boolean simd) {
      this.assignment = assignment;
      this.simd = simd;
    }
  }

  /**
   * Puts every vector under its nearest centroid.
   *
   * <p>Each row is decided on its own, so the rows are split into contiguous ranges and every range
   * writes only its own slots: the assignment is the one a single thread would have produced, in
   * the same order and to the same list.
   */
  private Nearest nearestCentroid(
      float[][] vectors, float[][] centroids, int threads, boolean simdKernel) {
    if (scoring.metric() == VectorIndexDefinition.Metric.L2) {
      if (simdKernel) {
        int[] vectorized = nearestCentroidL2Simd(vectors, centroids, threads);
        if (vectorized != null) {
          return new Nearest(vectorized, true);
        }
      }

      return new Nearest(nearestCentroidL2(vectors, centroids, threads), false);
    }

    // a cosine or dot-product assignment keeps walking one vector at a time: the blocked kernel
    // is written for the L2 sum, and those two metrics are not what the payloads in use are built
    // by, so their path stays the one their bytes were built by
    int[] assignment = new int[vectors.length];
    runInParallel(
        threads,
        vectors.length,
        range -> {
          for (int i = range[0]; i < range[1]; i++) {
            assignment[i] = nearestCentroidOf(vectors[i], centroids);
          }
        });
    return new Nearest(assignment, false);
  }

  /**
   * Puts every vector under its nearest centroid by L2 distance.
   *
   * <p>The rows are walked in blocks: the block is converted to double once, then each centroid row
   * is loaded once and compared with every row of the block, two rows at a time so both share the
   * centroid's loads. A row still decides its list on its own, in list order and with the same
   * tie-break: when two centroids are equally far away, the earlier list keeps the row.
   * Package-private so the block order and the four accumulators can be compared with the
   * sequential kernel they replaced.
   */
  static int[] nearestCentroidL2(float[][] vectors, float[][] centroids, int threads) {
    int[] assignment = new int[vectors.length];
    if (vectors.length == 0) {
      return assignment;
    }

    int dimension = centroids[0].length;
    runInParallel(
        threads,
        vectors.length,
        range -> assignRange(vectors, centroids, dimension, range, assignment));
    return assignment;
  }

  /**
   * Puts every vector under its nearest centroid with the vector API kernel.
   *
   * <p>The kernel is optional: it is compiled apart from the rest of the structure because the
   * module it needs is not in the JDK's default module graph. This returns {@code null} when the
   * kernel cannot be loaded, which leaves the caller to run the exact kernel rather than fail the
   * build.
   *
   * <p>Package-private so the assignment it produces can be compared with the exact kernel on the
   * same vectors and centroids.
   */
  static int[] nearestCentroidL2Simd(float[][] vectors, float[][] centroids, int threads) {
    return nearestCentroidL2Simd(vectors, centroids, threads, SimdAssignHolder.kernel());
  }

  /**
   * The same assignment with the kernel given, or {@code null} for none: a caller that has to prove
   * the fallback path uses it without depending on whether the JVM opened the incubator module.
   */
  static int[] nearestCentroidL2Simd(
      float[][] vectors, float[][] centroids, int threads, AssignKernel kernel) {
    if (kernel == null) {
      return null;
    }

    int[] assignment = new int[vectors.length];
    if (vectors.length == 0 || centroids.length == 0) {
      return assignment;
    }

    int dimension = centroids[0].length;
    runInParallel(
        threads,
        vectors.length,
        range -> kernel.assignRange(vectors, centroids, dimension, range, assignment));
    return assignment;
  }

  /**
   * Loads the optional vector API kernel on first use, and never retries.
   *
   * <p>The class is loaded reflectively because the main source set cannot reference the incubator
   * module: a JVM that was not started with {@code --add-modules jdk.incubator.vector} still builds
   * every structure, and only a build that asks for this kernel pays for finding out. The probe
   * call runs the kernel on the calling thread, so a module that is missing fails here rather than
   * inside a worker, and the build then falls back to the exact kernel.
   */
  private static final class SimdAssignHolder {
    private static final AssignKernel KERNEL = loadKernel(IvfKernel.class.getClassLoader());

    private static AssignKernel kernel() {
      return KERNEL;
    }
  }

  /**
   * Loads the optional kernel with the given class loader, or returns {@code null} when it cannot
   * be loaded or run. The probe runs here, on the calling thread, so a missing incubator module
   * fails before a worker thread ever reaches the kernel.
   */
  static AssignKernel loadKernel(ClassLoader loader) {
    try {
      Class<?> type = Class.forName(SIMD_ASSIGN_KERNEL_CLASS, true, loader);
      AssignKernel kernel = (AssignKernel) type.getDeclaredConstructor().newInstance();
      // running it once links the classes of the incubator module here, where a failure is
      // caught, instead of leaving it to the first worker that calls the kernel; the probe is a
      // full block of rows against two centroids, so both kernel paths run
      float[][] vectors = {new float[16], new float[16], new float[16], new float[16]};
      float[][] centroids = {new float[16], new float[16]};
      kernel.assignRange(vectors, centroids, 16, new int[] {0, 4}, new int[4]);
      return kernel;
    } catch (ReflectiveOperationException | LinkageError e) {
      LOG.warn(
          "Cannot run the vector API assignment kernel, so builds that ask for it fall back to"
              + " the exact kernel. Start the JVM with --add-modules jdk.incubator.vector to use"
              + " it.",
          e);
      return null;
    }
  }

  /** Assigns the rows of one range of vectors, one block at a time. */
  private static void assignRange(
      float[][] vectors, float[][] centroids, int dimension, int[] range, int[] assignment) {
    int blockCount = Math.min(ASSIGN_VECTOR_BLOCK, range[1] - range[0]);
    // the scratch of one range: the block as doubles, one centroid row, and the per-row results
    double[] rows = new double[blockCount * dimension];
    double[] centroidRow = new double[dimension];
    double[] best = new double[blockCount];
    int[] nearest = new int[blockCount];
    double[] distances = new double[2];

    for (int begin = range[0]; begin < range[1]; begin += ASSIGN_VECTOR_BLOCK) {
      int end = Math.min(begin + ASSIGN_VECTOR_BLOCK, range[1]);
      int block = end - begin;
      for (int b = 0; b < block; b++) {
        float[] row = vectors[begin + b];
        int base = b * dimension;
        for (int d = 0; d < dimension; d++) {
          // a float converts to a double exactly, so this is the arithmetic the sequential kernel
          // did, only out of a layout the inner loop can read without converting every dimension
          rows[base + d] = row[d];
        }
      }

      copyRow(centroids[0], centroidRow);
      for (int b = 0; b < block; b++) {
        best[b] = l2Distance(rows, b * dimension, centroidRow, dimension);
        nearest[b] = 0;
      }

      for (int list = 1; list < centroids.length; list++) {
        copyRow(centroids[list], centroidRow);
        int b = 0;
        for (; b + 1 < block; b += 2) {
          l2DistancePair(
              rows, b * dimension, (b + 1) * dimension, centroidRow, dimension, distances);
          keepNearer(distances[0], list, b, best, nearest);
          keepNearer(distances[1], list, b + 1, best, nearest);
        }

        for (; b < block; b++) {
          keepNearer(
              l2Distance(rows, b * dimension, centroidRow, dimension), list, b, best, nearest);
        }
      }

      for (int b = 0; b < block; b++) {
        assignment[begin + b] = nearest[b];
      }
    }
  }

  /** Keeps the list of a strictly closer centroid; an equal distance keeps the earlier list. */
  private static void keepNearer(double distance, int list, int b, double[] best, int[] nearest) {
    if (Double.compare(distance, best[b]) < 0) {
      best[b] = distance;
      nearest[b] = list;
    }
  }

  /** Copies a float row into the double row the inner loops read. */
  private static void copyRow(float[] source, double[] target) {
    for (int d = 0; d < target.length; d++) {
      target[d] = source[d];
    }
  }

  /**
   * The L2 distance of one row from a centroid.
   *
   * <p>The sum is accumulated in the order the sequential kernel accumulated it, one dimension at a
   * time. Splitting it into several accumulators shortens the dependency chain, but it also rounds
   * the total differently, and a total that differs in its last bits can put a near-tied row under
   * another centroid: the payload of a rebuild would then not be the one the previous build wrote.
   * The block layout is what makes this loop fast without changing the sum.
   */
  private static double l2Distance(double[] rows, int base, double[] centroid, int dimension) {
    double squared = 0.0;
    for (int d = 0; d < dimension; d++) {
      double difference = rows[base + d] - centroid[d];
      squared += difference * difference;
    }

    return Math.sqrt(squared);
  }

  /**
   * The L2 distance of two rows from one centroid, loading each centroid value once for both.
   *
   * <p>Each row accumulates in dimension order, exactly as {@link #l2Distance} does; sharing the
   * loads changes nothing about either sum, and the two rows give the loop two independent chains.
   */
  private static void l2DistancePair(
      double[] rows,
      int firstBase,
      int secondBase,
      double[] centroid,
      int dimension,
      double[] distances) {
    double firstSquared = 0.0;
    double secondSquared = 0.0;
    for (int d = 0; d < dimension; d++) {
      double centroidValue = centroid[d];
      double firstDifference = rows[firstBase + d] - centroidValue;
      firstSquared += firstDifference * firstDifference;
      double secondDifference = rows[secondBase + d] - centroidValue;
      secondSquared += secondDifference * secondDifference;
    }

    distances[0] = Math.sqrt(firstSquared);
    distances[1] = Math.sqrt(secondSquared);
  }

  /** The nearest centroid of one vector, breaking ties towards the earlier list. */
  private int nearestCentroidOf(float[] vector, float[][] centroids) {
    int best = 0;
    double bestScore = scoring.score(vector, centroids[0]);
    for (int list = 1; list < centroids.length; list++) {
      double score = scoring.score(vector, centroids[list]);
      if (scoring.compareScores(score, bestScore) < 0) {
        best = list;
        bestScore = score;
      }
    }

    return best;
  }

  /**
   * Calls {@code body} once per range, on at most {@code threads} threads.
   *
   * <p>Ranges are contiguous and disjoint, so a body that writes only its own slots produces the
   * same arrays as the serial loop it replaces. One thread, or nothing to split, keeps the calls on
   * the calling thread rather than paying for a pool that would hand back the work immediately.
   */
  private static void runInParallel(int threads, int count, Consumer<int[]> body) {
    if (threads <= 1 || count <= 1) {
      body.accept(new int[] {0, count});
      return;
    }

    int parts = Math.min(threads, count);
    ExecutorService pool = Executors.newFixedThreadPool(parts);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    CountDownLatch finished = new CountDownLatch(parts);
    try {
      for (int part = 0; part < parts; part++) {
        int start = (int) ((long) part * count / parts);
        int end = (int) ((long) (part + 1) * count / parts);
        int[] range = new int[] {start, end};
        pool.execute(
            () -> {
              try {
                body.accept(range);
              } catch (Throwable t) {
                // an Error in a worker would otherwise end that thread with nothing recorded, and
                // the build would continue on whatever slots the worker did not fill
                failure.compareAndSet(null, t);
              } finally {
                finished.countDown();
              }
            });
      }

      finished.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while building the IVF index", e);
    } finally {
      pool.shutdownNow();
    }

    Throwable failed = failure.get();
    if (failed instanceof RuntimeException runtimeException) {
      throw runtimeException;
    }

    if (failed instanceof Error error) {
      throw error;
    }
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
          .metric(VectorIndexDefinition.Metric.DOT)
          .metric(VectorIndexDefinition.Metric.L2)
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
          .metric(VectorIndexDefinition.Metric.DOT)
          .metric(VectorIndexDefinition.Metric.L2)
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
