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
import org.apache.iceberg.index.IndexAdapter.PreparedSearch;
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
public final class IvfKernel implements VectorKernel {

  private static final long serialVersionUID = 1L;

  public static final String NAME = "ivf";

  private static final int FORMAT_VERSION = 1;

  /** The payload written when the vectors are quantized. */
  private static final int QUANTIZED_FORMAT_VERSION = 2;

  private static final int MAX_ITERATIONS = 25;

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
    Preconditions.checkNotNull(entries, "Index entries are null");
    List<IndexEntry> materialized = Lists.newArrayList(entries);
    int count = materialized.size();

    List<String> dataFiles = VectorPayloads.dataFilesOf(materialized);
    float[][] vectors = decodeAll(materialized);

    int listCount = options.nlistFor(count);
    float[][] centroids = listCount == 0 ? new float[0][] : train(vectors, listCount);
    int[][] assignments = assign(vectors, centroids);

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
          writeQuantizedList(out, dataFiles, materialized, vectors, members);
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

    return ByteBuffer.wrap(payload.toByteArray());
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
  private void writeQuantizedList(
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
  }

  @Override
  public List<IndexEntry> entries(ByteBuffer payload) {
    throw new UnsupportedOperationException(
        "The " + NAME + " kernel writes auxiliary payloads, which carry no entries to decode");
  }

  @Override
  public PreparedSearch prepare(ByteBuffer payload) {
    Loaded loaded = load(VectorPayloads.toBytes(payload));
    return new ListSearcher(loaded);
  }

  /**
   * What the decoded lists cost: every entry with the vector or the code it was stored as, the
   * centroids the search compares the query against, and the per-list centres a quantized index
   * keeps to score with.
   *
   * <p>A quantized list is the case a single factor gets most wrong: its entries hold a handful of
   * bytes each where an unquantized entry holds a whole vector.
   */
  private long preparedBytes(Loaded loaded) {
    long total =
        PreparedStructureSize.object(4L * PreparedStructureSize.REFERENCE_BYTES)
            + PreparedStructureSize.object(2L * PreparedStructureSize.REFERENCE_BYTES + Long.BYTES)
            + PreparedStructureSize.references(loaded.centroids.length)
            + PreparedStructureSize.nestedList(loaded.lists.size(), 0);
    for (float[] centroid : loaded.centroids) {
      total += PreparedStructureSize.floats(centroid.length);
    }

    // the payload carries a dictionary of data files and every entry points at the one decoded
    // string for its file, so the strings are counted once for the payload rather than once per
    // entry: a long path is one kilobyte, not one kilobyte per vector
    IdentityHashMap<String, Boolean> dataFiles = new IdentityHashMap<>();
    for (List<IvfEntry> members : loaded.lists) {
      for (IvfEntry entry : members) {
        if (dataFiles.put(entry.dataFile, Boolean.TRUE) == null) {
          total += PreparedStructureSize.string(entry.dataFile);
        }
      }
    }

    long entryBytes = entryBytes();
    for (List<IvfEntry> members : loaded.lists) {
      total += PreparedStructureSize.nestedList(members.size(), entryBytes);
    }

    if (loaded.listCenters != null) {
      total += PreparedStructureSize.references(loaded.listCenters.length);
      for (float[] center : loaded.listCenters) {
        if (center != null) {
          total += PreparedStructureSize.array((long) center.length * Float.BYTES);
        }
      }
    }

    if (loaded.listQuantizers != null) {
      total += PreparedStructureSize.references(loaded.listQuantizers.length);
      for (RaBitQQuantizer listQuantizer : loaded.listQuantizers) {
        if (listQuantizer != null) {
          total += listQuantizer.estimatedBytes();
        }
      }
    }

    return total;
  }

  /** Object fields and payload retained by an entry; its list slot is counted separately. */
  private long entryBytes() {
    long payload =
        isQuantized()
            ? PreparedStructureSize.bytes(quantizer.codeBytes())
                + PreparedStructureSize.floats(RaBitQQuantizer.FACTORS)
            : PreparedStructureSize.floats(dimension);
    return PreparedStructureSize.entry(
        Long.BYTES + 4L * PreparedStructureSize.REFERENCE_BYTES, payload);
  }

  /** A prepared search over the decoded lists, which reports what it holds. */
  private final class ListSearcher implements PreparedSearch {
    private final Loaded loaded;
    private final long bytes;

    private ListSearcher(Loaded loaded) {
      this.loaded = loaded;
      this.bytes = preparedBytes(loaded);
    }

    @Override
    public long estimatedBytes() {
      return bytes;
    }

    @Override
    public List<IndexSearchResult> search(ByteBuffer query, int topK) {
      Preconditions.checkArgument(topK >= 0, "Top-k must be non-negative: %s", topK);
      if (topK == 0 || loaded.lists.isEmpty()) {
        return ImmutableList.of();
      }

      float[] queryVector =
          VectorPayloads.decodeVector(VectorPayloads.toBytes(query), dimension, "Query");
      // the partitions worth visiting, chosen by the same measure the vectors were partitioned by
      int visited = Math.min(options.nprobe(), loaded.centroids.length);
      List<IndexSearchResult> results = Lists.newArrayList();
      for (int list : nearestCentroids(queryVector, loaded.centroids, visited)) {
        if (isQuantized()) {
          scoreQuantizedList(queryVector, loaded, list, results);
        } else {
          for (IvfEntry entry : loaded.lists.get(list)) {
            results.add(
                IndexSearchResult.of(
                    entry.dataFile, entry.rowPosition, scoring.score(queryVector, entry.vector)));
          }
        }
      }

      results.sort(scoring.comparator());
      return results.size() > topK
          ? ImmutableList.copyOf(results.subList(0, topK))
          : ImmutableList.copyOf(results);
    }
  }

  /** Reads the centroids and the lists of a payload, once per payload that is searched. */
  private Loaded load(byte[] bytes) {
    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
      VectorPayloads.readHeader(in, NAME, payloadVersion(), "IVF");
      int dims = in.readInt();
      Preconditions.checkArgument(
          dims == dimension, "IVF payload has %s dimensions, the index has %s", dims, dimension);
      // read conditionally rather than by passing a dummy, and checked with a plain if because
      // Preconditions evaluates its message arguments before it decides whether to throw
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
      float[][] centroids = new float[listCount][dimension];
      for (int list = 0; list < listCount; list++) {
        for (int d = 0; d < dimension; d++) {
          centroids[list][d] = in.readFloat();
        }
      }

      String[] dataFiles = readDataFiles(in);
      int entryCount = in.readInt();
      Preconditions.checkArgument(entryCount >= 0, "Invalid IVF entry count: %s", entryCount);
      List<List<IvfEntry>> lists = Lists.newArrayListWithCapacity(listCount);
      float[][] listCenters = new float[listCount][];
      for (int list = 0; list < listCount; list++) {
        if (isQuantized()) {
          float[] listCenter = new float[dimension];
          for (int d = 0; d < dimension; d++) {
            listCenter[d] = in.readFloat();
          }
          listCenters[list] = listCenter;
        }
        lists.add(readList(in, dataFiles));
      }

      RaBitQQuantizer[] listQuantizers = null;
      if (isQuantized()) {
        listQuantizers = new RaBitQQuantizer[listCount];
        for (int list = 0; list < listCount; list++) {
          listQuantizers[list] =
              new RaBitQQuantizer(dimension, seed, quantizer.bits(), listCenters[list]);
        }
      }

      return new Loaded(centroids, lists, listCenters, listQuantizers);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to load the IVF index payload", e);
    }
  }

  private static String[] readDataFiles(DataInputStream in) throws IOException {
    int dataFileCount = in.readInt();
    Preconditions.checkArgument(
        dataFileCount >= 0, "Invalid IVF data file count: %s", dataFileCount);
    String[] dataFiles = new String[dataFileCount];
    for (int i = 0; i < dataFileCount; i++) {
      dataFiles[i] = in.readUTF();
    }
    return dataFiles;
  }

  private List<IvfEntry> readList(DataInputStream in, String[] dataFiles) throws IOException {
    int listSize = in.readInt();
    Preconditions.checkArgument(listSize >= 0, "Invalid IVF list size: %s", listSize);
    List<IvfEntry> members = Lists.newArrayListWithCapacity(listSize);
    byte[] code = isQuantized() ? new byte[quantizer.codeBytes()] : null;
    for (int i = 0; i < listSize; i++) {
      int dataFileIndex = in.readInt();
      Preconditions.checkArgument(
          dataFileIndex >= 0 && dataFileIndex < dataFiles.length,
          "Invalid IVF data file index: %s",
          dataFileIndex);
      long rowPosition = in.readLong();
      if (isQuantized()) {
        in.readFully(code);
        float[] factors = new float[RaBitQQuantizer.FACTORS];
        for (int f = 0; f < factors.length; f++) {
          factors[f] = in.readFloat();
        }
        members.add(new IvfEntry(dataFiles[dataFileIndex], rowPosition, code.clone(), factors));
      } else {
        float[] vector = new float[dimension];
        for (int d = 0; d < dimension; d++) {
          vector[d] = in.readFloat();
        }
        members.add(new IvfEntry(dataFiles[dataFileIndex], rowPosition, vector));
      }
    }
    return members;
  }

  /** A decoded payload: the centroids that partition it, and the vectors under each of them. */
  private static final class Loaded {
    private final float[][] centroids;
    private final List<List<IvfEntry>> lists;
    private final float[][] listCenters;
    private final RaBitQQuantizer[] listQuantizers;

    private Loaded(
        float[][] centroids,
        List<List<IvfEntry>> lists,
        float[][] listCenters,
        RaBitQQuantizer[] listQuantizers) {
      this.centroids = centroids;
      this.lists = lists;
      this.listCenters = listCenters;
      this.listQuantizers = listQuantizers;
    }
  }

  /**
   * Scores one list whose vectors are stored as codes.
   *
   * <p>Each list has its own centre, so the query is turned into a residual against it before it is
   * rotated; the estimate is then of the distance between two residuals, which is the distance
   * between the vectors themselves. The code is a candidate generator: it orders a wide set well
   * and the top of it badly, so what it returns is meant to be re-scored exactly.
   */
  private void scoreQuantizedList(
      float[] queryVector, Loaded loaded, int list, List<IndexSearchResult> results) {
    RaBitQQuantizer listQuantizer = loaded.listQuantizers[list];
    float[] rotatedQuery = listQuantizer.rotateQuery(queryVector);
    double queryLengthSquared = 0.0;
    float[] center = loaded.listCenters[list];
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

    for (IvfEntry entry : loaded.lists.get(list)) {
      double score;
      switch (scoring.metric()) {
        case EUCLIDEAN:
          score =
              Math.sqrt(
                  listQuantizer.estimatedDistanceSquared(
                      entry.code, 0, entry.factors, rotatedQuery, queryLengthSquared));
          break;
        case DOT_PRODUCT:
          score =
              listQuantizer.estimatedInnerProduct(
                  entry.code, 0, entry.factors, rotatedQuery, queryVector);
          break;
        case COSINE:
          double inner =
              listQuantizer.estimatedInnerProduct(
                  entry.code, 0, entry.factors, rotatedQuery, queryVector);
          double vectorSquared = RaBitQQuantizer.squaredLength(entry.factors, centerSquared);
          score =
              vectorSquared <= 0.0 || querySquared <= 0.0
                  ? 0.0
                  : inner / (Math.sqrt(vectorSquared) * Math.sqrt(querySquared));
          break;
        default:
          throw new IllegalStateException("Unknown vector index metric: " + scoring.metric());
      }
      results.add(IndexSearchResult.of(entry.dataFile, entry.rowPosition, score));
    }
  }

  /** The indexes of the {@code count} centroids closest to the query, best first. */
  private int[] nearestCentroids(float[] query, float[][] centroids, int count) {
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

  /** A vector in a list, with the row it came from. */
  private static final class IvfEntry {
    private final String dataFile;
    private final long rowPosition;

    /** The vector itself, or null when the entry holds a code. */
    private final float[] vector;

    /** The code of the vector, or null when the entry holds the vector. */
    private final byte[] code;

    /** The numbers stored with a code: length, correction, dot with the list's centre. */
    private final float[] factors;

    private IvfEntry(String dataFile, long rowPosition, float[] vector) {
      this(dataFile, rowPosition, vector, null, null);
    }

    private IvfEntry(String dataFile, long rowPosition, byte[] code, float[] factors) {
      this(dataFile, rowPosition, null, code, factors);
    }

    private IvfEntry(
        String dataFile, long rowPosition, float[] vector, byte[] code, float[] factors) {
      this.dataFile = dataFile;
      this.rowPosition = rowPosition;
      this.vector = vector;
      this.code = code;
      this.factors = factors;
    }
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
