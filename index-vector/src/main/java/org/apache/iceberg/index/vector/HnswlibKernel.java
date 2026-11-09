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

import com.github.jelmerk.knn.DistanceFunction;
import com.github.jelmerk.knn.Item;
import com.github.jelmerk.knn.ProgressListener;
import com.github.jelmerk.knn.SearchResult;
import com.github.jelmerk.knn.hnsw.HnswIndex;
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
 * A navigable small-world graph over the vectors.
 *
 * <p>The graph is built and searched by hnswlib-java, a pure Java library, so this kernel has no
 * native library to load. Building hands the library a thread pool when the caller has cores to
 * give: inserting one item at a time uses one core, and building the graph is the expensive part of
 * a build.
 *
 * <p>The graph ranks candidates by its own distance, so a hit is re-scored against the vector it
 * holds and the re-scored results are ordered before they are truncated. That is what lets an
 * approximate search report the score the exact scan would have given.
 *
 * <p>A prepared graph is measured, never derived from the counts a build records. Those counts say
 * how many nodes the graph holds and how many neighbours a node may keep, but the retained size of
 * a graph also depends on how many layers each node occupies and on how much neighbour capacity
 * every layer list was allocated, which only the loaded graph states. The kernel therefore does not
 * price a payload from a record.
 */
public final class HnswlibKernel implements VectorKernel {

  private static final long serialVersionUID = 1L;

  public static final String NAME = "hnswlib";

  private static final int FORMAT_VERSION = 1;

  /** The two length bytes every {@link DataInputStream#readUTF()} string carries at least. */
  private static final int MIN_UTF_BYTES = 2;

  private static final ProgressListener NO_PROGRESS = (workDone, max) -> {};

  /**
   * The progress interval, set where the library's remainder never reaches it.
   *
   * <p>The library takes the remainder of the completed count and this interval to decide when to
   * report progress, so passing zero is a division by zero, and a negative or absent listener is
   * called when it does come up.
   */
  private static final int NEVER_REPORT_PROGRESS = Integer.MAX_VALUE;

  private final int dimension;
  private final HnswOptions options;
  private final VectorScoring scoring;

  private HnswlibKernel(int dimension, HnswOptions options, VectorScoring scoring) {
    this.dimension = dimension;
    this.options = options;
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
    Preconditions.checkNotNull(entries, "Index entries are null");
    Preconditions.checkArgument(parallelism >= 1, "Parallelism must be at least one");
    List<IndexEntry> materialized = Lists.newArrayList(entries);
    HnswIndex<Long, float[], VectorItem, Float> index =
        HnswIndex.newBuilder(dimension, distanceFunction(), materialized.size())
            .withM(options.maxM())
            .withEfConstruction(options.efConstruction())
            .withEf(options.efSearch())
            .build();

    List<VectorItem> items = Lists.newArrayListWithCapacity(materialized.size());
    for (int i = 0; i < materialized.size(); i++) {
      items.add(
          new VectorItem(
              (long) i,
              VectorPayloads.decodeVector(materialized.get(i).payload(), dimension, "Entry")));
    }

    // one thread stays on the serial path, so a build that does not ask for threads produces the
    // graph it always did
    int threads = Math.max(1, Math.min(parallelism, items.size()));
    if (threads > 1) {
      try {
        index.addAll(items, threads, NO_PROGRESS, NEVER_REPORT_PROGRESS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Interrupted while building the HNSW index", e);
      }
    } else {
      for (VectorItem item : items) {
        index.add(item);
      }
    }

    ByteArrayOutputStream graph = new ByteArrayOutputStream();
    try {
      index.save(graph);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to serialize the HNSW index", e);
    }

    List<String> dataFiles = VectorPayloads.dataFilesOf(materialized);
    ByteArrayOutputStream payload = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(payload)) {
      VectorPayloads.writeHeader(out, NAME, FORMAT_VERSION);
      byte[] graphBytes = graph.toByteArray();
      out.writeInt(graphBytes.length);
      out.write(graphBytes);
      // the same dictionary the entries use: a graph node refers to its data file by position
      out.writeInt(dataFiles.size());
      for (String dataFile : dataFiles) {
        out.writeUTF(dataFile);
      }

      out.writeInt(materialized.size());
      for (IndexEntry entry : materialized) {
        out.writeInt(dataFiles.indexOf(entry.dataFile()));
        out.writeLong(entry.rowPosition());
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to encode the HNSW index payload", e);
    }
    int processors = Runtime.getRuntime().availableProcessors();
    long dictionaryBytes = VectorMemoryEstimates.dictionaryBytes(dataFiles);
    return VectorMemoryEstimates.result(
        this,
        ByteBuffer.wrap(payload.toByteArray()),
        preparedBytes(index, materialized.size(), dictionaryBytes, true),
        processors,
        Map.of(
            "mapping-count", (long) materialized.size(),
            "node-count", (long) index.size(),
            "dimension", (long) dimension,
            "max-m", (long) index.getM(),
            "max-count", (long) index.getMaxItemCount(),
            "data-file-count", (long) dataFiles.size(),
            "data-file-string-bytes", dictionaryBytes));
  }

  @Override
  public List<IndexEntry> entries(ByteBuffer payload) {
    // the graph payload holds the row a node points at, not the entry as it was: a kernel that
    // structures the vectors has no entry-bearing form
    throw new UnsupportedOperationException(
        "The " + NAME + " kernel writes auxiliary payloads, which carry no entries to decode");
  }

  @Override
  public PreparedSearch prepare(ByteBuffer payload) {
    byte[] bytes = VectorPayloads.toBytes(payload);
    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
      VectorPayloads.readHeader(in, NAME, FORMAT_VERSION, "HNSW");
      int graphLength = in.readInt();
      Preconditions.checkArgument(graphLength >= 0, "Invalid HNSW graph length: %s", graphLength);
      int graphRemaining = VectorPayloads.remaining(in);
      Preconditions.checkArgument(
          graphLength <= graphRemaining,
          "HNSW payload ends inside the graph: %s bytes remain, %s needed",
          graphRemaining,
          graphLength);
      byte[] graph = new byte[graphLength];
      in.readFully(graph);
      int dataFileCount = in.readInt();
      Preconditions.checkArgument(
          dataFileCount >= 0, "Invalid HNSW data file count: %s", dataFileCount);
      int dictionaryRemaining = VectorPayloads.remaining(in);
      long minimumDictionaryBytes = (long) dataFileCount * MIN_UTF_BYTES;
      Preconditions.checkArgument(
          minimumDictionaryBytes <= dictionaryRemaining,
          "HNSW payload ends inside the data file dictionary: %s bytes remain, %s needed",
          dictionaryRemaining,
          minimumDictionaryBytes);
      String[] dictionary = new String[dataFileCount];
      for (int i = 0; i < dataFileCount; i++) {
        dictionary[i] = in.readUTF();
      }

      int mappingCount = in.readInt();
      Preconditions.checkArgument(
          mappingCount >= 0, "Invalid HNSW mapping count: %s", mappingCount);
      int mappingRemaining = VectorPayloads.remaining(in);
      long bytesPerMapping = Integer.BYTES + Long.BYTES;
      long minimumMappingBytes = (long) mappingCount * bytesPerMapping;
      Preconditions.checkArgument(
          minimumMappingBytes <= mappingRemaining,
          "Invalid HNSW mapping count %s: the %s-byte payload ends before those mappings (each"
              + " mapping needs %s bytes, at most %s fit)",
          mappingCount,
          mappingRemaining,
          bytesPerMapping,
          mappingRemaining / bytesPerMapping);
      String[] dataFiles = new String[mappingCount];
      long[] positions = new long[mappingCount];
      for (int i = 0; i < mappingCount; i++) {
        int dataFileIndex = in.readInt();
        Preconditions.checkArgument(
            dataFileIndex >= 0 && dataFileIndex < dictionary.length,
            "Invalid HNSW data file index: %s",
            dataFileIndex);
        dataFiles[i] = dictionary[dataFileIndex];
        positions[i] = in.readLong();
      }

      // the load happens here, once per payload, and the returned searcher only traverses
      HnswIndex<Long, float[], VectorItem, Float> index =
          HnswIndex.load(new ByteArrayInputStream(graph));
      index.setEf(options.efSearch());
      long preparedBytes =
          preparedBytes(
              index,
              mappingCount,
              PreparedStructureSize.distinctStrings(Arrays.asList(dataFiles)),
              false);
      return new GraphSearcher(index, dataFiles, positions, mappingCount, preparedBytes);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to load the HNSW index payload", e);
    }
  }

  /** Counts the allocated graph, mapping and non-static searcher's kernel state. */
  private static long preparedBytes(
      HnswIndex<?, ?, ?, ?> index, int mappingCount, long dictionaryBytes, boolean building) {
    long graphBytes = HnswStructureSize.bytes(index, building);
    if (graphBytes == 0) {
      return 0;
    }
    return graphBytes
        + PreparedStructureSize.object(
            4L * PreparedStructureSize.REFERENCE_BYTES + Integer.BYTES + Long.BYTES)
        + PreparedStructureSize.object(Integer.BYTES + 2L * PreparedStructureSize.REFERENCE_BYTES)
        + PreparedStructureSize.object(3L * Integer.BYTES)
        + PreparedStructureSize.object(PreparedStructureSize.REFERENCE_BYTES)
        + PreparedStructureSize.references(mappingCount)
        + PreparedStructureSize.longs(mappingCount)
        + dictionaryBytes;
  }

  /** A loaded graph, which reports what it holds. */
  private final class GraphSearcher implements PreparedSearch {
    private final HnswIndex<Long, float[], VectorItem, Float> index;
    private final String[] dataFiles;
    private final long[] positions;
    private final int mappingCount;
    private final long bytes;

    private GraphSearcher(
        HnswIndex<Long, float[], VectorItem, Float> index,
        String[] dataFiles,
        long[] positions,
        int mappingCount,
        long bytes) {
      this.index = index;
      this.dataFiles = dataFiles;
      this.positions = positions;
      this.mappingCount = mappingCount;
      this.bytes = bytes;
    }

    @Override
    public long estimatedBytes() {
      return VectorMemoryEstimates.knownBytes(bytes);
    }

    @Override
    public List<IndexSearchResult> search(ByteBuffer query, int topK) {
      Preconditions.checkArgument(topK >= 0, "Top-k must be non-negative: %s", topK);
      if (topK == 0) {
        return ImmutableList.of();
      }

      float[] queryVector =
          VectorPayloads.decodeVector(VectorPayloads.toBytes(query), dimension, "Query");
      // the graph is asked for as many candidates as its search width allows, not just for the
      // number asked for: the traversal is the same either way, and a wider candidate set gives
      // the exact re-scoring below more true neighbours to order
      int candidates = Math.max(topK, options.efSearch());
      List<SearchResult<VectorItem, Float>> nearest = index.findNearest(queryVector, candidates);

      List<IndexSearchResult> results = Lists.newArrayList();
      for (SearchResult<VectorItem, Float> result : nearest) {
        int id = Math.toIntExact(result.item().id());
        Preconditions.checkArgument(
            id >= 0 && id < mappingCount, "HNSW result id out of range: %s", id);
        // re-score with the exact metric on the stored vector, so a score is the one the exact
        // search would have given
        results.add(
            IndexSearchResult.of(
                dataFiles[id], positions[id], scoring.score(queryVector, result.item().vector())));
      }

      // the candidates come back in the order the graph ranked them, which is not necessarily the
      // order of the exact scores, so the re-scored results are ordered by those before
      // truncating
      results.sort(scoring.comparator());
      return results.size() > topK ? Lists.newArrayList(results.subList(0, topK)) : results;
    }
  }

  private DistanceFunction<float[], Float> distanceFunction() {
    switch (scoring.metric()) {
      case COSINE:
        return com.github.jelmerk.knn.DistanceFunctions.FLOAT_COSINE_DISTANCE;
      case DOT_PRODUCT:
        return com.github.jelmerk.knn.DistanceFunctions.FLOAT_INNER_PRODUCT;
      case EUCLIDEAN:
        return com.github.jelmerk.knn.DistanceFunctions.FLOAT_EUCLIDEAN_DISTANCE;
      default:
        throw new IllegalStateException("Unknown vector index metric: " + scoring.metric());
    }
  }

  /** A vector with a numeric id, stored inside the HNSW graph. */
  private static final class VectorItem implements Item<Long, float[]> {
    private final long itemId;
    private final float[] itemVector;

    private VectorItem(long itemId, float[] itemVector) {
      this.itemId = itemId;
      this.itemVector = itemVector;
    }

    @Override
    public Long id() {
      return itemId;
    }

    @Override
    public float[] vector() {
      return itemVector;
    }

    @Override
    public int dimensions() {
      return itemVector.length;
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
          .structure(VectorIndexDefinition.VectorStructure.HNSW)
          .quantizer(VectorIndexDefinition.VectorQuantizer.NONE)
          .metric(VectorIndexDefinition.Metric.COSINE)
          .metric(VectorIndexDefinition.Metric.DOT_PRODUCT)
          .metric(VectorIndexDefinition.Metric.EUCLIDEAN)
          // hnswlib-java is pure Java, so a deployment needs nothing beyond the jar
          .nativeLibrary(false)
          // the library can add to a graph, which a refresh does not use because it rebuilds the
          // index files of the data files that changed
          .incrementalAdd(true)
          .build();
    }

    @Override
    public VectorKernel create(VectorIndexDefinition definition) {
      return new HnswlibKernel(
          definition.dimension(),
          HnswOptions.from(definition.properties()),
          new VectorScoring(definition.metric()));
    }
  }
}
