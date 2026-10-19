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
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.index.IndexAdapter;
import org.apache.iceberg.index.IndexDefinition;
import org.apache.iceberg.index.IndexEntry;
import org.apache.iceberg.index.IndexSearchResult;
import org.apache.iceberg.index.vector.VectorIndexDefinition.VectorQuantizer;
import org.apache.iceberg.index.vector.VectorIndexDefinition.VectorStructure;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

/**
 * The vector adapter: payload codec, exact search, and metric-aware ordering for vector indexes.
 *
 * <p>Vector values (entry payloads and queries) are encoded as {@code [format version][length]
 * [float values]}. An entry-bearing index file payload is {@code [format version][entry count] [per
 * entry: data file, row position, payload length, payload bytes]}. Both encodings are versioned so
 * they can evolve independently of the container and the metadata model.
 *
 * <p>Search is exact (linear scan) by default. In {@link
 * VectorIndexDefinition.VectorStructure#HNSW} mode, the adapter also builds an HNSW graph over the
 * entries and stores it as an auxiliary payload: {@code [format version][graph length][serialized
 * graph][mapping from item id to row reference]}. Approximate search loads the graph, retrieves
 * candidates, and re-scores them with the exact metric, so result scores are identical to exact
 * search regardless of the graph.
 */
public final class VectorIndexAdapter implements IndexAdapter, java.io.Serializable {

  private static final long serialVersionUID = 1L;

  /** Blob type of entry-bearing vector index files. */
  public static final String ENTRY_BLOB_TYPE = "iceberg-vector-entries-v1";

  /** Blob type of auxiliary HNSW index files. */
  public static final String AUXILIARY_BLOB_TYPE = "iceberg-vector-hnsw-v1";

  /**
   * Building an index hands the library a listener to call as the work proceeds. There is nothing
   * to report here, and the library takes the remainder of the completed count and the interval to
   * decide when to call it, so the interval is one the remainder never reaches rather than zero,
   * which the library would divide by.
   */
  private static final ProgressListener NO_PROGRESS = (workDone, max) -> {};

  private static final int NEVER_REPORT_PROGRESS = Integer.MAX_VALUE;

  private static final int VALUE_FORMAT_VERSION = 1;
  private static final int ENTRIES_FORMAT_VERSION = 2;
  private static final int HNSW_PAYLOAD_VERSION = 2;

  private final VectorIndexDefinition definition;
  private final HnswOptions hnswOptions;

  public VectorIndexAdapter(VectorIndexDefinition definition) {
    Preconditions.checkNotNull(definition, "Vector index definition is null");
    // the structure and the quantizer are recorded so a definition can express what is coming, but
    // this build only has a kernel for these. Failing here beats building an index nothing can
    // search, and the message says what is available.
    Preconditions.checkArgument(
        definition.structure() == VectorStructure.FLAT
            || definition.structure() == VectorStructure.HNSW,
        "Unsupported vector structure %s: this build searches %s and %s",
        definition.structure(),
        VectorStructure.FLAT,
        VectorStructure.HNSW);
    Preconditions.checkArgument(
        definition.quantizer() == VectorQuantizer.NONE,
        "Unsupported vector quantizer %s: this build stores vectors as they are (%s)",
        definition.quantizer(),
        VectorQuantizer.NONE);
    this.definition = definition;
    // the options are read once, so a malformed one fails when the adapter is made rather than in
    // the middle of a build or a search
    this.hnswOptions = HnswOptions.from(definition.properties());
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
  public boolean writesAuxiliaryFiles() {
    return definition.structure() == VectorIndexDefinition.VectorStructure.HNSW;
  }

  /** Encodes a vector value as an entry payload or query payload. */
  public byte[] encodeVector(float[] vector) {
    return encodeVector(vector, "Vector");
  }

  /** Encodes a query vector. */
  public ByteBuffer encodeQuery(float[] query) {
    return ByteBuffer.wrap(encodeVector(query, "Query"));
  }

  /** Decodes a vector value encoded by {@link #encodeVector}. */
  /**
   * Encodes a column value into an entry payload: the value is the engine's representation of a
   * numeric array column, an iterable of numbers.
   */
  @Override
  public byte[] encodeEntryValue(Object columnValue) {
    List<Float> values = Lists.newArrayList();
    for (Object value : (Iterable<?>) columnValue) {
      values.add(((Number) value).floatValue());
    }

    float[] vector = new float[values.size()];
    for (int i = 0; i < vector.length; i++) {
      vector[i] = values.get(i);
    }
    return encodeVector(vector);
  }

  public float[] decodeVector(byte[] payload) {
    return decodeVector(payload, "Vector");
  }

  /** Decodes a query vector encoded by {@link #encodeQuery}. */
  public float[] decodeQuery(ByteBuffer query) {
    return decodeVector(toBytes(query), "Query");
  }

  @Override
  public ByteBuffer encodeEntries(List<IndexEntry> entries) {
    Preconditions.checkNotNull(entries, "Index entries are null");
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(bytes)) {
      out.writeInt(ENTRIES_FORMAT_VERSION);
      // the data files an entry points at are written once, as a dictionary, and an entry refers to
      // one by position: a path is long and every entry of a file repeats it, which is most of the
      // payload once there are many entries per file
      List<String> dataFiles = dataFilesOf(entries);
      out.writeInt(dataFiles.size());
      for (String dataFile : dataFiles) {
        out.writeUTF(dataFile);
      }

      out.writeInt(entries.size());
      for (IndexEntry entry : entries) {
        decodeVector(entry.payload(), "Entry");
        out.writeInt(dataFiles.indexOf(entry.dataFile()));
        out.writeLong(entry.rowPosition());
        byte[] payload = entry.payload();
        out.writeInt(payload.length);
        out.write(payload);
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to encode vector index entries", e);
    }
    return ByteBuffer.wrap(bytes.toByteArray());
  }

  @Override
  public List<IndexEntry> decodeEntries(ByteBuffer payload) {
    Preconditions.checkNotNull(payload, "Entry payload is null");
    byte[] bytes = toBytes(payload);
    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
      int version = in.readInt();
      Preconditions.checkArgument(
          version == ENTRIES_FORMAT_VERSION,
          "Unsupported vector entries format version: %s",
          version);
      int dataFileCount = in.readInt();
      Preconditions.checkArgument(
          dataFileCount >= 0, "Invalid vector data file count: %s", dataFileCount);
      List<String> dataFiles = Lists.newArrayListWithCapacity(dataFileCount);
      for (int i = 0; i < dataFileCount; i++) {
        dataFiles.add(in.readUTF());
      }

      int entryCount = in.readInt();
      Preconditions.checkArgument(entryCount >= 0, "Invalid vector entry count: %s", entryCount);
      List<IndexEntry> entries = Lists.newArrayList();
      for (int i = 0; i < entryCount; i++) {
        int dataFileIndex = in.readInt();
        Preconditions.checkArgument(
            dataFileIndex >= 0 && dataFileIndex < dataFiles.size(),
            "Invalid vector data file index: %s",
            dataFileIndex);
        String dataFile = dataFiles.get(dataFileIndex);
        long rowPosition = in.readLong();
        int payloadLength = in.readInt();
        Preconditions.checkArgument(
            payloadLength >= 0, "Invalid entry payload length: %s", payloadLength);
        byte[] entryPayload = new byte[payloadLength];
        in.readFully(entryPayload);
        entries.add(IndexEntry.of(dataFile, rowPosition, entryPayload));
      }
      return ImmutableList.copyOf(entries);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to decode vector index entries", e);
    }
  }

  /**
   * The distinct data files of the given entries, in the order they first appear, which is the
   * dictionary an encoded entry refers to its data file by.
   */
  private static List<String> dataFilesOf(List<IndexEntry> entries) {
    Set<String> dataFiles = Sets.newLinkedHashSet();
    for (IndexEntry entry : entries) {
      dataFiles.add(entry.dataFile());
    }
    return ImmutableList.copyOf(dataFiles);
  }

  @Override
  public PreparedSearch prepare(ByteBuffer payload) {
    // the entries are decoded once, so a batch pays the decode per index file instead of per query
    List<IndexEntry> entries = decodeEntries(payload);
    return (query, topK) -> {
      Preconditions.checkArgument(topK >= 0, "Top-k must be non-negative: %s", topK);
      if (topK == 0) {
        return ImmutableList.of();
      }

      float[] queryVector = decodeQuery(query);
      List<IndexSearchResult> results = Lists.newArrayList();
      for (IndexEntry entry : entries) {
        float[] vector = decodeVector(entry.payload(), "Entry");
        results.add(
            IndexSearchResult.of(
                entry.dataFile(), entry.rowPosition(), score(queryVector, vector)));
      }

      results.sort(resultComparator());
      if (results.size() > topK) {
        results = Lists.newArrayList(results.subList(0, topK));
      }
      return ImmutableList.copyOf(results);
    };
  }

  @Override
  public List<IndexSearchResult> search(ByteBuffer payload, ByteBuffer query, int topK) {
    return prepare(payload).search(query, topK);
  }

  @Override
  public ByteBuffer encodeAuxiliaryPayload(List<IndexEntry> entries) {
    return encodeAuxiliaryPayload(entries, 1);
  }

  @Override
  public ByteBuffer encodeAuxiliaryPayload(List<IndexEntry> entries, int parallelism) {
    Preconditions.checkNotNull(entries, "Index entries are null");
    Preconditions.checkArgument(parallelism >= 1, "Parallelism must be at least one");
    List<IndexEntry> materialized = Lists.newArrayList(entries);
    HnswIndex<Long, float[], VectorItem, Float> index =
        HnswIndex.newBuilder(definition.dimension(), distanceFunction(), materialized.size())
            .withM(hnswOptions.maxM())
            .withEfConstruction(hnswOptions.efConstruction())
            .withEf(hnswOptions.efSearch())
            .build();

    List<VectorItem> items = Lists.newArrayListWithCapacity(materialized.size());
    for (int i = 0; i < materialized.size(); i++) {
      items.add(new VectorItem((long) i, decodeVector(materialized.get(i).payload(), "Entry")));
    }

    // inserting the items one at a time only ever uses one core, and building the graph is the
    // expensive part of a build, so a caller that has more cores to give gets a thread pool here.
    // One thread stays on this path so that a single threaded build produces the same graph it
    // always did.
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

    ByteArrayOutputStream payload = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(payload)) {
      out.writeInt(HNSW_PAYLOAD_VERSION);
      byte[] graphBytes = graph.toByteArray();
      out.writeInt(graphBytes.length);
      out.write(graphBytes);
      // the same dictionary the entries use: a graph node refers to its data file by position
      List<String> dataFiles = dataFilesOf(materialized);
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
    return ByteBuffer.wrap(payload.toByteArray());
  }

  @Override
  public PreparedSearch prepareAuxiliary(ByteBuffer payload) {
    byte[] bytes = toBytes(payload);
    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
      int version = in.readInt();
      Preconditions.checkArgument(
          version == HNSW_PAYLOAD_VERSION, "Unsupported HNSW payload format version: %s", version);
      int graphLength = in.readInt();
      Preconditions.checkArgument(graphLength >= 0, "Invalid HNSW graph length: %s", graphLength);
      byte[] graph = new byte[graphLength];
      in.readFully(graph);
      int dataFileCount = in.readInt();
      Preconditions.checkArgument(
          dataFileCount >= 0, "Invalid HNSW data file count: %s", dataFileCount);
      String[] dictionary = new String[dataFileCount];
      for (int i = 0; i < dataFileCount; i++) {
        dictionary[i] = in.readUTF();
      }

      int mappingCount = in.readInt();
      Preconditions.checkArgument(
          mappingCount >= 0, "Invalid HNSW mapping count: %s", mappingCount);
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
      index.setEf(hnswOptions.efSearch());

      return (query, topK) -> {
        Preconditions.checkArgument(topK >= 0, "Top-k must be non-negative: %s", topK);
        if (topK == 0) {
          return ImmutableList.of();
        }

        float[] queryVector = decodeQuery(query);
        // the graph is asked for as many candidates as its search width allows, not just for the
        // number asked for: the traversal is the same either way, and a wider candidate set gives
        // the exact re-scoring below more true neighbours to order
        int candidates = Math.max(topK, hnswOptions.efSearch());
        List<SearchResult<VectorItem, Float>> nearest = index.findNearest(queryVector, candidates);

        List<IndexSearchResult> results = Lists.newArrayList();
        for (SearchResult<VectorItem, Float> result : nearest) {
          int id = Math.toIntExact(result.item().id());
          Preconditions.checkArgument(
              id >= 0 && id < mappingCount, "HNSW result id out of range: %s", id);
          // re-score with the exact metric, on the stored vector, so a score is the one exact
          // search
          // would give
          results.add(
              IndexSearchResult.of(
                  dataFiles[id], positions[id], score(queryVector, result.item().vector())));
        }

        // the candidates come back in the order the graph ranked them, which is not necessarily the
        // order of the exact scores, so the re-scored results are ordered by those before
        // truncating
        results.sort(resultComparator());
        return results.size() > topK ? Lists.newArrayList(results.subList(0, topK)) : results;
      };
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to load the HNSW index payload", e);
    }
  }

  @Override
  public List<IndexSearchResult> searchAuxiliary(ByteBuffer payload, ByteBuffer query, int topK) {
    return prepareAuxiliary(payload).search(query, topK);
  }

  @Override
  public Comparator<IndexSearchResult> resultComparator() {
    Comparator<IndexSearchResult> tieBreaker =
        Comparator.comparing(IndexSearchResult::dataFile)
            .thenComparingLong(IndexSearchResult::rowPosition);
    switch (definition.metric()) {
      case EUCLIDEAN:
        return Comparator.comparingDouble(IndexSearchResult::score).thenComparing(tieBreaker);
      case COSINE:
      case DOT_PRODUCT:
        return Comparator.comparingDouble(IndexSearchResult::score)
            .reversed()
            .thenComparing(tieBreaker);
      default:
        throw new IllegalStateException("Unknown vector index metric: " + definition.metric());
    }
  }

  private DistanceFunction<float[], Float> distanceFunction() {
    switch (definition.metric()) {
      case COSINE:
        return com.github.jelmerk.knn.DistanceFunctions.FLOAT_COSINE_DISTANCE;
      case DOT_PRODUCT:
        return com.github.jelmerk.knn.DistanceFunctions.FLOAT_INNER_PRODUCT;
      case EUCLIDEAN:
        return com.github.jelmerk.knn.DistanceFunctions.FLOAT_EUCLIDEAN_DISTANCE;
      default:
        throw new IllegalStateException("Unknown vector index metric: " + definition.metric());
    }
  }

  private byte[] encodeVector(float[] vector, String description) {
    Preconditions.checkNotNull(vector, description + " vector is null");
    Preconditions.checkArgument(vector.length > 0, description + " vector is empty");
    Preconditions.checkArgument(
        vector.length == definition.dimension(),
        "%s dimension %s does not match index dimension %s",
        description,
        vector.length,
        definition.dimension());
    for (float value : vector) {
      Preconditions.checkArgument(
          Float.isFinite(value), "%s values must be finite: %s", description, value);
    }
    ByteBuffer buffer = ByteBuffer.allocate(8 + 4 * vector.length);
    buffer.putInt(VALUE_FORMAT_VERSION);
    buffer.putInt(vector.length);
    for (float value : vector) {
      buffer.putFloat(value);
    }
    return buffer.array();
  }

  private float[] decodeVector(byte[] payload, String description) {
    Preconditions.checkNotNull(payload, description + " payload is null");
    ByteBuffer buffer = ByteBuffer.wrap(payload);
    int version = buffer.getInt();
    Preconditions.checkArgument(
        version == VALUE_FORMAT_VERSION,
        "Unsupported %s value format version: %s",
        description,
        version);
    int length = buffer.getInt();
    Preconditions.checkArgument(length > 0, "%s dimension is empty", description);
    Preconditions.checkArgument(
        length == definition.dimension(),
        "%s dimension %s does not match index dimension %s",
        description,
        length,
        definition.dimension());
    float[] vector = new float[length];
    for (int i = 0; i < length; i++) {
      vector[i] = buffer.getFloat();
      Preconditions.checkArgument(
          Float.isFinite(vector[i]), "%s values must be finite: %s", description, vector[i]);
    }
    return vector;
  }

  private double score(float[] query, float[] vector) {
    switch (definition.metric()) {
      case COSINE:
        double dot = 0.0;
        double queryNorm = 0.0;
        double vectorNorm = 0.0;
        for (int i = 0; i < query.length; i++) {
          dot += (double) query[i] * vector[i];
          queryNorm += (double) query[i] * query[i];
          vectorNorm += (double) vector[i] * vector[i];
        }
        return queryNorm == 0.0 || vectorNorm == 0.0
            ? 0.0
            : dot / (Math.sqrt(queryNorm) * Math.sqrt(vectorNorm));
      case DOT_PRODUCT:
        double product = 0.0;
        for (int i = 0; i < query.length; i++) {
          product += (double) query[i] * vector[i];
        }
        return product;
      case EUCLIDEAN:
        double distanceSquared = 0.0;
        for (int i = 0; i < query.length; i++) {
          double difference = (double) query[i] - vector[i];
          distanceSquared += difference * difference;
        }
        return Math.sqrt(distanceSquared);
      default:
        throw new IllegalStateException("Unknown vector index metric: " + definition.metric());
    }
  }

  private static byte[] toBytes(ByteBuffer buffer) {
    byte[] bytes = new byte[buffer.remaining()];
    buffer.duplicate().get(bytes);
    return bytes;
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
}
