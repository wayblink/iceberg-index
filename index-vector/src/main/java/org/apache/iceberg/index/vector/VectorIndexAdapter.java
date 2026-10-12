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
import org.apache.iceberg.index.IndexAdapter;
import org.apache.iceberg.index.IndexDefinition;
import org.apache.iceberg.index.IndexEntry;
import org.apache.iceberg.index.IndexSearchResult;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * The vector adapter: payload codec, exact search, and metric-aware ordering for vector indexes.
 *
 * <p>Vector values (entry payloads and queries) are encoded as {@code [format version][length]
 * [float values]}. An entry-bearing index file payload is {@code [format version][entry count] [per
 * entry: data file, row position, payload length, payload bytes]}. Both encodings are versioned so
 * they can evolve independently of the container and the metadata model.
 *
 * <p>Search is exact (linear scan) by default. In {@link VectorIndexDefinition.IndexMode#HNSW}
 * mode, the adapter also builds an HNSW graph over the entries and stores it as an auxiliary
 * payload: {@code [format version][graph length][serialized graph][mapping from item id to row
 * reference]}. Approximate search loads the graph, retrieves candidates, and re-scores them with
 * the exact metric, so result scores are identical to exact search regardless of the graph.
 */
public final class VectorIndexAdapter implements IndexAdapter, java.io.Serializable {

  private static final long serialVersionUID = 1L;

  /** Blob type of entry-bearing vector index files. */
  public static final String ENTRY_BLOB_TYPE = "iceberg-vector-entries-v1";

  /** Blob type of auxiliary HNSW index files. */
  public static final String AUXILIARY_BLOB_TYPE = "iceberg-vector-hnsw-v1";

  private static final int VALUE_FORMAT_VERSION = 1;
  private static final int ENTRIES_FORMAT_VERSION = 1;
  private static final int HNSW_PAYLOAD_FORMAT_VERSION = 1;

  private final VectorIndexDefinition definition;

  public VectorIndexAdapter(VectorIndexDefinition definition) {
    this.definition = Preconditions.checkNotNull(definition, "Vector index definition is null");
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
    return definition.mode() == VectorIndexDefinition.IndexMode.HNSW;
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
      out.writeInt(entries.size());
      for (IndexEntry entry : entries) {
        decodeVector(entry.payload(), "Entry");
        out.writeUTF(entry.dataFile());
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
      int entryCount = in.readInt();
      Preconditions.checkArgument(entryCount >= 0, "Invalid vector entry count: %s", entryCount);
      List<IndexEntry> entries = Lists.newArrayList();
      for (int i = 0; i < entryCount; i++) {
        String dataFile = in.readUTF();
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

  @Override
  public List<IndexSearchResult> search(ByteBuffer payload, ByteBuffer query, int topK) {
    Preconditions.checkArgument(topK >= 0, "Top-k must be non-negative: %s", topK);
    float[] queryVector = decodeQuery(query);
    if (topK == 0) {
      return ImmutableList.of();
    }

    List<IndexEntry> entries = decodeEntries(payload);
    List<IndexSearchResult> results = Lists.newArrayList();
    for (IndexEntry entry : entries) {
      float[] vector = decodeVector(entry.payload(), "Entry");
      results.add(
          IndexSearchResult.of(entry.dataFile(), entry.rowPosition(), score(queryVector, vector)));
    }

    results.sort(resultComparator());
    if (results.size() > topK) {
      results = Lists.newArrayList(results.subList(0, topK));
    }
    return ImmutableList.copyOf(results);
  }

  @Override
  public ByteBuffer encodeAuxiliaryPayload(List<IndexEntry> entries) {
    Preconditions.checkNotNull(entries, "Index entries are null");
    List<IndexEntry> materialized = Lists.newArrayList(entries);
    HnswIndex<Long, float[], VectorItem, Float> index =
        HnswIndex.newBuilder(definition.dimension(), distanceFunction(), materialized.size())
            .withM(definition.maxM())
            .withEfConstruction(definition.efConstruction())
            .withEf(definition.efSearch())
            .build();
    for (int i = 0; i < materialized.size(); i++) {
      float[] vector = decodeVector(materialized.get(i).payload(), "Entry");
      index.add(new VectorItem((long) i, vector));
    }

    ByteArrayOutputStream graph = new ByteArrayOutputStream();
    try {
      index.save(graph);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to serialize the HNSW index", e);
    }

    ByteArrayOutputStream payload = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(payload)) {
      out.writeInt(HNSW_PAYLOAD_FORMAT_VERSION);
      byte[] graphBytes = graph.toByteArray();
      out.writeInt(graphBytes.length);
      out.write(graphBytes);
      out.writeInt(materialized.size());
      for (IndexEntry entry : materialized) {
        out.writeUTF(entry.dataFile());
        out.writeLong(entry.rowPosition());
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to encode the HNSW index payload", e);
    }
    return ByteBuffer.wrap(payload.toByteArray());
  }

  @Override
  public List<IndexSearchResult> searchAuxiliary(ByteBuffer payload, ByteBuffer query, int topK) {
    Preconditions.checkArgument(topK >= 0, "Top-k must be non-negative: %s", topK);
    float[] queryVector = decodeQuery(query);
    if (topK == 0) {
      return ImmutableList.of();
    }

    byte[] bytes = toBytes(payload);
    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
      int version = in.readInt();
      Preconditions.checkArgument(
          version == HNSW_PAYLOAD_FORMAT_VERSION,
          "Unsupported HNSW payload format version: %s",
          version);
      int graphLength = in.readInt();
      Preconditions.checkArgument(graphLength >= 0, "Invalid HNSW graph length: %s", graphLength);
      byte[] graph = new byte[graphLength];
      in.readFully(graph);
      int mappingCount = in.readInt();
      Preconditions.checkArgument(
          mappingCount >= 0, "Invalid HNSW mapping count: %s", mappingCount);
      String[] dataFiles = new String[mappingCount];
      long[] positions = new long[mappingCount];
      for (int i = 0; i < mappingCount; i++) {
        dataFiles[i] = in.readUTF();
        positions[i] = in.readLong();
      }

      HnswIndex<Long, float[], VectorItem, Float> index =
          HnswIndex.load(new ByteArrayInputStream(graph));
      index.setEf(definition.efSearch());

      // the graph is asked for as many candidates as its search width allows, not just for the
      // number asked for: the traversal is the same either way, and a wider candidate set gives the
      // exact re-scoring below more true neighbours to order
      int candidates = Math.max(topK, definition.efSearch());
      List<SearchResult<VectorItem, Float>> nearest = index.findNearest(queryVector, candidates);

      List<IndexSearchResult> results = Lists.newArrayList();
      for (SearchResult<VectorItem, Float> result : nearest) {
        int id = Math.toIntExact(result.item().id());
        Preconditions.checkArgument(
            id >= 0 && id < mappingCount, "HNSW result id out of range: %s", id);
        // re-score with the exact metric, on the stored vector, so a score is the one exact search
        // would give
        results.add(
            IndexSearchResult.of(
                dataFiles[id], positions[id], score(queryVector, result.item().vector())));
      }

      // the candidates come back in the order the graph ranked them, which is not necessarily the
      // order of the exact scores, so the re-scored results are ordered by those before truncating
      results.sort(resultComparator());
      return results.size() > topK ? Lists.newArrayList(results.subList(0, topK)) : results;
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to search the HNSW index payload", e);
    }
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
