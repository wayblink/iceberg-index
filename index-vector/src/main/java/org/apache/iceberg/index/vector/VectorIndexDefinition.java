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

import java.util.Map;
import org.apache.iceberg.index.IndexDefinition;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

/**
 * The vector index definition: the indexed source field, the vector dimension, and the similarity
 * or distance metric.
 *
 * <p>This is the first {@link IndexDefinition} implementation; other index types (scalar, text, and
 * so on) follow the same pattern without adding new top-level concepts.
 *
 * <p>The {@link IndexMode} selects the search strategy: {@link IndexMode#EXACT} stores the entries
 * as an entry-bearing payload and scans it linearly, while {@link IndexMode#HNSW} additionally
 * builds an HNSW graph over the entries and stores it as an auxiliary payload for approximate
 * search.
 */
public final class VectorIndexDefinition implements IndexDefinition {

  private static final long serialVersionUID = 1L;

  /** Search strategies supported by the vector index. */
  public enum IndexMode {
    /** Exact linear scan over the entry payload. */
    EXACT,
    /** Approximate search over an HNSW graph stored as an auxiliary payload. */
    HNSW
  }

  /** Similarity and distance metrics supported by the vector index. */
  public enum Metric {
    COSINE,
    DOT_PRODUCT,
    EUCLIDEAN
  }

  private final int fieldId;
  private final int dimension;
  private final Metric metric;
  private final IndexMode mode;
  private final int maxM;
  private final int efConstruction;
  private final int efSearch;
  private final Map<String, String> properties;

  private VectorIndexDefinition(
      int fieldId,
      int dimension,
      Metric metric,
      IndexMode mode,
      int maxM,
      int efConstruction,
      int efSearch,
      Map<String, String> properties) {
    Preconditions.checkArgument(fieldId > 0, "Invalid vector field id: %s", fieldId);
    Preconditions.checkArgument(dimension > 0, "Invalid vector dimension: %s", dimension);
    Preconditions.checkNotNull(metric, "Vector index metric is null");
    Preconditions.checkNotNull(mode, "Vector index mode is null");
    Preconditions.checkArgument(maxM > 0, "Invalid HNSW m: %s", maxM);
    Preconditions.checkArgument(
        efConstruction > 0, "Invalid HNSW efConstruction: %s", efConstruction);
    Preconditions.checkArgument(efSearch > 0, "Invalid HNSW efSearch: %s", efSearch);
    this.fieldId = fieldId;
    this.dimension = dimension;
    this.metric = metric;
    this.mode = mode;
    this.maxM = maxM;
    this.efConstruction = efConstruction;
    this.efSearch = efSearch;
    this.properties = ImmutableMap.copyOf(properties);
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public String type() {
    return VectorIndexType.TYPE;
  }

  @Override
  public int fieldId() {
    return fieldId;
  }

  public int dimension() {
    return dimension;
  }

  public Metric metric() {
    return metric;
  }

  /** Returns the search strategy selected for this index. */
  public IndexMode mode() {
    return mode;
  }

  /** Returns the HNSW graph degree parameter; only used in {@link IndexMode#HNSW}. */
  public int maxM() {
    return maxM;
  }

  /** Returns the HNSW construction ef parameter; only used in {@link IndexMode#HNSW}. */
  public int efConstruction() {
    return efConstruction;
  }

  /** Returns the HNSW search ef parameter; only used in {@link IndexMode#HNSW}. */
  public int efSearch() {
    return efSearch;
  }

  public Map<String, String> properties() {
    return properties;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof VectorIndexDefinition)) {
      return false;
    }
    VectorIndexDefinition that = (VectorIndexDefinition) other;
    return fieldId == that.fieldId
        && dimension == that.dimension
        && metric == that.metric
        && mode == that.mode
        && maxM == that.maxM
        && efConstruction == that.efConstruction
        && efSearch == that.efSearch
        && properties.equals(that.properties);
  }

  @Override
  public int hashCode() {
    int result = Integer.hashCode(fieldId);
    result = 31 * result + Integer.hashCode(dimension);
    result = 31 * result + metric.hashCode();
    result = 31 * result + mode.hashCode();
    result = 31 * result + Integer.hashCode(maxM);
    result = 31 * result + Integer.hashCode(efConstruction);
    result = 31 * result + Integer.hashCode(efSearch);
    result = 31 * result + properties.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("fieldId", fieldId)
        .add("dimension", dimension)
        .add("metric", metric)
        .add("mode", mode)
        .add("maxM", maxM)
        .add("efConstruction", efConstruction)
        .add("efSearch", efSearch)
        .add("properties", properties)
        .toString();
  }

  public static final class Builder {
    private Integer fieldId;
    private Integer dimension;
    private Metric metric;
    private IndexMode mode = IndexMode.EXACT;
    private int maxM = 16;
    private int efConstruction = 200;
    private int efSearch = 50;
    private final ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();

    private Builder() {}

    public Builder fieldId(int newFieldId) {
      this.fieldId = newFieldId;
      return this;
    }

    public Builder dimension(int newDimension) {
      this.dimension = newDimension;
      return this;
    }

    public Builder metric(Metric newMetric) {
      this.metric = newMetric;
      return this;
    }

    /** Sets the search strategy; defaults to {@link IndexMode#EXACT}. */
    public Builder mode(IndexMode newMode) {
      this.mode = newMode;
      return this;
    }

    /** Sets the HNSW graph degree; defaults to 16. */
    public Builder maxM(int newMaxM) {
      this.maxM = newMaxM;
      return this;
    }

    /** Sets the HNSW construction ef; defaults to 200. */
    public Builder efConstruction(int newEfConstruction) {
      this.efConstruction = newEfConstruction;
      return this;
    }

    /** Sets the HNSW search ef; defaults to 50. */
    public Builder efSearch(int newEfSearch) {
      this.efSearch = newEfSearch;
      return this;
    }

    public Builder property(String key, String value) {
      properties.put(key, value);
      return this;
    }

    public Builder properties(Map<String, String> newProperties) {
      properties.putAll(newProperties);
      return this;
    }

    public VectorIndexDefinition build() {
      Preconditions.checkNotNull(fieldId, "Vector field id is not set");
      Preconditions.checkNotNull(dimension, "Vector dimension is not set");
      Preconditions.checkNotNull(metric, "Vector index metric is not set");
      Preconditions.checkNotNull(mode, "Vector index mode is not set");
      return new VectorIndexDefinition(
          fieldId, dimension, metric, mode, maxM, efConstruction, efSearch, properties.build());
    }
  }
}
