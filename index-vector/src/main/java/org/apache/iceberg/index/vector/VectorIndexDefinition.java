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
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * The vector index definition: the indexed source field, the vector dimension, and the similarity
 * or distance metric.
 *
 * <p>This is the first {@link IndexDefinition} implementation; other index types (scalar, text, and
 * so on) follow the same pattern without adding new top-level concepts.
 *
 * <p>How the vectors are organised ({@link VectorStructure}) and how each one is stored ({@link
 * VectorQuantizer}) are two separate choices. Every structure can be paired with every quantizer in
 * principle, so they are two fields rather than one enumeration: a single one would have to grow to
 * one value per combination.
 */
public final class VectorIndexDefinition implements IndexDefinition {

  private static final long serialVersionUID = 1L;

  /** How the vectors of a table are organised for search. */
  public enum VectorStructure {
    /**
     * Every vector in one list, scanned in full; also the reference the others are measured
     * against.
     */
    FLAT,
    /** A navigable small-world graph over the vectors. */
    HNSW,
    /**
     * Vectors clustered around centroids, searched by visiting the nearest lists. Not implemented.
     */
    IVF
  }

  /**
   * How a vector is stored, which is independent of how the vectors are organised.
   *
   * <p>A quantizer trades accuracy of the stored form for size. <strong>Not implemented
   * yet:</strong> no kernel applies {@link #RABITQ}, so asking for it fails rather than silently
   * falling back.
   */
  public enum VectorQuantizer {
    /** The vector as it was given. */
    NONE,
    /**
     * One bit per dimension, scored approximately and re-ranked against the rows. Not implemented.
     */
    RABITQ
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
  private final VectorStructure structure;
  private final VectorQuantizer quantizer;
  private final Map<String, String> properties;

  private VectorIndexDefinition(
      int fieldId,
      int dimension,
      Metric metric,
      VectorStructure structure,
      VectorQuantizer quantizer,
      Map<String, String> properties) {
    Preconditions.checkArgument(fieldId > 0, "Invalid vector field id: %s", fieldId);
    Preconditions.checkArgument(dimension > 0, "Invalid vector dimension: %s", dimension);
    Preconditions.checkNotNull(metric, "Vector index metric is null");
    Preconditions.checkNotNull(structure, "Vector index structure is null");
    Preconditions.checkNotNull(quantizer, "Vector index quantizer is null");
    this.fieldId = fieldId;
    this.dimension = dimension;
    this.metric = metric;
    this.structure = structure;
    this.quantizer = quantizer;
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

  /** Returns how the vectors are organised. */
  public VectorStructure structure() {
    return structure;
  }

  /** Returns how each vector is stored. */
  public VectorQuantizer quantizer() {
    return quantizer;
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
        && structure == that.structure
        && quantizer == that.quantizer
        && properties.equals(that.properties);
  }

  @Override
  public int hashCode() {
    int result = Integer.hashCode(fieldId);
    result = 31 * result + Integer.hashCode(dimension);
    result = 31 * result + metric.hashCode();
    result = 31 * result + structure.hashCode();
    result = 31 * result + quantizer.hashCode();
    result = 31 * result + properties.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("fieldId", fieldId)
        .add("dimension", dimension)
        .add("metric", metric)
        .add("structure", structure)
        .add("quantizer", quantizer)
        .add("properties", properties)
        .toString();
  }

  public static final class Builder {
    private Integer fieldId;
    private Integer dimension;
    private Metric metric;
    private VectorStructure structure = VectorStructure.FLAT;
    private VectorQuantizer quantizer = VectorQuantizer.NONE;
    private final Map<String, String> properties = Maps.newLinkedHashMap();

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

    /** Sets how the vectors are organised; defaults to {@link VectorStructure#FLAT}. */
    public Builder structure(VectorStructure newStructure) {
      this.structure = newStructure;
      return this;
    }

    /** Sets how each vector is stored; defaults to {@link VectorQuantizer#NONE}. */
    public Builder quantizer(VectorQuantizer newQuantizer) {
      this.quantizer = newQuantizer;
      return this;
    }

    /**
     * Sets the HNSW graph degree. Sugar over {@link #property}, so that a caller with a number in
     * hand does not have to turn it into a string.
     */
    public Builder maxM(int newMaxM) {
      return property(HnswOptions.MAX_M, Integer.toString(newMaxM));
    }

    /** Sets the HNSW construction ef; sugar over {@link #property}. */
    public Builder efConstruction(int newEfConstruction) {
      return property(HnswOptions.EF_CONSTRUCTION, Integer.toString(newEfConstruction));
    }

    /** Sets the HNSW search ef; sugar over {@link #property}. */
    public Builder efSearch(int newEfSearch) {
      return property(HnswOptions.EF_SEARCH, Integer.toString(newEfSearch));
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
      Preconditions.checkNotNull(structure, "Vector index structure is not set");
      Preconditions.checkNotNull(quantizer, "Vector index quantizer is not set");
      return new VectorIndexDefinition(
          fieldId, dimension, metric, structure, quantizer, ImmutableMap.copyOf(properties));
    }
  }
}
