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
package org.apache.iceberg.index;

import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

/**
 * The vector index definition: the indexed source field, the vector dimension, and the similarity
 * or distance metric.
 *
 * <p>This is the first {@link IndexDefinition} implementation; other index types (scalar, text, and
 * so on) follow the same pattern without adding new top-level concepts.
 */
public final class VectorIndexDefinition implements IndexDefinition {

  /** Similarity and distance metrics supported by the vector index. */
  public enum Metric {
    COSINE,
    DOT_PRODUCT,
    EUCLIDEAN
  }

  private final int fieldId;
  private final int dimension;
  private final Metric metric;
  private final Map<String, String> properties;

  private VectorIndexDefinition(
      int fieldId, int dimension, Metric metric, Map<String, String> properties) {
    Preconditions.checkArgument(fieldId > 0, "Invalid vector field id: %s", fieldId);
    Preconditions.checkArgument(dimension > 0, "Invalid vector dimension: %s", dimension);
    Preconditions.checkNotNull(metric, "Vector index metric is null");
    this.fieldId = fieldId;
    this.dimension = dimension;
    this.metric = metric;
    this.properties = ImmutableMap.copyOf(properties);
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public String type() {
    return VectorIndexType.TYPE;
  }

  public int fieldId() {
    return fieldId;
  }

  public int dimension() {
    return dimension;
  }

  public Metric metric() {
    return metric;
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
        && properties.equals(that.properties);
  }

  @Override
  public int hashCode() {
    int result = Integer.hashCode(fieldId);
    result = 31 * result + Integer.hashCode(dimension);
    result = 31 * result + metric.hashCode();
    result = 31 * result + properties.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("fieldId", fieldId)
        .add("dimension", dimension)
        .add("metric", metric)
        .add("properties", properties)
        .toString();
  }

  public static final class Builder {
    private Integer fieldId;
    private Integer dimension;
    private Metric metric;
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
      return new VectorIndexDefinition(fieldId, dimension, metric, properties.build());
    }
  }
}
