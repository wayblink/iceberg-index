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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.Locale;
import org.apache.iceberg.index.IndexAdapter;
import org.apache.iceberg.index.IndexDefinition;
import org.apache.iceberg.index.IndexType;
import org.apache.iceberg.index.IndexTypes;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

/**
 * The {@link IndexType} for vector indexes, registered in {@link IndexTypes}.
 *
 * <p>Owns the {@code vector} type identifier, the JSON serialization of {@link
 * VectorIndexDefinition}, and creation of the {@link VectorIndexAdapter}.
 */
public final class VectorIndexType implements IndexType {

  /** The stable type identifier for vector indexes. */
  public static final String TYPE = "vector";

  private static final String FIELD_ID = "field-id";
  private static final String DIMENSION = "dimension";
  private static final String METRIC = "metric";
  private static final String MODE = "mode";
  private static final String M = "m";
  private static final String EF_CONSTRUCTION = "ef-construction";
  private static final String EF_SEARCH = "ef-search";
  private static final String PROPERTIES = "properties";
  private static final String STRUCTURE = "structure";
  private static final String QUANTIZER = "quantizer";

  @Override
  public String type() {
    return TYPE;
  }

  @Override
  public IndexDefinition parseDefinition(JsonNode node) {
    VectorIndexDefinition.Builder builder =
        VectorIndexDefinition.builder()
            .fieldId(JsonUtil.getInt(FIELD_ID, node))
            .dimension(JsonUtil.getInt(DIMENSION, node))
            .metric(parseMetric(JsonUtil.getString(METRIC, node)))
            .structure(parseStructure(node))
            .quantizer(parseQuantizer(node))
            .maxM(JsonUtil.getInt(M, node))
            .efConstruction(JsonUtil.getInt(EF_CONSTRUCTION, node))
            .efSearch(JsonUtil.getInt(EF_SEARCH, node));
    if (node.has(PROPERTIES)) {
      builder.properties(JsonUtil.getStringMap(PROPERTIES, node));
    }
    return builder.build();
  }

  @Override
  public void writeDefinition(IndexDefinition definition, JsonGenerator generator)
      throws IOException {
    Preconditions.checkArgument(
        definition instanceof VectorIndexDefinition,
        "Invalid definition for vector index type: %s",
        definition);
    VectorIndexDefinition vectorDefinition = (VectorIndexDefinition) definition;
    generator.writeNumberField(FIELD_ID, vectorDefinition.fieldId());
    generator.writeNumberField(DIMENSION, vectorDefinition.dimension());
    generator.writeStringField(METRIC, vectorDefinition.metric().name());
    generator.writeStringField(STRUCTURE, vectorDefinition.structure().name());
    generator.writeStringField(QUANTIZER, vectorDefinition.quantizer().name());
    generator.writeNumberField(M, vectorDefinition.maxM());
    generator.writeNumberField(EF_CONSTRUCTION, vectorDefinition.efConstruction());
    generator.writeNumberField(EF_SEARCH, vectorDefinition.efSearch());
    if (!vectorDefinition.properties().isEmpty()) {
      JsonUtil.writeStringMap(PROPERTIES, vectorDefinition.properties(), generator);
    }
  }

  @Override
  public IndexAdapter createAdapter(IndexDefinition definition) {
    Preconditions.checkArgument(
        definition instanceof VectorIndexDefinition,
        "Invalid definition for vector index type: %s",
        definition);
    return new VectorIndexAdapter((VectorIndexDefinition) definition);
  }

  private static VectorIndexDefinition.Metric parseMetric(String value) {
    try {
      return VectorIndexDefinition.Metric.valueOf(value.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Unsupported vector index metric: " + value, e);
    }
  }

  private static VectorIndexDefinition.VectorStructure parseStructure(JsonNode node) {
    if (node.has(STRUCTURE)) {
      String value = JsonUtil.getString(STRUCTURE, node);
      try {
        return VectorIndexDefinition.VectorStructure.valueOf(value.toUpperCase(Locale.ROOT));
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("Unsupported vector index structure: " + value, e);
      }
    }

    // definitions written before the two axes were separated name a single mode; EXACT meant a flat
    // scan and HNSW meant a graph, both with the vectors stored as they were
    if (node.has(MODE)) {
      switch (JsonUtil.getString(MODE, node).toUpperCase(Locale.ROOT)) {
        case "EXACT":
          return VectorIndexDefinition.VectorStructure.FLAT;
        case "HNSW":
          return VectorIndexDefinition.VectorStructure.HNSW;
        default:
          throw new IllegalArgumentException(
              "Unsupported vector index mode: " + JsonUtil.getString(MODE, node));
      }
    }

    return VectorIndexDefinition.VectorStructure.FLAT;
  }

  private static VectorIndexDefinition.VectorQuantizer parseQuantizer(JsonNode node) {
    if (!node.has(QUANTIZER)) {
      // a definition written before the two axes were separated never quantized
      return VectorIndexDefinition.VectorQuantizer.NONE;
    }

    String value = JsonUtil.getString(QUANTIZER, node);
    try {
      return VectorIndexDefinition.VectorQuantizer.valueOf(value.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Unsupported vector index quantizer: " + value, e);
    }
  }
}
