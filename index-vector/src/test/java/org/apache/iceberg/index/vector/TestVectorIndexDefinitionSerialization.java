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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.iceberg.index.vector.VectorIndexDefinition.VectorQuantizer;
import org.apache.iceberg.index.vector.VectorIndexDefinition.VectorStructure;
import org.apache.iceberg.util.JsonUtil;
import org.junit.jupiter.api.Test;

/**
 * How a vector index definition says what it wants.
 *
 * <p>How the vectors are organised and how each is stored are separate axes, and the definition
 * records them separately. Definitions written before they were separated name a single mode; those
 * still have to read back as the structure they meant, or every index already on a table would stop
 * being readable.
 */
class TestVectorIndexDefinitionSerialization {

  private static final VectorIndexType TYPE = new VectorIndexType();

  @Test
  public void readsADefinitionThatNamesAModeRatherThanTwoAxes() {
    VectorIndexDefinition exact =
        (VectorIndexDefinition) TYPE.parseDefinition(node(exactLegacyJson()));
    assertThat(exact.structure()).as("EXACT meant a flat scan").isEqualTo(VectorStructure.FLAT);
    assertThat(exact.quantizer()).isEqualTo(VectorQuantizer.NONE);

    VectorIndexDefinition hnsw =
        (VectorIndexDefinition) TYPE.parseDefinition(node(hnswLegacyJson()));
    assertThat(hnsw.structure()).isEqualTo(VectorStructure.HNSW);
    assertThat(hnsw.quantizer()).isEqualTo(VectorQuantizer.NONE);
    HnswOptions options = HnswOptions.from(hnsw.properties());
    assertThat(options.maxM()).isEqualTo(8);
    assertThat(options.efConstruction()).isEqualTo(100);
    assertThat(options.efSearch()).isEqualTo(64);
  }

  @Test
  public void writesTheTwoAxesAndReadsThemBack() {
    VectorIndexDefinition definition =
        VectorIndexDefinition.builder()
            .fieldId(2)
            .dimension(128)
            .metric(VectorIndexDefinition.Metric.EUCLIDEAN)
            .structure(VectorStructure.HNSW)
            .quantizer(VectorQuantizer.NONE)
            .maxM(16)
            .efConstruction(200)
            .efSearch(50)
            .build();

    String json =
        JsonUtil.generate(
            generator -> {
              generator.writeStartObject();
              TYPE.writeDefinition(definition, generator);
              generator.writeEndObject();
            },
            false);

    assertThat(json).contains("\"structure\":\"HNSW\"");
    assertThat(json).contains("\"quantizer\":\"NONE\"");
    assertThat(json).doesNotContain("\"mode\"");

    VectorIndexDefinition parsed = (VectorIndexDefinition) TYPE.parseDefinition(node(json));
    assertThat(parsed).isEqualTo(definition);
  }

  @Test
  public void defaultsToAFlatScanWithTheVectorsAsTheyAre() {
    VectorIndexDefinition definition =
        VectorIndexDefinition.builder()
            .fieldId(2)
            .dimension(4)
            .metric(VectorIndexDefinition.Metric.COSINE)
            .build();
    assertThat(definition.structure()).isEqualTo(VectorStructure.FLAT);
    assertThat(definition.quantizer()).isEqualTo(VectorQuantizer.NONE);
  }

  @Test
  public void writesTheHnswOptionsAsPropertiesRatherThanFields() {
    VectorIndexDefinition definition =
        VectorIndexDefinition.builder()
            .fieldId(2)
            .dimension(8)
            .metric(VectorIndexDefinition.Metric.EUCLIDEAN)
            .structure(VectorStructure.HNSW)
            .maxM(32)
            .efConstruction(400)
            .efSearch(120)
            .build();

    String json =
        JsonUtil.generate(
            generator -> {
              generator.writeStartObject();
              TYPE.writeDefinition(definition, generator);
              generator.writeEndObject();
            },
            false);

    // the options are properties of the definition, not fields of its own
    assertThat(json).contains("\"properties\"");
    assertThat(json).contains("\"max-m\":\"32\"");
    assertThat(json).contains("\"ef-construction\":\"400\"");
    assertThat(json).contains("\"ef-search\":\"120\"");
    assertThat(json).doesNotContain("\"m\"");
    assertThat(json).doesNotContain("\"ef-construction\":400");

    assertThat(TYPE.parseDefinition(node(json))).isEqualTo(definition);
  }

  @Test
  public void rejectsAnOptionTheStructureDoesNotRead() {
    // nlist belongs to IVF, not to HNSW; taking it silently would hide a definition that asks for
    // something the structure it named cannot do
    assertThatThrownBy(
            () ->
                new VectorIndexAdapter(
                    VectorIndexDefinition.builder()
                        .fieldId(2)
                        .dimension(8)
                        .metric(VectorIndexDefinition.Metric.EUCLIDEAN)
                        .structure(VectorStructure.HNSW)
                        .property("nlist", "128")
                        .build()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unknown HNSW option nlist")
        .hasMessageContaining("max-m");
  }

  @Test
  public void rejectsAnOptionThatIsNotANumber() {
    assertThatThrownBy(
            () ->
                new VectorIndexAdapter(
                    VectorIndexDefinition.builder()
                        .fieldId(2)
                        .dimension(8)
                        .metric(VectorIndexDefinition.Metric.EUCLIDEAN)
                        .structure(VectorStructure.HNSW)
                        .property(HnswOptions.MAX_M, "sixteen")
                        .build()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("HNSW option max-m is not a number: sixteen");
  }

  @Test
  public void rejectsAStructureItDoesNotKnow() {
    assertThatThrownBy(
            () ->
                TYPE.parseDefinition(
                    node(
                        "{\"field-id\":2,\"dimension\":4,\"metric\":\"COSINE\","
                            + "\"structure\":\"LSH\",\"m\":16,"
                            + "\"ef-construction\":200,\"ef-search\":50}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsupported vector index structure: LSH");
  }

  @Test
  public void rejectsAQuantizerItDoesNotKnow() {
    assertThatThrownBy(
            () ->
                TYPE.parseDefinition(
                    node(
                        "{\"field-id\":2,\"dimension\":4,\"metric\":\"COSINE\","
                            + "\"structure\":\"FLAT\",\"quantizer\":\"SCALAR\",\"m\":16,"
                            + "\"ef-construction\":200,\"ef-search\":50}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsupported vector index quantizer: SCALAR");
  }

  @Test
  public void refusesToBuildAnIndexNothingCanSearchYet() {
    // IVF and RaBitQ are named so that a definition can express them, but no kernel implements
    // either, and building an index nothing can read is worse than refusing
    assertThatThrownBy(
            () ->
                new VectorIndexAdapter(
                    VectorIndexDefinition.builder()
                        .fieldId(2)
                        .dimension(4)
                        .metric(VectorIndexDefinition.Metric.COSINE)
                        .structure(VectorStructure.IVF)
                        .build()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsupported vector structure IVF")
        .hasMessageContaining("FLAT");

    assertThatThrownBy(
            () ->
                new VectorIndexAdapter(
                    VectorIndexDefinition.builder()
                        .fieldId(2)
                        .dimension(4)
                        .metric(VectorIndexDefinition.Metric.COSINE)
                        .quantizer(VectorQuantizer.RABITQ)
                        .build()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsupported vector quantizer RABITQ");
  }

  private static String exactLegacyJson() {
    return "{\"field-id\":1,\"dimension\":2,\"metric\":\"COSINE\",\"mode\":\"EXACT\","
        + "\"m\":16,\"ef-construction\":200,\"ef-search\":50}";
  }

  private static String hnswLegacyJson() {
    return "{\"field-id\":1,\"dimension\":2,\"metric\":\"COSINE\",\"mode\":\"HNSW\","
        + "\"m\":8,\"ef-construction\":100,\"ef-search\":64}";
  }

  private static JsonNode node(String json) {
    try {
      return JsonUtil.mapper().readTree(json);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
