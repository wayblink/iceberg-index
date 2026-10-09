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
package org.apache.iceberg.index.text;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import org.apache.iceberg.index.IndexAdapter;
import org.apache.iceberg.index.IndexDefinition;
import org.apache.iceberg.index.IndexType;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

/** Index type of BM25 text indexes. */
public final class TextIndexType implements IndexType {

  /** The stable type identifier for text indexes. */
  public static final String TYPE = "text";

  private static final String FIELD_ID = "field-id";
  private static final String K1 = "k1";
  private static final String B = "b";
  private static final String MIN_TOKEN_LENGTH = "min-token-length";
  private static final String LOWERCASE = "lowercase";
  private static final String STOPWORDS = "stopwords";
  private static final String PROPERTIES = "properties";

  @Override
  public String type() {
    return TYPE;
  }

  @Override
  public IndexDefinition parseDefinition(JsonNode node) {
    TextIndexDefinition.Builder builder =
        TextIndexDefinition.builder().fieldId(JsonUtil.getInt(FIELD_ID, node));
    if (node.has(K1)) {
      builder.k1(node.get(K1).asDouble());
    }
    if (node.has(B)) {
      builder.b(node.get(B).asDouble());
    }
    if (node.has(MIN_TOKEN_LENGTH)) {
      builder.minTokenLength(JsonUtil.getInt(MIN_TOKEN_LENGTH, node));
    }
    if (node.has(LOWERCASE)) {
      builder.lowercase(JsonUtil.getBool(LOWERCASE, node));
    }
    if (node.has(STOPWORDS)) {
      for (String stopword : JsonUtil.getString(STOPWORDS, node).split(",")) {
        if (!stopword.trim().isEmpty()) {
          builder.stopword(stopword.trim());
        }
      }
    }
    if (node.has(PROPERTIES)) {
      builder.properties(JsonUtil.getStringMap(PROPERTIES, node));
    }
    return builder.build();
  }

  @Override
  public void writeDefinition(IndexDefinition definition, JsonGenerator generator)
      throws IOException {
    Preconditions.checkArgument(
        definition instanceof TextIndexDefinition,
        "Invalid definition for text index type: %s",
        definition);
    TextIndexDefinition textDefinition = (TextIndexDefinition) definition;
    generator.writeNumberField(FIELD_ID, textDefinition.fieldId());
    generator.writeNumberField(K1, textDefinition.k1());
    generator.writeNumberField(B, textDefinition.b());
    generator.writeNumberField(MIN_TOKEN_LENGTH, textDefinition.minTokenLength());
    generator.writeBooleanField(LOWERCASE, textDefinition.lowercase());
    if (!textDefinition.stopwords().isEmpty()) {
      generator.writeStringField(STOPWORDS, String.join(",", textDefinition.stopwords()));
    }
    if (!textDefinition.properties().isEmpty()) {
      JsonUtil.writeStringMap(PROPERTIES, textDefinition.properties(), generator);
    }
  }

  @Override
  public IndexAdapter createAdapter(IndexDefinition definition) {
    Preconditions.checkArgument(
        definition instanceof TextIndexDefinition,
        "Invalid definition for text index type: %s",
        definition);
    return new TextIndexAdapter((TextIndexDefinition) definition);
  }
}
