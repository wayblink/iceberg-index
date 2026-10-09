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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import org.apache.iceberg.util.JsonUtil;

/** A minimal {@link IndexType} used by core tests so core needs no real adapter. */
public final class TestingIndexType implements IndexType {

  /** The type identifier of the testing index type. */
  public static final String TYPE = "testing";

  private static final String WIDTH = "width";
  private static final String PROPERTIES = "properties";

  @Override
  public String type() {
    return TYPE;
  }

  @Override
  public IndexDefinition parseDefinition(JsonNode node) {
    TestingIndexDefinition definition = TestingIndexDefinition.of(JsonUtil.getInt(WIDTH, node));
    return definition;
  }

  @Override
  public void writeDefinition(IndexDefinition definition, JsonGenerator generator)
      throws IOException {
    if (!(definition instanceof TestingIndexDefinition)) {
      throw new IllegalArgumentException(
          "Invalid definition for testing index type: " + definition);
    }
    TestingIndexDefinition testingDefinition = (TestingIndexDefinition) definition;
    generator.writeNumberField(WIDTH, testingDefinition.width());
  }

  @Override
  public IndexAdapter createAdapter(IndexDefinition definition) {
    if (!(definition instanceof TestingIndexDefinition)) {
      throw new IllegalArgumentException(
          "Invalid definition for testing index type: " + definition);
    }
    return new TestingIndexAdapter((TestingIndexDefinition) definition);
  }
}
