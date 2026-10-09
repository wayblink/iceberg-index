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
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.JsonUtil;

/** Definition of a testing index type whose search needs all payloads together. */
public final class TestingCorrelatedIndexDefinition implements IndexDefinition {

  /** The type identifier of the correlated testing index type. */
  public static final String TYPE = "testing-correlated";

  private static final String WIDTH = "width";

  private final int width;

  private TestingCorrelatedIndexDefinition(int width) {
    this.width = width;
  }

  public static TestingCorrelatedIndexDefinition of(int width) {
    return new TestingCorrelatedIndexDefinition(width);
  }

  @Override
  public String type() {
    return TYPE;
  }

  public int width() {
    return width;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof TestingCorrelatedIndexDefinition)) {
      return false;
    }
    return width == ((TestingCorrelatedIndexDefinition) other).width;
  }

  @Override
  public int hashCode() {
    return width;
  }

  @Override
  public String toString() {
    return "TestingCorrelatedIndexDefinition{width=" + width + '}';
  }

  /** Index type of the correlated testing index. */
  public static final class Type implements IndexType {

    @Override
    public String type() {
      return TYPE;
    }

    @Override
    public IndexDefinition parseDefinition(JsonNode node) {
      return of(JsonUtil.getInt(WIDTH, node));
    }

    @Override
    public void writeDefinition(IndexDefinition definition, JsonGenerator generator)
        throws IOException {
      generator.writeNumberField(WIDTH, ((TestingCorrelatedIndexDefinition) definition).width());
    }

    @Override
    public IndexAdapter createAdapter(IndexDefinition definition) {
      return new Adapter((TestingCorrelatedIndexDefinition) definition);
    }
  }

  /** Adapter whose scores depend on all payloads of a snapshot, to exercise correlated search. */
  public static final class Adapter implements IndexAdapter {

    private final TestingCorrelatedIndexDefinition definition;
    private final TestingIndexAdapter entriesAdapter;

    public Adapter(TestingCorrelatedIndexDefinition definition) {
      this.definition = definition;
      this.entriesAdapter = new TestingIndexAdapter(TestingIndexDefinition.of(definition.width()));
    }

    @Override
    public IndexDefinition definition() {
      return definition;
    }

    @Override
    public String entryBlobType() {
      return TestingIndexAdapter.BLOB_TYPE;
    }

    @Override
    public boolean searchesAllPayloadsTogether() {
      return true;
    }

    @Override
    public ByteBuffer encodeEntries(List<IndexEntry> entries) {
      return entriesAdapter.encodeEntries(entries);
    }

    @Override
    public List<IndexEntry> decodeEntries(ByteBuffer payload) {
      return entriesAdapter.decodeEntries(payload);
    }

    /**
     * Scores with a term that depends on the number of payloads, so a result is only correct when
     * every payload of the snapshot was passed together.
     */
    @Override
    public java.util.List<IndexSearchResult> searchAll(
        java.util.List<java.nio.ByteBuffer> payloads, java.nio.ByteBuffer query, int topK) {
      List<IndexSearchResult> results = Lists.newArrayList();
      for (java.nio.ByteBuffer payload : payloads) {
        for (IndexEntry entry : decodeEntries(payload)) {
          int value = TestingIndexAdapter.payloadValue(entry.payload());
          results.add(
              IndexSearchResult.of(entry.dataFile(), entry.rowPosition(), value + payloads.size()));
        }
      }

      results.sort(resultComparator());
      return results.size() > topK ? Lists.newArrayList(results.subList(0, topK)) : results;
    }

    @Override
    public List<IndexSearchResult> search(ByteBuffer payload, ByteBuffer query, int topK) {
      throw new UnsupportedOperationException("Correlated search must go through searchAll");
    }

    @Override
    public byte[] encodeEntryValue(Object columnValue) {
      return TestingIndexAdapter.valuePayload(((Number) columnValue).intValue());
    }

    @Override
    public Comparator<IndexSearchResult> resultComparator() {
      return entriesAdapter.resultComparator();
    }
  }
}
