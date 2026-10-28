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
package org.apache.iceberg.index.spark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import org.apache.iceberg.TestTables.LocalFileIO;
import org.apache.iceberg.index.IndexAdapter;
import org.apache.iceberg.index.IndexBlob;
import org.apache.iceberg.index.IndexBuildResult;
import org.apache.iceberg.index.IndexDefinition;
import org.apache.iceberg.index.IndexEntry;
import org.apache.iceberg.index.IndexFileMemory;
import org.apache.iceberg.index.IndexSearchResult;
import org.apache.iceberg.index.IndexType;
import org.apache.iceberg.index.IndexTypes;
import org.apache.iceberg.index.PuffinIndexFileContainer;
import org.apache.iceberg.index.TestingIndexAdapter;
import org.apache.iceberg.index.TestingIndexDefinition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestIndexCacheReadContext {
  private static final IndexDefinition DEFINITION = new Definition();

  @Test
  void unknownActualPreparedStateCannotUseACompatibleBuildEstimate(@TempDir Path temp)
      throws IOException {
    IndexTypes.register(new UnknownPreparedType());
    UnknownPreparedAdapter adapter = new UnknownPreparedAdapter();
    ByteBuffer payload =
        adapter.encodeEntries(
            List.of(IndexEntry.of("data", 7, TestingIndexAdapter.valuePayload(10))));
    byte[] bytes = new byte[payload.remaining()];
    payload.duplicate().get(bytes);
    try (LocalFileIO io = new LocalFileIO()) {
      String location = temp.resolve("index.puffin").toString();
      new PuffinIndexFileContainer()
          .write(io.newOutputFile(location), List.of(IndexBlob.of(adapter.entryBlobType(), bytes)));
      IndexFileMemory memory = adapter.encodeEntriesWithMemory(List.of()).memory();
      IndexCacheProtocol.Command command =
          IndexCacheProtocol.Command.builder()
              .session("session")
              .executorId("executor")
              .incarnation("incarnation")
              .indexUuid("index")
              .epoch(1)
              .planId("plan")
              .commandId("command")
              .type(IndexCacheProtocol.CommandType.LOAD)
              .file(
                  IndexCacheProtocol.File.builder()
                      .location(location)
                      .cacheKey("key")
                      .estimatedBytes(192L)
                      .memory(memory)
                      .build())
              .definition(DEFINITION)
              .io(io)
              .build();
      assertThatThrownBy(() -> new IndexCacheReadContext().load(command))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("unknown");
    }
    // Unknown memory must not make the adapter itself unsearchable.
    List<IndexSearchResult> hits =
        adapter.prepare(payload).search(ByteBuffer.wrap(TestingIndexAdapter.valuePayload(0)), 1);
    assertThat(hits).extracting(IndexSearchResult::score).containsExactly(10.0);
  }

  private static class UnknownPreparedAdapter extends TestingIndexAdapter {
    private UnknownPreparedAdapter() {
      super(TestingIndexDefinition.of(1));
    }

    @Override
    public IndexDefinition definition() {
      return DEFINITION;
    }

    @Override
    public IndexBuildResult encodeEntriesWithMemory(List<IndexEntry> entries) {
      ByteBuffer payload = encodeEntries(entries);
      return IndexBuildResult.of(
          payload,
          IndexFileMemory.builder()
              .payloadBytes(payload.remaining())
              .preparedBytes(80)
              .modelVersion(2)
              .readerId("unknown-prepared/1")
              .runtimeProfile("fixture")
              .build());
    }
  }

  private static class Definition implements IndexDefinition {
    private static final long serialVersionUID = 1L;

    @Override
    public String type() {
      return "test-unknown-prepared";
    }

    @Override
    public int fieldId() {
      return 1;
    }
  }

  private static class UnknownPreparedType implements IndexType {
    @Override
    public String type() {
      return DEFINITION.type();
    }

    @Override
    public IndexDefinition parseDefinition(JsonNode node) {
      return DEFINITION;
    }

    @Override
    public void writeDefinition(IndexDefinition definition, JsonGenerator generator)
        throws IOException {
      generator.writeStartObject();
      generator.writeNumberField("field-id", definition.fieldId());
      generator.writeEndObject();
    }

    @Override
    public IndexAdapter createAdapter(IndexDefinition definition) {
      return new UnknownPreparedAdapter();
    }
  }
}
