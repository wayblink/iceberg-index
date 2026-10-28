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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.apache.iceberg.index.vector.VectorIndexAdapter;
import org.apache.iceberg.index.vector.VectorIndexDefinition;
import org.apache.iceberg.index.vector.VectorIndexTypes;
import org.apache.iceberg.io.InputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestIndexCacheReadContext {
  private static final IndexDefinition DEFINITION = new Definition();

  @Test
  void readerProofIsIncludedInTheWrapperCharge() {
    VectorIndexTypes.register();
    VectorIndexAdapter adapter =
        new VectorIndexAdapter(
            VectorIndexDefinition.builder()
                .fieldId(1)
                .dimension(2)
                .metric(VectorIndexDefinition.Metric.EUCLIDEAN)
                .structure(VectorIndexDefinition.VectorStructure.FLAT)
                .quantizer(VectorIndexDefinition.VectorQuantizer.NONE)
                .properties(Map.of())
                .build());
    IndexAdapter.PreparedSearch prepared = adapter.prepare(adapter.encodeEntries(List.of()));
    ExecutorIndexCache.CachedFile file =
        ExecutorIndexCache.loaded(10, prepared).withContext(adapter);
    assertThat(file.hasKnownContext()).isTrue();
    // Independent lower bound: wrapper 64, lock+sync 56, receipt array header+SHA-256 48.
    assertThat(file.bytes()).isGreaterThanOrEqualTo(prepared.estimatedBytes() + 64 + 56 + 48);
  }

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
                      .estimatedBytes(IndexCacheMemory.residentBytes(memory))
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

  @Test
  void warmUnknownQueryCannotBecomeManaged(@TempDir Path temp) throws IOException {
    warmTakeover(temp, false);
  }

  @Test
  void warmQueryCannotBypassCurrentRuntimeProfile(@TempDir Path temp) throws IOException {
    warmTakeover(temp, true);
  }

  private void warmTakeover(Path temp, boolean mismatch) throws IOException {
    IndexTypes.register(new UnknownPreparedType());
    UnknownPreparedAdapter adapter = new UnknownPreparedAdapter();
    AtomicInteger reads = new AtomicInteger();
    try (LocalFileIO io =
        new LocalFileIO() {
          @Override
          public InputFile newInputFile(String path) {
            reads.incrementAndGet();
            return super.newInputFile(path);
          }
        }) {
      String location = temp.resolve("warm.puffin").toString();
      ByteBuffer payload =
          adapter.encodeEntries(
              List.of(IndexEntry.of("data", 7, TestingIndexAdapter.valuePayload(10))));
      byte[] bytes = new byte[payload.remaining()];
      payload.get(bytes);
      new PuffinIndexFileContainer()
          .write(io.newOutputFile(location), List.of(IndexBlob.of(adapter.entryBlobType(), bytes)));
      ExecutorIndexCache cache = ExecutorIndexCache.get();
      cache.clear();
      ExecutorIndexCache.Config config =
          ExecutorIndexCache.Config.of(true, 10_000).withOwner("table");
      String key =
          DistributedIndexSearch.cacheKey(
              "table", DistributedIndexSearch.adapterKey(adapter), location);
      try {
        ExecutorIndexCache.CachedFile warm =
            cache.file(
                location,
                config,
                DistributedIndexSearch.adapterKey(adapter),
                () -> DistributedIndexSearch.loadFile(io, location, adapter));
        cache.applyTarget("index", 1, Set.of(key));
        long charged = cache.stats().bytes();
        reads.set(0);
        IndexFileMemory memory = adapter.encodeEntriesWithMemory(List.of()).memory();
        if (mismatch) {
          memory = memory.toBuilder().runtimeProfile("processors=different-executor").build();
        }
        IndexCacheProtocol.Command command =
            IndexCacheProtocol.Command.builder()
                .session("session")
                .executorId("executor")
                .incarnation("incarnation")
                .indexUuid("index")
                .epoch(1)
                .planId("plan")
                .commandId("warm-command")
                .type(IndexCacheProtocol.CommandType.LOAD)
                .file(
                    IndexCacheProtocol.File.builder()
                        .location(location)
                        .cacheKey(key)
                        .owner("table")
                        .estimatedBytes(IndexCacheMemory.residentBytes(memory))
                        .memory(memory)
                        .build())
                .definition(DEFINITION)
                .io(io)
                .build();
        try (ExecutorIndexCacheWorker worker =
            new ExecutorIndexCacheWorker(
                "session",
                "executor",
                "incarnation",
                cache,
                config,
                new IndexCacheReadContext(),
                IndexCacheWorkerConfig.parse(java.util.Map.of()))) {
          IndexCacheProtocol.Ack ack = worker.execute(command);
          assertThat(ack.status()).isNotEqualTo(IndexCacheProtocol.AckStatus.LOADED);
          assertThat(reads).hasValue(0);
          assertThat(cache.stats().bytes()).isEqualTo(charged);
          try (ExecutorIndexCache.Lease lease = cache.acquireManaged("index", 1, key)) {
            assertThat(lease).isNotNull();
            IndexCacheReadContext context = new IndexCacheReadContext();
            ExecutorIndexCache.Loader reuse =
                new ExecutorIndexCache.Loader() {
                  @Override
                  public ExecutorIndexCache.CachedFile load() {
                    return context.load(command);
                  }

                  @Override
                  public void validate() {
                    context.validate(command);
                  }

                  @Override
                  public void validate(ExecutorIndexCache.CachedFile file) {
                    context.validate(command, file);
                  }
                };
            assertThatThrownBy(() -> cache.installManaged("index", 1, key, config, reuse))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(mismatch ? "incompatible" : "unknown");
            assertThat(reads).hasValue(0);
            assertThat(cache.stats().bytes()).isEqualTo(charged);
            List<IndexSearchResult> leasedResults =
                lease
                    .file()
                    .searching(
                        searcher ->
                            searcher.search(
                                ByteBuffer.wrap(TestingIndexAdapter.valuePayload(0)), 1));
            assertThat(leasedResults).extracting(IndexSearchResult::score).containsExactly(10.0);
          }
          List<IndexSearchResult> results =
              warm.searching(
                  searcher ->
                      searcher.search(ByteBuffer.wrap(TestingIndexAdapter.valuePayload(0)), 1));
          assertThat(results).extracting(IndexSearchResult::score).containsExactly(10.0);
        }
      } finally {
        cache.clear();
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void realFlatWarmLeaseRequiresTheCurrentProfile(boolean mismatch, @TempDir Path temp)
      throws IOException {
    VectorIndexTypes.register();
    VectorIndexDefinition definition =
        VectorIndexDefinition.builder()
            .fieldId(1)
            .dimension(2)
            .metric(VectorIndexDefinition.Metric.EUCLIDEAN)
            .structure(VectorIndexDefinition.VectorStructure.FLAT)
            .quantizer(VectorIndexDefinition.VectorQuantizer.NONE)
            .properties(Map.of())
            .build();
    VectorIndexAdapter adapter = new VectorIndexAdapter(definition);
    AtomicInteger reads = new AtomicInteger();
    try (LocalFileIO io =
        new LocalFileIO() {
          @Override
          public InputFile newInputFile(String path) {
            reads.incrementAndGet();
            return super.newInputFile(path);
          }
        }) {
      String location = temp.resolve("flat.puffin").toString();
      IndexBuildResult built =
          adapter.encodeEntriesWithMemory(
              List.of(IndexEntry.of("data", 7, adapter.encodeVector(new float[] {3, 4}))));
      byte[] bytes = new byte[built.payload().remaining()];
      built.payload().duplicate().get(bytes);
      new PuffinIndexFileContainer()
          .write(io.newOutputFile(location), List.of(IndexBlob.of(adapter.entryBlobType(), bytes)));
      ExecutorIndexCache cache = ExecutorIndexCache.get();
      cache.clear();
      ExecutorIndexCache.Config config =
          ExecutorIndexCache.Config.of(true, 10_000).withOwner("table");
      String adapterKey = DistributedIndexSearch.adapterKey(adapter);
      String key = DistributedIndexSearch.cacheKey("table", adapterKey, location);
      try {
        ExecutorIndexCache.CachedFile warm =
            cache.file(
                location,
                config,
                adapterKey,
                () -> DistributedIndexSearch.loadFile(io, location, adapter));
        cache.applyTarget("index", 1, Set.of(key));
        try (ExecutorIndexCache.Lease lease = cache.acquireManaged("index", 1, key)) {
          assertThat(lease).isNotNull();
          long charged = cache.stats().bytes();
          reads.set(0);
          IndexFileMemory memory = built.memory();
          assertThat(memory).isNotNull();
          if (mismatch) {
            memory =
                memory.toBuilder()
                    .runtimeProfile(
                        memory
                            .runtimeProfile()
                            .replace(
                                "processors=" + Runtime.getRuntime().availableProcessors(),
                                "processors=999999"))
                    .build();
          }
          IndexCacheProtocol.Command command =
              IndexCacheProtocol.Command.builder()
                  .session("session")
                  .executorId("executor")
                  .incarnation("incarnation")
                  .indexUuid("index")
                  .epoch(1)
                  .planId("plan")
                  .commandId("flat-command")
                  .type(IndexCacheProtocol.CommandType.LOAD)
                  .file(
                      IndexCacheProtocol.File.builder()
                          .location(location)
                          .cacheKey(key)
                          .owner("table")
                          .estimatedBytes(IndexCacheMemory.residentBytes(memory))
                          .memory(memory)
                          .build())
                  .definition(definition)
                  .io(io)
                  .build();
          IndexCacheReadContext context = new IndexCacheReadContext();
          ExecutorIndexCache.Loader loader =
              new ExecutorIndexCache.Loader() {
                @Override
                public ExecutorIndexCache.CachedFile load() {
                  return context.load(command);
                }

                @Override
                public void validate() {
                  context.validate(command);
                }

                @Override
                public void validate(ExecutorIndexCache.CachedFile file) {
                  context.validate(command, file);
                }
              };
          if (mismatch) {
            assertThatThrownBy(() -> cache.installManaged("index", 1, key, config, loader))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("incompatible");
          } else {
            assertThat(cache.installManaged("index", 1, key, config, loader)).isSameAs(warm);
          }
          assertThat(reads).hasValue(0);
          assertThat(cache.stats().bytes()).isEqualTo(charged);
          cache.releaseIndex("index", 2);
          assertThat(cache.stats().bytes()).isEqualTo(charged);
          List<IndexSearchResult> results =
              lease
                  .file()
                  .searching(
                      searcher ->
                          searcher.search(
                              ByteBuffer.wrap(adapter.encodeVector(new float[] {0, 0})), 1));
          assertThat(results).extracting(IndexSearchResult::score).containsExactly(5.0);
        }
        assertThat(cache.stats().bytes()).isZero();
      } finally {
        cache.clear();
      }
    }
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
