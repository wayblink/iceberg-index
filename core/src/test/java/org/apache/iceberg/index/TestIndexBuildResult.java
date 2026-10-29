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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class TestIndexBuildResult {
  @Test
  void rejectsMissingPayload() {
    assertThatThrownBy(() -> IndexBuildResult.of(null, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("payload");
  }

  @Test
  void rejectsPayloadSizeThatDoesNotMatchEncodedWindow() {
    IndexFileMemory memory =
        IndexFileMemory.builder()
            .payloadBytes(3)
            .preparedBytes(73)
            .modelVersion(1)
            .readerId("testing/1")
            .runtimeProfile("fixture")
            .build();
    assertThatThrownBy(() -> IndexBuildResult.of(ByteBuffer.wrap(new byte[] {1, 2}), memory))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("payload bytes");
  }

  @Test
  void keepsEncodedWindowWhenBuffersAreConsumed() {
    ByteBuffer payload = ByteBuffer.wrap(new byte[] {9, 1, 2, 9});
    payload.position(1).limit(3);
    IndexBuildResult built = IndexBuildResult.of(payload, null);
    payload.clear();
    byte[] encoded = new byte[2];
    built.payload().get(encoded);
    assertThat(encoded).containsExactly(new byte[] {1, 2});
    assertThat(built.payload().remaining()).isEqualTo(2);
    assertThat(built.memory()).isNull();
  }

  @Test
  void legacyAdapterKeepsExistingEncodingWithoutInventingEstimate() {
    TestingIndexAdapter adapter = new TestingIndexAdapter(TestingIndexDefinition.of(4));
    IndexBuildResult built =
        adapter.encodeEntriesWithMemory(
            List.of(IndexEntry.of("data.parquet", 7, TestingIndexAdapter.valuePayload(10))));
    assertThat(built.memory()).isNull();
    List<IndexEntry> decoded = adapter.decodeEntries(built.payload());
    assertThat(decoded).extracting(IndexEntry::dataFile).containsExactly("data.parquet");
    assertThat(decoded).extracting(IndexEntry::rowPosition).containsExactly(7L);
    assertThat(TestingIndexAdapter.payloadValue(decoded.get(0).payload())).isEqualTo(10);
  }

  @ParameterizedTest
  @EnumSource(BufferKind.class)
  void defaultPreparedEntriesKeepSnapshot(BufferKind kind) {
    assertSnapshot(kind, false);
  }

  @ParameterizedTest
  @EnumSource(BufferKind.class)
  void defaultPreparedAuxiliaryKeepsSnapshot(BufferKind kind) {
    assertSnapshot(kind, true);
  }

  @ParameterizedTest
  @EnumSource(BufferKind.class)
  void defaultPreparedEntriesKeepSearchWindow(BufferKind kind) {
    assertSearchWindow(kind, false);
  }

  @ParameterizedTest
  @EnumSource(BufferKind.class)
  void defaultPreparedAuxiliaryKeepsSearchWindow(BufferKind kind) {
    assertSearchWindow(kind, true);
  }

  @ParameterizedTest
  @EnumSource(BufferKind.class)
  void defaultPreparedEntriesAllowWritableSearchView(BufferKind kind) {
    assertWritableSearchView(kind, false);
  }

  @ParameterizedTest
  @EnumSource(BufferKind.class)
  void defaultPreparedAuxiliaryAllowsWritableSearchView(BufferKind kind) {
    assertWritableSearchView(kind, true);
  }

  @ParameterizedTest
  @EnumSource(BufferKind.class)
  void defaultPreparedEntriesKeepInputByteOrder(BufferKind kind) {
    assertByteOrder(kind, false);
  }

  @ParameterizedTest
  @EnumSource(BufferKind.class)
  void defaultPreparedAuxiliaryKeepsInputByteOrder(BufferKind kind) {
    assertByteOrder(kind, true);
  }

  private void assertByteOrder(BufferKind kind, boolean auxiliary) {
    TestingIndexAdapter adapter =
        new ConsumingAdapter() {
          @Override
          public ByteBuffer encodeEntries(List<IndexEntry> entries) {
            ByteBuffer payload = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
            payload.putInt(1);
            payload.flip();
            return payload;
          }

          @Override
          public List<IndexSearchResult> search(ByteBuffer payload, ByteBuffer query, int topK) {
            return List.of(IndexSearchResult.of("data", 7, payload.getInt()));
          }
        };
    PayloadInput input = payloadInput(adapter, kind);
    input.window().order(ByteOrder.LITTLE_ENDIAN);
    IndexAdapter.PreparedSearch prepared =
        auxiliary ? adapter.prepareAuxiliary(input.window()) : adapter.prepare(input.window());
    ByteBuffer query = ByteBuffer.allocate(0);
    assertThat(prepared.search(query, 1)).extracting(IndexSearchResult::score).containsExactly(1.0);
    input.window().order(ByteOrder.BIG_ENDIAN);
    assertThat(prepared.search(query, 1)).extracting(IndexSearchResult::score).containsExactly(1.0);
  }

  private void assertWritableSearchView(BufferKind kind, boolean auxiliary) {
    TestingIndexAdapter adapter =
        new ConsumingAdapter() {
          @Override
          public List<IndexSearchResult> search(ByteBuffer payload, ByteBuffer query, int topK) {
            // The search SPI accepts a ByteBuffer without requiring a read-only view.
            payload.put(0, payload.get(0));
            return super.search(payload, query, topK);
          }
        };
    PayloadInput input = payloadInput(adapter, kind);
    IndexAdapter.PreparedSearch prepared =
        auxiliary ? adapter.prepareAuxiliary(input.window()) : adapter.prepare(input.window());
    ByteBuffer query = ByteBuffer.wrap(TestingIndexAdapter.valuePayload(0));
    assertHit(prepared.search(query, 1));
    assertHit(prepared.search(query, 1));
  }

  private void assertSnapshot(BufferKind kind, boolean auxiliary) {
    TestingIndexAdapter adapter = new ConsumingAdapter();
    PayloadInput input = payloadInput(adapter, kind);
    int position = input.window().position();
    int limit = input.window().limit();
    IndexAdapter.PreparedSearch prepared =
        auxiliary ? adapter.prepareAuxiliary(input.window()) : adapter.prepare(input.window());
    assertThat(input.window().position()).isEqualTo(position);
    assertThat(input.window().limit()).isEqualTo(limit);

    // Change only the encoded value, leaving a valid but different payload behind.
    input.storage().put(input.storage().limit() - 1, (byte) 20);
    assertHit(prepared.search(ByteBuffer.wrap(TestingIndexAdapter.valuePayload(0)), 1));
    assertThat(prepared.estimatedBytes()).isZero();
  }

  private void assertSearchWindow(BufferKind kind, boolean auxiliary) {
    TestingIndexAdapter adapter = new ConsumingAdapter();
    PayloadInput input = payloadInput(adapter, kind);
    int position = input.window().position();
    int limit = input.window().limit();
    IndexAdapter.PreparedSearch prepared =
        auxiliary ? adapter.prepareAuxiliary(input.window()) : adapter.prepare(input.window());
    ByteBuffer query = ByteBuffer.wrap(TestingIndexAdapter.valuePayload(0));
    assertHit(prepared.search(query, 1));
    assertThat(input.window().position()).isEqualTo(position);
    assertThat(input.window().limit()).isEqualTo(limit);
    input.window().clear();
    assertHit(prepared.search(query, 1));
  }

  private static void assertHit(List<IndexSearchResult> results) {
    assertThat(results).hasSize(1);
    assertThat(results.get(0).dataFile()).isEqualTo("data.parquet");
    assertThat(results.get(0).rowPosition()).isEqualTo(7L);
    assertThat(results.get(0).score()).isEqualTo(10.0);
  }

  private static PayloadInput payloadInput(TestingIndexAdapter adapter, BufferKind kind) {
    ByteBuffer encoded =
        adapter.encodeEntries(
            List.of(IndexEntry.of("data.parquet", 7, TestingIndexAdapter.valuePayload(10))));
    int padding = 8;
    ByteBuffer storage =
        kind == BufferKind.DIRECT
            ? ByteBuffer.allocateDirect(encoded.remaining() + 2 * padding)
            : ByteBuffer.allocate(encoded.remaining() + 2 * padding);
    storage.position(padding);
    storage.put(encoded);
    storage.limit(storage.position());
    ByteBuffer window = storage.duplicate();
    window.position(padding);
    window =
        switch (kind) {
          case WINDOW, DIRECT -> window;
          case SLICE -> window.slice();
          case READ_ONLY -> window.asReadOnlyBuffer();
        };
    return new PayloadInput(storage, window);
  }

  private enum BufferKind {
    WINDOW,
    SLICE,
    READ_ONLY,
    DIRECT
  }

  private record PayloadInput(ByteBuffer storage, ByteBuffer window) {}

  private static class ConsumingAdapter extends TestingIndexAdapter {
    private ConsumingAdapter() {
      super(TestingIndexDefinition.of(4));
    }

    @Override
    public List<IndexSearchResult> search(ByteBuffer payload, ByteBuffer query, int topK) {
      List<IndexSearchResult> results = super.search(payload, query, topK);
      payload.position(payload.limit());
      return results;
    }

    @Override
    public List<IndexSearchResult> searchAuxiliary(ByteBuffer payload, ByteBuffer query, int topK) {
      return search(payload, query, topK);
    }
  }

  @Test
  void legacyAdapterStillRejectsUnsupportedAuxiliaryEncoding() {
    TestingIndexAdapter adapter = new TestingIndexAdapter(TestingIndexDefinition.of(4));
    assertThatThrownBy(() -> adapter.encodeAuxiliaryPayloadWithMemory(List.of(), 1))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("does not write auxiliary payloads");
  }
}
