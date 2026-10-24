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
import java.util.List;
import org.junit.jupiter.api.Test;

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

  @Test
  void legacyAdapterStillRejectsUnsupportedAuxiliaryEncoding() {
    TestingIndexAdapter adapter = new TestingIndexAdapter(TestingIndexDefinition.of(4));
    assertThatThrownBy(() -> adapter.encodeAuxiliaryPayloadWithMemory(List.of(), 1))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("does not write auxiliary payloads");
  }
}
