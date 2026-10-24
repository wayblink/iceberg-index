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

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestIndexFileMemory {

  @Test
  void retainsEstimationContext() {
    IndexFileMemory memory = builder().build();
    assertThat(memory.fileBytes()).isEqualTo(8192L);
    assertThat(memory.payloadBytes()).isEqualTo(1024L);
    assertThat(memory.preparedBytes()).isEqualTo(4096L);
    assertThat(memory.modelVersion()).isEqualTo(1);
    assertThat(memory.readerId()).isEqualTo("fixture-reader-v1");
    assertThat(memory.runtimeProfile()).isEqualTo("fixture-jvm-profile");
    assertThat(memory.structure()).containsExactlyEntriesOf(Map.of("nodes", 2L));
  }

  @Test
  void permitsMissingContainerSizeDuringBuild() {
    assertThat(builder().fileBytes(null).build().fileBytes()).isNull();
  }

  @Test
  void updatesContainerSizeWithoutLosingBuildEstimate() {
    IndexFileMemory built = builder().fileBytes(null).build();
    IndexFileMemory written = built.toBuilder().fileBytes(8192L).build();
    assertThat(written).isEqualTo(builder().build());
    assertThat(built.fileBytes()).isNull();
  }

  @Test
  void rejectsNegativeContainerSize() {
    assertThatThrownBy(() -> builder().fileBytes(-1L).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("file bytes");
  }

  @Test
  void rejectsNegativePayloadSize() {
    assertThatThrownBy(() -> builder().payloadBytes(-1L).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("payload bytes");
  }

  @Test
  void rejectsNegativePreparedSize() {
    assertThatThrownBy(() -> builder().preparedBytes(-1L).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("prepared bytes");
  }

  @Test
  void rejectsResidentSizeOverflow() {
    assertThatThrownBy(() -> builder().payloadBytes(Long.MAX_VALUE).preparedBytes(1).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("overflow");
  }

  @Test
  void permitsZeroSizes() {
    IndexFileMemory memory = builder().fileBytes(0L).payloadBytes(0).preparedBytes(0).build();
    assertThat(memory.payloadBytes()).isZero();
    assertThat(memory.preparedBytes()).isZero();
  }

  @ParameterizedTest
  @ValueSource(ints = {0, -1})
  void rejectsInvalidModelVersion(int version) {
    assertThatThrownBy(() -> builder().modelVersion(version).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("model version");
  }

  @Test
  void retainsUnknownModelVersion() {
    assertThat(builder().modelVersion(99).build().modelVersion()).isEqualTo(99);
  }

  @ParameterizedTest
  @ValueSource(strings = {"", " "})
  void rejectsEmptyReaderId(String readerId) {
    assertThatThrownBy(() -> builder().readerId(readerId).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("reader ID");
  }

  @ParameterizedTest
  @ValueSource(strings = {"", " "})
  void rejectsEmptyRuntimeProfile(String profile) {
    assertThatThrownBy(() -> builder().runtimeProfile(profile).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("runtime profile");
  }

  @Test
  void rejectsMissingEstimationFields() {
    assertThatThrownBy(() -> IndexFileMemory.builder().build())
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> builder().readerId(null).build())
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> builder().runtimeProfile(null).build())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void rejectsInvalidStructure() {
    assertThatThrownBy(() -> builder().structure(Map.of("nodes", -1L)).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("structure");
    assertThatThrownBy(() -> builder().structure(Map.of("", 2L)).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("structure");
    Map<String, Long> nullValue = new HashMap<>();
    nullValue.put("nodes", null);
    assertThatThrownBy(() -> builder().structure(nullValue).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("structure");
  }

  @Test
  void copiesStructure() {
    Map<String, Long> structure = new HashMap<>();
    structure.put("nodes", 2L);
    IndexFileMemory memory = builder().structure(structure).build();
    structure.put("nodes", 3L);
    assertThat(memory.structure()).containsExactlyEntriesOf(Map.of("nodes", 2L));
    assertThatThrownBy(() -> memory.structure().put("nodes", 4L))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void rebuildDoesNotMutateOriginalStructure() {
    IndexFileMemory original = builder().build();
    IndexFileMemory updated = original.toBuilder().structure(Map.of("nodes", 3L)).build();
    assertThat(original.structure()).containsExactlyEntriesOf(Map.of("nodes", 2L));
    assertThat(updated.structure()).containsExactlyEntriesOf(Map.of("nodes", 3L));
    assertThat(updated).isNotEqualTo(original);
  }

  private static IndexFileMemory.Builder builder() {
    return IndexFileMemory.builder()
        .fileBytes(8192L)
        .payloadBytes(1024)
        .preparedBytes(4096)
        .modelVersion(1)
        .readerId("fixture-reader-v1")
        .runtimeProfile("fixture-jvm-profile")
        .structure(Map.of("nodes", 2L));
  }
}
