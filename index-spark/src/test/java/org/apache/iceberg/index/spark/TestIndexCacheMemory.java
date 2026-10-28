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

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.TestTables.LocalFileIO;
import org.apache.iceberg.index.IndexAdapter;
import org.apache.iceberg.index.IndexEntry;
import org.apache.iceberg.index.IndexFile;
import org.apache.iceberg.index.IndexFileMemory;
import org.apache.iceberg.index.IndexManifest;
import org.apache.iceberg.index.IndexManifestList;
import org.apache.iceberg.index.IndexSearchResult;
import org.apache.iceberg.index.IndexSnapshotWriter;
import org.apache.iceberg.index.PuffinIndexFileContainer;
import org.apache.iceberg.index.vector.VectorIndexAdapter;
import org.apache.iceberg.index.vector.VectorIndexDefinition;
import org.apache.iceberg.index.vector.VectorIndexDefinition.Metric;
import org.apache.iceberg.index.vector.VectorIndexDefinition.VectorQuantizer;
import org.apache.iceberg.index.vector.VectorIndexDefinition.VectorStructure;
import org.apache.iceberg.index.vector.VectorIndexTypes;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class TestIndexCacheMemory {
  @BeforeAll
  static void registerVectorIndexType() {
    VectorIndexTypes.register();
  }

  @Test
  void independentPreparedStateDoesNotRetainContainer() throws ReflectiveOperationException {
    byte[] container = new byte[20_000];
    byte[] state = new byte[] {10};
    IndexAdapter.PreparedSearch prepared =
        new IndexAdapter.PreparedSearch() {
          @Override
          public List<IndexSearchResult> search(ByteBuffer query, int topK) {
            return List.of(IndexSearchResult.of("data", 7, state[0]));
          }

          @Override
          public long estimatedBytes() {
            return 80;
          }
        };
    ExecutorIndexCache.CachedFile loaded = ExecutorIndexCache.loaded(container, prepared);
    assertThat(reachable(loaded, container, new IdentityHashMap<>())).isFalse();
    assertThat(reachable(loaded, state, new IdentityHashMap<>())).isTrue();
    List<IndexSearchResult> results =
        loaded.searching(searcher -> searcher.search(ByteBuffer.allocate(0), 1));
    assertThat(results).extracting(IndexSearchResult::score).containsExactly(10.0);
    assertThat(loaded.bytes()).isGreaterThanOrEqualTo(80).isLessThan(1_024);
  }

  @Test
  void releasedInputSizeDoesNotChangePreparedCharge() {
    IndexAdapter.PreparedSearch prepared =
        new IndexAdapter.PreparedSearch() {
          @Override
          public List<IndexSearchResult> search(ByteBuffer query, int topK) {
            return List.of();
          }

          @Override
          public long estimatedBytes() {
            return 80;
          }
        };
    assertThat(ExecutorIndexCache.loaded(new byte[20_000], prepared).bytes())
        .isEqualTo(ExecutorIndexCache.loaded(new byte[100], prepared).bytes());
  }

  private static boolean reachable(
      Object root, Object target, IdentityHashMap<Object, Boolean> seen)
      throws ReflectiveOperationException {
    if (root == target) {
      return true;
    }
    if (root == null || seen.put(root, true) != null) {
      return false;
    }
    if (root instanceof Object[] values) {
      for (Object value : values) {
        if (reachable(value, target, seen)) {
          return true;
        }
      }
      return false;
    }
    // Inspect only Iceberg-owned objects; no JDK module opening or GC-dependent assertions.
    if (!root.getClass().getName().startsWith("org.apache.iceberg.")) {
      return false;
    }
    for (Class<?> type = root.getClass(); type != Object.class; type = type.getSuperclass()) {
      for (Field field : type.getDeclaredFields()) {
        if (!Modifier.isStatic(field.getModifiers()) && !field.getType().isPrimitive()) {
          field.setAccessible(true);
          if (reachable(field.get(root), target, seen)) {
            return true;
          }
        }
      }
    }
    return false;
  }

  @Test
  void unknownMemoryCannotBecomeFreeCapacity() {
    assertThatThrownBy(() -> IndexCacheMemory.residentBytes(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("memory is unknown");
  }

  @Test
  void diagnosticContainerLengthDoesNotOverflowResident() {
    IndexFileMemory memory = memory(Long.MAX_VALUE, 7, 80).toBuilder().modelVersion(2).build();
    assertThat(IndexCacheMemory.residentBytes(memory)).isGreaterThanOrEqualTo(80).isLessThan(1_024);
  }

  @Test
  void knownPreparedChargeDoesNotRequireContainerLength() {
    IndexFileMemory memory = memory(null, 7, 80).toBuilder().modelVersion(2).build();
    assertThat(IndexCacheMemory.residentBytes(memory)).isGreaterThanOrEqualTo(80).isLessThan(1_024);
  }

  @Test
  void legacyMemoryIsNotUsedAsCurrentCapacity() {
    assertThatThrownBy(() -> IndexCacheMemory.residentBytes(memory(7L, 7, 80)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("memory model");
  }

  @Test
  void loadedChargeCannotOverflowIntoNegativeOccupancy() {
    assertThatThrownBy(
            () ->
                ExecutorIndexCache.loaded(
                    new byte[1],
                    new IndexAdapter.PreparedSearch() {
                      @Override
                      public List<IndexSearchResult> search(ByteBuffer query, int topK) {
                        return List.of();
                      }

                      @Override
                      public long estimatedBytes() {
                        return Long.MAX_VALUE;
                      }
                    }))
        .isInstanceOf(ArithmeticException.class);
  }

  private static IndexFileMemory memory(Long fileBytes, long payloadBytes, long preparedBytes) {
    return IndexFileMemory.builder()
        .fileBytes(fileBytes)
        .payloadBytes(payloadBytes)
        .preparedBytes(preparedBytes)
        .modelVersion(1)
        .readerId("fixture")
        .runtimeProfile("fixture")
        .build();
  }

  @ParameterizedTest
  @CsvSource({
    "FLAT,NONE,none", "FLAT,NONE,zstd",
    "HNSW,NONE,none", "HNSW,NONE,zstd",
    "IVF,NONE,none", "IVF,NONE,zstd",
    "IVF,RABITQ,none", "IVF,RABITQ,zstd"
  })
  void manifestEstimateMatchesIndependentPreparedCharge(
      VectorStructure structure, VectorQuantizer quantizer, String codec, @TempDir Path tempDir)
      throws IOException {
    VectorIndexAdapter adapter =
        new VectorIndexAdapter(
            VectorIndexDefinition.builder()
                .fieldId(1)
                .dimension(16)
                .metric(Metric.EUCLIDEAN)
                .structure(structure)
                .quantizer(quantizer)
                .properties(
                    switch (structure) {
                      case FLAT -> Map.of();
                      case HNSW -> Map.of("max-m", "8");
                      case IVF -> Map.of("nlist", "4", "nprobe", "1");
                    })
                .build());
    List<IndexEntry> entries = new ArrayList<>();
    for (int row = 0; row < 100; row++) {
      float[] vector = new float[16];
      for (int dimension = 0; dimension < vector.length; dimension++) {
        vector[dimension] = (row + dimension) % 127;
      }
      entries.add(IndexEntry.of("data-" + row % 3, row, adapter.encodeVector(vector)));
    }

    String property = PuffinIndexFileContainer.INDEX_BLOB_COMPRESSION_PROPERTY;
    String previous = System.getProperty(property);
    String indexLocation = tempDir.resolve("index").toString();
    try (LocalFileIO fileIO = new LocalFileIO()) {
      IndexSnapshotWriter writer =
          new IndexSnapshotWriter(fileIO, new PuffinIndexFileContainer(), entries.size());
      try {
        System.setProperty(property, codec);
        List<IndexFile> written = writer.writeIndexFiles(indexLocation, 1, adapter, entries);
        String manifestList = writer.writeManifestList(indexLocation, 1, written);
        String manifest =
            IndexManifestList.read(fileIO.newInputFile(manifestList)).manifests().get(0).location();
        List<IndexFile> published = IndexManifest.read(fileIO.newInputFile(manifest)).indexFiles();
        assertThat(published).hasSize(1);
        IndexFile file = published.get(0);
        ExecutorIndexCache.CachedFile loaded =
            DistributedIndexSearch.loadFile(fileIO, file.location(), adapter);
        long expected = file.memory().preparedBytes() + 112;
        assertThat(file.memory().modelVersion()).isEqualTo(2);
        assertThat(loaded.bytes()).isEqualTo(expected);
        long planned = IndexCacheMemory.residentBytes(file.memory());
        assertThat(planned).isEqualTo(expected);
        assertThat(Math.addExact(file.memory().payloadBytes(), file.memory().preparedBytes()))
            .as("Blob-buffer charge is not the whole-container cache charge")
            .isNotEqualTo(planned);
        ExecutorIndexCache cache = ExecutorIndexCache.get();
        cache.file(
            file.location(), ExecutorIndexCache.Config.of(true, expected), "adapter", () -> loaded);
        assertThat(cache.stats().bytes()).isEqualTo(planned);
        assertThat(DistributedIndexSearch.workingSetBytes(fileIO, published, adapter))
            .isEqualTo(planned);
      } finally {
        try {
          writer.abortAttempt(indexLocation);
        } finally {
          if (previous == null) {
            System.clearProperty(property);
          } else {
            System.setProperty(property, previous);
          }
          ExecutorIndexCache.get().clear();
        }
      }
    }
  }
}
