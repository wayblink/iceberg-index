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
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.apache.iceberg.index.spark.IndexCacheMetadata.OwnedMap;
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
import org.junit.jupiter.params.provider.ValueSource;

class TestIndexCacheMemory {
  @BeforeAll
  static void registerVectorIndexType() {
    VectorIndexTypes.register();
  }

  @Test
  void cacheRootChargeIncludesLongKeyAndOwnerStorage() throws ReflectiveOperationException {
    VectorIndexAdapter adapter =
        new VectorIndexAdapter(
            VectorIndexDefinition.builder()
                .fieldId(1)
                .dimension(2)
                .metric(Metric.EUCLIDEAN)
                .structure(VectorStructure.FLAT)
                .build());
    IndexAdapter.PreparedSearch prepared = adapter.prepare(adapter.encodeEntries(List.of()));
    ExecutorIndexCache cache = ExecutorIndexCache.get();
    String owner = new String("owner".repeat(2_000).toCharArray());
    String location = "file".repeat(2_000);
    cache.clear();
    try {
      cache.file(
          location,
          ExecutorIndexCache.Config.of(true, 100_000).withOwner(owner),
          DistributedIndexSearch.adapterKey(adapter),
          () -> ExecutorIndexCache.loaded(0, prepared).withContext(adapter));
      Field field = ExecutorIndexCache.class.getDeclaredField("protectedFiles");
      field.setAccessible(true);
      Map<?, ?> roots = (Map<?, ?>) field.get(cache);
      assertThat(roots).hasSize(1);
      IdentityHashMap<Object, Boolean> seen = new IdentityHashMap<>();
      long lower = 0;
      for (Map.Entry<?, ?> entry : roots.entrySet()) {
        ExecutorIndexCache.CachedFile file = (ExecutorIndexCache.CachedFile) entry.getValue();
        // Independently checked lower bounds: empty FLAT state, wrapper/lock and one map node.
        // The receipt and ownership collections are deliberately omitted, so this is a lower
        // bound, not an exact retained-heap measurement or the production charge formula.
        lower += 112 + 112 + 40;
        for (String value : List.of((String) entry.getKey(), file.owner())) {
          if (seen.put(value, Boolean.TRUE) == null) {
            long width = value.chars().anyMatch(character -> character > 255) ? 2 : 1;
            lower += 24 + ((16 + width * value.length() + 7) / 8) * 8;
          }
        }
      }
      assertThat(cache.chargeSnapshot().residentBytes())
          .as("Charge must cover the independently counted cache-root lower bound")
          .isGreaterThanOrEqualTo(lower);
    } finally {
      cache.clear();
    }
  }

  @Test
  void sharedOwnerUsesOneOwnedTextAndClearReleasesTables() throws ReflectiveOperationException {
    ExecutorIndexCache cache = ExecutorIndexCache.get();
    cache.clear();
    String owner = "共享表".repeat(2_000);
    IndexAdapter.PreparedSearch prepared = (query, topK) -> List.of();
    Constructor<ExecutorIndexCache> constructor = ExecutorIndexCache.class.getDeclaredConstructor();
    constructor.setAccessible(true);
    ExecutorIndexCache measuring = constructor.newInstance();
    long budget;
    try {
      for (int i = 0; i < 100; i++) {
        measuring.file(
            "路径" + i,
            ExecutorIndexCache.Config.of(true, Long.MAX_VALUE).withOwner(owner),
            "reader",
            () -> ExecutorIndexCache.loaded(new byte[0], prepared, 128));
      }
      budget = measuring.shape().effectiveAdmissionBytes(Set.of(owner + "|reader|路径99"));
    } finally {
      measuring.clear();
    }
    try {
      for (int i = 0; i < 100; i++) {
        cache.file(
            "路径" + i,
            ExecutorIndexCache.Config.of(true, budget).withOwner(new String(owner.toCharArray())),
            "reader",
            () -> ExecutorIndexCache.loaded(new byte[0], prepared, 128));
      }
      Field field = ExecutorIndexCache.class.getDeclaredField("protectedFiles");
      field.setAccessible(true);
      Map<?, ?> roots = (Map<?, ?>) field.get(cache);
      assertThat(roots).hasSize(100);
      Object owned = null;
      for (Object value : roots.values()) {
        String current = ((ExecutorIndexCache.CachedFile) value).owner();
        assertThat(current).isEqualTo(owner).isNotSameAs(owner);
        if (owned != null) {
          assertThat(current).isSameAs(owned);
        }
        owned = current;
      }
      assertThat(cache.chargeSnapshot().residentBytes()).isGreaterThan(40_000);
      cache.clear();
      assertThat(cache.chargeSnapshot().residentBytes()).isZero();
      assertThat(cache.chargeSnapshot().controlBytes()).isZero();
      Field table = roots.getClass().getDeclaredField("table");
      table.setAccessible(true);
      assertThat((Object[]) table.get(roots)).isEmpty();
    } finally {
      cache.clear();
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"latin", "中文"})
  void metadataSnapshotCoversIndependentRoots(String alphabet) throws ReflectiveOperationException {
    ExecutorIndexCache cache = ExecutorIndexCache.get();
    cache.clear();
    String owner = alphabet.repeat(1_000);
    String key = owner + "|reader|" + alphabet.repeat(2_000);
    try {
      cache.applyTarget(alphabet + "#index", 1, Set.of(key));
      cache.installManaged(
          alphabet + "#index",
          1,
          key,
          ExecutorIndexCache.Config.of(true, 1_000_000).withOwner(owner),
          () -> ExecutorIndexCache.loaded(new byte[0], (query, topK) -> List.of(), 128));
      cache.recordIndexAttribution(alphabet + "#association", Set.of(key));
      IdentityHashMap<Object, Boolean> seen = new IdentityHashMap<>();
      long lower = 0;
      for (String name :
          List.of(
              "probation",
              "protectedFiles",
              "attribution",
              "bytesByOwner",
              "entriesByOwner",
              "ownership",
              "asked",
              "loading",
              "reservations",
              "reservedByOwner",
              "texts")) {
        Field field = ExecutorIndexCache.class.getDeclaredField(name);
        field.setAccessible(true);
        lower += metadataRootLowerBound(field.get(cache), seen);
      }
      assertThat(cache.chargeSnapshot().usedBytes()).isGreaterThanOrEqualTo(lower);
    } finally {
      cache.clear();
    }
  }

  // Independent cache-root oracle: explicit owned roots, actual fields and array lengths. JDK
  // internals and unproved prepared representations are omitted, never opened reflectively.
  private static long metadataRootLowerBound(Object value, IdentityHashMap<Object, Boolean> seen)
      throws ReflectiveOperationException {
    if (value == null || seen.put(value, Boolean.TRUE) != null) {
      return 0;
    }
    if (value instanceof String text) {
      long width = text.chars().anyMatch(character -> character > 255) ? 2 : 1;
      return 24 + ((16 + width * text.length() + 7) / 8) * 8;
    }
    Class<?> type = value.getClass();
    if (type.isArray()) {
      int length = Array.getLength(value);
      if (length == 0) {
        return 0; // The shared empty array is part of the explicit empty-manager baseline.
      }
      Class<?> component = type.getComponentType();
      long width =
          component == byte.class || component == boolean.class
              ? 1
              : component == char.class || component == short.class
                  ? 2
                  : component == long.class || component == double.class ? 8 : 4;
      long bytes = ((16 + width * length + 7) / 8) * 8;
      if (!component.isPrimitive()) {
        for (int i = 0; i < length; i++) {
          bytes += metadataRootLowerBound(Array.get(value, i), seen);
        }
      }
      return bytes;
    }
    if (!type.getName().startsWith("org.apache.iceberg.index.spark.IndexCacheMetadata$")
        && type != IndexCacheOwnership.class
        && !type.getName().startsWith("org.apache.iceberg.index.spark.IndexCacheOwnership$")
        && type != ExecutorIndexCache.CachedFile.class) {
      return 0;
    }
    if (value instanceof Map<?, ?> map && map.isEmpty()
        || value instanceof Set<?> set && set.isEmpty()) {
      return 0;
    }
    long fields = 12;
    long children = 0;
    for (Class<?> current = type; current != Object.class; current = current.getSuperclass()) {
      for (Field field : current.getDeclaredFields()) {
        if (Modifier.isStatic(field.getModifiers())) {
          continue;
        }
        Class<?> fieldType = field.getType();
        fields +=
            fieldType == byte.class || fieldType == boolean.class
                ? 1
                : fieldType == char.class || fieldType == short.class
                    ? 2
                    : fieldType == long.class || fieldType == double.class ? 8 : 4;
        if (!fieldType.isPrimitive() && current.getName().startsWith("org.apache.iceberg.")) {
          field.setAccessible(true);
          children += metadataRootLowerBound(field.get(value), seen);
        }
      }
    }
    return ((fields + 7) / 8) * 8 + children;
  }

  @Test
  void optionalAttributionRejectsGrowthWithoutRetainingTextOrTable() {
    ExecutorIndexCache cache = ExecutorIndexCache.get();
    cache.clear();
    try {
      cache.file(
          "file",
          ExecutorIndexCache.Config.of(true, 100_000).withOwner("owner"),
          "reader",
          () -> ExecutorIndexCache.loaded(new byte[0], (query, topK) -> List.of(), 128));
      cache.recordIndexAttribution("small", Set.of("owner|reader|file"));
      long before = cache.chargeSnapshot().usedBytes();
      cache.recordIndexAttribution("large".repeat(100_000), Set.of("owner|reader|file"));
      assertThat(cache.indexAttribution().get("owner|reader|file")).containsExactly("small");
      assertThat(cache.chargeSnapshot().usedBytes()).isEqualTo(before);
    } finally {
      cache.clear();
    }
  }

  @Test
  void metadataProjectionDoesNotReestimateLivePreparedState() {
    ExecutorIndexCache cache = ExecutorIndexCache.get();
    cache.clear();
    AtomicInteger estimates = new AtomicInteger();
    IndexAdapter.PreparedSearch prepared =
        new IndexAdapter.PreparedSearch() {
          @Override
          public List<IndexSearchResult> search(ByteBuffer query, int topK) {
            return List.of();
          }

          @Override
          public long estimatedBytes() {
            estimates.incrementAndGet();
            return 128;
          }
        };
    try {
      cache.file(
          "file",
          ExecutorIndexCache.Config.of(true, 100_000).withOwner("owner"),
          "reader",
          () -> ExecutorIndexCache.loaded(0, prepared));
      int published = estimates.get();
      cache.chargeSnapshot();
      cache.chargeSnapshot();
      cache.recordIndexAttribution("index", Set.of("owner|reader|file"));
      assertThat(estimates.get()).isEqualTo(published);
    } finally {
      cache.clear();
    }
  }

  @Test
  void eachExecutorOwnsItsOwnSharedOwnerCopy() throws ReflectiveOperationException {
    Constructor<ExecutorIndexCache> constructor = ExecutorIndexCache.class.getDeclaredConstructor();
    constructor.setAccessible(true);
    ExecutorIndexCache first = constructor.newInstance();
    ExecutorIndexCache second = constructor.newInstance();
    String owner = "共享owner".repeat(1_000);
    ExecutorIndexCache.Config config = ExecutorIndexCache.Config.of(true, 100_000).withOwner(owner);
    try {
      for (ExecutorIndexCache cache : List.of(first, second)) {
        for (String location : List.of("first", "second")) {
          cache.file(
              location,
              config,
              "reader",
              () -> ExecutorIndexCache.loaded(new byte[0], (query, topK) -> List.of(), 128));
        }
      }
      Field field = ExecutorIndexCache.class.getDeclaredField("protectedFiles");
      field.setAccessible(true);
      Map<?, ?> firstFiles = (Map<?, ?>) field.get(first);
      Map<?, ?> secondFiles = (Map<?, ?>) field.get(second);
      String firstOwner = null;
      String secondOwner = null;
      for (Object file : firstFiles.values()) {
        String value = ((ExecutorIndexCache.CachedFile) file).owner();
        if (firstOwner != null) {
          assertThat(value).isSameAs(firstOwner);
        }
        firstOwner = value;
      }
      for (Object file : secondFiles.values()) {
        String value = ((ExecutorIndexCache.CachedFile) file).owner();
        if (secondOwner != null) {
          assertThat(value).isSameAs(secondOwner);
        }
        secondOwner = value;
      }
      assertThat(firstOwner).isEqualTo(secondOwner).isNotSameAs(secondOwner);
      long ownerLower = 24 + ((16 + 2L * owner.length() + 7) / 8) * 8;
      assertThat(first.chargeSnapshot().usedBytes() + second.chargeSnapshot().usedBytes())
          .isGreaterThanOrEqualTo(2 * ownerLower);
    } finally {
      first.clear();
      second.clear();
    }
  }

  @Test
  void lastLeaseReleasesResidentRootsButRetainedPolicyRemainsCharged() {
    ExecutorIndexCache cache = ExecutorIndexCache.get();
    cache.clear();
    String key = "owner|reader|file";
    try {
      cache.applyTarget("index", 1, Set.of(key));
      cache.installManaged(
          "index",
          1,
          key,
          ExecutorIndexCache.Config.of(true, 100_000).withOwner("owner"),
          () -> ExecutorIndexCache.loaded(new byte[0], (query, topK) -> List.of(), 128));
      ExecutorIndexCache.ChargeSnapshot before = cache.chargeSnapshot();
      try (ExecutorIndexCache.Lease lease = cache.acquireManaged("index", 1, key)) {
        assertThat(lease).isNotNull();
        cache.releaseIndex("index", 2);
        ExecutorIndexCache.ChargeSnapshot held = cache.chargeSnapshot();
        assertThat(held.holders()).isEqualTo(1);
        assertThat(held.residentBytes()).isPositive();
        assertThat(held.usedBytes()).isEqualTo(held.residentBytes() + held.controlBytes());
        assertThat(held.releasingBytes()).isPositive().isLessThanOrEqualTo(held.usedBytes());
      }
      ExecutorIndexCache.ChargeSnapshot after = cache.chargeSnapshot();
      assertThat(after.version()).isGreaterThan(before.version());
      assertThat(after.residentBytes()).isZero();
      assertThat(after.controlBytes()).isPositive();
      assertThat(after.usedBytes()).isEqualTo(after.controlBytes());
      assertThat(after.groupRefs()).isZero();
      assertThat(cache.stats().bytes()).isZero();
    } finally {
      cache.clear();
    }
  }

  @Test
  void metadataMapPreservesEntryValuesThroughResizeAndRemoval() {
    OwnedMap<String, Long> owned = new OwnedMap<>();
    Map<String, Long> reference = new LinkedHashMap<>();
    for (int i = 0; i < 100; i++) {
      owned.put("key" + i, (long) i);
      reference.put("key" + i, (long) i);
    }
    for (int i = 0; i < 90; i++) {
      owned.remove("key" + i);
      reference.remove("key" + i);
    }
    assertThat(owned.entrySet()).containsAll(reference.entrySet());
    assertThat(owned.hashCode()).isEqualTo(reference.hashCode());
    owned.clear();
    assertThat(owned.entrySet()).isEmpty();
  }

  @Test
  void longMetadataCannotBeAdmittedUsingOnlyThePreparedQuote() {
    VectorIndexAdapter adapter =
        new VectorIndexAdapter(
            VectorIndexDefinition.builder()
                .fieldId(1)
                .dimension(2)
                .metric(Metric.EUCLIDEAN)
                .structure(VectorStructure.FLAT)
                .build());
    IndexAdapter.PreparedSearch prepared = adapter.prepare(adapter.encodeEntries(List.of()));
    ExecutorIndexCache cache = ExecutorIndexCache.get();
    String owner = new String("owner".repeat(2_000).toCharArray());
    String location = "file".repeat(2_000);
    cache.clear();
    try {
      ExecutorIndexCache.CachedFile result =
          cache.file(
              location,
              ExecutorIndexCache.Config.of(true, 1_024).withOwner(owner),
              DistributedIndexSearch.adapterKey(adapter),
              () -> ExecutorIndexCache.loaded(0, prepared).withContext(adapter));
      assertThat(cache.stats().entries())
          .as("A prepared quote below budget cannot admit metadata above budget")
          .isZero();
      List<IndexSearchResult> hits =
          result.searching(
              searcher ->
                  searcher.search(ByteBuffer.wrap(adapter.encodeVector(new float[] {1, 0})), 1));
      assertThat(hits).isEmpty();
    } finally {
      cache.clear();
    }
  }

  @Test
  void ownerlessWorkingSetIsUnknownEvenWithoutPayloadResolution() {
    assertThat(DistributedIndexSearch.workingSetBytes(null, List.of(), null)).isEqualTo(-1);
  }

  @Test
  void nodeProjectionSharesOwnerButChargesAnotherOwnership() {
    String owner = "table-owner".repeat(200);
    IndexFileMemory descriptor = memory(null, 7, 128).toBuilder().modelVersion(2).build();
    IndexCacheProtocol.File first =
        IndexCacheProtocol.File.builder()
            .location("file1")
            .cacheKey("key1")
            .owner(owner)
            .memory(descriptor)
            .estimatedBytes(IndexCacheMemory.residentBytes(descriptor))
            .build();
    IndexCacheProtocol.File second =
        IndexCacheProtocol.File.builder()
            .location("file2")
            .cacheKey("key2")
            .owner(owner)
            .memory(descriptor)
            .estimatedBytes(IndexCacheMemory.residentBytes(descriptor))
            .build();
    ExecutorIndexCache.Shape empty = ExecutorIndexCache.Shape.empty();
    ExecutorIndexCache.Shape one = empty.project("index", 1, List.of(first));
    ExecutorIndexCache.Shape shared = one.project("index", 1, List.of(second));
    ExecutorIndexCache.Shape together = empty.project("index", 1, List.of(first, second));
    assertThat(shared.charge().usedBytes()).isEqualTo(together.charge().usedBytes());
    assertThat(shared.charge().usedBytes())
        .isLessThan(
            one.charge().usedBytes()
                + empty.project("index", 1, List.of(second)).charge().usedBytes());
    ExecutorIndexCache.Shape newOwnership = one.project("another-index", 1, List.of(first));
    assertThat(newOwnership.charge().usedBytes()).isGreaterThan(one.charge().usedBytes());
    assertThat(newOwnership.charge().residentBytes())
        .isGreaterThanOrEqualTo(one.charge().residentBytes())
        .isLessThan(one.charge().residentBytes() + descriptor.preparedBytes());
    assertThat(newOwnership.holders()).hasSize(1);
    assertThat(newOwnership.charge().groupRefs()).isEqualTo(2);
    assertThat(one.charge().groupRefs()).isEqualTo(1);
  }

  @Test
  void necessaryMetadataFailurePreservesTargetAndLease() {
    ExecutorIndexCache cache = ExecutorIndexCache.get();
    cache.clear();
    String key = "owner|reader|file";
    long budget = 4_096;
    try {
      cache.applyTarget("index", 1, Set.of(key));
      cache.installManaged(
          "index",
          1,
          key,
          ExecutorIndexCache.Config.of(true, budget).withOwner("owner"),
          () -> ExecutorIndexCache.loaded(new byte[0], (query, topK) -> List.of(), 128));
      try (ExecutorIndexCache.Lease lease = cache.acquireManaged("index", 1, key)) {
        assertThat(lease).isNotNull();
        ExecutorIndexCache.ChargeSnapshot before = cache.chargeSnapshot();
        cache.applyTarget("index", 2, Set.of("large".repeat(4_000)));
        cache.addManagedKeys("index", 1, Set.of("large".repeat(4_000)));
        cache.applyTarget("新owner".repeat(4_000), 1, Set.of(key));
        ExecutorIndexCache.ChargeSnapshot after = cache.chargeSnapshot();
        assertThat(after.usedBytes()).isEqualTo(before.usedBytes());
        assertThat(after.groupRefs()).isEqualTo(before.groupRefs());
        assertThat(after.holders()).isEqualTo(1);
        assertThat(after.usedBytes() + after.reservedBytes()).isLessThanOrEqualTo(budget);
        try (ExecutorIndexCache.Lease current = cache.acquireManaged("index", 1, key)) {
          assertThat(current).isNotNull();
        }
        cache.releaseIndex("index", 2);
        assertThat(cache.stats().bytes()).isPositive();
      }
      assertThat(cache.stats().bytes()).isZero();
      assertThat(cache.chargeSnapshot().usedBytes()).isLessThanOrEqualTo(budget);
    } finally {
      cache.clear();
    }
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
        long expected = IndexCacheMemory.residentBytes(file.memory());
        assertThat(file.memory().modelVersion()).isEqualTo(2);
        assertThat(loaded.bytes()).isEqualTo(expected);
        long planned = IndexCacheMemory.residentBytes(file.memory());
        assertThat(planned).isEqualTo(expected);
        assertThat(Math.addExact(file.memory().payloadBytes(), file.memory().preparedBytes()))
            .as("Blob-buffer charge is not the whole-container cache charge")
            .isNotEqualTo(planned);
        String owner = "table";
        String key = DistributedIndexSearch.adapterKey(adapter);
        long plannedSet = DistributedIndexSearch.workingSetBytes(fileIO, published, adapter, owner);
        IndexCacheProtocol.File prospect =
            IndexCacheProtocol.File.builder()
                .location(file.location())
                .owner(owner)
                .cacheKey(DistributedIndexSearch.cacheKey(owner, key, file.location()))
                .memory(file.memory())
                .estimatedBytes(planned)
                .build();
        ExecutorIndexCache.ChargeSnapshot admission =
            ExecutorIndexCache.Shape.empty().projectAdmission(null, 0, List.of(prospect)).charge();
        long capacity = Math.addExact(admission.usedBytes(), admission.reservedBytes());
        assertThat(plannedSet).isEqualTo(admission.residentBytes()).isGreaterThan(planned);
        ExecutorIndexCache cache = ExecutorIndexCache.get();
        cache.file(
            file.location(),
            ExecutorIndexCache.Config.of(true, capacity).withOwner(owner),
            key,
            () -> loaded);
        assertThat(cache.stats().entries()).isEqualTo(1);
        assertThat(cache.stats().bytes()).isEqualTo(plannedSet);
        assertThat(cache.chargeSnapshot().usedBytes()).isLessThanOrEqualTo(capacity);
        assertThat(DistributedIndexSearch.workingSetBytes(fileIO, published, adapter))
            .isEqualTo(-1);
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

  @Test
  void aRecordWrittenUnderAnotherProcessorCountIsReestimatedRatherThanRefused() {
    VectorIndexAdapter adapter =
        new VectorIndexAdapter(
            VectorIndexDefinition.builder()
                .fieldId(1)
                .dimension(2)
                .metric(Metric.EUCLIDEAN)
                .structure(VectorStructure.FLAT)
                .build());
    IndexFileMemory current = IndexCacheMemory.currentDescriptor(adapter);
    assertThat(current).isNotNull();

    // A JVM with another processor count writes the same layout and a different processor number.
    // That number decides which prepared state may be shared, not whether the record describes the
    // file, so it must not read as a foreign runtime.
    IndexFileMemory otherProcessors =
        current.toBuilder()
            .runtimeProfile(current.runtimeProfile().replaceAll("processors=\\d+", "processors=1"))
            .build();

    assertThat(IndexCacheMemory.compatibleModel(otherProcessors, adapter)).isTrue();
    assertThat(IndexCacheMemory.compatible(otherProcessors, adapter))
        .as("a resident copy is still reused only under the profile it was prepared with")
        .isFalse();
    assertThat(
            IndexCacheMemory.compatibleModel(
                current.toBuilder().readerId("another-reader").build(), adapter))
        .as("a record written by another reader cannot be re-estimated")
        .isFalse();
  }
}
