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
package org.apache.iceberg.index.vector;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.jelmerk.knn.hnsw.HnswIndex;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;
import org.apache.iceberg.index.IndexAdapter;
import org.apache.iceberg.index.IndexEntry;
import org.apache.iceberg.index.vector.VectorIndexDefinition.Metric;
import org.apache.iceberg.index.vector.VectorIndexDefinition.VectorQuantizer;
import org.apache.iceberg.index.vector.VectorIndexDefinition.VectorStructure;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Pins the size a prepared structure reports.
 *
 * <p>The cache budgets on this number, and the fallback it replaced -- twice the payload -- was
 * wrong in both directions: too high for a quantized list, whose entries are a few bytes each, and
 * too low for a graph, whose connection lists cost about as much again as the vectors they index. A
 * budget derived from the wrong number is why the cache covered 7 of 13 files per executor.
 */
class TestPreparedStructureSize {

  private static final int DIMENSION = 16;

  @BeforeAll
  static void registerVectorIndexType() {
    VectorIndexTypes.register();
  }

  @Test
  void anUnquantizedListReportsMorePerEntryThanAQuantizedOne() {
    VectorIndexAdapter exact = adapter(VectorStructure.IVF, "NONE", nprobe());
    VectorIndexAdapter quantized = adapter(VectorStructure.IVF, "RABITQ", nprobe());

    long exactBytes = preparedBytes(exact, payload(exact, 400, false));
    long quantizedBytes = preparedBytes(quantized, payload(quantized, 400, false));

    // four bits per dimension is a sixteenth of the vector it replaced, so a single factor for
    // both is what made a derived budget too small for an index that is mostly quantized
    assertThat(quantizedBytes).isLessThan(exactBytes);
    assertThat(quantizedBytes).isPositive();
  }

  @Test
  void aQuantizedIndexStillReportsItsCentres() {
    VectorIndexAdapter quantized = adapter(VectorStructure.IVF, "RABITQ", nprobe());

    // the same entries in more lists means more centroids and more per-list centres, which the
    // entries alone do not account for
    long few = preparedBytes(quantized, payload(quantized, 400, false));
    long many = preparedBytes(quantized, payload(quantized, 400, true));

    assertThat(many).isGreaterThanOrEqualTo(few);
  }

  @Test
  void aFlatIndexReportsWhatItsEntriesCost() {
    VectorIndexAdapter flat = adapter(VectorStructure.FLAT, "NONE", ImmutableMap.of());
    ByteBuffer payload = payload(flat, 400, false);

    long reported = preparedBytes(flat, payload);

    // the entries are decoded and kept, so the structure is at least the payload it read
    assertThat(reported).isGreaterThanOrEqualTo(payload.remaining());
  }

  @Test
  void aGraphReportsMoreThanTheVectorsItIndexes() {
    VectorIndexAdapter graph =
        adapter(
            VectorStructure.HNSW, "NONE", ImmutableMap.of("max-m", "16", "ef-construction", "100"));
    ByteBuffer payload = payload(graph, 400, false);

    long reported = preparedBytes(graph, payload);
    long storedBytes = 400L * DIMENSION * Float.BYTES;

    // a node's connection lists are a comparable cost to its vector: reporting only the payload
    // leaves the cache under-charged for exactly the structure that costs the most
    assertThat(reported).isGreaterThan(storedBytes);
  }

  @Test
  void everyKernelReportsSomething() {
    for (VectorStructure structure : VectorStructure.values()) {
      VectorIndexAdapter adapter = adapter(structure, "NONE", defaultOptions(structure));
      long reported = preparedBytes(adapter, payload(adapter, 20, false));
      assertThat(reported).as("reported bytes of %s", structure).isPositive();
    }
  }

  @Test
  void stringsIncludeTheirBackingArrayAndUtf16Characters() {
    // 24-byte String object and a separate 16-byte array header, both aligned to 8 bytes.
    assertThat(PreparedStructureSize.string("a")).isGreaterThanOrEqualTo(48);
    assertThat(PreparedStructureSize.string("é".repeat(1024))).isGreaterThanOrEqualTo(1064);
    assertThat(PreparedStructureSize.string("中".repeat(1024))).isGreaterThanOrEqualTo(2088);
  }

  @Test
  void equalButSeparatelyRetainedStringsAreNotSharedObjects() {
    String first = new String(new char[] {'a'});
    String second = new String(new char[] {'a'});
    assertThat(PreparedStructureSize.distinctStrings(List.of(first, first, second)))
        .isGreaterThanOrEqualTo(96);
  }

  @Test
  void graphEstimateDoesNotGrowQuadraticallyWithNodes() {
    VectorIndexAdapter graph =
        sizedAdapter(
            VectorStructure.HNSW,
            VectorQuantizer.NONE,
            16,
            Map.of("max-m", "8", "ef-construction", "50"));
    long small = prepare(graph, 1000, 16, i -> "a").estimatedBytes();
    long large = prepare(graph, 2000, 16, i -> "a").estimatedBytes();
    assertThat(large)
        .as("doubling nodes in a bounded-degree graph")
        .isLessThanOrEqualTo(small * 5 / 2);
  }

  @Test
  void graphCountsLongUniquePaths() {
    VectorIndexAdapter graph =
        sizedAdapter(
            VectorStructure.HNSW,
            VectorQuantizer.NONE,
            16,
            Map.of("max-m", "8", "ef-construction", "50"));
    long shortPaths = prepare(graph, 100, 16, i -> "file-" + i).estimatedBytes();
    long longPaths = prepare(graph, 100, 16, i -> "a".repeat(4096) + i).estimatedBytes();
    assertThat(longPaths - shortPaths).isBetween(400_000L, 420_000L);
  }

  @Test
  void graphCountsSharedUtf16PathOnlyOnce() {
    VectorIndexAdapter graph =
        sizedAdapter(
            VectorStructure.HNSW,
            VectorQuantizer.NONE,
            16,
            Map.of("max-m", "8", "ef-construction", "50"));
    long latin1 = prepare(graph, 100, 16, i -> "é".repeat(1024)).estimatedBytes();
    long utf16 = prepare(graph, 100, 16, i -> "中".repeat(1024)).estimatedBytes();
    assertThat(utf16 - latin1).isBetween(1024L, 1040L);
  }

  @Test
  void normalGraphFitsAReasonableEntryBudget() {
    VectorIndexAdapter graph =
        sizedAdapter(
            VectorStructure.HNSW,
            VectorQuantizer.NONE,
            16,
            Map.of("max-m", "8", "ef-construction", "50"));
    // The former N-squared term alone charged 1.2 GB for these 10,000 nodes.
    assertThat(prepare(graph, 10_000, 16, i -> "data.parquet").estimatedBytes())
        .isBetween(640_000L, 256L * 1024 * 1024);
  }

  @Test
  void emptyIvfStillCountsOuterArrays() {
    VectorIndexAdapter ivf =
        sizedAdapter(
            VectorStructure.IVF, VectorQuantizer.RABITQ, 16, Map.of("nlist", "64", "nprobe", "1"));
    // Empty centroids, listCenters and listQuantizers each retain a 16-byte array header.
    assertThat(prepare(ivf, 0, 16, i -> "a").estimatedBytes()).isGreaterThanOrEqualTo(48);
  }

  @Test
  void graphChargesAllocatedCapacityRatherThanOnlyMappedNodes() throws Exception {
    VectorIndexAdapter graph =
        sizedAdapter(
            VectorStructure.HNSW,
            VectorQuantizer.NONE,
            16,
            Map.of("max-m", "8", "ef-construction", "50"));
    IndexAdapter.PreparedSearch prepared = prepare(graph, 100, 16, i -> "a");
    HnswIndex<?, ?, ?, ?> index = (HnswIndex<?, ?, ?, ?>) field(prepared, "index");
    index.resize(100_000);
    ByteArrayOutputStream serialized = new ByteArrayOutputStream();
    index.save(serialized);
    ByteArrayOutputStream payload = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(payload)) {
      VectorPayloads.writeHeader(out, HnswlibKernel.NAME, 1);
      out.writeInt(serialized.size());
      out.write(serialized.toByteArray());
      out.writeInt(1);
      out.writeUTF("a");
      out.writeInt(100);
      for (int i = 0; i < 100; i++) {
        out.writeInt(0);
        out.writeLong(i);
      }
    }
    // Node slots alone cost 400 KB; visited buffers also use maxItemCount, not mapping count.
    assertThat(graph.prepareAuxiliary(ByteBuffer.wrap(payload.toByteArray())).estimatedBytes())
        .isGreaterThanOrEqualTo(
            400_000L + (Runtime.getRuntime().availableProcessors() + 1L) * 12_504L);
  }

  @Test
  void quantizedEstimateCoversRetainedArraysAndEntryObjects() throws Exception {
    VectorIndexAdapter quantized =
        sizedAdapter(
            VectorStructure.IVF,
            VectorQuantizer.RABITQ,
            1024,
            Map.of("nlist", "64", "nprobe", "1", "rabitq-bits", "1"));
    IndexAdapter.PreparedSearch prepared = prepare(quantized, 64, 1024, i -> "a");
    Object loaded = field(prepared, "loaded");
    Map<Object, Boolean> seen = new IdentityHashMap<>();
    long lowerBound =
        retainedArrays(field(loaded, "centroids"), seen)
            + retainedArrays(field(loaded, "listCenters"), seen);
    Object[] quantizers = (Object[]) field(loaded, "listQuantizers");
    lowerBound += retainedArrayBytes(quantizers.length * 4L);
    for (Object q : quantizers) {
      lowerBound += 48; // object with three ints, a long and two array references
      lowerBound += retainedArrays(field(q, "center"), seen);
      lowerBound += retainedArrays(field(q, "signs"), seen);
    }
    List<?> lists = (List<?>) field(loaded, "lists");
    lowerBound += 32 + retainedArrayBytes(lists.size() * 4L);
    for (Object list : lists) {
      List<?> members = (List<?>) list;
      lowerBound += 32 + retainedArrayBytes(members.size() * 4L);
      for (Object entry : members) {
        lowerBound += 40; // row position and four references, including null fields
        lowerBound += retainedArrays(field(entry, "code"), seen);
        lowerBound += retainedArrays(field(entry, "factors"), seen);
      }
    }
    assertThat(prepared.estimatedBytes())
        .as("retained arrays and objects, excluding paths and searcher")
        .isGreaterThanOrEqualTo(lowerBound);
  }

  @Test
  void ivfEstimateCoversLongDataFileString() {
    VectorIndexAdapter ivf =
        sizedAdapter(
            VectorStructure.IVF, VectorQuantizer.NONE, 16, Map.of("nlist", "1", "nprobe", "1"));
    assertThat(prepare(ivf, 1, 16, i -> "a".repeat(1024)).estimatedBytes())
        .isGreaterThanOrEqualTo(1064);
  }

  @Test
  void flatDoesNotChargeSharedPathOncePerEntry() {
    VectorIndexAdapter flat =
        sizedAdapter(VectorStructure.FLAT, VectorQuantizer.NONE, 16, Map.of());
    long shortPath = prepare(flat, 1000, 16, i -> "a").estimatedBytes();
    long longPath = prepare(flat, 1000, 16, i -> "a".repeat(1024)).estimatedBytes();
    assertThat(longPath - shortPath)
        .as("one dictionary String, not 1000 copies")
        .isBetween(1000L, 1040L);
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1})
  void sparseQuantizedIvfCoversAllRetainedPayloadArrays(int count) throws Exception {
    VectorIndexAdapter quantized =
        sizedAdapter(
            VectorStructure.IVF, VectorQuantizer.RABITQ, 1024, Map.of("nlist", "1", "nprobe", "1"));
    IndexAdapter.PreparedSearch prepared = prepare(quantized, count, 1024, i -> "a");
    long lowerBound = retainedPayloadArrays(prepared, new IdentityHashMap<>());
    assertThat(prepared.estimatedBytes())
        .as("All reachable payload arrays, including any retained kernel state")
        .isGreaterThanOrEqualTo(lowerBound);
  }

  private static long retainedPayloadArrays(Object instance, Map<Object, Boolean> seen)
      throws Exception {
    if (instance == null || seen.put(instance, true) != null) {
      return 0;
    }
    if (instance instanceof float[]) {
      return retainedArrayBytes(((float[]) instance).length * 4L);
    }
    if (instance instanceof byte[]) {
      return retainedArrayBytes(((byte[]) instance).length);
    }
    if (instance instanceof Object[]) {
      Object[] values = (Object[]) instance;
      long bytes = retainedArrayBytes(values.length * 4L);
      for (Object value : values) {
        bytes += retainedPayloadArrays(value, seen);
      }
      return bytes;
    }
    if (instance instanceof Iterable<?>) {
      long bytes = 0;
      for (Object value : (Iterable<?>) instance) {
        bytes += retainedPayloadArrays(value, seen);
      }
      return bytes;
    }
    if (!instance.getClass().getPackageName().equals("org.apache.iceberg.index.vector")) {
      return 0;
    }
    long bytes = 0;
    for (Field field : instance.getClass().getDeclaredFields()) {
      if (!Modifier.isStatic(field.getModifiers()) && !field.getType().isPrimitive()) {
        field.setAccessible(true);
        bytes += retainedPayloadArrays(field.get(instance), seen);
      }
    }
    return bytes;
  }

  private static Object field(Object instance, String name) throws Exception {
    Field field = instance.getClass().getDeclaredField(name);
    field.setAccessible(true);
    return field.get(instance);
  }

  private static long retainedArrayBytes(long contents) {
    return (16 + contents + 7) / 8 * 8;
  }

  private static long retainedArrays(Object array, Map<Object, Boolean> seen) {
    if (array == null || seen.put(array, true) != null) {
      return 0;
    }
    if (array instanceof float[]) {
      return retainedArrayBytes(((float[]) array).length * 4L);
    }
    if (array instanceof byte[]) {
      return retainedArrayBytes(((byte[]) array).length);
    }
    Object[] children = (Object[]) array;
    long result = retainedArrayBytes(children.length * 4L);
    for (Object child : children) {
      result += retainedArrays(child, seen);
    }
    return result;
  }

  private static VectorIndexAdapter sizedAdapter(
      VectorStructure structure,
      VectorQuantizer quantizer,
      int dimension,
      Map<String, String> options) {
    return new VectorIndexAdapter(
        VectorIndexDefinition.builder()
            .fieldId(1)
            .dimension(dimension)
            .metric(Metric.EUCLIDEAN)
            .structure(structure)
            .quantizer(quantizer)
            .properties(options)
            .build());
  }

  private static IndexAdapter.PreparedSearch prepare(
      VectorIndexAdapter adapter, int count, int dimension, IntFunction<String> path) {
    List<IndexEntry> entries = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      float[] vector = new float[dimension];
      for (int d = 0; d < dimension; d++) {
        vector[d] = (i + d) % 127;
      }
      entries.add(IndexEntry.of(path.apply(i), i, adapter.encodeVector(vector)));
    }
    ByteBuffer payload =
        adapter.writesAuxiliaryFiles()
            ? adapter.encodeAuxiliaryPayload(entries)
            : adapter.encodeEntries(entries);
    return adapter.writesAuxiliaryFiles()
        ? adapter.prepareAuxiliary(payload)
        : adapter.prepare(payload);
  }

  private static long preparedBytes(VectorIndexAdapter adapter, ByteBuffer payload) {
    IndexAdapter.PreparedSearch search =
        adapter.writesAuxiliaryFiles()
            ? adapter.prepareAuxiliary(payload)
            : adapter.prepare(payload);
    return search.estimatedBytes();
  }

  private static ImmutableMap<String, String> nprobe() {
    return ImmutableMap.of("nprobe", "4");
  }

  private static ImmutableMap<String, String> defaultOptions(VectorStructure structure) {
    return structure == VectorStructure.HNSW
        ? ImmutableMap.of("max-m", "8", "ef-construction", "50")
        : structure == VectorStructure.IVF ? ImmutableMap.of("nprobe", "2") : ImmutableMap.of();
  }

  private static VectorIndexAdapter adapter(
      VectorStructure structure, String quantizer, ImmutableMap<String, String> options) {
    return new VectorIndexAdapter(
        VectorIndexDefinition.builder()
            .fieldId(1)
            .dimension(DIMENSION)
            .metric(Metric.EUCLIDEAN)
            .structure(structure)
            .quantizer(VectorQuantizer.valueOf(quantizer))
            .properties(options)
            .build());
  }

  /** An index of {@code count} vectors, written the way a build writes one. */
  private static ByteBuffer payload(VectorIndexAdapter adapter, int count, boolean manyLists) {
    List<IndexEntry> entries = Lists.newArrayListWithCapacity(count);
    for (int i = 0; i < count; i++) {
      float[] vector = new float[DIMENSION];
      for (int d = 0; d < DIMENSION; d++) {
        vector[d] = (i + d) % 7;
      }

      entries.add(
          IndexEntry.of(
              "data-" + (manyLists ? i : i % 4) + ".parquet", i, adapter.encodeVector(vector)));
    }

    return adapter.writesAuxiliaryFiles()
        ? adapter.encodeAuxiliaryPayload(entries)
        : adapter.encodeEntries(entries);
  }
}
