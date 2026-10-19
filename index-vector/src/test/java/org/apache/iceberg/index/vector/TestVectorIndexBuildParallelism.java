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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.iceberg.index.IndexEntry;
import org.apache.iceberg.index.IndexSearchResult;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

/**
 * What building an HNSW payload on a thread pool does and does not change.
 *
 * <p>Building the graph is the expensive part of a build, and inserting the entries one at a time
 * only uses one core, so a caller with cores to spare asks for several threads. That splits the
 * insertions across a pool, which changes the order they happen in, so what has to stay true is
 * that a hit still leads back to the entry it came from: a search resolves a hit by the id the
 * entry was given, not by where the graph happened to put it, and this checks that survives a
 * parallel build.
 */
class TestVectorIndexBuildParallelism {

  private static final int DIMENSION = 8;
  private static final int ENTRIES = 500;

  @Test
  public void findsEveryEntryInAnIndexBuiltOnAThreadPool() {
    VectorIndexAdapter adapter = adapter();
    List<IndexEntry> entries = entries(adapter);

    for (int threads : new int[] {1, 2, 4, 16}) {
      ByteBuffer payload = adapter.encodeAuxiliaryPayload(entries, threads);

      for (int i = 0; i < ENTRIES; i++) {
        List<IndexSearchResult> results =
            adapter.searchAuxiliary(payload, adapter.encodeQuery(vector(i)), 1);
        assertThat(results).as("a hit at all, with %s threads", threads).hasSize(1);
        assertThat(results.get(0).dataFile())
            .as("the data file of entry %s, with %s threads", i, threads)
            .isEqualTo("data-" + i / 100 + ".parquet");
        assertThat(results.get(0).rowPosition())
            .as("the position of entry %s, with %s threads", i, threads)
            .isEqualTo(i);
      }
    }
  }

  @Test
  public void rejectsAParallelismBelowOne() {
    VectorIndexAdapter adapter = adapter();
    assertThatThrownBy(() -> adapter.encodeAuxiliaryPayload(entries(adapter), 0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Parallelism must be at least one");
  }

  @Test
  public void buildsWhenAskedForMoreThreadsThanThereAreEntries() {
    VectorIndexAdapter adapter = adapter();
    List<IndexEntry> entries = entries(adapter);

    assertThat(adapter.encodeAuxiliaryPayload(entries, ENTRIES + 1)).isNotNull();
  }

  private static VectorIndexAdapter adapter() {
    return new VectorIndexAdapter(
        VectorIndexDefinition.builder()
            .fieldId(1)
            .dimension(DIMENSION)
            .metric(VectorIndexDefinition.Metric.COSINE)
            .structure(VectorIndexDefinition.VectorStructure.HNSW)
            .maxM(16)
            .efConstruction(200)
            .efSearch(200)
            .build());
  }

  private static List<IndexEntry> entries(VectorIndexAdapter adapter) {
    List<IndexEntry> entries = Lists.newArrayListWithCapacity(ENTRIES);
    for (int i = 0; i < ENTRIES; i++) {
      entries.add(
          IndexEntry.of("data-" + i / 100 + ".parquet", i, adapter.encodeVector(vector(i))));
    }
    return entries;
  }

  /** A vector that is distinct for every entry, so an entry can only match itself. */
  private static float[] vector(int entry) {
    float[] values = new float[DIMENSION];
    for (int d = 0; d < DIMENSION; d++) {
      values[d] = (float) Math.sin(entry * 0.7 + d * 1.3);
    }
    return values;
  }
}
