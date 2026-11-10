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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.index.IndexBuildResult;
import org.apache.iceberg.index.IndexEntry;
import org.apache.iceberg.index.vector.VectorIndexDefinition.Metric;
import org.apache.iceberg.index.vector.VectorIndexDefinition.VectorQuantizer;
import org.apache.iceberg.index.vector.VectorIndexDefinition.VectorStructure;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The bytes an IVF file holds do not depend on how many threads built it.
 *
 * <p>Decoding, the k-means assignment and the final assignment each decide one row at a time, so a
 * build that splits those rows across threads must land on the same centroids and the same lists as
 * a build that does not. Anything that reorders the float sums of k-means would break this, which
 * is why the accumulation stays on one thread.
 */
class TestIvfBuildThreads {
  private static final int DIMENSION = 32;

  @BeforeAll
  static void registerVectorIndexType() {
    VectorIndexTypes.register();
  }

  @ParameterizedTest(name = "{0}")
  @ValueSource(strings = {"EUCLIDEAN", "COSINE"})
  void thePayloadDoesNotDependOnHowManyThreadsBuiltIt(String metric) {
    Map<String, String> quantizedOptions = Map.of("nlist", "8", "nprobe", "2", "rabitq-bits", "3");
    Map<String, String> plainOptions = Map.of("nlist", "8", "nprobe", "2");
    VectorIndexAdapter quantized =
        adapter(Metric.valueOf(metric), VectorQuantizer.RABITQ, quantizedOptions);
    VectorIndexAdapter plain = adapter(Metric.valueOf(metric), VectorQuantizer.NONE, plainOptions);
    List<IndexEntry> entries = entries(quantized, 2000);

    assertSameBytes(quantized, entries, metric + "/RABITQ");
    assertSameBytes(plain, entries, metric + "/NONE");
  }

  private void assertSameBytes(
      VectorIndexAdapter adapter, List<IndexEntry> entries, String description) {
    IndexBuildResult single = adapter.encodeAuxiliaryPayloadWithMemory(entries, 1);
    IndexBuildResult many = adapter.encodeAuxiliaryPayloadWithMemory(entries, 4);

    assertThat(many.payload().remaining())
        .as("%s: one thread and four threads write the same length", description)
        .isEqualTo(single.payload().remaining());
    assertThat(bytes(many.payload()))
        .as("%s: one thread and four threads write the same bytes", description)
        .isEqualTo(bytes(single.payload()));
    assertThat(many.memory().preparedBytes())
        .as("%s: the estimate describes the same payload", description)
        .isEqualTo(single.memory().preparedBytes());
  }

  private static byte[] bytes(ByteBuffer payload) {
    ByteBuffer copy = payload.duplicate();
    byte[] bytes = new byte[copy.remaining()];
    copy.get(bytes);
    return bytes;
  }

  private static VectorIndexAdapter adapter(
      Metric metric, VectorQuantizer quantizer, Map<String, String> options) {
    return new VectorIndexAdapter(
        VectorIndexDefinition.builder()
            .fieldId(1)
            .dimension(DIMENSION)
            .metric(metric)
            .structure(VectorStructure.IVF)
            .quantizer(quantizer)
            .properties(options)
            .build());
  }

  private static List<IndexEntry> entries(VectorIndexAdapter adapter, int count) {
    List<IndexEntry> entries = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      List<Float> value = new ArrayList<>(DIMENSION);
      for (int d = 0; d < DIMENSION; d++) {
        value.add((float) Math.sin(0.01 * (i * DIMENSION + d)));
      }
      entries.add(
          IndexEntry.of("data-" + (i % 4) + ".parquet", i, adapter.encodeEntryValue(value)));
    }

    return entries;
  }
}
