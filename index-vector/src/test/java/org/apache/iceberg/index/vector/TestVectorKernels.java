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

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.iceberg.index.IndexAdapter.PreparedSearch;
import org.apache.iceberg.index.IndexEntry;
import org.apache.iceberg.index.IndexSearchResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * How a definition finds the kernel that serves it.
 *
 * <p>The registry is where a third-party index library is attached, so what matters is that a
 * definition naming a structure or a quantizer only some other kernel offers resolves to that
 * kernel, and that a definition nothing can build is refused by name rather than built anyway.
 */
class TestVectorKernels {

  private static final String FAKE = "fake-ivf";

  @BeforeAll
  static void registerVectorIndexType() {
    VectorIndexTypes.register();
    // a kernel for a structure the shipped ones do not offer, standing in for the third-party
    // library that will offer it
    VectorKernels.register(new FakeIvfFactory());
  }

  @Test
  public void resolvesADefinitionToTheKernelThatOffersIt() {
    VectorIndexAdapter adapter =
        new VectorIndexAdapter(
            VectorIndexDefinition.builder()
                .fieldId(1)
                .dimension(2)
                .metric(VectorIndexDefinition.Metric.EUCLIDEAN)
                .structure(VectorIndexDefinition.VectorStructure.IVF)
                .build());

    assertThat(adapter.kernel().name()).isEqualTo(FAKE);
    assertThat(adapter.kernel().capabilities().structures())
        .containsExactly(VectorIndexDefinition.VectorStructure.IVF);

    // and the adapter really delegates: the fake kernel answers, not a default
    ByteBuffer payload =
        adapter.encodeAuxiliaryPayload(
            ImmutableList.of(
                IndexEntry.of("data.parquet", 0, adapter.encodeVector(new float[] {1.0f, 0.0f}))),
            1);
    assertThat(
            adapter
                .prepareAuxiliary(payload)
                .search(adapter.encodeQuery(new float[] {1.0f, 0.0f}), 1))
        .extracting(IndexSearchResult::dataFile)
        .containsExactly(FakeIvfFactory.ANSWER);
  }

  @Test
  public void refusesACombinationNoKernelOffers() {
    assertThatThrownBy(
            () ->
                new VectorIndexAdapter(
                    VectorIndexDefinition.builder()
                        .fieldId(1)
                        .dimension(2)
                        .metric(VectorIndexDefinition.Metric.EUCLIDEAN)
                        // a graph whose vectors are quantized: no kernel offers it, because the
                        // graph library keeps the vectors itself
                        .structure(VectorIndexDefinition.VectorStructure.HNSW)
                        .quantizer(VectorIndexDefinition.VectorQuantizer.RABITQ)
                        .build()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("No vector kernel builds a HNSW index with the RABITQ quantizer")
        .hasMessageContaining(FAKE);
  }

  @Test
  public void reportsAKernelItDoesNotKnowByName() {
    assertThatThrownBy(() -> VectorKernels.fromName("nonesuch"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unknown vector kernel: nonesuch")
        .hasMessageContaining(FAKE);
  }

  @Test
  public void refusesASecondKernelOfTheSameName() {
    assertThatThrownBy(() -> VectorKernels.register(new FakeIvfFactory()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("A vector kernel named " + FAKE + " is already registered");
  }

  @Test
  public void keepsTheKernelLibrariesOutOfTheAdapter() {
    // the adapter is the seam's caller: a field of its own that came from an index library would
    // mean the library had leaked back into the layer that is meant to be indifferent to it
    for (Field field : VectorIndexAdapter.class.getDeclaredFields()) {
      assertThat(field.getType().getName())
          .as("the type of field %s", field.getName())
          .doesNotStartWith("com.github.jelmerk");
    }
  }

  /** Stands in for a kernel a third-party library will provide. */
  private static final class FakeIvfFactory implements VectorKernelFactory {
    private static final String ANSWER = "answered-by-the-fake";

    @Override
    public String name() {
      return FAKE;
    }

    @Override
    public KernelCapabilities capabilities() {
      return KernelCapabilities.builder()
          .structure(VectorIndexDefinition.VectorStructure.IVF)
          .quantizer(VectorIndexDefinition.VectorQuantizer.NONE)
          .metric(VectorIndexDefinition.Metric.EUCLIDEAN)
          .build();
    }

    @Override
    public VectorKernel create(VectorIndexDefinition definition) {
      return new VectorKernel() {
        @Override
        public String name() {
          return FAKE;
        }

        @Override
        public KernelCapabilities capabilities() {
          return FakeIvfFactory.this.capabilities();
        }

        @Override
        public int payloadFormatVersion() {
          return 1;
        }

        @Override
        public ByteBuffer build(List<IndexEntry> entries, int parallelism) {
          return ByteBuffer.allocate(0);
        }

        @Override
        public List<IndexEntry> entries(ByteBuffer payload) {
          return ImmutableList.of();
        }

        @Override
        public PreparedSearch prepare(ByteBuffer payload) {
          return (query, topK) ->
              topK == 0
                  ? ImmutableList.of()
                  : ImmutableList.of(IndexSearchResult.of(ANSWER, 0L, 1.0));
        }
      };
    }
  }
}
