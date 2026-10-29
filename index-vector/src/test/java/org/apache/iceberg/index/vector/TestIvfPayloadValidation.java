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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.iceberg.index.IndexSearchResult;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * What loading an IVF payload checks before it returns a searcher.
 *
 * <p>A payload is written by one build and read by another, so a bad one has to be refused where
 * the record is, not where a query later happens to look: a data file index in a list nothing
 * probes is still wrong. The checks all happen after the payload is snapshotted and before any
 * array is allocated from an unvalidated count, which is what keeps a malformed header from asking
 * for gigabytes.
 */
class TestIvfPayloadValidation {

  private static final int DIMENSION = 4;

  @BeforeAll
  static void registerVectorIndexType() {
    VectorIndexTypes.register();
  }

  @Test
  void refusesAnInvalidDataFileIndexInAListAQueryWouldNotVisit() {
    float[][] centroid = {{0f, 0f, 0f, 0f}, {10f, 0f, 0f, 0f}};
    byte[] payload =
        IvfTestPayloads.plain(
            DIMENSION,
            new String[] {"data-0.parquet"},
            new float[][] {centroid[0], centroid[1]},
            2,
            List.of(
                List.of(new IvfTestPayloads.Vector(0, 0, vector(0f))),
                List.of(new IvfTestPayloads.Vector(7, 1, vector(1f)))));

    assertThatThrownBy(() -> kernel(false, 1).prepare(ByteBuffer.wrap(payload)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("list 1")
        .hasMessageContaining("record 0")
        .hasMessageContaining("data file index 7");
  }

  @Test
  void refusesANegativeListCount() {
    byte[] payload =
        raw(
            out -> {
              out.writeInt(3);
              out.writeUTF(IvfTestPayloads.KERNEL);
              out.writeInt(IvfTestPayloads.PLAIN_VERSION);
              out.writeInt(DIMENSION);
              out.writeInt(2);
              for (int list = 0; list < 2; list++) {
                for (int d = 0; d < DIMENSION; d++) {
                  out.writeFloat(list + d);
                }
              }
              out.writeInt(1);
              out.writeUTF("data-0.parquet");
              out.writeInt(1);
              out.writeInt(1);
              out.writeInt(0);
              out.writeLong(0);
              for (int d = 0; d < DIMENSION; d++) {
                out.writeFloat(d);
              }
              out.writeInt(-1);
            });

    assertThatThrownBy(() -> kernel(false, 1).prepare(ByteBuffer.wrap(payload)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("list 1")
        .hasMessageContaining("-1");
  }

  @Test
  void refusesAnEntryCountThatDisagreesWithTheLists() {
    byte[] payload =
        IvfTestPayloads.plain(
            DIMENSION,
            new String[] {"data-0.parquet"},
            new float[][] {vector(0f)},
            5,
            List.of(List.of(new IvfTestPayloads.Vector(0, 0, vector(0f)))));

    assertThatThrownBy(() -> kernel(false, 1).prepare(ByteBuffer.wrap(payload)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("entry count 5")
        .hasMessageContaining("1 record");
  }

  @Test
  void refusesATruncatedCentroid() {
    byte[] payload =
        IvfTestPayloads.plain(
            DIMENSION,
            new String[] {"data-0.parquet"},
            new float[][] {vector(0f)},
            1,
            List.of(List.of(new IvfTestPayloads.Vector(0, 0, vector(0f)))));
    // the header is 4 + 2 + 3 + 4 = 13 bytes, then dimension, list count and the first centroid
    byte[] cut = IvfTestPayloads.truncated(payload, 13 + 4 + 4 + 8);

    assertThatThrownBy(() -> kernel(false, 1).prepare(ByteBuffer.wrap(cut)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("centroids")
        .hasMessageContaining("ends");
  }

  @Test
  void refusesATruncatedQuantizedCode() {
    byte[] payload = quantizedPayload();
    // cut in the middle of the first record's code: the list claims one record of a full stride
    byte[] cut = IvfTestPayloads.truncated(payload, payload.length - 3 * Float.BYTES - 2);

    assertThatThrownBy(() -> kernel(true, 1).prepare(ByteBuffer.wrap(cut)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("list 0");
  }

  @Test
  void refusesTruncatedFactors() {
    byte[] payload = quantizedPayload();
    byte[] cut = IvfTestPayloads.truncated(payload, payload.length - 4);

    assertThatThrownBy(() -> kernel(true, 1).prepare(ByteBuffer.wrap(cut)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("list 0");
  }

  @Test
  @Timeout(20)
  void refusesAHugeListCountBeforeAllocatingForIt() {
    byte[] payload =
        raw(
            out -> {
              out.writeInt(3);
              out.writeUTF(IvfTestPayloads.KERNEL);
              out.writeInt(IvfTestPayloads.PLAIN_VERSION);
              out.writeInt(DIMENSION);
              out.writeInt(500_000_000);
              out.writeInt(0);
            });

    assertThatThrownBy(() -> kernel(false, 1).prepare(ByteBuffer.wrap(payload)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("list count")
        .hasMessageContaining("500000000");
  }

  @Test
  @Timeout(20)
  void refusesAHugeDictionaryCountBeforeAllocatingForIt() {
    byte[] payload =
        raw(
            out -> {
              out.writeInt(3);
              out.writeUTF(IvfTestPayloads.KERNEL);
              out.writeInt(IvfTestPayloads.PLAIN_VERSION);
              out.writeInt(DIMENSION);
              out.writeInt(1);
              for (int d = 0; d < DIMENSION; d++) {
                out.writeFloat(d);
              }
              out.writeInt(Integer.MAX_VALUE);
            });

    assertThatThrownBy(() -> kernel(false, 1).prepare(ByteBuffer.wrap(payload)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("data file count")
        .hasMessageContaining(String.valueOf(Integer.MAX_VALUE));
  }

  @Test
  void allowsTrailingBytesAfterTheLists() {
    byte[] payload =
        IvfTestPayloads.plain(
            DIMENSION,
            new String[] {"data-0.parquet"},
            new float[][] {vector(0f)},
            1,
            List.of(List.of(new IvfTestPayloads.Vector(0, 3, vector(0f)))));
    byte[] withTrailer = IvfTestPayloads.concatenate(payload, new byte[] {9, 8, 7, 6, 5, 4, 3, 2});

    VectorIndexAdapter adapter = adapter(false, 4);
    List<IndexSearchResult> results =
        adapter.prepare(ByteBuffer.wrap(withTrailer)).search(adapter.encodeQuery(vector(0f)), 5);
    assertThat(results).extracting(IndexSearchResult::rowPosition).containsExactly(3L);
  }

  @Test
  void readsAPayloadFromBeforeKernelsWereNamed() {
    byte[] payload =
        IvfTestPayloads.legacy(
            DIMENSION,
            new String[] {"data-0.parquet"},
            new float[][] {vector(0f)},
            1,
            List.of(List.of(new IvfTestPayloads.Vector(0, 4, vector(0f)))));

    VectorIndexAdapter adapter = adapter(false, 4);
    List<IndexSearchResult> results =
        adapter.prepare(ByteBuffer.wrap(payload)).search(adapter.encodeQuery(vector(0f)), 5);
    assertThat(results).extracting(IndexSearchResult::rowPosition).containsExactly(4L);
  }

  @Test
  void keepsPathsWithANulAndSupplementaryCharacters() {
    String nul = "nul\u0000path.parquet";
    String chinese = "中文目录/café.parquet";
    String supplementary = "a\uD83D\uDE00b.parquet";
    byte[] payload =
        IvfTestPayloads.plain(
            DIMENSION,
            new String[] {nul, chinese, supplementary},
            new float[][] {vector(0f)},
            3,
            List.of(
                List.of(
                    new IvfTestPayloads.Vector(0, 0, vector(0f)),
                    new IvfTestPayloads.Vector(1, 1, vector(1f)),
                    new IvfTestPayloads.Vector(2, 2, vector(2f)))));

    VectorIndexAdapter adapter = adapter(false, 4);
    List<IndexSearchResult> results =
        adapter.prepare(ByteBuffer.wrap(payload)).search(adapter.encodeQuery(vector(0f)), 3);
    assertThat(results)
        .extracting(IndexSearchResult::dataFile)
        .containsExactlyInAnyOrder(nul, chinese, supplementary);
  }

  private static byte[] quantizedPayload() {
    int bits = 2;
    int padded = 4;
    int codeBytes = (padded * bits + 7) / 8;
    byte[] code = new byte[codeBytes];
    return IvfTestPayloads.quantized(
        DIMENSION,
        bits,
        7L,
        new String[] {"data-0.parquet"},
        new float[][] {vector(0f)},
        1,
        new float[][] {vector(0f)},
        List.of(List.of(new IvfTestPayloads.Code(0, 9, code, new float[] {1f, 1f, 0f}))));
  }

  private static float[] vector(float seed) {
    float[] values = new float[DIMENSION];
    for (int d = 0; d < DIMENSION; d++) {
      values[d] = seed + d;
    }
    return values;
  }

  private static VectorKernel kernel(boolean quantized, int nprobe) {
    return adapter(quantized, nprobe).kernel();
  }

  private static VectorIndexAdapter adapter(boolean quantized, int nprobe) {
    VectorIndexDefinition.Builder builder =
        VectorIndexDefinition.builder()
            .fieldId(1)
            .dimension(DIMENSION)
            .metric(VectorIndexDefinition.Metric.EUCLIDEAN)
            .structure(VectorIndexDefinition.VectorStructure.IVF)
            .property(IvfOptions.NPROBE, Integer.toString(nprobe));
    if (quantized) {
      builder
          .quantizer(VectorIndexDefinition.VectorQuantizer.RABITQ)
          .property(RaBitQQuantizer.BITS, "2");
    }
    return new VectorIndexAdapter(builder.build());
  }

  private interface Body {
    void write(DataOutputStream out) throws IOException;
  }

  private static byte[] raw(Body body) {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(bytes)) {
      body.write(out);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }

    return bytes.toByteArray();
  }
}
