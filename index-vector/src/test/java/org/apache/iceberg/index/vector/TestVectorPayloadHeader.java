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
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

class TestVectorPayloadHeader {

  private static VectorKernel kernel(String name, int version) {
    return new VectorKernel() {
      @Override
      public String name() {
        return name;
      }

      @Override
      public KernelCapabilities capabilities() {
        return KernelCapabilities.builder()
            .structure(VectorIndexDefinition.VectorStructure.FLAT)
            .quantizer(VectorIndexDefinition.VectorQuantizer.NONE)
            .metric(VectorIndexDefinition.Metric.EUCLIDEAN)
            .nativeLibrary(false)
            .incrementalAdd(false)
            .build();
      }

      @Override
      public int payloadFormatVersion() {
        return version;
      }

      @Override
      public ByteBuffer build(
          java.util.List<org.apache.iceberg.index.IndexEntry> entries, int parallelism) {
        throw new UnsupportedOperationException();
      }

      @Override
      public java.util.List<org.apache.iceberg.index.IndexEntry> entries(ByteBuffer payload) {
        throw new UnsupportedOperationException();
      }

      @Override
      public org.apache.iceberg.index.IndexAdapter.PreparedSearch prepare(ByteBuffer payload) {
        throw new UnsupportedOperationException();
      }
    };
  }

  private static byte[] named(String kernel, int version) throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(bytes)) {
      out.writeInt(3);
      out.writeUTF(kernel);
      out.writeInt(version);
      out.write(new byte[] {1, 2, 3});
    }

    return bytes.toByteArray();
  }

  @Test
  void aLegacyHeaderIsAttributedToWhicheverKernelReadsIt() {
    ByteBuffer payload = ByteBuffer.allocate(8);
    payload.putInt(2);
    payload.putInt(0x1234);
    payload.flip();

    VectorPayloadHeader header = VectorPayloadHeader.of(payload);
    assertThat(header.legacy()).isTrue();
    assertThat(header.readableBy(kernel("anything", 99))).isTrue();
  }

  @Test
  void theSameKernelAndVersionIsReadable() throws Exception {
    VectorPayloadHeader header = VectorPayloadHeader.of(named("ivf", 2));
    assertThat(header.kernel()).isEqualTo("ivf");
    assertThat(header.kernelVersion()).isEqualTo(2);
    assertThat(header.readableBy(kernel("ivf", 2))).isTrue();
  }

  @Test
  void anotherKernelOrVersionIsNotReadable() throws Exception {
    VectorPayloadHeader header = VectorPayloadHeader.of(named("ivf", 2));
    assertThat(header.readableBy(kernel("hnswlib", 2))).isFalse();
    assertThat(header.readableBy(kernel("ivf", 3))).isFalse();
  }

  @Test
  void anUnknownHeaderVersionIsReported() {
    ByteBuffer payload = ByteBuffer.allocate(8);
    payload.putInt(99);
    payload.putInt(0);
    payload.flip();

    assertThatThrownBy(() -> VectorPayloadHeader.of(payload))
        .hasMessageContaining("Unknown payload header version 99");
  }
}
