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
import java.nio.ByteOrder;
import org.junit.jupiter.api.Test;

/**
 * The byte layer the in-place IVF layout reads through.
 *
 * <p>A payload is written by {@code DataOutputStream}, so every number is big-endian and a path is
 * a modified UTF-8 string. The reader has to reproduce exactly that, and it has to compute where a
 * record's code lives with {@code long} arithmetic: a payload can be larger than 256 MiB, where
 * multiplying the byte base by eight in an {@code int} wraps and silently reads another record.
 */
class TestIvfPayloadBytes {

  @Test
  void readsNumbersInTheByteOrderTheWriterUses() throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(bytes)) {
      out.writeInt(0x01020304);
      out.writeLong(0x0102030405060708L);
      out.writeFloat(3.25f);
      out.writeInt(-1);
      out.writeLong(Long.MIN_VALUE);
    }

    VectorPayloadReader reader = new VectorPayloadReader(bytes.toByteArray());
    ByteBuffer oracle = ByteBuffer.wrap(bytes.toByteArray()).order(ByteOrder.BIG_ENDIAN);
    assertThat(reader.readInt()).isEqualTo(oracle.getInt());
    assertThat(reader.readLong()).isEqualTo(oracle.getLong());
    assertThat(Float.floatToRawIntBits(reader.readFloat()))
        .isEqualTo(Float.floatToRawIntBits(oracle.getFloat()));
    assertThat(reader.readInt()).isEqualTo(-1);
    assertThat(reader.readLong()).isEqualTo(Long.MIN_VALUE);
    assertThat(reader.remaining()).isZero();
  }

  @Test
  void readsAStringExactlyAsWriteUtfWritesIt() throws Exception {
    for (String path :
        new String[] {
          "data/a.parquet", "nul\u0000inside", "中文目录/文件.parquet", "café-résumé", "a\uD83D\uDE00b"
        }) {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      try (DataOutputStream out = new DataOutputStream(bytes)) {
        out.writeInt(7);
        out.writeUTF(path);
      }

      VectorPayloadReader reader = new VectorPayloadReader(bytes.toByteArray());
      assertThat(reader.readInt()).isEqualTo(7);
      assertThat(reader.readUTF()).as("path %s", path).isEqualTo(path);
      assertThat(reader.remaining()).isZero();
    }
  }

  @Test
  void readsAtAStraightAfterAPositionAndLeavesTheRestAlone() throws Exception {
    ByteBuffer buffer = ByteBuffer.allocate(24).order(ByteOrder.BIG_ENDIAN);
    buffer.putInt(11).putLong(12L).putFloat(13.5f).putInt(14);
    byte[] bytes = buffer.array();

    VectorPayloadReader reader = new VectorPayloadReader(bytes, 4);
    assertThat(reader.readLong()).isEqualTo(12L);
    assertThat(Float.floatToRawIntBits(reader.floatAt(12)))
        .isEqualTo(Float.floatToRawIntBits(13.5f));
    assertThat(reader.intAt(16)).isEqualTo(14);
    assertThat(reader.position()).isEqualTo(12);
    assertThat(reader.intAt(0)).isEqualTo(11);
  }

  @Test
  void rejectsAReadPastTheEndInsteadOfReturningZeros() {
    VectorPayloadReader reader = new VectorPayloadReader(new byte[] {0, 0, 0});
    assertThatThrownBy(reader::readInt)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("payload ends");
    assertThatThrownBy(() -> reader.intAt(8))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("payload ends");
  }

  @Test
  void computesTheStridesOfTheDocumentedFormatsWithLongArithmetic() {
    // d = 129 pads to 256 coordinates; two bits per coordinate is 64 bytes per code.
    assertThat(IvfLayout.codeBytes(256L, 2)).isEqualTo(64L);
    assertThat(IvfLayout.quantizedStride(64L)).isEqualTo(88L);
    assertThat(IvfLayout.plainStride(129L)).isEqualTo(4L + 8L + 4L * 129L);
    assertThat(IvfLayout.quantizedListHeader(129L)).isEqualTo(4L * 129L + 4L);
    assertThat(IvfLayout.plainListHeader()).isEqualTo(4L);
    // every bit count the writer accepts, on a padded dimension that is not the logical one
    for (int bits = 1; bits <= 8; bits++) {
      long codeBytes = IvfLayout.codeBytes(256L, bits);
      assertThat(codeBytes).isEqualTo((256L * bits + 7L) / 8L);
      assertThat(IvfLayout.quantizedStride(codeBytes)).isEqualTo(24L + codeBytes);
    }
  }

  @Test
  void rejectsStrideArithmeticThatWouldWrap() {
    assertThatThrownBy(() -> IvfLayout.codeBytes(Long.MAX_VALUE / 2L, 8))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Code size");
    assertThatThrownBy(() -> IvfLayout.codeBytes(0L, 2))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> IvfLayout.plainStride(Long.MAX_VALUE / 2L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("stride");
    assertThatThrownBy(() -> IvfLayout.recordEnd(0L, Long.MAX_VALUE / 2L, 88L, "IVF list 0"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("IVF list 0");
    assertThatThrownBy(() -> IvfLayout.recordEnd(100L, -1L, 88L, "IVF list 0"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("IVF list 0");
  }

  @Test
  void addressesACodeBitBeyond256MiBWithoutIntWrap() {
    // The old reader computed codeBase * 8 in an int: 250,000,000 * 8 already overflows and the
    // sum with a coordinated bit offset wraps negative. Addresses are computed in long instead.
    long codeBase = 250_000_000L;
    long payloadLength = 400_000_000L;
    long codeBytes = 100_000_000L;
    assertThat(IvfLayout.codeByteAddress(codeBase, 40_000_000L, 8, 7, codeBytes, payloadLength))
        .isEqualTo(290_000_000);

    long lastByte =
        IvfLayout.codeByteAddress(codeBase, codeBytes - 1, 8, 7, codeBytes, 350_000_000L);
    assertThat(lastByte).isEqualTo(349_999_999);
  }

  @Test
  void rejectsCodeAddressesOutsideTheirCodeOrThePayload() {
    assertThatThrownBy(() -> IvfLayout.codeByteAddress(10L, 9L, 8, 0, 9L, 1_000L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("code");
    assertThatThrownBy(() -> IvfLayout.codeByteAddress(10L, 0L, 8, 0, 9L, 10L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("payload");
    assertThatThrownBy(
            () ->
                IvfLayout.codeByteAddress(
                    2_200_000_000L, 40_000_000L, 8, 0, 100_000_000L, 3_000_000_000L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("payload");
    assertThatThrownBy(
            () ->
                IvfLayout.codeByteAddress(
                    0L, Long.MAX_VALUE / 4L, 8, 0, Long.MAX_VALUE / 2L, Long.MAX_VALUE))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("payload");
  }
}
