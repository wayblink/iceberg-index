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

import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Where a record of an IVF payload starts, and where the code inside it lives.
 *
 * <p>The payload stores a list's records back to back at a fixed stride: a data file index, a row
 * position, and then either the vector itself or a quantized code and its factors. The code of a
 * quantizer is {@code ceil(padded * bits / 8)} bytes, computed from the padded dimension rather
 * than the logical one, and every step of that is computed as a {@code long} and checked: a
 * malformed header must fail here rather than silently wrap and read another record's bytes.
 *
 * <p>{@link #codeByteAddress} takes a coordinate and a bit within that coordinate's level and adds
 * the result to the code's byte base. It does not scale the base by eight -- that is the overflow
 * the old reader had at 256 MiB -- so the same arithmetic works for a payload of any length an
 * array can hold.
 */
final class IvfLayout {

  /** The data file index and row position written before every record's payload. */
  private static final long RECORD_PREFIX = Integer.BYTES + Long.BYTES;

  /** The length, correction and centre dot written after a quantized code. */
  private static final long QUANTIZED_FACTORS = 3L * Float.BYTES;

  private IvfLayout() {}

  static long codeBytes(long padded, int bits) {
    Preconditions.checkArgument(padded > 0, "Padded dimension must be positive: %s", padded);
    Preconditions.checkArgument(bits >= 1, "Bits per dimension must be positive: %s", bits);
    try {
      return Math.addExact(Math.multiplyExact(padded, (long) bits), 7L) / Byte.SIZE;
    } catch (ArithmeticException e) {
      throw new IllegalArgumentException(
          "Code size overflows for padded dimension " + padded + " and " + bits + " bits", e);
    }
  }

  static long plainStride(long dimension) {
    Preconditions.checkArgument(dimension > 0, "Dimension must be positive: %s", dimension);
    try {
      return Math.addExact(RECORD_PREFIX, Math.multiplyExact((long) Float.BYTES, dimension));
    } catch (ArithmeticException e) {
      throw new IllegalArgumentException("Record stride overflows for dimension " + dimension, e);
    }
  }

  static long quantizedStride(long codeBytes) {
    Preconditions.checkArgument(codeBytes >= 0, "Code size is negative: %s", codeBytes);
    try {
      return Math.addExact(RECORD_PREFIX + QUANTIZED_FACTORS, codeBytes);
    } catch (ArithmeticException e) {
      throw new IllegalArgumentException(
          "Record stride overflows for " + codeBytes + " code bytes", e);
    }
  }

  static long plainListHeader() {
    return Integer.BYTES;
  }

  static long quantizedListHeader(long dimension) {
    Preconditions.checkArgument(dimension > 0, "Dimension must be positive: %s", dimension);
    try {
      return Math.addExact(Math.multiplyExact((long) Float.BYTES, dimension), Integer.BYTES);
    } catch (ArithmeticException e) {
      throw new IllegalArgumentException("List header overflows for dimension " + dimension, e);
    }
  }

  static long listHeader(long dimension, boolean quantized) {
    return quantized ? quantizedListHeader(dimension) : plainListHeader();
  }

  /** The end of {@code count} records of {@code stride} bytes, from {@code start}. */
  static long recordEnd(long start, long count, long stride, String description) {
    Preconditions.checkArgument(
        start >= 0, "%s starts at a negative offset: %s", description, start);
    Preconditions.checkArgument(
        count >= 0, "%s has a negative record count: %s", description, count);
    try {
      return Math.addExact(start, Math.multiplyExact(count, stride));
    } catch (ArithmeticException e) {
      throw new IllegalArgumentException(
          description + " does not fit the payload: " + count + " records of " + stride + " bytes",
          e);
    }
  }

  /** Fails unless {@code end} is inside a payload of {@code payloadLength} bytes. */
  static void requireWithin(long end, long payloadLength, String description) {
    Preconditions.checkArgument(
        end <= payloadLength,
        "%s ends at byte %s, after the %s-byte payload",
        description,
        end,
        payloadLength);
  }

  /**
   * The byte that holds bit {@code bit} of the level of {@code coordinate}, in a code starting at
   * {@code codeBase}.
   *
   * <p>The bit position is relative to the code, so it is multiplied and added in {@code long} and
   * only narrowed once it is known to be inside the code and the payload.
   */
  static int codeByteAddress(
      long codeBase, long coordinate, int bits, int bit, long codeBytes, long payloadLength) {
    Preconditions.checkArgument(coordinate >= 0, "Negative code coordinate: %s", coordinate);
    Preconditions.checkArgument(
        bit >= 0 && bit < bits, "Code bit %s is outside %s bits per coordinate", bit, bits);
    long localBit;
    long address;
    try {
      localBit = Math.addExact(Math.multiplyExact(coordinate, (long) bits), bit);
      long byteOffset = localBit >>> 3;
      Preconditions.checkArgument(
          byteOffset < codeBytes,
          "Code coordinate %s is outside the %s-byte code",
          coordinate,
          codeBytes);
      address = Math.addExact(codeBase, byteOffset);
    } catch (ArithmeticException e) {
      throw new IllegalArgumentException(
          "Code address overflows for coordinate " + coordinate + " at payload byte " + codeBase,
          e);
    }

    Preconditions.checkArgument(
        address >= 0 && address < payloadLength,
        "Code address %s for coordinate %s is outside the %s-byte payload",
        address,
        coordinate,
        payloadLength);
    try {
      return Math.toIntExact(address);
    } catch (ArithmeticException e) {
      throw new IllegalArgumentException(
          "Code address " + address + " is outside an addressable payload", e);
    }
  }
}
