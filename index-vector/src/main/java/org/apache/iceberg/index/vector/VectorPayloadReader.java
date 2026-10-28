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
 * Reads the numbers and strings of a payload at fixed offsets, without a buffer object per read.
 *
 * <p>A payload written by {@code DataOutputStream} is big-endian, and a string is written by {@code
 * writeUTF}, whose length prefix counts bytes and whose encoding is modified UTF-8 rather than
 * UTF-8. This reads both, so a record can be scored where it lies instead of being decoded into
 * objects.
 *
 * <p>Offsets are {@code long} on the way in. A payload can pass 256 MiB, where scaling a byte
 * address by eight for a bit position overflows an {@code int}; only an address that a checked
 * calculation has shown to be inside the array is narrowed.
 */
final class VectorPayloadReader {

  private final byte[] bytes;
  private int position;
  private final int limit;

  VectorPayloadReader(byte[] bytes) {
    this(bytes, 0, bytes.length);
  }

  VectorPayloadReader(byte[] bytes, int position) {
    this(bytes, position, bytes.length);
  }

  VectorPayloadReader(byte[] bytes, int position, int limit) {
    Preconditions.checkNotNull(bytes, "Payload bytes are null");
    Preconditions.checkArgument(
        position >= 0 && position <= limit && limit <= bytes.length,
        "Invalid payload window: [%s, %s) of %s bytes",
        position,
        limit,
        bytes.length);
    this.bytes = bytes;
    this.position = position;
    this.limit = limit;
  }

  byte[] payload() {
    return bytes;
  }

  int position() {
    return position;
  }

  void position(int newPosition) {
    Preconditions.checkArgument(
        newPosition >= 0 && newPosition <= limit,
        "Invalid payload position %s in a window of %s bytes",
        newPosition,
        limit);
    this.position = newPosition;
  }

  int limit() {
    return limit;
  }

  int remaining() {
    return limit - position;
  }

  void skip(long length, String description) {
    Preconditions.checkArgument(length >= 0, "Negative skip for %s: %s", description, length);
    position = index((long) position + length, description);
  }

  int readInt() {
    require(Integer.BYTES, "an int");
    int value =
        ((bytes[position] & 0xFF) << 24)
            | ((bytes[position + 1] & 0xFF) << 16)
            | ((bytes[position + 2] & 0xFF) << 8)
            | (bytes[position + 3] & 0xFF);
    position += Integer.BYTES;
    return value;
  }

  int readUnsignedShort() {
    require(Short.BYTES, "an unsigned short");
    int value = ((bytes[position] & 0xFF) << 8) | (bytes[position + 1] & 0xFF);
    position += Short.BYTES;
    return value;
  }

  long readLong() {
    long high = readInt();
    long low = readInt() & 0xFFFFFFFFL;
    return (high << 32) | low;
  }

  float readFloat() {
    return Float.intBitsToFloat(readInt());
  }

  /**
   * Reads a string the way {@code DataInputStream.readUTF} does.
   *
   * <p>The length prefix is the number of bytes that follow, and the encoding is modified UTF-8:
   * {@code NUL} is two bytes and characters outside the basic plane are two surrogates, each three
   * bytes. Reading it as ordinary UTF-8 would shift every string after the first one containing a
   * NUL.
   */
  String readUTF() {
    int length = readUnsignedShort();
    require(length, "a string");
    int end = position + length;
    char[] chars = new char[length];
    int read = position;
    int written = 0;
    while (read < end) {
      int first = bytes[read] & 0xFF;
      if (first > 0x7F) {
        break;
      }

      read += 1;
      chars[written++] = (char) first;
    }

    while (read < end) {
      int first = bytes[read] & 0xFF;
      switch (first >> 4) {
        case 0, 1, 2, 3, 4, 5, 6, 7 -> {
          read += 1;
          chars[written++] = (char) first;
        }
        case 12, 13 -> {
          continuation(bytes, read + 1, end);
          chars[written++] = (char) (((first & 0x1F) << 6) | (bytes[read + 1] & 0x3F));
          read += 2;
        }
        case 14 -> {
          continuation(bytes, read + 1, end);
          continuation(bytes, read + 2, end);
          chars[written++] =
              (char)
                  (((first & 0x0F) << 12)
                      | ((bytes[read + 1] & 0x3F) << 6)
                      | (bytes[read + 2] & 0x3F));
          read += 3;
        }
        default -> throw malformed(read);
      }
    }

    position = end;
    return new String(chars, 0, written);
  }

  int intAt(long offset) {
    int at = index(offset, "an int");
    requireAt(at, Integer.BYTES, offset);
    return ((bytes[at] & 0xFF) << 24)
        | ((bytes[at + 1] & 0xFF) << 16)
        | ((bytes[at + 2] & 0xFF) << 8)
        | (bytes[at + 3] & 0xFF);
  }

  long longAt(long offset) {
    long high = intAt(offset);
    long low = intAt(offset + Integer.BYTES) & 0xFFFFFFFFL;
    return (high << 32) | low;
  }

  float floatAt(long offset) {
    return Float.intBitsToFloat(intAt(offset));
  }

  byte byteAt(long offset) {
    int at = index(offset, "a byte");
    requireAt(at, 1, offset);
    return bytes[at];
  }

  private int index(long offset, String description) {
    int at;
    try {
      at = Math.toIntExact(offset);
    } catch (ArithmeticException e) {
      throw new IllegalArgumentException(
          "IVF payload offset " + offset + " for " + description + " is outside the payload", e);
    }

    Preconditions.checkArgument(
        at >= 0 && at <= limit,
        "IVF payload ends after %s bytes, offset %s for %s does not fit",
        limit,
        offset,
        description);
    return at;
  }

  private void requireAt(int at, int length, long offset) {
    Preconditions.checkArgument(
        at >= 0 && at + (long) length <= limit,
        "IVF payload ends after %s bytes, %s bytes at offset %s do not fit",
        limit,
        length,
        offset);
  }

  private void require(int length, String description) {
    Preconditions.checkArgument(
        length >= 0 && (long) position + length <= limit,
        "IVF payload ends after %s bytes, %s needs %s more",
        limit,
        description,
        length);
  }

  private static void continuation(byte[] bytes, int at, int end) {
    Preconditions.checkArgument(
        at < end, "IVF payload string ends inside a multi-byte character at byte %s", at);
    Preconditions.checkArgument(
        (bytes[at] & 0xC0) == 0x80,
        "IVF payload string has malformed modified UTF-8 at byte %s",
        at);
  }

  private IllegalArgumentException malformed(int end) {
    return new IllegalArgumentException(
        "IVF payload string has malformed modified UTF-8 before byte " + end);
  }
}
