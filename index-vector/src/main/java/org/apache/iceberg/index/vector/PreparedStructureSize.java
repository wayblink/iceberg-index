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
 * What a prepared structure holds, in bytes, counted from the payload it was read from.
 *
 * <p>A cache that charges a payload twice for the structure built from it charges the wrong amount:
 * a graph costs more than the vectors it indexes, and a quantized list costs far less than the
 * vectors it replaced. Guessing one factor for both is what keeps a budget from covering the files
 * a query reads, and the number is not observable after the fact -- a Java object does not report
 * its own weight, and guessing from the heap afterwards answers a different question.
 *
 * <p>So it is counted from the payload, which is authoritative: the payload says how many entries
 * there are, how wide they are and how many connections each node has. The result is an estimate of
 * the live object graph, including object headers and array padding, and it is checked in tests
 * against the sizes the structures actually report.
 */
final class PreparedStructureSize {

  /** Bytes an array or object costs before its contents, on a 64-bit JVM with compressed oops. */
  static final int HEADER_BYTES = 16;

  /** Arrays are padded to a multiple of this. */
  private static final int ALIGNMENT = 8;

  /** A reference to another object, which is what a list slot costs. */
  static final int REFERENCE_BYTES = 4;

  /** An {@code ArrayList} slot in a {@code List<List<...>>}. */
  private static final int ARRAY_LIST_BYTES = HEADER_BYTES + 2 * REFERENCE_BYTES + 4;

  /** A {@code LinkedHashMap} entry, which a hash-based collection costs per key. */
  private static final int MAP_ENTRY_BYTES = 40 + REFERENCE_BYTES;

  private PreparedStructureSize() {}

  /** Rounds a raw byte count the way the JVM lays an array out. */
  static long array(long bytes) {
    Preconditions.checkArgument(bytes >= 0, "Array size is negative: %s", bytes);
    return align(HEADER_BYTES + bytes);
  }

  /** The cost of a {@code float[]} of this length. */
  static long floats(int length) {
    return array((long) length * Float.BYTES);
  }

  /** The cost of a {@code byte[]} of this length. */
  static long bytes(int length) {
    return array(length);
  }

  /** The cost of a {@code long[]} of this length. */
  static long longs(int length) {
    return array((long) length * Long.BYTES);
  }

  /** The cost of an {@code int[]} of this length. */
  static long ints(int length) {
    return array((long) length * Integer.BYTES);
  }

  /** The cost of a {@code String} holding this many characters. */
  static long string(int length) {
    // 16 bytes of header, then the compressed latin-1 contents when they fit
    return HEADER_BYTES + align(length + 3L) + REFERENCE_BYTES;
  }

  /**
   * What one entry of a structure costs: a reference to it from the list that holds it, the object
   * itself, and whatever it points at.
   */
  static long entry(long ownFields, long... arrays) {
    long total = HEADER_BYTES + ownFields + REFERENCE_BYTES;
    for (long array : arrays) {
      total += array;
    }

    return align(total);
  }

  /** What one element of a nested list costs, including the list object that holds it. */
  static long nestedList(long elements, long elementBytes) {
    return align(ARRAY_LIST_BYTES + array(REFERENCE_BYTES + elements * REFERENCE_BYTES))
        + elements * elementBytes
        + MAP_ENTRY_BYTES;
  }

  /** What a hash-set of strings of these lengths costs. */
  static long stringSet(long[] lengths, long tableCapacity) {
    long total = align(HEADER_BYTES + REFERENCE_BYTES) + tableCapacity * REFERENCE_BYTES;
    for (long length : lengths) {
      total += string((int) length) + MAP_ENTRY_BYTES;
    }

    return total;
  }

  private static long align(long bytes) {
    long remainder = bytes % ALIGNMENT;
    return remainder == 0 ? bytes : bytes + (ALIGNMENT - remainder);
  }
}
