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

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Structural estimates of retained heap on a 64-bit JVM with compressed references, 8-byte
 * alignment and compact strings. These are not heap measurements or allocation-peak limits.
 *
 * <p>Payload buffers and query scratch space are excluded. Object headers and collection slack are
 * conservative approximations; different JVM layouts and disabled compact strings can change the
 * result. References belong to their containing objects or arrays, not to their targets.
 */
final class PreparedStructureSize {

  static final int HEADER_BYTES = 16;
  static final int REFERENCE_BYTES = 4;
  private static final int ALIGNMENT = 8;
  private static final int STRING_OBJECT_BYTES = 24;
  private static final int MAP_ENTRY_BYTES = 40;

  private PreparedStructureSize() {}

  static long object(long fields) {
    return align(HEADER_BYTES + fields);
  }

  static long array(long bytes) {
    Preconditions.checkArgument(bytes >= 0, "Array size is negative: %s", bytes);
    return align(HEADER_BYTES + bytes);
  }

  static long references(int length) {
    return array((long) length * REFERENCE_BYTES);
  }

  static long floats(int length) {
    return array((long) length * Float.BYTES);
  }

  static long bytes(int length) {
    return array(length);
  }

  static long longs(int length) {
    return array((long) length * Long.BYTES);
  }

  static long ints(int length) {
    return array((long) length * Integer.BYTES);
  }

  /** One String object and its separate backing byte array, without an incoming reference. */
  static long string(CharSequence value) {
    long bytes = value.length();
    for (int i = 0; i < value.length(); i++) {
      if (value.charAt(i) > 0xFF) {
        bytes *= 2;
        break;
      }
    }
    return STRING_OBJECT_BYTES + array(bytes);
  }

  /** Counts retained String objects by identity; equal values need not share storage. */
  static long distinctStrings(Iterable<String> values) {
    Set<String> seen = Collections.newSetFromMap(new IdentityHashMap<>());
    long total = 0;
    for (String value : values) {
      if (seen.add(value)) {
        total += string(value);
      }
    }
    return total;
  }

  /** Conservative hash-map allowance including table slack and separately retained keys/values. */
  static long map(long entries, long keyBytes, long valueBytes) {
    return object(4L * REFERENCE_BYTES + 2L * Integer.BYTES)
        + array(2L * entries * REFERENCE_BYTES)
        + entries * (MAP_ENTRY_BYTES + keyBytes + valueBytes);
  }

  /** Object fields and retained arrays; incoming references are counted by the caller. */
  static long entry(long ownFields, long... arrays) {
    long total = object(ownFields);
    for (long array : arrays) {
      total += array;
    }
    return total;
  }

  /** An exactly sized ArrayList backing array and its elements. */
  static long nestedList(long elements, long elementBytes) {
    return object(2L * REFERENCE_BYTES + 2L * Integer.BYTES)
        + array(elements * REFERENCE_BYTES)
        + elements * elementBytes;
  }

  private static long align(long bytes) {
    return (bytes + ALIGNMENT - 1) / ALIGNMENT * ALIGNMENT;
  }
}
