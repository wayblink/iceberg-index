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

import java.lang.reflect.Array;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Structural estimates of retained heap on a 64-bit JVM with compressed references, 8-byte
 * alignment and compact strings. These are not heap measurements or allocation-peak limits.
 *
 * <p>External input buffers and query scratch are excluded, but owned arrays are included. The
 * 16-byte object-header allowance rounds up the supported HotSpot 17 header; collection backing
 * capacities must be taken from the actual representation. References belong to their containing
 * objects or arrays, not to their targets. Callers must gate estimates on the runtime profile.
 */
final class PreparedStructureSize {

  static final int HEADER_BYTES = 16;
  static final int REFERENCE_BYTES = 4;
  static final int ALIGNMENT = 8;
  private static final int STRING_OBJECT_BYTES = 24;

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

  /** Identity receipt for owned state; it does not traverse arbitrary objects. */
  static final class Receipt {
    private final Set<Object> seen = Collections.newSetFromMap(new IdentityHashMap<>());

    long object(Object value, long fields) {
      return value != null && seen.add(value) ? PreparedStructureSize.object(fields) : 0;
    }

    long array(Object value) {
      if (value == null || !seen.add(value)) {
        return 0;
      }
      Class<?> component = value.getClass().getComponentType();
      int width =
          !component.isPrimitive()
              ? REFERENCE_BYTES
              : component == byte.class || component == boolean.class
                  ? 1
                  : component == short.class || component == char.class
                      ? 2
                      : component == long.class || component == double.class ? 8 : 4;
      return PreparedStructureSize.array((long) Array.getLength(value) * width);
    }

    long string(String value) {
      return value != null && seen.add(value) ? PreparedStructureSize.string(value) : 0;
    }
  }

  private static long align(long bytes) {
    return (bytes + ALIGNMENT - 1) / ALIGNMENT * ALIGNMENT;
  }
}
