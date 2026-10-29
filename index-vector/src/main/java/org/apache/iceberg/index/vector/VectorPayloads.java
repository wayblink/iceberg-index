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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.iceberg.index.IndexEntry;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * The parts of a payload every kernel writes the same way: the header that says who wrote it, the
 * encoding of a vector, and the dictionary of data files an entry refers to.
 *
 * <p>The dictionary is shared because a path is long and every entry of a file repeats it, which is
 * most of a payload once there are many entries per file; a kernel that wrote paths per entry would
 * pay for them per entry.
 */
final class VectorPayloads {

  static final int VALUE_FORMAT_VERSION = 1;

  /**
   * The payload header written before kernels were named: a bare format version.
   *
   * <p>Only this index type ever wrote it, so it is attributed to whichever kernel writes that role
   * now, which is what keeps an index built before the header changed readable.
   */
  private static final int LEGACY_PAYLOAD_HEADER = 2;

  /** The header that names the kernel and its own format version. */
  private static final int PAYLOAD_HEADER = 3;

  private VectorPayloads() {}

  static void writeHeader(DataOutputStream out, String kernel, int kernelVersion)
      throws IOException {
    out.writeInt(PAYLOAD_HEADER);
    out.writeUTF(kernel);
    out.writeInt(kernelVersion);
  }

  /**
   * How many bytes are left, which is what every count in a payload has to be checked against.
   *
   * <p>A {@link DataInputStream} only reports this with a checked exception, and the counts that
   * follow are four bytes wide: a field that claims two billion entries has to fail where it is
   * read rather than at the array allocation, where the executor dies of an {@code
   * OutOfMemoryError} that says nothing about the payload.
   */
  static int remaining(DataInputStream in) {
    try {
      return in.available();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read an index payload", e);
    }
  }

  static void readHeader(
      DataInputStream in, String expectedKernel, int expectedVersion, String role)
      throws IOException {
    int version = in.readInt();
    if (version == LEGACY_PAYLOAD_HEADER) {
      return;
    }

    Preconditions.checkArgument(
        version == PAYLOAD_HEADER,
        "Unsupported %s payload format version: %s (this build reads %s and %s)",
        role,
        version,
        LEGACY_PAYLOAD_HEADER,
        PAYLOAD_HEADER);

    String kernel = in.readUTF();
    int kernelVersion = in.readInt();
    Preconditions.checkArgument(
        expectedKernel.equals(kernel),
        "%s payload was written by kernel %s version %s; this build reads kernel %s version %s",
        role,
        kernel,
        kernelVersion,
        expectedKernel,
        expectedVersion);
    Preconditions.checkArgument(
        kernelVersion == expectedVersion,
        "%s payload was written by kernel %s version %s; this build reads version %s",
        role,
        kernel,
        kernelVersion,
        expectedVersion);
  }

  static byte[] encodeVector(float[] vector, int dimension, String description) {
    Preconditions.checkNotNull(vector, "%s vector is null", description);
    Preconditions.checkArgument(vector.length > 0, "%s vector is empty", description);
    Preconditions.checkArgument(
        vector.length == dimension,
        "%s dimension %s does not match index dimension %s",
        description,
        vector.length,
        dimension);
    for (float value : vector) {
      Preconditions.checkArgument(
          Float.isFinite(value), "%s values must be finite: %s", description, value);
    }

    ByteBuffer buffer = ByteBuffer.allocate(8 + 4 * vector.length);
    buffer.putInt(VALUE_FORMAT_VERSION);
    buffer.putInt(vector.length);
    for (float value : vector) {
      buffer.putFloat(value);
    }
    return buffer.array();
  }

  static float[] decodeVector(byte[] payload, int dimension, String description) {
    Preconditions.checkNotNull(payload, "%s payload is null", description);
    ByteBuffer buffer = ByteBuffer.wrap(payload);
    int version = buffer.getInt();
    Preconditions.checkArgument(
        version == VALUE_FORMAT_VERSION,
        "Unsupported %s value format version: %s",
        description,
        version);
    int length = buffer.getInt();
    Preconditions.checkArgument(length > 0, "%s dimension is empty", description);
    Preconditions.checkArgument(
        length == dimension,
        "%s dimension %s does not match index dimension %s",
        description,
        length,
        dimension);
    float[] vector = new float[length];
    for (int i = 0; i < length; i++) {
      vector[i] = buffer.getFloat();
      Preconditions.checkArgument(
          Float.isFinite(vector[i]), "%s values must be finite: %s", description, vector[i]);
    }
    return vector;
  }

  /** The data files the entries point at, in the order they first appear. */
  static List<String> dataFilesOf(List<IndexEntry> entries) {
    List<String> dataFiles = Lists.newArrayList();
    java.util.Map<String, Boolean> seen = Maps.newLinkedHashMap();
    for (IndexEntry entry : entries) {
      if (seen.put(entry.dataFile(), Boolean.TRUE) == null) {
        dataFiles.add(entry.dataFile());
      }
    }
    return dataFiles;
  }

  static byte[] toBytes(ByteBuffer buffer) {
    byte[] bytes = new byte[buffer.remaining()];
    buffer.duplicate().get(bytes);
    return bytes;
  }
}
