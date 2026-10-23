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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * What a payload says about the kernel that wrote it, read without loading the payload.
 *
 * <p>A payload begins with a header: a format version, and from the version that named kernels
 * onwards, the kernel's name and its own format version. A reader compares that against what this
 * build can read, which is what lets an index written by another version be reported as needing a
 * rebuild rather than failing in the middle of a query.
 */
public final class VectorPayloadHeader {

  /** The header written before kernels were named: a bare format version. */
  private static final int LEGACY_HEADER = 2;

  /** The header that names the kernel and its own format version. */
  private static final int NAMED_HEADER = 3;

  private final int headerVersion;
  private final String kernel;
  private final int kernelVersion;

  private VectorPayloadHeader(int headerVersion, String kernel, int kernelVersion) {
    this.headerVersion = headerVersion;
    this.kernel = kernel;
    this.kernelVersion = kernelVersion;
  }

  /**
   * Reads the header of a payload.
   *
   * @throws IllegalArgumentException when the payload does not start with a header this build knows
   */
  public static VectorPayloadHeader of(ByteBuffer payload) {
    Preconditions.checkNotNull(payload, "Payload is null");
    ByteBuffer copy = payload.duplicate();
    try (DataInputStream in =
        new DataInputStream(
            new ByteArrayInputStream(
                copy.array(), copy.arrayOffset() + copy.position(), copy.remaining()))) {
      int version = in.readInt();
      if (version == LEGACY_HEADER) {
        return new VectorPayloadHeader(version, null, 0);
      }

      Preconditions.checkArgument(
          version == NAMED_HEADER,
          "Unknown payload header version %s: this build reads %s and %s",
          version,
          LEGACY_HEADER,
          NAMED_HEADER);
      return new VectorPayloadHeader(version, in.readUTF(), in.readInt());
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read the payload header", e);
    }
  }

  public static VectorPayloadHeader of(byte[] payload) {
    return of(ByteBuffer.wrap(payload));
  }

  public int headerVersion() {
    return headerVersion;
  }

  /** The kernel that wrote the payload, or null when the header predates named kernels. */
  public String kernel() {
    return kernel;
  }

  /** The writer's own format version, or zero when the header predates named kernels. */
  public int kernelVersion() {
    return kernelVersion;
  }

  public boolean legacy() {
    return kernel == null;
  }

  /**
   * Whether the given kernel can read this payload.
   *
   * <p>A payload without a kernel name is attributed to whichever kernel writes that role now,
   * which is how an index built before the header changed stays readable; a named payload has to
   * name the same kernel and the same version, since a different version is what a rebuild is for.
   */
  public boolean readableBy(VectorKernel reader) {
    Preconditions.checkNotNull(reader, "Reader kernel is null");
    if (legacy()) {
      return true;
    }

    return reader.name().equals(kernel) && reader.payloadFormatVersion() == kernelVersion;
  }

  @Override
  public String toString() {
    return legacy() ? "legacy header " + headerVersion : kernel + " version " + kernelVersion;
  }
}
