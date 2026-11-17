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

import java.io.IOException;
import java.io.UncheckedIOException;
import org.apache.iceberg.index.vector.VectorIndexAdapter.RangeReadMetrics;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/** Byte ranges within one uncompressed IVF blob. */
final class IvfRangeInput implements AutoCloseable {
  private final SeekableInputStream stream;
  private final long offset;
  private final int length;
  private final String location;
  private long nanos;
  private long bytesRead;
  private long requests;

  IvfRangeInput(SeekableInputStream stream, long offset, int length, String location) {
    this.stream = stream;
    this.offset = offset;
    this.length = length;
    this.location = location;
  }

  int length() {
    return length;
  }

  byte[] read(int position, int size) {
    Preconditions.checkArgument(
        position >= 0 && size >= 0 && position <= length - size,
        "Invalid IVF range (%s, %s) of %s in %s",
        position,
        size,
        length,
        location);
    byte[] bytes = new byte[size];
    long started = System.nanoTime();
    try {
      if (stream instanceof RangeReadable range) {
        range.readFully(offset + position, bytes);
      } else {
        stream.seek(offset + position);
        IOUtil.readFully(stream, bytes, 0, size);
      }

      bytesRead += size;
      requests++;
      return bytes;
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read IVF range in " + location, e);
    } finally {
      nanos += System.nanoTime() - started;
    }
  }

  RangeReadMetrics metrics() {
    return new RangeReadMetrics(nanos, bytesRead, requests);
  }

  @Override
  public void close() {
    try {
      stream.close();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close IVF file " + location, e);
    }
  }
}
