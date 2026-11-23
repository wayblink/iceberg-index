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
package org.apache.iceberg.index.text;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;

/** Addresses a payload by offset, from memory or from a file, without loading it whole. */
final class TextIndexSource {
  private TextIndexSource() {}

  interface Source {
    ByteBuffer read(int offset, int length) throws IOException;
  }

  static Source source(ByteBuffer bytes) {
    return (offset, length) -> bytes.slice(bytes.position() + offset, length);
  }

  static Source source(SeekableInputStream stream, long base) {
    return (offset, length) -> {
      byte[] bytes = new byte[length];
      if (stream instanceof RangeReadable range) {
        range.readFully(base + offset, bytes);
      } else {
        stream.seek(base + offset);
        IOUtil.readFully(stream, bytes, 0, length);
      }
      return ByteBuffer.wrap(bytes);
    };
  }
}
