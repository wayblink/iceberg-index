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
package org.apache.iceberg.index.spark;

import org.apache.iceberg.index.IndexFileMemory;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/** Converts compatible blob-build estimates to the executor's retained-container cache charge. */
final class IndexCacheMemory {
  private IndexCacheMemory() {}

  /** Requires a matching reader model/runtime profile and a known container length. */
  static long residentBytes(IndexFileMemory memory) {
    Preconditions.checkArgument(memory != null, "Index file memory is unknown");
    Preconditions.checkArgument(memory.fileBytes() != null, "Index container length is unknown");
    // The whole Puffin file replaces the blob buffer; both buffers are not charged together.
    return residentBytes(memory.fileBytes(), memory.preparedBytes());
  }

  static long residentBytes(long retainedBufferBytes, long preparedBytes) {
    return Math.addExact(
        retainedBufferBytes, preparedBytes > 0 ? preparedBytes : retainedBufferBytes);
  }
}
