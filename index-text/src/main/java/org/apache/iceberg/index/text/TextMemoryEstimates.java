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

import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.iceberg.index.IndexBuildResult;
import org.apache.iceberg.index.IndexFileMemory;
import org.apache.iceberg.index.IndexMemoryLayout;

/** Estimates the owned payload array and its prepared shard wrapper. */
final class TextMemoryEstimates {
  private static final int MODEL_VERSION = 2;
  private static final String PAYLOAD_BYTES = "payload-bytes";

  private TextMemoryEstimates() {}

  static IndexBuildResult result(ByteBuffer payload, int parallelism, String readerId) {
    String profile = IndexMemoryLayout.profile(parallelism);
    int size = payload.remaining();
    return IndexBuildResult.of(
        payload,
        profile == null
            ? null
            : IndexFileMemory.builder()
                .modelVersion(MODEL_VERSION)
                .payloadBytes(size)
                .preparedBytes(preparedBytes(size))
                .readerId(readerId)
                .runtimeProfile(profile)
                .structure(Map.of(PAYLOAD_BYTES, (long) size))
                .build());
  }

  static Long retainedBytes(long payloadBytes, Map<String, Long> structure) {
    Long size = structure.get(PAYLOAD_BYTES);
    return size != null
            && size == payloadBytes
            && size > 0
            && size <= Integer.MAX_VALUE
            && IndexMemoryLayout.supported()
        ? preparedBytes(size)
        : null;
  }

  static long preparedBytes(long size) {
    return IndexMemoryLayout.supported()
        ? IndexMemoryLayout.alignedSize(IndexMemoryLayout.REFERENCE_BYTES)
            + IndexMemoryLayout.alignedSize(size)
        : 0L;
  }
}
