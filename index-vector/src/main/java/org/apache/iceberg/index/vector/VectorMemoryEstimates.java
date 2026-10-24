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

import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.iceberg.index.IndexBuildResult;
import org.apache.iceberg.index.IndexFileMemory;

final class VectorMemoryEstimates {
  private static final int MODEL_VERSION = 1;

  private VectorMemoryEstimates() {}

  static IndexBuildResult result(
      VectorKernel kernel,
      ByteBuffer payload,
      long preparedBytes,
      int processors,
      Map<String, Long> structure) {
    return IndexBuildResult.of(
        payload,
        IndexFileMemory.builder()
            .payloadBytes(payload.remaining())
            .preparedBytes(preparedBytes)
            .modelVersion(MODEL_VERSION)
            .readerId("vector/" + kernel.name() + "/" + kernel.payloadFormatVersion())
            .runtimeProfile(profile(processors))
            .structure(structure)
            .build());
  }

  static String profile(int processors) {
    return "reference-bytes="
        + PreparedStructureSize.REFERENCE_BYTES
        + ";alignment="
        + PreparedStructureSize.ALIGNMENT
        + ";compact-strings=true;processors="
        + processors;
  }

  static long dictionaryBytes(Iterable<String> dataFiles) {
    long bytes = 0;
    for (String dataFile : dataFiles) {
      bytes = Math.addExact(bytes, PreparedStructureSize.string(dataFile));
    }

    return bytes;
  }
}
