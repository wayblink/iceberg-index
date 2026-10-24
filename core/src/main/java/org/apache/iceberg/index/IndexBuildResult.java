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
package org.apache.iceberg.index;

import java.nio.ByteBuffer;
import javax.annotation.Nullable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/** An encoded index payload and its optional retained-memory estimate. */
public final class IndexBuildResult {
  private final ByteBuffer payload;
  private final IndexFileMemory memory;

  private IndexBuildResult(ByteBuffer payload, IndexFileMemory memory) {
    Preconditions.checkArgument(payload != null, "Index build payload is null");
    if (memory != null) {
      Preconditions.checkArgument(
          memory.payloadBytes() == payload.remaining(),
          "Index build memory payload bytes do not match encoded payload: %s != %s",
          memory.payloadBytes(),
          payload.remaining());
    }

    this.payload = payload.duplicate();
    this.memory = memory;
  }

  public static IndexBuildResult of(ByteBuffer payload, @Nullable IndexFileMemory memory) {
    return new IndexBuildResult(payload, memory);
  }

  /** Returns a view of the encoded payload with an independent position and limit. */
  public ByteBuffer payload() {
    return payload.duplicate();
  }

  /** Returns the retained-memory estimate, or null when it is unknown. */
  @Nullable
  public IndexFileMemory memory() {
    return memory;
  }
}
