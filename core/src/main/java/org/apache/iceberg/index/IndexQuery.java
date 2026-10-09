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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * A query against an index snapshot.
 *
 * <p>The source table snapshot selects the index snapshot to use; the query must match the snapshot
 * the reader is asked for. The payload is opaque to the generic layer and is encoded by the {@link
 * IndexAdapter} of the index type (for example, the vector adapter encodes a query vector).
 */
public final class IndexQuery {

  private final long sourceTableSnapshotId;
  private final ByteBuffer payload;
  private final int topK;

  private IndexQuery(long sourceTableSnapshotId, ByteBuffer payload, int topK) {
    Preconditions.checkArgument(
        sourceTableSnapshotId >= 0, "Invalid source table snapshot id: %s", sourceTableSnapshotId);
    Preconditions.checkNotNull(payload, "Query payload is null");
    Preconditions.checkArgument(topK >= 0, "Top-k must be non-negative: %s", topK);
    this.sourceTableSnapshotId = sourceTableSnapshotId;
    this.payload = payload;
    this.topK = topK;
  }

  public static IndexQuery of(long sourceTableSnapshotId, ByteBuffer payload, int topK) {
    return new IndexQuery(sourceTableSnapshotId, payload, topK);
  }

  public long sourceTableSnapshotId() {
    return sourceTableSnapshotId;
  }

  /** Returns a duplicate so callers cannot mutate the query payload. */
  public ByteBuffer payload() {
    return payload.duplicate();
  }

  public int topK() {
    return topK;
  }

  @Override
  public String toString() {
    return "IndexQuery{sourceTableSnapshotId=" + sourceTableSnapshotId + ", topK=" + topK + "}";
  }
}
