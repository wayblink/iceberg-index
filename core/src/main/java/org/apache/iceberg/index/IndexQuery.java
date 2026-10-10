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
 *
 * <p>How strictly the requested source table snapshot has to be covered is expressed by the {@link
 * Consistency}. The default requires an index snapshot bound to exactly that table snapshot, which
 * is what a planner uses after it has matched the two. A caller that accepts a stale index can ask
 * for the newest index snapshot at or before the requested table snapshot, and must then consult
 * {@link IndexReader#uncoveredDataFiles} to find out what the index does not cover.
 */
public final class IndexQuery {

  /** How closely the index snapshot has to be bound to the requested source table snapshot. */
  public enum Consistency {
    /** The index must have a snapshot bound to exactly the requested source table snapshot. */
    EXACT_SNAPSHOT,
    /** The newest index snapshot at or before the requested source table snapshot may be used. */
    AT_OR_BEFORE
  }

  private final long sourceTableSnapshotId;
  private final ByteBuffer payload;
  private final int topK;
  private final Consistency consistency;

  private IndexQuery(
      long sourceTableSnapshotId, ByteBuffer payload, int topK, Consistency consistency) {
    Preconditions.checkArgument(
        sourceTableSnapshotId >= 0, "Invalid source table snapshot id: %s", sourceTableSnapshotId);
    Preconditions.checkNotNull(payload, "Query payload is null");
    Preconditions.checkArgument(topK >= 0, "Top-k must be non-negative: %s", topK);
    Preconditions.checkNotNull(consistency, "Consistency is null");
    this.sourceTableSnapshotId = sourceTableSnapshotId;
    this.payload = payload;
    this.topK = topK;
    this.consistency = consistency;
  }

  public static IndexQuery of(long sourceTableSnapshotId, ByteBuffer payload, int topK) {
    return new IndexQuery(sourceTableSnapshotId, payload, topK, Consistency.EXACT_SNAPSHOT);
  }

  /** Returns a copy of this query that accepts an index snapshot at or before the requested one. */
  public IndexQuery withConsistency(Consistency newConsistency) {
    return new IndexQuery(sourceTableSnapshotId, payload, topK, newConsistency);
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

  public Consistency consistency() {
    return consistency;
  }

  @Override
  public String toString() {
    return "IndexQuery{sourceTableSnapshotId="
        + sourceTableSnapshotId
        + ", topK="
        + topK
        + ", consistency="
        + consistency
        + "}";
  }
}
