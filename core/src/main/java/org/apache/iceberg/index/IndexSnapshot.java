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

import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

/**
 * An immutable index build for one source table snapshot.
 *
 * <p>This is the generic counterpart of an Iceberg table {@code Snapshot}: it mirrors the table
 * snapshot in structure, but not in authority. A table snapshot <em>is</em> the authoritative
 * state, while an index snapshot is a <em>derived build</em> that answers "for table snapshot S,
 * which version of this index was built and considered usable at some point in time".
 *
 * <p>Field roles:
 *
 * <ul>
 *   <li>{@code snapshotId}: identity of this index snapshot, unique within the index.
 *   <li>{@code sourceTableSnapshotId}: <b>binding</b> - the table snapshot this build was derived
 *       from. This index snapshot is only valid for that table snapshot. Readers and planners must
 *       verify the binding before using it, and must refresh or fall back once the table has
 *       advanced.
 *   <li>{@code parentIndexSnapshotId}: <b>lineage</b> - the index snapshot this build was derived
 *       from, mirroring {@code parent-snapshot-id}; {@code null} for a full rebuild. The parent
 *       link enables incremental refresh (reusing index files for unchanged source data files),
 *       bounds compaction scope, and makes orphan-file cleanup safe.
 *   <li>{@code timestampMillis}: when the build was published. Used for auditing and time-travel;
 *       never used for correctness decisions.
 *   <li>{@code status}: publication status; only {@link Status#PUBLISHED} snapshots may be used by
 *       readers and planners.
 *   <li>{@code indexManifestList}: the root of this snapshot's file listing, equivalent to a table
 *       snapshot's {@code manifest-list}.
 * </ul>
 *
 * <p>Invariants: an index has at most one snapshot per source table snapshot; the parent is null or
 * its source table snapshot is an ancestor of this snapshot's source; index snapshots and their
 * files are immutable, so change is only ever expressed by publishing a new snapshot.
 */
public final class IndexSnapshot {

  /** Publication status of an index snapshot. */
  public enum Status {
    /** Built and published; readers and planners may use it. */
    PUBLISHED
  }

  private final long snapshotId;
  private final long sourceTableSnapshotId;
  private final Long parentIndexSnapshotId;
  private final long timestampMillis;
  private final Status status;
  private final String indexManifestList;
  private final Map<String, String> properties;

  private IndexSnapshot(
      long snapshotId,
      long sourceTableSnapshotId,
      Long parentIndexSnapshotId,
      long timestampMillis,
      Status status,
      String indexManifestList,
      Map<String, String> properties) {
    Preconditions.checkArgument(snapshotId >= 0, "Invalid index snapshot id: %s", snapshotId);
    Preconditions.checkArgument(
        sourceTableSnapshotId >= 0, "Invalid source table snapshot id: %s", sourceTableSnapshotId);
    if (parentIndexSnapshotId != null) {
      Preconditions.checkArgument(
          parentIndexSnapshotId >= 0,
          "Invalid parent index snapshot id: %s",
          parentIndexSnapshotId);
      Preconditions.checkArgument(
          !parentIndexSnapshotId.equals(snapshotId),
          "Index snapshot cannot be its own parent: %s",
          snapshotId);
    }
    Preconditions.checkArgument(timestampMillis >= 0, "Invalid timestamp: %s", timestampMillis);
    Preconditions.checkNotNull(status, "Index snapshot status is null");
    Preconditions.checkArgument(
        indexManifestList != null && !indexManifestList.isEmpty(),
        "Index manifest list location is empty");
    this.snapshotId = snapshotId;
    this.sourceTableSnapshotId = sourceTableSnapshotId;
    this.parentIndexSnapshotId = parentIndexSnapshotId;
    this.timestampMillis = timestampMillis;
    this.status = status;
    this.indexManifestList = indexManifestList;
    this.properties = ImmutableMap.copyOf(properties);
  }

  public static Builder builder() {
    return new Builder();
  }

  public long snapshotId() {
    return snapshotId;
  }

  public long sourceTableSnapshotId() {
    return sourceTableSnapshotId;
  }

  /** Returns the index snapshot this build was derived from, or null for a full rebuild. */
  public Long parentIndexSnapshotId() {
    return parentIndexSnapshotId;
  }

  public long timestampMillis() {
    return timestampMillis;
  }

  public Status status() {
    return status;
  }

  public String indexManifestList() {
    return indexManifestList;
  }

  public Map<String, String> properties() {
    return properties;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof IndexSnapshot)) {
      return false;
    }
    IndexSnapshot that = (IndexSnapshot) other;
    return snapshotId == that.snapshotId
        && sourceTableSnapshotId == that.sourceTableSnapshotId
        && timestampMillis == that.timestampMillis
        && java.util.Objects.equals(parentIndexSnapshotId, that.parentIndexSnapshotId)
        && status == that.status
        && indexManifestList.equals(that.indexManifestList)
        && properties.equals(that.properties);
  }

  @Override
  public int hashCode() {
    int result = Long.hashCode(snapshotId);
    result = 31 * result + Long.hashCode(sourceTableSnapshotId);
    result = 31 * result + java.util.Objects.hashCode(parentIndexSnapshotId);
    result = 31 * result + Long.hashCode(timestampMillis);
    result = 31 * result + status.hashCode();
    result = 31 * result + indexManifestList.hashCode();
    result = 31 * result + properties.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("snapshotId", snapshotId)
        .add("sourceTableSnapshotId", sourceTableSnapshotId)
        .add("parentIndexSnapshotId", parentIndexSnapshotId)
        .add("timestampMillis", timestampMillis)
        .add("status", status)
        .add("indexManifestList", indexManifestList)
        .add("properties", properties)
        .toString();
  }

  /** Returns a builder copy of this snapshot, for lineage edits such as truncating a parent. */
  public Builder toBuilder() {
    return new Builder()
        .snapshotId(snapshotId)
        .sourceTableSnapshotId(sourceTableSnapshotId)
        .parentIndexSnapshotId(parentIndexSnapshotId)
        .timestampMillis(timestampMillis)
        .status(status)
        .indexManifestList(indexManifestList)
        .properties(properties);
  }

  public static final class Builder {
    private Long snapshotId;
    private Long sourceTableSnapshotId;
    private Long parentIndexSnapshotId;
    private Long timestampMillis;
    private Status status = Status.PUBLISHED;
    private String indexManifestList;
    private final ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();

    private Builder() {}

    public Builder snapshotId(long newSnapshotId) {
      this.snapshotId = newSnapshotId;
      return this;
    }

    public Builder sourceTableSnapshotId(long newSourceTableSnapshotId) {
      this.sourceTableSnapshotId = newSourceTableSnapshotId;
      return this;
    }

    /** Sets the parent index snapshot, or null for a full rebuild. */
    public Builder parentIndexSnapshotId(Long newParentIndexSnapshotId) {
      this.parentIndexSnapshotId = newParentIndexSnapshotId;
      return this;
    }

    public Builder timestampMillis(long newTimestampMillis) {
      this.timestampMillis = newTimestampMillis;
      return this;
    }

    public Builder status(Status newStatus) {
      this.status = newStatus;
      return this;
    }

    public Builder indexManifestList(String newIndexManifestList) {
      this.indexManifestList = newIndexManifestList;
      return this;
    }

    public Builder property(String key, String value) {
      properties.put(key, value);
      return this;
    }

    public Builder properties(Map<String, String> newProperties) {
      properties.putAll(newProperties);
      return this;
    }

    public IndexSnapshot build() {
      Preconditions.checkNotNull(snapshotId, "Index snapshot id is not set");
      Preconditions.checkNotNull(sourceTableSnapshotId, "Source table snapshot id is not set");
      Preconditions.checkNotNull(timestampMillis, "Timestamp is not set");
      return new IndexSnapshot(
          snapshotId,
          sourceTableSnapshotId,
          parentIndexSnapshotId,
          timestampMillis,
          status,
          indexManifestList,
          properties.build());
    }
  }
}
