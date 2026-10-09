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
 * A request to build or refresh an index snapshot.
 *
 * <p>The entries are the index entries to write for the source table snapshot: the full set for a
 * full build ({@link IndexBuilder}), or only the entries of added and changed data files for an
 * incremental refresh ({@link IndexRefresher}).
 */
public final class IndexBuildRequest {

  private final IndexMetadata currentMetadata;
  private final long sourceTableSnapshotId;
  private final Iterable<IndexEntry> entries;
  private final long timestampMillis;
  private final Map<String, String> properties;

  private IndexBuildRequest(
      IndexMetadata currentMetadata,
      long sourceTableSnapshotId,
      Iterable<IndexEntry> entries,
      long timestampMillis,
      Map<String, String> properties) {
    Preconditions.checkNotNull(currentMetadata, "Current index metadata is null");
    Preconditions.checkArgument(
        sourceTableSnapshotId >= 0, "Invalid source table snapshot id: %s", sourceTableSnapshotId);
    Preconditions.checkNotNull(entries, "Index entries are null");
    Preconditions.checkArgument(timestampMillis >= 0, "Invalid timestamp: %s", timestampMillis);
    this.currentMetadata = currentMetadata;
    this.sourceTableSnapshotId = sourceTableSnapshotId;
    this.entries = entries;
    this.timestampMillis = timestampMillis;
    this.properties = ImmutableMap.copyOf(properties);
  }

  public static Builder builder() {
    return new Builder();
  }

  public IndexMetadata currentMetadata() {
    return currentMetadata;
  }

  public long sourceTableSnapshotId() {
    return sourceTableSnapshotId;
  }

  public Iterable<IndexEntry> entries() {
    return entries;
  }

  public long timestampMillis() {
    return timestampMillis;
  }

  public Map<String, String> properties() {
    return properties;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("indexUuid", currentMetadata.indexUuid())
        .add("sourceTableSnapshotId", sourceTableSnapshotId)
        .add("timestampMillis", timestampMillis)
        .add("properties", properties)
        .toString();
  }

  public static final class Builder {
    private IndexMetadata currentMetadata;
    private Long sourceTableSnapshotId;
    private Iterable<IndexEntry> entries;
    private Long timestampMillis;
    private final ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();

    private Builder() {}

    public Builder currentMetadata(IndexMetadata newCurrentMetadata) {
      this.currentMetadata = newCurrentMetadata;
      return this;
    }

    public Builder sourceTableSnapshotId(long newSourceTableSnapshotId) {
      this.sourceTableSnapshotId = newSourceTableSnapshotId;
      return this;
    }

    public Builder entries(Iterable<IndexEntry> newEntries) {
      this.entries = newEntries;
      return this;
    }

    public Builder timestampMillis(long newTimestampMillis) {
      this.timestampMillis = newTimestampMillis;
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

    public IndexBuildRequest build() {
      Preconditions.checkNotNull(currentMetadata, "Current index metadata is not set");
      Preconditions.checkNotNull(sourceTableSnapshotId, "Source table snapshot id is not set");
      Preconditions.checkNotNull(entries, "Index entries are not set");
      Preconditions.checkNotNull(timestampMillis, "Timestamp is not set");
      return new IndexBuildRequest(
          currentMetadata, sourceTableSnapshotId, entries, timestampMillis, properties.build());
    }
  }
}
