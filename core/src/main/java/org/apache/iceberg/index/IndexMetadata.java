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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

/**
 * The authoritative root metadata of an index, the counterpart of Iceberg {@code TableMetadata}.
 *
 * <p>Index metadata identifies the index and the table it belongs to (by {@code table-uuid}, so
 * table renames do not affect it), carries the {@link IndexDefinition} selected by {@code
 * index-type}, and lists the published {@link IndexSnapshot index snapshots}. It is stored as a
 * sidecar next to the table (see the index data model design) and is the atomic publication point:
 * a new index snapshot becomes visible only when this metadata is updated.
 *
 * <p>Invariants: snapshot ids are unique; each source table snapshot has at most one index
 * snapshot; a non-null parent refers to another snapshot of the same index.
 */
public final class IndexMetadata {

  public static final int FORMAT_VERSION = 1;

  /**
   * Index property holding the entry budget that groups source data files into index files: data
   * files are packed into a group until the budget is reached, and each group becomes one index
   * file. Zero, the default, means one index file per data file. A refresh keeps the budget the
   * index was built with.
   */
  public static final String MAX_ENTRIES_PER_INDEX_FILE_PROPERTY = "max-entries-per-index-file";

  private final String indexUuid;

  /** An optional human-readable name, so a table can hold several indexes of one column. */
  private final String name;

  private final String tableUuid;
  private final String tableLocation;
  private final String location;
  private final String indexType;
  private final IndexDefinition definition;
  private final Long lastUpdatedMillis;
  private final Map<String, String> properties;
  private final List<IndexSnapshot> snapshots;

  private IndexMetadata(
      String indexUuid,
      String name,
      String tableUuid,
      String tableLocation,
      String location,
      String indexType,
      IndexDefinition definition,
      Long lastUpdatedMillis,
      Map<String, String> properties,
      List<IndexSnapshot> snapshots) {
    Preconditions.checkArgument(indexUuid != null && !indexUuid.isEmpty(), "Index UUID is empty");
    Preconditions.checkArgument(tableUuid != null && !tableUuid.isEmpty(), "Table UUID is empty");
    Preconditions.checkArgument(location != null && !location.isEmpty(), "Index location is empty");
    Preconditions.checkArgument(indexType != null && !indexType.isEmpty(), "Index type is empty");
    Preconditions.checkNotNull(definition, "Index definition is null");
    Preconditions.checkArgument(
        indexType.equals(definition.type()),
        "Index type %s does not match definition type %s",
        indexType,
        definition.type());
    if (lastUpdatedMillis != null) {
      Preconditions.checkArgument(
          lastUpdatedMillis >= 0, "Invalid last updated timestamp: %s", lastUpdatedMillis);
    }

    Set<Long> snapshotIds = Sets.newHashSet();
    Set<Long> sourceSnapshotIds = Sets.newHashSet();
    for (IndexSnapshot snapshot : snapshots) {
      Preconditions.checkNotNull(snapshot, "Index snapshot is null");
      Preconditions.checkArgument(
          snapshotIds.add(snapshot.snapshotId()),
          "Duplicate index snapshot id: %s",
          snapshot.snapshotId());
      Preconditions.checkArgument(
          sourceSnapshotIds.add(snapshot.sourceTableSnapshotId()),
          "Duplicate source table snapshot id: %s",
          snapshot.sourceTableSnapshotId());
      Long parent = snapshot.parentIndexSnapshotId();
      if (parent != null) {
        Preconditions.checkArgument(
            !parent.equals(snapshot.snapshotId()),
            "Index snapshot %s cannot be its own parent",
            snapshot.snapshotId());
        Preconditions.checkArgument(
            snapshotsContainParent(snapshots, parent),
            "Parent index snapshot %s of snapshot %s is not part of this index",
            parent,
            snapshot.snapshotId());
      }
    }

    this.indexUuid = indexUuid;
    this.name = name;
    this.tableUuid = tableUuid;
    this.tableLocation = tableLocation;
    this.location = location;
    this.indexType = indexType;
    this.definition = definition;
    this.lastUpdatedMillis = lastUpdatedMillis;
    this.properties = ImmutableMap.copyOf(properties);
    this.snapshots = ImmutableList.copyOf(snapshots);
  }

  private static boolean snapshotsContainParent(List<IndexSnapshot> snapshots, Long parent) {
    for (IndexSnapshot snapshot : snapshots) {
      if (snapshot.snapshotId() == parent) {
        return true;
      }
    }
    return false;
  }

  public static Builder builder() {
    return new Builder();
  }

  public int formatVersion() {
    return FORMAT_VERSION;
  }

  public String indexUuid() {
    return indexUuid;
  }

  /**
   * The name this index was built with, or null.
   *
   * <p>A name is not the identity of an index -- that is the UUID -- but it is what a caller uses
   * to say which of several indexes over one column to search. An index built without one can only
   * be the only index over its column.
   */
  public String name() {
    return name;
  }

  public String tableUuid() {
    return tableUuid;
  }

  /** Returns the table location, if known, used only to resolve the table. */
  public String tableLocation() {
    return tableLocation;
  }

  public String location() {
    return location;
  }

  public String indexType() {
    return indexType;
  }

  public IndexDefinition definition() {
    return definition;
  }

  public Long lastUpdatedMillis() {
    return lastUpdatedMillis;
  }

  public Map<String, String> properties() {
    return properties;
  }

  /**
   * Returns the entry budget that groups data files into index files, or zero when each data file
   * gets an index file of its own.
   *
   * <p>The budget is an upper bound and never splits a data file, so a table whose data files are
   * larger than the budget keeps one index file per data file, and only a table with many small
   * data files sees them merged into larger index files, which is what makes an HNSW graph span
   * more vectors. See {@link #MAX_ENTRIES_PER_INDEX_FILE_PROPERTY}.
   */
  public long maxEntriesPerIndexFile() {
    String value = properties.get(MAX_ENTRIES_PER_INDEX_FILE_PROPERTY);
    if (value == null) {
      return 0L;
    }
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "Invalid " + MAX_ENTRIES_PER_INDEX_FILE_PROPERTY + " property: " + value, e);
    }
  }

  public List<IndexSnapshot> snapshots() {
    return snapshots;
  }

  /**
   * Returns the index snapshot built for a source table snapshot, or null when the index does not
   * cover that table snapshot. The returned snapshot is only valid for the requested source table
   * snapshot; callers must not use it for any other table snapshot.
   */
  public IndexSnapshot snapshotForSourceTableSnapshot(long sourceTableSnapshotId) {
    for (IndexSnapshot snapshot : snapshots) {
      if (snapshot.sourceTableSnapshotId() == sourceTableSnapshotId) {
        return snapshot;
      }
    }
    return null;
  }

  /**
   * Returns the index snapshot with the highest snapshot id (the tip of the index history chain),
   * or null when the index has no snapshots yet.
   */
  public IndexSnapshot latestSnapshot() {
    IndexSnapshot latest = null;
    for (IndexSnapshot snapshot : snapshots) {
      if (latest == null || snapshot.snapshotId() > latest.snapshotId()) {
        latest = snapshot;
      }
    }
    return latest;
  }

  /**
   * Returns a copy of this metadata with the given snapshot appended.
   *
   * <p>The snapshot must satisfy the index invariants (unique snapshot id, uncovered source table
   * snapshot, and a parent that belongs to this index). Publishing the result is a separate,
   * explicit step through the {@link IndexCatalog}.
   */
  public IndexMetadata withSnapshot(IndexSnapshot snapshot) {
    return toBuilder().addSnapshot(snapshot).build();
  }

  /** Returns a builder pre-populated with this metadata's fields. */
  public Builder toBuilder() {
    Builder builder =
        builder()
            .indexUuid(indexUuid)
            .name(name)
            .tableUuid(tableUuid)
            .location(location)
            .indexType(indexType)
            .definition(definition)
            .properties(properties);
    if (tableLocation != null) {
      builder.tableLocation(tableLocation);
    }
    if (lastUpdatedMillis != null) {
      builder.lastUpdatedMillis(lastUpdatedMillis);
    }
    for (IndexSnapshot snapshot : snapshots) {
      builder.addSnapshot(snapshot);
    }
    return builder;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof IndexMetadata)) {
      return false;
    }
    IndexMetadata that = (IndexMetadata) other;
    return indexUuid.equals(that.indexUuid)
        && tableUuid.equals(that.tableUuid)
        && Objects.equals(tableLocation, that.tableLocation)
        && location.equals(that.location)
        && indexType.equals(that.indexType)
        && definition.equals(that.definition)
        && Objects.equals(lastUpdatedMillis, that.lastUpdatedMillis)
        && Objects.equals(name, that.name)
        && properties.equals(that.properties)
        && snapshots.equals(that.snapshots);
  }

  @Override
  public int hashCode() {
    int result = indexUuid.hashCode();
    result = 31 * result + Objects.hashCode(name);
    result = 31 * result + tableUuid.hashCode();
    result = 31 * result + Objects.hashCode(tableLocation);
    result = 31 * result + location.hashCode();
    result = 31 * result + indexType.hashCode();
    result = 31 * result + definition.hashCode();
    result = 31 * result + Objects.hashCode(lastUpdatedMillis);
    result = 31 * result + properties.hashCode();
    result = 31 * result + snapshots.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("indexUuid", indexUuid)
        .add("name", name)
        .add("tableUuid", tableUuid)
        .add("tableLocation", tableLocation)
        .add("location", location)
        .add("indexType", indexType)
        .add("definition", definition)
        .add("lastUpdatedMillis", lastUpdatedMillis)
        .add("properties", properties)
        .add("snapshots", snapshots)
        .toString();
  }

  public static final class Builder {
    private String indexUuid;
    private String name;
    private String tableUuid;
    private String tableLocation;
    private String location;
    private String indexType;
    private IndexDefinition definition;
    private Long lastUpdatedMillis;
    private final ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
    private final ImmutableList.Builder<IndexSnapshot> snapshots = ImmutableList.builder();

    private Builder() {}

    public Builder indexUuid(String newIndexUuid) {
      this.indexUuid = newIndexUuid;
      return this;
    }

    public Builder name(String newName) {
      this.name = newName;
      return this;
    }

    public Builder tableUuid(String newTableUuid) {
      this.tableUuid = newTableUuid;
      return this;
    }

    public Builder tableLocation(String newTableLocation) {
      this.tableLocation = newTableLocation;
      return this;
    }

    public Builder location(String newLocation) {
      this.location = newLocation;
      return this;
    }

    public Builder indexType(String newIndexType) {
      this.indexType = newIndexType;
      return this;
    }

    public Builder definition(IndexDefinition newDefinition) {
      this.definition = newDefinition;
      if (indexType == null && newDefinition != null) {
        this.indexType = newDefinition.type();
      }
      return this;
    }

    public Builder lastUpdatedMillis(long newLastUpdatedMillis) {
      this.lastUpdatedMillis = newLastUpdatedMillis;
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

    public Builder addSnapshot(IndexSnapshot snapshot) {
      Preconditions.checkNotNull(snapshot, "Index snapshot is null");
      snapshots.add(snapshot);
      return this;
    }

    public IndexMetadata build() {
      Preconditions.checkNotNull(indexUuid, "Index UUID is not set");
      Preconditions.checkNotNull(tableUuid, "Table UUID is not set");
      Preconditions.checkNotNull(location, "Index location is not set");
      Preconditions.checkNotNull(indexType, "Index type is not set");
      Preconditions.checkNotNull(definition, "Index definition is not set");
      return new IndexMetadata(
          indexUuid,
          name,
          tableUuid,
          tableLocation,
          location,
          indexType,
          definition,
          lastUpdatedMillis,
          properties.build(),
          snapshots.build());
    }
  }
}
