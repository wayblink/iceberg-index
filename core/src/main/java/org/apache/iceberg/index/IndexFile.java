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
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

/**
 * An immutable index file: one payload file of an index snapshot, as declared by an {@link
 * IndexManifest}.
 *
 * <p>An index file has a {@link Kind} and a blob type. Entry-bearing files contain {@link
 * IndexEntry index entries} that resolve to source table rows; auxiliary files carry supporting
 * payloads such as graph layers or quantizers. The physical file is stored in an {@link
 * IndexFileContainer}, which is Puffin by default. The file also declares which source data files
 * it covers, which is what lets a planner detect files that still need a residual scan.
 */
public final class IndexFile {

  /** Kinds of index files. */
  public enum Kind {
    /** Contains entries that reference source table rows. */
    ENTRY_BEARING,
    /** Contains supporting payloads that do not directly reference table rows. */
    AUXILIARY
  }

  private final String location;
  private final Kind kind;
  private final String blobType;
  private final long entryCount;
  private final List<String> coveredDataFiles;

  private IndexFile(
      String location, Kind kind, String blobType, long entryCount, List<String> coveredDataFiles) {
    Preconditions.checkArgument(
        location != null && !location.isEmpty(), "Index file location is empty");
    Preconditions.checkNotNull(kind, "Index file kind is null");
    Preconditions.checkArgument(
        blobType != null && !blobType.isEmpty(), "Index file blob type is empty");
    Preconditions.checkArgument(entryCount >= 0, "Invalid index file entry count: %s", entryCount);
    Preconditions.checkNotNull(coveredDataFiles, "Covered data files are null");
    for (String dataFile : coveredDataFiles) {
      Preconditions.checkArgument(
          dataFile != null && !dataFile.isEmpty(), "Covered data file location is empty");
    }
    this.location = location;
    this.kind = kind;
    this.blobType = blobType;
    this.entryCount = entryCount;
    this.coveredDataFiles = ImmutableList.copyOf(coveredDataFiles);
  }

  public static Builder builder() {
    return new Builder();
  }

  public String location() {
    return location;
  }

  public Kind kind() {
    return kind;
  }

  public String blobType() {
    return blobType;
  }

  public long entryCount() {
    return entryCount;
  }

  /** Returns the source data files covered by this index file. */
  public List<String> coveredDataFiles() {
    return coveredDataFiles;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof IndexFile)) {
      return false;
    }
    IndexFile that = (IndexFile) other;
    return entryCount == that.entryCount
        && location.equals(that.location)
        && kind == that.kind
        && blobType.equals(that.blobType)
        && coveredDataFiles.equals(that.coveredDataFiles);
  }

  @Override
  public int hashCode() {
    int result = location.hashCode();
    result = 31 * result + kind.hashCode();
    result = 31 * result + blobType.hashCode();
    result = 31 * result + Long.hashCode(entryCount);
    result = 31 * result + coveredDataFiles.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("location", location)
        .add("kind", kind)
        .add("blobType", blobType)
        .add("entryCount", entryCount)
        .add("coveredDataFiles", coveredDataFiles)
        .toString();
  }

  public static final class Builder {
    private String location;
    private Kind kind;
    private String blobType;
    private Long entryCount;
    private final ImmutableList.Builder<String> coveredDataFiles = ImmutableList.builder();

    private Builder() {}

    public Builder location(String newLocation) {
      this.location = newLocation;
      return this;
    }

    public Builder kind(Kind newKind) {
      this.kind = newKind;
      return this;
    }

    public Builder blobType(String newBlobType) {
      this.blobType = newBlobType;
      return this;
    }

    public Builder entryCount(long newEntryCount) {
      this.entryCount = newEntryCount;
      return this;
    }

    public Builder addCoveredDataFile(String dataFile) {
      coveredDataFiles.add(dataFile);
      return this;
    }

    public Builder coveredDataFiles(List<String> newCoveredDataFiles) {
      coveredDataFiles.addAll(newCoveredDataFiles);
      return this;
    }

    public IndexFile build() {
      Preconditions.checkNotNull(location, "Index file location is not set");
      Preconditions.checkNotNull(kind, "Index file kind is not set");
      Preconditions.checkNotNull(blobType, "Index file blob type is not set");
      Preconditions.checkNotNull(entryCount, "Index file entry count is not set");
      return new IndexFile(location, kind, blobType, entryCount, coveredDataFiles.build());
    }
  }
}
