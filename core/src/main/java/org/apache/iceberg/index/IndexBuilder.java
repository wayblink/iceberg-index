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
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Builds a full index snapshot for one source table snapshot.
 *
 * <p>The builder is engine-neutral: the caller supplies the index entries (typically read from the
 * table's data files by an engine such as Spark) and the builder writes the index files, the
 * manifest and the manifest list, then returns updated metadata with the new snapshot appended.
 * Publishing that metadata through the {@link IndexCatalog} is a separate step, which is what makes
 * the new snapshot visible to readers.
 *
 * <p>A full build has no parent: it derives nothing from the previous snapshot. Use {@link
 * IndexRefresher} to reuse unchanged index files.
 */
public final class IndexBuilder {

  private final IndexSnapshotWriter writer;

  public IndexBuilder(FileIO fileIO) {
    this.writer = new IndexSnapshotWriter(fileIO);
  }

  public IndexBuilder(IndexSnapshotWriter writer) {
    this.writer = Preconditions.checkNotNull(writer, "Index snapshot writer is null");
  }

  /**
   * Builds a full index snapshot for the request's source table snapshot.
   *
   * @param request the current metadata, the source table snapshot, the entries to index and the
   *     publication timestamp
   * @return updated metadata with the new snapshot appended, not yet published
   * @throws IllegalArgumentException if the index already covers the source table snapshot
   */
  public IndexMetadata build(IndexBuildRequest request) {
    IndexMetadata current = request.currentMetadata();
    Preconditions.checkArgument(
        current.snapshotForSourceTableSnapshot(request.sourceTableSnapshotId()) == null,
        "Index already covers source table snapshot: %s",
        request.sourceTableSnapshotId());
    IndexAdapter adapter = adapter(current);
    long snapshotId = nextSnapshotId(current);

    List<IndexFile> indexFiles =
        writer.writeIndexFiles(current.location(), snapshotId, adapter, request.entries());
    String manifestList = writer.writeManifestList(current.location(), snapshotId, indexFiles);

    IndexSnapshot snapshot =
        IndexSnapshot.builder()
            .snapshotId(snapshotId)
            .sourceTableSnapshotId(request.sourceTableSnapshotId())
            .parentIndexSnapshotId(null)
            .timestampMillis(request.timestampMillis())
            .indexManifestList(manifestList)
            .properties(request.properties())
            .build();
    return current.withSnapshot(snapshot);
  }

  static IndexAdapter adapter(IndexMetadata metadata) {
    return IndexTypes.fromType(metadata.indexType()).createAdapter(metadata.definition());
  }

  static long nextSnapshotId(IndexMetadata metadata) {
    IndexSnapshot latest = metadata.latestSnapshot();
    return latest == null ? 1L : latest.snapshotId() + 1;
  }
}
