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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * Refreshes an index incrementally for a new source table snapshot, reusing the parent snapshot's
 * index files wherever the underlying source data files have not changed.
 *
 * <p>The caller computes what changed between the parent's source table snapshot and the new one
 * (typically from the table's snapshot history) and passes the source data files that were removed
 * or rewritten, plus the entries of the added and changed data files. Index files that cover a
 * removed or rewritten data file are dropped; the rest are carried over into the new manifest
 * without being rewritten. The new snapshot's parent is the previous snapshot, which is what makes
 * the reuse explicit and lets retention tell shared files from orphans.
 *
 * <p>Like the builder, the refresher only writes files and returns updated metadata; publishing
 * through the {@link IndexCatalog} is a separate step.
 */
public final class IndexRefresher {

  private final FileIO fileIO;
  private final IndexSnapshotWriter writer;

  public IndexRefresher(FileIO fileIO) {
    this.fileIO = Preconditions.checkNotNull(fileIO, "File IO is null");
    this.writer = new IndexSnapshotWriter(fileIO);
  }

  public IndexRefresher(FileIO fileIO, IndexSnapshotWriter writer) {
    this.fileIO = Preconditions.checkNotNull(fileIO, "File IO is null");
    this.writer = Preconditions.checkNotNull(writer, "Index snapshot writer is null");
  }

  /**
   * Refreshes the index for the request's source table snapshot.
   *
   * @param request the current metadata, the new source table snapshot, the entries of added and
   *     changed data files, and the publication timestamp
   * @param removedDataFiles source data files that were removed or rewritten since the parent
   *     snapshot; index files covering them are dropped
   * @return updated metadata with the new snapshot appended, not yet published
   * @throws IllegalArgumentException if the index already covers the source table snapshot or has
   *     no snapshot to refresh from
   */
  public IndexMetadata refresh(IndexBuildRequest request, Set<String> removedDataFiles) {
    IndexMetadata current = request.currentMetadata();
    Preconditions.checkArgument(
        current.snapshotForSourceTableSnapshot(request.sourceTableSnapshotId()) == null,
        "Index already covers source table snapshot: %s",
        request.sourceTableSnapshotId());
    IndexSnapshot parent = current.latestSnapshot();
    Preconditions.checkArgument(
        parent != null, "Cannot refresh an index without an index snapshot");

    IndexAdapter adapter = IndexBuilder.adapter(current);
    long snapshotId = IndexBuilder.nextSnapshotId(current);

    List<IndexFile> carriedOver = reusableIndexFiles(parent, removedDataFiles);
    List<IndexFile> newIndexFiles;
    String manifestList;
    try {
      newIndexFiles =
          writer.writeIndexFiles(current.location(), snapshotId, adapter, request.entries());
      List<IndexFile> indexFiles = Lists.newArrayList(carriedOver);
      indexFiles.addAll(newIndexFiles);
      manifestList = writer.writeManifestList(current.location(), snapshotId, indexFiles);
    } catch (RuntimeException e) {
      // carried-over files belong to the parent snapshot and must survive a failed refresh
      writer.abortAttempt(current.location());
      throw e;
    }
    writer.completeAttempt(current.location());
    // the ledger has served its purpose once the files are in place: the failure modes that remain
    // (the publish itself failing, or the process dying) leave at most unreferenced files, which
    // the cleanup sweep removes
    writer.finishAttempt(current.location());

    IndexSnapshot snapshot =
        IndexSnapshot.builder()
            .snapshotId(snapshotId)
            .sourceTableSnapshotId(request.sourceTableSnapshotId())
            .parentIndexSnapshotId(parent.snapshotId())
            .timestampMillis(request.timestampMillis())
            .indexManifestList(manifestList)
            .properties(request.properties())
            .build();
    return current.withSnapshot(snapshot);
  }

  /** Returns the parent's index files that do not cover any removed or rewritten data file. */
  private List<IndexFile> reusableIndexFiles(IndexSnapshot parent, Set<String> removedDataFiles) {
    List<IndexFile> reusable = Lists.newArrayList();
    for (IndexManifestList.ManifestRef ref :
        readManifestList(parent.indexManifestList()).manifests()) {
      for (IndexFile indexFile : readManifest(ref.location()).indexFiles()) {
        boolean removed = false;
        for (String dataFile : indexFile.coveredDataFiles()) {
          if (removedDataFiles.contains(dataFile)) {
            removed = true;
            break;
          }
        }
        if (!removed) {
          reusable.add(indexFile);
        }
      }
    }
    return ImmutableList.copyOf(reusable);
  }

  private IndexManifestList readManifestList(String location) {
    try {
      return IndexManifestList.read(fileIO.newInputFile(location));
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read index manifest list: " + location, e);
    }
  }

  private IndexManifest readManifest(String location) {
    try {
      return IndexManifest.read(fileIO.newInputFile(location));
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read index manifest: " + location, e);
    }
  }
}
