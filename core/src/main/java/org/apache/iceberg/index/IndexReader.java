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
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

/**
 * Reads an index snapshot and exposes exact top-k search and coverage information.
 *
 * <p>The reader is engine-neutral: it takes an {@link IndexQuery}, resolves the index snapshot
 * bound to the query's source table snapshot, searches the entry-bearing index files, and returns
 * row references and scores. Callers resolve the references back to table rows themselves, which is
 * where filtering, delete semantics, and final projection happen.
 *
 * <p>Correctness rules enforced here: by default the index snapshot must be bound to the requested
 * source table snapshot (otherwise the query fails), and files that the index does not cover are
 * reported so callers can run a residual scan instead of assuming the index covers the whole table.
 * A query that asks for {@link IndexQuery.Consistency#AT_OR_BEFORE} is answered from the newest
 * index snapshot at or before the requested source table snapshot, which may be stale; the coverage
 * report then tells the caller what the index does not cover.
 */
public final class IndexReader {

  private final IndexMetadata metadata;
  private final FileIO fileIO;
  private final IndexFileContainer container;

  public IndexReader(IndexMetadata metadata, FileIO fileIO) {
    this(metadata, fileIO, new PuffinIndexFileContainer());
  }

  public IndexReader(IndexMetadata metadata, FileIO fileIO, IndexFileContainer container) {
    this.metadata = Preconditions.checkNotNull(metadata, "Index metadata is null");
    this.fileIO = Preconditions.checkNotNull(fileIO, "File IO is null");
    this.container = Preconditions.checkNotNull(container, "Index file container is null");
  }

  /**
   * Searches the index snapshot built from the requested source table snapshot.
   *
   * @throws IllegalArgumentException if the index does not cover the source table snapshot
   */
  public List<IndexSearchResult> search(IndexQuery query) {
    Preconditions.checkNotNull(query, "Index query is null");
    IndexSnapshot snapshot = snapshotForQuery(query);
    IndexAdapter adapter =
        IndexTypes.fromType(metadata.indexType()).createAdapter(metadata.definition());

    IndexManifestList manifestList = readManifestList(snapshot);
    if (adapter.searchesAllPayloadsTogether()) {
      return searchCorrelated(adapter, manifestList, query);
    }

    String auxiliaryBlobType = adapter.auxiliaryBlobType();
    List<IndexSearchResult> results = Lists.newArrayList();
    for (IndexManifestList.ManifestRef ref : manifestList.manifests()) {
      IndexManifest manifest = readManifest(ref.location());
      for (IndexFile indexFile : manifest.indexFiles()) {
        if (indexFile.kind() == IndexFile.Kind.ENTRY_BEARING
            && adapter.entryBlobType().equals(indexFile.blobType())) {
          IndexBlob blob = readBlob(indexFile.location(), adapter.entryBlobType());
          results.addAll(
              adapter.search(ByteBuffer.wrap(blob.payload()), query.payload(), query.topK()));
        } else if (auxiliaryBlobType != null
            && indexFile.kind() == IndexFile.Kind.AUXILIARY
            && auxiliaryBlobType.equals(indexFile.blobType())) {
          IndexBlob blob = readBlob(indexFile.location(), auxiliaryBlobType);
          results.addAll(
              adapter.searchAuxiliary(
                  ByteBuffer.wrap(blob.payload()), query.payload(), query.topK()));
        }
        // other payload types are skipped for forward compatibility
      }
    }

    results.sort(adapter.resultComparator());
    if (results.size() > query.topK()) {
      results = Lists.newArrayList(results.subList(0, query.topK()));
    }
    return ImmutableList.copyOf(results);
  }

  /**
   * Searches with an adapter that needs all payloads of the snapshot together: the payloads of each
   * kind are collected and handed to the adapter, which merges the statistics they carry before
   * scoring. See {@link IndexAdapter#searchesAllPayloadsTogether()}.
   */
  private List<IndexSearchResult> searchCorrelated(
      IndexAdapter adapter, IndexManifestList manifestList, IndexQuery query) {
    String auxiliaryBlobType = adapter.auxiliaryBlobType();
    List<ByteBuffer> entryPayloads = Lists.newArrayList();
    List<ByteBuffer> auxiliaryPayloads = Lists.newArrayList();
    for (IndexManifestList.ManifestRef ref : manifestList.manifests()) {
      IndexManifest manifest = readManifest(ref.location());
      for (IndexFile indexFile : manifest.indexFiles()) {
        if (indexFile.kind() == IndexFile.Kind.ENTRY_BEARING
            && adapter.entryBlobType().equals(indexFile.blobType())) {
          IndexBlob blob = readBlob(indexFile.location(), adapter.entryBlobType());
          entryPayloads.add(ByteBuffer.wrap(blob.payload()));
        } else if (auxiliaryBlobType != null
            && indexFile.kind() == IndexFile.Kind.AUXILIARY
            && auxiliaryBlobType.equals(indexFile.blobType())) {
          IndexBlob blob = readBlob(indexFile.location(), auxiliaryBlobType);
          auxiliaryPayloads.add(ByteBuffer.wrap(blob.payload()));
        }
        // other payload types are skipped for forward compatibility
      }
    }

    List<IndexSearchResult> results = Lists.newArrayList();
    if (!entryPayloads.isEmpty()) {
      results.addAll(adapter.searchAll(entryPayloads, query.payload(), query.topK()));
    }
    if (!auxiliaryPayloads.isEmpty()) {
      results.addAll(adapter.searchAuxiliaryAll(auxiliaryPayloads, query.payload(), query.topK()));
    }

    results.sort(adapter.resultComparator());
    if (results.size() > query.topK()) {
      results = Lists.newArrayList(results.subList(0, query.topK()));
    }
    return ImmutableList.copyOf(results);
  }

  /**
   * Returns source data files that are not covered by the built index snapshot for the requested
   * source table snapshot.
   *
   * @throws IllegalArgumentException if the index does not cover the source table snapshot
   */
  public Set<String> uncoveredDataFiles(
      long sourceTableSnapshotId, Iterable<String> sourceDataFiles) {
    return uncoveredDataFiles(
        IndexQuery.of(sourceTableSnapshotId, ByteBuffer.allocate(0), 0), sourceDataFiles);
  }

  /**
   * Returns source data files that are not covered by the index snapshot a query is answered from,
   * which honors the query's consistency requirement.
   */
  public Set<String> uncoveredDataFiles(IndexQuery query, Iterable<String> sourceDataFiles) {
    Preconditions.checkNotNull(query, "Index query is null");
    Preconditions.checkNotNull(sourceDataFiles, "Source data files are null");
    IndexSnapshot snapshot = snapshotForQuery(query);
    IndexManifestList manifestList = readManifestList(snapshot);
    Set<String> indexed = Sets.newLinkedHashSet();
    for (IndexManifestList.ManifestRef ref : manifestList.manifests()) {
      indexed.addAll(readManifest(ref.location()).indexedDataFiles());
    }

    Set<String> uncovered = Sets.newLinkedHashSet();
    for (String dataFile : sourceDataFiles) {
      Preconditions.checkNotNull(dataFile, "Source data file is null");
      if (!indexed.contains(dataFile)) {
        uncovered.add(dataFile);
      }
    }
    return ImmutableSet.copyOf(uncovered);
  }

  /** Selects the index snapshot a query is answered from, honoring its consistency requirement. */
  private IndexSnapshot snapshotForQuery(IndexQuery query) {
    IndexSnapshot snapshot =
        query.consistency() == IndexQuery.Consistency.EXACT_SNAPSHOT
            ? metadata.snapshotForSourceTableSnapshot(query.sourceTableSnapshotId())
            : latestSnapshotAtOrBefore(query.sourceTableSnapshotId());
    Preconditions.checkArgument(
        snapshot != null,
        "No index snapshot of index %s covers source table snapshot %s with consistency %s",
        metadata.indexUuid(),
        query.sourceTableSnapshotId(),
        query.consistency());
    return snapshot;
  }

  /**
   * Returns the newest index snapshot whose source table snapshot is at or before the given one.
   */
  private IndexSnapshot latestSnapshotAtOrBefore(long sourceTableSnapshotId) {
    IndexSnapshot newest = null;
    for (IndexSnapshot snapshot : metadata.snapshots()) {
      if (snapshot.sourceTableSnapshotId() <= sourceTableSnapshotId
          && (newest == null
              || snapshot.sourceTableSnapshotId() > newest.sourceTableSnapshotId())) {
        newest = snapshot;
      }
    }
    return newest;
  }

  private IndexManifestList readManifestList(IndexSnapshot snapshot) {
    try {
      return IndexManifestList.read(fileIO.newInputFile(snapshot.indexManifestList()));
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read index manifest list", e);
    }
  }

  private IndexManifest readManifest(String location) {
    try {
      return IndexManifest.read(fileIO.newInputFile(location));
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read index manifest", e);
    }
  }

  private IndexBlob readBlob(String location, String blobType) {
    List<IndexBlob> blobs;
    try {
      blobs = container.read(fileIO.newInputFile(location));
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read index file: " + location, e);
    }
    for (IndexBlob blob : blobs) {
      if (blobType.equals(blob.type())) {
        return blob;
      }
    }
    throw new IllegalArgumentException(
        "Index file does not contain expected blob type " + blobType + ": " + location);
  }
}
