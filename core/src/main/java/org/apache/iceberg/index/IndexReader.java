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
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.SnapshotUtil;

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
  private final Table sourceTable;

  public IndexReader(IndexMetadata metadata, FileIO fileIO) {
    this(metadata, fileIO, new PuffinIndexFileContainer());
  }

  public IndexReader(IndexMetadata metadata, FileIO fileIO, IndexFileContainer container) {
    this.metadata = Preconditions.checkNotNull(metadata, "Index metadata is null");
    this.fileIO = Preconditions.checkNotNull(fileIO, "File IO is null");
    this.container = Preconditions.checkNotNull(container, "Index file container is null");
    this.sourceTable = null;
  }

  /**
   * A reader that can also answer a query from an index snapshot older than the requested source
   * table snapshot. That needs the table, because the order of its snapshots comes from their
   * parent chain: snapshot ids are not ordered.
   */
  public IndexReader(IndexMetadata metadata, Table sourceTable) {
    this.metadata = Preconditions.checkNotNull(metadata, "Index metadata is null");
    this.fileIO = Preconditions.checkNotNull(sourceTable, "Source table is null").io();
    this.container = new PuffinIndexFileContainer();
    this.sourceTable = sourceTable;
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

    ReadableFiles readable = readable(adapter, readManifestList(snapshot));
    checkReadable(readable, adapter);
    if (adapter.searchesAllPayloadsTogether()) {
      return searchCorrelated(adapter, readable, query);
    }

    List<IndexSearchResult> results = Lists.newArrayList();
    for (IndexFile indexFile : readable.entry()) {
      IndexBlob blob = readBlob(indexFile.location(), indexFile.blobType());
      results.addAll(
          adapter.search(ByteBuffer.wrap(blob.payload()), query.payload(), query.topK()));
    }

    for (IndexFile indexFile : readable.auxiliary()) {
      IndexBlob blob = readBlob(indexFile.location(), indexFile.blobType());
      results.addAll(
          adapter.searchAuxiliary(ByteBuffer.wrap(blob.payload()), query.payload(), query.topK()));
    }

    results.sort(adapter.resultComparator());
    if (results.size() > query.topK()) {
      results = Lists.newArrayList(results.subList(0, query.topK()));
    }
    return ImmutableList.copyOf(results);
  }

  /**
   * The index files of a snapshot this reader can decode, and the blob types of the files it has to
   * skip.
   *
   * <p>A file the reader cannot decode is recorded rather than dropped, because the caller has to
   * stop claiming coverage for it: a skipped file answers nothing, so its rows have to come from a
   * residual scan. That is what forward compatibility looks like when a newer build has written a
   * type this one does not know; a reader that can decode none of the files is a different case and
   * {@link #checkReadable} fails.
   */
  private ReadableFiles readable(IndexAdapter adapter, IndexManifestList manifestList) {
    Set<String> entryBlobTypes = adapter.readableEntryBlobTypes();
    Set<String> auxiliaryBlobTypes = adapter.readableAuxiliaryBlobTypes();
    List<IndexFile> entry = Lists.newArrayList();
    List<IndexFile> auxiliary = Lists.newArrayList();
    Set<String> skippedBlobTypes = Sets.newLinkedHashSet();
    for (IndexManifestList.ManifestRef ref : manifestList.manifests()) {
      for (IndexFile indexFile : readManifest(ref.location()).indexFiles()) {
        if (indexFile.kind() == IndexFile.Kind.ENTRY_BEARING
            && entryBlobTypes.contains(indexFile.blobType())) {
          entry.add(indexFile);
        } else if (indexFile.kind() == IndexFile.Kind.AUXILIARY
            && auxiliaryBlobTypes.contains(indexFile.blobType())) {
          auxiliary.add(indexFile);
        } else {
          skippedBlobTypes.add(indexFile.blobType());
        }
      }
    }

    return new ReadableFiles(entry, auxiliary, skippedBlobTypes);
  }

  /**
   * Refuses a snapshot none of whose files this build can decode.
   *
   * <p>When every file is skipped there is no forward compatibility left to preserve: the answer
   * would be empty, the same as a search that matched nothing, so the caller could not tell a
   * broken index from an empty result. Failing says which types were expected and which were found.
   */
  private void checkReadable(ReadableFiles readable, IndexAdapter adapter) {
    if (readable.all().isEmpty() && !readable.skippedBlobTypes().isEmpty()) {
      throw new IllegalStateException(
          "Index "
              + metadata.indexUuid()
              + " has no file this reader can decode: it reads "
              + expectedBlobTypes(adapter)
              + ", the files carry "
              + readable.skippedBlobTypes()
              + "; rebuild the index or read it with a version that knows these types");
    }
  }

  private static Set<String> expectedBlobTypes(IndexAdapter adapter) {
    return ImmutableSet.<String>builder()
        .addAll(adapter.readableEntryBlobTypes())
        .addAll(adapter.readableAuxiliaryBlobTypes())
        .build();
  }

  private static final class ReadableFiles {
    private final List<IndexFile> entry;
    private final List<IndexFile> auxiliary;
    private final Set<String> skippedBlobTypes;

    private ReadableFiles(
        List<IndexFile> entry, List<IndexFile> auxiliary, Set<String> skippedBlobTypes) {
      this.entry = entry;
      this.auxiliary = auxiliary;
      this.skippedBlobTypes = skippedBlobTypes;
    }

    private List<IndexFile> entry() {
      return entry;
    }

    private List<IndexFile> auxiliary() {
      return auxiliary;
    }

    private Set<String> skippedBlobTypes() {
      return skippedBlobTypes;
    }

    private List<IndexFile> all() {
      return ImmutableList.<IndexFile>builder().addAll(entry).addAll(auxiliary).build();
    }
  }

  /**
   * Searches with an adapter that needs all payloads of the snapshot together: the payloads of each
   * kind are collected and handed to the adapter, which merges the statistics they carry before
   * scoring. See {@link IndexAdapter#searchesAllPayloadsTogether()}.
   */
  private List<IndexSearchResult> searchCorrelated(
      IndexAdapter adapter, ReadableFiles readable, IndexQuery query) {
    Payloads payloads = readPayloads(readable);

    List<IndexSearchResult> results = Lists.newArrayList();
    if (!payloads.entry.isEmpty()) {
      results.addAll(adapter.searchAll(payloads.entry, query.payload(), query.topK()));
    }
    if (!payloads.auxiliary.isEmpty()) {
      results.addAll(adapter.searchAuxiliaryAll(payloads.auxiliary, query.payload(), query.topK()));
    }

    results.sort(adapter.resultComparator());
    if (results.size() > query.topK()) {
      results = Lists.newArrayList(results.subList(0, query.topK()));
    }
    return ImmutableList.copyOf(results);
  }

  /**
   * Returns the index files of the snapshot a query is answered from, so an engine can read and
   * search them itself, on its own executors. The manifest list and the manifests are read here, on
   * the caller's thread, because they are small; only the payloads are left to the engine.
   *
   * @throws IllegalArgumentException if the index does not cover the source table snapshot
   */
  public List<IndexFile> indexFiles(IndexQuery query) {
    Preconditions.checkNotNull(query, "Index query is null");
    IndexSnapshot snapshot = snapshotForQuery(query);
    IndexAdapter adapter =
        IndexTypes.fromType(metadata.indexType()).createAdapter(metadata.definition());

    ReadableFiles readable = readable(adapter, readManifestList(snapshot));
    checkReadable(readable, adapter);
    return readable.all();
  }

  /**
   * Returns the statistics an engine needs to score rows the way the index does, for the snapshot a
   * query is answered from. Null when the index type has no statistics to merge.
   *
   * @throws IllegalArgumentException if the index does not cover the source table snapshot
   */
  public Object searchStatistics(IndexQuery query) {
    Preconditions.checkNotNull(query, "Index query is null");
    IndexSnapshot snapshot = snapshotForQuery(query);
    IndexAdapter adapter =
        IndexTypes.fromType(metadata.indexType()).createAdapter(metadata.definition());

    ReadableFiles readable = readable(adapter, readManifestList(snapshot));
    checkReadable(readable, adapter);
    Payloads payloads = readPayloads(readable);
    List<ByteBuffer> statisticsPayloads =
        adapter.writesAuxiliaryFiles() && !payloads.auxiliary.isEmpty()
            ? payloads.auxiliary
            : payloads.entry;
    return adapter.searchStatistics(statisticsPayloads);
  }

  /** The payloads of the given index files, by kind. */
  private Payloads readPayloads(ReadableFiles readable) {
    List<ByteBuffer> entryPayloads = Lists.newArrayListWithCapacity(readable.entry().size());
    for (IndexFile indexFile : readable.entry()) {
      IndexBlob blob = readBlob(indexFile.location(), indexFile.blobType());
      entryPayloads.add(ByteBuffer.wrap(blob.payload()));
    }

    List<ByteBuffer> auxiliaryPayloads =
        Lists.newArrayListWithCapacity(readable.auxiliary().size());
    for (IndexFile indexFile : readable.auxiliary()) {
      IndexBlob blob = readBlob(indexFile.location(), indexFile.blobType());
      auxiliaryPayloads.add(ByteBuffer.wrap(blob.payload()));
    }

    return new Payloads(entryPayloads, auxiliaryPayloads);
  }

  private static final class Payloads {
    private final List<ByteBuffer> entry;
    private final List<ByteBuffer> auxiliary;

    private Payloads(List<ByteBuffer> entry, List<ByteBuffer> auxiliary) {
      this.entry = entry;
      this.auxiliary = auxiliary;
    }
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
    IndexAdapter adapter =
        IndexTypes.fromType(metadata.indexType()).createAdapter(metadata.definition());

    // only the files this reader can decode cover the data files they name: a file that is skipped
    // answers nothing, so its rows have to come from a residual scan rather than from the index
    Set<String> indexed = Sets.newLinkedHashSet();
    for (IndexFile indexFile : readable(adapter, readManifestList(snapshot)).all()) {
      indexed.addAll(indexFile.coveredDataFiles());
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
   * Returns the newest index snapshot that may answer a query about the given source table
   * snapshot: the one whose source snapshot is the closest ancestor of it.
   *
   * <p>The order comes from the parent chain of the table's snapshots and never from snapshot ids,
   * which are random. This is why the reader needs the source table for this mode.
   */
  private IndexSnapshot latestSnapshotAtOrBefore(long sourceTableSnapshotId) {
    Preconditions.checkState(
        sourceTable != null,
        "Index %s cannot answer from an older index snapshot without the source table",
        metadata.indexUuid());

    Snapshot snapshot = sourceTable.snapshot(sourceTableSnapshotId);
    if (snapshot == null) {
      return null;
    }

    // index snapshots built from the same source snapshot are ordered by their own id, which is
    // sequential
    Map<Long, IndexSnapshot> newestBySource = Maps.newHashMap();
    for (IndexSnapshot candidate : metadata.snapshots()) {
      IndexSnapshot known = newestBySource.get(candidate.sourceTableSnapshotId());
      if (known == null || candidate.snapshotId() > known.snapshotId()) {
        newestBySource.put(candidate.sourceTableSnapshotId(), candidate);
      }
    }

    for (Snapshot ancestor :
        SnapshotUtil.ancestorsOf(sourceTableSnapshotId, sourceTable::snapshot)) {
      IndexSnapshot candidate = newestBySource.get(ancestor.snapshotId());
      if (candidate != null) {
        return candidate;
      }
    }

    return null;
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
