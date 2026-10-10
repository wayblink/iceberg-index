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
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

/**
 * Writes the files of an index snapshot: entry-bearing index files, the index manifest that lists
 * them with their source data file coverage, and the index manifest list.
 *
 * <p>This is the shared file-writing machinery of the index builder and refresher. File names carry
 * the snapshot id and a random attempt id so that retries never collide with earlier attempts.
 * Files are written at their final locations; an index snapshot becomes visible only when it is
 * published through the {@link IndexCatalog}.
 *
 * <p>Every attempt also keeps a write-ahead ledger under {@code staging/<attempt-id>/}: a marker
 * naming a file is written before the file itself, so the set of paths an attempt may have created
 * is known even if the attempt dies. {@link #abortAttempt} deletes exactly those paths, which keeps
 * a failed build from leaving garbage behind without listing directories ({@link FileIO} exposes no
 * listing operation). {@link #completeAttempt} records that the attempt's files are all in place,
 * which is what tells a recovery sweep that a leftover file is at most an unreferenced file rather
 * than an abandoned attempt. {@link #finishAttempt} drops the ledger once the snapshot is
 * published.
 */
public final class IndexSnapshotWriter {

  /** Default number of entries per index file. */
  public static final int DEFAULT_ENTRIES_PER_FILE = 10_000;

  private static final String DATA_DIR = "data";
  private static final String SNAPSHOTS_DIR = "snapshots";
  private static final String STAGING_DIR = "staging";
  private static final String COMPLETE_MARKER = "complete";
  private static final String FILE_MARKER_PREFIX = "f";
  private static final String MANIFEST_MARKER_PREFIX = "m";
  private static final String MANIFEST_LIST_MARKER = "ml";

  private final FileIO fileIO;
  private final IndexFileContainer container;
  private final int entriesPerFile;
  private final String attemptId;
  private final List<String> declaredPaths = Lists.newArrayList();

  public IndexSnapshotWriter(FileIO fileIO) {
    this(fileIO, new PuffinIndexFileContainer(), DEFAULT_ENTRIES_PER_FILE);
  }

  public IndexSnapshotWriter(FileIO fileIO, IndexFileContainer container, int entriesPerFile) {
    this(fileIO, container, entriesPerFile, newAttemptId());
  }

  public IndexSnapshotWriter(
      FileIO fileIO, IndexFileContainer container, int entriesPerFile, String attemptId) {
    this.fileIO = Preconditions.checkNotNull(fileIO, "File IO is null");
    this.container = Preconditions.checkNotNull(container, "Index file container is null");
    Preconditions.checkArgument(entriesPerFile > 0, "Invalid entries per file: %s", entriesPerFile);
    this.entriesPerFile = entriesPerFile;
    this.attemptId = Preconditions.checkNotNull(attemptId, "Attempt id is null");
  }

  /** Returns the id of this attempt, used in file names and in the staging ledger. */
  public String attemptId() {
    return attemptId;
  }

  /**
   * Records that every file of this attempt is in place. Must be called before the snapshot is
   * published, so that a crash afterwards leaves at most unreferenced files and never an attempt
   * whose files a recovery sweep would have to guess at.
   */
  public void completeAttempt(String indexLocation) {
    writeMarker(indexLocation, COMPLETE_MARKER, "");
  }

  /**
   * Drops the attempt ledger after the snapshot has been published. The files themselves stay: they
   * are referenced by the published snapshot now.
   */
  public void finishAttempt(String indexLocation) {
    deleteLedger(indexLocation);
  }

  /**
   * Deletes every file this attempt declared, plus the ledger. Safe to call at any point, including
   * after a partial write, and safe to call more than once.
   */
  public void abortAttempt(String indexLocation) {
    for (String path : declaredPaths) {
      deleteIfExists(path);
    }
    deleteLedger(indexLocation);
  }

  /**
   * Writes index files for the entries and returns their manifest entries.
   *
   * @param indexLocation the index root location
   * @param snapshotId the snapshot id used in file names
   * @param adapter the adapter that encodes entries into payloads
   * @param entries the entries to write; may span multiple data files
   */
  public List<IndexFile> writeIndexFiles(
      String indexLocation, long snapshotId, IndexAdapter adapter, Iterable<IndexEntry> entries) {
    Preconditions.checkNotNull(entries, "Index entries are null");
    List<IndexFile> indexFiles = Lists.newArrayList();
    List<IndexEntry> batch = Lists.newArrayList();
    for (IndexEntry entry : entries) {
      batch.add(entry);
      if (batch.size() >= entriesPerFile) {
        indexFiles.add(
            writeIndexFile(indexLocation, snapshotId, indexFiles.size(), adapter, batch));
        batch = Lists.newArrayList();
      }
    }
    if (!batch.isEmpty()) {
      indexFiles.add(writeIndexFile(indexLocation, snapshotId, indexFiles.size(), adapter, batch));
    }
    return ImmutableList.copyOf(indexFiles);
  }

  /**
   * Writes the manifest and manifest list for the given index files and returns the manifest list
   * location.
   *
   * @param indexLocation the index root location
   * @param snapshotId the snapshot id used in file names
   * @param indexFiles the index files of the snapshot, in order
   */
  public String writeManifestList(
      String indexLocation, long snapshotId, List<IndexFile> indexFiles) {
    Preconditions.checkNotNull(indexFiles, "Index files are null");
    ImmutableList.Builder<IndexManifestList.ManifestRef> refs = ImmutableList.builder();
    int seq = 0;
    for (List<IndexFile> group : partition(indexFiles)) {
      String manifestLocation =
          declare(
              indexLocation,
              MANIFEST_MARKER_PREFIX + seq,
              SNAPSHOTS_DIR + "/index-manifest-" + snapshotId + "-" + seq + "-" + newId());
      write(manifestLocation, out -> IndexManifest.write(out, group));
      refs.add(
          IndexManifestList.ManifestRef.of(
              manifestLocation,
              group.size(),
              group.stream().mapToLong(IndexFile::entryCount).sum()));
      seq += 1;
    }

    String listLocation =
        declare(
            indexLocation,
            MANIFEST_LIST_MARKER,
            SNAPSHOTS_DIR + "/index-manifest-list-" + snapshotId + "-" + newId());
    write(listLocation, out -> IndexManifestList.write(out, refs.build()));
    return listLocation;
  }

  /**
   * Groups index files into manifests. One manifest holds them all today; grouping by a size budget
   * keeps a single manifest from becoming a hot spot once an index covers many data files.
   */
  private static List<List<IndexFile>> partition(List<IndexFile> indexFiles) {
    return ImmutableList.of(indexFiles);
  }

  private IndexFile writeIndexFile(
      String indexLocation,
      long snapshotId,
      int seq,
      IndexAdapter adapter,
      List<IndexEntry> batch) {
    String location =
        declare(
            indexLocation,
            FILE_MARKER_PREFIX + seq,
            DATA_DIR + "/index-file-" + snapshotId + "-" + seq + "-" + newId());
    IndexBlob blob;
    IndexFile.Kind kind;
    if (adapter.writesAuxiliaryFiles()) {
      String auxiliaryBlobType = adapter.auxiliaryBlobType();
      Preconditions.checkState(
          auxiliaryBlobType != null, "Adapter writes auxiliary files without a blob type");
      blob = IndexBlob.of(auxiliaryBlobType, adapter.encodeAuxiliaryPayload(batch).array());
      kind = IndexFile.Kind.AUXILIARY;
    } else {
      blob = IndexBlob.of(adapter.entryBlobType(), adapter.encodeEntries(batch).array());
      kind = IndexFile.Kind.ENTRY_BEARING;
    }
    write(location, out -> container.write(out, ImmutableList.of(blob)));
    return IndexFile.builder()
        .location(location)
        .kind(kind)
        .blobType(blob.type())
        .entryCount(batch.size())
        .coveredDataFiles(coveredDataFiles(batch))
        .build();
  }

  private static List<String> coveredDataFiles(List<IndexEntry> batch) {
    Set<String> dataFiles = Sets.newLinkedHashSet();
    for (IndexEntry entry : batch) {
      dataFiles.add(entry.dataFile());
    }
    return ImmutableList.copyOf(dataFiles);
  }

  /**
   * Names a file in the ledger before it is written, and returns its location. The marker is the
   * write-ahead record that lets a later recovery sweep find the file without listing directories.
   */
  private String declare(String indexLocation, String marker, String relativePath) {
    String location = indexLocation + "/" + relativePath;
    record(indexLocation, marker, location);
    return location;
  }

  /**
   * Records a file that was written outside this writer, for instance by a distributed job, in the
   * ledger of this attempt. The marker is written after the fact, so a crash before this call is
   * only covered by the unreferenced-file sweep, not by {@link #abortAttempt}.
   */
  public void record(String indexLocation, String marker, String location) {
    writeMarker(indexLocation, marker, location);
    declaredPaths.add(location);
  }

  private void writeMarker(String indexLocation, String marker, String path) {
    write(
        stagingPath(indexLocation, marker),
        out -> {
          try (PositionOutputStream stream = out.create()) {
            stream.write(path.getBytes(StandardCharsets.UTF_8));
          }
        });
  }

  private String stagingPath(String indexLocation, String marker) {
    return indexLocation + "/" + STAGING_DIR + "/" + attemptId + "/" + marker;
  }

  /** Deletes the ledger markers, keeping the paths they named. */
  private void deleteLedger(String indexLocation) {
    for (int seq = 0; markerExists(indexLocation, FILE_MARKER_PREFIX + seq); seq++) {
      deleteIfExists(stagingPath(indexLocation, FILE_MARKER_PREFIX + seq));
    }
    for (int seq = 0; markerExists(indexLocation, MANIFEST_MARKER_PREFIX + seq); seq++) {
      deleteIfExists(stagingPath(indexLocation, MANIFEST_MARKER_PREFIX + seq));
    }
    deleteIfExists(stagingPath(indexLocation, MANIFEST_LIST_MARKER));
    deleteIfExists(stagingPath(indexLocation, COMPLETE_MARKER));
  }

  private boolean markerExists(String indexLocation, String marker) {
    return fileIO.newInputFile(stagingPath(indexLocation, marker)).exists();
  }

  private void deleteIfExists(String path) {
    if (fileIO.newInputFile(path).exists()) {
      fileIO.deleteFile(path);
    }
  }

  private void write(String location, ThrowingWrite write) {
    OutputFile outputFile = fileIO.newOutputFile(location);
    try {
      write.write(outputFile);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to write " + location, e);
    }
  }

  private interface ThrowingWrite {
    void write(OutputFile outputFile) throws IOException;
  }

  private static String newId() {
    return UUID.randomUUID().toString();
  }

  private static String newAttemptId() {
    return UUID.randomUUID().toString();
  }
}
