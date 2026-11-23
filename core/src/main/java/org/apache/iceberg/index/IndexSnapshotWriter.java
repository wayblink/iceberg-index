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
import java.util.Locale;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;

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

  /** Default number of index files per index manifest. */
  public static final int DEFAULT_FILES_PER_MANIFEST = 1_000;

  private static final String DATA_DIR = "data";
  private static final String SNAPSHOTS_DIR = "snapshots";
  private static final String STAGING_DIR = "staging";
  private static final String STARTED_MARKER = "started";
  private static final String COMPLETE_MARKER = "complete";
  private static final String FILE_MARKER_PREFIX = "f";
  private static final String MANIFEST_MARKER_PREFIX = "m";
  private static final String MANIFEST_LIST_MARKER = "ml";
  private static final String STATISTICS_MARKER = "statistics";
  private static final String MANIFEST_LIST_PREFIX = "index-manifest-list-";
  private static final String FILE_COUNT = "files=";
  private static final String MANIFEST_COUNT = "manifests=";
  private static final String ATTEMPT_SEPARATOR = "-attempt-";

  private final FileIO fileIO;
  private final IndexFileContainer container;
  private final int entriesPerFile;
  private final int filesPerManifest;
  private final String attemptId;
  private final List<String> declaredPaths = Lists.newArrayList();
  private int fileMarkers = 0;
  private int manifestMarkers = 0;

  public IndexSnapshotWriter(FileIO fileIO) {
    this(fileIO, new PuffinIndexFileContainer(), DEFAULT_ENTRIES_PER_FILE);
  }

  public IndexSnapshotWriter(FileIO fileIO, IndexFileContainer container, int entriesPerFile) {
    this(fileIO, container, entriesPerFile, DEFAULT_FILES_PER_MANIFEST);
  }

  public IndexSnapshotWriter(
      FileIO fileIO, IndexFileContainer container, int entriesPerFile, int filesPerManifest) {
    this(fileIO, container, entriesPerFile, filesPerManifest, newAttemptId());
  }

  public IndexSnapshotWriter(
      FileIO fileIO,
      IndexFileContainer container,
      int entriesPerFile,
      int filesPerManifest,
      String attemptId) {
    this.fileIO = Preconditions.checkNotNull(fileIO, "File IO is null");
    this.container = Preconditions.checkNotNull(container, "Index file container is null");
    Preconditions.checkArgument(entriesPerFile > 0, "Invalid entries per file: %s", entriesPerFile);
    Preconditions.checkArgument(
        filesPerManifest > 0, "Invalid files per manifest: %s", filesPerManifest);
    this.entriesPerFile = entriesPerFile;
    this.filesPerManifest = filesPerManifest;
    this.attemptId = Preconditions.checkNotNull(attemptId, "Attempt id is null");
  }

  /** Returns the id of this attempt, used in file names and in the staging ledger. */
  public String attemptId() {
    return attemptId;
  }

  /** Records an attempt before workers start writing files. */
  public void startAttempt(String indexLocation) {
    writeMarker(indexLocation, STARTED_MARKER, "");
  }

  /**
   * Records that every file is in place. The ledger protects these files until publication or until
   * cleanup finds that the attempt has stopped making progress.
   */
  public void completeAttempt(String indexLocation) {
    writeMarker(
        indexLocation,
        COMPLETE_MARKER,
        String.format(
            Locale.ROOT, "%s%d %s%d", FILE_COUNT, fileMarkers, MANIFEST_COUNT, manifestMarkers));
  }

  /**
   * Drops the attempt ledger after the snapshot has been published. The files themselves stay: they
   * are referenced by the published snapshot now.
   */
  public void finishAttempt(String indexLocation) {
    deleteLedger(indexLocation);
  }

  /** Removes the ledger associated with a manifest list after the catalog resolves publication. */
  public static void finishPublication(FileIO io, String indexLocation, String manifestList) {
    String attempt = attemptOf(indexLocation, manifestList);
    if (attempt != null) {
      new IndexSnapshotWriter(
              io,
              new PuffinIndexFileContainer(),
              DEFAULT_ENTRIES_PER_FILE,
              DEFAULT_FILES_PER_MANIFEST,
              attempt)
          .finishAttempt(indexLocation);
    }
  }

  /**
   * Deletes the files of an attempt whose publication never landed.
   *
   * <p>Nothing names the files of an unpublished attempt: the metadata that would have is the one
   * the publication failed to write, so the attempt becomes an orphan index the moment the catalog
   * refuses the snapshot. The attempt's ledger is the only record of what it wrote, so the declared
   * paths are read back from it and removed, and the ledger goes with them.
   *
   * @param manifestList the manifest list of the snapshot that was not published, which names the
   *     attempt that wrote it
   */
  public static void abortPublication(FileIO io, String manifestList) {
    String indexLocation = indexLocationOf(manifestList);
    if (indexLocation == null) {
      return;
    }

    String attempt = attemptOf(indexLocation, manifestList);
    if (attempt == null) {
      return;
    }

    new IndexSnapshotWriter(
            io,
            new PuffinIndexFileContainer(),
            DEFAULT_ENTRIES_PER_FILE,
            DEFAULT_FILES_PER_MANIFEST,
            attempt)
        .abortLedger(indexLocation);
  }

  /**
   * The attempt a manifest list names, or null when the location is not one this writer wrote.
   *
   * <p>The attempt is in the name rather than in the file, so it is known without opening the
   * manifest list, which is what lets a writer that never had the attempt in memory find its files.
   */
  private static String attemptOf(String indexLocation, String manifestList) {
    String prefix = indexLocation + "/" + SNAPSHOTS_DIR + "/" + MANIFEST_LIST_PREFIX;
    int separator = manifestList.indexOf(ATTEMPT_SEPARATOR, prefix.length());
    if (!manifestList.startsWith(prefix) || separator < 0) {
      return null;
    }

    String attempt = manifestList.substring(separator + ATTEMPT_SEPARATOR.length());
    return attempt.matches("[a-zA-Z0-9-]+") ? attempt : null;
  }

  /** The index a manifest list belongs to, or null when it is not an index manifest list. */
  private static String indexLocationOf(String manifestList) {
    int snapshots = manifestList.indexOf("/" + SNAPSHOTS_DIR + "/" + MANIFEST_LIST_PREFIX);
    return snapshots < 0 ? null : manifestList.substring(0, snapshots);
  }

  /**
   * Deletes what a ledger on disk declares, for a caller that no longer has this writer's state.
   */
  private void abortLedger(String indexLocation) {
    List<String> declared = Lists.newArrayList();
    declaredPathsFromLedger(indexLocation, FILE_MARKER_PREFIX, declared);
    declaredPathsFromLedger(indexLocation, MANIFEST_MARKER_PREFIX, declared);
    for (String marker : List.of(MANIFEST_LIST_MARKER, STATISTICS_MARKER)) {
      String path = readMarker(indexLocation, marker);
      if (path != null) {
        declared.add(path.trim());
      }
    }

    runInParallel(declared, this::deleteIfExists);
    deleteLedger(indexLocation);
  }

  /** Reads the paths a marker sequence declares, stopping where the sequence ends. */
  private void declaredPathsFromLedger(String indexLocation, String prefix, List<String> declared) {
    for (int seq = 0; ; seq += 1) {
      String path = readMarker(indexLocation, prefix + seq);
      if (path == null) {
        return;
      }

      declared.add(path.trim());
    }
  }

  /**
   * Deletes every file this attempt declared, plus the ledger. Safe to call at any point, including
   * after a partial write, and safe to call more than once.
   */
  public void abortAttempt(String indexLocation) {
    runInParallel(declaredPaths, this::deleteIfExists);
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
            SNAPSHOTS_DIR
                + "/"
                + MANIFEST_LIST_PREFIX
                + snapshotId
                + "-"
                + newId()
                + ATTEMPT_SEPARATOR
                + attemptId);
    write(listLocation, out -> IndexManifestList.write(out, refs.build()));
    return listLocation;
  }

  /**
   * Groups index files into manifests of at most {@code filesPerManifest} entries. One manifest per
   * snapshot would be simpler, but it would become a single large file that has to be read and
   * written as a whole once an index covers many data files; the manifest list has always carried
   * several manifests, so this only fills in the writing side.
   */
  private List<List<IndexFile>> partition(List<IndexFile> indexFiles) {
    if (indexFiles.size() <= filesPerManifest) {
      return ImmutableList.of(indexFiles);
    }
    ImmutableList.Builder<List<IndexFile>> groups = ImmutableList.builder();
    List<IndexFile> group = Lists.newArrayList();
    for (IndexFile indexFile : indexFiles) {
      group.add(indexFile);
      if (group.size() >= filesPerManifest) {
        groups.add(ImmutableList.copyOf(group));
        group = Lists.newArrayList();
      }
    }
    if (!group.isEmpty()) {
      groups.add(ImmutableList.copyOf(group));
    }
    return groups.build();
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
    IndexBuildResult built;
    String blobType;
    IndexFile.Kind kind;
    if (adapter.writesAuxiliaryFiles()) {
      blobType = adapter.auxiliaryBlobType();
      Preconditions.checkState(
          blobType != null, "Adapter writes auxiliary files without a blob type");
      built = adapter.encodeAuxiliaryPayloadWithMemory(batch, 1);
      kind = IndexFile.Kind.AUXILIARY;
    } else {
      blobType = adapter.entryBlobType();
      built = adapter.encodeEntriesWithMemory(batch);
      kind = IndexFile.Kind.ENTRY_BEARING;
    }
    IndexBlob blob =
        kind == IndexFile.Kind.AUXILIARY
            ? adapter.auxiliaryBlob(built.payload())
            : IndexBlob.of(blobType, ByteBuffers.toByteArray(built.payload()));
    write(location, out -> container.write(out, ImmutableList.of(blob)));
    IndexFileMemory memory = built.memory();
    if (memory != null) {
      memory = memory.toBuilder().fileBytes(fileIO.newInputFile(location).getLength()).build();
    }

    return IndexFile.builder()
        .location(location)
        .kind(kind)
        .blobType(blob.type())
        .entryCount(batch.size())
        .coveredDataFiles(coveredDataFiles(batch))
        .memory(memory)
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
    if (marker.startsWith(FILE_MARKER_PREFIX)) {
      fileMarkers += 1;
    } else if (marker.startsWith(MANIFEST_MARKER_PREFIX) && !MANIFEST_LIST_MARKER.equals(marker)) {
      manifestMarkers += 1;
    }
  }

  /** Records the location of the snapshot statistics before writing it. */
  public void recordStatistics(String indexLocation, String location) {
    record(indexLocation, STATISTICS_MARKER, location);
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

  /**
   * Deletes the ledger markers, keeping the paths they named.
   *
   * <p>The completion marker names how many markers the attempt wrote, so the markers are deleted
   * directly instead of probing for the end of each sequence first: an attempt of 33 index files
   * has 38 markers, and every probe is another round trip to the file store. An attempt that died
   * before completing has no counts to read, and probing is the fallback that still cleans it.
   */
  private void deleteLedger(String indexLocation) {
    int[] counts = ledgerCounts(indexLocation);
    List<String> present = Lists.newArrayList();
    if (counts != null) {
      for (int seq = 0; seq < counts[0]; seq += 1) {
        present.add(stagingPath(indexLocation, FILE_MARKER_PREFIX + seq));
      }
      for (int seq = 0; seq < counts[1]; seq += 1) {
        present.add(stagingPath(indexLocation, MANIFEST_MARKER_PREFIX + seq));
      }
    }

    // a marker written by an attempt that named no counts is only found by probing where its
    // sequence ends, which also covers a sequence longer than the counts claim
    addRemainingMarkers(indexLocation, FILE_MARKER_PREFIX, counts == null ? 0 : counts[0], present);
    addRemainingMarkers(
        indexLocation, MANIFEST_MARKER_PREFIX, counts == null ? 0 : counts[1], present);

    // the completion marker is written after the manifest list, so its counts mean both exist
    List<String> presentFixed = Lists.newArrayList();
    if (counts != null) {
      presentFixed.add(stagingPath(indexLocation, MANIFEST_LIST_MARKER));
      presentFixed.add(stagingPath(indexLocation, COMPLETE_MARKER));
    }

    runInParallel(present, this::deleteMarker);
    runInParallel(presentFixed, this::deleteMarker);
    List<String> optional =
        counts == null
            ? List.of(
                stagingPath(indexLocation, MANIFEST_LIST_MARKER),
                stagingPath(indexLocation, STATISTICS_MARKER),
                stagingPath(indexLocation, COMPLETE_MARKER),
                stagingPath(indexLocation, STARTED_MARKER))
            : List.of(
                stagingPath(indexLocation, STATISTICS_MARKER),
                stagingPath(indexLocation, STARTED_MARKER));
    runInParallel(optional, this::deleteIfExists);
  }

  /** Adds the markers of a sequence that the completion counts did not name, by probing its end. */
  private void addRemainingMarkers(
      String indexLocation, String prefix, int from, List<String> markers) {
    for (int seq = from; markerExists(indexLocation, prefix + seq); seq += 1) {
      markers.add(stagingPath(indexLocation, prefix + seq));
    }
  }

  /** The marker counts an attempt recorded when it completed, or null when it never did. */
  private int[] ledgerCounts(String indexLocation) {
    String content = readMarker(indexLocation, COMPLETE_MARKER);
    if (content == null) {
      return null;
    }

    int files = -1;
    int manifests = -1;
    for (String part : Splitter.on(' ').omitEmptyStrings().trimResults().split(content)) {
      if (part.startsWith(FILE_COUNT)) {
        files = parseCount(part.substring(FILE_COUNT.length()));
      } else if (part.startsWith(MANIFEST_COUNT)) {
        manifests = parseCount(part.substring(MANIFEST_COUNT.length()));
      }
    }

    return files < 0 || manifests < 0 ? null : new int[] {files, manifests};
  }

  private static int parseCount(String value) {
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      return -1;
    }
  }

  private String readMarker(String indexLocation, String marker) {
    String path = stagingPath(indexLocation, marker);
    try (SeekableInputStream in = fileIO.newInputFile(path).newStream()) {
      return new String(ByteStreams.toByteArray(in), StandardCharsets.UTF_8);
    } catch (RuntimeException e) {
      if (!exists(path)) {
        return null;
      }
      throw e;
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read " + path, e);
    }
  }

  private boolean markerExists(String indexLocation, String marker) {
    return exists(stagingPath(indexLocation, marker));
  }

  private boolean exists(String path) {
    return fileIO.newInputFile(path).exists();
  }

  private void deleteIfExists(String path) {
    if (exists(path)) {
      deleteMarker(path);
    }
  }

  private void deleteMarker(String path) {
    try {
      fileIO.deleteFile(path);
    } catch (RuntimeException e) {
      // the marker is gone already: a sweep or a retry removed it, which is what was being asked
      if (!exists(path)) {
        return;
      }
      throw e;
    }
  }

  private void runInParallel(List<String> paths, Consumer<String> delete) {
    if (paths.isEmpty()) {
      return;
    }

    Tasks.foreach(paths)
        .executeWith(ThreadPools.getDeleteWorkerPool())
        .run(
            path -> {
              delete.accept(path);
            });
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
