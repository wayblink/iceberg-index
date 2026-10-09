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
import java.util.UUID;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
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
 */
public final class IndexSnapshotWriter {

  /** Default number of entries per index file. */
  public static final int DEFAULT_ENTRIES_PER_FILE = 10_000;

  private static final String DATA_DIR = "data";
  private static final String SNAPSHOTS_DIR = "snapshots";

  private final FileIO fileIO;
  private final IndexFileContainer container;
  private final int entriesPerFile;

  public IndexSnapshotWriter(FileIO fileIO) {
    this(fileIO, new PuffinIndexFileContainer(), DEFAULT_ENTRIES_PER_FILE);
  }

  public IndexSnapshotWriter(FileIO fileIO, IndexFileContainer container, int entriesPerFile) {
    this.fileIO = Preconditions.checkNotNull(fileIO, "File IO is null");
    this.container = Preconditions.checkNotNull(container, "Index file container is null");
    Preconditions.checkArgument(entriesPerFile > 0, "Invalid entries per file: %s", entriesPerFile);
    this.entriesPerFile = entriesPerFile;
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
    String manifestLocation =
        indexLocation + "/" + SNAPSHOTS_DIR + "/index-manifest-" + snapshotId + "-" + newId();
    write(manifestLocation, out -> IndexManifest.write(out, indexFiles));

    String listLocation =
        indexLocation + "/" + SNAPSHOTS_DIR + "/index-manifest-list-" + snapshotId + "-" + newId();
    long entryCount = indexFiles.stream().mapToLong(IndexFile::entryCount).sum();
    write(
        listLocation,
        out ->
            IndexManifestList.write(
                out,
                ImmutableList.of(
                    IndexManifestList.ManifestRef.of(
                        manifestLocation, indexFiles.size(), entryCount))));
    return listLocation;
  }

  private IndexFile writeIndexFile(
      String indexLocation,
      long snapshotId,
      int seq,
      IndexAdapter adapter,
      List<IndexEntry> batch) {
    String location =
        indexLocation + "/" + DATA_DIR + "/index-file-" + snapshotId + "-" + seq + "-" + newId();
    List<IndexBlob> blobs =
        ImmutableList.of(
            IndexBlob.of(adapter.entryBlobType(), adapter.encodeEntries(batch).array()));
    write(location, out -> container.write(out, blobs));
    return IndexFile.builder()
        .location(location)
        .kind(IndexFile.Kind.ENTRY_BEARING)
        .blobType(adapter.entryBlobType())
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
}
