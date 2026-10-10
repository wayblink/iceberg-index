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
package org.apache.iceberg.index.spark;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.index.IndexAdapter;
import org.apache.iceberg.index.IndexBlob;
import org.apache.iceberg.index.IndexEntry;
import org.apache.iceberg.index.IndexFile;
import org.apache.iceberg.index.IndexFileContainer;
import org.apache.iceberg.index.IndexManifest;
import org.apache.iceberg.index.IndexManifestList;
import org.apache.iceberg.index.IndexManifestList.ManifestRef;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.IndexSnapshot;
import org.apache.iceberg.index.IndexSnapshotWriter;
import org.apache.iceberg.index.IndexTypes;
import org.apache.iceberg.index.PuffinIndexFileContainer;
import org.apache.iceberg.index.text.TextIndexTypes;
import org.apache.iceberg.index.vector.VectorIndexTypes;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import scala.Tuple2;

/**
 * Builds a vector index snapshot for an Iceberg table snapshot with Spark.
 *
 * <p>Entries are read by {@link IndexEntries}, shuffled by data file so that all rows of a data
 * file land in one partition, and written as one index file per data file per partition. That keeps
 * source-file coverage precise: a refresh only has to drop the index files of data files that were
 * removed or rewritten.
 *
 * <p>Like the core builder, this only writes files and returns updated metadata; publishing it
 * through the {@code IndexCatalog} is a separate step.
 */
public final class SparkIndexBuilder {

  private static final String DATA_DIR = "data";
  private static final String FILE_MARKER_PREFIX = "f";

  static {
    // the vector and text adapters live in their own modules; register them with the core registry
    VectorIndexTypes.register();
    TextIndexTypes.register();
  }

  private SparkIndexBuilder() {}

  /**
   * Builds a full index snapshot for the given source table snapshot.
   *
   * @param spark the Spark session
   * @param table the Iceberg table to index
   * @param currentMetadata the index metadata to extend
   * @param sourceTableSnapshotId the source table snapshot to build from
   * @param vectorColumn the name of the numeric array column to index
   * @param timestampMillis the publication timestamp of the new snapshot
   * @return updated metadata with the new snapshot appended, not yet published
   */
  public static IndexMetadata build(
      SparkSession spark,
      Table table,
      IndexMetadata currentMetadata,
      long sourceTableSnapshotId,
      String vectorColumn,
      long timestampMillis) {
    validateSourceExists(table, sourceTableSnapshotId);

    IndexAdapter adapter = adapterFor(currentMetadata);
    String indexLocation = currentMetadata.location();
    long snapshotId = nextSnapshotId(currentMetadata);
    FileIO fileIO = table.io();

    IndexSnapshotWriter writer = new IndexSnapshotWriter(fileIO);
    List<IndexFile> indexFiles;
    String manifestList;
    try {
      indexFiles =
          buildIndexFiles(
              spark,
              table,
              indexLocation,
              snapshotId,
              sourceTableSnapshotId,
              vectorColumn,
              adapter,
              null,
              currentMetadata.entriesPerIndexFile(),
              writer);
      manifestList = writer.writeManifestList(indexLocation, snapshotId, indexFiles);
    } catch (RuntimeException e) {
      writer.abortAttempt(indexLocation);
      throw e;
    }
    writer.completeAttempt(indexLocation);
    // the ledger has served its purpose once the files are in place: the failure modes that remain
    // (the publish itself failing, or the process dying) leave at most unreferenced files, which
    // the cleanup sweep removes
    writer.finishAttempt(indexLocation);

    IndexSnapshot snapshot =
        IndexSnapshot.builder()
            .snapshotId(snapshotId)
            .sourceTableSnapshotId(sourceTableSnapshotId)
            .parentIndexSnapshotId(null)
            .timestampMillis(timestampMillis)
            .indexManifestList(manifestList)
            .build();
    return currentMetadata.withSnapshot(snapshot);
  }

  /**
   * Refreshes an index incrementally against a newer source table snapshot.
   *
   * <p>Index files of data files that are still present are carried over unchanged (parent-index
   * snapshot lineage); index files of removed or rewritten data files are dropped, and new data
   * files are indexed. If nothing changed, the metadata is returned unchanged.
   *
   * @param spark the Spark session
   * @param table the Iceberg table to index
   * @param currentMetadata the current index metadata; it must have at least one snapshot
   * @param sourceTableSnapshotId the newer source table snapshot to refresh from
   * @param vectorColumn the name of the numeric array column to index
   * @param timestampMillis the publication timestamp of the new snapshot
   * @return updated metadata with the new snapshot appended, or the input metadata when nothing
   *     changed
   */
  public static IndexMetadata refresh(
      SparkSession spark,
      Table table,
      IndexMetadata currentMetadata,
      long sourceTableSnapshotId,
      String vectorColumn,
      long timestampMillis) {
    IndexSnapshot previous = currentMetadata.latestSnapshot();
    if (previous == null) {
      throw new IllegalArgumentException(
          "Cannot refresh an index without an index snapshot; build it first");
    }

    validateSourceLineage(table, currentMetadata, sourceTableSnapshotId);

    IndexAdapter adapter = adapterFor(currentMetadata);
    String indexLocation = currentMetadata.location();
    long snapshotId = previous.snapshotId() + 1;
    FileIO fileIO = table.io();

    Set<String> currentFiles =
        Sets.newHashSet(IndexEntries.dataFiles(table, sourceTableSnapshotId));
    Set<String> previouslyCovered = coveredDataFiles(fileIO, previous);
    Set<String> removedFiles = Sets.difference(previouslyCovered, currentFiles);
    Set<String> newFiles = Sets.difference(currentFiles, previouslyCovered);
    // data files present in both snapshots whose delete files changed must be re-indexed: the
    // delete files of a snapshot are part of its data, so their entries changed
    Set<String> deleteChangedFiles =
        deleteChangedFiles(
            table,
            previous.sourceTableSnapshotId(),
            sourceTableSnapshotId,
            Sets.intersection(currentFiles, previouslyCovered));
    if (removedFiles.isEmpty() && newFiles.isEmpty() && deleteChangedFiles.isEmpty()) {
      return currentMetadata;
    }

    Set<String> staleFiles = Sets.union(removedFiles, Sets.union(newFiles, deleteChangedFiles));
    // an index file can cover several data files, so a change to any of them rewrites the whole
    // file: the rebuild set is expanded through the parent's index files, which are the record of
    // what was grouped together. Looking at the current assignment would miss the group of a data
    // file that was removed, because it is no longer part of the snapshot.
    List<IndexFile> parentFiles = indexFiles(fileIO, previous);
    Set<String> rebuildFiles = IndexGrouping.expandToGroups(parentFiles, staleFiles);

    List<IndexFile> carried = Lists.newArrayList();
    for (IndexFile indexFile : parentFiles) {
      if (Sets.intersection(Sets.newHashSet(indexFile.coveredDataFiles()), rebuildFiles)
          .isEmpty()) {
        carried.add(indexFile);
      }
    }

    IndexSnapshotWriter writer = new IndexSnapshotWriter(fileIO);
    List<IndexFile> indexFiles;
    String manifestList;
    try {
      List<IndexFile> newIndexFiles =
          buildIndexFiles(
              spark,
              table,
              indexLocation,
              snapshotId,
              sourceTableSnapshotId,
              vectorColumn,
              adapter,
              rebuildFiles,
              currentMetadata.entriesPerIndexFile(),
              writer);
      indexFiles = Lists.newArrayList(carried);
      indexFiles.addAll(newIndexFiles);
      manifestList = writer.writeManifestList(indexLocation, snapshotId, indexFiles);
    } catch (RuntimeException e) {
      // carried-over files belong to the parent snapshot and must survive a failed refresh
      writer.abortAttempt(indexLocation);
      throw e;
    }
    writer.completeAttempt(indexLocation);

    IndexSnapshot snapshot =
        IndexSnapshot.builder()
            .snapshotId(snapshotId)
            .sourceTableSnapshotId(sourceTableSnapshotId)
            .parentIndexSnapshotId(previous.snapshotId())
            .timestampMillis(timestampMillis)
            .indexManifestList(manifestList)
            .build();
    return currentMetadata.withSnapshot(snapshot);
  }

  /**
   * Writes one index file per data file (restricted to {@code onlyFiles} when non-null) and returns
   * the index files without writing a manifest or snapshot.
   */
  private static List<IndexFile> buildIndexFiles(
      SparkSession spark,
      Table table,
      String indexLocation,
      long snapshotId,
      long sourceTableSnapshotId,
      String vectorColumn,
      IndexAdapter adapter,
      Set<String> onlyFiles,
      long entriesPerIndexFile,
      IndexSnapshotWriter writer) {
    if (onlyFiles != null && onlyFiles.isEmpty()) {
      return Lists.newArrayList();
    }

    boolean writesAuxiliary = adapter.writesAuxiliaryFiles();
    FileIO fileIO = table.io();
    // data files are packed into groups, and one index file is written per group: with the default
    // budget of zero every group holds a single data file, which is the 1:1 layout
    Map<String, Integer> assignment =
        IndexGrouping.restrict(
            IndexGrouping.assign(table, sourceTableSnapshotId, entriesPerIndexFile),
            onlyFiles == null
                ? Sets.newHashSet(IndexEntries.dataFiles(table, sourceTableSnapshotId))
                : onlyFiles);

    Dataset<Row> entries =
        IndexEntries.entries(spark, table, sourceTableSnapshotId, vectorColumn, adapter);
    if (onlyFiles != null) {
      entries =
          entries.filter(functions.col(IndexEntries.DATA_FILE_COLUMN).isin(onlyFiles.toArray()));
    }

    Broadcast<Map<String, Integer>> groups =
        JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(assignment);
    List<Row> indexFileRows =
        entries
            .toJavaRDD()
            .mapToPair(row -> Tuple2.apply(groups.value().getOrDefault(row.getString(0), 0), row))
            .groupByKey(new HashPartitioner(Math.max(1, assignment.size())))
            .mapPartitions(
                (FlatMapFunction<Iterator<Tuple2<Integer, Iterable<Row>>>, Row>)
                    iterator -> {
                      IndexFileContainer fileContainer = new PuffinIndexFileContainer();
                      List<Row> files = Lists.newArrayList();
                      int seq = 0;
                      while (iterator.hasNext()) {
                        Tuple2<Integer, Iterable<Row>> group = iterator.next();
                        List<IndexEntry> groupEntries = Lists.newArrayList();
                        Set<String> covered = Sets.newLinkedHashSet();
                        for (Row row : group._2()) {
                          covered.add(row.getString(0));
                          groupEntries.add(
                              IndexEntry.of(row.getString(0), row.getLong(1), row.getAs(2)));
                        }

                        String location =
                            indexLocation
                                + "/"
                                + DATA_DIR
                                + "/index-file-"
                                + snapshotId
                                + "-"
                                + seq
                                + "-"
                                + UUID.randomUUID();
                        String blobType;
                        byte[] payload;
                        IndexFile.Kind kind;
                        if (writesAuxiliary) {
                          blobType = adapter.auxiliaryBlobType();
                          payload = adapter.encodeAuxiliaryPayload(groupEntries).array();
                          kind = IndexFile.Kind.AUXILIARY;
                        } else {
                          blobType = adapter.entryBlobType();
                          payload = adapter.encodeEntries(groupEntries).array();
                          kind = IndexFile.Kind.ENTRY_BEARING;
                        }
                        fileContainer.write(
                            fileIO.newOutputFile(location),
                            List.of(IndexBlob.of(blobType, payload)));
                        files.add(
                            RowFactory.create(
                                location,
                                (long) groupEntries.size(),
                                Lists.newArrayList(covered),
                                kind.name(),
                                blobType));
                        seq++;
                      }
                      return files.iterator();
                    })
            .collect();

    // the index files were written by the executors, so record them in this attempt's ledger now
    // that the driver knows every path: a failure from here on can then be aborted precisely
    int seq = 0;
    for (Row row : indexFileRows) {
      writer.record(indexLocation, FILE_MARKER_PREFIX + seq, row.getString(0));
      seq += 1;
    }

    List<IndexFile> indexFiles = Lists.newArrayListWithCapacity(indexFileRows.size());
    for (Row row : indexFileRows) {
      indexFiles.add(
          IndexFile.builder()
              .location(row.getString(0))
              .kind(IndexFile.Kind.valueOf(row.getString(3)))
              .blobType(row.getString(4))
              .entryCount(row.getLong(1))
              .coveredDataFiles(row.getAs(2))
              .build());
    }

    return indexFiles;
  }

  /**
   * Validates that an index refresh stays on the table's snapshot lineage: the new source snapshot
   * must be the same as, or a descendant of, the snapshot the newest index snapshot was built from.
   * Refreshing from an older snapshot (for example after a rollback) would silently drop the newer
   * entries, so it is rejected.
   */
  private static void validateSourceLineage(
      Table table, IndexMetadata currentMetadata, long sourceTableSnapshotId) {
    IndexSnapshot previous = currentMetadata.latestSnapshot();
    if (previous == null || previous.sourceTableSnapshotId() == sourceTableSnapshotId) {
      return;
    }

    Long ancestor = parentSnapshotId(table, sourceTableSnapshotId);
    while (ancestor != null) {
      if (ancestor == previous.sourceTableSnapshotId()) {
        return;
      }
      ancestor = parentSnapshotId(table, ancestor);
    }

    throw new IllegalArgumentException(
        "Cannot refresh the index of table "
            + table.name()
            + " from table snapshot "
            + sourceTableSnapshotId
            + ": it is not a descendant of table snapshot "
            + previous.sourceTableSnapshotId()
            + ", which the newest index snapshot was built from");
  }

  private static void validateSourceExists(Table table, long sourceTableSnapshotId) {
    if (table.snapshot(sourceTableSnapshotId) == null) {
      throw new IllegalArgumentException(
          "Table snapshot "
              + sourceTableSnapshotId
              + " of table "
              + table.name()
              + " does not exist");
    }
  }

  /** Returns the parent snapshot id of a table snapshot, or null if it is unknown or a root. */
  private static Long parentSnapshotId(Table table, long snapshotId) {
    for (Snapshot snapshot : table.snapshots()) {
      if (snapshot.snapshotId() == snapshotId) {
        return snapshot.parentId();
      }
    }
    return null;
  }

  /**
   * Returns the data files that exist in both snapshots but whose set of applicable delete files
   * changed, so their indexed entries are stale.
   */
  private static Set<String> deleteChangedFiles(
      Table table, long previousSnapshotId, long currentSnapshotId, Set<String> commonDataFiles) {
    if (commonDataFiles.isEmpty()) {
      return Sets.newHashSet();
    }

    Map<String, Set<String>> previousDeletes =
        IndexEntries.deleteFilesByDataFile(table, previousSnapshotId);
    Map<String, Set<String>> currentDeletes =
        IndexEntries.deleteFilesByDataFile(table, currentSnapshotId);
    Set<String> changed = Sets.newHashSet();
    for (String dataFile : commonDataFiles) {
      if (!previousDeletes
          .getOrDefault(dataFile, Sets.newHashSet())
          .equals(currentDeletes.getOrDefault(dataFile, Sets.newHashSet()))) {
        changed.add(dataFile);
      }
    }
    return changed;
  }

  private static Set<String> coveredDataFiles(FileIO fileIO, IndexSnapshot snapshot) {
    Set<String> covered = Sets.newHashSet();
    for (IndexFile indexFile : indexFiles(fileIO, snapshot)) {
      covered.addAll(indexFile.coveredDataFiles());
    }
    return covered;
  }

  private static List<IndexFile> indexFiles(FileIO fileIO, IndexSnapshot snapshot) {
    List<IndexFile> indexFiles = Lists.newArrayList();
    try {
      IndexManifestList manifestList =
          IndexManifestList.read(fileIO.newInputFile(snapshot.indexManifestList()));
      for (ManifestRef ref : manifestList.manifests()) {
        indexFiles.addAll(IndexManifest.read(fileIO.newInputFile(ref.location())).indexFiles());
      }
    } catch (IOException e) {
      throw new UncheckedIOException(
          "Failed to read index manifests of snapshot " + snapshot.snapshotId(), e);
    }
    return indexFiles;
  }

  @SuppressWarnings("unchecked")
  private static List<String> coveredDataFiles(Row row) {
    return (List<String>) row.getAs(2);
  }

  /** Creates the adapter for the index metadata's definition. */
  private static IndexAdapter adapterFor(IndexMetadata metadata) {
    return IndexTypes.fromType(metadata.indexType()).createAdapter(metadata.definition());
  }

  private static long nextSnapshotId(IndexMetadata metadata) {
    IndexSnapshot latest = metadata.latestSnapshot();
    return latest == null ? 1L : latest.snapshotId() + 1;
  }
}
