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
package org.apache.iceberg.spark.index;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
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
import org.apache.iceberg.index.PuffinIndexFileContainer;
import org.apache.iceberg.index.vector.VectorIndexAdapter;
import org.apache.iceberg.index.vector.VectorIndexDefinition;
import org.apache.iceberg.index.vector.VectorIndexDefinition.Metric;
import org.apache.iceberg.index.vector.VectorIndexTypes;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

/**
 * Builds a vector index snapshot for an Iceberg table snapshot with Spark.
 *
 * <p>Entries are read by {@link VectorIndexEntries}, shuffled by data file so that all rows of a
 * data file land in one partition, and written as one index file per data file per partition. That
 * keeps source-file coverage precise: a refresh only has to drop the index files of data files that
 * were removed or rewritten.
 *
 * <p>Like the core builder, this only writes files and returns updated metadata; publishing it
 * through the {@code IndexCatalog} is a separate step.
 */
public final class SparkIndexBuilder {

  private static final String DATA_DIR = "data";

  static {
    // the vector adapter lives in iceberg-index-vector; register it with the core registry
    VectorIndexTypes.register();
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

    VectorIndexDefinition definition = (VectorIndexDefinition) currentMetadata.definition();
    String indexLocation = currentMetadata.location();
    long snapshotId = nextSnapshotId(currentMetadata);
    FileIO fileIO = table.io();

    List<IndexFile> indexFiles =
        buildIndexFiles(
            spark,
            table,
            indexLocation,
            snapshotId,
            sourceTableSnapshotId,
            vectorColumn,
            definition,
            null);

    String manifestList =
        new IndexSnapshotWriter(fileIO).writeManifestList(indexLocation, snapshotId, indexFiles);

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

    VectorIndexDefinition definition = (VectorIndexDefinition) currentMetadata.definition();
    String indexLocation = currentMetadata.location();
    long snapshotId = previous.snapshotId() + 1;
    FileIO fileIO = table.io();

    Set<String> currentFiles =
        Sets.newHashSet(VectorIndexEntries.dataFiles(table, sourceTableSnapshotId));
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
    List<IndexFile> carried = Lists.newArrayList();
    for (IndexFile indexFile : indexFiles(fileIO, previous)) {
      if (Sets.intersection(Sets.newHashSet(indexFile.coveredDataFiles()), staleFiles).isEmpty()) {
        carried.add(indexFile);
      }
    }

    List<IndexFile> newIndexFiles =
        buildIndexFiles(
            spark,
            table,
            indexLocation,
            snapshotId,
            sourceTableSnapshotId,
            vectorColumn,
            definition,
            Sets.union(newFiles, deleteChangedFiles));
    List<IndexFile> indexFiles = Lists.newArrayList();
    indexFiles.addAll(carried);
    indexFiles.addAll(newIndexFiles);

    String manifestList =
        new IndexSnapshotWriter(fileIO).writeManifestList(indexLocation, snapshotId, indexFiles);

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
      VectorIndexDefinition definition,
      Set<String> onlyFiles) {
    if (onlyFiles != null && onlyFiles.isEmpty()) {
      return Lists.newArrayList();
    }

    int fieldId = definition.fieldId();
    int dimension = definition.dimension();
    Metric metric = definition.metric();
    boolean hnsw = definition.mode() == VectorIndexDefinition.IndexMode.HNSW;
    int maxM = definition.maxM();
    int efConstruction = definition.efConstruction();
    int efSearch = definition.efSearch();
    FileIO fileIO = table.io();

    Dataset<Row> entries =
        VectorIndexEntries.entries(spark, table, sourceTableSnapshotId, vectorColumn, definition);
    if (onlyFiles != null) {
      entries =
          entries.filter(
              functions.col(VectorIndexEntries.DATA_FILE_COLUMN).isin(onlyFiles.toArray()));
    }

    List<Row> indexFileRows =
        entries
            .repartition(functions.col(VectorIndexEntries.DATA_FILE_COLUMN))
            .toJavaRDD()
            .mapPartitions(
                (FlatMapFunction<Iterator<Row>, Row>)
                    iterator -> {
                      IndexFileContainer fileContainer = new PuffinIndexFileContainer();
                      VectorIndexAdapter adapter =
                          new VectorIndexAdapter(
                              VectorIndexDefinition.builder()
                                  .fieldId(fieldId)
                                  .dimension(dimension)
                                  .metric(metric)
                                  .mode(
                                      hnsw
                                          ? VectorIndexDefinition.IndexMode.HNSW
                                          : VectorIndexDefinition.IndexMode.EXACT)
                                  .maxM(maxM)
                                  .efConstruction(efConstruction)
                                  .efSearch(efSearch)
                                  .build());
                      Map<String, List<IndexEntry>> entriesByFile = new LinkedHashMap<>();
                      while (iterator.hasNext()) {
                        Row row = iterator.next();
                        entriesByFile
                            .computeIfAbsent(row.getString(0), key -> Lists.newArrayList())
                            .add(IndexEntry.of(row.getString(0), row.getLong(1), row.getAs(2)));
                      }

                      List<Row> files = Lists.newArrayList();
                      int seq = 0;
                      for (Map.Entry<String, List<IndexEntry>> group : entriesByFile.entrySet()) {
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
                        if (hnsw) {
                          blobType = adapter.auxiliaryBlobType();
                          payload = adapter.encodeAuxiliaryPayload(group.getValue()).array();
                          kind = IndexFile.Kind.AUXILIARY;
                        } else {
                          blobType = adapter.entryBlobType();
                          payload = adapter.encodeEntries(group.getValue()).array();
                          kind = IndexFile.Kind.ENTRY_BEARING;
                        }
                        fileContainer.write(
                            fileIO.newOutputFile(location),
                            List.of(IndexBlob.of(blobType, payload)));
                        files.add(
                            RowFactory.create(
                                location,
                                (long) group.getValue().size(),
                                List.of(group.getKey()),
                                kind.name(),
                                blobType));
                        seq++;
                      }
                      return files.iterator();
                    })
            .collect();

    List<IndexFile> indexFiles = Lists.newArrayListWithCapacity(indexFileRows.size());
    for (Row row : indexFileRows) {
      indexFiles.add(
          IndexFile.builder()
              .location(row.getString(0))
              .kind(IndexFile.Kind.valueOf(row.getString(3)))
              .blobType(row.getString(4))
              .entryCount(row.getLong(1))
              .coveredDataFiles(coveredDataFiles(row))
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
        VectorIndexEntries.deleteFilesByDataFile(table, previousSnapshotId);
    Map<String, Set<String>> currentDeletes =
        VectorIndexEntries.deleteFilesByDataFile(table, currentSnapshotId);
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

  private static long nextSnapshotId(IndexMetadata metadata) {
    IndexSnapshot latest = metadata.latestSnapshot();
    return latest == null ? 1L : latest.snapshotId() + 1;
  }
}
