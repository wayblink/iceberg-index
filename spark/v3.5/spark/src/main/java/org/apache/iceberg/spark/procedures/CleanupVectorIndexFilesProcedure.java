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
package org.apache.iceberg.spark.procedures;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.index.IndexFile;
import org.apache.iceberg.index.IndexManifest;
import org.apache.iceberg.index.IndexManifestList;
import org.apache.iceberg.index.IndexManifestList.ManifestRef;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.IndexSnapshot;
import org.apache.iceberg.index.SidecarIndexCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.procedures.SparkProcedures.ProcedureBuilder;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Cleans up unreferenced files of a table's vector indexes and expires index snapshots whose source
 * table snapshot no longer exists.
 *
 * <p>An index snapshot is bound to a source table snapshot. When that table snapshot is expired,
 * the index snapshot can no longer be queried meaningfully, so it is dropped from the index
 * metadata and its files become eligible for deletion. Only a prefix of stale index snapshots is
 * dropped, and the new oldest snapshot becomes a root, which keeps the parent lineage of the
 * retained snapshots intact.
 *
 * <p>Files under the index tree's {@code data/} and {@code snapshots/} directories that are not
 * referenced by any retained index snapshot are deleted. That covers files of expired index
 * snapshots as well as leftovers of failed builds. The registry and metadata files are never
 * touched. Do not run this while a build or refresh is in flight: files of in-progress writes are
 * not yet referenced and would be deleted.
 *
 * <pre>{@code
 * CALL spark_catalog.system.cleanup_vector_index_files(table => 'db.documents');
 * }</pre>
 */
class CleanupVectorIndexFilesProcedure extends BaseProcedure {
  private static final ProcedureParameter TABLE_PARAM =
      requiredInParameter("table", DataTypes.StringType);
  private static final ProcedureParameter[] PARAMETERS = new ProcedureParameter[] {TABLE_PARAM};

  private static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField("index_uuid", DataTypes.StringType, false, Metadata.empty()),
            new StructField("expired_snapshots", DataTypes.LongType, false, Metadata.empty()),
            new StructField("deleted_files", DataTypes.LongType, false, Metadata.empty()),
            new StructField("deleted_bytes", DataTypes.LongType, false, Metadata.empty())
          });

  public static ProcedureBuilder builder() {
    return new BaseProcedure.Builder<CleanupVectorIndexFilesProcedure>() {
      @Override
      public CleanupVectorIndexFilesProcedure doBuild() {
        return new CleanupVectorIndexFilesProcedure(tableCatalog());
      }
    };
  }

  private CleanupVectorIndexFilesProcedure(TableCatalog tableCatalog) {
    super(tableCatalog);
  }

  @Override
  public ProcedureParameter[] parameters() {
    return PARAMETERS;
  }

  @Override
  public StructType outputType() {
    return OUTPUT_TYPE;
  }

  @Override
  public InternalRow[] call(InternalRow args) {
    VectorIndexProcedures.registerVectorIndexType();
    ProcedureInput input = new ProcedureInput(spark(), tableCatalog(), PARAMETERS, args);

    return modifyIcebergTable(
        input.ident(TABLE_PARAM),
        table -> {
          List<InternalRow> rows = Lists.newArrayList();
          SidecarIndexCatalog catalog = VectorIndexProcedures.indexCatalog(table);
          for (IndexMetadata metadata : catalog.listIndexes(table.location())) {
            rows.add(cleanupIndex(table, catalog, metadata));
          }
          return rows.toArray(new InternalRow[0]);
        });
  }

  private InternalRow cleanupIndex(
      Table table, SidecarIndexCatalog catalog, IndexMetadata metadata) {
    Set<Long> liveTableSnapshotIds = liveTableSnapshotIds(table);
    List<IndexSnapshot> snapshots = metadata.snapshots();

    // drop the prefix of index snapshots whose source table snapshot no longer exists; the first
    // retained snapshot becomes a root so its parent lineage stays valid
    int retainedFrom = 0;
    while (retainedFrom < snapshots.size()
        && !liveTableSnapshotIds.contains(snapshots.get(retainedFrom).sourceTableSnapshotId())) {
      retainedFrom++;
    }

    List<IndexSnapshot> retained = snapshots.subList(retainedFrom, snapshots.size());
    int expiredSnapshots = retainedFrom;

    Set<String> referenced = referencedFiles(table.io(), retained);
    Map<String, Long> orphans = orphanFiles(table, metadata, referenced);
    long deletedBytes = 0L;
    for (long size : orphans.values()) {
      deletedBytes += size;
    }
    deleteFiles(table, orphans.keySet());

    if (expiredSnapshots > 0) {
      catalog.updateIndex(withoutSnapshots(metadata, retained));
    }

    return newInternalRow(
        UTF8String.fromString(metadata.indexUuid()),
        (long) expiredSnapshots,
        (long) orphans.size(),
        deletedBytes);
  }

  private static Set<Long> liveTableSnapshotIds(Table table) {
    Set<Long> snapshotIds = Sets.newHashSet();
    for (Snapshot snapshot : table.snapshots()) {
      snapshotIds.add(snapshot.snapshotId());
    }
    return snapshotIds;
  }

  /** All files referenced by the retained snapshots: manifest lists, manifests and index files. */
  private static Set<String> referencedFiles(
      org.apache.iceberg.io.FileIO fileIO, List<IndexSnapshot> retained) {
    Set<String> referenced = Sets.newHashSet();
    for (IndexSnapshot snapshot : retained) {
      referenced.add(snapshot.indexManifestList());
      try {
        IndexManifestList manifestList =
            IndexManifestList.read(fileIO.newInputFile(snapshot.indexManifestList()));
        for (ManifestRef ref : manifestList.manifests()) {
          referenced.add(ref.location());
          for (IndexFile indexFile :
              IndexManifest.read(fileIO.newInputFile(ref.location())).indexFiles()) {
            referenced.add(indexFile.location());
          }
        }
      } catch (IOException e) {
        throw new UncheckedIOException(
            "Failed to read index manifests of snapshot " + snapshot.snapshotId(), e);
      }
    }
    return referenced;
  }

  /**
   * Lists the files under the index tree's {@code data/} and {@code snapshots/} directories that
   * are not referenced by a retained snapshot, with their sizes.
   */
  private Map<String, Long> orphanFiles(
      Table table, IndexMetadata metadata, Set<String> referenced) {
    Map<String, Long> orphans = Maps.newHashMap();
    try {
      Configuration conf = spark().sessionState().newHadoopConf();
      Path indexDir = new Path(metadata.location());
      FileSystem fileSystem = indexDir.getFileSystem(conf);
      for (String dir :
          new String[] {SidecarIndexCatalog.SNAPSHOTS_DIR, SidecarIndexCatalog.DATA_DIR}) {
        Path directory = new Path(indexDir, dir);
        if (!fileSystem.exists(directory)) {
          continue;
        }

        RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(directory, true);
        while (files.hasNext()) {
          LocatedFileStatus status = files.next();
          String location = status.getPath().toUri().getPath();
          if (!referenced.contains(location) && !referenced.contains(status.getPath().toString())) {
            orphans.put(status.getPath().toString(), status.getLen());
          }
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(
          "Failed to list index files of index " + metadata.indexUuid(), e);
    }
    return orphans;
  }

  private void deleteFiles(Table table, Set<String> files) {
    try {
      Configuration conf = spark().sessionState().newHadoopConf();
      FileSystem fileSystem = new Path(table.location()).getFileSystem(conf);
      for (String file : files) {
        fileSystem.delete(new Path(file), false);
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to delete unreferenced index files", e);
    }
  }

  /** Rebuilds the metadata with only the retained snapshots, truncating the new oldest parent. */
  private static IndexMetadata withoutSnapshots(
      IndexMetadata metadata, List<IndexSnapshot> retained) {
    IndexMetadata.Builder builder =
        IndexMetadata.builder()
            .indexUuid(metadata.indexUuid())
            .tableUuid(metadata.tableUuid())
            .tableLocation(metadata.tableLocation())
            .location(metadata.location())
            .indexType(metadata.indexType())
            .definition(metadata.definition())
            .properties(metadata.properties());
    for (int i = 0; i < retained.size(); i++) {
      IndexSnapshot snapshot = retained.get(i);
      // the new oldest snapshot is a root: its parent was dropped
      builder.addSnapshot(
          i == 0 ? snapshot.toBuilder().parentIndexSnapshotId(null).build() : snapshot);
    }
    return builder.build();
  }
}
