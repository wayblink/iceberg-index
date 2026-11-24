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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Table;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.SidecarIndexCatalog;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Removes one index of a table: unregisters it, optionally deletes its files, and clears the report
 * of its builds.
 *
 * <p>The index and vector index procedures differ only in how they resolve the index to remove, so
 * they share this part: a drop of either kind has to leave the table's metadata pointing at no
 * index, and an index whose files were deleted must not leave a build report behind.
 */
final class IndexDrop {
  private static final Logger LOG = LoggerFactory.getLogger(IndexDrop.class);

  private IndexDrop() {}

  /** What a drop did: the index it removed, whether it was registered, and whether files went. */
  record Result(String indexUuid, boolean dropped, boolean purged) {}

  /**
   * Removes the index and reports what was done.
   *
   * <p>{@code purge} deletes the index files. Leaving them behind is for a caller that wants to
   * read the index after it is unregistered; such files are garbage to everyone else.
   */
  static Result drop(SparkSession spark, Table table, IndexMetadata metadata, boolean purge) {
    SidecarIndexCatalog catalog = VectorIndexProcedures.indexCatalog(table);
    if (purge) {
      // before the registration goes: a tree that cannot be deleted that way leaves the entry in
      // place, so the drop can be retried instead of leaving files nothing names any more
      purgeTree(spark, table, metadata.indexUuid());
    }

    // the tree already holds no file: the registration is removed without a second purge, which
    // would read the metadata the tree no longer has
    boolean dropped = catalog.dropIndex(table.location(), metadata.indexUuid(), false);

    // the column reports whether files were purged, which is what the caller asked for and what
    // the row is for; whether the index was still registered is an error when it was not
    return new Result(metadata.indexUuid(), dropped, dropped && purge);
  }

  /**
   * Removes a registry entry by UUID, for one whose metadata can no longer be read.
   *
   * <p>The listing skips such an entry, so it cannot be resolved by name or column; the UUID is the
   * only identifier that survives. A purge still removes what is left of the tree, so a drop of a
   * damaged entry is the repair path.
   *
   * @return whether the registry named the entry, which is false for a UUID it never held
   */
  static Result dropByUuid(SparkSession spark, Table table, String indexUuid, boolean purge) {
    SidecarIndexCatalog catalog = VectorIndexProcedures.indexCatalog(table);
    if (purge) {
      // a damaged entry has no readable metadata to enumerate its files from, so the tree its UUID
      // names is the only complete list; it is deleted before the entry so a failure leaves the
      // entry to retry
      purgeTree(spark, table, indexUuid);
    }

    // the tree already holds no file: the entry is removed without a second purge, which would
    // read the metadata the tree no longer has
    boolean dropped = catalog.dropIndex(table.location(), indexUuid, false);
    return new Result(indexUuid, dropped, dropped && purge);
  }

  /**
   * Deletes the tree under one index's UUID, which is everything a purge has to remove.
   *
   * <p>Enumerating payloads from the metadata finds the files a retained snapshot names, which
   * leaves the files an attempt wrote but never published, and for a damaged entry the metadata is
   * exactly what is gone. Nothing under {@code indexes/<uuid>} is shared with another index and
   * nothing names the directory after the entry is removed, so the purge deletes it whole.
   *
   * <p>The UUID becomes a path, and it comes from a registry a hand edit could corrupt: a value
   * that walked out of the index root would turn a purge into a delete of something else, so a
   * value that is not a plain name is refused.
   */
  static void purgeTree(SparkSession spark, Table table, String indexUuid) {
    if (indexUuid == null || !indexUuid.matches("[A-Za-z0-9_-]+")) {
      throw new IllegalArgumentException("Invalid index UUID for a purge: " + indexUuid);
    }

    String location = SidecarIndexCatalog.locationOf(table.location(), indexUuid);
    Path path = new Path(location);
    try {
      FileSystem fileSystem = path.getFileSystem(spark.sessionState().newHadoopConf());
      if (fileSystem.exists(path) && !fileSystem.delete(path, true)) {
        throw new UncheckedIOException(
            new IOException("Failed to delete the index tree " + location));
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to delete the index tree " + location, e);
    }
  }
}
