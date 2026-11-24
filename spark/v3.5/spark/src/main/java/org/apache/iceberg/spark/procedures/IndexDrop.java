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

import org.apache.iceberg.Table;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.SidecarIndexCatalog;
import org.apache.iceberg.index.spark.IndexBuildStatus;
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
  static Result drop(Table table, IndexMetadata metadata, boolean purge) {
    SidecarIndexCatalog catalog = VectorIndexProcedures.indexCatalog(table);
    boolean dropped = catalog.dropIndex(table.location(), metadata.indexUuid(), purge);
    if (dropped && purge) {
      // the report of the builds of this index lives beside the tree the drop purges, so it is
      // cleared here rather than left behind with nothing to describe
      try {
        IndexBuildStatus.delete(table.io(), IndexBuildStatus.statusDirFor(metadata.location()));
      } catch (RuntimeException e) {
        LOG.warn(
            "Failed to clear the build reports of the dropped index {}", metadata.indexUuid(), e);
      }
    }

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
  static Result dropByUuid(Table table, String indexUuid, boolean purge) {
    SidecarIndexCatalog catalog = VectorIndexProcedures.indexCatalog(table);
    boolean readable;
    try {
      catalog.loadIndex(table.location(), indexUuid);
      readable = true;
    } catch (RuntimeException e) {
      // a damaged entry has no readable metadata: the drop removes the registration and the
      // metadata tree it can still see, but it cannot know the data files the index wrote, so the
      // purge it reports is only the part that happened
      LOG.warn(
          "Dropping index {} without its metadata: its files cannot be purged by the drop",
          indexUuid,
          e);
      readable = false;
    }

    boolean dropped = catalog.dropIndex(table.location(), indexUuid, purge);
    return new Result(indexUuid, dropped, dropped && purge && readable);
  }
}
