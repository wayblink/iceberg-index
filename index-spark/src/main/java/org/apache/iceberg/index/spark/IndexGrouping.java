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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.index.IndexFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

/**
 * Assigns the source data files of a table snapshot to index file groups.
 *
 * <p>One index file can cover several data files: the model has always allowed it ({@code
 * IndexFile.coveredDataFiles} is a list), and grouping is what makes an HNSW graph span more than a
 * single data file, which is the point of doing it. The assignment is computed on the driver from
 * the data files of the snapshot and their record counts, so it is deterministic: the same snapshot
 * and budget always produce the same groups, which is what lets a refresh rebuild exactly the
 * groups that changed.
 *
 * <p>A budget of zero, the default, puts every data file in a group of its own, which is the
 * one-index-file-per-data-file layout. With a budget, data files are packed in snapshot order until
 * adding the next one would exceed it; a data file larger than the budget still gets a group of its
 * own rather than being split, because an index file records whole data files as its coverage.
 */
final class IndexGrouping {

  private IndexGrouping() {}

  /**
   * Assigns each data file of the snapshot to a group.
   *
   * @param table the table to index
   * @param snapshotId the source table snapshot whose data files are grouped
   * @param maxEntriesPerIndexFile the entry budget, or zero for one index file per data file
   * @return a map from data file location to group id, in snapshot order
   */
  static Map<String, Integer> assign(Table table, long snapshotId, long maxEntriesPerIndexFile) {
    List<DataFile> dataFiles = dataFiles(table, snapshotId);
    Map<String, Integer> assignment = Maps.newLinkedHashMap();
    if (maxEntriesPerIndexFile <= 0) {
      int group = 0;
      for (DataFile dataFile : dataFiles) {
        assignment.put(dataFile.path().toString(), group);
        group++;
      }
      return assignment;
    }

    int group = 0;
    long entries = 0L;
    for (DataFile dataFile : dataFiles) {
      long records = Math.max(1L, dataFile.recordCount());
      if (entries > 0 && entries + records > maxEntriesPerIndexFile) {
        group++;
        entries = 0L;
      }
      assignment.put(dataFile.path().toString(), group);
      entries += records;
    }
    return assignment;
  }

  /**
   * Expands a set of data files to the index files that cover them: an index file can cover several
   * data files, so a change to any one of them rewrites the whole file, and every data file it
   * covers has to be rebuilt.
   *
   * @param indexFiles the index files of the parent snapshot, which record their coverage
   * @param dataFiles the data files that changed
   * @return the given files plus every data file covered by an index file that covers one of them
   */
  static Set<String> expandToGroups(List<IndexFile> indexFiles, Set<String> dataFiles) {
    Set<String> expanded = Sets.newLinkedHashSet(dataFiles);
    for (IndexFile indexFile : indexFiles) {
      if (!Sets.intersection(Sets.newHashSet(indexFile.coveredDataFiles()), dataFiles).isEmpty()) {
        expanded.addAll(indexFile.coveredDataFiles());
      }
    }
    return expanded;
  }

  /** Restricts an assignment to the given data files, keeping the group ids they were assigned. */
  static Map<String, Integer> restrict(Map<String, Integer> assignment, Set<String> dataFiles) {
    Map<String, Integer> restricted = new LinkedHashMap<>();
    for (String dataFile : dataFiles) {
      Integer group = assignment.get(dataFile);
      if (group != null) {
        restricted.put(dataFile, group);
      }
    }
    return restricted;
  }

  private static List<DataFile> dataFiles(Table table, long snapshotId) {
    List<DataFile> dataFiles = Lists.newArrayList();
    try (CloseableIterable<FileScanTask> tasks =
        table.newScan().useSnapshot(snapshotId).planFiles()) {
      for (FileScanTask task : tasks) {
        dataFiles.add(task.file());
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to list data files of snapshot " + snapshotId, e);
    }
    // a stable order keeps the grouping deterministic across runs
    dataFiles.sort((first, second) -> first.path().toString().compareTo(second.path().toString()));
    return dataFiles;
  }
}
