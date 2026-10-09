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
import java.util.List;
import org.apache.iceberg.Table;
import org.apache.iceberg.index.IndexFile;
import org.apache.iceberg.index.IndexManifest;
import org.apache.iceberg.index.IndexManifestList;
import org.apache.iceberg.index.IndexManifestList.ManifestRef;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.IndexSnapshot;
import org.apache.iceberg.index.SidecarIndexCatalog;
import org.apache.iceberg.index.vector.VectorIndexDefinition;
import org.apache.iceberg.index.vector.VectorIndexTypes;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;

/** Shared helpers for the vector index SQL procedures and the batch search entry point. */
public final class VectorIndexes {

  private VectorIndexes() {}

  /** Registers the vector index type with the core index type registry. */
  public static void registerVectorIndexType() {
    VectorIndexTypes.register();
  }

  /** Returns the sidecar index catalog for the given table. */
  public static SidecarIndexCatalog indexCatalog(Table table) {
    return new SidecarIndexCatalog(table.io());
  }

  /** Resolves the field ID of the vector column, failing if the column does not exist. */
  public static int fieldId(Table table, String column) {
    Types.NestedField field = table.schema().findField(column);
    if (field == null) {
      throw new IllegalArgumentException(
          "Column " + column + " does not exist in table " + table.name());
    }
    return field.fieldId();
  }

  /** Resolves the column name of a field ID. */
  public static String columnName(Table table, int fieldId) {
    Types.NestedField field = table.schema().findField(fieldId);
    return field == null ? null : field.name();
  }

  /** Finds the vector index of a table by the indexed column, failing if none exists. */
  public static IndexMetadata findIndex(Table table, String column) {
    IndexMetadata metadata = findIndexOrNull(table, column);
    if (metadata == null) {
      throw new IllegalArgumentException(
          "No vector index exists for column " + column + " of table " + table.name());
    }
    return metadata;
  }

  /** Finds the vector index of a table by the indexed column, or null if none exists. */
  public static IndexMetadata findIndexOrNull(Table table, String column) {
    int fieldId = fieldId(table, column);
    for (IndexMetadata metadata : indexCatalog(table).listIndexes(table.location())) {
      if (metadata.definition() instanceof VectorIndexDefinition
          && ((VectorIndexDefinition) metadata.definition()).fieldId() == fieldId) {
        return metadata;
      }
    }
    return null;
  }

  /**
   * Counts the index files and entries of an index snapshot.
   *
   * @return a two-element array: index file count and entry count
   */
  public static long[] counts(FileIO fileIO, IndexSnapshot snapshot) {
    long indexFiles = 0;
    long entries = 0;
    if (snapshot == null) {
      return new long[] {0L, 0L};
    }

    List<IndexFile> files = Lists.newArrayList();
    try {
      IndexManifestList manifestList =
          IndexManifestList.read(fileIO.newInputFile(snapshot.indexManifestList()));
      for (ManifestRef ref : manifestList.manifests()) {
        files.addAll(IndexManifest.read(fileIO.newInputFile(ref.location())).indexFiles());
      }
    } catch (IOException e) {
      throw new UncheckedIOException(
          "Failed to read index manifests of snapshot " + snapshot.snapshotId(), e);
    }

    indexFiles = files.size();
    for (IndexFile indexFile : files) {
      entries += indexFile.entryCount();
    }

    return new long[] {indexFiles, entries};
  }
}
