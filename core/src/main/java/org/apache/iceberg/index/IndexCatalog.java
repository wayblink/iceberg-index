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

import java.util.List;

/**
 * Manages the lifecycle of {@link IndexMetadata} for a table: which indexes exist, how they are
 * discovered, and how their metadata is created, updated and dropped.
 *
 * <p>An index catalog is the seam between the index data model and the place where index metadata
 * is registered. The bundled implementation, {@link SidecarIndexCatalog}, stores a self-contained
 * metadata tree under the table location (the sidecar layout in the index data model design).
 * Registering indexes as first-class members of table metadata, or as REST catalog entities, can be
 * added later as alternative implementations without changing the data model, the file layer, or
 * the reader.
 *
 * <p>The catalog owns metadata only. Publishing a new index snapshot atomically (writing files
 * first, then switching the metadata root) is the job of the index builder.
 */
public interface IndexCatalog {

  /**
   * Registers a new index for a table and writes its initial, snapshot-less metadata.
   *
   * @param request the table, index identity and definition to register
   * @throws IllegalArgumentException if an index with the same UUID already exists for the table
   */
  IndexMetadata createIndex(CreateIndexRequest request);

  /**
   * Loads index metadata by UUID.
   *
   * @param tableLocation the table's location
   * @param indexUuid the index UUID
   * @throws IllegalArgumentException if the index does not exist
   */
  IndexMetadata loadIndex(String tableLocation, String indexUuid);

  /** Lists the indexes registered for a table, in registration order. */
  List<IndexMetadata> listIndexes(String tableLocation);

  /**
   * Lists the indexes registered for a table that belong to the given table instance.
   *
   * <p>An index is stored under the table's location, which outlives the table: dropping a table
   * leaves the sidecar behind, and a table created again at the same location is a different table
   * with a different UUID. The indexes left behind are not the new table's, and reporting them
   * would refuse to build the indexes it needs, so they are left out.
   *
   * <p>The default reports everything, for a catalog that keeps its indexes somewhere the table's
   * own lifecycle removes.
   *
   * @param tableLocation the table's location
   * @param tableUuid the uuid of the table instance asking, or null when it is not known
   */
  default List<IndexMetadata> listIndexes(String tableLocation, String tableUuid) {
    return listIndexes(tableLocation);
  }

  /**
   * Writes updated index metadata back to the index's metadata tree.
   *
   * @param metadata the updated metadata; its {@link IndexMetadata#location()} selects the tree
   */
  IndexMetadata updateIndex(IndexMetadata metadata);

  /**
   * Removes an index from the table's registry.
   *
   * @param tableLocation the table's location
   * @param indexUuid the index UUID
   * @param purge whether to also delete the index's files (manifest lists, manifests, index files,
   *     and metadata); when false, files are left for later cleanup
   * @return true if the index was registered and has been removed
   */
  boolean dropIndex(String tableLocation, String indexUuid, boolean purge);
}
