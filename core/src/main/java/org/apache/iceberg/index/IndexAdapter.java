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

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Type-specific codec and search strategy for one index.
 *
 * <p>The adapter owns everything specific to an index type: how entries are encoded into and
 * decoded from the payload of an entry-bearing index file, how a query payload is interpreted, how
 * entries are scored, and how merged results are ordered. The generic index layer treats all
 * payloads as opaque bytes.
 */
public interface IndexAdapter {

  /** Returns the definition this adapter was created for. */
  IndexDefinition definition();

  /**
   * Returns the blob type used for entry-bearing index files of this index type, for example {@code
   * iceberg-vector-entries-v1}.
   */
  String entryBlobType();

  /** Encodes entries into the payload of an entry-bearing index file. */
  ByteBuffer encodeEntries(List<IndexEntry> entries);

  /** Decodes the payload of an entry-bearing index file into entries. */
  List<IndexEntry> decodeEntries(ByteBuffer payload);

  /**
   * Returns the top-k results for an encoded query over the entries in one index file payload.
   *
   * @param payload the encoded entries of a single index file
   * @param query the encoded query, produced by this index type
   * @param topK the maximum number of results
   */
  List<IndexSearchResult> search(ByteBuffer payload, ByteBuffer query, int topK);

  /**
   * Orders merged results from multiple index files. Implementations apply the metric direction
   * (for example ascending distance or descending similarity) and break ties on the row reference.
   */
  Comparator<IndexSearchResult> resultComparator();

  /**
   * Returns the blob type of the auxiliary payload this adapter searches, or null when it only
   * searches entry-bearing payloads.
   *
   * <p>An auxiliary payload carries everything needed to search without the entry-bearing payload,
   * for example an HNSW graph plus the mapping from item ids to row references.
   */
  @Nullable
  default String auxiliaryBlobType() {
    return null;
  }

  /**
   * Returns true when this adapter writes auxiliary index files instead of entry-bearing ones for a
   * new snapshot.
   */
  default boolean writesAuxiliaryFiles() {
    return false;
  }

  /** Encodes entries into the payload of an auxiliary index file. */
  default ByteBuffer encodeAuxiliaryPayload(List<IndexEntry> entries) {
    throw new UnsupportedOperationException("This index type does not write auxiliary payloads");
  }

  /**
   * Returns the top-k results for an encoded query over the entries in one auxiliary payload.
   *
   * @param payload the encoded auxiliary content of a single index file
   * @param query the encoded query, produced by this index type
   * @param topK the maximum number of results
   */
  default List<IndexSearchResult> searchAuxiliary(ByteBuffer payload, ByteBuffer query, int topK) {
    throw new UnsupportedOperationException("This index type does not search auxiliary payloads");
  }
}
