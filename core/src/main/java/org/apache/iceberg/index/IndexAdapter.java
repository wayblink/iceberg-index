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
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

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
   * Returns true when this adapter must see all index file payloads of a snapshot together, because
   * scoring needs statistics merged across them.
   *
   * <p>For example a BM25 text index scores a document with corpus-wide document frequency and
   * average document length, which are sums over the per-file statistics of every index file. When
   * this returns true, the reader collects the payloads of each kind and passes them to {@link
   * #searchAll} or {@link #searchAuxiliaryAll} instead of searching file by file. Index types whose
   * scoring is local to one file (vectors) return false and keep the per-file path.
   */
  default boolean searchesAllPayloadsTogether() {
    return false;
  }

  /**
   * Searches all entry-bearing payloads of a snapshot together.
   *
   * <p>The default implementation searches each payload independently and merges the results, which
   * is what the per-file reader path does. Adapters that need merged statistics override this.
   */
  default List<IndexSearchResult> searchAll(List<ByteBuffer> payloads, ByteBuffer query, int topK) {
    return searchAndMerge(payloads, payload -> search(payload, query, topK), topK);
  }

  /**
   * Searches all auxiliary payloads of a snapshot together.
   *
   * <p>Like {@link #searchAll}, but for auxiliary payloads.
   */
  default List<IndexSearchResult> searchAuxiliaryAll(
      List<ByteBuffer> payloads, ByteBuffer query, int topK) {
    return searchAndMerge(payloads, payload -> searchAuxiliary(payload, query, topK), topK);
  }

  /**
   * Encodes one column value into the payload of an index entry.
   *
   * <p>The value is the indexed column's value in the engine's representation: a {@code List<?
   * extends Number>} for numeric array columns, a {@code String} for text columns. Engines use this
   * to build entries without knowing the index type, so a new adapter needs no engine change.
   *
   * @throws UnsupportedOperationException when this index type does not encode raw column values
   */
  default byte[] encodeEntryValue(Object columnValue) {
    throw new UnsupportedOperationException(
        "This index type does not encode raw column values: " + definition().type());
  }

  private List<IndexSearchResult> searchAndMerge(
      List<ByteBuffer> payloads,
      java.util.function.Function<ByteBuffer, List<IndexSearchResult>> searchOne,
      int topK) {
    List<IndexSearchResult> results = Lists.newArrayList();
    for (ByteBuffer payload : payloads) {
      results.addAll(searchOne.apply(payload));
    }
    results.sort(resultComparator());
    if (results.size() > topK) {
      results = Lists.newArrayList(results.subList(0, topK));
    }
    return results;
  }

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

  /**
   * Returns true when this index type is built and refreshed as a single table-wide index file
   * instead of one index file per data file.
   *
   * <p>Index types whose scoring depends on corpus-wide statistics (for example the document
   * frequency and average document length of a BM25 text index) cannot be split per data file: a
   * per-file index would score with stale statistics. A table-wide index file covers all data files
   * of the snapshot and is rebuilt as a whole whenever anything changes.
   */
  default boolean isTableWide() {
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
