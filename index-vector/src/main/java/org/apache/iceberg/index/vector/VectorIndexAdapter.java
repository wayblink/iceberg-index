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
package org.apache.iceberg.index.vector;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.index.IndexAdapter;
import org.apache.iceberg.index.IndexBuildResult;
import org.apache.iceberg.index.IndexDefinition;
import org.apache.iceberg.index.IndexEntry;
import org.apache.iceberg.index.IndexSearchResult;
import org.apache.iceberg.index.vector.VectorIndexDefinition.VectorStructure;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;

/**
 * Turns index entries into payloads and searches them, by handing the work to a kernel.
 *
 * <p>What this class does is everything that does not depend on how the vectors are organised: the
 * definition's identity, the blob types an index file is recorded under, the encoding of a single
 * vector, and how results from several index files are ordered. Building a payload and searching
 * one belongs to a {@link VectorKernel}, which is the seam a third-party index library is plugged
 * into.
 *
 * <p>A definition is checked against what a kernel says it can build before anything else happens,
 * so a request for a structure or a quantizer this build does not implement is refused by name
 * rather than producing an index nothing could search.
 */
public final class VectorIndexAdapter implements IndexAdapter, java.io.Serializable {

  private static final long serialVersionUID = 1L;

  public static final String ENTRY_BLOB_TYPE = "iceberg-vector-entries-v1";

  public static final String AUXILIARY_BLOB_TYPE = "iceberg-vector-auxiliary-v1";

  private final VectorIndexDefinition definition;
  private final VectorKernel kernel;
  private final VectorScoring scoring;

  public VectorIndexAdapter(VectorIndexDefinition definition) {
    Preconditions.checkNotNull(definition, "Vector index definition is null");
    this.definition = definition;
    // the kernel is resolved once, so a definition nothing can build fails here rather than in the
    // middle of a build, and the options are parsed as part of making it
    this.kernel = VectorKernels.forDefinition(definition);
    this.scoring = new VectorScoring(definition.metric());
  }

  @Override
  public IndexDefinition definition() {
    return definition;
  }

  @Override
  public String entryBlobType() {
    return ENTRY_BLOB_TYPE;
  }

  @Override
  public String auxiliaryBlobType() {
    return AUXILIARY_BLOB_TYPE;
  }

  @Override
  public Set<String> readableAuxiliaryBlobTypes() {
    // one type per role: the algorithm that wrote a payload is recorded inside it, so it never
    // belongs in the type a reader looks for
    return ImmutableSet.of(AUXILIARY_BLOB_TYPE);
  }

  @Override
  public boolean writesAuxiliaryFiles() {
    // a flat scan keeps its entries in the entry-bearing payload; a structure over them needs a
    // payload of its own
    return definition.structure() != VectorStructure.FLAT;
  }

  public byte[] encodeVector(float[] vector) {
    return VectorPayloads.encodeVector(vector, definition.dimension(), "Index");
  }

  public ByteBuffer encodeQuery(float[] query) {
    Preconditions.checkNotNull(query, "Query vector is null");
    return ByteBuffer.wrap(VectorPayloads.encodeVector(query, definition.dimension(), "Query"));
  }

  @Override
  public byte[] encodeEntryValue(Object columnValue) {
    List<Float> values =
        org.apache.iceberg.relocated.com.google.common.collect.Lists.newArrayList();
    for (Object value : (Iterable<?>) columnValue) {
      values.add(((Number) value).floatValue());
    }

    float[] vector = new float[values.size()];
    for (int i = 0; i < vector.length; i++) {
      vector[i] = values.get(i);
    }
    Preconditions.checkArgument(
        vector.length == definition.dimension(),
        "Vector column has %s dimensions, the index was built for %s",
        vector.length,
        definition.dimension());
    return encodeVector(vector);
  }

  /** The vector an entry holds, for a caller that has a payload rather than the row. */
  public float[] decodeVector(byte[] payload) {
    return VectorPayloads.decodeVector(payload, definition.dimension(), "Entry");
  }

  /** The vector a query encodes to. */
  public float[] decodeQuery(ByteBuffer query) {
    return VectorPayloads.decodeVector(
        VectorPayloads.toBytes(query), definition.dimension(), "Query");
  }

  @Override
  public ByteBuffer encodeEntries(List<IndexEntry> entries) {
    return kernel.build(entries, 1);
  }

  @Override
  public IndexBuildResult encodeEntriesWithMemory(List<IndexEntry> entries) {
    return kernel.buildWithMemory(entries, 1);
  }

  @Override
  public List<IndexEntry> decodeEntries(ByteBuffer payload) {
    return kernel.entries(payload);
  }

  @Override
  public PreparedSearch prepare(ByteBuffer payload) {
    return kernel.prepare(payload);
  }

  @Override
  public List<IndexSearchResult> search(ByteBuffer payload, ByteBuffer query, int topK) {
    return kernel.search(payload, query, topK);
  }

  @Override
  public ByteBuffer encodeAuxiliaryPayload(List<IndexEntry> entries) {
    return kernel.build(entries, 1);
  }

  @Override
  public ByteBuffer encodeAuxiliaryPayload(List<IndexEntry> entries, int parallelism) {
    return kernel.build(entries, parallelism);
  }

  @Override
  public IndexBuildResult encodeAuxiliaryPayloadWithMemory(
      List<IndexEntry> entries, int parallelism) {
    return kernel.buildWithMemory(entries, parallelism);
  }

  @Override
  public PreparedSearch prepareAuxiliary(ByteBuffer payload) {
    return kernel.prepare(payload);
  }

  @Override
  public List<IndexSearchResult> searchAuxiliary(ByteBuffer payload, ByteBuffer query, int topK) {
    return kernel.search(payload, query, topK);
  }

  @Override
  public Comparator<IndexSearchResult> resultComparator() {
    return scoring.comparator();
  }

  /** The kernel this definition resolved to, for tests and for a caller that has to report it. */
  public VectorKernel kernel() {
    return kernel;
  }
}
