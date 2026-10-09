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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * A minimal {@link IndexAdapter} used by core tests: entry payloads are a single int value, search
 * scores by that value in descending order, and no auxiliary payloads are used.
 */
public final class TestingIndexAdapter implements IndexAdapter {

  /** Blob type of testing index files. */
  public static final String BLOB_TYPE = "iceberg-testing-entries-v1";

  private static final int FORMAT_VERSION = 1;

  private final TestingIndexDefinition definition;

  public TestingIndexAdapter(TestingIndexDefinition definition) {
    this.definition = Preconditions.checkNotNull(definition, "Testing definition is null");
  }

  /** Encodes an entry payload for the given value. */
  public static byte[] valuePayload(int value) {
    ByteBuffer buffer = ByteBuffer.allocate(4);
    buffer.putInt(value);
    return buffer.array();
  }

  /** Decodes an entry payload into its value. */
  public static int payloadValue(byte[] payload) {
    return ByteBuffer.wrap(payload).getInt();
  }

  @Override
  public IndexDefinition definition() {
    return definition;
  }

  @Override
  public String entryBlobType() {
    return BLOB_TYPE;
  }

  @Override
  public ByteBuffer encodeEntries(List<IndexEntry> entries) {
    Preconditions.checkNotNull(entries, "Index entries are null");
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(bytes)) {
      out.writeInt(FORMAT_VERSION);
      out.writeInt(entries.size());
      for (IndexEntry entry : entries) {
        out.writeUTF(entry.dataFile());
        out.writeLong(entry.rowPosition());
        byte[] payload = entry.payload();
        Preconditions.checkArgument(payload.length == 4, "Invalid testing payload length");
        out.write(payload);
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to encode testing entries", e);
    }
    return ByteBuffer.wrap(bytes.toByteArray());
  }

  @Override
  public List<IndexEntry> decodeEntries(ByteBuffer payload) {
    Preconditions.checkNotNull(payload, "Entry payload is null");
    byte[] bytes = new byte[payload.remaining()];
    payload.duplicate().get(bytes);
    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
      int version = in.readInt();
      Preconditions.checkArgument(
          version == FORMAT_VERSION, "Unsupported testing entries format version: %s", version);
      int count = in.readInt();
      List<IndexEntry> entries = Lists.newArrayList();
      for (int i = 0; i < count; i++) {
        String dataFile = in.readUTF();
        long rowPosition = in.readLong();
        byte[] entryPayload = new byte[4];
        in.readFully(entryPayload);
        entries.add(IndexEntry.of(dataFile, rowPosition, entryPayload));
      }
      return ImmutableList.copyOf(entries);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to decode testing entries", e);
    }
  }

  @Override
  public List<IndexSearchResult> search(ByteBuffer payload, ByteBuffer query, int topK) {
    Preconditions.checkArgument(topK >= 0, "Top-k must be non-negative: %s", topK);
    Preconditions.checkNotNull(query, "Query is null");
    int queryValue = query.duplicate().getInt(0);
    if (topK == 0) {
      return ImmutableList.of();
    }

    List<IndexEntry> entries = decodeEntries(payload);
    List<IndexSearchResult> results = Lists.newArrayList();
    for (IndexEntry entry : entries) {
      results.add(
          IndexSearchResult.of(
              entry.dataFile(), entry.rowPosition(), payloadValue(entry.payload()) - queryValue));
    }

    results.sort(resultComparator());
    if (results.size() > topK) {
      results = Lists.newArrayList(results.subList(0, topK));
    }
    return ImmutableList.copyOf(results);
  }

  @Override
  public Comparator<IndexSearchResult> resultComparator() {
    Comparator<IndexSearchResult> tieBreaker =
        Comparator.comparing(IndexSearchResult::dataFile)
            .thenComparingLong(IndexSearchResult::rowPosition);
    return Comparator.comparingDouble(IndexSearchResult::score)
        .reversed()
        .thenComparing(tieBreaker);
  }
}
