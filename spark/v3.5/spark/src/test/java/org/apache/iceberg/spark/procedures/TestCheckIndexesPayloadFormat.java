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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.iceberg.index.IndexEntry;
import org.apache.iceberg.index.IndexFile;
import org.apache.iceberg.index.text.TextIndexAdapter;
import org.apache.iceberg.index.text.TextIndexDefinition;
import org.junit.jupiter.api.Test;

/**
 * Pins how the check decides which payloads it can read: for an index type that names its version
 * in the blob type, the manifest alone; for the text index, the format version in the payload
 * header, the way the vector index is checked by its kernel version. The blob type names the role
 * either way and does not change when the payload layout does.
 */
class TestCheckIndexesPayloadFormat {

  private static final String TEXT_AUXILIARY_BLOB_TYPE = "iceberg-text-bm25-v1";
  private static final String TEXT_AUXILIARY_BLOB_TYPE_REPLACED = "iceberg-text-bm25-v2";

  private static TextIndexAdapter textAdapter() {
    return new TextIndexAdapter(TextIndexDefinition.builder().fieldId(2).build());
  }

  private static List<IndexEntry> entries() {
    TextIndexAdapter adapter = textAdapter();
    return List.of(
        IndexEntry.of("a.parquet", 0, adapter.encodeDocument("ice iceberg")),
        IndexEntry.of("a.parquet", 1, adapter.encodeDocument("ice vector")));
  }

  @Test
  public void aPayloadOfTheLayoutThisBuildWritesIsReadable() {
    assertThat(
            CheckIndexesProcedure.readablePayload(
                textAdapter(), IndexFile.Kind.AUXILIARY, TEXT_AUXILIARY_BLOB_TYPE))
        .isTrue();
  }

  @Test
  public void aPayloadOfAReplacedLayoutIsNotReadable() {
    assertThat(
            CheckIndexesProcedure.readablePayload(
                textAdapter(), IndexFile.Kind.AUXILIARY, TEXT_AUXILIARY_BLOB_TYPE_REPLACED))
        .isFalse();
  }

  @Test
  public void aPayloadOfTheFirstLayoutIsNotReadable() {
    // the layouts it no longer reads must be reported, not silently skipped
    assertThat(
            CheckIndexesProcedure.readablePayload(
                textAdapter(), IndexFile.Kind.AUXILIARY, "iceberg-text-bm25-v3"))
        .isFalse();
  }

  @Test
  public void aPayloadOfAnUnknownFormatIsNotReadable() {
    // the version is part of the name, so a blob type this build does not list is exactly what a
    // query would skip, and reporting it as readable would send an operator away with a broken
    // index
    assertThat(
            CheckIndexesProcedure.readablePayload(
                textAdapter(), IndexFile.Kind.AUXILIARY, "iceberg-text-bm25-unknown"))
        .isFalse();
  }

  @Test
  public void anEntryBearingPayloadIsNotReadableJustBecauseTheAuxiliaryOneIs() {
    // a type that writes only auxiliary payloads reads none in the entry-bearing role, and a file
    // that claims that role carries something this build cannot decode
    assertThat(
            CheckIndexesProcedure.readablePayload(
                textAdapter(), IndexFile.Kind.ENTRY_BEARING, TEXT_AUXILIARY_BLOB_TYPE))
        .isFalse();
  }

  @Test
  public void aTextPayloadOfTheVersionThisBuildWritesIsReadable() {
    TextIndexAdapter adapter = textAdapter();
    ByteBuffer payload = adapter.encodeAuxiliaryPayload(entries());

    assertThat(adapter.payloadVersion(payload))
        .as("the version in the signature is the one this build reads")
        .isEqualTo(adapter.payloadFormatVersion());
  }

  @Test
  public void aTextPayloadOfAReplacedVersionIsNotReadable() {
    TextIndexAdapter adapter = textAdapter();
    // the bytes of a payload of this build with only the version in its signature changed: that is
    // what an index written by the previous layout looks like, and the header is the only place a
    // reader can tell it apart from one it can decode
    ByteBuffer payload = adapter.encodeAuxiliaryPayload(entries());
    payload.putInt(payload.position(), 0x424D3101);

    assertThat(adapter.payloadVersion(payload))
        .as("a reader matches the version in the signature")
        .isEqualTo(1)
        .isNotEqualTo(adapter.payloadFormatVersion());
  }

  @Test
  public void aPayloadWithoutASignatureIsNotReadable() {
    TextIndexAdapter adapter = textAdapter();

    assertThatThrownBy(() -> adapter.payloadVersion(ByteBuffer.wrap(new byte[] {1, 2, 3})))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Truncated BM25 payload header");
    assertThatThrownBy(
            () ->
                adapter.payloadVersion(
                    ByteBuffer.wrap("NOTMAGIC".getBytes(StandardCharsets.US_ASCII))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Not a BM25 payload");
  }
}
