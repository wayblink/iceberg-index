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

import org.apache.iceberg.index.IndexAdapter;
import org.apache.iceberg.index.IndexFile;
import org.apache.iceberg.index.text.TextIndexAdapter;
import org.apache.iceberg.index.text.TextIndexDefinition;
import org.junit.jupiter.api.Test;

/**
 * Pins which payloads the check accepts for an index type that names its version in the blob type,
 * which is the only thing that can tell such a payload apart from one this build cannot read.
 */
class TestCheckIndexesPayloadFormat {

  private static final String TEXT_AUXILIARY_BLOB_TYPE = "iceberg-text-bm25-v1";

  private static IndexAdapter textAdapter() {
    return new TextIndexAdapter(TextIndexDefinition.builder().fieldId(2).build());
  }

  @Test
  public void aPayloadOfTheVersionThisBuildWritesIsReadable() {
    assertThat(
            CheckIndexesProcedure.readablePayload(
                textAdapter(), IndexFile.Kind.AUXILIARY, TEXT_AUXILIARY_BLOB_TYPE))
        .isTrue();
  }

  @Test
  public void aPayloadOfALaterVersionIsNotReadable() {
    // the version is part of the name, so a blob type this build does not list is exactly what a
    // query would skip, and reporting it as readable would send an operator away with a broken
    // index
    assertThat(
            CheckIndexesProcedure.readablePayload(
                textAdapter(), IndexFile.Kind.AUXILIARY, "iceberg-text-bm25-v2"))
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
}
