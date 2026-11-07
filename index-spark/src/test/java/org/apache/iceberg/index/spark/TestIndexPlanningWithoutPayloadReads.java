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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;
import org.apache.iceberg.Files;
import org.apache.iceberg.index.IndexFile;
import org.apache.iceberg.index.IndexFileMemory;
import org.apache.iceberg.index.IndexManifest;
import org.apache.iceberg.index.IndexManifestList;
import org.apache.iceberg.index.vector.VectorIndexAdapter;
import org.apache.iceberg.index.vector.VectorIndexDefinition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Planning prices every file of an index on the driver before anything else can happen, so a file
 * whose record is missing or does not describe this runtime must be bounded from its length instead
 * of being read. These tests count the payload streams a planning pass opens: that property fails
 * loudly when a payload is read again, unlike a duration, which depends on the machine.
 */
class TestIndexPlanningWithoutPayloadReads {
  @TempDir private File tempDir;

  @Test
  void aMatchingRecordIsTrustedAndNothingIsRead() {
    TestIndexFixtures.CountingIO io = new TestIndexFixtures.CountingIO();
    TestIndexFixtures.BuiltIndex built =
        TestIndexFixtures.build(tempDir, io, "idx-record", "data.parquet");
    List<IndexFile> published = publishedFiles(built);
    io.reset();

    IndexCacheProtocol.Target target = IndexCacheController.target(built.table(), built.metadata());

    assertThat(target.files()).hasSameSizeAs(published);
    for (IndexCacheProtocol.File planned : target.files()) {
      IndexFileMemory recorded = fileAt(published, planned.location()).memory();
      assertThat(recorded).isNotNull();
      assertThat(planned.memory()).isEqualTo(recorded);
      assertThat(planned.estimatedBytes()).isEqualTo(IndexCacheMemory.residentBytes(recorded));
    }

    assertThat(io.payloadStreams()).isZero();
    assertThat(io.payloadLengths()).as("a matching record needs no length either").isZero();
  }

  @Test
  void aMissingRecordIsBoundedFromTheLengthWithoutReadingThePayload() {
    TestIndexFixtures.CountingIO io = new TestIndexFixtures.CountingIO();
    TestIndexFixtures.BuiltIndex built =
        TestIndexFixtures.build(tempDir, io, "idx-no-record", "data.parquet");
    rewriteMemory(built, recorded -> null);
    io.reset();

    IndexCacheProtocol.Target target = IndexCacheController.target(built.table(), built.metadata());

    assertThat(target.files()).isNotEmpty();
    for (IndexCacheProtocol.File planned : target.files()) {
      long length = Files.localInput(planned.location()).getLength();
      assertThat(planned.memory()).isNotNull();
      assertThat(planned.memory().preparedBytes())
          .as("the legacy rule bounds the file from its bytes")
          .isEqualTo(IndexCacheMemory.residentBytes(length, 0L));
      assertThat(planned.estimatedBytes())
          .isEqualTo(IndexCacheMemory.residentBytes(planned.memory()))
          .isPositive();
    }

    assertThat(io.payloadStreams()).as("planning must not read an index payload").isZero();
    assertThat(io.payloadLengths())
        .as("the length of every file is asked for once, and a length is not a payload read")
        .isEqualTo(target.files().size());
  }

  @Test
  void anIncompatibleRecordIsBoundedWithoutReadingAndCarriesTheCurrentReader() {
    TestIndexFixtures.CountingIO io = new TestIndexFixtures.CountingIO();
    TestIndexFixtures.BuiltIndex built =
        TestIndexFixtures.build(tempDir, io, "idx-incompatible", "data.parquet");
    rewriteMemory(
        built,
        recorded ->
            recorded.toBuilder()
                .modelVersion(1)
                .readerId("old-reader")
                .runtimeProfile("old-runtime")
                .build());
    List<IndexFile> published = publishedFiles(built);
    io.reset();

    IndexFileMemory descriptor = IndexCacheMemory.currentDescriptor(adapter(built));
    assertThat(descriptor).isNotNull();

    IndexCacheProtocol.Target target = IndexCacheController.target(built.table(), built.metadata());

    assertThat(target.files()).isNotEmpty();
    for (IndexCacheProtocol.File planned : target.files()) {
      IndexFileMemory recorded = fileAt(published, planned.location()).memory();
      assertThat(recorded).isNotNull();
      assertThat(recorded.fileBytes()).isNotNull();
      assertThat(planned.memory()).isNotNull();
      // The record still states the container size, so the bound needs no file system call.
      assertThat(planned.memory().preparedBytes())
          .isEqualTo(IndexCacheMemory.residentBytes(recorded.fileBytes(), 0L));
      // The bound describes this runtime's resident state, so it carries the descriptor of the
      // reader that will load the file rather than the one that wrote the record.
      assertThat(planned.memory().modelVersion()).isEqualTo(descriptor.modelVersion());
      assertThat(planned.memory().readerId()).isEqualTo(descriptor.readerId());
      assertThat(planned.memory().runtimeProfile()).isEqualTo(descriptor.runtimeProfile());
      assertThat(IndexCacheMemory.residentBytes(planned.memory())).isPositive();
    }

    assertThat(io.payloadStreams()).isZero();
    assertThat(io.payloadLengths()).as("the record states the length already").isZero();
  }

  @Test
  void aQueryWorkingSetIsBoundedFromLengthsWithoutReadingPayloads() {
    TestIndexFixtures.CountingIO io = new TestIndexFixtures.CountingIO();
    TestIndexFixtures.BuiltIndex built =
        TestIndexFixtures.build(tempDir, io, "idx-working-set", "data.parquet", "data-2.parquet");
    rewriteMemory(built, recorded -> null);
    List<IndexFile> published = publishedFiles(built);
    io.reset();

    long workingSet =
        DistributedIndexSearch.workingSetBytes(
            io, published, adapter(built), built.table().location());

    assertThat(workingSet).as("a bounded working set is not unknown").isPositive();
    assertThat(io.payloadStreams())
        .as("the first query of an index must not read its payloads")
        .isZero();
    assertThat(io.payloadLengths()).isEqualTo(published.size());
  }

  @Test
  void aFileWhoseLengthIsUnknownIsPlannedAsUnknownRatherThanFailing() {
    TestIndexFixtures.CountingIO io = new TestIndexFixtures.CountingIO();
    TestIndexFixtures.BuiltIndex built =
        TestIndexFixtures.build(tempDir, io, "idx-missing", "data.parquet");
    rewriteManifest(
        built, file -> rebuild(file).location(file.location() + ".missing").memory(null).build());
    List<IndexFile> published = publishedFiles(built);
    io.reset();

    IndexCacheProtocol.Target target = IndexCacheController.target(built.table(), built.metadata());

    assertThat(target.files()).isNotEmpty();
    assertThat(target.files())
        .as("an unreadable length leaves the file explicitly unknown, not free")
        .allSatisfy(planned -> assertThat(planned.estimatedBytes()).isNull())
        .allSatisfy(planned -> assertThat(planned.memory()).isNull());
    assertThat(io.payloadStreams()).isZero();
    assertThat(
            DistributedIndexSearch.workingSetBytes(
                io, published, adapter(built), built.table().location()))
        .isEqualTo(-1);
  }

  private static VectorIndexAdapter adapter(TestIndexFixtures.BuiltIndex built) {
    return new VectorIndexAdapter((VectorIndexDefinition) built.metadata().definition());
  }

  private static IndexFile fileAt(List<IndexFile> files, String location) {
    for (IndexFile file : files) {
      if (file.location().equals(location)) {
        return file;
      }
    }

    throw new AssertionError("No index file at " + location);
  }

  private static List<IndexFile> publishedFiles(TestIndexFixtures.BuiltIndex built) {
    try {
      return IndexManifest.read(built.table().io().newInputFile(manifestLocation(built)))
          .indexFiles();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static String manifestLocation(TestIndexFixtures.BuiltIndex built) {
    String listLocation = built.metadata().snapshots().get(0).indexManifestList();
    try {
      return IndexManifestList.read(built.table().io().newInputFile(listLocation))
          .manifests()
          .get(0)
          .location();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Replaces the manifest of the built index, which is how a test states what a record says. */
  private static void rewriteManifest(
      TestIndexFixtures.BuiltIndex built, UnaryOperator<IndexFile> rewrite) {
    String manifestLocation = manifestLocation(built);
    List<IndexFile> rewritten = new ArrayList<>();
    try {
      for (IndexFile file :
          IndexManifest.read(built.table().io().newInputFile(manifestLocation)).indexFiles()) {
        rewritten.add(rewrite.apply(file));
      }

      built.table().io().deleteFile(manifestLocation);
      IndexManifest.write(built.table().io().newOutputFile(manifestLocation), rewritten);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static void rewriteMemory(
      TestIndexFixtures.BuiltIndex built, UnaryOperator<IndexFileMemory> memory) {
    rewriteManifest(built, file -> rebuild(file).memory(memory.apply(file.memory())).build());
  }

  private static IndexFile.Builder rebuild(IndexFile file) {
    return IndexFile.builder()
        .location(file.location())
        .kind(file.kind())
        .blobType(file.blobType())
        .entryCount(file.entryCount())
        .coveredDataFiles(file.coveredDataFiles());
  }
}
