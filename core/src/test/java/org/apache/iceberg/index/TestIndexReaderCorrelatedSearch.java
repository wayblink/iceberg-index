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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import org.apache.iceberg.TestTables.LocalFileIO;
import org.apache.iceberg.index.IndexFile.Kind;
import org.apache.iceberg.index.IndexManifestList.ManifestRef;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Tests the correlated search path of the reader and the default merge behavior of the adapter. */
class TestIndexReaderCorrelatedSearch {

  @BeforeAll
  static void registerTypes() {
    IndexTypes.register(new TestingIndexType());
    IndexTypes.register(new TestingCorrelatedIndexDefinition.Type());
  }

  @Test
  public void correlatedSearchSeesAllPayloadsTogether(@TempDir Path tempDir) throws IOException {
    LocalFileIO fileIO = new LocalFileIO();
    TestingCorrelatedIndexDefinition.Adapter adapter =
        new TestingCorrelatedIndexDefinition.Adapter(TestingCorrelatedIndexDefinition.of(4));

    // two index files in one manifest, as a snapshot with two entry-bearing files would have
    String firstFile = tempDir.resolve("index-file-1.puffin").toString();
    String secondFile = tempDir.resolve("index-file-2.puffin").toString();
    String manifestLocation = tempDir.resolve("index-manifest-1.imn").toString();
    String listLocation = tempDir.resolve("index-manifest-list-1.iml").toString();

    writeIndexFile(
        fileIO,
        firstFile,
        adapter,
        List.of(
            IndexEntry.of("data-1.parquet", 0, TestingIndexAdapter.valuePayload(10)),
            IndexEntry.of("data-1.parquet", 1, TestingIndexAdapter.valuePayload(30))));
    writeIndexFile(
        fileIO,
        secondFile,
        adapter,
        List.of(IndexEntry.of("data-2.parquet", 0, TestingIndexAdapter.valuePayload(20))));

    IndexManifest.write(
        fileIO.newOutputFile(manifestLocation),
        List.of(
            IndexFile.builder()
                .location(firstFile)
                .kind(Kind.ENTRY_BEARING)
                .blobType(adapter.entryBlobType())
                .entryCount(2)
                .coveredDataFiles(List.of("data-1.parquet"))
                .build(),
            IndexFile.builder()
                .location(secondFile)
                .kind(Kind.ENTRY_BEARING)
                .blobType(adapter.entryBlobType())
                .entryCount(1)
                .coveredDataFiles(List.of("data-2.parquet"))
                .build()));
    IndexManifestList.write(
        fileIO.newOutputFile(listLocation), List.of(ManifestRef.of(manifestLocation, 2, 3)));

    IndexMetadata metadata =
        IndexMetadata.builder()
            .indexUuid("index-uuid")
            .tableUuid("table-uuid")
            .location(tempDir.toString())
            .definition(TestingCorrelatedIndexDefinition.of(4))
            .addSnapshot(
                IndexSnapshot.builder()
                    .snapshotId(10)
                    .sourceTableSnapshotId(20)
                    .timestampMillis(30)
                    .indexManifestList(listLocation)
                    .build())
            .build();

    // the score of every hit is value + 2 (the payload count), which is only possible when both
    // payloads were handed to the adapter together
    List<IndexSearchResult> results =
        new IndexReader(metadata, fileIO).search(IndexQuery.of(20, ByteBuffer.allocate(4), 2));
    assertThat(results).hasSize(2);
    assertThat(results.get(0).dataFile()).isEqualTo("data-1.parquet");
    assertThat(results.get(0).rowPosition()).isEqualTo(1);
    assertThat(results.get(0).score()).isEqualTo(32.0);
    assertThat(results.get(1).score()).isEqualTo(22.0);
  }

  @Test
  public void defaultSearchAllMergesPerFileResults(@TempDir Path tempDir) throws IOException {
    TestingIndexAdapter adapter = new TestingIndexAdapter(TestingIndexDefinition.of(4));
    ByteBuffer query = ByteBuffer.allocate(4);

    // two payloads searched independently and merged must equal searching them together through
    // the default searchAll
    List<IndexEntry> firstEntries =
        List.of(
            IndexEntry.of("data-1.parquet", 0, TestingIndexAdapter.valuePayload(10)),
            IndexEntry.of("data-1.parquet", 1, TestingIndexAdapter.valuePayload(40)));
    List<IndexEntry> secondEntries =
        List.of(IndexEntry.of("data-2.parquet", 0, TestingIndexAdapter.valuePayload(30)));

    List<IndexSearchResult> merged =
        adapter.searchAll(
            List.of(adapter.encodeEntries(firstEntries), adapter.encodeEntries(secondEntries)),
            query,
            2);

    assertThat(merged).hasSize(2);
    assertThat(merged.get(0).score()).isEqualTo(40.0);
    assertThat(merged.get(1).score()).isEqualTo(30.0);
  }

  @Test
  public void encodeEntryValueRejectsUnsupportedValues() {
    TestingIndexAdapter adapter = new TestingIndexAdapter(TestingIndexDefinition.of(4));
    // the testing adapter does not implement column value encoding, so the default applies
    assertThatThrownBy(() -> adapter.encodeEntryValue("not a number"))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("does not encode raw column values");
  }

  private static void writeIndexFile(
      LocalFileIO fileIO,
      String location,
      TestingCorrelatedIndexDefinition.Adapter adapter,
      List<IndexEntry> entries)
      throws IOException {
    new PuffinIndexFileContainer()
        .write(
            fileIO.newOutputFile(location),
            List.of(IndexBlob.of(adapter.entryBlobType(), adapter.encodeEntries(entries).array())));
  }
}
