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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import org.apache.iceberg.TestTables.LocalFileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Which index files a reader picks up, by the blob type they were recorded under.
 *
 * <p>The reader filters on the blob type, so renaming one is not cosmetic: a reader that recognised
 * only the name it writes would skip every index written before the rename, and skip it silently.
 * An adapter therefore reports every name it can read, and this records a file under one of the old
 * names to check it is still found.
 */
class TestIndexReaderBlobTypes {

  @BeforeAll
  static void registerTestingIndexType() {
    IndexTypes.register(new TestingIndexType());
  }

  @Test
  public void readsAFileRecordedUnderABlobTypeTheAdapterNoLongerWrites(@TempDir Path tempDir)
      throws IOException {
    LocalFileIO fileIO = new LocalFileIO();
    TestingIndexAdapter adapter = new TestingIndexAdapter(TestingIndexDefinition.of(2));
    IndexFileContainer container = new PuffinIndexFileContainer();

    String file = tempDir.resolve("index-file-renamed.puffin").toString();
    String manifestLocation = tempDir.resolve("index-manifest-renamed.imn").toString();
    String listLocation = tempDir.resolve("index-manifest-list-renamed.iml").toString();

    byte[] payload =
        adapter
            .encodeEntries(
                Lists.newArrayList(
                    IndexEntry.of("data-1.parquet", 0, TestingIndexAdapter.valuePayload(10)),
                    IndexEntry.of("data-2.parquet", 1, TestingIndexAdapter.valuePayload(2))))
            .array();
    container.write(
        fileIO.newOutputFile(file),
        List.of(IndexBlob.of(TestingIndexAdapter.LEGACY_BLOB_TYPE, payload)));

    // recorded under the name the adapter no longer writes
    IndexManifest.write(
        fileIO.newOutputFile(manifestLocation),
        List.of(
            IndexFile.builder()
                .location(file)
                .kind(IndexFile.Kind.ENTRY_BEARING)
                .blobType(TestingIndexAdapter.LEGACY_BLOB_TYPE)
                .entryCount(2)
                .coveredDataFiles(List.of("data-1.parquet", "data-2.parquet"))
                .build()));
    IndexManifestList.write(
        fileIO.newOutputFile(listLocation),
        List.of(IndexManifestList.ManifestRef.of(manifestLocation, 2, 3)));

    IndexMetadata metadata =
        IndexMetadata.builder()
            .indexUuid("index-uuid")
            .tableUuid("table-uuid")
            .location(tempDir.toString())
            .definition(adapter.definition())
            .addSnapshot(
                IndexSnapshot.builder()
                    .snapshotId(10)
                    .sourceTableSnapshotId(20)
                    .timestampMillis(30)
                    .indexManifestList(listLocation)
                    .build())
            .build();

    IndexReader reader = new IndexReader(metadata, fileIO);
    assertThat(reader.search(IndexQuery.of(20, query(), 1)))
        .as("a file under the old blob type is read, not skipped")
        .extracting(IndexSearchResult::dataFile)
        .containsExactly("data-1.parquet");

    // and its data files count as covered, which a skipped file would not
    assertThat(reader.uncoveredDataFiles(20, List.of("data-1.parquet", "data-2.parquet")))
        .isEmpty();
  }

  private static ByteBuffer query() {
    ByteBuffer query = ByteBuffer.allocate(4);
    query.putInt(5);
    query.flip();
    return query;
  }
}
