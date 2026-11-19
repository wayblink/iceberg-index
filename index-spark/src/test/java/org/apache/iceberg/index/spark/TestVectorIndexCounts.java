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

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.apache.iceberg.TestTables.LocalFileIO;
import org.apache.iceberg.index.IndexManifestList;
import org.apache.iceberg.index.IndexManifestList.ManifestRef;
import org.apache.iceberg.index.IndexSnapshot;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestVectorIndexCounts {

  @Test
  public void countsSumTheManifestListSummaries(@TempDir Path tempDir) throws IOException {
    try (LocalFileIO fileIO = new LocalFileIO()) {
      String indexLocation = tempDir.resolve("indexes/index-uuid").toString();
      String manifestList = indexLocation + "/snapshots/index-manifest-list-1-attempt-1";
      IndexManifestList.write(
          fileIO.newOutputFile(manifestList),
          List.of(
              ManifestRef.of(indexLocation + "/snapshots/index-manifest-1-0-a", 3, 30L),
              ManifestRef.of(indexLocation + "/snapshots/index-manifest-1-1-b", 2, 20L)));

      IndexSnapshot snapshot =
          IndexSnapshot.builder()
              .snapshotId(1L)
              .sourceTableSnapshotId(1L)
              .timestampMillis(1L)
              .indexManifestList(manifestList)
              .build();

      // the manifests themselves are not written: the counts come from the list, which is what
      // keeps
      // a listing to one read per index instead of one read per manifest
      assertThat(VectorIndexes.counts(fileIO, snapshot)).containsExactly(5L, 50L);
    }
  }

  @Test
  public void countsOfASnapshotWithoutFilesAreZero(@TempDir Path tempDir) throws IOException {
    try (LocalFileIO fileIO = new LocalFileIO()) {
      String indexLocation = tempDir.resolve("indexes/index-uuid").toString();
      String manifestList = indexLocation + "/snapshots/index-manifest-list-1-attempt-1";
      IndexManifestList.write(fileIO.newOutputFile(manifestList), List.of());

      IndexSnapshot snapshot =
          IndexSnapshot.builder()
              .snapshotId(1L)
              .sourceTableSnapshotId(1L)
              .timestampMillis(1L)
              .indexManifestList(manifestList)
              .build();

      assertThat(VectorIndexes.counts(fileIO, snapshot)).containsExactly(0L, 0L);
    }
  }

  @Test
  public void countsOfAMissingSnapshotAreZero() {
    assertThat(VectorIndexes.counts(new LocalFileIO(), null)).containsExactly(0L, 0L);
  }
}
