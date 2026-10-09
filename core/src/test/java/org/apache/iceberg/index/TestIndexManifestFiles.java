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

import static org.apache.iceberg.Files.localInput;
import static org.apache.iceberg.Files.localOutput;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.apache.iceberg.index.IndexFile.Kind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestIndexManifestFiles {

  @Test
  public void roundTripsManifestAndList(@TempDir Path tempDir) throws IOException {
    IndexFile entryBearing =
        IndexFile.builder()
            .location("index-file-1.puffin")
            .kind(Kind.ENTRY_BEARING)
            .blobType(VectorIndexAdapter.ENTRY_BLOB_TYPE)
            .entryCount(2)
            .coveredDataFiles(List.of("data-1.parquet", "data-2.parquet"))
            .build();
    IndexFile auxiliary =
        IndexFile.builder()
            .location("index-file-2.puffin")
            .kind(Kind.AUXILIARY)
            .blobType("iceberg-vector-graph-v1")
            .entryCount(0)
            .coveredDataFiles(List.of())
            .build();

    String manifestLocation = tempDir.resolve("index-manifest-1.imn").toString();
    IndexManifest.write(localOutput(manifestLocation), List.of(entryBearing, auxiliary));
    IndexManifest manifest = IndexManifest.read(localInput(manifestLocation));
    assertThat(manifest.indexFiles()).containsExactly(entryBearing, auxiliary);
    assertThat(manifest.indexedDataFiles())
        .containsExactlyInAnyOrder("data-1.parquet", "data-2.parquet");

    String listLocation = tempDir.resolve("index-manifest-list-1.iml").toString();
    IndexManifestList.ManifestRef ref = IndexManifestList.ManifestRef.of(manifestLocation, 2, 2);
    IndexManifestList.write(localOutput(listLocation), List.of(ref));
    IndexManifestList list = IndexManifestList.read(localInput(listLocation));
    assertThat(list.manifests()).containsExactly(ref);
  }
}
