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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestPuffinIndexFileContainer {

  private final IndexFileContainer container = new PuffinIndexFileContainer();

  @Test
  public void roundTripsMultipleBlobs(@TempDir Path tempDir) throws IOException {
    String location = tempDir.resolve("index-file-1.puffin").toString();
    IndexBlob first = IndexBlob.of("iceberg-vector-entries-v1", new byte[] {1, 2, 3, 4});
    IndexBlob second = IndexBlob.of("iceberg-vector-graph-v1", new byte[] {9});

    container.write(localOutput(location), List.of(first, second));
    assertThat(container.read(localInput(location))).containsExactly(first, second);
  }

  @Test
  public void compressesBlobsAndReadsThemBack(@TempDir Path tempDir) throws IOException {
    String location = tempDir.resolve("index-file-1.puffin").toString();
    // an index payload is many entries over few data files, which repeats long runs of the same
    // bytes, so it compresses about as well as the column the index was built from
    byte[] payload = new byte[1 << 20];
    for (int i = 0; i < payload.length; i++) {
      payload[i] = (byte) (i % 97);
    }
    IndexBlob blob = IndexBlob.of("iceberg-vector-entries-v2", payload);

    container.write(localOutput(location), List.of(blob));

    assertThat(container.read(localInput(location))).containsExactly(blob);
    assertThat(java.nio.file.Files.size(java.nio.file.Paths.get(location)))
        .isLessThan(payload.length / 2);
  }

  @Test
  public void canBeToldToWriteBlobsUncompressed(@TempDir Path tempDir) throws IOException {
    String location = tempDir.resolve("index-file-1.puffin").toString();
    byte[] payload = new byte[1 << 20];
    for (int i = 0; i < payload.length; i++) {
      payload[i] = (byte) (i % 97);
    }
    IndexBlob blob = IndexBlob.of("iceberg-vector-entries-v2", payload);

    System.setProperty(PuffinIndexFileContainer.INDEX_BLOB_COMPRESSION_PROPERTY, "none");
    try {
      container.write(localOutput(location), List.of(blob));
      assertThat(container.read(localInput(location))).containsExactly(blob);
      assertThat(java.nio.file.Files.size(java.nio.file.Paths.get(location)))
          .isGreaterThan(payload.length);
    } finally {
      System.clearProperty(PuffinIndexFileContainer.INDEX_BLOB_COMPRESSION_PROPERTY);
    }
  }
}
