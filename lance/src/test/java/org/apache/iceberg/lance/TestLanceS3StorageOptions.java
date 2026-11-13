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
package org.apache.iceberg.lance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.lance.file.LanceFileReader;
import org.lance.file.LanceFileWriter;

/**
 * Proves the translated properties reach Lance and are used, against a local MinIO.
 *
 * <p>Runs only when a MinIO answers on {@value #MINIO_PORT} with the credentials below; otherwise
 * it is skipped. To reproduce:
 *
 * <pre>
 * docker run -d --rm --name lance-minio -p 19000:9000 \
 *   -e MINIO_ROOT_USER=lance3043 -e MINIO_ROOT_PASSWORD=lance3043pw \
 *   minio/minio server /data
 * # then create the bucket "lance3043"
 * </pre>
 */
class TestLanceS3StorageOptions {

  private static final int MINIO_PORT = 19000;
  private static final String ENDPOINT = "http://127.0.0.1:" + MINIO_PORT;
  private static final String ACCESS_KEY = "lance3043";
  private static final String SECRET_KEY = "lance3043pw";
  private static final String BUCKET = "lance3043";
  private static final int ROWS = 64;

  private static final Schema SCHEMA =
      new Schema(
          Lists.newArrayList(
              Field.nullable("id", new ArrowType.Int(64, true)),
              Field.nullable("payload", new ArrowType.Int(64, true))));

  /** Iceberg's spelling of the same settings, as a user would put them in a catalog or table. */
  private static final Map<String, String> ICEBERG_PROPERTIES =
      ImmutableMap.<String, String>builder()
          .put("s3.endpoint", ENDPOINT)
          .put("s3.access-key-id", ACCESS_KEY)
          .put("s3.secret-access-key", SECRET_KEY)
          .put("s3.region", "us-east-1")
          .put("s3.path-style-access", "true")
          .put("fs.s3a.connection.ssl.enabled", "false")
          .build();

  /** The Lance-specific escape hatch, used for knobs Iceberg has no name for. */
  private static final Map<String, String> LANCE_PROPERTIES =
      ImmutableMap.<String, String>builder()
          .put("lance.storage.endpoint", ENDPOINT)
          .put("lance.storage.access_key_id", ACCESS_KEY)
          .put("lance.storage.secret_access_key", SECRET_KEY)
          .put("lance.storage.region", "us-east-1")
          .put("lance.storage.virtual_hosted_style_request", "false")
          .put("lance.storage.allow_http", "true")
          .build();

  @Test
  void writesAndReadsWithIcebergS3Properties() throws Exception {
    Assumptions.assumeTrue(minioIsUp(), "no MinIO on port " + MINIO_PORT);
    String path = "s3://" + BUCKET + "/test/from-iceberg-properties.lance";

    writeFile(path, LanceStorageOptions.from(ICEBERG_PROPERTIES));
    assertThat(readRowCount(path, LanceStorageOptions.from(ICEBERG_PROPERTIES))).isEqualTo(ROWS);
  }

  @Test
  void writesAndReadsWithTheLanceEscapeHatch() throws Exception {
    Assumptions.assumeTrue(minioIsUp(), "no MinIO on port " + MINIO_PORT);
    String path = "s3://" + BUCKET + "/test/from-lance-properties.lance";

    writeFile(path, LanceStorageOptions.from(LANCE_PROPERTIES));
    assertThat(readRowCount(path, LanceStorageOptions.from(LANCE_PROPERTIES))).isEqualTo(ROWS);
  }

  @Test
  void theEndpointIsUsedRatherThanIgnored() {
    Assumptions.assumeTrue(minioIsUp(), "no MinIO on port " + MINIO_PORT);
    // an endpoint that only exists in this test: if Lance reads the key, the failure names it
    Map<String, String> options =
        LanceStorageOptions.from(
            ImmutableMap.of(
                "s3.endpoint", "http://no-such-endpoint.invalid:9",
                "s3.access-key-id", ACCESS_KEY,
                "s3.secret-access-key", SECRET_KEY,
                "s3.path-style-access", "true",
                "fs.s3a.connection.ssl.enabled", "false"));

    assertThatThrownBy(() -> writeFile("s3://" + BUCKET + "/test/unreachable.lance", options))
        .isInstanceOf(Exception.class)
        .hasMessageContaining("no-such-endpoint.invalid");
  }

  private static void writeFile(String path, Map<String, String> storageOptions) throws Exception {
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        LanceFileWriter writer = LanceFileWriter.open(path, allocator, null, storageOptions);
        VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, allocator)) {
      root.allocateNew();
      BigIntVector id = (BigIntVector) root.getVector("id");
      BigIntVector payload = (BigIntVector) root.getVector("payload");
      for (int row = 0; row < ROWS; row++) {
        id.setSafe(row, row);
        payload.setSafe(row, row * 10L);
      }

      root.setRowCount(ROWS);
      writer.write(root);
    }
  }

  private static long readRowCount(String path, Map<String, String> storageOptions)
      throws Exception {
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        LanceFileReader reader = LanceFileReader.open(path, storageOptions, allocator);
        ArrowReader batches = reader.readAll(null, null, 32)) {
      long rows = 0;
      while (batches.loadNextBatch()) {
        rows += batches.getVectorSchemaRoot().getRowCount();
      }

      return rows;
    }
  }

  private static boolean minioIsUp() {
    try (Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("127.0.0.1", MINIO_PORT), 500);
      return true;
    } catch (IOException e) {
      return false;
    }
  }
}
