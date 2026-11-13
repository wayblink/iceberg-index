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

import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

/**
 * Lance reads object-store settings from bare keys ({@code endpoint}, {@code access_key_id}), not
 * from the prefixed keys Iceberg and Hadoop use. A table property written the Iceberg way has to be
 * translated before it reaches {@code LanceFileReader}/{@code LanceFileWriter}, or it is silently
 * ignored.
 */
class TestLanceStorageOptions {

  @Test
  void stripsTheLanceStoragePrefix() {
    assertThat(
            LanceStorageOptions.from(
                ImmutableMap.of(
                    "lance.storage.endpoint", "http://oss.internal:8080",
                    "lance.storage.allow_http", "true")))
        .isEqualTo(ImmutableMap.of("endpoint", "http://oss.internal:8080", "allow_http", "true"));
  }

  @Test
  void mapsIcebergS3PropertiesToBareKeys() {
    assertThat(
            LanceStorageOptions.from(
                ImmutableMap.of(
                    "s3.endpoint", "http://oss.internal:8080",
                    "s3.access-key-id", "ak",
                    "s3.secret-access-key", "sk",
                    "s3.session-token", "token",
                    "s3.region", "cn-north-1")))
        .isEqualTo(
            ImmutableMap.of(
                "endpoint", "http://oss.internal:8080",
                "access_key_id", "ak",
                "secret_access_key", "sk",
                "session_token", "token",
                "region", "cn-north-1"));
  }

  @Test
  void mapsHadoopS3APropertiesToBareKeys() {
    assertThat(
            LanceStorageOptions.from(
                ImmutableMap.of(
                    "fs.s3a.endpoint", "http://oss.internal:8080",
                    "fs.s3a.access.key", "ak",
                    "fs.s3a.secret.key", "sk",
                    "fs.s3a.session.token", "token",
                    "fs.s3a.endpoint.region", "cn-north-1")))
        .isEqualTo(
            ImmutableMap.of(
                "endpoint", "http://oss.internal:8080",
                "access_key_id", "ak",
                "secret_access_key", "sk",
                "session_token", "token",
                "region", "cn-north-1"));
  }

  @Test
  void invertsPathStyleAccess() {
    // Iceberg/Hadoop say "path style = yes"; Lance asks for the opposite flag
    assertThat(
            LanceStorageOptions.from(
                ImmutableMap.of(
                    "s3.path-style-access", "true", "fs.s3a.path.style.access", "true")))
        .isEqualTo(ImmutableMap.of("virtual_hosted_style_request", "false"));

    assertThat(LanceStorageOptions.from(ImmutableMap.of("s3.path-style-access", "false")))
        .isEqualTo(ImmutableMap.of("virtual_hosted_style_request", "true"));
  }

  @Test
  void invertsSslEnabled() {
    // Hadoop says "ssl enabled"; Lance asks whether plain http is allowed
    assertThat(LanceStorageOptions.from(ImmutableMap.of("fs.s3a.connection.ssl.enabled", "false")))
        .isEqualTo(ImmutableMap.of("allow_http", "true"));

    assertThat(LanceStorageOptions.from(ImmutableMap.of("fs.s3a.connection.ssl.enabled", "true")))
        .isEqualTo(ImmutableMap.of("allow_http", "false"));
  }

  @Test
  void theLanceStoragePrefixWinsOverAnIcebergEquivalent() {
    assertThat(
            LanceStorageOptions.from(
                ImmutableMap.of(
                    "s3.endpoint", "http://from-s3-property:9000",
                    "lance.storage.endpoint", "http://explicit:8080")))
        .isEqualTo(ImmutableMap.of("endpoint", "http://explicit:8080"));
  }

  @Test
  void ignoresPropertiesThatAreNotStorageSettings() {
    Map<String, String> properties =
        ImmutableMap.of(
            "current-snapshot-id", "1",
            "write.parquet.compression-codec", "zstd",
            "lance.batch-size", "4096",
            "comment", "not a storage option");

    assertThat(LanceStorageOptions.from(properties)).isEmpty();
  }

  @Test
  void ignoresUnknownStoragePropertiesInsteadOfFailingTheRead() {
    // an unsupported knob must not break a table that already carries it
    assertThat(
            LanceStorageOptions.from(
                ImmutableMap.of(
                    "s3.endpoint", "http://oss.internal:8080",
                    "s3.some-future-knob", "1",
                    "gcs.project-id", "proj")))
        .isEqualTo(ImmutableMap.of("endpoint", "http://oss.internal:8080"));
  }

  @Test
  void ignoresAnUnparsablePathStyleValue() {
    assertThat(LanceStorageOptions.from(ImmutableMap.of("s3.path-style-access", "maybe")))
        .isEmpty();
  }
}
