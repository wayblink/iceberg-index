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
package org.apache.iceberg.io;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;

/**
 * Pins the order in which the four places that can hold object-store settings are consulted, and
 * what is allowed out of the Hadoop configuration.
 *
 * <p>The first three are resolved here; the process environment stays the object-store client's own
 * last resort and must never appear in the map, because the client reads it whether or not anything
 * was resolved.
 */
class TestStorageProperties {

  @Test
  void keepsOnlyStorageKeysFromTheHadoopConfiguration() {
    Map<String, String> conf = Maps.newLinkedHashMap();
    conf.put("fs.s3a.endpoint", "http://oss.internal:8080");
    conf.put("fs.s3a.access.key", "ak");
    conf.put("fs.defaultFS", "hdfs://namenode:8020");
    conf.put("spark.executor.memory", "4g");
    conf.put("lance.storage.region", "cn-north-1");

    assertThat(StorageProperties.fromConf(conf.entrySet()))
        .containsExactlyInAnyOrderEntriesOf(
            ImmutableMap.of(
                "fs.s3a.endpoint",
                "http://oss.internal:8080",
                "fs.s3a.access.key",
                "ak",
                "lance.storage.region",
                "cn-north-1"));
  }

  @Test
  void theTableWinsOverTheCatalogAndTheHadoopConfiguration() {
    Map<String, String> tableProperties =
        ImmutableMap.of("s3.endpoint", "http://table.example:9000");
    Map<String, String> conf =
        ImmutableMap.of(
            "fs.s3a.endpoint", "http://conf.example:9000", "fs.s3a.region", "conf-region");

    Map<String, String> resolved =
        StorageProperties.resolve(
            new FixedFileIO(ImmutableMap.of("s3.endpoint", "http://catalog.example:9000")),
            tableProperties,
            conf.entrySet());

    assertThat(resolved)
        .containsEntry("s3.endpoint", "http://table.example:9000")
        // the conf still contributes keys the more specific sources do not carry
        .containsEntry("fs.s3a.region", "conf-region");
  }

  @Test
  void theCatalogWinsOverTheHadoopConfiguration() {
    Map<String, String> resolved =
        StorageProperties.resolve(
            new FixedFileIO(ImmutableMap.of("s3.endpoint", "http://catalog.example:9000")),
            ImmutableMap.of(),
            ImmutableMap.of("fs.s3a.endpoint", "http://conf.example:9000").entrySet());

    assertThat(resolved).containsEntry("s3.endpoint", "http://catalog.example:9000");
  }

  @Test
  void aFileIoThatDoesNotExposeItsConfigurationIsNotFatal() {
    Map<String, String> resolved =
        StorageProperties.resolve(
            new HiddenFileIO(),
            ImmutableMap.of("s3.access-key-id", "ak"),
            ImmutableMap.of("fs.s3a.endpoint", "http://conf.example:9000").entrySet());

    assertThat(resolved)
        .containsEntry("fs.s3a.endpoint", "http://conf.example:9000")
        .containsEntry("s3.access-key-id", "ak");
  }

  @Test
  void aStorageKeyIsOneAnObjectStoreClientUnderstands() {
    assertThat(StorageProperties.isStorageProperty("s3.endpoint")).isTrue();
    assertThat(StorageProperties.isStorageProperty("fs.s3a.access.key")).isTrue();
    assertThat(StorageProperties.isStorageProperty("fs.s3.partition.size")).isTrue();
    assertThat(StorageProperties.isStorageProperty("fs.gs.project-id")).isTrue();
    assertThat(StorageProperties.isStorageProperty("fs.azure.account.key")).isTrue();
    assertThat(StorageProperties.isStorageProperty("lance.storage.endpoint")).isTrue();
    assertThat(StorageProperties.isStorageProperty("fs.defaultFS")).isFalse();
    assertThat(StorageProperties.isStorageProperty("spark.sql.shuffle.partitions")).isFalse();
    assertThat(StorageProperties.isStorageProperty("s3a.endpoint")).isFalse();
  }

  private static class FixedFileIO implements FileIO {
    private final Map<String, String> properties;

    FixedFileIO(Map<String, String> properties) {
      this.properties = properties;
    }

    @Override
    public Map<String, String> properties() {
      return properties;
    }

    @Override
    public InputFile newInputFile(String path) {
      throw new UnsupportedOperationException("not used");
    }

    @Override
    public OutputFile newOutputFile(String path) {
      throw new UnsupportedOperationException("not used");
    }

    @Override
    public void deleteFile(String path) {
      throw new UnsupportedOperationException("not used");
    }
  }

  private static class HiddenFileIO extends FixedFileIO {
    HiddenFileIO() {
      super(ImmutableMap.of());
    }

    @Override
    public Map<String, String> properties() {
      throw new UnsupportedOperationException("this FileIO does not expose its configuration");
    }
  }
}
