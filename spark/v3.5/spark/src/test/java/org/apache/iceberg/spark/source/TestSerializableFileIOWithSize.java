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
package org.apache.iceberg.spark.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

class TestSerializableFileIOWithSize {

  @Test
  void newInputFileWithLength() {
    FileIO mockFileIO = mock(FileIO.class);
    FileIO serializableFileIO = SerializableFileIOWithSize.wrap(mockFileIO);
    String path = "gs://bucket/path/to/file.parquet";
    long length = 1024L;

    serializableFileIO.newInputFile(path, length);

    verify(mockFileIO).newInputFile(path, length);
  }

  @Test
  void newInputFileWithoutLength() {
    FileIO mockFileIO = mock(FileIO.class);
    FileIO serializableFileIO = SerializableFileIOWithSize.wrap(mockFileIO);
    String path = "gs://bucket/path/to/file.parquet";

    serializableFileIO.newInputFile(path);

    verify(mockFileIO).newInputFile(path);
  }

  @Test
  void carriesTheEnginesStorageSettingsToTheExecutors() {
    FileIO mockFileIO = mock(FileIO.class);
    when(mockFileIO.properties()).thenReturn(ImmutableMap.of("fs.s3a.access.key", "catalog-key"));

    FileIO serializableFileIO =
        SerializableFileIOWithSize.wrap(
            mockFileIO,
            ImmutableMap.of(
                "fs.s3a.endpoint", "http://oss.internal:8080",
                "fs.s3a.access.key", "conf-key"));

    // a format that opens files itself reads the map, so the Hadoop configuration's settings have
    // to be in it; the FileIO's own settings are more specific and win where they overlap
    assertThat(serializableFileIO.properties())
        .containsEntry("fs.s3a.endpoint", "http://oss.internal:8080")
        .containsEntry("fs.s3a.access.key", "catalog-key");
  }

  @Test
  void carriesItsSettingsForAFileIoThatExposesNone() {
    FileIO mockFileIO = mock(FileIO.class);
    when(mockFileIO.properties())
        .thenThrow(new UnsupportedOperationException("this FileIO does not expose its config"));

    FileIO serializableFileIO =
        SerializableFileIOWithSize.wrap(
            mockFileIO, ImmutableMap.of("fs.s3a.endpoint", "http://oss.internal:8080"));

    assertThat(serializableFileIO.properties())
        .containsEntry("fs.s3a.endpoint", "http://oss.internal:8080");
  }
}
