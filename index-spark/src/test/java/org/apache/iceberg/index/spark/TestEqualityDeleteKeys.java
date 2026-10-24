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

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class TestEqualityDeleteKeys {

  @ParameterizedTest
  @MethodSource("nanoTypes")
  void rejectsNanosecondTimestampKeys(Types.TimestampNanoType type) {
    assertRejected(List.of(Types.NestedField.required(2, "event_time", type)));
  }

  @ParameterizedTest
  @MethodSource("nanoTypes")
  void rejectsNanosecondTimestampAfterOtherKeys(Types.TimestampNanoType type) {
    assertRejected(
        List.of(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "event_time", type),
            Types.NestedField.required(3, "category", Types.StringType.get())));
  }

  @ParameterizedTest
  @MethodSource("ordinaryTypes")
  void acceptsKeysWithoutNanosecondTimestamps(Type type) {
    assertThatCode(
            () ->
                IndexEntries.refuseNanosKeys(
                    List.of(Types.NestedField.required(1, "key", type)), deleteFiles()))
        .doesNotThrowAnyException();
  }

  @Test
  void acceptsEmptyKeys() {
    assertThatCode(() -> IndexEntries.refuseNanosKeys(List.of(), deleteFiles()))
        .doesNotThrowAnyException();
  }

  private static void assertRejected(List<Types.NestedField> keyFields) {
    assertThatThrownBy(() -> IndexEntries.refuseNanosKeys(keyFields, deleteFiles()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("nanosecond timestamp column event_time")
        .hasMessageContaining("a row is read as microseconds")
        .hasMessageContaining("query without the index")
        .hasMessageContaining("/data/equality-1.parquet")
        .hasMessageContaining("/data/equality-2.parquet");
  }

  private static List<Types.TimestampNanoType> nanoTypes() {
    return List.of(Types.TimestampNanoType.withZone(), Types.TimestampNanoType.withoutZone());
  }

  private static List<Type> ordinaryTypes() {
    return List.of(
        Types.IntegerType.get(),
        Types.LongType.get(),
        Types.StringType.get(),
        Types.DateType.get(),
        Types.TimeType.get(),
        Types.TimestampType.withZone(),
        Types.TimestampType.withoutZone(),
        Types.BinaryType.get());
  }

  private static List<DeleteFile> deleteFiles() {
    return List.of(deleteFile("/data/equality-1.parquet"), deleteFile("/data/equality-2.parquet"));
  }

  private static DeleteFile deleteFile(String path) {
    return FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
        .ofEqualityDeletes(2)
        .withPath(path)
        .withFormat(FileFormat.PARQUET)
        .withFileSizeInBytes(1)
        .withRecordCount(1)
        .build();
  }
}
