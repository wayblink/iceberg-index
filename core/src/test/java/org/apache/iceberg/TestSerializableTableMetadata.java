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
package org.apache.iceberg;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestSerializableTableMetadata {
  private static final Schema SCHEMA = new Schema(required(1, "id", Types.IntegerType.get(), "id"));
  private static final String TABLE_LOCATION = "mem://table";
  private static final String METADATA_LOCATION = "mem://table/metadata/v1.metadata.json";

  @Test
  public void copyOfLoadedMetadataDoesNotReadTheMetadataFile() {
    InMemoryFileIO io = new InMemoryFileIO();
    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            SCHEMA, PartitionSpec.unpartitioned(), TABLE_LOCATION, ImmutableMap.of());
    TableMetadataParser.write(metadata, io.newOutputFile(METADATA_LOCATION));
    TableMetadata loaded = TableMetadataParser.read(io, METADATA_LOCATION);
    Table table = new BaseTable(new StaticTableOperations(loaded, io), "t");

    // the metadata file is gone, so anything that reads it now fails
    io.deleteFile(METADATA_LOCATION);

    Table withMetadata = SerializableTable.copyOf(table, loaded);
    assertThat(withMetadata.currentSnapshot()).isNull();
    assertThat(withMetadata.snapshot(1L)).isNull();

    // the old copy has only the location, so it reads the file and fails: this is the read that
    // the overload above removes, and the one this test would not otherwise notice
    assertThatThrownBy(() -> SerializableTable.copyOf(table).currentSnapshot())
        .isInstanceOf(NotFoundException.class);
  }
}
