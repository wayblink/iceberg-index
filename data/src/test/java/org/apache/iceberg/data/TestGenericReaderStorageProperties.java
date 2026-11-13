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
package org.apache.iceberg.data;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.formats.FormatModel;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.formats.ModelWriteBuilder;
import org.apache.iceberg.formats.ReadBuilder;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A format that opens files itself (Lance, and anything else with its own object store) cannot read
 * the settings from {@code InputFile}. The reader has to hand them over, and this pins that it
 * does: the table's properties reach the format model's {@code ReadBuilder}.
 */
class TestGenericReaderStorageProperties {

  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

  private final Map<String, String> received = Maps.newLinkedHashMap();

  @TempDir private File tableDir;
  private Table table;

  @BeforeEach
  void createTable() {
    FormatModelRegistry.register(new RecordFormatModel(received));

    table =
        TestTables.create(tableDir, "storage_properties", SCHEMA, PartitionSpec.unpartitioned(), 2);
    table.updateProperties().set("s3.endpoint", "http://oss.internal:8080").commit();
    table
        .newAppend()
        .appendFile(
            DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath(new File(tableDir, "data.lance").getAbsolutePath())
                .withFormat(FileFormat.LANCE)
                .withFileSizeInBytes(1024)
                .withRecordCount(0)
                .build())
        .commit();
  }

  @Test
  void passesTheTablesStoragePropertiesToTheFormatModel() throws IOException {
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
      // the stub reader produces no rows; the read is only here to reach the ReadBuilder
      assertThat(records).isEmpty();
    }

    assertThat(received).containsEntry("s3.endpoint", "http://oss.internal:8080");
  }

  /** Registers a stub model for LANCE that records the configuration it is given. */
  private static class RecordFormatModel implements FormatModel<Record, Void> {
    private final Map<String, String> received;

    RecordFormatModel(Map<String, String> received) {
      this.received = received;
    }

    @Override
    public FileFormat format() {
      return FileFormat.LANCE;
    }

    @Override
    public Class<Record> type() {
      return Record.class;
    }

    @Override
    public Class<Void> schemaType() {
      return Void.class;
    }

    @Override
    public ModelWriteBuilder<Record, Void> writeBuilder(EncryptedOutputFile outputFile) {
      throw new UnsupportedOperationException("this model only reads");
    }

    @Override
    public ReadBuilder<Record, Void> readBuilder(InputFile inputFile) {
      return new RecordingReadBuilder(received);
    }
  }

  private static class RecordingReadBuilder implements ReadBuilder<Record, Void> {
    private final Map<String, String> received;

    RecordingReadBuilder(Map<String, String> received) {
      this.received = received;
    }

    @Override
    public ReadBuilder<Record, Void> split(long start, long length) {
      return this;
    }

    @Override
    public ReadBuilder<Record, Void> project(Schema schema) {
      return this;
    }

    @Override
    public ReadBuilder<Record, Void> engineProjection(Void schema) {
      return this;
    }

    @Override
    public ReadBuilder<Record, Void> caseSensitive(boolean caseSensitive) {
      return this;
    }

    @Override
    public ReadBuilder<Record, Void> filter(Expression filter) {
      return this;
    }

    @Override
    public ReadBuilder<Record, Void> set(String key, String value) {
      received.put(key, value);
      return this;
    }

    @Override
    public ReadBuilder<Record, Void> reuseContainers() {
      return this;
    }

    @Override
    public ReadBuilder<Record, Void> recordsPerBatch(int rowsPerBatch) {
      return this;
    }

    @Override
    public ReadBuilder<Record, Void> idToConstant(Map<Integer, ?> idToConstant) {
      return this;
    }

    @Override
    public ReadBuilder<Record, Void> withNameMapping(NameMapping nameMapping) {
      return this;
    }

    @Override
    public CloseableIterable<Record> build() {
      return CloseableIterable.withNoopClose(Collections.emptyList());
    }
  }
}
