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

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.formats.ReadBuilder;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Reproduces the reader lifecycle Iceberg drives: open a reader, consume it, then close the
 * iterator explicitly (Spark's BaseReader.close does exactly that). The round-trip suite passes
 * without closing the iterator, which hides this path.
 */
class TestLanceReaderClose {

  @TempDir Path tempDir;

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "name", Types.StringType.get()));

  private LanceFormatModel<Record, Void> model() {
    return LanceFormatModel.create(
        Record.class,
        Void.class,
        (icebergSchema, arrowSchema, engineSchema) ->
            GenericLanceWriter.create(icebergSchema, arrowSchema),
        (icebergSchema, arrowSchema, engineSchema, idToConstant) ->
            GenericLanceReader.buildReader(icebergSchema, arrowSchema, idToConstant));
  }

  private void write(LanceFormatModel<Record, Void> model, File file, int rows) throws IOException {
    EncryptedOutputFile out = EncryptedFiles.plainAsEncryptedOutput(Files.localOutput(file));
    try (FileAppender<Record> appender =
        model.writeBuilder(out).schema(SCHEMA).content(FileContent.DATA).build()) {
      for (int i = 0; i < rows; i++) {
        GenericRecord record = GenericRecord.create(SCHEMA);
        record.setField("id", i);
        record.setField("name", "n" + i);
        appender.add(record);
      }
    }
  }

  private int readAndClose(LanceFormatModel<Record, Void> model, File file) throws IOException {
    InputFile input = Files.localInput(file);
    ReadBuilder<Record, Void> builder = model.readBuilder(input).project(SCHEMA);
    int count = 0;
    try (CloseableIterable<Record> iterable = builder.build();
        CloseableIterator<Record> it = iterable.iterator()) {
      while (it.hasNext()) {
        it.next();
        count++;
      }
    }
    return count;
  }

  @Test
  void closesTheReaderAfterAFullScan() throws IOException {
    LanceFormatModel<Record, Void> model = model();
    File file = tempDir.resolve("close_one.lance").toFile();
    write(model, file, 100);

    assertThat(readAndClose(model, file)).isEqualTo(100);
  }

  @Test
  void readsTwoFilesInARow() throws IOException {
    LanceFormatModel<Record, Void> model = model();
    File first = tempDir.resolve("close_a.lance").toFile();
    File second = tempDir.resolve("close_b.lance").toFile();
    write(model, first, 10);
    write(model, second, 20);

    assertThat(readAndClose(model, first)).isEqualTo(10);
    assertThat(readAndClose(model, second)).isEqualTo(20);
    assertThat(readAndClose(model, first)).isEqualTo(10);
  }

  @Test
  void closesAReaderThatWasOnlyPartiallyConsumed() throws IOException {
    LanceFormatModel<Record, Void> model = model();
    File file = tempDir.resolve("close_partial.lance").toFile();
    write(model, file, 100);

    List<Record> firstRows = Lists.newArrayList();
    try (CloseableIterable<Record> iterable =
            model.readBuilder(Files.localInput(file)).project(SCHEMA).build();
        CloseableIterator<Record> it = iterable.iterator()) {
      for (int i = 0; i < 5 && it.hasNext(); i++) {
        firstRows.add(it.next());
      }
    }
    assertThat(firstRows).hasSize(5);
  }
}
