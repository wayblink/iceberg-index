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

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Sizes of the same SIFT-like values stored as float, int and tinyint list columns. */
class TestLanceEncodingSizes {

  private static final int ROWS = 200_000;
  private static final int DIM = 128;
  @TempDir Path tempDir;

  private LanceFormatModel<Record, Void> model() {
    return LanceFormatModel.create(
        Record.class,
        Void.class,
        (icebergSchema, arrowSchema, engineSchema) ->
            GenericLanceWriter.create(icebergSchema, arrowSchema),
        (icebergSchema, arrowSchema, engineSchema, idToConstant) ->
            GenericLanceReader.buildReader(icebergSchema, arrowSchema, idToConstant));
  }

  private long write(Types.NestedField vectorField, java.util.function.IntFunction<Object> vectorOf)
      throws IOException {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "pk", Types.LongType.get()), vectorField);
    LanceFormatModel<Record, Void> model = model();
    File file = tempDir.resolve(vectorField.name() + "." + vectorField.type() + ".lance").toFile();
    EncryptedOutputFile out = EncryptedFiles.plainAsEncryptedOutput(Files.localOutput(file));

    Random random = new Random(0);
    try (FileAppender<Record> appender =
        model.writeBuilder(out).schema(schema).content(FileContent.DATA).build()) {
      for (int row = 0; row < ROWS; row++) {
        GenericRecord record = GenericRecord.create(schema);
        record.setField("pk", (long) row);
        record.setField(vectorField.name(), vectorOf.apply(row));
        appender.add(record);
      }
    }
    return file.length();
  }

  @Test
  void sizesOfTheSameValuesInDifferentTypes() throws IOException {
    int[][] values = new int[ROWS][DIM];
    Random random = new Random(0);
    for (int row = 0; row < ROWS; row++) {
      for (int d = 0; d < DIM; d++) {
        values[row][d] = random.nextInt(117); // SIFT-like 0..116
      }
    }

    long float32 =
        write(
            Types.NestedField.required(
                2, "vec", Types.ListType.ofRequired(3, Types.FloatType.get())),
            row -> {
              List<Float> out = new java.util.ArrayList<>(DIM);
              for (int v : values[row]) {
                out.add((float) v);
              }
              return out;
            });

    long int32 =
        write(
            Types.NestedField.required(
                2, "vec", Types.ListType.ofRequired(3, Types.IntegerType.get())),
            row -> {
              List<Integer> out = new java.util.ArrayList<>(DIM);
              for (int v : values[row]) {
                out.add(v);
              }
              return out;
            });

    long valuesTotal = (long) ROWS * DIM;
    System.out.printf(
        "rows=%d dim=%d values=%d%n", ROWS, DIM, valuesTotal);
    System.out.printf(
        "float32 list: %d bytes  (%.3f B/value)%n", float32, float32 * 1.0 / valuesTotal);
    System.out.printf(
        "int32   list: %d bytes  (%.3f B/value)%n", int32, int32 * 1.0 / valuesTotal);
    System.out.printf("int vs float ratio: %.2fx%n", int32 * 1.0 / float32);
  }
}
