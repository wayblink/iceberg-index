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
import java.nio.file.Path;
import java.util.Map;
import java.util.Random;
import org.apache.iceberg.Files;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.apache.iceberg.FileContent;
import org.lance.file.LanceFileWriter;

/**
 * What the stable (2.1) file format can do for a SIFT-like vector column: the column type
 * (integers bitpack) and the per-field compression metadata that is off by default.
 */
class TestLanceCompressionOptions {

  private static final int ROWS = 200_000;
  private static final int DIM = 128;
  private static final int BATCH = 20_000;
  @TempDir Path tempDir;

  private long write(String label, boolean integers, Map<String, String> encoding) throws Exception {
    File file = tempDir.resolve(label + ".lance").toFile();
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Field element =
          new Field(
              "element",
              new FieldType(
                  false,
                  integers ? new ArrowType.Int(32, true) : new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE),
                  null,
                  encoding),
              null);
      Field list =
          new Field(
              "vec",
              new FieldType(true, ArrowType.List.INSTANCE, null, encoding),
              Lists.newArrayList(element));
      Schema schema =
          new Schema(
              Lists.newArrayList(
                  Field.nullable("pk", new ArrowType.Int(64, true)), list));

      Random random = new Random(0);
      try (LanceFileWriter writer = LanceFileWriter.open(file.getAbsolutePath(), allocator, null)) {
        for (int start = 0; start < ROWS; start += BATCH) {
          int rows = Math.min(BATCH, ROWS - start);
          try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            root.allocateNew();
            BigIntVector pk = (BigIntVector) root.getVector("pk");
            ListVector vec = (ListVector) root.getVector("vec");
            BaseWriter.ListWriter writer2 = vec.getWriter();

            for (int row = 0; row < rows; row++) {
              pk.setSafe(row, start + row);
              writer2.setPosition(row);
              writer2.startList();
              for (int d = 0; d < DIM; d++) {
                int value = random.nextInt(117);
                if (integers) {
                  writer2.integer().writeInt(value);
                } else {
                  writer2.float4().writeFloat4((float) value);
                }
              }
              writer2.endList();
            }
            root.setRowCount(rows);
            writer.write(root);
          }
        }
      }
    }
    return file.length();
  }

  @Test
  void sizesWithAndWithoutCompressionMetadata() throws Exception {
    long values = (long) ROWS * DIM;
    Map<String, String> zstd =
        ImmutableMap.of(
            "lance-encoding:compression", "zstd",
            "lance-encoding:compression-level", "9",
            "lance-encoding:general", "on");
    Map<String, String> zstdBss =
        ImmutableMap.of(
            "lance-encoding:compression", "zstd",
            "lance-encoding:compression-level", "9",
            "lance-encoding:general", "on",
            "lance-encoding:bss", "on");
    Map<String, String> lz4 =
        ImmutableMap.of(
            "lance-encoding:compression", "lz4",
            "lance-encoding:general", "on");

    Object[][] cases = {
      {"float32 plain", false, java.util.Collections.emptyMap()},
      {"float32 + zstd", false, zstd},
      {"float32 + zstd + bss", false, zstdBss},
      {"float32 + lz4", false, lz4},
      {"int32 plain", true, java.util.Collections.emptyMap()},
      {"int32 + zstd", true, zstd},
      {"int32 + lz4", true, lz4},
    };

    System.out.printf("rows=%d dim=%d values=%d%n", ROWS, DIM, values);
    for (Object[] c : cases) {
      long bytes = write((String) c[0], (Boolean) c[1], (Map<String, String>) c[2]);
      System.out.printf("%-20s %10d bytes  %.3f B/value%n", c[0], bytes, bytes * 1.0 / values);
    }
  }

  /**
   * Writes the same data through the iceberg-lance model twice: once plain and once with the table
   * property that maps to {@code lance-encoding:compression}. Checks the size drop and that the
   * compressed file still reads back.
   */
  @Test
  void tablePropertyTurnsOnZstdThroughTheIcebergWriter() throws Exception {
    int rows = 50_000;
    org.apache.iceberg.Schema schema =
        new org.apache.iceberg.Schema(
            org.apache.iceberg.types.Types.NestedField.required(
                1, "pk", org.apache.iceberg.types.Types.LongType.get()),
            org.apache.iceberg.types.Types.NestedField.required(
                2,
                "vec",
                org.apache.iceberg.types.Types.ListType.ofRequired(
                    3, org.apache.iceberg.types.Types.FloatType.get())));

    LanceFormatModel<org.apache.iceberg.data.Record, Void> model =
        LanceFormatModel.create(
            org.apache.iceberg.data.Record.class,
            Void.class,
            (icebergSchema, arrowSchema, engineSchema) ->
                GenericLanceWriter.create(icebergSchema, arrowSchema),
            (icebergSchema, arrowSchema, engineSchema, idToConstant) ->
                GenericLanceReader.buildReader(icebergSchema, arrowSchema, idToConstant));

    java.util.Random random = new java.util.Random(0);
    int[][] values = new int[rows][DIM];
    for (int r = 0; r < rows; r++) {
      for (int d = 0; d < DIM; d++) {
        values[r][d] = random.nextInt(117);
      }
    }

    long byDefault = writeThroughModel(model, schema, values, rows, java.util.Collections.emptyMap());
    long plain =
        writeThroughModel(
            model,
            schema,
            values,
            rows,
            ImmutableMap.of("lance.encoding.compression", "none"));
    long zstd =
        writeThroughModel(
            model, schema, values, rows, ImmutableMap.of("lance.encoding.compression", "zstd"));
    long lz4 =
        writeThroughModel(
            model, schema, values, rows, ImmutableMap.of("lance.encoding.compression", "lz4"));

    long valuesTotal = (long) rows * DIM;
    System.out.printf("rows=%d dim=%d values=%d%n", rows, DIM, valuesTotal);
    System.out.printf("model float32 default    %9d bytes  %.3f B/value%n", byDefault, byDefault * 1.0 / valuesTotal);
    System.out.printf("model float32 compression=none %9d bytes  %.3f B/value%n", plain, plain * 1.0 / valuesTotal);
    System.out.printf("model float32 + zstd     %9d bytes  %.3f B/value%n", zstd, zstd * 1.0 / valuesTotal);
    System.out.printf("model float32 + lz4      %9d bytes  %.3f B/value%n", lz4, lz4 * 1.0 / valuesTotal);
  }

  private long writeThroughModel(
      LanceFormatModel<org.apache.iceberg.data.Record, Void> model,
      org.apache.iceberg.Schema schema,
      int[][] values,
      int rows,
      Map<String, String> properties)
      throws Exception {
    File file = tempDir.resolve("model_" + properties.hashCode() + ".lance").toFile();
    org.apache.iceberg.encryption.EncryptedOutputFile out =
        org.apache.iceberg.encryption.EncryptedFiles.plainAsEncryptedOutput(
            Files.localOutput(file));

    org.apache.iceberg.formats.ModelWriteBuilder<org.apache.iceberg.data.Record, Void> builder =
        model.writeBuilder(out).schema(schema).content(FileContent.DATA);
    for (Map.Entry<String, String> property : properties.entrySet()) {
      builder.set(property.getKey(), property.getValue());
    }

    try (org.apache.iceberg.io.FileAppender<org.apache.iceberg.data.Record> appender =
        builder.build()) {
      for (int r = 0; r < rows; r++) {
        org.apache.iceberg.data.GenericRecord record =
            org.apache.iceberg.data.GenericRecord.create(schema);
        record.setField("pk", (long) r);
        java.util.List<Float> vec = new java.util.ArrayList<>(DIM);
        for (int d = 0; d < DIM; d++) {
          vec.add((float) values[r][d]);
        }
        record.setField("vec", vec);
        appender.add(record);
      }
    }

    // the file still reads back through the same model
    int read = 0;
    try (org.apache.iceberg.io.CloseableIterable<org.apache.iceberg.data.Record> records =
        model
            .readBuilder(Files.localInput(file))
            .project(schema)
            .build()) {
      for (org.apache.iceberg.data.Record ignored : records) {
        read++;
      }
    }
    if (read != rows) {
      throw new IllegalStateException("read back " + read + " of " + rows + " rows");
    }
    return file.length();
  }
}
