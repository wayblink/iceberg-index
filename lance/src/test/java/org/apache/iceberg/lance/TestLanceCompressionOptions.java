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
}
