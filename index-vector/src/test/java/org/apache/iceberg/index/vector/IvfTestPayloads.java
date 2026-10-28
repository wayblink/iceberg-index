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
package org.apache.iceberg.index.vector;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

/**
 * Writes IVF payloads the way a builder does, independently of the reader under test.
 *
 * <p>The points of this writer are that it is separate code from the parser and that it uses {@code
 * DataOutputStream} throughout: big-endian numbers and {@code writeUTF} paths, including paths with
 * a NUL, non-Latin characters, accents and supplementary characters. A reader that treats the paths
 * as ordinary UTF-8 or the numbers as little-endian agrees with this only by coincidence.
 */
final class IvfTestPayloads {

  static final String KERNEL = "ivf";
  static final int PLAIN_VERSION = 1;
  static final int QUANTIZED_VERSION = 2;
  static final int LEGACY_HEADER = 2;

  private IvfTestPayloads() {}

  /** A record of an unquantized list. */
  static final class Vector {
    final int dataFileIndex;
    final long rowPosition;
    final float[] values;

    Vector(int dataFileIndex, long rowPosition, float[] values) {
      this.dataFileIndex = dataFileIndex;
      this.rowPosition = rowPosition;
      this.values = values;
    }
  }

  /** A record of a quantized list. */
  static final class Code {
    final int dataFileIndex;
    final long rowPosition;
    final byte[] code;
    final float[] factors;

    Code(int dataFileIndex, long rowPosition, byte[] code, float[] factors) {
      this.dataFileIndex = dataFileIndex;
      this.rowPosition = rowPosition;
      this.code = code;
      this.factors = factors;
    }
  }

  static byte[] plain(
      int dimension,
      String[] dataFiles,
      float[][] centroids,
      long entryCount,
      List<List<Vector>> lists) {
    return plain(dimension, dataFiles, centroids, entryCount, lists, PLAIN_VERSION);
  }

  static byte[] plain(
      int dimension,
      String[] dataFiles,
      float[][] centroids,
      long entryCount,
      List<List<Vector>> lists,
      int version) {
    return write(
        out -> {
          header(out, version);
          out.writeInt(dimension);
          out.writeInt(centroids.length);
          writeFloats(out, centroids);
          writeDictionary(out, dataFiles);
          out.writeInt((int) entryCount);
          for (List<Vector> list : lists) {
            out.writeInt(list.size());
            for (Vector record : list) {
              out.writeInt(record.dataFileIndex);
              out.writeLong(record.rowPosition);
              writeFloats(out, record.values);
            }
          }
        });
  }

  static byte[] quantized(
      int dimension,
      int bits,
      long seed,
      String[] dataFiles,
      float[][] centroids,
      long entryCount,
      float[][] listCenters,
      List<List<Code>> lists) {
    return write(
        out -> {
          header(out, QUANTIZED_VERSION);
          out.writeInt(dimension);
          out.writeInt(bits);
          out.writeLong(seed);
          out.writeInt(centroids.length);
          writeFloats(out, centroids);
          writeDictionary(out, dataFiles);
          out.writeInt((int) entryCount);
          for (int list = 0; list < lists.size(); list++) {
            writeFloats(out, listCenters[list]);
            out.writeInt(lists.get(list).size());
            for (Code record : lists.get(list)) {
              out.writeInt(record.dataFileIndex);
              out.writeLong(record.rowPosition);
              out.write(record.code);
              writeFloats(out, record.factors);
            }
          }
        });
  }

  /** Writes a payload whose header carries no kernel name, as a build before this format did. */
  static byte[] legacy(
      int dimension,
      String[] dataFiles,
      float[][] centroids,
      long entryCount,
      List<List<Vector>> lists) {
    return write(
        out -> {
          out.writeInt(LEGACY_HEADER);
          out.writeInt(dimension);
          out.writeInt(centroids.length);
          writeFloats(out, centroids);
          writeDictionary(out, dataFiles);
          out.writeInt((int) entryCount);
          for (List<Vector> list : lists) {
            out.writeInt(list.size());
            for (Vector record : list) {
              out.writeInt(record.dataFileIndex);
              out.writeLong(record.rowPosition);
              writeFloats(out, record.values);
            }
          }
        });
  }

  static byte[] concatenate(byte[] payload, byte[] trailer) {
    byte[] joined = new byte[payload.length + trailer.length];
    System.arraycopy(payload, 0, joined, 0, payload.length);
    System.arraycopy(trailer, 0, joined, payload.length, trailer.length);
    return joined;
  }

  static byte[] truncated(byte[] payload, int length) {
    byte[] cut = new byte[length];
    System.arraycopy(payload, 0, cut, 0, length);
    return cut;
  }

  private static void header(DataOutputStream out, int version) throws IOException {
    out.writeInt(3);
    out.writeUTF(KERNEL);
    out.writeInt(version);
  }

  private static void writeDictionary(DataOutputStream out, String[] dataFiles) throws IOException {
    out.writeInt(dataFiles.length);
    for (String dataFile : dataFiles) {
      out.writeUTF(dataFile);
    }
  }

  private static void writeFloats(DataOutputStream out, float[][] rows) throws IOException {
    for (float[] row : rows) {
      writeFloats(out, row);
    }
  }

  private static void writeFloats(DataOutputStream out, float[] values) throws IOException {
    for (float value : values) {
      out.writeFloat(value);
    }
  }

  private interface Body {
    void write(DataOutputStream out) throws IOException;
  }

  private static byte[] write(Body body) {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(bytes)) {
      body.write(out);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to write the test payload", e);
    }

    return bytes.toByteArray();
  }
}
