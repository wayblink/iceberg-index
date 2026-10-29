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

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * A count read out of a payload bounds what may be allocated from it.
 *
 * <p>Every allocation a kernel makes is sized by a number in the payload, and those numbers are
 * four bytes wide: a corrupted or truncated file that claims two billion entries used to be
 * believed, and the executor died of an {@code OutOfMemoryError} that the scheduler reports as "an
 * executor exited caused by one of the running tasks". Each count is checked against the bytes that
 * are actually present before anything is allocated from it.
 */
class TestPayloadAllocationBounds {
  private static final int DIMENSION = 4;
  // the writers are versioned, and a version change has to fail here rather than silently build a
  // payload the reader would refuse
  private static final int FORMAT_VERSION = 1;

  @BeforeAll
  static void registerVectorIndexType() {
    VectorIndexTypes.register();
  }

  @Test
  void refusesAnHnswGraphLongerThanThePayload() {
    // the header is valid and the four bytes after it claim a two gigabyte graph
    byte[] payload =
        raw(
            out -> {
              VectorPayloads.writeHeader(out, HnswlibKernel.NAME, FORMAT_VERSION);
              out.writeInt(Integer.MAX_VALUE);
            });

    assertThatThrownBy(
            () ->
                kernel(VectorIndexDefinition.VectorStructure.HNSW)
                    .prepare(ByteBuffer.wrap(payload)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("ends inside the graph");
  }

  @Test
  void refusesAnHnswMappingCountLongerThanThePayload() {
    // an empty graph and no data files, then a mapping count that cannot fit in the remaining bytes
    byte[] payload =
        raw(
            out -> {
              VectorPayloads.writeHeader(out, HnswlibKernel.NAME, FORMAT_VERSION);
              out.writeInt(0);
              out.writeInt(0);
              out.writeInt(Integer.MAX_VALUE);
            });

    assertThatThrownBy(
            () ->
                kernel(VectorIndexDefinition.VectorStructure.HNSW)
                    .prepare(ByteBuffer.wrap(payload)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("mapping count");
  }

  @Test
  void refusesMoreEntriesThanThePayloadHolds() {
    // one data file, then an entry count that would allocate two billion slots for a payload that
    // ends right here
    byte[] payload =
        raw(
            out -> {
              VectorPayloads.writeHeader(out, BruteforceKernel.NAME, FORMAT_VERSION);
              out.writeInt(1);
              out.writeUTF("data-0.parquet");
              out.writeInt(Integer.MAX_VALUE);
            });

    assertThatThrownBy(
            () ->
                kernel(VectorIndexDefinition.VectorStructure.FLAT)
                    .entries(ByteBuffer.wrap(payload)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("entry count");
  }

  @Test
  void refusesAListCountWhoseStructuresWouldNotFit() {
    // a list count is four bytes wide, so a payload can name more lists than the structures they
    // would allocate; the check is arithmetic, so a test can ask it without the two gigabyte
    // payload
    IvfKernel kernel = (IvfKernel) kernel(VectorIndexDefinition.VectorStructure.IVF);
    long maxLists = IvfKernel.MAX_STRUCTURE_BYTES / kernel.structureBytesPerList();

    assertThatCode(() -> kernel.checkStructureFits(1000)).doesNotThrowAnyException();
    assertThatThrownBy(() -> kernel.checkStructureFits((int) (maxLists + 1)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("structures");
  }

  private static VectorKernel kernel(VectorIndexDefinition.VectorStructure structure) {
    return VectorKernels.forDefinition(
        VectorIndexDefinition.builder()
            .fieldId(1)
            .dimension(DIMENSION)
            .metric(VectorIndexDefinition.Metric.COSINE)
            .structure(structure)
            .build());
  }

  private interface Body {
    void write(DataOutputStream out) throws IOException;
  }

  private static byte[] raw(Body body) {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(bytes)) {
      body.write(out);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }

    return bytes.toByteArray();
  }
}
