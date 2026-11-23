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
package org.apache.iceberg.parquet;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.inmemory.InMemoryInputFile;
import org.apache.iceberg.io.InputFile;
import org.junit.jupiter.api.Test;

/** The read builder reports whether it returns exactly the positions a caller asks for. */
class TestParquetFormatModel {

  @Test
  void aScalarReaderReturnsTheRowsAtTheRequestedPositions() {
    ParquetFormatModel<Object, Void, ParquetValueReader<?>> model =
        ParquetFormatModel.create(
            Object.class,
            Void.class,
            null,
            (schema, fileSchema, engineSchema, idToConstant) -> null);

    assertThat(model.readBuilder(inputFile()).supportsPositions()).isTrue();
  }

  @Test
  void aBatchReaderDoesNotReturnTheRowsAtTheRequestedPositions() {
    // the batch reader hands back row batches: positions reach the scalar reader only, so a batch
    // builder must not promise rows the caller would then number by position
    ParquetFormatModel<Object, Void, VectorizedReader<?>> model =
        ParquetFormatModel.create(
            Object.class, Void.class, (schema, fileSchema, engineSchema, idToConstant) -> null);

    assertThat(model.readBuilder(inputFile()).supportsPositions()).isFalse();
  }

  private static InputFile inputFile() {
    return new InMemoryInputFile(new byte[0]);
  }
}
