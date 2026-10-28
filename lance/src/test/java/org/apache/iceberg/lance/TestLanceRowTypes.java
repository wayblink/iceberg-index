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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/**
 * Pins what a Lance data file carries through the row path, which is the release boundary of the
 * format: a schema that asks for more has to fail when the reader or writer is built, naming the
 * columns, rather than on the first row that holds a value.
 */
class TestLanceRowTypes {

  private static final Schema FLAT =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "name", Types.StringType.get()),
          Types.NestedField.optional(
              3, "tags", Types.ListType.ofOptional(4, Types.StringType.get())));

  private static final Types.StructType PAYLOAD =
      Types.StructType.of(
          Types.NestedField.optional(11, "a", Types.IntegerType.get()),
          Types.NestedField.optional(12, "b", Types.StringType.get()));

  @Test
  void aFlatSchemaIsCarried() {
    LanceRowTypes.checkWritable(FLAT);
    LanceRowTypes.checkReadable(FLAT);
  }

  @Test
  void aStructColumnIsRejectedWhenTheWriterIsBuilt() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(10, "payload", PAYLOAD));

    // the column is named, because a table with many columns has to say which one is the reason
    assertThatThrownBy(() -> LanceRowTypes.checkWritable(schema))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("cannot be written")
        .hasMessageContaining("payload")
        .hasMessageContaining("struct")
        .hasMessageContaining("write.format.default");
  }

  @Test
  void aMapColumnIsRejectedWhenTheReaderIsBuilt() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(
                20,
                "attributes",
                Types.MapType.ofOptional(21, 22, Types.StringType.get(), Types.StringType.get())));

    assertThatThrownBy(() -> LanceRowTypes.checkReadable(schema))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("cannot be read back")
        .hasMessageContaining("attributes")
        .hasMessageContaining("map");
  }

  @Test
  void aListOfStructsIsRejected() {
    // the element decides: a list is carried only when what it holds can be
    Schema schema =
        new Schema(
            Types.NestedField.optional(30, "events", Types.ListType.ofOptional(31, PAYLOAD)));

    assertThatThrownBy(() -> LanceRowTypes.checkWritable(schema))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("events");
  }

  @Test
  void aMetadataColumnIsNotAColumnOfTheFile() {
    // a reader's schema carries the partition column, which is a struct holding nothing: its value
    // comes from the reader's constants, so it is not a reason to refuse the file
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(
                MetadataColumns.PARTITION_COLUMN_ID, "_partition", Types.StructType.of()));

    LanceRowTypes.checkReadable(schema);
    LanceRowTypes.checkWritable(schema);
  }

  @Test
  void aColumnNameWithADotIsRefusedByName() {
    // a Lance field name cannot contain a dot: the file format reads it as a nested path. Writing
    // one fails inside the native writer as a close error, so the boundary says so up front.
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "a.b", Types.StringType.get()));

    assertThatThrownBy(() -> LanceRowTypes.checkWritable(schema))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("a.b")
        .hasMessageContaining("dot")
        .hasMessageContaining("write.format.default");
    assertThatThrownBy(() -> LanceRowTypes.checkReadable(schema))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("a.b");
  }

  @Test
  void everyColumnThatCannotPassIsNamedAtOnce() {
    // one build fails with the whole answer, rather than one column per attempt
    Schema schema =
        new Schema(
            Types.NestedField.optional(10, "payload", PAYLOAD),
            Types.NestedField.optional(
                20,
                "attributes",
                Types.MapType.ofOptional(21, 22, Types.StringType.get(), Types.StringType.get())),
            Types.NestedField.required(1, "id", Types.LongType.get()));

    assertThatThrownBy(() -> LanceRowTypes.checkWritable(schema))
        .hasMessageContaining("payload")
        .hasMessageContaining("attributes")
        .hasMessageNotContaining("id (long)");
  }
}
