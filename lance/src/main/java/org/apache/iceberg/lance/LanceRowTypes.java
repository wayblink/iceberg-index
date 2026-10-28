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

import java.util.List;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * The columns a Lance data file carries through the row path.
 *
 * <p>A Lance file is Arrow-based and its schema can hold nested types, but a row reaches it as a
 * map of column name to value: that conversion covers primitives and lists of primitives, and a
 * struct or a map has no such value. This is the release boundary of the format, so it is checked
 * when a reader or writer is built and reported once, with every column that cannot pass, rather
 * than failing on the first row that happens to hold a value or writing a file that cannot be read
 * back.
 */
public final class LanceRowTypes {

  private LanceRowTypes() {}

  /**
   * Fails when the schema holds a column a Lance data file cannot be written with.
   *
   * @throws UnsupportedOperationException naming every column that cannot be written
   */
  public static void checkWritable(Schema schema) {
    check(schema, "written");
  }

  /**
   * Fails when the schema holds a column a Lance data file cannot be read back into a row.
   *
   * @throws UnsupportedOperationException naming every column that cannot be read
   */
  public static void checkReadable(Schema schema) {
    check(schema, "read back");
  }

  private static void check(Schema schema, String what) {
    List<String> unsupported = Lists.newArrayList();
    for (Types.NestedField field : schema.columns()) {
      // a metadata column is not carried by the file: its value comes from the reader's constants,
      // and the partition column for one is a struct that holds nothing a file would store
      if (MetadataColumns.isMetadataColumn(field.fieldId())) {
        continue;
      }

      if (!carries(field.type())) {
        unsupported.add(field.name() + " (" + field.type() + ")");
      } else if (field.name().contains(".")) {
        // a file field name cannot contain a dot: the format reads it as a path, and the native
        // writer rejects the file only after the rows are buffered
        unsupported.add(field.name() + " (a field name cannot contain a dot)");
      }
    }

    if (unsupported.isEmpty()) {
      return;
    }

    throw new UnsupportedOperationException(
        "Lance data files carry only flat columns and lists of them in this release, and a field name"
            + " cannot contain a dot, so these columns cannot be "
            + what
            + ": "
            + String.join(", ", unsupported)
            + ". Write the table in another format, for example with 'write.format.default' = "
            + "'parquet', rename the column, or flatten it");
  }

  /** Whether one column can pass through the row path, which a list can only if its element can. */
  private static boolean carries(Type type) {
    if (type.isStructType() || type.isMapType()) {
      return false;
    }

    if (type.isListType()) {
      return carries(type.asListType().elementType());
    }

    // a primitive is a value in the map; the reader and writer name the ones they convert, and an
    // unknown one fails there with its own message
    return true;
  }
}
