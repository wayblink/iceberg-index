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
package org.apache.iceberg.index.spark;

import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Row;

/**
 * A scalar filter of a vector query, asked of the rows the search proposes.
 *
 * <p>A search that already drew its candidates from the filtered rows never needs this, but a
 * search that could not asks it here instead: the rows that fail are dropped, and the caller asks
 * the index for more until the window is full again. Dropping them is the whole of the post-filter
 * link.
 *
 * <p>The filter is evaluated against the row as it is read, so the columns it names have to be part
 * of the read. A colum whose value the evaluator cannot compare, which this project's types leave
 * to a separate decision, is rejected when the filter is built rather than halfway through a query.
 */
final class IndexRowFilter {
  private final Evaluator evaluator;

  IndexRowFilter(Schema schema, Expression filter) {
    Preconditions.checkArgument(schema != null, "Schema is null");
    Preconditions.checkArgument(filter != null, "Filter is null");
    this.evaluator = new Evaluator(schema.asStruct(), filter);
  }

  /** Whether the row is part of the answer the unfiltered table would have given. */
  boolean matches(Row row) {
    return row != null && evaluator.eval(new RowStruct(row));
  }

  /** The struct a row is read as, by the position of each field. */
  private static final class RowStruct implements org.apache.iceberg.StructLike {
    private final Row row;

    private RowStruct(Row row) {
      this.row = row;
    }

    @Override
    public int size() {
      return row.size();
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      Object value = row.get(pos);
      if (value == null) {
        return null;
      }

      return javaClass.cast(convert(value, javaClass));
    }

    /**
     * Spark hands back a value of its own representation for some plain types, and the evaluator
     * compares against the one Iceberg uses.
     */
    private static Object convert(Object value, Class<?> javaClass) {
      if (javaClass == Integer.class) {
        return ((Number) value).intValue();
      }

      if (javaClass == Long.class) {
        return ((Number) value).longValue();
      }

      if (javaClass == Float.class) {
        return ((Number) value).floatValue();
      }

      if (javaClass == Double.class) {
        return ((Number) value).doubleValue();
      }

      return value;
    }

    @Override
    public <T> void set(int pos, T value) {
      throw new UnsupportedOperationException("A read row is not writable");
    }
  }

  /** The type of each field a filter may name, for the check that it can be evaluated at all. */
  static boolean isComparable(Types.NestedField field) {
    switch (field.type().typeId()) {
      case BOOLEAN:
      case INTEGER:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case STRING:
        return true;
      default:
        return false;
    }
  }
}
