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
import org.apache.iceberg.types.Type;
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
 * of the read. A column whose value arrives in a representation of its own, a date or a timestamp,
 * is converted by what the column means rather than by the class it arrived in.
 */
public final class IndexRowFilter implements java.io.Serializable {
  private static final long serialVersionUID = 1L;

  private static final long MICROS_PER_SECOND = 1_000_000L;
  private static final long NANOS_PER_MICRO = 1_000L;

  private final Evaluator evaluator;
  private final Types.StructType readAs;

  public IndexRowFilter(Schema schema, Expression filter) {
    Preconditions.checkArgument(schema != null, "Schema is null");
    Preconditions.checkArgument(filter != null, "Filter is null");
    this.readAs = schema.asStruct();
    this.evaluator = new Evaluator(readAs, filter);
  }

  /** Whether the row is part of the answer the unfiltered table would have given. */
  public boolean matches(Row row) {
    return row != null && evaluator.eval(new RowStruct(row, readAs));
  }

  /** The struct a row is read as, by the position of each field. */
  private static final class RowStruct implements org.apache.iceberg.StructLike {
    private final Row row;
    private final Types.StructType struct;

    private RowStruct(Row row, Types.StructType struct) {
      this.row = row;
      this.struct = struct;
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

      return javaClass.cast(convert(value, struct.fields().get(pos).type(), javaClass));
    }

    /**
     * Turns a value the row carries into the one Iceberg compares against.
     *
     * <p>Two representations differ. A plain number may arrive in a narrower box than the field's
     * type asks for, which is a matter of the class. A date or a timestamp arrives as the java.time
     * or java.sql value that Spark's own reader produces, while Iceberg compares an int number of
     * days or a long number of microseconds, so those are converted by what the field means rather
     * than by the class the value happens to have: the class of a date and of an int are both
     * integers to Iceberg, and only the type tells them apart.
     */
    private static Object convert(Object value, Type type, Class<?> javaClass) {
      switch (type.typeId()) {
        case DATE:
          return days(value);
        case TIME:
          return microsOfDay(value);
        case TIMESTAMP:
          return micros(value, NANOS_PER_MICRO);
        case TIMESTAMP_NANO:
          return micros(value, 1L);
        default:
          return narrowed(value, javaClass);
      }
    }

    private static Object narrowed(Object value, Class<?> javaClass) {
      if (!(value instanceof Number)) {
        return value;
      }

      Number number = (Number) value;
      if (javaClass == Integer.class) {
        return number.intValue();
      }

      if (javaClass == Long.class) {
        return number.longValue();
      }

      if (javaClass == Float.class) {
        return number.floatValue();
      }

      if (javaClass == Double.class) {
        return number.doubleValue();
      }

      return value;
    }

    /** A date as the int number of days from the epoch that Iceberg stores it as. */
    private static Object days(Object value) {
      if (value instanceof java.sql.Date) {
        return (int) ((java.sql.Date) value).toLocalDate().toEpochDay();
      }

      if (value instanceof java.time.LocalDate) {
        return (int) ((java.time.LocalDate) value).toEpochDay();
      }

      return narrowed(value, Integer.class);
    }

    /** A time of day as the long number of microseconds since midnight. */
    private static Object microsOfDay(Object value) {
      if (value instanceof java.sql.Time) {
        return ((java.sql.Time) value).toLocalTime().toNanoOfDay() / NANOS_PER_MICRO;
      }

      if (value instanceof java.time.LocalTime) {
        return ((java.time.LocalTime) value).toNanoOfDay() / NANOS_PER_MICRO;
      }

      return narrowed(value, Long.class);
    }

    /**
     * A timestamp as the long number Iceberg stores it as, in the unit the field is measured in.
     *
     * @param divisor how many of the value's units make up one of the field's
     */
    private static Object micros(Object value, long divisor) {
      long epochMicros;
      if (value instanceof java.sql.Timestamp) {
        java.time.Instant instant = ((java.sql.Timestamp) value).toInstant();
        epochMicros =
            Math.addExact(
                Math.multiplyExact(instant.getEpochSecond(), MICROS_PER_SECOND),
                instant.getNano() / NANOS_PER_MICRO);
      } else if (value instanceof java.time.LocalDateTime) {
        java.time.LocalDateTime local = (java.time.LocalDateTime) value;
        epochMicros =
            Math.addExact(
                Math.multiplyExact(
                    local.toEpochSecond(java.time.ZoneOffset.UTC), MICROS_PER_SECOND),
                local.getNano() / NANOS_PER_MICRO);
      } else if (value instanceof java.time.Instant) {
        java.time.Instant instant = (java.time.Instant) value;
        epochMicros =
            Math.addExact(
                Math.multiplyExact(instant.getEpochSecond(), MICROS_PER_SECOND),
                instant.getNano() / NANOS_PER_MICRO);
      } else {
        return narrowed(value, Long.class);
      }

      return divisor == NANOS_PER_MICRO
          ? (Object) Long.valueOf(epochMicros)
          : (Object) Long.valueOf(Math.multiplyExact(epochMicros, NANOS_PER_MICRO));
    }

    @Override
    public <T> void set(int pos, T value) {
      throw new UnsupportedOperationException("A read row is not writable");
    }
  }
}
