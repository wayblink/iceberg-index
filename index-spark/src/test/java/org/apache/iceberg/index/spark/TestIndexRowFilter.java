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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.junit.jupiter.api.Test;

/** A row the filter turns down is not part of the answer, however near the query it is. */
class TestIndexRowFilter {
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.required(2, "price", Types.LongType.get()),
          Types.NestedField.required(3, "category", Types.StringType.get()));

  @Test
  void keepsTheRowsTheFilterAdmits() {
    IndexRowFilter filter = new IndexRowFilter(SCHEMA, Expressions.equal("category", "books"));

    assertThat(filter.matches(row(1, 10L, "books"))).isTrue();
    assertThat(filter.matches(row(2, 10L, "toys"))).isFalse();
  }

  @Test
  void comparesNumbersOfTheWiderTypeUnixTimeUses() {
    IndexRowFilter filter = new IndexRowFilter(SCHEMA, Expressions.lessThan("price", 100L));

    assertThat(filter.matches(row(1, 99L, "books"))).isTrue();
    assertThat(filter.matches(row(1, 100L, "books"))).isFalse();
  }

  @Test
  void keepsTheRowsWhenTheFilterIsAConjunction() {
    IndexRowFilter filter =
        new IndexRowFilter(
            SCHEMA,
            Expressions.and(
                Expressions.equal("category", "books"), Expressions.greaterThan("id", 5)));

    assertThat(filter.matches(row(6, 10L, "books"))).isTrue();
    assertThat(filter.matches(row(5, 10L, "books"))).isFalse();
    assertThat(filter.matches(row(6, 10L, "toys"))).isFalse();
  }

  @Test
  void turnsDownARowItCannotRead() {
    IndexRowFilter filter = new IndexRowFilter(SCHEMA, Expressions.equal("category", "books"));

    assertThat(filter.matches(null))
        .as("a row the fetch-back could not resolve is not a candidate")
        .isFalse();
  }

  @Test
  void readsARowWhoseNumbersSparkHandedBackAsIntegers() {
    IndexRowFilter filter = new IndexRowFilter(SCHEMA, Expressions.lessThan("price", 100L));
    // a row built the way a fetch-back builds one: a long column may still arrive as an int
    Row narrow = RowFactory.create(Integer.valueOf(1), Integer.valueOf(42), "books");

    assertThat(filter.matches(narrow))
        .as("a value is compared by what it means, not by the box it arrived in")
        .isTrue();
  }

  private static Row row(int id, long price, String category) {
    return RowFactory.create(Integer.valueOf(id), Long.valueOf(price), category);
  }

  /** The columns a filter names, which have to be read for it to be evaluated. */
  @Test
  void keepsTheFieldsItCanCompare() {
    List<Types.NestedField> fields = SCHEMA.columns();

    assertThat(IndexRowFilter.isComparable(fields.get(0))).isTrue();
    assertThat(IndexRowFilter.isComparable(fields.get(1))).isTrue();
    assertThat(IndexRowFilter.isComparable(fields.get(2))).isTrue();
  }
}
