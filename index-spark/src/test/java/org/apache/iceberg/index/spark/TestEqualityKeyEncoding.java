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

import java.math.BigDecimal;
import java.time.LocalDate;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/** Pins the canonical form of an equality delete key value. */
class TestEqualityKeyEncoding {

  @Test
  public void aDecimalKeysTheSameWhateverItsScale() {
    Types.DecimalType type = Types.DecimalType.of(9, 2);

    // the scale of a decimal is not part of its value, and the two sides of a delete can read the
    // same number with a different one
    assertThat(IndexEntries.encodedValue(type, new BigDecimal("2.50")))
        .isEqualTo(IndexEntries.encodedValue(type, new BigDecimal("2.5")));
    assertThat(IndexEntries.encodedValue(type, new BigDecimal("0.00")))
        .isEqualTo(IndexEntries.encodedValue(type, new BigDecimal("0")));
    assertThat(IndexEntries.encodedValue(type, new BigDecimal("-1.20")))
        .isEqualTo(IndexEntries.encodedValue(type, new BigDecimal("-1.2")))
        .isNotEqualTo(IndexEntries.encodedValue(type, new BigDecimal("1.2")));
  }

  @Test
  public void aDateKeysTheSameWhateverJavaTypeCarriesIt() {
    Types.DateType type = Types.DateType.get();
    LocalDate day = LocalDate.of(2024, 1, 1);

    // the rows of a data file are read as LocalDate, a delete written by Spark carries the same day
    // as a number, and a reader that predates LocalDate hands back java.sql.Date: all three name
    // one day, so all three have to produce one key
    assertThat(IndexEntries.encodedValue(type, day))
        .isEqualTo(IndexEntries.encodedValue(type, day.toEpochDay()))
        .isEqualTo(IndexEntries.encodedValue(type, java.sql.Date.valueOf(day)));
  }
}
