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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.index.TextStatisticsPolicy;
import org.junit.jupiter.api.Test;

/** What a text build does with the corpus statistics, and how it names that choice. */
class TestTextStatisticsPolicy {

  @Test
  void parsesTheValueACallerPasses() {
    assertThat(TextStatisticsPolicy.parse("auto")).isEqualTo(TextStatisticsPolicy.AUTO);
    assertThat(TextStatisticsPolicy.parse("ALWAYS")).isEqualTo(TextStatisticsPolicy.ALWAYS);
    assertThat(TextStatisticsPolicy.parse(" never ")).isEqualTo(TextStatisticsPolicy.NEVER);
  }

  @Test
  void refusesAValueThatNamesNoPolicy() {
    assertThatThrownBy(() -> TextStatisticsPolicy.parse("sometimes"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("expected auto, always or never");
    assertThatThrownBy(() -> TextStatisticsPolicy.parse(null))
        .as("a missing value is not a policy")
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("expected auto, always or never");
  }

  @Test
  void storesTheCanonicalName() {
    assertThat(TextStatisticsPolicy.AUTO.value()).isEqualTo("auto");
    assertThat(TextStatisticsPolicy.ALWAYS.value()).isEqualTo("always");
    assertThat(TextStatisticsPolicy.NEVER.value()).isEqualTo("never");
  }

  @Test
  void mergesWithinTheLimitAndSkipsPastIt() {
    long limit = 1024L;

    assertThat(SparkIndexBuilder.writesTextStatistics(TextStatisticsPolicy.AUTO, limit - 1L, limit))
        .as("a merge below the limit runs")
        .isTrue();
    assertThat(SparkIndexBuilder.writesTextStatistics(TextStatisticsPolicy.AUTO, limit, limit))
        .as("a merge exactly at the limit runs")
        .isTrue();
    assertThat(SparkIndexBuilder.writesTextStatistics(TextStatisticsPolicy.AUTO, limit + 1L, limit))
        .as("a merge past the limit is skipped, so the index is published without statistics")
        .isFalse();
  }

  @Test
  void budgetsForHeapAndDriverResultLimits() {
    long gibibyte = 1024L * 1024 * 1024;
    assertThat(SparkIndexBuilder.textStatisticsMergeBudget(4 * gibibyte, gibibyte, 0))
        .isEqualTo(gibibyte / 4);
    assertThat(SparkIndexBuilder.textStatisticsMergeBudget(gibibyte, 4 * gibibyte, gibibyte / 8))
        .isEqualTo(gibibyte / 8);
    assertThat(SparkIndexBuilder.textStatisticsMergeBudget(64 * gibibyte, 64 * gibibyte, 0))
        .isLessThan(Integer.MAX_VALUE);
  }

  @Test
  void alwaysAndNeverDoNotDependOnTheSize() {
    long limit = 1024L;

    assertThat(SparkIndexBuilder.writesTextStatistics(TextStatisticsPolicy.ALWAYS, 1L << 40, limit))
        .as("a forced merge runs whatever the size")
        .isTrue();
    assertThat(SparkIndexBuilder.writesTextStatistics(TextStatisticsPolicy.NEVER, 0L, limit))
        .as("a skipped merge stays skipped whatever the size")
        .isFalse();
  }
}
