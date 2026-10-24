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

import org.junit.jupiter.api.Test;

class TestIndexCacheBudget {

  private static final long MB = 1024L * 1024L;
  private static final long GB = 1024L * MB;

  @Test
  void derivesABudgetThatHoldsTheWholeWorkingSetOnEveryExecutor() {
    // a search can be sent to any executor, so the budget has to hold every file rather than an
    // even slice of them: a budget of one executor's share read 33 of 39 files, because the files
    // did not happen to land evenly
    long preparedWorkingSet = 38 * 120 * MB;
    long budget = ExecutorIndexCache.Config.derive(null, 0, preparedWorkingSet, 3, 64 * GB, 0.25);

    assertThat(budget)
        .as("the budget has to cover the whole prepared working set, not a share of it")
        .isGreaterThanOrEqualTo(preparedWorkingSet);
  }

  @Test
  void keepsTheHeapFractionAsTheCeiling() {
    long workingSet = 100 * GB;
    long budget = ExecutorIndexCache.Config.derive(null, 0, workingSet, 3, 10 * GB, 0.25);

    assertThat(budget).isEqualTo((long) (10 * GB * 0.25));
  }

  @Test
  void keepsAConfiguredBudget() {
    long budget = ExecutorIndexCache.Config.derive("3g", 3 * GB, 38 * 120 * MB, 3, 10 * GB, 0.25);

    assertThat(budget).isEqualTo(3 * GB);
  }

  @Test
  void usesTheFloorWhenTheWorkingSetIsUnknown() {
    long budget = ExecutorIndexCache.Config.derive(null, 0, 0, 3, 10 * GB, 0.25);

    assertThat(budget).isEqualTo(ExecutorIndexCache.Config.MIN_BUDGET_BYTES);
  }

  @Test
  void neverDerivesMoreThanTheConfiguredBudgetCanBack() {
    // one executor of 1 GB cannot hold 800 MB of payload no matter what the working set is
    long budget = ExecutorIndexCache.Config.derive(null, 0, 100 * GB, 1, 1 * GB, 0.25);

    assertThat(budget).isEqualTo((long) (1 * GB * 0.25));
  }
}
