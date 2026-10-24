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
  void derivesABudgetThatHoldsWhatAPreparedShareCosts() {
    // 38 files of 120 MB over three executors: each holds about 1.5 GB of file, and the payloads
    // of those files cost several times that once they are prepared. The heap is large enough
    // that the fraction is not what decides, which is the case this is about.
    long workingSet = 38 * 120 * MB;
    long budget = ExecutorIndexCache.Config.derive(null, 0, workingSet, 3, 64 * GB, 0.25);

    long preparedShare = (long) (workingSet * ExecutorIndexCache.Config.PREPARED_TO_PAYLOAD / 3);
    assertThat(budget)
        .as("the budget has to cover what the share becomes, not what it weighs")
        .isGreaterThanOrEqualTo(preparedShare);
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
