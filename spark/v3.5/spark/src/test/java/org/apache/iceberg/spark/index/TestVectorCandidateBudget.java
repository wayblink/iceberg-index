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
package org.apache.iceberg.spark.index;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.junit.jupiter.api.Test;

class TestVectorCandidateBudget {
  @Test
  void entryBoundDoesNotChangeMultiplier() {
    ResolvedVectorParameters parameters =
        VectorQueryParameters.decide(
            VectorQueryParameters.capture(
                Map.of("spark.iceberg.index.vector.over-sample", "100"), Map.of(), null));
    VectorCandidateBudget budget = VectorCandidateBudget.derive(10, parameters.overSample(), 20L);
    assertThat(budget.requested()).isEqualTo(1000L);
    assertThat(budget.initial()).isEqualTo(20);
    assertThat(parameters.overSample()).isEqualTo(100L);
    assertThat(parameters.overSampleLocked()).isTrue();
  }

  @Test
  void unknownUpperBoundIsNotEmpty() {
    VectorCandidateBudget budget = VectorCandidateBudget.derive(10, 100L, null);
    assertThat(budget.requested()).isEqualTo(1000L);
    assertThat(budget.initial()).isEqualTo(1000);
    assertThat(budget.entryUpperBound()).isNull();
  }

  @Test
  void knownEmptyIndexHasZeroBudget() {
    VectorCandidateBudget budget = VectorCandidateBudget.derive(10, 100L, 0L);
    assertThat(budget.initial()).isZero();
    assertThat(budget.requested()).isEqualTo(1000L);
  }

  @Test
  void aboveIntBudgetCanBeBounded() {
    VectorCandidateBudget budget = VectorCandidateBudget.derive(2, Integer.MAX_VALUE, 20L);
    assertThat(budget.requested()).isEqualTo(4294967294L);
    assertThat(budget.initial()).isEqualTo(20);
  }

  @Test
  void aboveIntBudgetWithoutBoundFails() {
    assertThatThrownBy(() -> VectorCandidateBudget.derive(2, Integer.MAX_VALUE, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("4294967294");
  }

  @Test
  void aboveIntBudgetWithLargeBoundFails() {
    assertThatThrownBy(() -> VectorCandidateBudget.derive(2, Integer.MAX_VALUE, 4294967294L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("4294967294");
  }

  @Test
  void longOverflowFailsBeforeBounding() {
    assertThatThrownBy(() -> VectorCandidateBudget.derive(2, Long.MAX_VALUE, 20L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("overflow")
        .hasMessageContaining("9223372036854775807");
  }

  @Test
  void largestIntBudgetIsRepresentable() {
    assertThat(VectorCandidateBudget.derive(1, Integer.MAX_VALUE, null).initial())
        .isEqualTo(Integer.MAX_VALUE);
  }

  @Test
  void nextRequestsCumulativeRatherThanMissingOnly() {
    assertThat(VectorCandidateBudget.next(100, 7, 10, null)).isEqualTo(103);
  }

  @Test
  void nextCanBeBounded() {
    assertThat(VectorCandidateBudget.next(100, 0, 10, 105L)).isEqualTo(105);
  }

  @Test
  void nextCannotStopProgressSilently() {
    assertThatThrownBy(() -> VectorCandidateBudget.next(100, 0, 10, 100L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("progress");
  }

  @Test
  void nextCannotWrapInt() {
    assertThatThrownBy(() -> VectorCandidateBudget.next(Integer.MAX_VALUE, 0, 10, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("2147483657");
  }

  @Test
  void boundIsPerQueryNotSharedAcrossBatch() {
    assertThat(VectorCandidateBudget.derive(10, 100L, 20L).initial()).isEqualTo(20);
    assertThat(VectorCandidateBudget.derive(10, 100L, 20L).initial()).isEqualTo(20);
  }
}
