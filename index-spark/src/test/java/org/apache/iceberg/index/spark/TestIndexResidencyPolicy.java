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

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class TestIndexResidencyPolicy {
  @Test
  void omittedPolicyIsDistinctFromZero() {
    assertThat(IndexResidencyPolicy.parse(null)).isNull();
  }

  @ParameterizedTest
  @ValueSource(strings = {"0", " 0 "})
  void explicitZeroHasNoProtection(String value) {
    IndexResidencyPolicy policy = IndexResidencyPolicy.parse(value);
    assertThat(policy).isNotNull();
    assertThat(policy.forever()).isFalse();
    assertThat(policy.durationNanos()).isZero();
  }

  @ParameterizedTest
  @ValueSource(strings = {"forever", "FOREVER", " forever "})
  void indefiniteProtection(String value) {
    assertThat(IndexResidencyPolicy.parse(value).forever()).isTrue();
  }

  @ParameterizedTest
  @CsvSource({
    "1s,1",
    "30m,1800",
    "2h,7200",
    "3d,259200",
    "' 1M ',60",
    "0002s,2",
    "9223372036s,9223372036"
  })
  void finiteProtectionUsesTimeUnits(String value, long seconds) {
    IndexResidencyPolicy policy = IndexResidencyPolicy.parse(value);
    assertThat(policy.forever()).isFalse();
    assertThat(policy.durationNanos()).isEqualTo(TimeUnit.SECONDS.toNanos(seconds));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "",
        " ",
        "-1",
        "-1s",
        "0s",
        "0m",
        "1",
        "1x",
        "1ms",
        "1.5h",
        "+1s",
        "1 s",
        "forever1",
        "9223372037s",
        "106752d",
        "999999999999999999999s"
      })
  void rejectsInvalidOrOverflowingProtection(String value) {
    assertThatThrownBy(() -> IndexResidencyPolicy.parse(value))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("retention")
        .hasMessageContaining(value);
  }
}
