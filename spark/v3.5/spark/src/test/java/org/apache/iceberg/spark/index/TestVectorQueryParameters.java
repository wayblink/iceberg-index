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

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestVectorQueryParameters {
  private static final String OVER_SAMPLE = "spark.iceberg.index.vector.over-sample";
  private static final String INDEX_NAME = "spark.iceberg.index.vector.name";

  @Test
  void explicitDefaultIsLocked() {
    RawVectorParameters raw =
        VectorQueryParameters.capture(Map.of(OVER_SAMPLE, "1"), Map.of(), null);
    ResolvedVectorParameters chosen = VectorQueryParameters.decide(raw);
    assertThat(chosen.overSample()).isEqualTo(1L);
    assertThat(chosen.overSampleLocked()).isTrue();
    assertThat(chosen.overSampleOrigin()).isEqualTo("SESSION_CONF");
  }

  @Test
  void absenceIsNotDefaultBeforeDecision() {
    RawVectorParameters raw = VectorQueryParameters.capture(Map.of(), Map.of(), null);
    assertThat(raw.overSample()).isNull();
    ResolvedVectorParameters chosen = VectorQueryParameters.decide(raw);
    assertThat(chosen.overSample()).isEqualTo(1L);
    assertThat(chosen.overSampleLocked()).isFalse();
    assertThat(chosen.overSampleOrigin()).isEqualTo("COMPAT_DEFAULT");
    assertThat(chosen.overSampleReason()).isEqualTo("no-calibrated-policy");
  }

  @Test
  void sessionWinsSubmit() {
    RawVectorParameters raw =
        VectorQueryParameters.capture(Map.of(OVER_SAMPLE, "100"), Map.of(OVER_SAMPLE, "2"), null);
    ResolvedVectorParameters chosen = VectorQueryParameters.decide(raw);
    assertThat(chosen.overSample()).isEqualTo(100L);
    assertThat(chosen.overSampleOrigin()).isEqualTo("SESSION_CONF");
    assertThat(chosen.overSampleShadowedOrigin()).isEqualTo("SUBMIT_CONF");
  }

  @Test
  void submitOnlyIsLocked() {
    ResolvedVectorParameters chosen =
        VectorQueryParameters.decide(
            VectorQueryParameters.capture(Map.of(), Map.of(OVER_SAMPLE, "8"), null));
    assertThat(chosen.overSample()).isEqualTo(8L);
    assertThat(chosen.overSampleOrigin()).isEqualTo("SUBMIT_CONF");
    assertThat(chosen.overSampleLocked()).isTrue();
  }

  @Test
  void copiedSubmitOriginIsAmbiguous() {
    ResolvedVectorParameters chosen =
        VectorQueryParameters.decide(
            VectorQueryParameters.capture(
                Map.of(OVER_SAMPLE, "1"), Map.of(OVER_SAMPLE, "1"), null));
    assertThat(chosen.overSample()).isEqualTo(1L);
    assertThat(chosen.overSampleOrigin()).isEqualTo("USER_CONF(session-or-submit)");
    assertThat(chosen.overSampleLocked()).isTrue();
  }

  @Test
  void argumentIndexWinsSession() {
    RawVectorParameters raw =
        VectorQueryParameters.capture(
            Map.of(INDEX_NAME, "session-index"),
            Map.of(INDEX_NAME, "submit-index"),
            "argument-index");
    VectorParameter selected = VectorQueryParameters.decide(raw).indexName();
    assertThat(selected.value()).isEqualTo("argument-index");
    assertThat(selected.origin()).isEqualTo("CALL_ARGUMENT");
    assertThat(selected.locked()).isTrue();
    assertThat(selected.shadowedOrigin()).contains("SESSION_CONF", "SUBMIT_CONF");
  }

  @Test
  void sessionIndexWinsSubmit() {
    VectorParameter selected =
        VectorQueryParameters.decide(
                VectorQueryParameters.capture(
                    Map.of(INDEX_NAME, "session-index"), Map.of(INDEX_NAME, "submit-index"), null))
            .indexName();
    assertThat(selected.value()).isEqualTo("session-index");
    assertThat(selected.origin()).isEqualTo("SESSION_CONF");
  }

  @ParameterizedTest
  @ValueSource(strings = {"", " ", "abc", "0", "-1", "9223372036854775808"})
  void invalidExplicitOverSampleFails(String configured) {
    RawVectorParameters raw =
        VectorQueryParameters.capture(Map.of(OVER_SAMPLE, configured), Map.of(), null);
    assertThatThrownBy(() -> VectorQueryParameters.decide(raw))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(OVER_SAMPLE)
        .hasMessageContaining(configured);
  }

  @Test
  void invalidSessionIsNotReplacedByValidSubmit() {
    RawVectorParameters raw =
        VectorQueryParameters.capture(Map.of(OVER_SAMPLE, ""), Map.of(OVER_SAMPLE, "10"), null);
    assertThatThrownBy(() -> VectorQueryParameters.decide(raw))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(OVER_SAMPLE);
  }

  @Test
  void captureDoesNotRetainMutableConfiguration() {
    Map<String, String> session = new HashMap<>();
    session.put(OVER_SAMPLE, "100");
    RawVectorParameters raw = VectorQueryParameters.capture(session, Map.of(), null);
    session.put(OVER_SAMPLE, "2");
    assertThat(VectorQueryParameters.decide(raw).overSample()).isEqualTo(100L);
  }

  @Test
  void repeatDecisionDoesNotChangeLockedValues() {
    RawVectorParameters raw =
        VectorQueryParameters.capture(Map.of(OVER_SAMPLE, "100"), Map.of(), null);
    assertThat(VectorQueryParameters.decide(raw)).isEqualTo(VectorQueryParameters.decide(raw));
    assertThat(raw.overSample().value()).isEqualTo("100");
  }

  @Test
  void whitespaceIsAcceptedWithoutLosingSource() {
    ResolvedVectorParameters chosen =
        VectorQueryParameters.decide(
            VectorQueryParameters.capture(Map.of(OVER_SAMPLE, " 100 "), Map.of(), null));
    assertThat(chosen.overSample()).isEqualTo(100L);
    assertThat(chosen.overSampleOrigin()).isEqualTo("SESSION_CONF");
  }
}
