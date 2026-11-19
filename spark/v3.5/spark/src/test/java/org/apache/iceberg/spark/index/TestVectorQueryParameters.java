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
import org.apache.iceberg.index.vector.VectorIndexDefinition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestVectorQueryParameters {
  private static final String OVER_SAMPLE = "spark.iceberg.index.vector.over-sample";
  private static final String INDEX_NAME = "spark.iceberg.index.vector.name";

  private static final VectorIndexDefinition FLAT =
      VectorIndexDefinition.builder()
          .fieldId(1)
          .dimension(2)
          .metric(VectorIndexDefinition.Metric.L2)
          .structure(VectorIndexDefinition.VectorStructure.FLAT)
          .build();

  private static VectorIndexDefinition shaped(
      VectorIndexDefinition.VectorStructure structure,
      VectorIndexDefinition.VectorQuantizer quantizer,
      int bits) {
    Map<String, String> properties = new HashMap<>();
    if (quantizer == VectorIndexDefinition.VectorQuantizer.RABITQ) {
      properties.put("rabitq-bits", String.valueOf(bits));
    }
    return VectorIndexDefinition.builder()
        .fieldId(1)
        .dimension(2)
        .metric(VectorIndexDefinition.Metric.L2)
        .structure(structure)
        .quantizer(quantizer)
        .properties(properties)
        .build();
  }

  @Test
  void aProbeCountTheCallNamesWinsOverTheSession() {
    RawVectorParameters raw =
        VectorQueryParameters.capture(
            Map.of(VectorQueryParameters.NprobeConf(), "8"), Map.of(), null, "64", null);

    ResolvedVectorParameters chosen = VectorQueryParameters.decide(raw, FLAT);
    assertThat(chosen.nprobe())
        .as("the call asked for 64 and the session was only the default")
        .isEqualTo(64L);
    assertThat(chosen.nprobeOrigin()).isEqualTo("CALL_ARGUMENT");
    assertThat(chosen.nprobeLocked()).isTrue();
    assertThat(chosen.nprobeShadowedOrigin())
        .as("the layer the call overrode is still reported")
        .isEqualTo("SESSION_CONF");
  }

  @Test
  void aProbeCountThatIsNotPositiveIsRefused() {
    for (String bad : new String[] {"0", "-1", "many", " "}) {
      RawVectorParameters raw = VectorQueryParameters.capture(Map.of(), Map.of(), null, bad, null);
      assertThatThrownBy(() -> VectorQueryParameters.decide(raw, FLAT))
          .as("%s is refused rather than answered with another probe count", bad)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining(VectorQueryParameters.NprobeConf());
    }
  }

  @Test
  void aProbeCountThatDoesNotFitAnIntIsRefused() {
    for (String bad : new String[] {"2147483648", "9223372036854775807"}) {
      RawVectorParameters raw = VectorQueryParameters.capture(Map.of(), Map.of(), null, bad, null);
      assertThatThrownBy(() -> VectorQueryParameters.decide(raw, FLAT))
          .as("%s is refused rather than truncated to another count", bad)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining(VectorQueryParameters.NprobeConf());
    }
  }

  @Test
  void onlyAProbeCountTheCallNamesIsLocked() {
    RawVectorParameters raw =
        VectorQueryParameters.capture(
            Map.of(VectorQueryParameters.NprobeConf(), "8"), Map.of(), null);

    ResolvedVectorParameters chosen = VectorQueryParameters.decide(raw, FLAT);
    assertThat(chosen.nprobe()).as("the session's count is resolved and reported").isEqualTo(8L);
    assertThat(chosen.nprobeOrigin()).isEqualTo("SESSION_CONF");
    assertThat(chosen.nprobeLocked())
        .as("only a count the call named is handed to the encoder explicitly")
        .isFalse();
  }

  @Test
  void theOtherKnobsCanComeFromTheCallToo() {
    RawVectorParameters raw =
        VectorQueryParameters.capture(
            Map.of(VectorQueryParameters.OverSampleConf(), "5"), Map.of(), null, null, "0");

    ResolvedVectorParameters chosen = VectorQueryParameters.decide(raw, FLAT);
    assertThat(chosen.overSample()).isZero();
    assertThat(chosen.overSampleOrigin()).isEqualTo("CALL_ARGUMENT");
    assertThat(chosen.overSampleShadowedOrigin()).isEqualTo("SESSION_CONF");
    assertThat(chosen.filterMode())
        .as("the filter link is not one of the settings a call can name")
        .isEqualTo(VectorQueryParameters.FilterModePre());
    assertThat(chosen.filterModeOrigin()).isEqualTo("DEFAULT");
  }

  @Test
  void aCallValueThatIsNotANumberIsRefused() {
    RawVectorParameters raw = VectorQueryParameters.capture(Map.of(), Map.of(), null, null, "two");
    assertThatThrownBy(() -> VectorQueryParameters.decide(raw, FLAT))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(VectorQueryParameters.OverSampleConf());
  }

  @Test
  void theFilterLinkDefaultsToTheOneThatKeepsEveryRow() {
    ResolvedVectorParameters chosen =
        VectorQueryParameters.decide(VectorQueryParameters.capture(Map.of(), Map.of(), null), FLAT);

    assertThat(chosen.filterMode())
        .as("leaving the knob alone asks for the link that loses no rows")
        .isEqualTo(VectorQueryParameters.FilterModePre());
    assertThat(chosen.filterModeOrigin()).isEqualTo("DEFAULT");
  }

  @Test
  void theFilterLinkHoldsTheValueTheQueryNamed() {
    for (String mode :
        new String[] {
          VectorQueryParameters.FilterModePre(), VectorQueryParameters.FilterModePost()
        }) {
      ResolvedVectorParameters chosen =
          VectorQueryParameters.decide(
              VectorQueryParameters.capture(
                  Map.of(VectorQueryParameters.FilterModeConf(), mode), Map.of(), null),
              FLAT);

      assertThat(chosen.filterMode()).isEqualTo(mode);
      assertThat(chosen.filterModeOrigin()).isEqualTo("SESSION_CONF");
    }
  }

  @Test
  void theFilterLinkIsReadWithoutRegardToCase() {
    ResolvedVectorParameters chosen =
        VectorQueryParameters.decide(
            VectorQueryParameters.capture(
                Map.of(VectorQueryParameters.FilterModeConf(), " POST "), Map.of(), null),
            FLAT);

    assertThat(chosen.filterMode()).isEqualTo(VectorQueryParameters.FilterModePost());
  }

  @Test
  void aFilterLinkThatDoesNotExistIsRefused() {
    for (String unknown : new String[] {"sometimes", "auto"}) {
      RawVectorParameters raw =
          VectorQueryParameters.capture(
              Map.of(VectorQueryParameters.FilterModeConf(), unknown), Map.of(), null);

      assertThatThrownBy(() -> VectorQueryParameters.decide(raw, FLAT))
          .as("%s is refused rather than answered some other way", unknown)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining(VectorQueryParameters.FilterModeConf());
    }
  }

  @Test
  void explicitDefaultIsLocked() {
    RawVectorParameters raw =
        VectorQueryParameters.capture(Map.of(OVER_SAMPLE, "0"), Map.of(), null);
    ResolvedVectorParameters chosen = VectorQueryParameters.decide(raw, FLAT);
    assertThat(chosen.overSample()).isZero();
    assertThat(chosen.overSampleLocked()).isTrue();
    assertThat(chosen.overSampleOrigin()).isEqualTo("SESSION_CONF");
  }

  @Test
  void absenceIsNotDefaultBeforeDecision() {
    RawVectorParameters raw = VectorQueryParameters.capture(Map.of(), Map.of(), null);
    assertThat(raw.overSample()).isNull();
    ResolvedVectorParameters chosen = VectorQueryParameters.decide(raw, FLAT);
    assertThat(chosen.overSample()).isZero();
    assertThat(chosen.overSampleLocked()).isFalse();
    assertThat(chosen.overSampleOrigin()).isEqualTo("DEFAULT");
    assertThat(chosen.overSampleReason()).isNull();
  }

  @Test
  void sessionWinsSubmit() {
    RawVectorParameters raw =
        VectorQueryParameters.capture(Map.of(OVER_SAMPLE, "100"), Map.of(OVER_SAMPLE, "2"), null);
    ResolvedVectorParameters chosen = VectorQueryParameters.decide(raw, FLAT);
    assertThat(chosen.overSample()).isEqualTo(100L);
    assertThat(chosen.overSampleOrigin()).isEqualTo("SESSION_CONF");
    assertThat(chosen.overSampleShadowedOrigin()).isEqualTo("SUBMIT_CONF");
  }

  @Test
  void submitOnlyIsLocked() {
    ResolvedVectorParameters chosen =
        VectorQueryParameters.decide(
            VectorQueryParameters.capture(Map.of(), Map.of(OVER_SAMPLE, "8"), null), FLAT);
    assertThat(chosen.overSample()).isEqualTo(8L);
    assertThat(chosen.overSampleOrigin()).isEqualTo("SUBMIT_CONF");
    assertThat(chosen.overSampleLocked()).isTrue();
  }

  @Test
  void copiedSubmitOriginIsAmbiguous() {
    ResolvedVectorParameters chosen =
        VectorQueryParameters.decide(
            VectorQueryParameters.capture(Map.of(OVER_SAMPLE, "1"), Map.of(OVER_SAMPLE, "1"), null),
            FLAT);
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
    VectorParameter selected = VectorQueryParameters.decide(raw, FLAT).indexName();
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
                    Map.of(INDEX_NAME, "session-index"), Map.of(INDEX_NAME, "submit-index"), null),
                FLAT)
            .indexName();
    assertThat(selected.value()).isEqualTo("session-index");
    assertThat(selected.origin()).isEqualTo("SESSION_CONF");
  }

  @ParameterizedTest
  @ValueSource(strings = {"", " ", "abc", "-1", "9223372036854775808"})
  void invalidExplicitOverSampleFails(String configured) {
    RawVectorParameters raw =
        VectorQueryParameters.capture(Map.of(OVER_SAMPLE, configured), Map.of(), null);
    assertThatThrownBy(() -> VectorQueryParameters.decide(raw, FLAT))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(OVER_SAMPLE)
        .hasMessageContaining(configured);
  }

  @Test
  void invalidSessionIsNotReplacedByValidSubmit() {
    RawVectorParameters raw =
        VectorQueryParameters.capture(Map.of(OVER_SAMPLE, ""), Map.of(OVER_SAMPLE, "10"), null);
    assertThatThrownBy(() -> VectorQueryParameters.decide(raw, FLAT))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(OVER_SAMPLE);
  }

  @Test
  void captureDoesNotRetainMutableConfiguration() {
    Map<String, String> session = new HashMap<>();
    session.put(OVER_SAMPLE, "100");
    RawVectorParameters raw = VectorQueryParameters.capture(session, Map.of(), null);
    session.put(OVER_SAMPLE, "2");
    assertThat(VectorQueryParameters.decide(raw, FLAT).overSample()).isEqualTo(100L);
  }

  @Test
  void repeatDecisionDoesNotChangeLockedValues() {
    RawVectorParameters raw =
        VectorQueryParameters.capture(Map.of(OVER_SAMPLE, "100"), Map.of(), null);
    assertThat(VectorQueryParameters.decide(raw, FLAT))
        .isEqualTo(VectorQueryParameters.decide(raw, FLAT));
    assertThat(raw.overSample().value()).isEqualTo("100");
  }

  @Test
  void anUnsetMultipleIsZeroAcrossStructures() {
    RawVectorParameters unset = VectorQueryParameters.capture(Map.of(), Map.of(), null);

    assertThat(VectorQueryParameters.decide(unset, FLAT).overSample()).isZero();
    assertThat(
            VectorQueryParameters.decide(
                    unset,
                    shaped(
                        VectorIndexDefinition.VectorStructure.IVF,
                        VectorIndexDefinition.VectorQuantizer.NONE,
                        0))
                .overSample())
        .isZero();

    assertThat(defaultFor(VectorIndexDefinition.VectorStructure.IVF, 1)).isZero();
    assertThat(defaultFor(VectorIndexDefinition.VectorStructure.IVF, 2)).isZero();
    assertThat(defaultFor(VectorIndexDefinition.VectorStructure.IVF, 3)).isZero();
    assertThat(defaultFor(VectorIndexDefinition.VectorStructure.IVF, 4)).isZero();

    assertThat(
            VectorQueryParameters.decide(
                    unset,
                    shaped(
                        VectorIndexDefinition.VectorStructure.HNSW,
                        VectorIndexDefinition.VectorQuantizer.NONE,
                        0))
                .overSample())
        .isZero();
  }

  private static long defaultFor(VectorIndexDefinition.VectorStructure structure, int bits) {
    return VectorQueryParameters.decide(
            VectorQueryParameters.capture(Map.of(), Map.of(), null),
            shaped(structure, VectorIndexDefinition.VectorQuantizer.RABITQ, bits))
        .overSample();
  }

  @Test
  void whitespaceIsAcceptedWithoutLosingSource() {
    ResolvedVectorParameters chosen =
        VectorQueryParameters.decide(
            VectorQueryParameters.capture(Map.of(OVER_SAMPLE, " 100 "), Map.of(), null), FLAT);
    assertThat(chosen.overSample()).isEqualTo(100L);
    assertThat(chosen.overSampleOrigin()).isEqualTo("SESSION_CONF");
  }
}
