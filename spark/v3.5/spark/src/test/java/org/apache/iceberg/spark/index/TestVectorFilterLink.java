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
import org.apache.iceberg.index.IndexCandidates;
import org.junit.jupiter.api.Test;

/**
 * The rule picks the cheaper link, a query that named one gets it, and a query that named one it
 * cannot have is told.
 */
class TestVectorFilterLink {
  private static final int CAP = 100;
  private static final double LIMIT = 0.25;

  @Test
  void theRuleTakesTheFilteredRowsWhenTheyAreFew() {
    IndexCandidates decided =
        VectorFilterLink.decide(
            VectorQueryParameters.FilterModeAuto(),
            Map.of("a.parquet", new long[] {1L, 2L}),
            Long.valueOf(100L),
            CAP,
            LIMIT);

    assertThat(decided).isNotNull();
    assertThat(decided.count()).isEqualTo(2L);
  }

  @Test
  void theRuleLeavesTheFilterToAfterTheSearchWhenItKeepsTooMany() {
    IndexCandidates decided =
        VectorFilterLink.decide(
            VectorQueryParameters.FilterModeAuto(),
            Map.of("a.parquet", new long[] {1L, 2L, 3L}),
            Long.valueOf(4L),
            CAP,
            LIMIT);

    assertThat(decided)
        .as("reading the filter's columns for three quarters of the table buys nothing")
        .isNull();
  }

  @Test
  void aQueryThatNamedThePreFilterLinkKeepsItAtAnyDensity() {
    IndexCandidates decided =
        VectorFilterLink.decide(
            VectorQueryParameters.FilterModePre(),
            Map.of("a.parquet", new long[] {1L, 2L, 3L, 4L}),
            Long.valueOf(4L),
            CAP,
            LIMIT);

    assertThat(decided).isNotNull();
    assertThat(decided.count()).isEqualTo(4L);
  }

  @Test
  void aQueryThatNamedThePreFilterLinkIsNotAnsweredAnotherWayWhenItCannotBeHad() {
    assertThatThrownBy(
            () ->
                VectorFilterLink.decide(
                    VectorQueryParameters.FilterModePre(), null, Long.valueOf(1000L), CAP, LIMIT))
        .as("the whole point of naming a link is to find out when it was not used")
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(VectorQueryParameters.FilterModeConf())
        .hasMessageContaining("pre");
  }

  @Test
  void aQueryThatNamedThePostFilterLinkDoesNotReadTheRowsAtAll() {
    assertThat(
            VectorFilterLink.decide(
                VectorQueryParameters.FilterModePost(),
                Map.of("a.parquet", new long[] {1L}),
                Long.valueOf(100L),
                CAP,
                LIMIT))
        .as("the caller decides that before it reads anything, and this only judges what it has")
        .isNull();
  }

  @Test
  void theDensityIsUnknownWhenTheIndexDoesNotSayHowManyEntriesItHas() {
    assertThat(
            VectorFilterLink.decide(
                VectorQueryParameters.FilterModeAuto(),
                Map.of("a.parquet", new long[] {1L}),
                null,
                CAP,
                LIMIT))
        .as("a set of unknown density cannot be shown to be worth reading the columns for")
        .isNull();
  }

  @Test
  void anEmptySetIsACandidateSetAndNotAMissingOne() {
    IndexCandidates decided =
        VectorFilterLink.decide(
            VectorQueryParameters.FilterModeAuto(), Map.of(), Long.valueOf(100L), CAP, LIMIT);

    assertThat(decided)
        .as("a filter that keeps nothing is answered with nothing, not with the unfiltered rows")
        .isNotNull();
    assertThat(decided.isEmpty()).isTrue();
  }
}
