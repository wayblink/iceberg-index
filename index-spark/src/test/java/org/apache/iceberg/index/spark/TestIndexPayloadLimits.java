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

import org.junit.jupiter.api.Test;

public class TestIndexPayloadLimits {
  private static final long LIMIT = IndexPayloadLimits.MAX_PAYLOAD_BYTES;

  @Test
  void aPayloadThatFitsIsAccepted() {
    IndexPayloadLimits.checkFits(LIMIT, 1_000L, "file:/indexes/a");
    IndexPayloadLimits.checkFits(1L, 1L, "file:/indexes/a");
  }

  @Test
  void aPayloadPastTheLimitFailsTheBuildAndNamesTheProperty() {
    assertThatThrownBy(() -> IndexPayloadLimits.checkFits(LIMIT + 1, 2_000_000L, "file:/indexes/a"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("file:/indexes/a")
        .hasMessageContaining(Long.toString(LIMIT + 1))
        .hasMessageContaining("max-entries-per-index-file");
  }

  @Test
  void theMessageSuggestsABudgetThatWouldFit() {
    // 4 GiB payload over 4 M entries is about 1 KiB each, so the suggestion has to be safe: the
    // budget it names must keep such a file under the limit, and must not be absurdly small.
    long payloadBytes = 4L * 1024 * 1024 * 1024;
    long entries = 4_000_000L;
    long bytesPerEntry = payloadBytes / entries;

    String message = messageOf(payloadBytes, entries);
    long suggested = suggestedEntries(message);

    assertThat(suggested * bytesPerEntry).isLessThanOrEqualTo(LIMIT);
    assertThat(suggested).isGreaterThan(LIMIT / bytesPerEntry / 2);
  }

  private static String messageOf(long payloadBytes, long entries) {
    try {
      IndexPayloadLimits.checkFits(payloadBytes, entries, "file:/a");
    } catch (IllegalArgumentException e) {
      return e.getMessage();
    }

    throw new AssertionError("A payload past the limit was accepted");
  }

  private static long suggestedEntries(String message) {
    java.util.regex.Matcher matcher =
        java.util.regex.Pattern.compile("so (\\d+) entries per index file or fewer")
            .matcher(message);
    assertThat(matcher.find()).as("suggestion in: %s", message).isTrue();
    return Long.parseLong(matcher.group(1));
  }

  @Test
  void aSuggestionIsOmittedWhenThereIsNothingToDeriveItFrom() {
    assertThatThrownBy(() -> IndexPayloadLimits.checkFits(LIMIT + 1, 0L, "file:/a"))
        .hasMessageContaining("max-entries-per-index-file")
        .hasMessageNotContaining("entries per index file or fewer");
  }

  @Test
  void theReadSideSaysHowToRebuild() {
    assertThat(IndexPayloadLimits.tooLargeToRead(LIMIT + 5, "file:/a"))
        .contains("file:/a")
        .contains(Long.toString(LIMIT + 5))
        .contains("max-entries-per-index-file")
        .contains("rebuild");
  }
}
