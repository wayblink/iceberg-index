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
package org.apache.iceberg.lance;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

/**
 * Pins what a failed read says when no storage settings were resolved.
 *
 * <p>Without this the object-store client's own error is all there is, and it reports a missing
 * bucket on public AWS S3 for what is really a table on an S3-compatible store with nothing
 * configured — the shape that made the original report read like a product bug.
 */
class TestLanceOpenFailureMessage {

  private static final String PATH = "s3://bucket/table/data/0.lance";

  @Test
  void namesWhatIsMissingWhenNoEndpointWasResolved() {
    String message = LanceFormatModel.openFailureMessage(PATH, ImmutableMap.of());

    assertThat(message)
        .contains(PATH)
        .contains("no object-store endpoint was resolved")
        .contains("s3.endpoint")
        .contains("fs.s3a.endpoint")
        .contains("lance.storage.endpoint");
  }

  @Test
  void staysOutOfTheWayWhenAnEndpointWasResolved() {
    Map<String, String> storageOptions = ImmutableMap.of("endpoint", "http://oss.internal:8080");

    String message = LanceFormatModel.openFailureMessage(PATH, storageOptions);

    assertThat(message).contains(PATH).doesNotContain("no object-store endpoint was resolved");
  }
}
