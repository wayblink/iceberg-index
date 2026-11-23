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

import java.io.IOException;
import java.io.UncheckedIOException;
import org.junit.jupiter.api.Test;

/**
 * Which failures a partial build may keep as a gap.
 *
 * <p>A gap is a claim that these rows cannot be indexed, and a refresh is the only thing that takes
 * it back. A failure that a retry would get past is not that claim, so it has to fail the task
 * instead: this is the boundary that keeps a transient read error from becoming a permanent hole in
 * the index.
 */
class TestDataFailurePolicy {

  @Test
  void aVectorTheAdapterRejectsIsAGap() {
    assertThat(SparkIndexBuilder.isDataFailure(new IllegalArgumentException("3 dimensions")))
        .as("a vector of the wrong width fails the same way however often it is retried")
        .isTrue();
  }

  @Test
  void aPayloadPastTheLimitIsAGap() {
    assertThat(SparkIndexBuilder.isDataFailure(new IllegalArgumentException("payload too large")))
        .as("a payload past the limit is a property of the rows, not of the attempt")
        .isTrue();
  }

  @Test
  void aReadThatFailedIsNotAGap() {
    assertThat(
            SparkIndexBuilder.isDataFailure(
                new UncheckedIOException(new IOException("Connection reset"))))
        .as("a read that failed may succeed on the next attempt")
        .isFalse();
  }

  @Test
  void aFailureBelowTheDataIsNotAGap() {
    assertThat(SparkIndexBuilder.isDataFailure(new NullPointerException()))
        .as("a bug in the engine is not a statement about the rows")
        .isFalse();
    assertThat(SparkIndexBuilder.isDataFailure(new IllegalStateException("executor lost")))
        .as("an executor taken for its memory says nothing about the rows either")
        .isFalse();
    assertThat(SparkIndexBuilder.isDataFailure(new OutOfMemoryError()))
        .as("a group that did not fit is a build to retry, not a table to abandon")
        .isFalse();
  }

  @Test
  void anErrorThatIsNotAnIllegalArgumentIsNotAGap() {
    assertThat(SparkIndexBuilder.isDataFailure(new AssertionError("engine invariant")))
        .as("only a rejected row is a gap; every other failure fails the build")
        .isFalse();
  }
}
