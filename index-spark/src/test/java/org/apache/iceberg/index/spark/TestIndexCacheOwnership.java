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

import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

class TestIndexCacheOwnership {
  private final IndexCacheOwnership ownership = new IndexCacheOwnership();

  @Test
  void onlyCurrentTargetAndEpochMayAdmit() {
    ownership.applyTarget("A", 1, Set.of("a"));
    assertThat(ownership.mayAdmit("A", 1, "a")).isTrue();
    assertThat(ownership.mayAdmit("A", 0, "a")).isFalse();
    assertThat(ownership.mayAdmit("A", 2, "a")).isFalse();
    assertThat(ownership.mayAdmit("A", 1, "b")).isFalse();
    assertThat(ownership.mayAdmit("unknown", 1, "a")).isFalse();
  }

  @Test
  void offloadWinsOverSameOrOlderTarget() {
    ownership.applyTarget("A", 1, Set.of("a"));
    ownership.offload("A", 2);
    ownership.applyTarget("A", 1, Set.of("a"));
    ownership.applyTarget("A", 2, Set.of("a"));
    assertThat(ownership.mayAdmit("A", 1, "a")).isFalse();
    assertThat(ownership.mayAdmit("A", 2, "a")).isFalse();
    assertThat(ownership.owners("a")).isEmpty();
  }

  @Test
  void replacementDoesNotRetainOldUniqueTarget() {
    ownership.applyTarget("A", 1, Set.of("a", "common"));
    ownership.applyTarget("A", 2, Set.of("b", "common"));
    assertThat(ownership.mayAdmit("A", 2, "b")).isTrue();
    assertThat(ownership.mayAdmit("A", 2, "common")).isTrue();
    assertThat(ownership.mayAdmit("A", 2, "a")).isFalse();
    assertThat(ownership.owners("a")).isEmpty();
  }

  @Test
  void sameEpochCannotReplaceTarget() {
    ownership.applyTarget("A", 1, Set.of("a"));
    ownership.applyTarget("A", 1, Set.of("b"));
    assertThat(ownership.mayAdmit("A", 1, "a")).isTrue();
    assertThat(ownership.mayAdmit("A", 1, "b")).isFalse();
  }

  @Test
  void offloadBeforeLoadStillFencesLateCommands() {
    ownership.offload("A", 2);
    ownership.applyTarget("A", 1, Set.of("a"));
    ownership.applyTarget("A", 2, Set.of("a"));
    assertThat(ownership.mayAdmit("A", 2, "a")).isFalse();
    ownership.applyTarget("A", 3, Set.of("a"));
    assertThat(ownership.mayAdmit("A", 3, "a")).isTrue();
  }

  @Test
  void staleOffloadCannotPauseNewLoad() {
    ownership.applyTarget("A", 3, Set.of("a"));
    ownership.offload("A", 2);
    assertThat(ownership.mayAdmit("A", 3, "a")).isTrue();
  }

  @Test
  void sharedFileKeepsOtherIndexOwner() {
    ownership.applyTarget("A", 1, Set.of("common"));
    ownership.applyTarget("B", 1, Set.of("common"));
    assertThat(ownership.owners("common")).containsExactlyInAnyOrder("A", "B");
    ownership.offload("A", 2);
    ownership.release("A", 2);
    assertThat(ownership.owners("common")).containsExactly("B");
    assertThat(ownership.mayAdmit("B", 1, "common")).isTrue();
  }

  @Test
  void oldReleaseCannotClearNewOwnership() {
    ownership.applyTarget("A", 1, Set.of("a"));
    ownership.offload("A", 2);
    ownership.applyTarget("A", 3, Set.of("a"));
    ownership.release("A", 2);
    assertThat(ownership.mayAdmit("A", 3, "a")).isTrue();
    assertThat(ownership.owners("a")).containsExactly("A");
  }

  @Test
  void releaseKeepsHighWatermarkAndManagedKey() {
    ownership.applyTarget("A", 1, Set.of("a"));
    ownership.offload("A", 2);
    ownership.release("A", 2);
    ownership.applyTarget("A", 2, Set.of("a"));
    assertThat(ownership.mayAdmit("A", 2, "a")).isFalse();
    assertThat(ownership.isManagedKey("a")).isTrue();
  }

  @Test
  void targetAndOwnerSnapshotsCannotMutateLedger() {
    Set<String> keys = new HashSet<>(Set.of("a"));
    ownership.applyTarget("A", 1, keys);
    keys.clear();
    assertThat(ownership.mayAdmit("A", 1, "a")).isTrue();
    assertThatThrownBy(() -> ownership.owners("a").clear())
        .isInstanceOf(UnsupportedOperationException.class);
    assertThat(ownership.owners("a")).containsExactly("A");
  }
}
