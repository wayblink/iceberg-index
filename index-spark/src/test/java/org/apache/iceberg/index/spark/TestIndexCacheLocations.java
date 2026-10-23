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

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class TestIndexCacheLocations {

  private static final String[] FILES = {"s3://bucket/a.puffin", "s3://bucket/b.puffin"};

  private static IndexCacheLocations.Report report(
      String executor, long epoch, long seq, String... keys) {
    return new IndexCacheLocations.Report(
        executor,
        executor + ".host",
        epoch,
        seq,
        IndexCacheLocations.sortedHashes(Arrays.asList(keys)),
        0,
        0,
        0,
        0,
        keys.length * 100L,
        keys.length,
        System.currentTimeMillis());
  }

  @Test
  void keepsTheNewestSequenceOfOneExecutor() {
    IndexCacheLocations table = new IndexCacheLocations();
    table.merge(report("1", 7, 10, FILES[0], FILES[1]));
    table.merge(report("1", 7, 9, FILES[0]));

    assertThat(table.holders(FILES[1], 2)).hasSize(1);
  }

  @Test
  void aRestartedExecutorReplacesItsViewEntirely() {
    IndexCacheLocations table = new IndexCacheLocations();
    table.merge(report("1", 7, 10, FILES[0], FILES[1]));
    // same id, new JVM: its empty cache is the truth, not the old view
    table.merge(report("1", 8, 1));

    assertThat(table.holders(FILES[0], 2)).isEmpty();
    assertThat(table.size()).isEqualTo(1);
  }

  @Test
  void findsEveryExecutorThatHoldsAFile() {
    IndexCacheLocations table = new IndexCacheLocations();
    table.merge(report("1", 1, 1, FILES[0]));
    table.merge(report("2", 1, 1, FILES[0], FILES[1]));

    assertThat(table.holders(FILES[0], 2)).hasSize(2);
    assertThat(table.holders(FILES[1], 2)).hasSize(1);
  }

  @Test
  void respectsTheHolderLimit() {
    IndexCacheLocations table = new IndexCacheLocations();
    table.merge(report("1", 1, 1, FILES[0]));
    table.merge(report("2", 1, 1, FILES[0]));
    table.merge(report("3", 1, 1, FILES[0]));

    assertThat(table.holders(FILES[0], 2)).hasSize(2);
  }

  @Test
  void forgetsAnExecutorThatStoppedReporting() throws Exception {
    IndexCacheLocations table = new IndexCacheLocations();
    table.merge(report("1", 1, 1, FILES[0]));
    Thread.sleep(20);
    table.prune(1);

    assertThat(table.holders(FILES[0], 2)).isEmpty();
  }

  @Test
  void reportsHowMuchOfTheQueryIsCovered() {
    IndexCacheLocations table = new IndexCacheLocations();
    table.merge(report("1", 1, 1, FILES[0]));

    List<String> asked = Arrays.asList(FILES[0], FILES[1]);
    assertThat(table.coveredCount(asked)).isEqualTo(1);
    assertThat(table.summary(asked).coverage()).isEqualTo(0.5);
    assertThat(table.summary(asked).executors()).isEqualTo(1);
  }
}
