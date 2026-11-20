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
package org.apache.iceberg.index;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class TestIndexMetadata {

  @Test
  void replaceSnapshotReplacesTheBuildOfTheSameSourceInPlace() {
    IndexMetadata metadata =
        metadata(
            snapshot(1L, 10L, null, "/index/manifest-list-1"),
            snapshot(2L, 20L, 1L, "/index/manifest-list-2"));
    IndexSnapshot replacement = snapshot(3L, 20L, null, "/index/manifest-list-3");

    IndexMetadata replaced = metadata.replaceSnapshot(replacement);

    assertThat(replaced.snapshots()).extracting(IndexSnapshot::snapshotId).containsExactly(1L, 3L);
    assertThat(replaced.latestSnapshot()).isEqualTo(replacement);
    assertThat(replaced.snapshotForSourceTableSnapshot(20L)).isEqualTo(replacement);
    assertThat(replaced.snapshotForSourceTableSnapshot(10L).snapshotId()).isEqualTo(1L);
  }

  @Test
  void replaceSnapshotRepairsTheLineageOfAReplacementDerivedFromTheReplacedBuild() {
    IndexMetadata metadata =
        metadata(
            snapshot(1L, 10L, null, "/index/manifest-list-1"),
            snapshot(2L, 20L, 1L, "/index/manifest-list-2"));
    IndexSnapshot replacement = snapshot(3L, 20L, 2L, "/index/manifest-list-3");

    IndexMetadata replaced = metadata.replaceSnapshot(replacement);

    assertThat(replaced.snapshots()).hasSize(2);
    assertThat(replaced.latestSnapshot().snapshotId()).isEqualTo(3L);
    assertThat(replaced.latestSnapshot().parentIndexSnapshotId())
        .as("the replacement names the parent of the build it replaces")
        .isEqualTo(1L);
  }

  @Test
  void replaceSnapshotRepointsTheBuildsThatDescendFromTheReplacedOne() {
    IndexMetadata metadata =
        metadata(
            snapshot(1L, 10L, null, "/index/manifest-list-1"),
            snapshot(2L, 20L, 1L, "/index/manifest-list-2"),
            snapshot(3L, 30L, 2L, "/index/manifest-list-3"));
    // rebuilding the middle build: everything below it has to keep a parent it can name
    IndexSnapshot replacement = snapshot(4L, 20L, 1L, "/index/manifest-list-4");

    IndexMetadata replaced = metadata.replaceSnapshot(replacement);

    assertThat(replaced.snapshots()).hasSize(3);
    assertThat(replaced.snapshots())
        .extracting(IndexSnapshot::snapshotId)
        .containsExactly(1L, 4L, 3L);
    assertThat(replaced.snapshotForSourceTableSnapshot(20L).snapshotId()).isEqualTo(4L);
    assertThat(replaced.snapshotForSourceTableSnapshot(30L).parentIndexSnapshotId())
        .as("the build that descended from the replaced one now descends from its replacement")
        .isEqualTo(4L);
  }

  @Test
  void replaceSnapshotAppendsWhenNoBuildOfTheSourceExists() {
    IndexMetadata metadata = metadata(snapshot(1L, 10L, null, "/index/manifest-list-1"));
    IndexSnapshot appended = snapshot(2L, 20L, 1L, "/index/manifest-list-2");

    IndexMetadata replaced = metadata.replaceSnapshot(appended);

    assertThat(replaced.snapshots()).hasSize(2);
    assertThat(replaced.latestSnapshot()).isEqualTo(appended);
    assertThat(replaced.snapshotForSourceTableSnapshot(20L)).isEqualTo(appended);
  }

  private static IndexMetadata metadata(IndexSnapshot... snapshots) {
    IndexMetadata.Builder builder =
        IndexMetadata.builder()
            .indexUuid("index-uuid")
            .tableUuid("table-uuid")
            .location("/index")
            .definition(TestingIndexDefinition.of(4));
    for (IndexSnapshot snapshot : snapshots) {
      builder.addSnapshot(snapshot);
    }

    return builder.build();
  }

  private static IndexSnapshot snapshot(
      long snapshotId, long sourceTableSnapshotId, Long parent, String manifestList) {
    return IndexSnapshot.builder()
        .snapshotId(snapshotId)
        .sourceTableSnapshotId(sourceTableSnapshotId)
        .parentIndexSnapshotId(parent)
        .timestampMillis(1L)
        .indexManifestList(manifestList)
        .build();
  }
}
