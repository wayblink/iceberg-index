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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.index.IndexSnapshot.Status;
import org.junit.jupiter.api.Test;

class TestIndexMetadataParser {

  @Test
  public void roundTripsVectorIndexMetadata() {
    IndexMetadata metadata =
        IndexMetadata.builder()
            .indexUuid("index-uuid")
            .tableUuid("table-uuid")
            .tableLocation("s3://bucket/table")
            .location("s3://bucket/table/indexes/index-uuid")
            .definition(
                VectorIndexDefinition.builder()
                    .fieldId(17)
                    .dimension(2)
                    .metric(VectorIndexDefinition.Metric.COSINE)
                    .property("model", "v1")
                    .build())
            .lastUpdatedMillis(100L)
            .property("owner", "team")
            .addSnapshot(
                IndexSnapshot.builder()
                    .snapshotId(1)
                    .sourceTableSnapshotId(10)
                    .timestampMillis(30)
                    .indexManifestList("s3://bucket/table/indexes/index-uuid/snapshots/iml-1.bin")
                    .build())
            .addSnapshot(
                IndexSnapshot.builder()
                    .snapshotId(2)
                    .sourceTableSnapshotId(20)
                    .parentIndexSnapshotId(1L)
                    .timestampMillis(40)
                    .status(Status.PUBLISHED)
                    .indexManifestList("s3://bucket/table/indexes/index-uuid/snapshots/iml-2.bin")
                    .property("build", "incremental")
                    .build())
            .build();

    IndexMetadata parsed = IndexMetadataParser.fromJson(IndexMetadataParser.toJson(metadata));
    assertThat(parsed).isEqualTo(metadata);
    assertThat(parsed.definition()).isInstanceOf(VectorIndexDefinition.class);
    assertThat(parsed.indexType()).isEqualTo("vector");
    assertThat(parsed.snapshotForSourceTableSnapshot(20).parentIndexSnapshotId()).isEqualTo(1L);
    assertThat(parsed.snapshotForSourceTableSnapshot(10).parentIndexSnapshotId()).isNull();
    assertThat(parsed.snapshotForSourceTableSnapshot(20).status()).isEqualTo(Status.PUBLISHED);
  }

  @Test
  public void failsOnUnknownIndexType() {
    String json =
        "{\"format-version\": 1, \"index-uuid\": \"i\", \"table-uuid\": \"t\","
            + " \"location\": \"l\", \"index-type\": \"unknown\", \"definition\": {},"
            + " \"snapshots\": []}";
    assertThatThrownBy(() -> IndexMetadataParser.fromJson(json))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unknown index type");
  }

  @Test
  public void rejectsParentOutsideIndex() {
    assertThatThrownBy(
            () ->
                IndexMetadata.builder()
                    .indexUuid("index-uuid")
                    .tableUuid("table-uuid")
                    .location("s3://bucket/table/indexes/index-uuid")
                    .definition(
                        VectorIndexDefinition.builder()
                            .fieldId(1)
                            .dimension(2)
                            .metric(VectorIndexDefinition.Metric.COSINE)
                            .build())
                    .addSnapshot(
                        IndexSnapshot.builder()
                            .snapshotId(1)
                            .sourceTableSnapshotId(10)
                            .parentIndexSnapshotId(99L)
                            .timestampMillis(30)
                            .indexManifestList(
                                "s3://bucket/table/indexes/index-uuid/snapshots/iml-1.bin")
                            .build())
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("not part of this index");
  }
}
