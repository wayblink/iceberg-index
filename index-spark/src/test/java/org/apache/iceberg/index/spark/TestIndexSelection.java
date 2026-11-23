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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.index.CreateIndexRequest;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.SidecarIndexCatalog;
import org.apache.iceberg.index.vector.VectorIndexDefinition;
import org.apache.iceberg.index.vector.VectorIndexTypes;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A name selects an index in SQL and a UUID is its identity, so a name that is another index's UUID
 * makes one token mean two indexes. The build is refused and an existing table that already carries
 * such a pair still reports the ambiguity instead of quietly picking one of them.
 */
class TestIndexSelection {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.required(
              2, "vec", Types.ListType.ofRequired(3, Types.FloatType.get())));

  private static final String TABLE_NAME = "selection_table";
  private static final String COLUMN = "vec";

  private static final VectorIndexDefinition DEFINITION =
      VectorIndexDefinition.builder()
          .fieldId(2)
          .dimension(2)
          .metric(VectorIndexDefinition.Metric.COSINE)
          .structure(VectorIndexDefinition.VectorStructure.FLAT)
          .build();

  @TempDir private Path tempDir;

  private TestTables.TestTable table;
  private SidecarIndexCatalog catalog;

  @BeforeAll
  static void registerIndexType() {
    VectorIndexTypes.register();
  }

  @BeforeEach
  void before() {
    TestTables.clearTables();
    this.table =
        TestTables.create(
            tempDir.resolve("table").toFile(),
            TABLE_NAME,
            SCHEMA,
            PartitionSpec.unpartitioned(),
            SortOrder.unsorted(),
            2);
    this.catalog = new SidecarIndexCatalog(table.io(), table);
  }

  @Test
  void refusesABuildNamedAfterAnExistingIndexesUuid() {
    register("index-a", "alpha");

    assertThatThrownBy(
            () -> IndexSelection.checkCanBuild(table, COLUMN, "vector", TABLE_NAME, "index-a"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("is the UUID of the vector index")
        .hasMessageContaining("index-a");

    // a name no index uses is what building a second, named index over the column takes
    assertThatCode(() -> IndexSelection.checkCanBuild(table, COLUMN, "vector", TABLE_NAME, "beta"))
        .doesNotThrowAnyException();
  }

  @Test
  void resolvesAnIndexByNameOrUuid() {
    register("index-a", "alpha");

    assertThat(IndexSelection.resolve(table, COLUMN, "vector", "alpha").indexUuid())
        .isEqualTo("index-a");
    assertThat(IndexSelection.resolve(table, COLUMN, "vector", "index-a").indexUuid())
        .isEqualTo("index-a");
    assertThat(IndexSelection.resolveAny(table, COLUMN, "alpha").indexUuid()).isEqualTo("index-a");
    assertThat(IndexSelection.resolveAny(table, COLUMN, "index-a").indexUuid())
        .isEqualTo("index-a");
  }

  @Test
  void refusesASelectorThatIsOneIndexesNameAndAnothersUuid() {
    register("index-a", "alpha");
    // a table written before names were checked against UUIDs: index-b is named after index-a's
    // UUID, so the selector means both and resolution must not attribute it to whichever came first
    register("index-b", "index-a");

    assertThatThrownBy(() -> IndexSelection.resolve(table, COLUMN, "vector", "index-a"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("ambiguous")
        .hasMessageContaining("index-b");

    assertThatThrownBy(() -> IndexSelection.resolveAny(table, COLUMN, "index-a"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("ambiguous")
        .hasMessageContaining("index-b");

    // both indexes stay reachable through tokens that identify exactly one of them
    assertThat(IndexSelection.resolve(table, COLUMN, "vector", "alpha").indexUuid())
        .isEqualTo("index-a");
    assertThat(IndexSelection.resolve(table, COLUMN, "vector", "index-b").indexUuid())
        .isEqualTo("index-b");
  }

  private IndexMetadata register(String indexUuid, String name) {
    return catalog.createIndex(
        CreateIndexRequest.builder()
            .tableLocation(table.location())
            .tableUuid(table.uuid().toString())
            .indexUuid(indexUuid)
            .name(name)
            .definition(DEFINITION)
            .build());
  }
}
