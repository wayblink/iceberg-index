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
package org.apache.iceberg.spark.extensions;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Table;
import org.apache.iceberg.index.SidecarIndexCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * What happens to an index when the table it belongs to is dropped and created again.
 *
 * <p>An index is stored in a sidecar directory beside the table's data rather than inside the
 * table's metadata, so dropping the table leaves it behind. A table created again at the same
 * location is a different table with a different uuid, and the indexes it finds on disk are not
 * its: reporting them as existing refuses to build the index it needs, and reading through them
 * would answer from rows that no longer exist.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class TestVectorIndexRecreation extends ExtensionsTestBase {

  private static final String TABLE = "vectors_recreated";

  @AfterEach
  public void dropTable() {
    sql("DROP TABLE IF EXISTS %s PURGE", tableName(TABLE));
  }

  @TestTemplate
  public void buildsAnIndexAfterTheTableIsDroppedAndCreatedAgain() {
    String location = createTableWithData(TABLE);
    List<Object[]> first = buildIndex(TABLE);
    assertThat(first).hasSize(1);
    String firstIndex = first.get(0)[0].toString();

    // dropping the table removes its metadata and data but not the sidecar the index lives in
    sql("DROP TABLE %s PURGE", tableName(TABLE));
    sql(
        "CREATE TABLE %s (id bigint, embedding array<float>) USING iceberg LOCATION '%s'",
        tableName(TABLE), location);
    sql("INSERT INTO %s VALUES (1, array(1.0, 0.0)), (2, array(0.0, 1.0))", tableName(TABLE));

    List<Object[]> second = buildIndex(TABLE);
    assertThat(second).hasSize(1);
    assertThat(second.get(0)[0].toString())
        .as("the index the new table built")
        .isNotEqualTo(firstIndex);
    assertThat(((Number) second.get(0)[3]).longValue()).as("entries indexed").isEqualTo(2L);

    // the table reports its own index, not the one the table before it left behind
    assertThat(listIndexes(TABLE)).containsExactly(second.get(0)[0].toString());
  }

  @TestTemplate
  public void forgetsTheIndexesOfTheTableThatWasThereBefore() {
    createTableWithData(TABLE);
    buildIndex(TABLE);

    Table table = validationCatalog.loadTable(tableIdentifier(TABLE));
    String tableUuid = String.valueOf(table.uuid());
    String registry = table.properties().get(SidecarIndexCatalog.REGISTRY_PROPERTY);
    assertThat(table.io().newInputFile(table.location() + "/indexes/" + registry).exists())
        .as("the sidecar is still on disk after the table is dropped")
        .isTrue();

    sql("DROP TABLE %s PURGE", tableName(TABLE));
    sql(
        "CREATE TABLE %s (id bigint, embedding array<float>) USING iceberg LOCATION '%s'",
        tableName(TABLE), table.location());
    sql("INSERT INTO %s VALUES (1, array(1.0, 0.0)), (2, array(0.0, 1.0))", tableName(TABLE));

    Table recreated = validationCatalog.loadTable(tableIdentifier(TABLE));
    assertThat(String.valueOf(recreated.uuid()))
        .as("a table created again at the same location is a different table")
        .isNotEqualTo(tableUuid);

    // nothing is listed, so nothing claims the column already has an index
    assertThat(listIndexes(TABLE)).isEmpty();
  }

  @TestTemplate
  public void dropsTheIndexAndItsFiles() {
    createTableWithData(TABLE);
    List<Object[]> built = buildIndex(TABLE);
    String indexUuid = built.get(0)[0].toString();

    Table table = validationCatalog.loadTable(tableIdentifier(TABLE));
    String indexMetadata = table.location() + "/indexes/" + indexUuid;

    List<Object[]> dropped =
        sql(
            "CALL %s.system.drop_vector_index(table => '%s', column => 'embedding')",
            catalogName, tableName(TABLE));
    assertThat(dropped).hasSize(1);
    assertThat(dropped.get(0)[0].toString()).isEqualTo(indexUuid);
    assertThat(dropped.get(0)[1]).isEqualTo(true);

    assertThat(listIndexes(TABLE)).isEmpty();
    // the metadata is what makes an index loadable, and purging removes it; the directory it lived
    // in is left because deleting a directory is not something a FileIO can do
    for (int slot = 0; slot < 2; slot++) {
      assertThat(
              table.io().newInputFile(indexMetadata + "/index-metadata-pointer-" + slot).exists())
          .as("the metadata pointer of the dropped index is gone")
          .isFalse();
    }

    // and the column can be indexed again, which is what the drop is for
    assertThat(buildIndex(TABLE)).hasSize(1);
  }

  private org.apache.iceberg.catalog.TableIdentifier tableIdentifier(String name) {
    return org.apache.iceberg.catalog.TableIdentifier.of("default", name);
  }

  /** Creates the table with two rows and returns its location. */
  private String createTableWithData(String name) {
    sql("CREATE TABLE %s (id bigint, embedding array<float>) USING iceberg", tableName(name));
    sql("INSERT INTO %s VALUES (1, array(1.0, 0.0)), (2, array(0.0, 1.0))", tableName(name));
    return validationCatalog.loadTable(tableIdentifier(name)).location();
  }

  private List<Object[]> buildIndex(String name) {
    return sql(
        "CALL %s.system.build_vector_index(table => '%s', column => 'embedding', dimension => 2, metric => 'cosine')",
        catalogName, tableName(name));
  }

  private List<String> listIndexes(String name) {
    List<String> uuids = Lists.newArrayList();
    for (Object[] row :
        sql("CALL %s.system.list_indexes(table => '%s')", catalogName, tableName(name))) {
      uuids.add(row[0].toString());
    }
    return uuids;
  }
}
