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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Table;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.SidecarIndexCatalog;
import org.apache.iceberg.index.spark.VectorIndexes;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.apache.iceberg.util.JsonUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * The generic index drop: it removes an index of any type so the column can be indexed again with
 * another layout, analyzer or parameters.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class TestDropIndexProcedure extends ExtensionsTestBase {
  private static final String TABLE = "drop_index";

  @AfterEach
  public void dropTable() {
    sql("DROP TABLE IF EXISTS %s PURGE", tableName(TABLE));
  }

  @TestTemplate
  public void dropsATextIndexAndItsFiles() {
    createTextTable();
    buildTextIndex();
    Table table = reload();
    IndexMetadata metadata = VectorIndexes.findAnyIndexOrNull(table, "body");
    assertThat(metadata).isNotNull();
    assertThat(metadata.indexType()).isEqualTo("text");
    String statistics = metadata.snapshots().get(0).statisticsLocation();
    assertThat(table.io().newInputFile(statistics).exists()).as("index files before").isTrue();

    List<Object[]> dropped = dropByColumn("");
    assertThat(dropped).hasSize(1);
    assertThat(dropped.get(0)[0].toString()).isEqualTo(metadata.indexUuid());
    assertThat(dropped.get(0)[1].toString()).isEqualTo("text");
    assertThat((Boolean) dropped.get(0)[2]).isTrue();

    assertThat(listIndexes()).isEmpty();
    assertThat(table.io().newInputFile(statistics).exists()).as("purged index files").isFalse();
  }

  @TestTemplate
  public void keepsTheFilesWhenPurgeIsFalse() {
    createTextTable();
    buildTextIndex();
    Table table = reload();
    IndexMetadata metadata = VectorIndexes.findAnyIndexOrNull(table, "body");
    String statistics = metadata.snapshots().get(0).statisticsLocation();

    List<Object[]> dropped = dropByColumn(", purge => false");
    assertThat((Boolean) dropped.get(0)[2]).isFalse();
    assertThat(listIndexes()).as("the table has no index").isEmpty();
    assertThat(table.io().newInputFile(statistics).exists())
        .as("files left for inspection")
        .isTrue();
  }

  @TestTemplate
  public void rebuildsTheIndexAfterTheDrop() {
    createTextTable();
    buildTextIndex();
    String first = VectorIndexes.findAnyIndexOrNull(reload(), "body").indexUuid();
    dropByColumn("");

    List<Object[]> rebuilt = buildTextIndex();
    assertThat(rebuilt).hasSize(1);
    assertThat(rebuilt.get(0)[0].toString()).as("a new index").isNotEqualTo(first);
    assertThat(listIndexes()).containsExactly(rebuilt.get(0)[0].toString());
  }

  @TestTemplate
  public void dropsAVectorIndexThroughTheSameProcedure() {
    sql("CREATE TABLE %s (id bigint, embedding array<float>) USING iceberg", tableName(TABLE));
    sql("INSERT INTO %s VALUES (1, array(1.0, 0.0)), (2, array(0.0, 1.0))", tableName(TABLE));
    List<Object[]> built =
        sql(
            "CALL %s.system.build_vector_index(table => '%s', column => 'embedding', "
                + "dimension => 2, metric => 'cosine')",
            catalogName, tableName(TABLE));

    List<Object[]> dropped =
        sql(
            "CALL %s.system.drop_index(table => '%s', column => 'embedding')",
            catalogName, tableName(TABLE));
    assertThat(dropped.get(0)[0].toString()).isEqualTo(built.get(0)[0].toString());
    assertThat(dropped.get(0)[1].toString())
        .as("the index type the drop reports")
        .isEqualTo("vector");
    assertThat(listIndexes()).isEmpty();
  }

  @TestTemplate
  public void failsWhenTheColumnHasNoIndex() {
    createTextTable();
    assertThatThrownBy(() -> dropByColumn(""))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("No index exists for column body");
  }

  @TestTemplate
  public void failsWhenTheColumnDoesNotExist() {
    createTextTable();
    buildTextIndex();
    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.drop_index(table => '%s', column => 'missing')",
                    catalogName, tableName(TABLE)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("does not exist in table");
  }

  @TestTemplate
  public void namesTheExistingIndexesWhenTheNameDoesNotMatch() {
    createTextTable();
    buildTextIndex();
    String uuid = VectorIndexes.findAnyIndexOrNull(reload(), "body").indexUuid();
    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.drop_index(table => '%s', column => 'body', index => 'missing')",
                    catalogName, tableName(TABLE)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("No index named missing")
        .hasMessageContaining(uuid);
  }

  @TestTemplate
  public void dropsANamedIndexByName() {
    sql("CREATE TABLE %s (id bigint, embedding array<float>) USING iceberg", tableName(TABLE));
    sql("INSERT INTO %s VALUES (1, array(1.0, 0.0)), (2, array(0.0, 1.0))", tableName(TABLE));
    List<Object[]> built =
        sql(
            "CALL %s.system.build_vector_index(table => '%s', column => 'embedding', "
                + "dimension => 2, metric => 'cosine', name => 'flat_bruteforce', "
                + "structure => 'FLAT')",
            catalogName, tableName(TABLE));

    List<Object[]> dropped =
        sql(
            "CALL %s.system.drop_index(table => '%s', column => 'embedding', "
                + "index => 'flat_bruteforce')",
            catalogName, tableName(TABLE));
    assertThat(dropped.get(0)[0].toString()).isEqualTo(built.get(0)[0].toString());
    assertThat(dropped.get(0)[1].toString()).isEqualTo("vector");
  }

  @TestTemplate
  public void dropsADamagedEntryByUuidAndRebuilds() {
    createTextTable();
    buildTextIndex();
    Table table = reload();
    String uuid = VectorIndexes.findAnyIndexOrNull(table, "body").indexUuid();
    deleteIndexMetadata(table, uuid);

    // one entry whose metadata is gone no longer fails the listing of the table
    assertThat(listIndexes()).isEmpty();

    // the column cannot resolve it because the listing skips it, so the UUID it was named by is
    // what removes it; that is the repair path for a registry the files disagree with
    List<Object[]> dropped =
        sql(
            "CALL %s.system.drop_index(table => '%s', column => 'body', index => '%s')",
            catalogName, tableName(TABLE), uuid);
    assertThat(dropped).hasSize(1);
    assertThat(dropped.get(0)[0].toString()).isEqualTo(uuid);
    assertThat(dropped.get(0)[1].toString())
        .as("a damaged entry has no readable index type")
        .isEqualTo("unknown");
    assertThat((Boolean) dropped.get(0)[2]).isTrue();

    // and the column can be indexed again once the registry is repaired
    List<Object[]> rebuilt = buildTextIndex();
    assertThat(rebuilt).hasSize(1);
    assertThat(listIndexes()).containsExactly(rebuilt.get(0)[0].toString());
  }

  /**
   * Deletes the metadata the registry names for one index, leaving the registration behind: this is
   * the damaged entry a registry and the files under it disagree about.
   */
  private static void deleteIndexMetadata(Table table, String indexUuid) {
    String registryFile = table.properties().get(SidecarIndexCatalog.REGISTRY_PROPERTY);
    assertThat(registryFile).as("the table names its registry").isNotNull();
    String indexes = table.location() + "/" + SidecarIndexCatalog.INDEXES_DIR;
    JsonNode registry;
    try (SeekableInputStream in =
        table.io().newInputFile(indexes + "/" + registryFile).newStream()) {
      registry =
          JsonUtil.parse(
              new String(ByteStreams.toByteArray(in), StandardCharsets.UTF_8), node -> node);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    String root = registry.get("metadata-files").get(indexUuid).asText();
    table
        .io()
        .deleteFile(
            indexes + "/" + indexUuid + "/" + SidecarIndexCatalog.METADATA_DIR + "/" + root);
  }

  private void createTextTable() {
    sql("CREATE TABLE %s (id bigint, body string) USING iceberg", tableName(TABLE));
    sql(
        "INSERT INTO %s VALUES "
            + "(1, 'iceberg table format for large datasets'), "
            + "(2, 'vector search over embeddings'), "
            + "(3, 'iceberg snapshot isolation and manifests')",
        tableName(TABLE));
  }

  private List<Object[]> buildTextIndex() {
    return sql(
        "CALL %s.system.build_text_index(table => '%s', column => 'body')",
        catalogName, tableName(TABLE));
  }

  /** Drops by column, with extra arguments appended to the call. */
  private List<Object[]> dropByColumn(String extraArguments) {
    return sql(
        "CALL %s.system.drop_index(table => '%s', column => 'body'%s)",
        catalogName, tableName(TABLE), extraArguments);
  }

  private List<String> listIndexes() {
    List<String> uuids = Lists.newArrayList();
    for (Object[] row :
        sql("CALL %s.system.list_indexes(table => '%s')", catalogName, tableName(TABLE))) {
      uuids.add(row[0].toString());
    }

    return uuids;
  }

  private Table reload() {
    return validationCatalog.loadTable(
        org.apache.iceberg.catalog.TableIdentifier.of("default", TABLE));
  }
}
