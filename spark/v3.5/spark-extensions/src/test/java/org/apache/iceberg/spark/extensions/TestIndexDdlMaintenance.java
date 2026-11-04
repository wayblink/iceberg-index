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

import java.util.List;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.index.vector.VectorIndexTypes;
import org.apache.spark.sql.AnalysisException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestIndexDdlMaintenance extends ExtensionsTestBase {

  @BeforeAll
  static void registerVectorIndexType() {
    VectorIndexTypes.register();
  }

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", qualified("ddl_refresh_" + catalogName));
    sql("DROP TABLE IF EXISTS %s", qualified("ddl_drop_" + catalogName));
    sql("DROP TABLE IF EXISTS %s", qualified("ddl_ambiguous_" + catalogName));
    sql("DROP TABLE IF EXISTS %s", qualified("ddl_uuid_" + catalogName));
    sql("DROP TABLE IF EXISTS %s", qualified("ddl_uuid_gone_" + catalogName));
    sql("DROP TABLE IF EXISTS %s", qualified("ddl_quoted_" + catalogName));
  }

  @TestTemplate
  public void testRefreshIndexByNameCoversNewFiles() {
    String table = qualified("ddl_refresh_" + catalogName);
    createIndexedTable(table, "emb_flat");

    List<Object[]> before = sql("CALL %s.system.list_indexes(table => '%s')", catalogName, table);
    long entriesBefore = ((Number) before.get(0)[6]).longValue();
    long snapshotBefore = ((Number) before.get(0)[3]).longValue();
    long indexSnapshotBefore = ((Number) before.get(0)[9]).longValue();

    sql("INSERT INTO %s VALUES (7, array(0.5, 0.5)), (8, array(-0.5, -0.5))", table);
    sql("ALTER TABLE %s REFRESH INDEX emb_flat", table);

    List<Object[]> after = sql("CALL %s.system.list_indexes(table => '%s')", catalogName, table);
    assertThat(after).hasSize(1);
    assertThat(((Number) after.get(0)[6]).longValue()).isEqualTo(entriesBefore + 2);
    assertThat(((Number) after.get(0)[3]).longValue()).isNotEqualTo(snapshotBefore);
    assertThat(after.get(0)[8]).isEqualTo("emb_flat");
    // a refresh publishes a new index snapshot, which is what a run has to pin before it measures
    assertThat(((Number) after.get(0)[9]).longValue()).isNotEqualTo(indexSnapshotBefore);
  }

  @TestTemplate
  public void testDropIndexByNameRemovesIt() {
    String table = qualified("ddl_drop_" + catalogName);
    createIndexedTable(table, "emb_flat");

    sql("ALTER TABLE %s DROP INDEX emb_flat", table);

    List<Object[]> remaining =
        sql("CALL %s.system.list_indexes(table => '%s')", catalogName, table);
    assertThat(remaining).isEmpty();
  }

  @TestTemplate
  public void testAnIndexIsAddressedByItsUUIDAsWellAsByItsName() {
    String table = qualified("ddl_uuid_" + catalogName);
    createIndexedTable(table, "by_uuid");
    String uuid = indexUuid(table, "by_uuid");

    sql("INSERT INTO %s VALUES (7, array(0.5, 0.5)), (8, array(-0.5, -0.5))", table);
    sql("ALTER TABLE %s REFRESH INDEX `%s`", table, uuid);

    // the UUID names the same index the name does, and a refresh through it covered the new files
    List<Object[]> refreshed = sql("SHOW INDEXES FROM %s", table);
    assertThat(refreshed).hasSize(1);
    assertThat(((Number) refreshed.get(0)[6]).longValue()).isEqualTo(8L);

    sql("ALTER TABLE %s DROP INDEX `%s`", table, uuid);
    assertThat(sql("SHOW INDEXES FROM %s", table)).isEmpty();
  }

  @TestTemplate
  public void testTheUUIDOfADroppedTableAddressesNothing() {
    String table = qualified("ddl_uuid_gone_" + catalogName);
    createIndexedTable(table, "gone");
    String dropped = indexUuid(table, "gone");

    // the same name, a different table: the UUID belongs to the one that is gone
    sql("DROP TABLE %s", table);
    createIndexedTable(table, "gone");
    assertThat(indexUuid(table, "gone")).isNotEqualTo(dropped);

    assertThatThrownBy(() -> sql("ALTER TABLE %s DROP INDEX `%s`", table, dropped))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("Index " + dropped + " does not exist");
  }

  @TestTemplate
  public void testAQuotedTableNameAddressesTheSameIndex() {
    String name = "ddl_quoted_" + catalogName;
    String table = qualified(name);
    createIndexedTable(table, "quoted");
    String quoted = String.format("`%s`.`default`.`%s`", catalogName, name);

    sql("INSERT INTO %s VALUES (7, array(0.5, 0.5))", table);
    sql("ALTER TABLE %s REFRESH INDEX quoted", quoted);

    List<Object[]> refreshed = sql("SHOW INDEXES FROM %s", table);
    assertThat(refreshed).hasSize(1);
    assertThat(((Number) refreshed.get(0)[6]).longValue()).isEqualTo(7L);

    // backticks quote the parts of the name, they are not part of it
    sql("ALTER TABLE %s DROP INDEX quoted", quoted);
    assertThat(sql("SHOW INDEXES FROM %s", table)).isEmpty();
  }

  @TestTemplate
  public void testUnknownOrAmbiguousNamesFailWhileAnalyzing() {
    String table = qualified("ddl_ambiguous_" + catalogName);
    sql("CREATE TABLE %s (id bigint, a array<double>, b array<double>) USING iceberg", table);
    sql("INSERT INTO %s VALUES (1, array(1.0, 0.0), array(0.0, 1.0))", table);
    sql(
        "ALTER TABLE %s CREATE INDEX shared USING vector (a) WITH (dimension = 2, metric = 'cosine')",
        table);
    sql(
        "ALTER TABLE %s CREATE INDEX shared USING vector (b) WITH (dimension = 2, metric = 'cosine')",
        table);

    assertThatThrownBy(() -> sql("ALTER TABLE %s DROP INDEX missing_one", table))
        .isInstanceOf(org.apache.spark.sql.AnalysisException.class)
        .hasMessageContaining("Index missing_one does not exist");

    // the name is per column, so the same name on two columns cannot be addressed by name alone
    assertThatThrownBy(() -> sql("ALTER TABLE %s DROP INDEX shared", table))
        .isInstanceOf(org.apache.spark.sql.AnalysisException.class)
        .hasMessageContaining("ambiguous");
  }

  @TestTemplate
  public void testAnUnnamedTextIndexIsNotAddressedByName() {
    String table = qualified("ddl_drop_" + catalogName);
    sql("CREATE TABLE %s (id bigint, body string) USING iceberg", table);
    sql("INSERT INTO %s VALUES (1, 'error 1064')", table);
    sql("ALTER TABLE %s CREATE INDEX USING fts (body)", table);

    // a text index has no name yet: saying so is more useful than pretending the name is wrong
    assertThatThrownBy(() -> sql("ALTER TABLE %s REFRESH INDEX body", table))
        .isInstanceOf(org.apache.spark.sql.AnalysisException.class)
        .hasMessageContaining("unnamed index on body");
  }

  private String indexUuid(String table, String indexName) {
    List<Object[]> shown = sql("SHOW INDEXES FROM %s", table);
    assertThat(shown).anySatisfy(row -> assertThat(row[8]).isEqualTo(indexName));
    return (String)
        shown.stream()
            .filter(row -> indexName.equals(row[8]))
            .findFirst()
            .orElseThrow(() -> new AssertionError("no index " + indexName + " in " + table))[0];
  }

  private String qualified(String name) {
    return catalogName + ".default." + name;
  }

  private void createIndexedTable(String table, String indexName) {
    sql("CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg", table);
    sql(
        "INSERT INTO %s VALUES (1, array(1.0, 0.0)), (2, array(0.9, 0.1)), (3, array(0.0, 1.0)), "
            + "(4, array(0.1, 0.95)), (5, array(-1.0, 0.0)), (6, array(0.0, -1.0))",
        table);
    sql(
        "ALTER TABLE %s CREATE INDEX %s USING vector (embedding) "
            + "WITH (dimension = 2, metric = 'cosine', structure = 'FLAT')",
        table, indexName);
  }
}
