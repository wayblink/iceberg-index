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
  }

  @TestTemplate
  public void testRefreshIndexByNameCoversNewFiles() {
    String table = qualified("ddl_refresh_" + catalogName);
    createIndexedTable(table, "emb_flat");

    List<Object[]> before = sql("CALL %s.system.list_indexes(table => '%s')", catalogName, table);
    long entriesBefore = ((Number) before.get(0)[6]).longValue();
    long snapshotBefore = ((Number) before.get(0)[3]).longValue();

    sql("INSERT INTO %s VALUES (7, array(0.5, 0.5)), (8, array(-0.5, -0.5))", table);
    sql("ALTER TABLE %s REFRESH INDEX emb_flat", table);

    List<Object[]> after = sql("CALL %s.system.list_indexes(table => '%s')", catalogName, table);
    assertThat(after).hasSize(1);
    assertThat(((Number) after.get(0)[6]).longValue()).isEqualTo(entriesBefore + 2);
    assertThat(((Number) after.get(0)[3]).longValue()).isNotEqualTo(snapshotBefore);
    assertThat(after.get(0)[8]).isEqualTo("emb_flat");
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
