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
public class TestShowIndexes extends ExtensionsTestBase {

  private static final int NUM_ROWS = 6;

  @BeforeAll
  static void registerVectorIndexType() {
    VectorIndexTypes.register();
  }

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", qualified("show_indexes_" + catalogName));
    sql("DROP TABLE IF EXISTS %s", qualified("show_indexes_absent_" + catalogName));
  }

  @TestTemplate
  public void testShowIndexesReturnsTheProcedureListing() {
    String table = qualified("show_indexes_" + catalogName);
    createIndexedTable(table);

    List<Object[]> shown = sql("SHOW INDEXES FROM %s", table);
    List<Object[]> listed = sql("CALL %s.system.list_indexes(table => '%s')", catalogName, table);

    // one implementation answers both spellings: the columns and the rows cannot drift apart
    assertThat(spark.sql("SHOW INDEXES FROM " + table).columns())
        .containsExactly(
            spark
                .sql(
                    String.format("CALL %s.system.list_indexes(table => '%s')", catalogName, table))
                .columns());
    assertThat(shown).hasSameSizeAs(listed).hasSize(1);
    assertThat(shown.get(0)).containsExactly(listed.get(0));
    assertThat(shown.get(0)[2]).isEqualTo("vector");
    assertThat(shown.get(0)[4]).isEqualTo("PUBLISHED");
    assertThat(((Number) shown.get(0)[6]).longValue()).isEqualTo(NUM_ROWS);
  }

  @TestTemplate
  public void testShowIndexesUnquotesIdentifiers() {
    String name = "show_indexes_" + catalogName;
    createIndexedTable(qualified(name));

    List<Object[]> plain = sql("SHOW INDEXES FROM %s", qualified(name));
    List<Object[]> quoted = sql("SHOW INDEXES FROM `%s`.`default`.`%s`", catalogName, name);

    // backticks quote the parts, they are not part of the catalog or the table name
    assertThat(quoted).hasSameSizeAs(plain);
    assertThat(quoted.get(0)).containsExactly(plain.get(0));
  }

  @TestTemplate
  public void testShowIndexesOnMissingTableFails() {
    // the listing procedure decides how a missing table is reported
    assertThatThrownBy(
            () -> sql("SHOW INDEXES FROM %s", qualified("show_indexes_absent_" + catalogName)))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("show_indexes_absent");
  }

  private String qualified(String name) {
    return catalogName + ".default." + name;
  }

  private void createIndexedTable(String table) {
    sql("CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg", table);
    sql(
        "INSERT INTO %s VALUES (1, array(1.0, 0.0)), (2, array(0.9, 0.1)), (3, array(0.0, 1.0)), "
            + "(4, array(0.1, 0.95)), (5, array(-1.0, 0.0)), (6, array(0.0, -1.0))",
        table);
    sql(
        "CALL %s.system.build_vector_index(table => '%s', column => 'embedding', dimension => 2, metric => 'cosine')",
        catalogName, table);
  }
}
