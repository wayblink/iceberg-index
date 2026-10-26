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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.iceberg.index.vector.VectorIndexTypes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

public class TestIndexResidencyProcedures extends ExtensionsTestBase {
  private static final String TABLE_NAME = "test_index_residency";

  @BeforeAll
  static void registerVectorIndexType() {
    VectorIndexTypes.register();
  }

  @BeforeEach
  public void createIndexedTable() {
    sql("CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg", tableName);
    sql(
        "INSERT INTO %s VALUES (1, array(1.0, 0.0)), (2, array(0.0, 1.0))",
        tableName);
    sql(
        "CALL %s.system.build_vector_index(table => '%s', column => 'embedding', dimension => 2, metric => 'cosine')",
        catalogName, tableName);
  }

  @AfterEach
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void loadWithoutThePluginNamesThePlugin() {
    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.load_index(table => '%s', index => '%s')",
                    catalogName, tableName, indexUuid()))
        .hasMessageContaining("spark.plugins");
  }

  @TestTemplate
  public void offloadWithoutThePluginNamesThePlugin() {
    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.offload_index(table => '%s', index => '%s')",
                    catalogName, tableName, indexUuid()))
        .hasMessageContaining("spark.plugins");
  }

  @TestTemplate
  public void anUnknownIndexIsRejectedBeforeThePluginIsNeeded() {
    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.load_index(table => '%s', index => 'missing')",
                    catalogName, tableName))
        .hasMessageContaining("No index named missing");
  }

  @TestTemplate
  public void aRejectedCallLeavesTheIndexUsable() {
    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.load_index(table => '%s', index => '%s')",
                    catalogName, tableName, indexUuid()))
        .hasMessageContaining("spark.plugins");

    List<Object[]> rows =
        sql(
            "CALL %s.system.list_indexes(table => '%s')",
            catalogName, tableName);
    org.assertj.core.api.Assertions.assertThat(rows).hasSize(1);
  }

  private String indexUuid() {
    List<Object[]> rows =
        sql("CALL %s.system.list_indexes(table => '%s')", catalogName, tableName);
    return rows.get(0)[0].toString();
  }
}
