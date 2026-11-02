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
public class TestCreateIndexDdl extends ExtensionsTestBase {

  @BeforeAll
  static void registerVectorIndexType() {
    VectorIndexTypes.register();
  }

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", qualified("create_index_" + catalogName));
    sql("DROP TABLE IF EXISTS %s", qualified("create_index_text_" + catalogName));
    sql("DROP TABLE IF EXISTS %s", qualified("create_index_missing_" + catalogName));
    sql("DROP TABLE IF EXISTS %s", qualified("create_index_nested_" + catalogName));
  }

  @TestTemplate
  public void testCreateVectorIndexBuildsTheNamedIndex() {
    String table = qualified("create_index_" + catalogName);
    createVectorTable(table);

    sql(
        "ALTER TABLE %s CREATE INDEX emb_flat USING vector (embedding) "
            + "WITH (dimension = 2, metric = 'cosine', structure = 'FLAT')",
        table);

    List<Object[]> listed = sql("CALL %s.system.list_indexes(table => '%s')", catalogName, table);
    assertThat(listed).hasSize(1);
    assertThat(listed.get(0)[1]).isEqualTo("embedding");
    assertThat(listed.get(0)[2]).isEqualTo("vector");
    assertThat(listed.get(0)[4]).isEqualTo("PUBLISHED");
    assertThat(((Number) listed.get(0)[6]).longValue()).isEqualTo(6L);
    assertThat(listed.get(0)[8]).isEqualTo("emb_flat");
    assertThat(listed.get(0)[7].toString()).contains("\"metric\":\"COSINE\"");
  }

  @TestTemplate
  public void testCreateIndexKeepsOptionValueTypes() {
    String table = qualified("create_index_" + catalogName);
    createVectorTable(table);

    // a number stays a number and a string stays a string: the keys are the procedure's own
    // parameters, so a build that a CALL can run is a build this statement can run
    sql(
        "ALTER TABLE %s CREATE INDEX emb_ivf USING vector (embedding) "
            + "WITH (dimension = 2, structure = 'IVF', options = MAP('nlist', '4', 'nprobe', '4'))",
        table);

    List<Object[]> listed = sql("CALL %s.system.list_indexes(table => '%s')", catalogName, table);
    assertThat(listed).hasSize(1);
    assertThat(listed.get(0)[7].toString()).contains("\"structure\":\"IVF\"");
    assertThat(((Number) listed.get(0)[6]).longValue()).isEqualTo(6L);
  }

  @TestTemplate
  public void testCreateTextIndexWithoutAName() {
    String table = qualified("create_index_text_" + catalogName);
    sql("CREATE TABLE %s (id bigint, body string) USING iceberg", table);
    sql("INSERT INTO %s VALUES (1, 'error 1064'), (2, 'error 1215'), (3, 'warning')", table);

    // a text index has no name yet, and the statement does not invent one to look complete
    sql("ALTER TABLE %s CREATE INDEX USING fts (body)", table);

    List<Object[]> listed = sql("CALL %s.system.list_indexes(table => '%s')", catalogName, table);
    assertThat(listed).hasSize(1);
    assertThat(listed.get(0)[1]).isEqualTo("body");
    assertThat(listed.get(0)[2]).isEqualTo("text");
    assertThat(listed.get(0)[8]).isNull();
  }

  @TestTemplate
  public void testCreateIndexRejectsWhatItCannotBuild() {
    String table = qualified("create_index_" + catalogName);
    createVectorTable(table);

    assertThatThrownBy(() -> sql("ALTER TABLE %s CREATE INDEX i USING btree (id)", table))
        .isInstanceOf(org.apache.spark.sql.AnalysisException.class)
        .hasMessageContaining("Unsupported index method: btree");

    assertThatThrownBy(() -> sql("ALTER TABLE %s CREATE INDEX i USING vector (embedding)", table))
        .isInstanceOf(Exception.class)
        .hasMessageContaining("dimension");

    assertThatThrownBy(
            () ->
                sql(
                    "ALTER TABLE %s CREATE INDEX i USING vector (embedding) "
                        + "WITH (dimensions = 2)",
                    table))
        .isInstanceOf(Exception.class)
        .hasMessageContaining("dimensions");

    assertThatThrownBy(
            () ->
                sql(
                    "ALTER TABLE %s CREATE INDEX i USING vector (embedding) "
                        + "WITH (dimension = 2, dimension = 3)",
                    table))
        .isInstanceOf(org.apache.spark.sql.AnalysisException.class)
        .hasMessageContaining("Duplicate index option: dimension");
  }

  @TestTemplate
  public void testCreateIndexNamesTheColumnItCannotFind() {
    String table = qualified("create_index_missing_" + catalogName);
    createVectorTable(table);

    assertThatThrownBy(
            () ->
                sql(
                    "ALTER TABLE %s CREATE INDEX i USING vector (missing_col) "
                        + "WITH (dimension = 2)",
                    table))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Column missing_col does not exist");
  }

  @TestTemplate
  public void testAnIndexColumnIsATopLevelColumn() {
    String table = qualified("create_index_nested_" + catalogName);
    sql("CREATE TABLE %s (id bigint, a struct<b: array<double>>) USING iceberg", table);
    sql("INSERT INTO %s SELECT 1, named_struct('b', array(1.0d, 0.0d))", table);

    // an index is built on a column, and a dotted path is not one: the refusal names the path
    // rather than building an index on something else
    assertThatThrownBy(
            () ->
                sql("ALTER TABLE %s CREATE INDEX i USING vector (a.b) WITH (dimension = 2)", table))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("a.b does not exist");
  }

  @TestTemplate
  public void testCreateIndexRejectsANamedTextIndex() {
    String table = qualified("create_index_text_" + catalogName);
    sql("CREATE TABLE %s (id bigint, body string) USING iceberg", table);

    assertThatThrownBy(() -> sql("ALTER TABLE %s CREATE INDEX body_fts USING fts (body)", table))
        .isInstanceOf(org.apache.spark.sql.AnalysisException.class)
        .hasMessageContaining("text index cannot be named yet");
  }

  private String qualified(String name) {
    return catalogName + ".default." + name;
  }

  private void createVectorTable(String table) {
    sql("CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg", table);
    sql(
        "INSERT INTO %s VALUES (1, array(1.0, 0.0)), (2, array(0.9, 0.1)), (3, array(0.0, 1.0)), "
            + "(4, array(0.1, 0.95)), (5, array(-1.0, 0.0)), (6, array(0.0, -1.0))",
        table);
  }
}
