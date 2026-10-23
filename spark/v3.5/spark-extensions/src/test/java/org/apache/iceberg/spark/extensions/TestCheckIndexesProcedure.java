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
import org.apache.iceberg.Table;
import org.junit.jupiter.api.TestTemplate;

/**
 * The check that says whether an index's payload can still be read.
 *
 * <p>A payload names the kernel and version that wrote it, so a build whose kernel changed cannot
 * read an index written before that, and the fix is a rebuild. The check has to find those before a
 * query does, and has to say which index needs it.
 */
public class TestCheckIndexesProcedure extends ExtensionsTestBase {

  @TestTemplate
  public void aReadableIndexIsReportedAsSuch() {
    String table = tableName("check_ok_" + catalogName);
    createAndIndex(table);

    List<Object[]> rows = sql("CALL %s.system.check_indexes(table => '%s')", catalogName, table);
    assertThat(rows).hasSize(1);

    Object[] row = rows.get(0);
    assertThat((Boolean) row[8]).as("readable").isTrue();
    assertThat((String) row[5]).as("the kernel that wrote it").isNotNull();
    assertThat((Integer) row[6]).as("its payload version").isEqualTo((Integer) row[7]);
    assertThat((Boolean) row[10]).as("stale").isFalse();
    assertThat(row[11]).as("no suggestion for a readable index").isNull();
  }

  @TestTemplate
  public void anIndexBehindTheTableIsStaleButReadable() {
    String table = tableName("check_stale_" + catalogName);
    createAndIndex(table);
    sql("INSERT INTO %s VALUES (100, array(9.0d, 9.0d))", table);

    List<Object[]> rows = sql("CALL %s.system.check_indexes(table => '%s')", catalogName, table);
    assertThat(rows).hasSize(1);
    assertThat((Boolean) rows.get(0)[8]).as("readable").isTrue();
    assertThat((Boolean) rows.get(0)[10]).as("stale").isTrue();
  }

  @TestTemplate
  public void everyIndexOfTheColumnIsReported() {
    String table = tableName("check_two_" + catalogName);
    createAndIndex(table);
    sql(
        "CALL %s.system.build_vector_index(table => '%s', column => 'embedding', dimension => 2, "
            + "name => 'flat_second', structure => 'FLAT')",
        catalogName, table);

    List<Object[]> rows = sql("CALL %s.system.check_indexes(table => '%s')", catalogName, table);
    assertThat(rows).hasSize(2);
    List<Object> names =
        rows.stream()
            .map(row -> row[3] == null ? null : row[3].toString())
            .collect(java.util.stream.Collectors.toList());
    assertThat(names).contains("flat_second");
  }

  private String tableLocation(Table table) {
    return table.name();
  }

  private void createAndIndex(String table) {
    sql("CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg", table);
    sql(
        "INSERT INTO %s SELECT id, array(CAST(id AS double), CAST(10 - id AS double)) "
            + "FROM range(0, 10)",
        table);
    sql(
        "CALL %s.system.build_vector_index(table => '%s', column => 'embedding', dimension => 2)",
        catalogName, table);
  }
}
