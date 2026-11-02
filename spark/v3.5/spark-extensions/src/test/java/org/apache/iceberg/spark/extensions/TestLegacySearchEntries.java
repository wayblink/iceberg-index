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

import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.index.text.TextIndexTypes;
import org.apache.iceberg.index.vector.VectorIndexTypes;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * The entries a caller of the search features could already use, answered with what they answered
 * before.
 *
 * <p>The typed functions and the index DDL were added beside these, so a caller who wrote a {@code
 * CALL} or one of the three table functions does not have to change anything: the columns, their
 * order, the ordinal of a batch query and the arity of the scalar text functions are the contract
 * this class holds still.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class TestLegacySearchEntries extends ExtensionsTestBase {

  @BeforeAll
  static void registerIndexTypes() {
    VectorIndexTypes.register();
    TextIndexTypes.register();
  }

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", qualified("legacy_vector"));
    sql("DROP TABLE IF EXISTS %s", qualified("legacy_text"));
    sql("DROP TABLE IF EXISTS %s", qualified("legacy_text_b"));
  }

  @TestTemplate
  public void theProceduresAnswerWithTheColumnsTheyAlwaysDid() {
    String vector = createVectorTable("legacy_vector");
    String text = createTextTable("legacy_text");
    vectorIndex(vector, "embedding");
    textIndex(text);

    assertThat(
            callColumns(
                "CALL %s.system.vector_search(table => '%s', column => 'embedding', "
                    + "query => array(0.9d, 0.1d), k => 2)",
                vector))
        .containsExactly("data_file", "pos", "score", "row");

    assertThat(
            callColumns(
                "CALL %s.system.vector_search_batch(table => '%s', "
                    + "column => 'embedding', queries => array('0.9,0.1', '0.1,0.9'), k => 2)",
                vector))
        .containsExactly("query_id", "data_file", "pos", "score", "row");

    assertThat(
            callColumns(
                "CALL %s.system.text_search(table => '%s', column => 'body', "
                    + "query => 'error', k => 2)",
                text))
        .containsExactly("data_file", "pos", "score", "row");

    assertThat(
            callColumns(
                "CALL %s.system.text_search_batch(table => '%s', column => 'body', "
                    + "queries => array('error', 'timeout'), k => 2)",
                text))
        .containsExactly("query_id", "data_file", "pos", "score", "row");

    assertThat(callColumns("CALL %s.system.list_indexes(table => '%s')", vector))
        .containsExactly(
            "index_uuid",
            "column",
            "index_type",
            "source_snapshot_id",
            "status",
            "index_files",
            "entries",
            "definition",
            "index_name");

    assertThat(
            callColumns(
                "CALL %s.system.refresh_vector_index(table => '%s', " + "column => 'embedding')",
                vector))
        .containsExactly("index_uuid", "source_snapshot_id", "index_files", "entries");

    assertThat(callColumns("CALL %s.system.index_build_status(table => '%s')", vector))
        .containsExactly(
            "table",
            "column",
            "index_uuid",
            "index_name",
            "index_type",
            "state",
            "started_at",
            "updated_at",
            "age_ms",
            "stale",
            "files_built",
            "files_total",
            "files_failed",
            "rows",
            "error");

    assertThat(callColumns("CALL %s.system.check_indexes(table => '%s')", vector))
        .containsExactly(
            "table",
            "column",
            "index_uuid",
            "index_name",
            "index_type",
            "writer",
            "writer_version",
            "reader_version",
            "readable",
            "source_snapshot_id",
            "stale",
            "uncovered_data_files",
            "suggestion");

    // the drop is last in this sweep because it is the only call that changes the table
    assertThat(
            callColumns(
                "CALL %s.system.drop_vector_index(table => '%s', " + "column => 'embedding')",
                vector))
        .containsExactly("index_uuid", "purged");
  }

  @TestTemplate
  public void theTableFunctionsAnswerWithTheColumnsTheyAlwaysDid() {
    String vector = createVectorTable("legacy_vector");
    String text = createTextTable("legacy_text");
    vectorIndex(vector, "embedding");
    textIndex(text);

    assertThat(
            columns(
                "SELECT * FROM vector_index_search('%s', 'embedding', " + "array(0.9d, 0.1d), 2)",
                vector))
        .containsExactly("data_file", "pos", "score", "row");

    assertThat(columns("SELECT * FROM text_index_search('%s', 'body', 'error', 2)", text))
        .containsExactly("data_file", "pos", "score", "row");

    // a batch call takes its arguments by position, in the order the procedure takes them by name,
    // and it reports which query a hit came from as the ordinal of that query
    Dataset<Row> batchAnswer =
        spark.sql(
            String.format(
                "SELECT * FROM vector_index_search_batch('%s', 'embedding', "
                    + "array('0.9,0.1', '0.1,0.9'), 2)",
                vector));
    assertThat(Arrays.asList(batchAnswer.columns()))
        .containsExactly("query_id", "data_file", "pos", "score", "row");
    assertThat(batchAnswer.collectAsList().stream().map(row -> row.getInt(0)).distinct())
        .containsExactlyInAnyOrder(0, 1);
  }

  @TestTemplate
  public void theTextFunctionsRefuseAnArgumentTheyCannotApply() {
    String text = createTextTable("legacy_text");
    textIndex(text);

    // an option the index cannot answer must fail rather than be dropped: a caller who wrote it
    // would otherwise believe it was applied. How the failure is wrapped is a separate matter
    // (#2893), so only the words are pinned here.
    assertThatThrownBy(
            () -> run("SELECT id FROM %s WHERE match(body, 'error', 'operator=AND')", text))
        .hasMessageContaining("options are not supported");

    assertThatThrownBy(() -> run("SELECT text_score(body, 'error', 'operator=AND') FROM %s", text))
        .hasMessageContaining("options are not supported");

    // two arguments are the whole call, in the shape the rewrite answers
    assertThat(run("SELECT id FROM %s WHERE match(body, 'error')", text)).isNotEmpty();
  }

  @TestTemplate
  public void theTextScoreIsStillAProjectionOfTheTable() {
    String text = createTextTable("legacy_text");
    textIndex(text);

    // the score was a projection before there was a table function, and it still is: it can be
    // selected, aliased and ordered by, and the row that matches the term comes first
    List<Row> scored =
        run(
            "SELECT id, text_score(body, 'error') AS s FROM %s "
                + "ORDER BY text_score(body, 'error') DESC LIMIT 2",
            text);
    assertThat(scored).isNotEmpty();
    assertThat(scored.get(0).getLong(0)).isEqualTo(1L);
    assertThat(scored.get(0).getDouble(1)).isGreaterThan(0.0d);
  }

  /** Runs a procedure call and returns the columns it answered with, in order. */
  private List<String> callColumns(String format, String table) {
    return columns(format, catalogName, table);
  }

  /** Runs the statement and returns the columns it answered with, in order. */
  private List<String> columns(String format, Object... args) {
    Dataset<Row> answer = spark.sql(String.format(format, args));
    answer.collectAsList();
    return Arrays.asList(answer.columns());
  }

  private List<Row> run(String format, Object... args) {
    return spark.sql(String.format(format, args)).collectAsList();
  }

  private String createVectorTable(String name) {
    String table = qualified(name);
    sql("CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg", table);
    sql(
        "INSERT INTO %s VALUES (1, array(1.0, 0.0)), (2, array(0.9, 0.1)), (3, array(0.0, 1.0))",
        table);
    return table;
  }

  private String createTextTable(String name) {
    String table = qualified(name);
    sql("CREATE TABLE %s (id bigint, body string) USING iceberg", table);
    sql("INSERT INTO %s VALUES (1, 'error 1064'), (2, 'timeout after 30s')", table);
    return table;
  }

  private void vectorIndex(String table, String column) {
    sql(
        "CALL %s.system.build_vector_index(table => '%s', column => '%s', dimension => 2, "
            + "mode => 'exact')",
        catalogName, table, column);
  }

  private void textIndex(String table) {
    sql("CALL %s.system.build_text_index(table => '%s', column => 'body')", catalogName, table);
  }

  private String qualified(String name) {
    return catalogName + ".default." + name + "_" + catalogName;
  }
}
