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
import static org.assertj.core.api.Assertions.within;

import java.nio.file.Path;
import java.util.List;
import org.apache.iceberg.index.text.TextIndexTypes;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests the search table function and the text score: a ranked list of the table's own rows, with
 * the score that ranked them.
 *
 * <p>The score is the interesting part. It is computed from corpus statistics the index holds, so a
 * projected score that is not bound to those statistics either refuses to compute or reports a
 * number read out of the wrong column; both have happened here before.
 */
class TestTextSearchTableFunction {

  private static final String COLUMNS = "(id bigint, body string) USING iceberg";
  private static final String ROWS =
      "(1, 'error 1064 on connect'), "
          + "(2, 'error 1215 deadlock'), "
          + "(3, 'warning only'), "
          + "(4, 'error 1064 again')";

  private static SparkSession spark;

  @BeforeAll
  static void startSpark(@TempDir Path warehouse) {
    TextIndexTypes.register();
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("text-search-table-function")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "4")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", warehouse.toUri().toString())
            .getOrCreate();
  }

  @AfterAll
  static void stopSpark() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  public void returnsTheRankedRowsAndTheirScore() {
    String table = tableWithIndex("ts_ranked");

    List<Row> hits =
        spark
            .sql(
                String.format(
                    "SELECT id, body, _score FROM search("
                        + "table => 'local.%s', query => 'error 1064', "
                        + "search_columns => array('body'), limit => 2)",
                    table))
            .collectAsList();

    assertThat(hits).hasSize(2);
    // the two rows that carry the term rank above the ones that do not
    assertThat(hits).extracting(row -> row.getLong(0)).doesNotContain(3L);
    assertThat(hits.get(0).getDouble(2)).isGreaterThanOrEqualTo(hits.get(1).getDouble(2));
    assertThat(hits.get(0).getDouble(2)).isPositive();

    // and the score is the one the scalar function reports for that row in a ranked query
    long best = hits.get(0).getLong(0);
    List<Row> ranked =
        spark
            .sql(
                String.format(
                    "SELECT id, text_score(body, 'error 1064') AS s FROM local.%s "
                        + "ORDER BY text_score(body, 'error 1064') DESC LIMIT 2",
                    table))
            .collectAsList();
    assertThat(ranked)
        .extracting(row -> row.getLong(0))
        .containsExactly(hits.get(0).getLong(0), hits.get(1).getLong(0));
    assertThat(hits.get(0).getDouble(2)).isCloseTo(ranked.get(0).getDouble(1), within(1e-9d));
    assertThat(best).isEqualTo(ranked.get(0).getLong(0));
  }

  @Test
  public void projectsOnlyTheColumnsThatWereAskedFor() {
    String table = tableWithIndex("ts_projection");

    Row row =
        spark
            .sql(
                String.format(
                    "SELECT * FROM search(table => 'local.%s', query => 'error 1064', "
                        + "search_columns => array('body'), limit => 1, columns => array('id'))",
                    table))
            .head();

    assertThat(row.size()).isEqualTo(2);
  }

  @Test
  public void ranksByTheNameTheScoreWasGiven() {
    String table = tableWithIndex("ts_alias");

    // naming the score and ordering by the name is how a caller reads it; the name has to resolve
    // to
    // the score of this query and not to anything else the table may call the same
    List<Row> byName =
        spark
            .sql(
                String.format(
                    "SELECT id, text_score(body, 'error 1064') AS _score FROM local.%s "
                        + "ORDER BY _score DESC LIMIT 2",
                    table))
            .collectAsList();

    List<Row> byExpression =
        spark
            .sql(
                String.format(
                    "SELECT id, text_score(body, 'error 1064') AS _score FROM local.%s "
                        + "ORDER BY text_score(body, 'error 1064') DESC LIMIT 2",
                    table))
            .collectAsList();

    assertThat(byName)
        .extracting(row -> row.getLong(0))
        .containsExactly(byExpression.get(0).getLong(0), byExpression.get(1).getLong(0));
    assertThat(byName.get(0).getDouble(1))
        .isCloseTo(byExpression.get(0).getDouble(1), within(1e-9d));
    assertThat(byName.get(0).getDouble(1)).isPositive();
  }

  @Test
  public void refusesWhatItCannotAnswer() {
    String table = tableWithIndex("ts_refusals");

    // the searched column has to be named: a text index belongs to one column
    assertThatThrownBy(
            () ->
                spark
                    .sql(
                        String.format(
                            "SELECT * FROM search(table => 'local.%s', query => 'error', limit => 1)",
                            table))
                    .collectAsList())
        .hasMessageContaining("search_columns");

    assertThatThrownBy(
            () ->
                spark
                    .sql(
                        String.format(
                            "SELECT * FROM search(table => 'local.%s', query => 'error', "
                                + "search_columns => array('body', 'id'), limit => 1)",
                            table))
                    .collectAsList())
        .hasMessageContaining("exactly one column");

    assertThatThrownBy(
            () ->
                spark
                    .sql(
                        String.format(
                            "SELECT * FROM search(table => 'local.%s', query => 'error', "
                                + "search_columns => array('body'), limit => 1, prefilter => true)",
                            table))
                    .collectAsList())
        .hasMessageContaining("prefilter");
  }

  @Test
  public void refusesAFilterLikeOptionOnTheScalarFunctions() {
    String table = tableWithIndex("ts_arity");

    // the options of a text query are not implemented; an extra argument must fail rather than be
    // dropped, or a caller would believe the operator was applied
    assertThatThrownBy(
            () ->
                spark
                    .sql(
                        String.format(
                            "SELECT id FROM local.%s WHERE match(body, 'error', 'operator=AND')",
                            table))
                    .collectAsList())
        .hasMessageContaining("options are not supported");
  }

  private static String tableWithIndex(String name) {
    String table = "db." + name;
    spark.sql(String.format("CREATE TABLE local.%s %s", table, COLUMNS));
    spark.sql(String.format("INSERT INTO local.%s VALUES %s", table, ROWS));
    spark.sql(
        String.format(
            "CALL local.system.build_text_index(table => '%s', column => 'body')", table));
    return table;
  }
}
