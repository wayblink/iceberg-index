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
 * Tests the automatic rewrite of a text search into a text index lookup, in both forms: the ranking
 * form, {@code ORDER BY text_score(...) DESC LIMIT k}, and the filter form, {@code WHERE
 * match(...)}.
 *
 * <p>A text search is only answered from an index snapshot bound to exactly the snapshot being
 * read, because a BM25 score depends on corpus statistics, so an index that is behind means
 * scanning. Every test uses a table of its own, because an index belongs to a column and a table.
 */
class TestRewriteTextSearch {

  private static final String COLUMNS = "(id bigint, body string) USING iceberg";
  private static final String DOCUMENTS =
      "(1, 'the quick brown fox jumps over the lazy dog'), "
          + "(2, 'the fox runs through the forest'), "
          + "(3, 'a dog sleeps in the sun'), "
          + "(4, 'the forest is quiet and the fox is quick')";

  private static SparkSession spark;

  @BeforeAll
  static void startSpark(@TempDir Path warehouse) {
    TextIndexTypes.register();
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("rewrite-text-search")
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
  public void rewritesARankedTextSearchIntoAnIndexLookup() {
    String table = tableWithIndex("docs_ranked");

    assertThat(planOf(table, ranking(table))).contains("LocalRelation");

    // the rows and their order are the ones the table-valued function returns, which searches the
    // same index through another path
    assertThat(rankingRows(table)).isEqualTo(functionRows(table, 2));
  }

  @Test
  public void rewritesAFilteredTextSearchIntoAnIndexLookup() {
    String table = tableWithIndex("docs_matched");

    assertThat(planOf(table, filtering(table))).contains("LocalRelation");

    // every row the function returns as a match is in the result, and nothing else
    assertThat(filteringRows(table)).containsExactlyInAnyOrderElementsOf(functionRows(table, 100));
  }

  @Test
  public void saysSoWhenThereIsNoIndex() {
    String table = table("docs_no_text_index");
    assertThat(planOf(table, ranking(table))).doesNotContain("LocalRelation");

    // a score needs the statistics of an index, so the query says so instead of returning rows in
    // an arbitrary order
    assertThatThrownBy(() -> spark.sql(ranking(table)).collectAsList())
        .hasStackTraceContaining("text_score needs a text index");
  }

  @Test
  public void leavesThePlanAloneForAnIndexThatIsBehind() {
    String table = tableWithIndex("docs_text_behind");
    // a row added after the index was built leaves the corpus statistics behind
    spark.sql(String.format("INSERT INTO local.%s VALUES (5, 'the fox and the dog rest')", table));

    assertThat(planOf(table, ranking(table))).doesNotContain("LocalRelation");
  }

  @Test
  public void leavesThePlanAloneWhenTheQueryIsNotARankedTextSearch() {
    String table = tableWithIndex("docs_text_not_ranked");
    assertThat(planOf(table, String.format("SELECT id FROM local.%s WHERE id > 1", table)))
        .doesNotContain("LocalRelation");
  }

  /** Creates the table without an index. */
  private static String table(String name) {
    String table = "db." + name;
    spark.sql(String.format("CREATE TABLE local.%s %s", table, COLUMNS));
    spark.sql(String.format("INSERT INTO local.%s VALUES %s", table, DOCUMENTS));
    return table;
  }

  /** Creates the table and builds a text index over its body column. */
  private static String tableWithIndex(String name) {
    String table = table(name);
    spark.sql(
        String.format(
            "CALL local.system.build_text_index(table => '%s', column => 'body')", table));
    return table;
  }

  private static String ranking(String table) {
    return String.format(
        "SELECT id FROM local.%s ORDER BY text_score(body, 'the fox') DESC LIMIT 2", table);
  }

  private static String filtering(String table) {
    return String.format("SELECT id FROM local.%s WHERE match(body, 'the fox')", table);
  }

  private static List<Row> rankingRows(String table) {
    return spark.sql(ranking(table)).collectAsList();
  }

  private static List<Row> filteringRows(String table) {
    return spark.sql(filtering(table)).collectAsList();
  }

  /** The ids the table-valued function returns for the same query, best first. */
  private static List<Row> functionRows(String table, int limit) {
    return spark
        .sql(
            String.format(
                "SELECT CAST(get_json_object(row, '$.id') AS bigint) AS id FROM "
                    + "text_index_search('local.%s', 'body', 'the fox', %d) ORDER BY score DESC",
                table, limit))
        .collectAsList();
  }

  private static String planOf(String table, String query) {
    return spark.sql(String.format(query, table)).queryExecution().optimizedPlan().toString();
  }
}
