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
  public void matchOfANullDocumentIsUnknownNotFalse() {
    String table = "db.docs_null_body";
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
    spark.sql(String.format("CREATE TABLE local.%s %s", table, COLUMNS));
    spark.sql(
        String.format(
            "INSERT INTO local.%s VALUES (1, 'the fox runs'), (2, NULL), (3, 'a quiet sun')",
            table));
    spark.sql(
        String.format(
            "CALL local.system.build_text_index(table => '%s', column => 'body')", table));

    // the null row is the only row this reads, so the expression is only asked about a null
    // document
    List<Row> nulls =
        spark
            .sql(String.format("SELECT match(body, 'fox') FROM local.%s WHERE body IS NULL", table))
            .collectAsList();
    assertThat(nulls).hasSize(1);
    // a null document holds no term, and SQL does not answer a comparison against it with false
    assertThat(nulls.get(0).isNullAt(0)).isTrue();

    // a filter still keeps only the row that holds the term
    assertThat(
            idsOf(
                spark
                    .sql(String.format("SELECT id FROM local.%s WHERE match(body, 'fox')", table))
                    .collectAsList()))
        .containsExactly(1L);
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

  @Test
  public void projectsTheScoreTheOrderingUsed() {
    String table = tableWithIndex("docs_score_projected");

    List<Row> rows =
        spark
            .sql(
                String.format(
                    "SELECT id, text_score(body, 'the fox') AS score FROM local.%s "
                        + "ORDER BY text_score(body, 'the fox') DESC LIMIT 2",
                    table))
            .collectAsList();
    assertThat(planOf(table, scoredRanking(table))).contains("LocalRelation");
    assertThat(rows).hasSize(2);

    // the function searches the same index, so it answers with the same documents and the same
    // scores: a projection that lands in the wrong column answers with something else entirely
    assertThat(idsOf(rows)).isEqualTo(idsOf(functionHits(table, 2)));
    assertThat(rows.get(0).getDouble(1))
        .as("the score of the best document")
        .isCloseTo(functionHits(table, 2).get(0).getDouble(1), within(1e-9));
  }

  @Test
  public void keepsTheColumnsOfTheProjectionInOrder() {
    String table = tableWithIndex("docs_score_reordered");

    // the score is asked for first and the document last, so a plan that answers with the scan's
    // columns in their own order puts the document where the score should be
    List<Row> rows =
        spark
            .sql(
                String.format(
                    "SELECT text_score(body, 'the fox') AS score, id, body FROM local.%s "
                        + "ORDER BY text_score(body, 'the fox') DESC LIMIT 1",
                    table))
            .collectAsList();
    assertThat(rows).hasSize(1);
    Row best = rows.get(0);
    List<Row> hits = functionHits(table, 1);
    assertThat(best.getDouble(0))
        .as("the score is a score")
        .isCloseTo(hits.get(0).getDouble(1), within(1e-9));
    assertThat(best.getLong(1))
        .as("the id is the id of that document")
        .isEqualTo(hits.get(0).getLong(0));
    assertThat(best.getString(2)).as("the document is the document").contains("fox");
  }

  @Test
  public void projectsTheScoreOfAMatchedRow() {
    String table = tableWithIndex("docs_score_matched");

    List<Row> rows =
        spark
            .sql(
                String.format(
                    "SELECT id, text_score(body, 'the fox') AS score FROM local.%s "
                        + "WHERE match(body, 'the fox')",
                    table))
            .collectAsList();
    assertThat(rows).isNotEmpty();
    assertThat(idsOf(rows)).containsExactlyInAnyOrderElementsOf(idsOf(functionHits(table, 100)));
    assertThat(rows).allSatisfy(row -> assertThat(row.getDouble(1)).isPositive());
  }

  @Test
  public void bindsEachSearchOfAPlanToItsOwnIndex() {
    String first = tableWithIndex("docs_two_searches_a");
    String second = tableWithIndex("docs_two_searches_b");

    // two searches in one plan, each over its own index: binding both to one index would answer
    // one of them with the other's corpus statistics
    List<Row> rows =
        spark
            .sql(
                String.format(
                    "(SELECT id, text_score(body, 'the fox') AS score FROM local.%s "
                        + "ORDER BY text_score(body, 'the fox') DESC LIMIT 2) UNION ALL "
                        + "(SELECT id, text_score(body, 'dog') AS score FROM local.%s "
                        + "ORDER BY text_score(body, 'dog') DESC LIMIT 2)",
                    first, second))
            .collectAsList();
    assertThat(rows).hasSize(4);
    assertThat(rows).allSatisfy(row -> assertThat(row.getDouble(1)).isPositive());
  }

  @Test
  void rejectsAnUnboundScoreFromAnotherTablesSameNamedColumn() {
    String first = indexedTextTable("docs_foreign_score_a", "(1, 'fox'), (2, 'fox dog dog dog')");
    String second = indexedTextTable("docs_foreign_score_b", "(1, 'fox'), (2, 'dog')");
    try {
      double expected =
          spark
              .sql(
                  String.format(
                      "SELECT score FROM text_index_search('local.%s', 'body', 'fox', 10)", second))
              .head()
              .getDouble(0);
      assertThat(expected).isCloseTo(Math.log(2), within(1e-9));

      // Only a's match is bound. b's score must not borrow a's corpus statistics merely because
      // its column and query have the same names; the unsupported expression stays unbound.
      assertThatThrownBy(
              () ->
                  spark
                      .sql(
                          String.format(
                              "SELECT b.id, text_score(b.body, 'fox') AS score FROM local.%s a "
                                  + "CROSS JOIN local.%s b WHERE match(a.body, 'fox')",
                              first, second))
                      .collectAsList())
          .hasStackTraceContaining("text_score needs a text index");
    } finally {
      spark.sql("DROP TABLE local." + first + " PURGE");
      spark.sql("DROP TABLE local." + second + " PURGE");
    }
  }

  @Test
  void bindsTheMatchedAttributeAboveAJoin() {
    String first = indexedTextTable("docs_join_score_a", "(1, 'fox'), (2, 'fox dog dog dog')");
    String second = indexedTextTable("docs_join_score_b", "(1, 'fox'), (2, 'dog')");
    try {
      double expected =
          spark
              .sql(
                  String.format(
                      "SELECT score FROM text_index_search('local.%s', 'body', 'fox', 10) "
                          + "WHERE get_json_object(row, '$.id') = '1'",
                      first))
              .head()
              .getDouble(0);
      List<Row> rows =
          spark
              .sql(
                  String.format(
                      "SELECT a.id, text_score(a.body, 'fox') AS score FROM local.%s a "
                          + "CROSS JOIN local.%s b WHERE match(a.body, 'fox')",
                      first, second))
              .collectAsList();
      assertThat(rows).hasSize(4);
      assertThat(rows)
          .filteredOn(row -> row.getLong(0) == 1)
          .hasSize(2)
          .allSatisfy(row -> assertThat(row.getDouble(1)).isCloseTo(expected, within(1e-9)));
    } finally {
      spark.sql("DROP TABLE local." + first + " PURGE");
      spark.sql("DROP TABLE local." + second + " PURGE");
    }
  }

  @Test
  void bindsSameQueryBranchesToDifferentCorpora() {
    String first = indexedTextTable("docs_same_query_a", "(1, 'fox'), (2, 'fox dog dog dog')");
    String second = indexedTextTable("docs_same_query_b", "(1, 'fox'), (2, 'dog')");
    try {
      List<Row> expected =
          spark
              .sql(
                  String.format(
                      "SELECT 1 AS source, CAST(get_json_object(row, '$.id') AS bigint) AS id, score "
                          + "FROM text_index_search('local.%s', 'body', 'fox', 10) UNION ALL "
                          + "SELECT 2 AS source, CAST(get_json_object(row, '$.id') AS bigint) AS id, score "
                          + "FROM text_index_search('local.%s', 'body', 'fox', 10) ORDER BY source, id",
                      first, second))
              .collectAsList();
      List<Row> actual =
          spark
              .sql(
                  String.format(
                      "SELECT 1 AS source, id, text_score(body, 'fox') AS score FROM local.%s "
                          + "WHERE match(body, 'fox') UNION ALL "
                          + "SELECT 2 AS source, id, text_score(body, 'fox') AS score FROM local.%s "
                          + "WHERE match(body, 'fox') ORDER BY source, id",
                      first, second))
              .collectAsList();
      assertThat(actual).hasSameSizeAs(expected);
      for (int i = 0; i < expected.size(); i++) {
        assertThat(actual.get(i).getInt(0)).isEqualTo(expected.get(i).getInt(0));
        assertThat(actual.get(i).getLong(1)).isEqualTo(expected.get(i).getLong(1));
        assertThat(actual.get(i).getDouble(2))
            .isCloseTo(expected.get(i).getDouble(2), within(1e-9));
      }
    } finally {
      spark.sql("DROP TABLE local." + first + " PURGE");
      spark.sql("DROP TABLE local." + second + " PURGE");
    }
  }

  private static String indexedTextTable(String name, String documents) {
    String table = "db." + name;
    spark.sql(String.format("CREATE TABLE local.%s %s", table, COLUMNS));
    spark.sql(String.format("INSERT INTO local.%s VALUES %s", table, documents));
    spark.sql(
        String.format(
            "CALL local.system.build_text_index(table => '%s', column => 'body')", table));
    return table;
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

  private static String scoredRanking(String table) {
    return String.format(
        "SELECT id, text_score(body, 'the fox') AS score FROM local.%s "
            + "ORDER BY text_score(body, 'the fox') DESC LIMIT 2",
        table);
  }

  private static List<Long> idsOf(List<Row> rows) {
    return rows.stream().map(row -> row.getLong(0)).collect(java.util.stream.Collectors.toList());
  }

  /** The hits of the table-valued function for the same query, with their scores. */
  private static List<Row> functionHits(String table, int limit) {
    return spark
        .sql(
            String.format(
                "SELECT CAST(get_json_object(row, '$.id') AS bigint) AS id, score FROM "
                    + "text_index_search('local.%s', 'body', 'the fox', %d) ORDER BY score DESC",
                table, limit))
        .collectAsList();
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
