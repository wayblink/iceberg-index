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
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.iceberg.Table;
import org.apache.iceberg.index.spark.VectorIndexes;
import org.apache.iceberg.index.text.TextIndexTypes;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.sql.Dataset;
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
  void keepsTheOutputOfADataframeRankingUnchanged() {
    String table = tableWithIndex("docs_dataframe_ranking");
    org.apache.spark.sql.Column score =
        org.apache.spark.sql.functions.call_function(
            "text_score",
            org.apache.spark.sql.functions.col("body"),
            org.apache.spark.sql.functions.lit("the fox"));
    Dataset<Row> ranked = spark.table("local." + table).orderBy(score.desc()).limit(2);

    // no select list means the engine's own plan must not grow the output by the score column the
    // rewrite carries inside the source
    assertThat(ranked.columns()).containsExactly("id", "body");
    // the caller asked for two columns and the plan the rewrite returns has to carry those two: a
    // score column left in it would make every downstream read of the plan wider than the dataset
    assertThat(ranked.queryExecution().optimizedPlan().output().size()).isEqualTo(2);
    List<Row> rows = ranked.collectAsList();
    assertThat(rows).hasSize(2);
  }

  @Test
  public void rewritesARankedTextSearchIntoAnIndexLookup() {
    String table = tableWithIndex("docs_ranked");

    // the ranking form is lowered to the source the table-valued function runs on, so the search
    // happens when the operator executes rather than while the query is planned
    assertThat(planOf(table, ranking(table))).contains("TextSearchSource");

    // the rows and their order are the ones the table-valued function returns, which searches the
    // same index through another path
    assertThat(rankingRows(table)).isEqualTo(functionRows(table, 2));
  }

  @Test
  public void readsOnlyTheColumnsTheQueryUses() {
    String table = tableWithIndex("docs_read_columns");

    // a query that names one column reads that column and the score, not the column the ranking was
    // written against: the ranking is the search's own, so the document text is not read back
    String query =
        String.format(
            "SELECT id FROM local.%s ORDER BY text_score(body, 'the fox') DESC LIMIT 2", table);
    assertThat(planOf(table, query)).contains("TextSearchSource").contains("reads=[id]");

    // a query that names no column of the row keeps the search answering with the whole row: a
    // fetch-back with no column to read is not what such a query asks for
    String scored =
        String.format(
            "SELECT text_score(body, 'the fox') AS s FROM local.%s ORDER BY s DESC LIMIT 2", table);
    assertThat(planOf(table, scored)).contains("reads=[id, body]");

    // the columns read are the only thing that changed: the answer is still the one the
    // table-valued
    // function returns for the same search
    assertThat(idsOf(spark.sql(query).collectAsList())).isEqualTo(idsOf(functionHits(table, 2)));
  }

  @Test
  public void rewritesARankedTextSearchWithAFilterIntoAPrefilteredLookup() {
    // four documents that contain the query term four, three, two and one times: their scores are
    // distinct, so the order of the filtered answer is a fact rather than a tie
    String table =
        indexedTextTable(
            "docs_ranked_filtered",
            "(1, 'fox fox fox fox'), (2, 'fox fox'), (3, 'fox'), (4, 'dog'), (5, 'fox fox fox')");

    String query =
        String.format(
            "SELECT id FROM local.%s WHERE id > 1 ORDER BY text_score(body, 'fox') DESC LIMIT 2",
            table);

    // the filter is carried into the search, so the source answers the top-k of the rows the filter
    // admits rather than a top-k the plan filters afterwards
    assertThat(planOf(table, query)).contains("TextSearchSource").contains("prefilter=true");

    // the expected answer is the whole ranking of the term with the filtered-out row dropped: rows
    // 5 and 2 are the best two of the rows the filter admits
    List<Long> filteredRanking =
        idsOf(functionHitsOf(table, "fox", 100)).stream()
            .filter(id -> id > 1)
            .limit(2)
            .collect(Collectors.toList());
    assertThat(filteredRanking).containsExactly(5L, 2L);
    assertThat(idsOf(spark.sql(query).collectAsList())).containsExactlyElementsOf(filteredRanking);

    // the other order, filtering the unfiltered top-2, answers with one row: that is the shape the
    // rewrite is here to avoid, and it is why the filter has to be part of the search
    List<Row> filteredTopK =
        spark
            .sql(
                String.format(
                    "SELECT id FROM (SELECT id FROM local.%s "
                        + "ORDER BY text_score(body, 'fox') DESC LIMIT 2) x WHERE id > 1",
                    table))
            .collectAsList();
    assertThat(idsOf(filteredTopK)).containsExactly(5L);
  }

  @Test
  void rewritesARankedTextSearchWithAFilterAndAProjectedScore() {
    String table = tableWithIndex("docs_ranked_filtered_score");

    String query =
        String.format(
            "SELECT id, text_score(body, 'the fox') AS score FROM local.%s WHERE id > 1 "
                + "ORDER BY score DESC LIMIT 2",
            table);
    assertThat(planOf(table, query)).contains("TextSearchSource").contains("prefilter=true");

    List<Long> expected =
        idsOf(functionHits(table, 100)).stream()
            .filter(id -> id > 1)
            .limit(2)
            .collect(Collectors.toList());
    List<Row> rows = spark.sql(query).collectAsList();
    assertThat(idsOf(rows)).containsExactlyElementsOf(expected);
    assertThat(rows)
        .allSatisfy(row -> assertThat(row.getDouble(1)).as("the projected score").isPositive());
  }

  @Test
  void saysWhichFilterARankedTextSearchCannotPush() {
    String table = tableWithIndex("docs_ranked_unpushable_filter");
    String query =
        String.format(
            "SELECT id FROM local.%s WHERE rand() > 0.5 "
                + "ORDER BY text_score(body, 'the fox') DESC LIMIT 2",
            table);

    // a non-deterministic filter cannot be judged from the index entry a row is ranked by, so the
    // query keeps its score. It is not answered with "no index was found" either: the index is
    // there and the filter is what stands in the way, so the error names the filter shape
    assertThat(spark.sql(query).queryExecution().analyzed().toString())
        .contains("text_score")
        .doesNotContain("TextSearchSource");
    assertThatThrownBy(() -> spark.sql(query).collectAsList())
        .hasStackTraceContaining("text_score cannot apply this filter before ranking")
        .hasStackTraceContaining("deterministic");
  }

  @Test
  void keepsTheUnfilteredRankingUnchanged() {
    String table = tableWithIndex("docs_ranked_unfiltered_shape");

    // the filter support must not change the plan of a ranking query without one
    String plan = planOf(table, ranking(table));
    assertThat(plan).contains("TextSearchSource").contains("prefilter=false");
    assertThat(rankingRows(table)).isEqualTo(functionRows(table, 2));
  }

  @Test
  public void leavesAnOuterOrderByOverANestedSearchToSpark() {
    // the outer order decides which two rows the query asks for; the inner score order only orders
    // the subquery. Lowering the nested score order to the index's limit would answer with the two
    // highest-scoring rows instead of the two smallest ids, and nothing would fail
    String table =
        indexedTextTable(
            "docs_nested_order",
            "(1, 'quiet sun'), (2, 'quiet moon'), (3, 'fox fox fox'), (4, 'fox fox')");

    String query =
        String.format(
            "SELECT id FROM (SELECT id, body FROM local.%s ORDER BY text_score(body, 'fox') DESC) x "
                + "ORDER BY id ASC LIMIT 2",
            table);

    assertThat(planOf(table, query)).doesNotContain("TextSearchSource");
    assertThat(idsOf(spark.sql(query).collectAsList())).containsExactly(1L, 2L);
  }

  @Test
  void plansARankedSearchWithoutSearching() {
    String table = tableWithIndex("docs_planned_only");

    // the search runs when the operator does. Reading and scoring the index files while the query
    // is planned is what made this form an order of magnitude slower than the table function that
    // asks the same index the same question
    assertThat(jobsDuring(() -> planOf(table, ranking(table))))
        .as("jobs started while planning a ranked text search")
        .isZero();
  }

  @Test
  void ranksAcrossEveryIndexFileOfATable() {
    // a data file per insert, so the index has one file each and an answer is the merge of all of
    // them: a search that read a single index file would answer with the rows of that file alone
    String table = "db.docs_many_index_files";
    spark.sql(String.format("CREATE TABLE local.%s %s", table, COLUMNS));
    for (String document :
        new String[] {
          "(1, 'the quick brown fox jumps over the lazy dog')",
          "(2, 'the fox runs through the forest')",
          "(3, 'a dog sleeps in the sun')",
          "(4, 'the forest is quiet and the fox is quick')",
          "(5, 'the dog and the fox rest')",
          "(6, 'a quiet sun over the forest')"
        }) {
      spark.sql(String.format("INSERT INTO local.%s VALUES %s", table, document));
    }
    // one entry per index file, so every data file gets a file of its own
    spark.sql(
        String.format(
            "CALL local.system.build_text_index(table => '%s', column => 'body', "
                + "max_entries_per_index_file => 1)",
            table));

    assertThat(indexFiles(table)).as("index files of the table").isGreaterThan(1);
    assertThat(planOf(table, ranking(table, 3))).contains("TextSearchSource");

    List<Row> ranked = spark.sql(ranking(table, 3)).collectAsList();
    assertThat(ranked).as("rows of a top-3 over several index files").hasSize(3);
    assertThat(idsOf(ranked)).isEqualTo(idsOf(functionRows(table, 3)));
  }

  @Test
  public void rewritesAFilteredTextSearchIntoAnIndexLookup() {
    String table = tableWithIndex("docs_matched");

    // the filter form asks for every match, which the source answers from the index: the plan is
    // lowered rather than materialised while the query is planned
    assertThat(planOf(table, filtering(table)))
        .contains("TextSearchSource")
        .doesNotContain("LocalRelation");

    // every row the function returns as a match is in the result, and nothing else
    assertThat(filteringRows(table)).containsExactlyInAnyOrderElementsOf(functionRows(table, 100));
  }

  @Test
  void matchesEveryRowOfAHistoricalSnapshot() {
    String table = "local.db.docs_historical_match";
    spark.sql(
        "CREATE TABLE " + table + " (id bigint, body string) USING iceberg PARTITIONED BY (id)");
    spark.sql(
        "INSERT INTO "
            + table
            + " VALUES (1,'fox'),(2,'fox'),(3,'fox'),(4,'fox'),(5,'fox'),(6,'fox'),(7,'fox'),(8,'fox')");
    long snapshot =
        spark
            .sql(
                "CALL local.system.build_text_index(table => 'db.docs_historical_match', column => 'body')")
            .head()
            .getLong(1);
    spark.sql("DELETE FROM " + table + " WHERE id > 1");
    spark.sql(
        "CALL local.system.refresh_text_index(table => 'db.docs_historical_match', column => 'body')");

    assertThat(
            idsOf(
                spark
                    .sql(
                        "SELECT id FROM "
                            + table
                            + " VERSION AS OF "
                            + snapshot
                            + " WHERE match(body, 'fox')")
                    .collectAsList()))
        .containsExactlyInAnyOrder(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L);
  }

  @Test
  void matchesAndProjectsAScoreWithoutPublishedStatistics() {
    String table = table("docs_without_statistics");
    spark.sql(
        "CALL local.system.build_text_index(table => '"
            + table
            + "', column => 'body', text_statistics => 'never')");
    String where = " FROM local." + table + " WHERE match(body, 'fox')";
    assertThat(jobsDuring(() -> planOf(table, "SELECT id" + where))).isZero();
    assertThat(idsOf(spark.sql("SELECT id, text_score(body, 'fox')" + where).collectAsList()))
        .containsExactlyInAnyOrder(1L, 2L, 4L);
  }

  @Test
  void plansMatchWithoutReadingTheStatisticsFile() throws Exception {
    String table = tableWithIndex("docs_match_without_statistics_read");
    Table loaded = Spark3Util.loadIcebergTable(spark, "local." + table);
    loaded
        .io()
        .deleteFile(
            VectorIndexes.findIndex(loaded, "body", "text").latestSnapshot().statisticsLocation());

    assertThat(planOf(table, filtering(table))).contains("TextSearchSource");
  }

  @Test
  void plansAFilteredSearchWithoutSearching() {
    String table = tableWithIndex("docs_matched_planned");

    assertThat(jobsDuring(() -> planOf(table, filtering(table))))
        .as("jobs started while planning a filtered text search")
        .isZero();
  }

  @Test
  void keepsTheOtherPredicatesOfAFilteredSearch() {
    String table =
        indexedTextTable(
            "docs_matched_and",
            "(1, 'the quick brown fox'), (2, 'the fox runs'), (3, 'a quiet sun')");
    String query =
        String.format("SELECT id FROM local.%s WHERE match(body, 'the fox') AND id > 1", table);

    // the match is answered by the search and the rest of the condition is still applied: both rows
    // that match are read, and the filter decides which of them the answer keeps
    assertThat(spark.sql(query).queryExecution().optimizedPlan().toString())
        .contains("TextSearchSource")
        .doesNotContain("LocalRelation");
    assertThat(idsOf(spark.sql(query).collectAsList())).containsExactly(2L);
  }

  @Test
  void readsOnlyTheColumnsAFilteredQueryUses() {
    String table =
        indexedTextTable("docs_matched_columns", "(1, 'the quick brown fox'), (2, 'the fox runs')");
    String query = String.format("SELECT id FROM local.%s WHERE match(body, 'the fox')", table);

    // the filter form answers with the rows the match found, and the columns those rows are read
    // with are the columns the plan above names, the same way the ranking form is narrowed
    assertThat(planOf(table, query)).contains("TextSearchSource").contains("reads=[id]");
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
    assertThat(planOf(table, ranking(table))).doesNotContain("TextSearchSource");

    // a score needs the statistics of an index, so the query says so instead of returning rows in
    // an arbitrary order
    assertThatThrownBy(() -> spark.sql(ranking(table)).collectAsList())
        .hasStackTraceContaining("text_score needs a text index");
  }

  @Test
  public void refusesToScoreAgainstOneOfSeveralIndexes() {
    String table = tableWithIndex("docs_text_two_indexes");
    spark.sql(
        String.format(
            "CALL local.system.build_text_index(table => '%s', column => 'body', name => 'body_case', "
                + "lowercase => false)",
            table));

    // the written form names no index, so it must not score with whichever index happened to be
    // found first: with two indexes built by different analyzers that would be a scoring bug the
    // caller cannot see
    assertThatThrownBy(() -> spark.sql(ranking(table)).collectAsList())
        .hasStackTraceContaining("has 2 text indexes")
        .hasStackTraceContaining("body_case");
  }

  @Test
  public void leavesThePlanAloneForAnIndexThatIsBehind() {
    String table = tableWithIndex("docs_text_behind");
    // a row added after the index was built leaves the corpus statistics behind
    spark.sql(String.format("INSERT INTO local.%s VALUES (5, 'the fox and the dog rest')", table));

    assertThat(planOf(table, ranking(table))).doesNotContain("TextSearchSource");
  }

  @Test
  public void leavesThePlanAloneWhenTheQueryIsNotARankedTextSearch() {
    String table = tableWithIndex("docs_text_not_ranked");
    assertThat(planOf(table, String.format("SELECT id FROM local.%s WHERE id > 1", table)))
        .doesNotContain("TextSearchSource");
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
    assertThat(planOf(table, scoredRanking(table))).contains("TextSearchSource");
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
    return ranking(table, 2);
  }

  private static String ranking(String table, int k) {
    return String.format(
        "SELECT id FROM local.%s ORDER BY text_score(body, 'the fox') DESC LIMIT %d", table, k);
  }

  /** How many index files the text index of a table has. */
  private static long indexFiles(String table) {
    return spark
        .sql(String.format("CALL local.system.list_indexes(table => '%s')", table))
        .head()
        .getLong(5);
  }

  /** How many jobs running the given action started. */
  private static long jobsDuring(Runnable action) {
    AtomicLong jobs = new AtomicLong();
    SparkListener listener =
        new SparkListener() {
          @Override
          public void onJobStart(SparkListenerJobStart jobStart) {
            jobs.incrementAndGet();
          }
        };
    spark.sparkContext().addSparkListener(listener);
    try {
      action.run();
    } finally {
      spark.sparkContext().removeSparkListener(listener);
    }

    return jobs.get();
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

  /** The hits of the table-valued function for a query, with their scores, best first. */
  private static List<Row> functionHitsOf(String table, String query, int limit) {
    return spark
        .sql(
            String.format(
                "SELECT CAST(get_json_object(row, '$.id') AS bigint) AS id, score FROM "
                    + "text_index_search('local.%s', 'body', '%s', %d) ORDER BY score DESC",
                table, query, limit))
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
