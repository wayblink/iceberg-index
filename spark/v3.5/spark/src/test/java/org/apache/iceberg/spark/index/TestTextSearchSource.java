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
package org.apache.iceberg.spark.index;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.index.CreateIndexRequest;
import org.apache.iceberg.index.IndexCatalog;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.IndexQuery;
import org.apache.iceberg.index.IndexSearchResult;
import org.apache.iceberg.index.SidecarIndexCatalog;
import org.apache.iceberg.index.spark.DistributedIndexSearch;
import org.apache.iceberg.index.spark.IndexEntries;
import org.apache.iceberg.index.spark.IndexSearch;
import org.apache.iceberg.index.spark.IndexSelection;
import org.apache.iceberg.index.spark.SparkIndexBuilder;
import org.apache.iceberg.index.spark.VectorIndexes;
import org.apache.iceberg.index.text.TextIndexAdapter;
import org.apache.iceberg.index.text.TextIndexDefinition;
import org.apache.iceberg.index.text.TextIndexTypes;
import org.apache.iceberg.types.Types;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.StageInfo;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import scala.runtime.BoxedUnit;

class TestTextSearchSource {
  private static Path warehouse;
  private static SparkSession spark;

  @BeforeAll
  static void startSpark(@TempDir Path warehouseDir) {
    warehouse = warehouseDir;
    TextIndexTypes.register();
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("text-search-source")
            .withExtensions(
                extensions -> {
                  IndexTableFunctions.register(extensions);
                  extensions.injectPlannerStrategy(VectorSearchStrategy::new);
                  return BoxedUnit.UNIT;
                })
            .config("spark.ui.enabled", "false")
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
  void appliesSparkEqualityInSignedZeroPartitions() {
    spark.sql(
        "CREATE TABLE local.db.signed_zero (id bigint, body string, d double) "
            + "USING iceberg PARTITIONED BY (d)");
    spark.sql(
        "INSERT INTO local.db.signed_zero VALUES "
            + "(1, 'ice', CAST('-0.0' AS DOUBLE)), (2, 'ice', CAST('0.0' AS DOUBLE))");
    buildIndex("signed_zero");

    List<Row> expected =
        spark.sql("SELECT id FROM " + search("signed_zero") + ") WHERE d = 0D").collectAsList();
    assertThat(expected).extracting(row -> row.getLong(0)).containsExactlyInAnyOrder(1L, 2L);
    assertThat(
            spark
                .sql("SELECT id FROM " + search("signed_zero") + ", filter => 'd = 0D')")
                .collectAsList())
        .containsExactlyInAnyOrderElementsOf(expected);
  }

  @Test
  void foldsCoercedFilterLiterals() {
    spark.sql("CREATE TABLE local.db.coerced_filter (id bigint, body string) USING iceberg");
    spark.sql("INSERT INTO local.db.coerced_filter VALUES (1, 'ice'), (2, 'ice')");
    buildIndex("coerced_filter");

    assertThat(
            spark
                .sql("SELECT id FROM " + search("coerced_filter") + ", filter => 'id = 2')")
                .collectAsList())
        .extracting(row -> row.getLong(0))
        .containsExactly(2L);
    assertThat(
            spark
                .sql("SELECT id FROM " + search("coerced_filter") + ", filter => 'id = NULL')")
                .collectAsList())
        .isEmpty();
  }

  @Test
  void evaluatesScalarFiltersWithoutPruning() {
    spark.sql("CREATE TABLE local.db.scalar_filter (id bigint, body string) USING iceberg");
    spark.sql("INSERT INTO local.db.scalar_filter VALUES (1, 'ice'), (2, 'ice')");
    buildIndex("scalar_filter");

    assertThat(
            spark
                .sql("SELECT id FROM " + search("scalar_filter") + ", filter => 'id % 2 = 0')")
                .collectAsList())
        .extracting(row -> row.getLong(0))
        .containsExactly(2L);
  }

  @Test
  void readsNestedResultColumns() {
    spark.sql(
        "CREATE TABLE local.db.nested_output "
            + "(id bigint, body string, payload struct<tags:array<map<string,int>>>) "
            + "USING iceberg TBLPROPERTIES ('write.format.default' = 'avro')");
    spark.sql(
        "INSERT INTO local.db.nested_output VALUES "
            + "(1, 'ice', NAMED_STRUCT('tags', ARRAY(MAP('year', 2026))))");
    buildIndex("nested_output");

    Row result = spark.sql("SELECT * FROM " + search("nested_output") + ")").head();
    assertThat(result.getLong(0)).isEqualTo(1L);
    assertThat(result.getString(1)).isEqualTo("ice");
    Row payload = result.getStruct(2);
    assertThat(payload.getList(0)).hasSize(1);
    assertThat(
            spark
                .sql("SELECT payload.tags[0]['year'] FROM " + search("nested_output") + ")")
                .head()
                .getInt(0))
        .isEqualTo(2026);
  }

  @Test
  void prunesFilesUsingFilterColumnBounds() {
    spark.sql(
        "CREATE TABLE local.db.file_metrics (id bigint, body string, tenant int) "
            + "USING iceberg");
    spark.sql("INSERT INTO local.db.file_metrics VALUES (1, 'ice', 1)");
    spark.sql("INSERT INTO local.db.file_metrics VALUES (2, 'ice', 2)");
    Table table = buildIndex("file_metrics");
    String excluded =
        table.currentSnapshot().addedDataFiles(table.io()).iterator().next().location();
    table.io().deleteFile(excluded);

    assertThat(
            spark
                .sql("SELECT id FROM " + search("file_metrics") + ", filter => 'tenant = 1')")
                .collectAsList())
        .extracting(row -> row.getLong(0))
        .containsExactly(1L);
  }

  @Test
  void readsTheRowsInTheAnswerJob() {
    spark.sql("CREATE TABLE local.db.answer_jobs (id bigint, body string) USING iceberg");
    spark.sql("INSERT INTO local.db.answer_jobs VALUES (1, 'ice fox'), (2, 'ice dog')");
    spark.sql("INSERT INTO local.db.answer_jobs VALUES (3, 'ice cat'), (4, 'ice')");
    Table table = buildIndex("answer_jobs");

    // the answer is read in one task per hit data file and put back in the order the hits merged
    // in by a shuffle over bands of that order, all inside the job that answers the query: a
    // fetch-back that collected the rows to the driver would be a job of its own, and the answer's
    // own job would read no data file
    List<IndexSearchResult> hits = hitsOf(table, "ice", 10);
    int hitFiles = (int) hits.stream().map(IndexSearchResult::dataFile).distinct().count();
    int bands = Math.min(hits.size(), spark.sparkContext().defaultParallelism());
    assertThat(hitFiles).as("data files the answer is read from").isGreaterThan(1);
    assertThat(bands).as("bands of the merged order the answer is assembled in").isGreaterThan(1);
    List<List<Integer>> jobs = new ArrayList<>();
    SparkListener countedJobs =
        new SparkListener() {
          @Override
          public void onJobStart(SparkListenerJobStart jobStart) {
            List<Integer> tasks = new ArrayList<>();
            scala.collection.Iterator<StageInfo> stages = jobStart.stageInfos().iterator();
            while (stages.hasNext()) {
              tasks.add(stages.next().numTasks());
            }

            jobs.add(tasks);
          }
        };

    spark.sparkContext().addSparkListener(countedJobs);
    List<Row> rows;
    try {
      rows = spark.sql("SELECT id FROM " + search("answer_jobs") + ")").collectAsList();
    } finally {
      spark.sparkContext().removeSparkListener(countedJobs);
    }

    assertThat(rows).hasSize(4);
    assertThat(jobs.get(jobs.size() - 1))
        .as("tasks of the answer's job: one per hit data file, then one per band of the order")
        .containsExactly(hitFiles, bands);
  }

  @Test
  void keepsTheOrderTheHitsMergedIn() {
    spark.sql("CREATE TABLE local.db.answer_order (id bigint, body string) USING iceberg");
    spark.sql(
        "INSERT INTO local.db.answer_order VALUES "
            + "(1, 'ice fox'), (2, 'ice ice ice ice'), (3, 'ice dog'), (4, 'ice')");
    buildIndex("answer_order");

    // the hits of this answer come from more than one data file, so its rows are read by more than
    // one task: the answer is still the order the hits merged in, best first
    List<Row> rows =
        spark.sql("SELECT id, _score FROM " + search("answer_order") + ")").collectAsList();
    assertThat(rows).hasSize(4);

    List<Double> scores = new ArrayList<>();
    rows.forEach(row -> scores.add(row.getDouble(1)));
    List<Double> ranked = new ArrayList<>();
    spark
        .sql("SELECT id, _score FROM " + search("answer_order") + ") ORDER BY _score DESC")
        .collectAsList()
        .forEach(row -> ranked.add(row.getDouble(1)));
    assertThat(scores)
        .as("the answer keeps the order the hits merged in, which ranks by score")
        .isSortedAccordingTo(Comparator.reverseOrder());
    assertThat(scores).isEqualTo(ranked);
  }

  @Test
  void readsTheHitFilesInTheTasksThatHitThem() {
    spark.sql("CREATE TABLE local.db.hit_files (id bigint, body string) USING iceberg");
    spark.sql("INSERT INTO local.db.hit_files VALUES (1, 'ice')");
    spark.sql("INSERT INTO local.db.hit_files VALUES (2, 'ice')");
    Table table = buildIndex("hit_files");

    List<String> columns = Arrays.asList("id", "body");
    List<FileScanTask> tasks = IndexEntries.scanTasks(table, table.currentSnapshot().snapshotId());
    List<IndexSearchResult> hits = hitsOf(table, "ice", 10);

    // the hits answer from two data files, and the fetch-back hands a file to a task: the reads are
    // two tasks rather than the one partition the driver used to fill
    assertThat(
            IndexEntries.rowsAtRdd(
                    spark,
                    table,
                    positionsOf(hits),
                    columns,
                    IndexEntries.sparkSchema(table, columns),
                    tasks)
                .getNumPartitions())
        .isEqualTo(2);

    // the answer itself is put back in the order the hits merged in by a shuffle over bands of that
    // order, so it is read and sorted by more than one task rather than by one task carrying every
    // row
    Dataset<Row> answer = spark.sql("SELECT id FROM " + search("hit_files") + ")");
    assertThat(answer.collectAsList()).hasSize(2);
    assertThat(answer.queryExecution().toRdd().getNumPartitions())
        .isEqualTo(Math.min(hits.size(), spark.sparkContext().defaultParallelism()));
  }

  @Test
  void limitsTheAnswerToTheHighestRankedRows() {
    spark.sql("CREATE TABLE local.db.answer_limit (id bigint, body string) USING iceberg");
    spark.sql(
        "INSERT INTO local.db.answer_limit VALUES "
            + "(1, 'ice fox'), (2, 'ice ice ice ice'), (3, 'ice dog'), (4, 'ice')");
    spark.sql(
        "INSERT INTO local.db.answer_limit VALUES "
            + "(5, 'ice ice'), (6, 'ice ice ice'), (7, 'ice cat'), (8, 'ice ice ice ice ice')");
    buildIndex("answer_limit");

    // the answer is put back in the merged order by bands of it, and it is read in more than one
    // partition: a LIMIT above the search takes its rows from the first partitions in order, so it
    // answers with the highest ranked rows of the whole answer, not with the rows of one data file.
    // The limit is larger than one band, so the answer has to cross a partition boundary and take
    // the rest of the rows from the next band to be right
    Dataset<Row> answer = spark.sql("SELECT id, _score FROM " + search("answer_limit") + ")");
    List<Row> all = answer.collectAsList();
    assertThat(all).hasSize(8);
    int bands = Math.min(all.size(), spark.sparkContext().defaultParallelism());
    assertThat(bands).as("the answer is assembled in bands of the merged order").isGreaterThan(1);
    assertThat(answer.queryExecution().toRdd().getNumPartitions()).isEqualTo(bands);

    int limit = 6;
    assertThat(limit).as("the limit crosses a band").isGreaterThan(all.size() / bands);
    List<Long> top = new ArrayList<>();
    all.subList(0, limit).forEach(row -> top.add(row.getLong(0)));
    List<Long> limited = new ArrayList<>();
    spark
        .sql("SELECT id FROM " + search("answer_limit") + ") LIMIT " + limit)
        .collectAsList()
        .forEach(row -> limited.add(row.getLong(0)));

    assertThat(limited)
        .as("LIMIT takes the highest ranked rows of the whole answer")
        .containsExactlyElementsOf(top);
  }

  @Test
  void answersWithTheRowsTheDriverAssembled() {
    spark.sql("CREATE TABLE local.db.answer_rows (id bigint, body string) USING iceberg");
    spark.sql("INSERT INTO local.db.answer_rows VALUES (1, 'ice fox'), (2, 'ice ice ice ice')");
    spark.sql("INSERT INTO local.db.answer_rows VALUES (3, 'ice dog'), (4, 'ice')");
    Table table = buildIndex("answer_rows");

    // the answer a driver used to assemble: the hits in their merged order, each with the row it
    // names, read back by file and position. That order is what a caller sees, so the answer is
    // held to it row by row, whichever task read which file.
    List<String> columns = Arrays.asList("id", "body");
    List<IndexSearchResult> hits = hitsOf(table, "ice", 10);
    assertThat(hits).isNotEmpty();
    assertThat(hits.stream().map(IndexSearchResult::dataFile).distinct().count())
        .as("data files the answer is read from")
        .isGreaterThan(1);
    Map<String, Map<Long, Row>> byFile =
        IndexEntries.rowsAt(
            spark,
            table,
            positionsOf(hits),
            columns,
            IndexEntries.sparkSchema(table, columns),
            IndexEntries.scanTasks(table, table.currentSnapshot().snapshotId()));
    List<List<Object>> expected = new ArrayList<>();
    hits.forEach(
        hit -> {
          Row row = byFile.get(hit.dataFile()).get(hit.rowPosition());
          assertThat(row).as("the row the hit names").isNotNull();
          expected.add(Arrays.asList(row.getLong(0), row.getString(1), hit.score()));
        });

    List<List<Object>> answer = new ArrayList<>();
    spark
        .sql("SELECT id, body, _score FROM " + search("answer_rows") + ")")
        .collectAsList()
        .forEach(
            row -> answer.add(Arrays.asList(row.getLong(0), row.getString(1), row.getDouble(2))));

    assertThat(answer).containsExactlyElementsOf(expected);
  }

  /** The hits of a search on the index built for a table, best first. */
  private static List<IndexSearchResult> hitsOf(Table table, String query, int k) {
    IndexMetadata metadata = IndexSelection.resolve(table, "body", "text", null);
    TextIndexAdapter adapter = new TextIndexAdapter((TextIndexDefinition) metadata.definition());
    return DistributedIndexSearch.searchBatch(
            spark,
            metadata,
            table,
            Collections.singletonList(
                IndexQuery.of(table.currentSnapshot().snapshotId(), adapter.encodeQuery(query), k)))
        .get(0);
  }

  /** The positions a fetch-back reads, per data file, for the given hits. */
  private static Map<String, Collection<Long>> positionsOf(List<IndexSearchResult> hits) {
    Map<String, Collection<Long>> positions = new LinkedHashMap<>();
    hits.forEach(
        hit ->
            positions
                .computeIfAbsent(hit.dataFile(), file -> new ArrayList<>())
                .add(hit.rowPosition()));
    return positions;
  }

  private static String search(String name) {
    return "search(table => 'local.db."
        + name
        + "', query => 'ice', search_columns => array('body')";
  }

  private static Table buildIndex(String name) {
    Table table =
        new HadoopTables(spark.sessionState().newHadoopConf())
            .load(warehouse.resolve("db").resolve(name).toUri().toString());
    IndexCatalog catalog = new SidecarIndexCatalog(table.io(), table);
    IndexMetadata created =
        catalog.createIndex(
            CreateIndexRequest.builder()
                .tableLocation(table.location())
                .tableUuid(table.uuid().toString())
                .indexUuid(name)
                .definition(
                    TextIndexDefinition.builder()
                        .fieldId(table.schema().findField("body").fieldId())
                        .build())
                .build());
    catalog.updateIndex(
        SparkIndexBuilder.build(
            spark, table, created, table.currentSnapshot().snapshotId(), "body", 1L));
    return table;
  }

  @Test
  void resolvesOnlyTheRequestedColumnsOfAHit() {
    spark.sql(
        "CREATE TABLE local.db.projected_row (id bigint, body string, note string) USING iceberg");
    spark.sql(
        "INSERT INTO local.db.projected_row VALUES "
            + "(1, 'ice iceberg', 'first'), (2, 'ice vector', 'second')");
    Table table = buildIndex("projected_row");
    IndexMetadata metadata = VectorIndexes.findIndex(table, "body", "text");
    ByteBuffer query =
        new TextIndexAdapter((TextIndexDefinition) metadata.definition()).encodeQuery("iceberg");

    // null is the whole row, which is the contract CALL text_search answers with
    List<Row> wholeRows =
        IndexSearch.search(spark, table, "body", metadata, query, 2, null, null).collectAsList();
    List<String> allColumns =
        table.schema().columns().stream().map(Types.NestedField::name).collect(Collectors.toList());
    List<Row> explicitRows =
        IndexSearch.search(spark, table, "body", metadata, query, 2, allColumns, null)
            .collectAsList();
    assertThat(explicitRows).hasSameSizeAs(wholeRows);
    assertThat(explicitRows.get(0).getString(3)).isEqualTo(wholeRows.get(0).getString(3));

    List<Row> idOnly =
        IndexSearch.search(spark, table, "body", metadata, query, 2, List.of("id"), null)
            .collectAsList();
    assertThat(wholeRows).hasSize(1);
    assertThat(idOnly).hasSize(1);
    Row whole = wholeRows.get(0);
    Row projected = idOnly.get(0);
    // the projection decides what a row carries, not which hits the search returns
    assertThat(projected.getString(0)).isEqualTo(whole.getString(0));
    assertThat(projected.getLong(1)).isEqualTo(whole.getLong(1));
    assertThat(projected.getDouble(2)).isEqualTo(whole.getDouble(2));
    assertThat(whole.getString(3)).contains("\"id\"", "\"body\"", "\"note\"");
    assertThat(projected.getString(3)).contains("\"id\"").doesNotContain("\"body\"", "\"note\"");
  }

  /**
   * The stages a CALL text_search logs are the work its tasks did, not the driver waiting for them.
   *
   * <p>The procedure creates the stage metrics itself, so they reach the tasks only when the batch
   * search hands them on: the score a shard costs and the clock its task took are the task's own
   * numbers, and the fetch-back that resolves the answer's rows is timed as its own phase.
   */
  @Test
  void callReportsTheStagesItsTasksSpent() {
    spark.sql("CREATE TABLE local.db.call_stages (id bigint, body string) USING iceberg");
    spark.sql(
        "INSERT INTO local.db.call_stages VALUES (1, 'ice'), (2, 'ice ice ice ice'), "
            + "(3, 'ice berg'), (4, 'nothing here')");
    Table table = buildIndex("call_stages");
    IndexMetadata metadata = VectorIndexes.findIndex(table, "body", "text");
    ByteBuffer query =
        new TextIndexAdapter((TextIndexDefinition) metadata.definition()).encodeQuery("ice");
    DistributedIndexSearch.SearchStages stages =
        DistributedIndexSearch.SearchStages.create(spark.sparkContext());

    // the search CALL text_search runs, with the stages the procedure logs
    List<Row> rows =
        IndexSearch.search(spark, table, "body", metadata, query, 2, null, stages).collectAsList();
    assertThat(rows).isNotEmpty();

    long probeScore = stages.value(DistributedIndexSearch.SearchStages.PROBE_SCORE_TIME);
    long taskTime = stages.value(DistributedIndexSearch.SearchStages.TASK_TIME);
    assertThat(probeScore).as("a task reported the shard it read and scored").isPositive();
    assertThat(taskTime).as("a task reported what its index file cost").isPositive();
    assertThat(probeScore)
        .as("the score is part of the task's clock, not more than it")
        .isLessThanOrEqualTo(taskTime);
    assertThat(stages.value(DistributedIndexSearch.SearchStages.INDEX_READ_TIME))
        .as("a shard is opened inside its search, so the read is reported as part of the score")
        .isZero();
    assertThat(stages.value(DistributedIndexSearch.SearchStages.FETCH_BACK_TIME))
        .as("the fetch-back of the answer's rows is timed as its own phase")
        .isPositive();
  }
}
