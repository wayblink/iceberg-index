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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.index.CreateIndexRequest;
import org.apache.iceberg.index.IndexCatalog;
import org.apache.iceberg.index.SidecarIndexCatalog;
import org.apache.iceberg.index.spark.SparkIndexBuilder;
import org.apache.iceberg.index.text.TextIndexDefinition;
import org.apache.iceberg.index.text.TextIndexTypes;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.scheduler.StageInfo;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import scala.runtime.BoxedUnit;

/**
 * Profiles the cost of assembling the answer of a text search, at the row sizes of the 10M table:
 * one content column of about 7.7 KB of text per row. Not a test of behaviour, a measurement
 * harness; it is meant to be run on its own against one implementation at a time and the numbers
 * compared.
 */
class TestTextSearchAnswerOrder {
  private static final int FILES = 8;
  private static final int ROWS = 10000;
  private static final int CHUNKS = 240;
  private static final int WARMUPS = 2;
  private static final int[] LIMITS = {10, 2000, 10000};

  private static Path warehouse;
  private static SparkSession spark;

  @BeforeAll
  static void startSpark(@TempDir Path warehouseDir) {
    warehouse = warehouseDir;
    TextIndexTypes.register();
    spark =
        SparkSession.builder()
            .master("local[8]")
            .appName("text-search-assembly-profile")
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
  void answersInTheRankOrderTheHitsMergedIn() {
    spark.sql(
        "CREATE TABLE local.db.profile (id bigint, body string, content string) USING iceberg");
    int perFile = ROWS / FILES;
    for (int file = 0; file < FILES; file++) {
      long from = (long) file * perFile;
      long to = from + perFile;
      // the content is text that does not repeat itself, so the answer is about as expensive to
      // move as the text of the real table is, not cheaper because it compresses away
      spark.sql(
          "INSERT INTO local.db.profile "
              + "SELECT id, concat('ice ', repeat('ice ', CAST(id % 3 AS INT)), 'doc', "
              + "CAST(id AS STRING)), "
              + "concat_ws('', collect_list(md5(concat(CAST(id AS STRING), '-', "
              + "CAST(chunk AS STRING))))) "
              + "FROM range("
              + from
              + ", "
              + to
              + ") AS docs(id) "
              + "LATERAL VIEW explode(sequence(0, "
              + (CHUNKS - 1)
              + ")) AS chunk "
              + "GROUP BY id");
    }

    buildIndex();

    TaskTimes times = new TaskTimes();
    spark.sparkContext().addSparkListener(times);
    try {
      for (int limit : LIMITS) {
        profile(limit, times);
      }
    } finally {
      spark.sparkContext().removeSparkListener(times);
    }
  }

  private static void profile(int limit, TaskTimes times) {
    String query =
        "SELECT id, _score, content FROM search(table => 'local.db.profile', query => 'ice', "
            + "search_columns => array('body'), limit => "
            + limit
            + ")";

    System.out.println(
        "PROFILE plan k=" + limit + ":" + spark.sql(query).queryExecution().executedPlan());

    for (int run = 0; run < WARMUPS; run++) {
      spark.sql(query).collectAsList();
    }

    int runs = limit > 2000 ? 5 : 7;
    List<Long> elapsed = new ArrayList<>();
    List<Row> answer = null;
    String stages = "";
    for (int run = 0; run < runs; run++) {
      TaskTimes.drain();
      times.reset();
      long start = System.nanoTime();
      answer = spark.sql(query).collectAsList();
      elapsed.add((System.nanoTime() - start) / 1_000_000L);
      TaskTimes.drain();
      if (run == runs - 1) {
        stages = times.report();
      }
    }

    assertThat(answer).as("the answer of k=%d", limit).hasSize(limit);
    double previous = Double.MAX_VALUE;
    for (Row row : answer) {
      assertThat(row.getDouble(1)).as("the answer ranks by score").isLessThanOrEqualTo(previous);
      previous = row.getDouble(1);
    }

    Collections.sort(elapsed);
    System.out.printf(
        Locale.ROOT,
        "%nPROFILE k=%d p50=%dms min=%dms max=%dms runs=%s digest=%d firstId=%d lastId=%d "
            + "contentChars=%d%s%n",
        limit,
        elapsed.get(elapsed.size() / 2),
        elapsed.get(0),
        elapsed.get(elapsed.size() - 1),
        elapsed,
        digest(answer),
        answer.get(0).getLong(0),
        answer.get(answer.size() - 1).getLong(0),
        answer.get(0).getString(2).length(),
        stages);
  }

  private static long digest(List<Row> rows) {
    long digest = 17;
    for (Row row : rows) {
      digest = digest * 31 + row.getLong(0);
      digest = digest * 31 + Double.doubleToLongBits(row.getDouble(1));
    }

    return digest;
  }

  private static void buildIndex() {
    Table table =
        new HadoopTables(spark.sessionState().newHadoopConf())
            .load(warehouse.resolve("db").resolve("profile").toUri().toString());
    IndexCatalog catalog = new SidecarIndexCatalog(table.io(), table);
    org.apache.iceberg.index.IndexMetadata created =
        catalog.createIndex(
            CreateIndexRequest.builder()
                .tableLocation(table.location())
                .tableUuid(table.uuid().toString())
                .indexUuid("profile")
                .definition(
                    TextIndexDefinition.builder()
                        .fieldId(table.schema().findField("body").fieldId())
                        .build())
                .build());
    catalog.updateIndex(
        SparkIndexBuilder.build(
            spark, table, created, table.currentSnapshot().snapshotId(), "body", 1L));
  }

  /** The jobs a run launched, and the cost of every stage, so one task carrying all is visible. */
  static final class TaskTimes extends SparkListener {
    private final Map<Integer, String> stageNames = new ConcurrentHashMap<>();
    // per stage: tasks, maxTaskMs, shuffleWriteBytes, shuffleReadBytes, inputBytes, recordsRead
    private final Map<Integer, long[]> stageCosts = new ConcurrentHashMap<>();
    private final Map<Integer, List<Integer>> jobStages = new ConcurrentHashMap<>();
    private final Map<Integer, Long> jobStart = new ConcurrentHashMap<>();
    private final Map<Integer, Long> jobTime = new ConcurrentHashMap<>();

    @Override
    public void onJobStart(SparkListenerJobStart start) {
      List<Integer> stages = new ArrayList<>();
      scala.collection.Iterator<StageInfo> infos = start.stageInfos().iterator();
      while (infos.hasNext()) {
        stages.add(infos.next().stageId());
      }

      jobStages.put(start.jobId(), stages);
      jobStart.put(start.jobId(), start.time());
    }

    @Override
    public void onJobEnd(SparkListenerJobEnd end) {
      Long start = jobStart.get(end.jobId());
      jobTime.put(end.jobId(), start == null ? -1 : end.time() - start);
    }

    @Override
    public void onStageCompleted(SparkListenerStageCompleted completed) {
      StageInfo info = completed.stageInfo();
      stageNames.put(info.stageId(), info.name());
      long[] cost = stageCosts.computeIfAbsent(info.stageId(), id -> new long[6]);
      synchronized (cost) {
        cost[0] = info.numTasks();
        TaskMetrics metrics = info.taskMetrics();
        if (metrics != null) {
          cost[2] = metrics.shuffleWriteMetrics().bytesWritten();
          cost[3] = metrics.shuffleReadMetrics().totalBytesRead();
          cost[4] = metrics.inputMetrics().bytesRead();
          cost[5] = metrics.inputMetrics().recordsRead();
        }
      }
    }

    @Override
    public void onTaskEnd(SparkListenerTaskEnd end) {
      long[] cost = stageCosts.computeIfAbsent(end.stageId(), id -> new long[6]);
      synchronized (cost) {
        cost[1] = Math.max(cost[1], end.taskInfo().duration());
      }
    }

    void reset() {
      stageNames.clear();
      stageCosts.clear();
      jobStages.clear();
      jobStart.clear();
      jobTime.clear();
    }

    static void drain() {
      try {
        spark.sparkContext().listenerBus().waitUntilEmpty(10000L);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    String report() {
      StringBuilder text = new StringBuilder();
      new TreeMap<>(jobStages)
          .forEach(
              (job, stages) -> {
                text.append(
                    String.format(
                        Locale.ROOT, "%n    job %d: %dms", job, jobTime.getOrDefault(job, -1L)));
                stages.forEach(
                    stage -> {
                      long[] cost = stageCosts.getOrDefault(stage, new long[6]);
                      text.append(
                          String.format(
                              Locale.ROOT,
                              "%n      stage %d: tasks=%d maxTask=%dms shuffleWrite=%.1fMB "
                                  + "shuffleRead=%.1fMB input=%.1fMB/%d rows %s",
                              stage,
                              cost[0],
                              cost[1],
                              cost[2] / 1e6,
                              cost[3] / 1e6,
                              cost[4] / 1e6,
                              cost[5],
                              stageNames.get(stage)));
                    });
              });
      return text.toString();
    }
  }
}
