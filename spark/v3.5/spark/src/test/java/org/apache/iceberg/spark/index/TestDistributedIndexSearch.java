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
import java.util.Comparator;
import java.util.List;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.index.CreateIndexRequest;
import org.apache.iceberg.index.IndexCatalog;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.IndexQuery;
import org.apache.iceberg.index.IndexSearchResult;
import org.apache.iceberg.index.SidecarIndexCatalog;
import org.apache.iceberg.index.spark.DistributedIndexSearch;
import org.apache.iceberg.index.spark.SparkIndexBuilder;
import org.apache.iceberg.index.vector.VectorIndexAdapter;
import org.apache.iceberg.index.vector.VectorIndexDefinition;
import org.apache.iceberg.index.vector.VectorIndexDefinition.Metric;
import org.apache.iceberg.index.vector.VectorIndexTypes;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The merge of the per-index-file results of a distributed search, over more index files than the
 * query asks for hits, and with hits that tie across files.
 *
 * <p>Each task keeps the best hits of its own index file and a tree reduce keeps the best of what
 * the tasks produced, so the merge never puts every per-file hit on the driver. That is only the
 * same answer as sorting everything if the order of the adapter is applied exactly, including the
 * way it breaks ties, which is what this checks against a brute force top-k.
 */
class TestDistributedIndexSearch {

  private static final int INSERTS = 12;

  @BeforeAll
  static void registerVectorIndexType() {
    VectorIndexTypes.register();
  }

  private static SparkSession spark;
  private static Path warehouseDir;

  @BeforeAll
  static void startSpark(@TempDir Path warehouse) {
    warehouseDir = warehouse;
    spark =
        SparkSession.builder()
            .master("local[4]")
            .appName("distributed-index-search")
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
  public void mergesTheResultsOfManyIndexFilesInTheOrderOfTheAdapter() {
    spark.sql("CREATE TABLE local.db.spread (id bigint, embedding array<double>) USING iceberg");

    // one data file per insert, and one index file per data file, so the query has more index files
    // to merge than hits it asks for. The vectors repeat, so several rows of different files are at
    // exactly the same distance from the query, which is what the tie breaker is for
    float[][] vectors = {
      {0.0f, 0.0f}, {1.0f, 0.0f}, {0.0f, 1.0f}, {1.0f, 1.0f},
      {2.0f, 0.0f}, {0.0f, 2.0f}, {2.0f, 2.0f}, {1.0f, 1.0f},
      {0.0f, 0.0f}, {3.0f, 0.0f}, {0.0f, 3.0f}, {3.0f, 3.0f},
    };
    for (float[] vector : vectors) {
      spark.sql(
          String.format(
              "INSERT INTO local.db.spread VALUES (%d, array(%f, %f))",
              inserted++, vector[0], vector[1]));
    }
    assertThat(inserted).isEqualTo(INSERTS);

    Table table =
        new HadoopTables(spark.sessionState().newHadoopConf())
            .load(warehouseDir.resolve("db/spread").toUri().toString());
    long snapshotId = table.currentSnapshot().snapshotId();

    IndexCatalog indexCatalog = new SidecarIndexCatalog(table.io(), table);
    IndexMetadata metadata =
        indexCatalog.createIndex(
            CreateIndexRequest.builder()
                .tableLocation(table.location())
                .tableUuid(table.uuid().toString())
                .indexUuid("spread-vectors")
                .definition(
                    VectorIndexDefinition.builder()
                        .fieldId(2)
                        .dimension(2)
                        .metric(Metric.EUCLIDEAN)
                        .build())
                .build());
    // one entry per index file, which is what gives the query many index files to merge
    IndexMetadata built =
        SparkIndexBuilder.build(spark, table, metadata, snapshotId, "embedding", 1L);
    indexCatalog.updateIndex(built);

    VectorIndexAdapter adapter = new VectorIndexAdapter((VectorIndexDefinition) built.definition());
    float[] query = {0.5f, 0.5f};

    for (int topK : new int[] {1, 3, 5, INSERTS}) {
      CountingListener listener = new CountingListener();
      spark.sparkContext().addSparkListener(listener);
      DistributedIndexSearch.SearchOutcome outcome;
      try {
        outcome =
            DistributedIndexSearch.search(
                spark, built, table, IndexQuery.of(snapshotId, adapter.encodeQuery(query), topK));
      } finally {
        spark.sparkContext().removeSparkListener(listener);
      }

      assertThat(outcome.hits()).containsExactlyElementsOf(bruteForceTopK(query, topK));
      assertThat(outcome.moreHits()).isEqualTo(topK < INSERTS);
      // one task per index file: the index file is the unit of work, so a byte budget or a minimum
      // partition count has no say in how the search is split up
      // the size a budget is derived from is measured by preparing each payload once, which reads
      // the files on the driver; the search itself still splits into one task per index file, and
      // the first search of a session is the one that pays for the measurement
      assertThat(listener.tasks).isBetween(INSERTS, 2 * INSERTS);
    }
  }

  /** Counts the tasks a search ran, which is what its parallelism was. */
  private static final class CountingListener extends SparkListener {
    private int tasks;

    @Override
    public void onTaskEnd(SparkListenerTaskEnd end) {
      tasks += 1;
    }
  }

  private static long inserted = 0;

  /** The top-k hits of a brute force scan, in the order the adapter puts them. */
  private List<IndexSearchResult> bruteForceTopK(float[] query, int topK) {
    List<IndexSearchResult> all = Lists.newArrayList();
    // the metadata columns give the row reference of every row, which is what an index entry holds
    for (Row row :
        spark.sql("SELECT _file, _pos, embedding FROM local.db.spread").collectAsList()) {
      List<Double> vector = row.getList(2);
      double squared = 0.0;
      for (int i = 0; i < vector.size(); i++) {
        double difference = vector.get(i) - query[i];
        squared += difference * difference;
      }
      all.add(IndexSearchResult.of(row.getString(0), row.getLong(1), Math.sqrt(squared)));
    }

    all.sort(
        Comparator.comparingDouble(IndexSearchResult::score)
            .thenComparing(IndexSearchResult::dataFile)
            .thenComparingLong(IndexSearchResult::rowPosition));
    return all.size() > topK ? all.subList(0, topK) : all;
  }
}
