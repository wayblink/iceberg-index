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
import java.util.List;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.index.CreateIndexRequest;
import org.apache.iceberg.index.IndexCatalog;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.IndexMetadataParser;
import org.apache.iceberg.index.IndexQuery;
import org.apache.iceberg.index.IndexReader;
import org.apache.iceberg.index.IndexSearchResult;
import org.apache.iceberg.index.SidecarIndexCatalog;
import org.apache.iceberg.index.VectorIndexAdapter;
import org.apache.iceberg.index.VectorIndexDefinition;
import org.apache.iceberg.index.VectorIndexDefinition.Metric;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import scala.Tuple2;

/**
 * End-to-end quickstart on static data: create a small table, build a vector index for it, publish
 * the index through the sidecar catalog, and query it back.
 *
 * <p>Runs on a local Spark session with a Hadoop catalog, so it needs no metastore. Run with:
 *
 * <pre>{@code
 * ./gradlew :iceberg-spark:iceberg-spark-4.1_2.13:test \
 *     --tests "org.apache.iceberg.spark.index.TestVectorIndexQuickstart" --info
 * }</pre>
 *
 * <p>The printed transcript shows every step: the static rows, the index metadata JSON, the index
 * file tree, and the ranked query results resolved back to table rows.
 */
class TestVectorIndexQuickstart {

  private static SparkSession spark;
  private static Path warehouseDir;

  @BeforeAll
  static void startSpark(@TempDir Path warehouse) {
    warehouseDir = warehouse;
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("vector-index-quickstart")
            .config("spark.ui.enabled", "false")
            .config(
                "spark.metrics.conf.*.sink.servlet.class",
                "org.apache.iceberg.spark.DummyMetricsServlet")
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
  public void buildsVectorIndexAndQueriesIt() {
    banner("STEP 1 · static data");
    spark.sql("CREATE TABLE local.db.vectors (id bigint, embedding array<double>) USING iceberg");
    spark.sql(
        "INSERT INTO local.db.vectors VALUES "
            + "(1, array(1.0, 0.0)), (2, array(0.9, 0.1)), (3, array(0.0, 1.0))");
    spark.sql(
        "INSERT INTO local.db.vectors VALUES (4, array(0.1, 0.95)), (5, array(-1.0, 0.0)), (6, array(0.0, -1.0))");
    spark.sql("SELECT * FROM local.db.vectors ORDER BY id").show(false);

    Table table =
        new HadoopTables(spark.sessionState().newHadoopConf())
            .load(warehouseDir.resolve("db/vectors").toUri().toString());
    long snapshotId = table.currentSnapshot().snapshotId();

    banner("STEP 2 · create the index (sidecar catalog, plan A)");
    IndexCatalog indexCatalog = new SidecarIndexCatalog(table.io());
    IndexMetadata metadata =
        indexCatalog.createIndex(
            CreateIndexRequest.builder()
                .tableLocation(table.location())
                .tableUuid(table.uuid().toString())
                .indexUuid("quickstart-vectors")
                .definition(
                    VectorIndexDefinition.builder()
                        .fieldId(2)
                        .dimension(2)
                        .metric(Metric.COSINE)
                        .build())
                .build());
    System.out.println("index location: " + metadata.location());

    banner("STEP 3 · build the index with Spark");
    IndexMetadata built =
        SparkIndexBuilder.build(spark, table, metadata, snapshotId, "embedding", 1000L);
    indexCatalog.updateIndex(built);
    System.out.println("index metadata after publish:");
    System.out.println(IndexMetadataParser.toJson(built, true));

    banner("STEP 4 · query through the core reader");
    VectorIndexAdapter adapter = new VectorIndexAdapter((VectorIndexDefinition) built.definition());
    IndexReader reader =
        new IndexReader(indexCatalog.loadIndex(table.location(), "quickstart-vectors"), table.io());
    List<IndexSearchResult> results =
        reader.search(IndexQuery.of(snapshotId, adapter.encodeQuery(new float[] {1.0f, 0.0f}), 3));

    banner("STEP 5 · ranked results, resolved back to table rows");
    for (IndexSearchResult result : results) {
      long id = idAtPosition(result.dataFile(), result.rowPosition());
      System.out.printf(
          "rank: id=%d  score=%.4f  (data file %s, row position %d)%n",
          id,
          result.score(),
          result.dataFile().substring(result.dataFile().lastIndexOf('/') + 1),
          result.rowPosition());
    }

    assertThat(results).hasSize(3);
    assertThat(results)
        .extracting(result -> idAtPosition(result.dataFile(), result.rowPosition()))
        .containsExactly(1L, 2L, 4L);
    assertThat(results)
        .extracting(IndexSearchResult::score)
        .isSortedAccordingTo(java.util.Comparator.reverseOrder());
    assertThat(reader.uncoveredDataFiles(snapshotId, dataFiles(table))).isEmpty();

    banner("quickstart complete: static data -> index -> query -> row resolution");
  }

  private List<String> dataFiles(Table table) {
    List<String> files = Lists.newArrayList();
    table.newScan().planFiles().forEach(task -> files.add(task.file().location().toString()));
    return files;
  }

  private long idAtPosition(String dataFile, long rowPosition) {
    List<Tuple2<Row, Long>> rows =
        spark.read().parquet(dataFile).select("id").toJavaRDD().zipWithIndex().collect();
    for (Tuple2<Row, Long> row : rows) {
      if (row._2() == rowPosition) {
        return row._1().getLong(0);
      }
    }
    throw new IllegalStateException("No row at position " + rowPosition + " in " + dataFile);
  }

  private static void banner(String text) {
    System.out.println();
    System.out.println("=== " + text + " ===");
  }
}
