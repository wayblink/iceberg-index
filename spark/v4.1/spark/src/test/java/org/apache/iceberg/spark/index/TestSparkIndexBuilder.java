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

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.index.CreateIndexRequest;
import org.apache.iceberg.index.IndexCatalog;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.IndexQuery;
import org.apache.iceberg.index.IndexReader;
import org.apache.iceberg.index.IndexSearchResult;
import org.apache.iceberg.index.IndexSnapshot;
import org.apache.iceberg.index.SidecarIndexCatalog;
import org.apache.iceberg.index.VectorIndexAdapter;
import org.apache.iceberg.index.VectorIndexDefinition;
import org.apache.iceberg.index.VectorIndexDefinition.Metric;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.TestBase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

class TestSparkIndexBuilder extends TestBase {

  private static final String TABLE_NAME = "default.vector_index_source";

  @BeforeAll
  public static void configureSparkCatalog() {
    spark
        .conf()
        .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog");
    spark.conf().set("spark.sql.catalog.spark_catalog.type", "hive");
  }

  @BeforeEach
  public void setupHmsUri() {
    spark.sessionState().catalogManager().reset();
    spark.conf().set("spark.hadoop." + METASTOREURIS.varname, hiveConf.get(METASTOREURIS.varname));
    spark.conf().set("spark.sql.catalog.spark_catalog.uri", hiveConf.get(METASTOREURIS.varname));
  }

  @Test
  public void buildsIndexFromTableSnapshot() throws IOException {
    sql("CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg", TABLE_NAME);
    sql("INSERT INTO %s VALUES (1, array(1.0, 0.0)), (2, array(0.0, 1.0))", TABLE_NAME);
    sql("INSERT INTO %s VALUES (3, array(0.7, 0.7))", TABLE_NAME);

    Table table = catalog.loadTable(TableIdentifier.parse(TABLE_NAME));
    long snapshotId = table.currentSnapshot().snapshotId();

    IndexCatalog indexCatalog = new SidecarIndexCatalog(table.io());
    IndexMetadata metadata =
        indexCatalog.createIndex(
            CreateIndexRequest.builder()
                .tableLocation(table.location())
                .tableUuid(table.uuid().toString())
                .indexUuid("index-uuid")
                .definition(
                    VectorIndexDefinition.builder()
                        .fieldId(2)
                        .dimension(2)
                        .metric(Metric.COSINE)
                        .build())
                .build());

    IndexMetadata updated =
        SparkIndexBuilder.build(spark, table, metadata, snapshotId, "embedding", 100L);

    assertThat(updated.snapshots()).hasSize(1);
    IndexSnapshot snapshot = updated.snapshots().get(0);
    assertThat(snapshot.sourceTableSnapshotId()).isEqualTo(snapshotId);
    assertThat(snapshot.parentIndexSnapshotId()).isNull();

    VectorIndexAdapter adapter =
        new VectorIndexAdapter((VectorIndexDefinition) updated.definition());
    IndexReader reader = new IndexReader(updated, table.io());
    List<IndexSearchResult> results =
        reader.search(
            IndexQuery.of(snapshotId, adapter.encodeQuery(new float[] {0.95f, 0.05f}), 2));
    assertThat(results).hasSize(2);

    // resolve the row references back to rows and check the _file + _pos anchors
    assertThat(vectorsByPosition(results.get(0).dataFile()).get(results.get(0).rowPosition()))
        .containsExactly(1.0, 0.0);
    assertThat(vectorsByPosition(results.get(1).dataFile()).get(results.get(1).rowPosition()))
        .containsExactly(0.7, 0.7);

    // every data file of the snapshot is covered; other files are not
    assertThat(reader.uncoveredDataFiles(snapshotId, dataFiles(table))).isEmpty();
    assertThat(reader.uncoveredDataFiles(snapshotId, List.of("some-other-file.parquet")))
        .containsExactly("some-other-file.parquet");
  }

  private List<String> dataFiles(Table table) throws IOException {
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      List<String> files = Lists.newArrayList();
      for (FileScanTask task : tasks) {
        files.add(task.file().location().toString());
      }
      return files;
    }
  }

  private Map<Long, List<Double>> vectorsByPosition(String dataFile) {
    List<Tuple2<org.apache.spark.sql.Row, Long>> rows =
        spark.read().parquet(dataFile).select("embedding").toJavaRDD().zipWithIndex().collect();
    Map<Long, List<Double>> vectors = Maps.newHashMap();
    for (Tuple2<org.apache.spark.sql.Row, Long> row : rows) {
      vectors.put(row._2(), row._1().getList(0));
    }
    return vectors;
  }
}
