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

import java.nio.file.Path;
import java.util.List;
import org.apache.iceberg.index.vector.VectorIndexTypes;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * An indexed search over a Lance-backed table, through the same entry point a caller uses.
 *
 * <p>The indexed path reads its index files the same way whichever data format the table uses, but
 * the rows behind the hits are read by the table's format, so a search over Lance exercises a path
 * parquet never does. This test exists because that path was the one place a cluster run returned
 * no rows while everything else looked healthy.
 */
class TestLanceVectorSearch {

  private static SparkSession spark;

  @BeforeAll
  static void startSpark(@TempDir Path warehouse) {
    VectorIndexTypes.register();
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("lance-vector-search")
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
  public void searchReturnsRowsFromALanceTable() {
    String table = "local.db.lance_vectors";
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
    spark.sql("DROP TABLE IF EXISTS " + table);
    spark.sql(
        "CREATE TABLE "
            + table
            + " (id bigint, embedding array<double>) USING iceberg "
            + "TBLPROPERTIES ('write.format.default' = 'lance')");
    spark.sql(
        String.format(
            "INSERT INTO %s SELECT id, array(CAST(id AS double), CAST(100 - id AS double)) "
                + "FROM range(0, 200)",
            table));
    spark.sql(
        String.format(
            "CALL local.system.build_vector_index("
                + "table => '%s', column => 'embedding', dimension => 2, metric => 'euclidean')",
            table));

    List<Row> single =
        spark
            .sql(
                String.format(
                    "CALL local.system.vector_search(table => '%s', column => 'embedding', "
                        + "query => array(0.0d, 100.0d), k => 3)",
                    table))
            .collectAsList();
    assertThat(single).isNotEmpty();
    assertThat(single.get(0).getString(3)).contains("\"id\":0");

    List<Row> batch =
        spark
            .sql(
                String.format(
                    "CALL local.system.vector_search_batch(table => '%s', column => 'embedding', "
                        + "queries => array('0.0,100.0', '5.0,95.0'), k => 3)",
                    table))
            .collectAsList();
    assertThat(batch).isNotEmpty();
    assertThat(batch.get(0).getString(4)).contains("\"id\":0");

    // the same query answered again has to give the same rows
    List<Row> again =
        spark
            .sql(
                String.format(
                    "CALL local.system.vector_search_batch(table => '%s', column => 'embedding', "
                        + "queries => array('0.0,100.0', '5.0,95.0'), k => 3)",
                    table))
            .collectAsList();
    assertThat(again).hasSameSizeAs(batch);
    for (int i = 0; i < batch.size(); i++) {
      assertThat(again.get(i).toString()).isEqualTo(batch.get(i).toString());
    }
  }
}
