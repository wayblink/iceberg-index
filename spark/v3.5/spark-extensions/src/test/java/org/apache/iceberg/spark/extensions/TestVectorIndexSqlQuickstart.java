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
 * End-to-end SQL walkthrough of the vector index: build, list, search and refresh, all through
 * Spark SQL {@code CALL} procedures, on a local Spark session with a Hadoop catalog.
 */
class TestVectorIndexSqlQuickstart {

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
            .master("local[2]")
            .appName("vector-index-sql-quickstart")
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
  public void buildListSearchAndRefreshThroughSql() {
    banner("STEP 1 · static data");
    spark.sql(
        "CREATE TABLE local.db.docs (id bigint, title string, embedding array<double>) USING iceberg");
    spark.sql(
        "INSERT INTO local.db.docs VALUES "
            + "(1, 'iceberg tables', array(1.0, 0.0)), "
            + "(2, 'vector search', array(0.9, 0.1)), "
            + "(3, 'parquet files', array(0.0, 1.0)), "
            + "(4, 'hnsw graphs', array(0.1, 0.95)), "
            + "(5, 'spark jobs', array(-1.0, 0.0)), "
            + "(6, 'delete files', array(0.0, -1.0))");
    spark.sql("SELECT * FROM local.db.docs ORDER BY id").show(false);

    banner("STEP 2 · build the index with one CALL");
    spark
        .sql(
            "CALL local.system.build_vector_index("
                + "table => 'db.docs', column => 'embedding', dimension => 2, metric => 'cosine')")
        .show(false);

    banner("STEP 3 · list the indexes");
    spark.sql("CALL local.system.list_vector_indexes(table => 'db.docs')").show(false);

    banner("STEP 4 · search the index with one CALL");
    // query close to (1.0, 0.0): the 'iceberg tables' row must come first
    spark
        .sql(
            "CALL local.system.vector_search("
                + "table => 'db.docs', column => 'embedding', "
                + "query => array(0.95d, 0.05d), k => 3)")
        .show(false);

    Row topHit =
        spark
            .sql(
                "CALL local.system.vector_search("
                    + "table => 'db.docs', column => 'embedding', "
                    + "query => array(0.95d, 0.05d), k => 1)")
            .head();
    assertThat(topHit.getString(3)).contains("\"title\":\"iceberg tables\"");

    banner("STEP 5 · new data arrives, refresh the index");
    spark.sql("INSERT INTO local.db.docs VALUES (7, 'puffin blobs', array(0.92, 0.12))");
    spark
        .sql("CALL local.system.refresh_vector_index(table => 'db.docs', column => 'embedding')")
        .show(false);

    banner("STEP 6 · the refreshed index finds the new row");
    spark
        .sql(
            "CALL local.system.vector_search("
                + "table => 'db.docs', column => 'embedding', "
                + "query => array(0.95d, 0.05d), k => 3)")
        .show(false);

    List<Row> refreshedHits =
        spark
            .sql(
                "CALL local.system.vector_search("
                    + "table => 'db.docs', column => 'embedding', "
                    + "query => array(0.95d, 0.05d), k => 3)")
            .collectAsList();
    // the newly inserted row (7) is now indexed and returned with its row JSON
    assertThat(refreshedHits).anyMatch(row -> row.getString(3).contains("\"id\":7"));
    assertThat(refreshedHits)
        .anyMatch(row -> row.getString(3).contains("\"title\":\"puffin blobs\""));

    banner("STEP 7 · HNSW (approximate) mode, same query, same top hit");
    spark.sql(
        "CREATE TABLE local.db.docs_hnsw (id bigint, title string, embedding array<double>) USING iceberg");
    spark.sql(
        "INSERT INTO local.db.docs_hnsw VALUES "
            + "(1, 'iceberg tables', array(1.0, 0.0)), "
            + "(2, 'vector search', array(0.9, 0.1)), "
            + "(3, 'parquet files', array(0.0, 1.0))");
    spark
        .sql(
            "CALL local.system.build_vector_index("
                + "table => 'db.docs_hnsw', column => 'embedding', dimension => 2, "
                + "metric => 'cosine', mode => 'HNSW', max_m => 8, ef_construction => 100, ef_search => 64)")
        .show(false);
    spark
        .sql(
            "CALL local.system.vector_search("
                + "table => 'db.docs_hnsw', column => 'embedding', "
                + "query => array(0.95d, 0.05d), k => 2)")
        .show(false);

    banner("DONE · index metadata tree on disk");
    System.out.println("warehouse: " + warehouseDir);
  }

  private static void banner(String text) {
    System.out.println();
    System.out.println("======== " + text + " ========");
  }
}
