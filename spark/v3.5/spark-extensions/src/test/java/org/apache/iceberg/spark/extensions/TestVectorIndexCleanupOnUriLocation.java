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
 * Cleanup of an index whose table lives under a location that names its scheme.
 *
 * <p>A table location is often written as a URI, and then the locations in the index metadata carry
 * the scheme while a directory listing hands back paths without it. Cleanup compares the two, so a
 * comparison that does not put them in the same form finds nothing referenced and deletes the index
 * it was asked to tidy, its live files included.
 */
class TestVectorIndexCleanupOnUriLocation {

  private static SparkSession spark;

  @BeforeAll
  static void startSpark(@TempDir Path warehouse) {
    // the warehouse is a URI, which is what puts the scheme into every location the index records
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("vector-index-cleanup-uri")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "4")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", warehouse.toUri().toString())
            .getOrCreate();
    VectorIndexTypes.register();
  }

  @AfterAll
  static void stopSpark() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  void cleanupKeepsTheFilesOfTheLiveIndex() {
    spark.sql(
        "CREATE TABLE local.db.uri_cleanup (id bigint, embedding array<double>) USING iceberg");
    spark.sql(
        "INSERT INTO local.db.uri_cleanup "
            + "SELECT id, array(CAST(id AS double), CAST(10 - id AS double)) FROM range(0, 10)");
    spark.sql(
        "CALL local.system.build_vector_index(table => 'db.uri_cleanup', column => 'embedding', "
            + "dimension => 2, metric => 'euclidean', structure => 'IVF', quantizer => 'RABITQ')");

    List<Row> before = nearest();
    assertThat(before).hasSize(3);

    List<Row> cleaned =
        spark
            .sql("CALL local.system.cleanup_index_files(table => 'db.uri_cleanup')")
            .collectAsList();
    assertThat(cleaned).hasSize(1);
    assertThat(cleaned.get(0).getLong(2))
        .as("nothing is unreferenced, so nothing may be deleted")
        .isZero();

    // the index is still whole: its metadata can be read and it still answers the query
    assertThat(
            spark.sql("CALL local.system.list_indexes(table => 'db.uri_cleanup')").collectAsList())
        .hasSize(1);
    assertThat(nearest()).isEqualTo(before);
  }

  private static List<Row> nearest() {
    return spark
        .sql(
            "SELECT id FROM local.db.uri_cleanup "
                + "ORDER BY vec_distance(embedding, array(0.0d, 10.0d)) LIMIT 3")
        .collectAsList();
  }
}
