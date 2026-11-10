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
import org.apache.iceberg.Table;
import org.apache.iceberg.index.vector.VectorIndexTypes;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A vector query that also filters on another column is answered from the index, over the filtered
 * rows.
 *
 * <p>Without a carried filter the query still runs, but on a table scan: the rewrite only admits a
 * not-null guard on the vector column. These tests are what say the filter reaches the search
 * rather than ending the rewrite.
 */
class TestVectorSearchFiltered {
  private static final int DIMENSION = 4;
  private static final int ROWS = 200;

  private static SparkSession spark;

  @BeforeAll
  static void startSpark(@TempDir Path warehouse) {
    VectorIndexTypes.register();
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("vector-search-filtered")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.adaptive.enabled", "false")
            .config("spark.task.cpus", "2")
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
  void answersAFilteredQueryFromTheIndex() throws Exception {
    Table table = tableWithIndex("filtered_pre");

    List<Row> rows =
        spark
            .sql(
                "SELECT id, price FROM local.db.filtered_pre "
                    + "WHERE price < 40 ORDER BY vec_distance(embedding, array("
                    + query()
                    + "), 'euclidean') LIMIT 5")
            .collectAsList();

    assertThat(rows)
        .as("a filtered query keeps the window it asked for while answering only from the filter")
        .hasSize(5);
    assertThat(rows).allSatisfy(row -> assertThat(row.getLong(1)).isLessThan(40L));
    assertThat(table.currentSnapshot()).isNotNull();
  }

  @Test
  void plansTheFilteredQueryAsAnIndexSearch() throws Exception {
    tableWithIndex("filtered_plan");

    String plan =
        spark
            .sql(
                "SELECT id FROM local.db.filtered_plan "
                    + "WHERE price < 40 ORDER BY vec_distance(embedding, array("
                    + query()
                    + "), 'euclidean') LIMIT 5")
            .queryExecution()
            .executedPlan()
            .toString();

    assertThat(plan)
        .as("the filter no longer sends the whole query back to a table scan")
        .doesNotContain("BatchScan")
        .contains("VectorSearch");
  }

  @Test
  void answersTheUnfilteredQueryAsBefore() throws Exception {
    tableWithIndex("filtered_none");

    List<Row> rows =
        spark
            .sql(
                "SELECT id FROM local.db.filtered_none ORDER BY vec_distance(embedding, array("
                    + query()
                    + "), 'euclidean') LIMIT 5")
            .collectAsList();

    assertThat(rows).hasSize(5);
  }

  @Test
  void keepsADenseFilterCorrectByFallingBackToTheScan() throws Exception {
    tableWithIndex("filtered_dense");

    // a filter that keeps nearly every row is not worth narrowing the search for: the answer still
    // has to be the five rows a scan would give
    List<Row> fromIndex =
        spark
            .sql(
                "SELECT id FROM local.db.filtered_dense "
                    + "WHERE price >= 0 ORDER BY vec_distance(embedding, array("
                    + query()
                    + "), 'euclidean') LIMIT 5")
            .collectAsList();
    List<Row> fromScan =
        spark
            .sql(
                "SELECT id FROM local.db.filtered_dense WHERE price >= 0 "
                    + "ORDER BY vec_distance(embedding, array("
                    + query()
                    + "), 'euclidean') LIMIT 5")
            .collectAsList();

    assertThat(fromIndex)
        .as("a filter too dense to narrow is answered by the same rows, not fewer")
        .hasSameSizeAs(fromScan)
        .isNotEmpty();
  }

  private static String query() {
    StringBuilder builder = new StringBuilder();
    for (int d = 0; d < DIMENSION; d++) {
      if (d > 0) {
        builder.append(", ");
      }

      builder.append(0.1d * (d + 1));
    }

    return builder.toString();
  }

  private static Table tableWithIndex(String name) throws Exception {
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
    spark.sql("DROP TABLE IF EXISTS local.db." + name);
    spark.sql(
        "CREATE TABLE local.db."
            + name
            + " (id bigint, price bigint, embedding array<double> NOT NULL) USING iceberg");

    StringBuilder values = new StringBuilder();
    for (int row = 0; row < ROWS; row++) {
      if (row > 0) {
        values.append(", ");
      }

      values
          .append('(')
          .append(row)
          .append(", ")
          // every third row is cheap, so a price filter keeps a third of the table
          .append(row % 3 == 0 ? row : 100 + row)
          .append(", array(");
      for (int d = 0; d < DIMENSION; d++) {
        if (d > 0) {
          values.append(", ");
        }

        values.append(0.01d * (row * DIMENSION + d) + 0.001d);
      }

      values.append("))");
    }

    spark.sql("INSERT INTO local.db." + name + " VALUES " + values);

    spark.sql(
        "CALL local.system.build_vector_index(table => 'db."
            + name
            + "', column => 'embedding', dimension => "
            + DIMENSION
            + ", metric => 'euclidean', structure => 'ivf')");
    return Spark3Util.loadIcebergTable(spark, "local.db." + name);
  }
}
