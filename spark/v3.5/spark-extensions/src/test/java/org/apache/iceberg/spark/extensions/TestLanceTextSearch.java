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
import java.util.stream.Collectors;
import org.apache.iceberg.index.text.TextIndexTypes;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A text search over a Lance-backed table, through the same entry points a caller uses.
 *
 * <p>The index files are read the same way whichever data format the table uses, but the documents
 * behind the hits are read by the table's format, and the deletes by their own. A fetch-back that
 * named parquet would fail on the file name alone, which is exactly what happened to the single
 * query form of the search while the batch form worked.
 */
class TestLanceTextSearch {

  private static SparkSession spark;

  @BeforeAll
  static void startSpark(@TempDir Path warehouse) {
    TextIndexTypes.register();
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("lance-text-search")
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
  public void searchReturnsDocumentsFromALanceTable() {
    String table = createIndexedTable("lance_text", "");

    List<Row> single =
        spark
            .sql(
                String.format(
                    "CALL local.system.text_search(table => '%s', column => 'body', "
                        + "query => 'iceberg table', k => 2)",
                    table))
            .collectAsList();
    assertThat(single).hasSize(2);
    Row best = single.get(0);
    assertThat(best.getString(0))
        .as("the hit is a file of the table's own format")
        .endsWith(".lance");
    assertThat(best.getString(3))
        .as("the document behind the hit is attached")
        .contains("\"id\":1");

    // the single query form and the batch form are the same search, so they have to answer with
    // the same hits rather than through two implementations that can drift apart
    List<Row> batch =
        spark
            .sql(
                String.format(
                    "CALL local.system.text_search_batch(table => '%s', column => 'body', "
                        + "queries => array('iceberg table'), k => 2)",
                    table))
            .collectAsList();
    assertThat(batch).hasSize(2);
    assertThat(hitsOf(single)).isEqualTo(hitsOf(batch));
  }

  @Test
  public void aDeletedDocumentComesBackWithoutARow() {
    String table =
        createIndexedTable("lance_text_deleted", ", 'write.delete.mode' = 'merge-on-read'");
    // the delete file follows the table's data format, so reading the documents has to apply a
    // Lance delete file rather than a parquet one; the index still covers the deleted document
    spark.sql(String.format("DELETE FROM %s WHERE id = 1", table));
    List<Row> deleteFiles =
        spark
            .sql(String.format("SELECT file_path FROM %s.all_files WHERE content != 0", table))
            .collectAsList();
    assertThat(deleteFiles)
        .as("the delete file is written in the table's format, not parquet")
        .isNotEmpty()
        .allSatisfy(row -> assertThat(row.getString(0)).endsWith(".lance"));

    List<Row> single =
        spark
            .sql(
                String.format(
                    "CALL local.system.text_search(table => '%s', column => 'body', "
                        + "query => 'iceberg table', k => 2)",
                    table))
            .collectAsList();
    assertThat(single).as("the deleted document is still a hit of the frozen index").hasSize(2);
    assertThat(hitsOf(single).stream().filter(row -> row.endsWith("|null")).count())
        .as("the deleted document has a hit but no row")
        .isEqualTo(1L);
  }

  private static String createIndexedTable(String name, String extraProperties) {
    String table = "local.db." + name;
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
    spark.sql("DROP TABLE IF EXISTS " + table);
    spark.sql(
        "CREATE TABLE "
            + table
            + " (id bigint, body string) USING iceberg "
            + "TBLPROPERTIES ('write.format.default' = 'lance'"
            + extraProperties
            + ")");
    spark.sql(
        String.format(
            "INSERT INTO %s VALUES (1, 'iceberg table format'), "
                + "(2, 'iceberg snapshot manifests'), (3, 'parquet columnar storage')",
            table));
    spark.sql(
        String.format(
            "CALL local.system.build_text_index(table => '%s', column => 'body')", table));
    return table;
  }

  /** The hits of a result as file, position, score and row, ignoring a batch's query id column. */
  private static List<String> hitsOf(List<Row> rows) {
    int offset = rows.get(0).size() == 5 ? 1 : 0;
    return rows.stream()
        .map(
            row ->
                row.getString(offset)
                    + "|"
                    + row.getLong(offset + 1)
                    + "|"
                    + row.getDouble(offset + 2)
                    + "|"
                    + (row.isNullAt(offset + 3) ? "null" : row.getString(offset + 3)))
        .collect(Collectors.toList());
  }
}
