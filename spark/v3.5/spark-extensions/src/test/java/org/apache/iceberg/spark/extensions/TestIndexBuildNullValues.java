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
import org.apache.iceberg.index.text.TextIndexTypes;
import org.apache.iceberg.index.vector.VectorIndexTypes;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A row with no value is skipped by the index types that cannot hold one, and kept by the ones that
 * can.
 *
 * <p>A vector index stores a vector per entry, so a null vector has nothing to store. A text index
 * stores a document's tokens, and a null document is a document with no tokens: dropping it would
 * remove that document from the document count and from the average document length that every BM25
 * score is computed from, which changes the scores of the other documents. The rule therefore
 * belongs to the index type, not to the build.
 */
class TestIndexBuildNullValues {
  private static SparkSession spark;

  @BeforeAll
  static void startSpark(@TempDir Path warehouse) {
    VectorIndexTypes.register();
    TextIndexTypes.register();
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("index-build-null-values")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
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
  void aTextIndexKeepsADocumentWithNoTokens() {
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
    spark.sql("CREATE TABLE local.db.null_text (id bigint, body string) USING iceberg");
    spark.sql(
        "INSERT INTO local.db.null_text VALUES (1, 'snow leopard'), (2, 'snow storm'), "
            + "(3, null)");
    spark.sql("CALL local.system.build_text_index(table => 'db.null_text', column => 'body')");

    // the null document is still a document: it contributes to the document count, which is what
    // the corpus statistics are computed from
    assertThat(entries("null_text")).isEqualTo(3L);

    List<Row> hits =
        spark
            .sql(
                "CALL local.system.text_search(table => 'db.null_text', column => 'body', "
                    + "query => 'snow', k => 2)")
            .collectAsList();
    assertThat(hits).as("the documents that hold the term").isNotEmpty();
  }

  @Test
  void aVectorIndexOverALanceTableSkipsARowWithNoVector() {
    // the same rule on the other file format: the build reads the rows through the table's own
    // reader, so a null that comes back as null there is skipped the same way
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db");
    spark.sql(
        "CREATE TABLE local.db.null_lance (id bigint, embedding array<double>) USING iceberg "
            + "TBLPROPERTIES ('write.format.default' = 'lance')");
    spark.sql(
        "INSERT INTO local.db.null_lance VALUES (1, array(1.0d, 0.0d)), (2, array(0.0d, 2.0d)), "
            + "(3, null)");
    spark.sql(
        "CALL local.system.build_vector_index(table => 'db.null_lance', column => 'embedding', "
            + "dimension => 2, metric => 'euclidean', structure => 'flat')");

    assertThat(entries("null_lance")).as("the rows that have a vector").isEqualTo(2L);

    List<Row> hits =
        spark
            .sql(
                "CALL local.system.vector_search(table => 'db.null_lance', "
                    + "column => 'embedding', query => array(1.0d, 0.0d), k => 3)")
            .collectAsList();
    assertThat(hits).hasSize(2);
  }

  private static long entries(String table) {
    return spark
        .sql("CALL local.system.list_indexes(table => 'db." + table + "')")
        .first()
        .getLong(6);
  }
}
