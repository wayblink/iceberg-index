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
import static org.assertj.core.api.Assertions.within;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.index.vector.VectorIndexTypes;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The typed entry points answer what the equivalent SQL query answers.
 *
 * <p>{@code vector_search} is a promise about a question, not about a plan: whether the rows come
 * from an index or from a scan, a caller that wrote {@code ORDER BY vec_distance(...) LIMIT k} has
 * to get the same rows with the same distances, and {@code search} the same rows and scores as
 * {@code ORDER BY text_score(...) DESC}. These cases compare the two answers row by row over the
 * shapes that used to differ: a null vector, a deleted row, an empty table, a table without a
 * usable index, and a filter above the limit.
 */
class TestTypedSearchEquivalence {

  private static final String COLUMNS =
      "(id bigint, body string, embedding array<double>) USING iceberg";
  private static final double DISTANCE_TOLERANCE = 1e-6;
  private static final double SCORE_TOLERANCE = 1e-9;

  private static SparkSession spark;

  @BeforeAll
  static void startSpark(@TempDir Path warehouse) {
    VectorIndexTypes.register();
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("typed-search-equivalence")
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
  public void vectorSearchAnswersWhatTheOrderByQueryAnswers() {
    String table = table("typed_vector", true);
    List<Row> typed = vectorSearch(table, "array(0.9d, 0.1d)", 3);
    List<Row> ordered = vectorOrderBy(table, "array(0.9d, 0.1d)", 3);

    assertThat(ids(typed)).isEqualTo(ids(ordered));
    for (int i = 0; i < typed.size(); i += 1) {
      assertThat(typed.get(i).getDouble(2))
          .as("row %s reports the distance the query computes", i)
          .isCloseTo(ordered.get(i).getDouble(2), within(DISTANCE_TOLERANCE));
    }
  }

  @Test
  public void textSearchAnswersWhatTheOrderByQueryAnswers() {
    String table = table("typed_text", false);
    spark.sql(
        String.format(
            "CALL local.system.build_text_index(table => 'db.%s', column => 'body')", table));

    List<Row> typed = textSearch(table, "fox", 2);
    List<Row> ordered = textOrderBy(table, "fox", 2);

    assertThat(ids(typed)).isEqualTo(ids(ordered));
    for (int i = 0; i < typed.size(); i += 1) {
      assertThat(typed.get(i).getDouble(2))
          .as("row %s reports the score the query computes", i)
          .isCloseTo(ordered.get(i).getDouble(2), within(SCORE_TOLERANCE));
    }
  }

  @Test
  public void aDeletedRowIsInNeitherAnswer() throws Exception {
    String table = table("typed_deletes", true);
    Table icebergTable = Spark3Util.loadIcebergTable(spark, "local.db." + table);

    // a position delete from SQL, and an equality delete written the way another engine writes it
    spark.sql(String.format("DELETE FROM local.db.%s WHERE id = 1", table));
    DeleteFile equalityDelete = writeEqualityDelete(icebergTable, 2L);
    icebergTable.newRowDelta().addDeletes(equalityDelete).commit();

    List<Row> typed = vectorSearch(table, "array(0.9d, 0.1d)", 5);
    List<Row> ordered = vectorOrderBy(table, "array(0.9d, 0.1d)", 5);
    System.err.println("DEBUG typed=" + typed + " ordered=" + ordered);
    System.err.println(
        "DEBUG raw="
            + spark
                .sql(
                    String.format(
                        "SELECT id, embedding IS NULL AS is_null, size(embedding) AS n FROM local.db.%s",
                        table))
                .collectAsList());

    assertThat(ids(typed)).isEqualTo(ids(ordered));
    assertThat(ids(typed)).doesNotContain(1L, 2L);
  }

  @Test
  public void anEmptyTableAnswersNoRows() {
    String table = table("typed_empty", false);
    spark.sql(String.format("DELETE FROM local.db.%s", table));

    assertThat(vectorSearch(table, "array(1.0d, 0.0d)", 3)).isEmpty();
    assertThat(vectorOrderBy(table, "array(1.0d, 0.0d)", 3)).isEmpty();
  }

  @Test
  public void withoutAnIndexTheAnswerIsStillTheSame() {
    String table = table("typed_no_index", false);

    List<Row> typed = vectorSearch(table, "array(0.9d, 0.1d)", 3);
    List<Row> ordered = vectorOrderBy(table, "array(0.9d, 0.1d)", 3);
    assertThat(ids(typed)).isEqualTo(ids(ordered));

    // and it is answered by a scan: no index source in the plan
    String plan =
        spark
            .sql(
                String.format(
                    "EXPLAIN SELECT * FROM vector_search(table => 'local.db.%s', "
                        + "query_vector => array(0.9d, 0.1d), vector_column => 'embedding', k => 3)",
                    table))
            .collectAsList()
            .stream()
            .map(row -> row.getString(0))
            .collect(Collectors.joining("\n"));
    assertThat(plan).doesNotContain("VectorSearchSource");
  }

  @Test
  public void aFilterNarrowsWhatTheLimitKept() {
    String table = table("typed_filter", true);

    // the current contract takes the k nearest and then filters them: a row outside the top-k is
    // not brought back because a filter would have kept it
    List<Row> top = vectorSearch(table, "array(1.0d, 0.0d)", 2);
    List<Row> filtered =
        spark
            .sql(
                String.format(
                    "SELECT id, _distance FROM vector_search(table => 'local.db.%s', "
                        + "query_vector => array(1.0d, 0.0d), vector_column => 'embedding', k => 2) "
                        + "WHERE id = 4",
                    table))
            .collectAsList();

    assertThat(ids(top)).doesNotContain(4L);
    assertThat(filtered).isEmpty();
  }

  private static String table(String name, boolean withVectorIndex) {
    spark.sql(String.format("CREATE TABLE local.db.%s %s", name, COLUMNS));
    spark.sql(
        String.format(
            "INSERT INTO local.db.%s VALUES "
                + "(1, 'the fox jumps', array(1.0d, 0.0d)), "
                + "(2, 'a dog sleeps', array(0.0d, 1.0d)), "
                + "(3, 'a fox and a dog', array(0.7071d, 0.7071d)), "
                + "(4, 'nothing to see', array(0.0d, -1.0d))",
            name));
    if (withVectorIndex) {
      spark.sql(
          String.format(
              "CALL local.system.build_vector_index("
                  + "table => 'db.%s', column => 'embedding', dimension => 2, "
                  + "structure => 'FLAT', metric => 'EUCLIDEAN')",
              name));
    }

    return name;
  }

  private static List<Row> vectorSearch(String table, String query, int k) {
    return spark
        .sql(
            String.format(
                "SELECT id, body, _distance FROM vector_search(table => 'local.db.%s', "
                    + "query_vector => %s, vector_column => 'embedding', k => %d, "
                    + "distance_type => 'euclidean')",
                table, query, k))
        .collectAsList();
  }

  private static List<Row> vectorOrderBy(String table, String query, int k) {
    return spark
        .sql(
            String.format(
                "SELECT id, body, vec_distance(embedding, %s) AS distance FROM local.db.%s "
                    + "ORDER BY distance LIMIT %d",
                query, table, k))
        .collectAsList();
  }

  private static List<Row> textSearch(String table, String query, int k) {
    return spark
        .sql(
            String.format(
                "SELECT id, body, _score FROM search(table => 'local.db.%s', query => '%s', "
                    + "search_columns => array('body'), k => %d)",
                table, query, k))
        .collectAsList();
  }

  private static List<Row> textOrderBy(String table, String query, int k) {
    return spark
        .sql(
            String.format(
                "SELECT id, body, text_score(body, '%s') AS score FROM local.db.%s "
                    + "ORDER BY score DESC LIMIT %d",
                query, table, k))
        .collectAsList();
  }

  private static List<Long> ids(List<Row> rows) {
    return rows.stream().map(row -> row.getLong(0)).collect(Collectors.toList());
  }

  /** Writes one equality delete row into a delete file in the table's own format. */
  private static DeleteFile writeEqualityDelete(Table table, long id) throws Exception {
    Schema keySchema = table.schema().select("id");
    EncryptedOutputFile out =
        EncryptedFiles.plainAsEncryptedOutput(
            table
                .io()
                .newOutputFile(table.location() + "/data/equality-delete-" + id + ".parquet"));
    EqualityDeleteWriter<Record> writer =
        FormatModelRegistry.equalityDeleteWriteBuilder(FileFormat.PARQUET, Record.class, out)
            .schema(keySchema)
            .spec(PartitionSpec.unpartitioned())
            .equalityFieldIds(keySchema.columns().get(0).fieldId())
            .build();
    try (writer) {
      writer.write(GenericRecord.create(keySchema).copy("id", id));
    }

    return writer.toDeleteFile();
  }
}
