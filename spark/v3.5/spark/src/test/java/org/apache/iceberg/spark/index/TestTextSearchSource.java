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

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.index.CreateIndexRequest;
import org.apache.iceberg.index.IndexCatalog;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.SidecarIndexCatalog;
import org.apache.iceberg.index.spark.IndexSearch;
import org.apache.iceberg.index.spark.SparkIndexBuilder;
import org.apache.iceberg.index.spark.VectorIndexes;
import org.apache.iceberg.index.text.TextIndexAdapter;
import org.apache.iceberg.index.text.TextIndexDefinition;
import org.apache.iceberg.index.text.TextIndexTypes;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import scala.runtime.BoxedUnit;

class TestTextSearchSource {
  private static Path warehouse;
  private static SparkSession spark;

  @BeforeAll
  static void startSpark(@TempDir Path warehouseDir) {
    warehouse = warehouseDir;
    TextIndexTypes.register();
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("text-search-source")
            .withExtensions(
                extensions -> {
                  IndexTableFunctions.register(extensions);
                  extensions.injectPlannerStrategy(VectorSearchStrategy::new);
                  return BoxedUnit.UNIT;
                })
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
  void appliesSparkEqualityInSignedZeroPartitions() {
    spark.sql(
        "CREATE TABLE local.db.signed_zero (id bigint, body string, d double) "
            + "USING iceberg PARTITIONED BY (d)");
    spark.sql(
        "INSERT INTO local.db.signed_zero VALUES "
            + "(1, 'ice', CAST('-0.0' AS DOUBLE)), (2, 'ice', CAST('0.0' AS DOUBLE))");
    buildIndex("signed_zero");

    List<Row> expected =
        spark.sql("SELECT id FROM " + search("signed_zero") + ") WHERE d = 0D").collectAsList();
    assertThat(expected).extracting(row -> row.getLong(0)).containsExactlyInAnyOrder(1L, 2L);
    assertThat(
            spark
                .sql("SELECT id FROM " + search("signed_zero") + ", filter => 'd = 0D')")
                .collectAsList())
        .containsExactlyInAnyOrderElementsOf(expected);
  }

  @Test
  void foldsCoercedFilterLiterals() {
    spark.sql("CREATE TABLE local.db.coerced_filter (id bigint, body string) USING iceberg");
    spark.sql("INSERT INTO local.db.coerced_filter VALUES (1, 'ice'), (2, 'ice')");
    buildIndex("coerced_filter");

    assertThat(
            spark
                .sql("SELECT id FROM " + search("coerced_filter") + ", filter => 'id = 2')")
                .collectAsList())
        .extracting(row -> row.getLong(0))
        .containsExactly(2L);
    assertThat(
            spark
                .sql("SELECT id FROM " + search("coerced_filter") + ", filter => 'id = NULL')")
                .collectAsList())
        .isEmpty();
  }

  @Test
  void evaluatesScalarFiltersWithoutPruning() {
    spark.sql("CREATE TABLE local.db.scalar_filter (id bigint, body string) USING iceberg");
    spark.sql("INSERT INTO local.db.scalar_filter VALUES (1, 'ice'), (2, 'ice')");
    buildIndex("scalar_filter");

    assertThat(
            spark
                .sql("SELECT id FROM " + search("scalar_filter") + ", filter => 'id % 2 = 0')")
                .collectAsList())
        .extracting(row -> row.getLong(0))
        .containsExactly(2L);
  }

  @Test
  void readsNestedResultColumns() {
    spark.sql(
        "CREATE TABLE local.db.nested_output "
            + "(id bigint, body string, payload struct<tags:array<map<string,int>>>) "
            + "USING iceberg TBLPROPERTIES ('write.format.default' = 'avro')");
    spark.sql(
        "INSERT INTO local.db.nested_output VALUES "
            + "(1, 'ice', NAMED_STRUCT('tags', ARRAY(MAP('year', 2026))))");
    buildIndex("nested_output");

    Row result = spark.sql("SELECT * FROM " + search("nested_output") + ")").head();
    assertThat(result.getLong(0)).isEqualTo(1L);
    assertThat(result.getString(1)).isEqualTo("ice");
    Row payload = result.getStruct(2);
    assertThat(payload.getList(0)).hasSize(1);
    assertThat(
            spark
                .sql("SELECT payload.tags[0]['year'] FROM " + search("nested_output") + ")")
                .head()
                .getInt(0))
        .isEqualTo(2026);
  }

  @Test
  void prunesFilesUsingFilterColumnBounds() {
    spark.sql(
        "CREATE TABLE local.db.file_metrics (id bigint, body string, tenant int) "
            + "USING iceberg");
    spark.sql("INSERT INTO local.db.file_metrics VALUES (1, 'ice', 1)");
    spark.sql("INSERT INTO local.db.file_metrics VALUES (2, 'ice', 2)");
    Table table = buildIndex("file_metrics");
    String excluded =
        table.currentSnapshot().addedDataFiles(table.io()).iterator().next().location();
    table.io().deleteFile(excluded);

    assertThat(
            spark
                .sql("SELECT id FROM " + search("file_metrics") + ", filter => 'tenant = 1')")
                .collectAsList())
        .extracting(row -> row.getLong(0))
        .containsExactly(1L);
  }

  @Test
  void resolvesOnlyTheRequestedColumnsOfAHit() {
    spark.sql(
        "CREATE TABLE local.db.projected_row (id bigint, body string, note string) USING iceberg");
    spark.sql(
        "INSERT INTO local.db.projected_row VALUES "
            + "(1, 'ice iceberg', 'first'), (2, 'ice vector', 'second')");
    Table table = buildIndex("projected_row");
    IndexMetadata metadata = VectorIndexes.findIndex(table, "body", "text");
    ByteBuffer query =
        new TextIndexAdapter((TextIndexDefinition) metadata.definition()).encodeQuery("iceberg");

    // null is the whole row, which is the contract CALL text_search answers with
    List<Row> wholeRows =
        IndexSearch.search(spark, table, "body", metadata, query, 2, null, null).collectAsList();
    List<String> allColumns =
        table.schema().columns().stream().map(Types.NestedField::name).collect(Collectors.toList());
    List<Row> explicitRows =
        IndexSearch.search(spark, table, "body", metadata, query, 2, allColumns, null)
            .collectAsList();
    assertThat(explicitRows).hasSameSizeAs(wholeRows);
    assertThat(explicitRows.get(0).getString(3)).isEqualTo(wholeRows.get(0).getString(3));

    List<Row> idOnly =
        IndexSearch.search(spark, table, "body", metadata, query, 2, List.of("id"), null)
            .collectAsList();
    assertThat(wholeRows).hasSize(1);
    assertThat(idOnly).hasSize(1);
    Row whole = wholeRows.get(0);
    Row projected = idOnly.get(0);
    // the projection decides what a row carries, not which hits the search returns
    assertThat(projected.getString(0)).isEqualTo(whole.getString(0));
    assertThat(projected.getLong(1)).isEqualTo(whole.getLong(1));
    assertThat(projected.getDouble(2)).isEqualTo(whole.getDouble(2));
    assertThat(whole.getString(3)).contains("\"id\"", "\"body\"", "\"note\"");
    assertThat(projected.getString(3)).contains("\"id\"").doesNotContain("\"body\"", "\"note\"");
  }

  private static String search(String name) {
    return "search(table => 'local.db."
        + name
        + "', query => 'ice', search_columns => array('body')";
  }

  private static Table buildIndex(String name) {
    Table table =
        new HadoopTables(spark.sessionState().newHadoopConf())
            .load(warehouse.resolve("db").resolve(name).toUri().toString());
    IndexCatalog catalog = new SidecarIndexCatalog(table.io(), table);
    IndexMetadata created =
        catalog.createIndex(
            CreateIndexRequest.builder()
                .tableLocation(table.location())
                .tableUuid(table.uuid().toString())
                .indexUuid(name)
                .definition(
                    TextIndexDefinition.builder()
                        .fieldId(table.schema().findField("body").fieldId())
                        .build())
                .build());
    catalog.updateIndex(
        SparkIndexBuilder.build(
            spark, table, created, table.currentSnapshot().snapshotId(), "body", 1L));
    return table;
  }
}
