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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Table;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.spark.VectorIndexes;
import org.apache.iceberg.index.text.TextIndexTypes;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Several text indexes over one column, told apart by name: the text side of the named-index
 * contract.
 *
 * <p>The index definitions differ in whether documents are lowercased, so the two indexes answer
 * the same query with different row counts. That makes "the query used the index it named" a
 * visible fact instead of an implementation detail.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class TestNamedTextIndexes extends ExtensionsTestBase {
  private static final String COEXIST = "named_text_coexist";
  private static final String CONFLICT = "named_text_conflict";
  private static final String UNNAMED = "named_text_unnamed";
  private static final String SELECT = "named_text_select";
  private static final String AMBIGUOUS = "named_text_ambiguous";
  private static final String DROP = "named_text_drop";
  private static final String REFRESH = "named_text_refresh";
  private static final String STATISTICS = "named_text_statistics";

  @BeforeAll
  static void registerTextIndexType() {
    TextIndexTypes.register();
  }

  @AfterEach
  public void removeTables() {
    for (String table :
        List.of(COEXIST, CONFLICT, UNNAMED, SELECT, AMBIGUOUS, DROP, REFRESH, STATISTICS)) {
      sql("DROP TABLE IF EXISTS %s PURGE", tableName(table));
    }
  }

  @TestTemplate
  public void twoNamedIndexesOverOneColumnCoexist() {
    String table = tableName(COEXIST);
    createTextTable(table);

    buildIndex(table, "body_lower", null);
    buildIndex(table, "body_case", false);

    List<Object[]> indexes = sql("CALL %s.system.list_indexes(table => '%s')", catalogName, table);
    assertThat(indexes).hasSize(2);
    assertThat(names(indexes)).containsExactlyInAnyOrder("body_lower", "body_case");
  }

  @TestTemplate
  public void aSecondBuildWithTheSameNameIsRefused() {
    String table = tableName(CONFLICT);
    createTextTable(table);
    buildIndex(table, "body_lower", null);

    assertThatThrownBy(() -> buildIndex(table, "body_lower", null))
        .hasMessageContaining("An index named body_lower already exists")
        .hasMessageContaining("column body");
  }

  @TestTemplate
  public void aSecondUnnamedIndexIsStillRefused() {
    String table = tableName(UNNAMED);
    createTextTable(table);
    buildIndex(table, null, null);

    assertThatThrownBy(() -> buildIndex(table, null, null))
        .hasMessageContaining("A text index already exists")
        .hasMessageContaining("name =>");
  }

  @TestTemplate
  public void aQueryCanChooseBetweenNamedIndexes() {
    String table = tableName(SELECT);
    createTextTable(table);
    // the documents differ in case, so lowercasing is what decides whether both are hits
    buildIndex(table, "body_lower", null);
    buildIndex(table, "body_case", false);

    // the lowercasing index sees both documents
    List<Object[]> lowercased =
        sql(
            "CALL %s.system.text_search(table => '%s', column => 'body', "
                + "query => 'iceberg', k => 10, index => 'body_lower')",
            catalogName, table);
    assertThat(lowercased).hasSize(2);

    // the case-sensitive index only sees the document that was already lowercase
    List<Object[]> caseSensitive =
        sql(
            "CALL %s.system.text_search(table => '%s', column => 'body', "
                + "query => 'iceberg', k => 10, index => 'body_case')",
            catalogName, table);
    assertThat(caseSensitive).hasSize(1);

    // a UUID selects the same index a name does
    List<Object[]> byUuid =
        sql(
            "CALL %s.system.text_search(table => '%s', column => 'body', "
                + "query => 'iceberg', k => 10, index => '%s')",
            catalogName, table, uuidOf(table, "body_case"));
    assertThat(rows(caseSensitive)).isEqualTo(rows(byUuid));

    // the batch entry point takes the same selection
    List<Object[]> batch =
        sql(
            "CALL %s.system.text_search_batch(table => '%s', column => 'body', "
                + "queries => array('iceberg'), k => 10, index => 'body_case')",
            catalogName, table);
    assertThat(batch).hasSize(1);

    // and so do the two table functions
    List<Object[]> hits =
        sql(
            "SELECT * FROM text_index_search('%s', 'body', 'iceberg', 10, index => 'body_case')",
            table);
    assertThat(hits).hasSize(1);

    List<Object[]> ranked =
        sql(
            "SELECT id FROM search(table => '%s', query => 'iceberg', "
                + "search_columns => array('body'), limit => 10, index => 'body_case')",
            table);
    assertThat(ranked).hasSize(1);
    assertThat(((Number) ranked.get(0)[0]).longValue()).isEqualTo(2L);
  }

  @TestTemplate
  public void aQueryWithoutANameRefusesToPickOne() {
    String table = tableName(AMBIGUOUS);
    createTextTable(table);
    buildIndex(table, "body_lower", null);
    buildIndex(table, "body_case", false);

    assertAmbiguous(
        () ->
            sql(
                "CALL %s.system.text_search(table => '%s', column => 'body', "
                    + "query => 'iceberg', k => 10)",
                catalogName, table));

    assertAmbiguous(
        () ->
            sql(
                "CALL %s.system.text_search_batch(table => '%s', column => 'body', "
                    + "queries => array('iceberg'), k => 10)",
                catalogName, table));

    assertAmbiguous(
        () -> sql("SELECT * FROM text_index_search('%s', 'body', 'iceberg', 10)", table));

    assertAmbiguous(
        () ->
            sql(
                "SELECT id FROM search(table => '%s', query => 'iceberg', "
                    + "search_columns => array('body'), limit => 10)",
                table));

    // refreshing without a name is ambiguous for the same reason: it must not silently rebuild
    // the first index it finds
    assertAmbiguous(
        () ->
            sql(
                "CALL %s.system.refresh_text_index(table => '%s', column => 'body')",
                catalogName, table));
  }

  @TestTemplate
  public void droppingOneNamedIndexLeavesTheOtherSearchable() {
    String table = tableName(DROP);
    createTextTable(table);
    buildIndex(table, "body_lower", null);
    buildIndex(table, "body_case", false);

    sql(
        "CALL %s.system.drop_index(table => '%s', column => 'body', index => 'body_case')",
        catalogName, table);

    List<Object[]> indexes = sql("CALL %s.system.list_indexes(table => '%s')", catalogName, table);
    assertThat(indexes).hasSize(1);
    assertThat(names(indexes)).containsExactly("body_lower");

    // with one index left, a query that names none works again
    List<Object[]> hits =
        sql(
            "CALL %s.system.text_search(table => '%s', column => 'body', "
                + "query => 'iceberg', k => 10)",
            catalogName, table);
    assertThat(hits).hasSize(2);
  }

  @TestTemplate
  public void refreshingOneNamedIndexLeavesTheOtherWhereItWas() {
    String table = tableName(REFRESH);
    createTextTable(table);
    buildIndex(table, "body_lower", null);
    buildIndex(table, "body_case", false);

    long lowerBefore = indexSnapshotId(table, "body_lower");
    long caseBefore = indexSnapshotId(table, "body_case");

    sql("INSERT INTO %s VALUES (3, 'iceberg data file manifests')", table);
    sql(
        "CALL %s.system.refresh_text_index(table => '%s', column => 'body', "
            + "index => 'body_lower')",
        catalogName, table);

    // a refresh changes the build, so its snapshot id differs; the arm nobody refreshed keeps its
    // build, which is the fact a named refresh exists to preserve
    assertThat(indexSnapshotId(table, "body_lower")).isNotEqualTo(lowerBefore);
    assertThat(indexSnapshotId(table, "body_case")).isEqualTo(caseBefore);

    // and the refreshed index sees the new document
    List<Object[]> hits =
        sql(
            "CALL %s.system.text_search(table => '%s', column => 'body', "
                + "query => 'data file manifests', k => 10, index => 'body_lower')",
            catalogName, table);
    assertThat(hits.stream().map(row -> (String) row[3]).collect(Collectors.toList()))
        .anyMatch(row -> row.contains("iceberg data file manifests"));
  }

  /**
   * A build chooses what its corpus statistics file is, and the choice is a property of the index.
   *
   * <p>A merged file is read once per query; a build that publishes none leaves every query summing
   * the shards' own counts, which is the same answer at a higher cost. The two indexes here answer
   * the same query with the same rows because the statistics are the same numbers either way.
   */
  @TestTemplate
  public void aBuildChoosesWhatItsStatisticsFileIs() {
    String table = tableName(STATISTICS);
    createTextTable(table);

    sql(
        "CALL %s.system.build_text_index(table => '%s', column => 'body', "
            + "name => 'body_merged', text_statistics => 'always')",
        catalogName, table);
    sql(
        "CALL %s.system.build_text_index(table => '%s', column => 'body', "
            + "name => 'body_summed', text_statistics => 'never')",
        catalogName, table);

    IndexMetadata merged = metadataOf(table, "body_merged");
    IndexMetadata summed = metadataOf(table, "body_summed");
    assertThat(merged.properties()).containsEntry(IndexMetadata.TEXT_STATISTICS_PROPERTY, "always");
    assertThat(summed.properties()).containsEntry(IndexMetadata.TEXT_STATISTICS_PROPERTY, "never");
    assertThat(merged.latestSnapshot().statisticsLocation()).isNotBlank();
    assertThat(summed.latestSnapshot().statisticsLocation())
        .as("a build told to skip the merge publishes no statistics file")
        .isNull();

    List<Object[]> byMerged =
        sql(
            "CALL %s.system.text_search(table => '%s', column => 'body', "
                + "query => 'iceberg iceberg', k => 10, index => 'body_merged')",
            catalogName, table);
    List<Object[]> bySummed =
        sql(
            "CALL %s.system.text_search(table => '%s', column => 'body', "
                + "query => 'iceberg iceberg', k => 10, index => 'body_summed')",
            catalogName, table);
    assertThat(rows(bySummed))
        .as("summing the shard counts answers what the merged file answers")
        .isEqualTo(rows(byMerged));

    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.build_text_index(table => '%s', column => 'body', "
                        + "name => 'body_bad', text_statistics => 'sometimes')",
                    catalogName, table))
        .hasMessageContaining("expected auto, always or never");
  }

  private void createTextTable(String table) {
    sql("CREATE TABLE %s (id bigint, body string) USING iceberg", table);
    sql(
        "INSERT INTO %s VALUES "
            + "(1, 'Iceberg table format for large datasets'), "
            + "(2, 'iceberg snapshot isolation and manifests')",
        table);
  }

  /**
   * Builds a named index, or an unnamed one when the name is null.
   *
   * @param lowercase whether documents are lowercased, or null to take the default
   */
  private void buildIndex(String table, String name, Boolean lowercase) {
    String nameArgument = name == null ? "" : ", name => '" + name + "'";
    String lowercaseArgument = lowercase == null ? "" : ", lowercase => " + lowercase;
    sql(
        "CALL %s.system.build_text_index(table => '%s', column => 'body'%s%s)",
        catalogName, table, lowercaseArgument, nameArgument);
  }

  private void assertAmbiguous(Runnable statement) {
    assertThatThrownBy(statement::run)
        .hasMessageContaining("has 2 text indexes")
        .hasMessageContaining("body_lower")
        .hasMessageContaining("body_case");
  }

  private static List<String> names(List<Object[]> indexes) {
    return indexes.stream().map(row -> (String) row[8]).collect(Collectors.toList());
  }

  private static List<String> rows(List<Object[]> hits) {
    return hits.stream().map(java.util.Arrays::toString).collect(Collectors.toList());
  }

  private String uuidOf(String table, String indexName) {
    return metadataOf(table, indexName).indexUuid();
  }

  private long indexSnapshotId(String table, String indexName) {
    return metadataOf(table, indexName).latestSnapshot().snapshotId();
  }

  private Table loadTable(String table) {
    try {
      return Spark3Util.loadIcebergTable(spark, table);
    } catch (ParseException | NoSuchTableException e) {
      throw new RuntimeException(e);
    }
  }

  private IndexMetadata metadataOf(String table, String indexName) {
    Table reloaded = loadTable(table);
    for (IndexMetadata metadata : VectorIndexes.indexesFor(reloaded, "body", "text")) {
      if (indexName.equals(metadata.name())) {
        return metadata;
      }
    }

    throw new IllegalStateException("No text index named " + indexName + " on " + table);
  }
}
