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
package org.apache.iceberg.spark.procedures;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.index.CreateIndexRequest;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.IndexSnapshot;
import org.apache.iceberg.index.SidecarIndexCatalog;
import org.apache.iceberg.index.spark.SparkIndexBuilder;
import org.apache.iceberg.index.text.TextIndexDefinition;
import org.apache.iceberg.index.text.TextIndexTypes;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestIndexPublicationLifecycle {
  private static SparkSession spark;
  private static Path warehouse;

  @BeforeAll
  static void start(@TempDir Path directory) {
    warehouse = directory;
    TextIndexTypes.register();
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("review-today")
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
  static void stop() {
    spark.stop();
  }

  private static Table table(String name) {
    return new HadoopTables(spark.sessionState().newHadoopConf())
        .load(warehouse.resolve("db/" + name).toUri().toString());
  }

  @Test
  void olderPublicationShouldNotRegressLatest() {
    spark.sql("CREATE TABLE local.db.ordering (id bigint, body string) USING iceberg");
    spark.sql("INSERT INTO local.db.ordering VALUES (1, 'ice')");
    spark.sql("CALL local.system.build_text_index(table => 'db.ordering', column => 'body')");
    Table table = table("ordering");
    SidecarIndexCatalog catalog = new SidecarIndexCatalog(table.io(), table);
    IndexMetadata initial = catalog.listIndexes(table.location()).get(0);
    spark.sql("INSERT INTO local.db.ordering VALUES (2, 'ice')");
    table.refresh();
    long older = table.currentSnapshot().snapshotId();
    spark.sql("INSERT INTO local.db.ordering VALUES (3, 'ice')");
    table.refresh();
    long newer = table.currentSnapshot().snapshotId();
    IndexSnapshot olderBuild =
        SparkIndexBuilder.refresh(spark, table, initial, older, "body", 10L).latestSnapshot();
    IndexSnapshot newerBuild =
        SparkIndexBuilder.refresh(spark, table, initial, newer, "body", 11L).latestSnapshot();
    VectorIndexProcedures.publishSnapshot(catalog, table, initial.indexUuid(), newerBuild);
    VectorIndexProcedures.publishSnapshot(catalog, table, initial.indexUuid(), olderBuild);
    assertThat(
            catalog
                .loadIndex(table.location(), initial.indexUuid())
                .latestSnapshot()
                .sourceTableSnapshotId())
        .isEqualTo(newer);
  }

  @Test
  void cleanupShouldLeaveFilesOfAnUnpublishedBuild() {
    spark.sql("CREATE TABLE local.db.cleanup_race (id bigint, body string) USING iceberg");
    spark.sql("INSERT INTO local.db.cleanup_race VALUES (1, 'ice')");
    Table table = table("cleanup_race");
    SidecarIndexCatalog catalog = new SidecarIndexCatalog(table.io(), table);
    IndexMetadata initial =
        catalog.createIndex(
            CreateIndexRequest.builder()
                .tableLocation(table.location())
                .tableUuid(table.uuid().toString())
                .indexUuid("cleanup-race")
                .definition(TextIndexDefinition.builder().fieldId(2).build())
                .build());
    IndexSnapshot pending =
        SparkIndexBuilder.build(
                spark, table, initial, table.currentSnapshot().snapshotId(), "body", 10L)
            .latestSnapshot();
    assertThat(table.io().newInputFile(pending.statisticsLocation()).exists()).isTrue();
    spark.sql("CALL local.system.cleanup_index_files(table => 'db.cleanup_race')");
    VectorIndexProcedures.publishSnapshot(catalog, table, initial.indexUuid(), pending);
    assertThat(table.io().newInputFile(pending.statisticsLocation()).exists()).isTrue();
    spark.sql("CALL local.system.cleanup_index_files(table => 'db.cleanup_race')");
    assertThat(table.io().newInputFile(pending.indexManifestList()).exists()).isTrue();
    assertThat(
            spark
                .sql(
                    "SELECT id FROM search(table => 'local.db.cleanup_race', "
                        + "query => 'ice', search_columns => array('body'))")
                .head()
                .getLong(0))
        .isEqualTo(1L);
  }

  @Test
  void refreshCoversManifestOnlySnapshotWithoutRebuilding() {
    spark.sql("CREATE TABLE local.db.metadata_only (id bigint, body string) USING iceberg");
    spark.sql("INSERT INTO local.db.metadata_only VALUES (1, 'ice')");
    spark.sql("CALL local.system.build_text_index(table => 'db.metadata_only', column => 'body')");
    Table table = table("metadata_only");
    SidecarIndexCatalog catalog = new SidecarIndexCatalog(table.io(), table);
    IndexSnapshot previous = catalog.listIndexes(table.location()).get(0).latestSnapshot();
    table.rewriteManifests().rewriteIf(manifest -> true).commit();
    table.refresh();
    assertThat(table.currentSnapshot().snapshotId()).isNotEqualTo(previous.sourceTableSnapshotId());

    spark.sql(
        "CALL local.system.refresh_text_index(table => 'db.metadata_only', column => 'body')");
    IndexMetadata refreshed = catalog.listIndexes(table.location()).get(0);
    IndexSnapshot snapshot =
        refreshed.snapshotForSourceTableSnapshot(table.currentSnapshot().snapshotId());
    assertThat(snapshot).isNotNull();
    assertThat(snapshot.indexManifestList()).isEqualTo(previous.indexManifestList());
    assertThat(snapshot.statisticsLocation()).isEqualTo(previous.statisticsLocation());
    assertThat(
            spark
                .sql(
                    "SELECT id FROM search(table => 'local.db.metadata_only', "
                        + "query => 'ice', search_columns => array('body'))")
                .head()
                .getLong(0))
        .isEqualTo(1L);
    assertThat(catalog.dropIndex(table.location(), refreshed.indexUuid(), true)).isTrue();
    assertThat(table.io().newInputFile(previous.indexManifestList()).exists()).isFalse();
    assertThat(table.io().newInputFile(previous.statisticsLocation()).exists()).isFalse();
  }
}
