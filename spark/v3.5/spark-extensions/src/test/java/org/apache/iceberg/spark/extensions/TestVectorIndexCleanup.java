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

import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.SidecarIndexCatalog;
import org.apache.iceberg.index.vector.VectorIndexTypes;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Verifies orphan file cleanup and its coordination with table snapshot expiration: unreferenced
 * files under the index tree are deleted, and index snapshots whose source table snapshot was
 * expired are dropped from the index metadata.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class TestVectorIndexCleanup extends ExtensionsTestBase {

  @BeforeAll
  static void registerVectorIndexType() {
    VectorIndexTypes.register();
  }

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName("vectors_cleanup_files_" + catalogName));
    sql("DROP TABLE IF EXISTS %s", tableName("vectors_cleanup_expire_" + catalogName));
  }

  @TestTemplate
  public void testDeletesUnreferencedFilesAndKeepsReferencedOnes() throws Exception {
    String tableName = tableName("vectors_cleanup_files_" + catalogName);
    sql("CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg", tableName);
    sql("INSERT INTO %s SELECT id, array(CAST(id AS double), 0.0) FROM range(0, 10)", tableName);
    sql(
        "CALL %s.system.build_vector_index("
            + "table => '%s', column => 'embedding', dimension => 2, metric => 'euclidean')",
        catalogName, tableName);

    int filesBefore = indexFilesOnDisk(tableName);

    // a leftover of a failed build: an unreferenced file inside the index tree
    writeOrphanFile(tableName);

    List<Object[]> output =
        sql("CALL %s.system.cleanup_vector_index_files(table => '%s')", catalogName, tableName);
    assertThat(output).hasSize(1);
    assertThat(output.get(0)[0]).isInstanceOf(String.class);
    assertThat(((Number) output.get(0)[1]).longValue()).isZero();
    assertThat(((Number) output.get(0)[2]).longValue()).isEqualTo(1L);
    assertThat(((Number) output.get(0)[3]).longValue()).isPositive();

    // the referenced files of the live index snapshot are kept, the orphan is gone
    assertThat(indexFilesOnDisk(tableName)).isEqualTo(filesBefore);
    assertThat(readOrphanFile(tableName)).isNull();

    // the index is still searchable after the cleanup
    assertThat(searchIds(tableName, 3)).containsExactly(0L, 1L, 2L);
  }

  @TestTemplate
  public void testExpiresIndexSnapshotsOfExpiredTableSnapshots() throws Exception {
    String tableName = tableName("vectors_cleanup_expire_" + catalogName);
    sql("CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg", tableName);
    sql("INSERT INTO %s SELECT id, array(CAST(id AS double), 0.0) FROM range(0, 10)", tableName);
    sql(
        "CALL %s.system.build_vector_index("
            + "table => '%s', column => 'embedding', dimension => 2, metric => 'euclidean')",
        catalogName, tableName);

    // a second table snapshot with a refreshed index snapshot bound to it
    sql("INSERT INTO %s SELECT id, array(CAST(id AS double), 0.0) FROM range(10, 20)", tableName);
    sql(
        "CALL %s.system.refresh_vector_index(table => '%s', column => 'embedding')",
        catalogName, tableName);

    int filesWithBothSnapshots = indexFilesOnDisk(tableName);

    // expire the first table snapshot: the index snapshot bound to it becomes stale
    sql(
        "CALL %s.system.expire_snapshots(table => '%s', older_than => TIMESTAMP '2100-01-01 00:00:00.000+0000', retain_last => 1)",
        catalogName, tableName);

    List<Object[]> output =
        sql("CALL %s.system.cleanup_vector_index_files(table => '%s')", catalogName, tableName);
    assertThat(output).hasSize(1);
    assertThat(((Number) output.get(0)[1]).longValue()).isEqualTo(1L);
    // the files referenced only by the expired index snapshot are deleted
    assertThat(((Number) output.get(0)[2]).longValue()).isPositive();
    assertThat(indexFilesOnDisk(tableName)).isLessThan(filesWithBothSnapshots);

    // the index still works and covers the current table snapshot
    List<Row> indexes =
        spark
            .sql("CALL " + catalogName + ".system.list_indexes(table => '" + tableName + "')")
            .collectAsList();
    assertThat(indexes).hasSize(1);
    assertThat(((Number) indexes.get(0).get(6)).longValue()).isEqualTo(20L);
    assertThat(searchIds(tableName, 2)).containsExactly(0L, 1L);
  }

  private List<Long> searchIds(String tableName, int topK) {
    return spark
        .sql(
            "CALL "
                + catalogName
                + ".system.vector_search(table => '"
                + tableName
                + "', column => 'embedding', query => array(0.0d, 0.0d), k => "
                + topK
                + ")")
        .collectAsList()
        .stream()
        .map(row -> parseId(row.getString(3)))
        .collect(Collectors.toList());
  }

  private int indexFilesOnDisk(String tableName) throws Exception {
    Path indexDir = indexDirectory(tableName);
    return countFiles(indexDir, "data") + countFiles(indexDir, "snapshots");
  }

  private int countFiles(Path indexDir, String subDir) throws Exception {
    Path directory = new Path(indexDir, subDir);
    FileSystem fileSystem = directory.getFileSystem(spark.sessionState().newHadoopConf());
    if (!fileSystem.exists(directory)) {
      return 0;
    }

    int count = 0;
    RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(directory, true);
    while (files.hasNext()) {
      files.next();
      count++;
    }
    return count;
  }

  private void writeOrphanFile(String tableName) throws Exception {
    Path orphan = new Path(indexDirectory(tableName), "data/orphan-from-failed-build.puffin");
    FileSystem fileSystem = orphan.getFileSystem(spark.sessionState().newHadoopConf());
    try (java.io.OutputStream out = fileSystem.create(orphan, true)) {
      out.write("leftover of a failed build".getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }
  }

  private FileStatus readOrphanFile(String tableName) throws Exception {
    Path orphan = new Path(indexDirectory(tableName), "data/orphan-from-failed-build.puffin");
    FileSystem fileSystem = orphan.getFileSystem(spark.sessionState().newHadoopConf());
    return fileSystem.exists(orphan) ? fileSystem.getFileStatus(orphan) : null;
  }

  private Path indexDirectory(String tableName) {
    String simpleName = tableName.substring(tableName.lastIndexOf('.') + 1);
    Table table = validationCatalog.loadTable(TableIdentifier.of("default", simpleName));
    List<IndexMetadata> indexes = new SidecarIndexCatalog(table.io()).listIndexes(table.location());
    assertThat(indexes).hasSize(1);
    return new Path(indexes.get(0).location());
  }

  private static long parseId(String rowJson) {
    int start = rowJson.indexOf("\"id\":") + 5;
    int end = rowJson.indexOf(',', start);
    return Long.parseLong(rowJson.substring(start, end == -1 ? rowJson.length() - 1 : end));
  }
}
