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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.index.SidecarIndexCatalog;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkCatalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * What a text index build that fails part way through leaves behind, and that its own failure does
 * not refuse a retry of the same column.
 *
 * <p>The build is made to fail by a {@link java.io.FileIO} that refuses every write of a text index
 * payload, which is the write an executor performs after the index has been registered and before
 * any snapshot exists. A registration without a snapshot looks like an index that exists, so a
 * retry that does not name it is refused until the failed build's registration is dropped.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class TestTextIndexBuildRollback extends ExtensionsTestBase {

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        "testhive",
        SparkCatalog.class.getName(),
        ImmutableMap.of(
            "type",
            "hive",
            CatalogProperties.FILE_IO_IMPL,
            FailingIndexFileIO.class.getName(),
            "default-namespace",
            "default")
      }
    };
  }

  @BeforeEach
  public void failIndexPayloadWrites() {
    FailingIndexFileIO.failPayloadWrites = true;
  }

  @AfterEach
  public void removeTables() {
    FailingIndexFileIO.failPayloadWrites = false;
    sql("DROP TABLE IF EXISTS %s PURGE", tableName("text_build_rollback_" + catalogName));
  }

  @TestTemplate
  public void aFailedBuildIsRolledBackAndCanBeRetried() throws Exception {
    String tableName = tableName("text_build_rollback_" + catalogName);
    sql("CREATE TABLE %s (id bigint, body string) USING iceberg", tableName);
    sql(
        "INSERT INTO %s VALUES "
            + "(1, 'iceberg table format for large datasets'), "
            + "(2, 'vector search over embeddings')",
        tableName);

    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.build_text_index(table => '%s', column => 'body')",
                    catalogName, tableName))
        .hasStackTraceContaining("Injected text index write failure");

    // nothing that looks like an index is left behind: the registration was dropped and the tree
    // it named was purged with it
    assertThat(sql("CALL %s.system.list_indexes(table => '%s')", catalogName, tableName)).isEmpty();

    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    assertThat(new SidecarIndexCatalog(table.io(), table).listIndexes(table.location())).isEmpty();

    // the failure the rollback deleted the report of is still readable, or the only account of why
    // the build failed would be gone with the index it described
    List<Object[]> failed =
        sql("CALL %s.system.index_build_status(table => '%s')", catalogName, tableName);
    assertThat(failed).hasSize(1);
    assertThat(failed.get(0)[1]).as("column").isEqualTo("body");
    assertThat(failed.get(0)[5]).as("state").isEqualTo("FAILED");
    assertThat((String) failed.get(0)[14])
        .as("reason")
        .contains("Injected text index write failure");

    // the same column can be built again, which it could not if the failed build's registration had
    // been left behind: a build without a name is refused while any index is registered for it
    FailingIndexFileIO.failPayloadWrites = false;
    List<Object[]> built =
        sql(
            "CALL %s.system.build_text_index(table => '%s', column => 'body')",
            catalogName, tableName);
    assertThat(built).hasSize(1);
    assertThat(built.get(0)[3]).as("indexed entries").isEqualTo(2L);

    List<Object[]> listed =
        sql("CALL %s.system.list_indexes(table => '%s')", catalogName, tableName);
    assertThat(listed).hasSize(1);
    assertThat(listed.get(0)[0]).as("the rebuilt index").isEqualTo(built.get(0)[0]);
    assertThat(listed.get(0)[0])
        .as("the failed index is no longer listed")
        .isNotEqualTo(failed.get(0)[2]);

    // one row, the successful attempt: the report of the failed one is cleared by the build that
    // replaced it, and the report of the rebuilt index is what the status call now reads
    List<Object[]> status =
        sql("CALL %s.system.index_build_status(table => '%s')", catalogName, tableName);
    assertThat(status).hasSize(1);
    assertThat(status.get(0)[5]).as("state").isEqualTo("SUCCEEDED");
    assertThat(status.get(0)[14]).as("no error").isNull();

    List<Object[]> hits =
        sql(
            "CALL %s.system.text_search(table => '%s', column => 'body', "
                + "query => 'iceberg table', k => 1)",
            catalogName, tableName);
    assertThat(hits).hasSize(1);
    assertThat(hits.get(0)[3].toString()).as("the resolved row").contains("iceberg table format");
  }

  /**
   * Fails every write of a text index payload while the flag is set.
   *
   * <p>The location of a payload is chosen inside the task that writes it, so a test cannot name
   * the file to fail; the flag is what makes the executor refuse the write and the test turns it
   * off to let the retry through. The tests run in local mode, so the flag is shared between the
   * driver and the task threads.
   */
  public static class FailingIndexFileIO extends HadoopFileIO {
    static volatile boolean failPayloadWrites = true;

    public FailingIndexFileIO() {}

    @Override
    public OutputFile newOutputFile(String path) {
      if (failPayloadWrites && path.contains("/indexes/") && path.contains("/data/index-file-")) {
        throw new UncheckedIOException(
            new IOException("Injected text index write failure: " + path));
      }

      return super.newOutputFile(path);
    }
  }
}
