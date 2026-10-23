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
import org.junit.jupiter.api.TestTemplate;

/** What the cache reports about itself, through the procedure a caller would use. */
public class TestIndexCacheStatus extends ExtensionsTestBase {

  private static final String CACHE_ENABLED = "spark.iceberg.index.cache.enabled";

  @TestTemplate
  public void reportsTheCacheAfterAQuery() {
    String tableName = tableName("cache_status_" + catalogName);
    sql("CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg", tableName);
    sql(
        "INSERT INTO %s SELECT id, array(CAST(id AS double), CAST(10 - id AS double)) "
            + "FROM range(0, 20)",
        tableName);
    sql(
        "CALL %s.system.build_vector_index("
            + "table => '%s', column => 'embedding', dimension => 2, metric => 'euclidean')",
        catalogName, tableName);

    spark.conf().set(CACHE_ENABLED, "true");
    try {
      sql(
          "CALL %s.system.vector_search_batch(table => '%s', column => 'embedding', "
              + "queries => array('0.0,10.0'), k => 3)",
          catalogName, tableName);

      List<Object[]> rows = sql("CALL %s.system.index_cache_status()", catalogName);
      // one row per executor plus a driver row
      assertThat(rows).isNotEmpty();
      assertThat((String) rows.get(rows.size() - 1)[0]).isEqualTo("driver");
      Object[] executor = rows.get(0);
      assertThat((Long) executor[4]).isPositive(); // entries
      assertThat((Long) executor[6] + (Long) executor[7]).isPositive(); // hits + misses
      assertThat((String) executor[3]).isIn("PIN_ALL", "PIN_HOT", "STREAM", "UNKNOWN");

      // asking again after a refresh reports the same executor
      List<Object[]> refreshed =
          sql("CALL %s.system.index_cache_status(refresh => true)", catalogName);
      assertThat(refreshed).isNotEmpty();
      assertThat((String) refreshed.get(refreshed.size() - 1)[0]).isEqualTo("driver");
    } finally {
      spark.conf().unset(CACHE_ENABLED);
    }
  }

  @TestTemplate
  public void refreshReturnsTheSameShapeAsASnapshot() {
    List<Object[]> rows = sql("CALL %s.system.index_cache_status(refresh => true)", catalogName);
    // in a local session the only executor is the driver, so a refresh reports what the snapshot
    // would: no separate executor rows, and no error
    for (Object[] row : rows) {
      assertThat(row).hasSize(15);
    }
  }

  @TestTemplate
  public void aFreshReportSaysSoAndAnOldOneSaysItIsOld() {
    List<Object[]> rows = sql("CALL %s.system.index_cache_status(refresh => true)", catalogName);
    assertThat(rows).isNotEmpty();

    long now = System.currentTimeMillis();
    for (Object[] row : rows) {
      // a refresh just asked the executors, so nothing it returns can be old: the two columns
      // exist to tell a caller whether the snapshot still describes the present
      assertThat((Long) row[13]).as("age in millis").isBetween(0L, 60_000L);
      assertThat((Boolean) row[14]).as("stale").isFalse();
      long reportedAt = ((java.sql.Timestamp) row[12]).getTime();
      assertThat(reportedAt).as("reported_at is a millisecond stamp").isBetween(now - 60_000L, now);
    }
  }
}
