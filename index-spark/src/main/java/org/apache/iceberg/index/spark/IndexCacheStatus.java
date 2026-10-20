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
package org.apache.iceberg.index.spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * What the index cache holds right now, as a table a caller can read.
 *
 * <p>The counters live in each executor's JVM, so the driver only knows what the executors said at
 * the end of their last task. {@link #snapshot} returns that, which costs nothing and is what a
 * caller wants when a query has just run; {@link #refresh} asks every executor again, which costs
 * one small job and is what a caller wants when nothing has run for a while.
 */
public final class IndexCacheStatus {

  /** One line of the report: an executor's cache, or the driver's view of the last query. */
  public static final class Row implements Serializable {
    private final String executorId;
    private final String host;
    private final long epoch;
    private final String mode;
    private final long entries;
    private final long bytes;
    private final long hits;
    private final long misses;
    private final long loads;
    private final long evictions;
    private final double coverage;
    private final long workingSetBytes;
    private final long reportedAtMillis;

    Row(
        String executorId,
        String host,
        long epoch,
        String mode,
        long entries,
        long bytes,
        long hits,
        long misses,
        long loads,
        long evictions,
        double coverage,
        long workingSetBytes,
        long reportedAtMillis) {
      this.executorId = executorId;
      this.host = host;
      this.epoch = epoch;
      this.mode = mode;
      this.entries = entries;
      this.bytes = bytes;
      this.hits = hits;
      this.misses = misses;
      this.loads = loads;
      this.evictions = evictions;
      this.coverage = coverage;
      this.workingSetBytes = workingSetBytes;
      this.reportedAtMillis = reportedAtMillis;
    }

    public String executorId() {
      return executorId;
    }

    public String host() {
      return host;
    }

    public long epoch() {
      return epoch;
    }

    public String mode() {
      return mode;
    }

    public long entries() {
      return entries;
    }

    public long bytes() {
      return bytes;
    }

    public long hits() {
      return hits;
    }

    public long misses() {
      return misses;
    }

    public long loads() {
      return loads;
    }

    public long evictions() {
      return evictions;
    }

    public double coverage() {
      return coverage;
    }

    public long workingSetBytes() {
      return workingSetBytes;
    }

    public long reportedAtMillis() {
      return reportedAtMillis;
    }
  }

  private IndexCacheStatus() {}

  private static final String DRIVER = "driver";

  /** What the executors reported at the end of their last task, plus the driver's own view. */
  public static List<Row> snapshot(SparkSession spark) {
    return rows(IndexCacheLocations.forContext(spark.sparkContext()));
  }

  /**
   * Asks every executor for its counts now, then returns the same shape.
   *
   * <p>A cache that has been idle since its last query would otherwise report numbers that say
   * nothing about the present, and the alternative -- reading an executor's log -- is not something
   * a caller can do from SQL.
   */
  public static List<Row> refresh(SparkSession spark) {
    IndexCacheLocations table = IndexCacheLocations.forContext(spark.sparkContext());
    int tasks = Math.max(1, Math.min(64, table.size()));
    List<Integer> slices = new ArrayList<>(tasks);
    for (int i = 0; i < tasks; i++) {
      slices.add(i);
    }

    JavaSparkContext context = JavaSparkContext.fromSparkContext(spark.sparkContext());
    JavaRDD<IndexCacheLocations.Report> reports =
        context
            .parallelize(slices, tasks)
            .map(
                (org.apache.spark.api.java.function.Function<Integer, IndexCacheLocations.Report>)
                    slice -> {
                      String executor = "unknown";
                      String host = "unknown";
                      try {
                        if (org.apache.spark.SparkEnv.get() != null) {
                          executor = org.apache.spark.SparkEnv.get().executorId();
                          host =
                              org.apache.spark.SparkEnv.get()
                                  .blockManager()
                                  .blockManagerId()
                                  .host();
                        }
                      } catch (RuntimeException e) {
                        host = "unknown";
                      }

                      // a local session runs this in the driver's JVM, which the summary row
                      // already covers
                      return DRIVER.equals(executor)
                          ? null
                          : ExecutorIndexCache.get().report(executor, host);
                    });

    table.mergeAll(
        reports.collect().stream()
            .filter(r -> r != null)
            .collect(java.util.stream.Collectors.toList()));
    return rows(table);
  }

  private static List<Row> rows(IndexCacheLocations table) {
    List<IndexCacheLocations.Report> reports = table.reports();
    List<Row> rows = new ArrayList<>(reports.size() + 1);
    long now = System.currentTimeMillis();
    for (IndexCacheLocations.Report report : reports) {
      rows.add(
          new Row(
              report.executorId(),
              report.host(),
              report.epoch(),
              table.lastMode(),
              report.entries(),
              report.bytes(),
              report.hits(),
              report.misses(),
              report.loads(),
              report.evictions(),
              table.lastCoverage(),
              table.lastWorkingSetBytes(),
              now));
    }

    boolean alreadyNamed = false;
    for (Row row : rows) {
      alreadyNamed |= DRIVER.equals(row.executorId());
    }

    // in a local session the query's task runs in the driver's own JVM, so its report is already
    // the whole story and a second row would only repeat it
    if (!rows.isEmpty() && !alreadyNamed) {
      rows.add(
          new Row(
              DRIVER,
              "",
              0,
              table.lastMode(),
              reports.size(),
              rows.stream().mapToLong(Row::bytes).sum(),
              rows.stream().mapToLong(Row::hits).sum(),
              rows.stream().mapToLong(Row::misses).sum(),
              rows.stream().mapToLong(Row::loads).sum(),
              rows.stream().mapToLong(Row::evictions).sum(),
              table.lastCoverage(),
              table.lastWorkingSetBytes(),
              table.lastQueryMillis() > 0 ? table.lastQueryMillis() : now));
    }

    return Collections.unmodifiableList(rows);
  }

  /** The cache decisions of the last query, so a caller can put the counts in context. */
  public static void recordQuery(
      SparkSession spark, String mode, long workingSetBytes, double coverage) {
    IndexCacheLocations.forContext(spark.sparkContext())
        .recordQuery(mode, workingSetBytes, coverage);
  }
}
