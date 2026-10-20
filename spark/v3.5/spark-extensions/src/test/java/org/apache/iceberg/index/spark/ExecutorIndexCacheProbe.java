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

/** Reads the executor cache's counters from a test that runs the executor in the same JVM. */
public final class ExecutorIndexCacheProbe {
  private ExecutorIndexCacheProbe() {}

  public static ExecutorIndexCache.Stats stats() {
    return ExecutorIndexCache.get().stats();
  }

  public static int locatedExecutors(org.apache.spark.sql.SparkSession spark) {
    return IndexCacheLocations.forContext(spark.sparkContext()).size();
  }

  public static double coverage(
      org.apache.spark.sql.SparkSession spark, java.util.List<String> cacheKeys) {
    return IndexCacheLocations.forContext(spark.sparkContext()).summary(cacheKeys).coverage();
  }

  public static String config(org.apache.spark.sql.SparkSession spark) {
    ExecutorIndexCache.Config config = ExecutorIndexCache.Config.from(spark, 0L);
    return "enabled="
        + config.enabled()
        + " bytes="
        + config.maxBytes()
        + " maxEntry="
        + config.maxEntryBytes();
  }
}
