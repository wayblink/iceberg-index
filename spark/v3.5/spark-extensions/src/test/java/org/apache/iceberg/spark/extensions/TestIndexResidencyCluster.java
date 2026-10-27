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
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.index.spark.IndexCachePlugin;
import org.apache.iceberg.index.vector.VectorIndexTypes;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Exercise the residency protocol on a cluster: the driver plans, executors load and acknowledge,
 * and a query reads what they hold. {@code local-cluster} runs each executor in its own JVM, so the
 * plugin, the control channel and the ACKs are the real ones rather than threads in one process.
 */
public class TestIndexResidencyCluster {
  private static final String CATALOG = "local";
  private static final String DATABASE = "db";
  private static final long CACHE_BYTES = 64L * 1024 * 1024;

  private static SparkSession spark;
  private static Path warehouse;

  @BeforeAll
  static void startCluster(@TempDir Path tempDir) throws IOException {
    // local-cluster forks an executor JVM through the Spark launcher, which needs a Spark
    // distribution on this host. Without one the executors cannot start at all, and a thread-based
    // stand-in would prove nothing about separate JVMs; the cluster smoke covers that case instead.
    assumeThat(sparkHome())
        .as("a Spark distribution is needed to fork local-cluster executors")
        .isNotNull();

    warehouse = tempDir.resolve("warehouse");
    Files.createDirectories(warehouse);
    VectorIndexTypes.register();

    spark =
        SparkSession.builder()
            .master("local-cluster[2,1,2048]")
            .appName("index-residency-cluster")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.default.parallelism", "2")
            .config("spark.testing", "true")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.plugins", IndexCachePlugin.class.getName())
            .config("spark.iceberg.index.cache.bytes", Long.toString(CACHE_BYTES))
            // local-cluster starts a JVM per executor and hands it this process's classpath, which
            // is where a Gradle test build keeps its classes; without it the plugin class is
            // absent.
            .config("spark.executor.extraClassPath", System.getProperty("java.class.path"))
            // A local-cluster executor is a separate JVM started by a worker, which refuses to
            // start when the application hands it no jars at all; this one carries the plugin and
            // the rest of the classpath comes over extraClassPath.
            .config("spark.jars", pluginJar())
            .config(
                "spark.executor.extraJavaOptions",
                "--add-opens=java.base/java.nio=ALL-UNNAMED "
                    + "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED")
            .config("spark.sql.catalog." + CATALOG, "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog." + CATALOG + ".type", "hadoop")
            .config("spark.sql.catalog." + CATALOG + ".warehouse", warehouse.toUri().toString())
            .getOrCreate();

    spark.sparkContext().setLogLevel("WARN");
    spark.sql("CREATE DATABASE IF NOT EXISTS " + CATALOG + "." + DATABASE);
  }

  @AfterAll
  static void stopCluster() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  @Test
  void loadedFilesAreAcknowledgedByRealExecutorsAndSearchKeepsTheSameRows() throws Exception {
    String table = CATALOG + "." + DATABASE + ".vectors";
    try {
      createVectors(table);
      String index = buildIndex(table);

      List<Long> uncached = searchIds(table);

      List<Row> load = call("load_index", table, index, "30m");
      assertThat(load).isNotEmpty();
      assertThat(load.get(0).getString(0))
          .as("load status, rows: %s", rows(load))
          .isEqualTo("COMPLETE");

      List<Row> fileRows = load.subList(1, load.size());
      assertThat(fileRows).isNotEmpty();
      assertThat(fileRows)
          .allSatisfy(
              row -> {
                assertThat(row.getString(8))
                    .as("executor that holds %s", row.getString(7))
                    .isNotNull();
                assertThat(row.getLong(9)).as("bytes of %s", row.getString(7)).isPositive();
                assertThat(row.getString(10)).as("gap of %s", row.getString(7)).isNull();
              });

      // An ACK names the executor that stored the copy, and local-cluster gives each of them its
      // own JVM; a thread pool in the driver could not produce two different hosts here.
      List<String> executors = new ArrayList<>();
      for (Row row : fileRows) {
        String executor = row.getString(8);
        if (!executors.contains(executor)) {
          executors.add(executor);
        }
      }

      assertThat(executors).isNotEmpty();
      assertThat(executorHosts()).as("executors in the application").hasSize(2);

      assertThat(searchIds(table)).containsExactlyElementsOf(uncached);

      List<Row> offload = call("offload_index", table, index, null);
      assertThat(offload.get(0).getString(0))
          .as("offload status, rows: %s", rows(offload))
          .isEqualTo("OFFLOADED");

      // Offloading frees the copies without touching the files: the query still answers exactly the
      // same rows, reading them from storage again.
      assertThat(searchIds(table)).containsExactlyElementsOf(uncached);
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + table);
    }
  }

  @Test
  void aPermanentRetentionSurvivesAReloadWithoutOne() throws Exception {
    String table = CATALOG + "." + DATABASE + ".vectors_permanent";
    try {
      createVectors(table);
      String index = buildIndex(table);

      List<Row> first = call("load_index", table, index, "forever");
      assertThat(first.get(0).getString(0)).isEqualTo("COMPLETE");
      assertThat(first.get(0).getBoolean(6)).as("permanent").isTrue();

      // A repeated load that names no retention keeps the policy already accepted.
      List<Row> second = call("load_index", table, index, null);
      assertThat(second.get(0).getString(0)).isEqualTo("COMPLETE");
      assertThat(second.get(0).getBoolean(6)).as("permanent after reload").isTrue();

      List<Row> offload = call("offload_index", table, index, null);
      assertThat(offload.get(0).getString(0)).isEqualTo("OFFLOADED");
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + table);
    }
  }

  private static String pluginJar() {
    try {
      java.security.CodeSource source =
          IndexCachePlugin.class.getProtectionDomain().getCodeSource();
      return source == null ? "" : new java.io.File(source.getLocation().toURI()).getAbsolutePath();
    } catch (java.net.URISyntaxException e) {
      throw new IllegalStateException("Cannot locate the plugin jar", e);
    }
  }

  private static File sparkHome() {
    String home = System.getenv("SPARK_HOME");
    if (home != null && new File(home, "bin/spark-submit").exists()) {
      return new File(home);
    }

    File fallback = new File("/opt/spark");
    return new File(fallback, "bin/spark-submit").exists() ? fallback : null;
  }

  private static List<String> executorHosts() {
    List<String> hosts = new ArrayList<>();
    for (org.apache.spark.SparkExecutorInfo info :
        spark.sparkContext().statusTracker().getExecutorInfos()) {
      hosts.add(info.host());
    }

    return hosts;
  }

  private static void createVectors(String table) {
    spark.sql(
        "CREATE TABLE "
            + table
            + " (id bigint, embedding array<double>) USING iceberg PARTITIONED BY (bucket(2, id))");
    StringBuilder values = new StringBuilder();
    for (int i = 0; i < 64; i++) {
      double angle = (i * Math.PI) / 32;
      if (i > 0) {
        values.append(", ");
      }

      values
          .append('(')
          .append(i)
          .append(", array(")
          .append(Math.cos(angle))
          .append("d, ")
          .append(Math.sin(angle))
          .append("d))");
    }

    spark.sql("INSERT INTO " + table + " VALUES " + values);
  }

  private static String buildIndex(String table) {
    List<Row> rows =
        spark
            .sql(
                String.format(
                    "CALL %s.system.build_vector_index(table => '%s', column => 'embedding', "
                        + "dimension => 2, metric => 'cosine', structure => 'FLAT')",
                    CATALOG, table))
            .collectAsList();
    assertThat(rows).hasSize(1);
    return rows.get(0).getString(0);
  }

  private static List<Long> searchIds(String table) {
    List<Row> rows =
        spark
            .sql(
                String.format(
                    "CALL %s.system.vector_search(table => '%s', column => 'embedding', "
                        + "query => array(1.0d, 0.0d), k => 5)",
                    CATALOG, table))
            .collectAsList();
    List<Long> ids = new ArrayList<>();
    for (Row row : rows) {
      ids.add(row.getLong(1));
    }

    return ids;
  }

  private static List<Row> call(String procedure, String table, String index, String retainFor) {
    String sql =
        retainFor == null
            ? String.format(
                "CALL %s.system.%s(table => '%s', index => '%s')", CATALOG, procedure, table, index)
            : String.format(
                "CALL %s.system.%s(table => '%s', index => '%s', retain_for => '%s')",
                CATALOG, procedure, table, index, retainFor);
    return spark.sql(sql).collectAsList();
  }

  private static String rows(List<Row> rows) {
    StringBuilder text = new StringBuilder();
    for (Row row : rows) {
      text.append('\n').append(row);
    }

    return text.toString();
  }
}
