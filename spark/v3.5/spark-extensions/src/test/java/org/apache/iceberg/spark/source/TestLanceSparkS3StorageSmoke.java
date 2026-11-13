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
package org.apache.iceberg.spark.source;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Path;
import java.util.List;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end smoke for the storage-configuration channel: a Lance-backed Iceberg table whose data
 * files live on a non-AWS S3 endpoint (MinIO), read back with no {@code AWS_*} in the environment.
 *
 * <p>It is the acceptance shape of the card: write once with the settings on the table, then strip
 * them from the table and read again, so the read can only come from the catalog's {@code FileIO}
 * properties — the channel that used to be empty.
 *
 * <p>Runs only when a MinIO answers on {@value #MINIO_PORT}; otherwise skipped. Reproduce with:
 *
 * <pre>
 * docker run -d --rm --name lance-minio -p 19000:9000 \
 *   -e MINIO_ROOT_USER=lance3043 -e MINIO_ROOT_PASSWORD=lance3043pw \
 *   minio/minio server /data
 * # then create the bucket "lance3043"
 * </pre>
 */
class TestLanceSparkS3StorageSmoke {

  private static final int MINIO_PORT = 19000;
  private static final String ENDPOINT = "http://127.0.0.1:" + MINIO_PORT;
  private static final String ACCESS_KEY = "lance3043";
  private static final String SECRET_KEY = "lance3043pw";
  private static final String BUCKET = "lance3043";
  private static final String CATALOG = "s3cat";
  private static final List<String> STORAGE_PROPERTIES =
      java.util.Arrays.asList(
          "s3.endpoint",
          "s3.region",
          "client.region",
          "s3.access-key-id",
          "s3.secret-access-key",
          "s3.path-style-access",
          "fs.s3a.connection.ssl.enabled");

  private static SparkSession spark;

  @TempDir private static Path warehouse;

  @BeforeAll
  static void startSpark() {
    Assumptions.assumeTrue(minioIsUp(), "no MinIO on port " + MINIO_PORT);

    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("lance-s3-storage-smoke")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog." + CATALOG, "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog." + CATALOG + ".type", "hadoop")
            .config("spark.sql.catalog." + CATALOG + ".warehouse", warehouse.toString())
            .config(
                "spark.sql.catalog." + CATALOG + ".io-impl",
                "org.apache.iceberg.io.ResolvingFileIO")
            .config("spark.sql.catalog." + CATALOG + ".s3.endpoint", ENDPOINT)
            // Iceberg's S3FileIO reads client.region, not s3.region
            .config("spark.sql.catalog." + CATALOG + ".client.region", "us-east-1")
            .config("spark.sql.catalog." + CATALOG + ".s3.access-key-id", ACCESS_KEY)
            .config("spark.sql.catalog." + CATALOG + ".s3.secret-access-key", SECRET_KEY)
            .config("spark.sql.catalog." + CATALOG + ".s3.path-style-access", "true")
            .config("spark.sql.catalog." + CATALOG + ".fs.s3a.connection.ssl.enabled", "false")
            .getOrCreate();
  }

  @AfterAll
  static void stopSpark() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  @Test
  void readsLanceDataFilesWithOnlyTheCatalogsStorageProperties() {
    spark.sql("CREATE DATABASE IF NOT EXISTS " + CATALOG + ".db");
    String table = CATALOG + ".db.lance_s3_smoke";
    String dataPath = "s3://" + BUCKET + "/spark-smoke/" + java.util.UUID.randomUUID();

    // the write side has no FileIO channel: its only sources are the table and the environment
    String tableStorage =
        String.format(
            "'s3.endpoint'='%s', 's3.region'='us-east-1', 'client.region'='us-east-1', "
                + "'s3.access-key-id'='%s', 's3.secret-access-key'='%s', "
                + "'s3.path-style-access'='true', 'fs.s3a.connection.ssl.enabled'='false', ",
            ENDPOINT, ACCESS_KEY, SECRET_KEY);

    spark.sql(
        "CREATE TABLE "
            + table
            + " (id bigint, txt string) USING iceberg TBLPROPERTIES ("
            + "'write.format.default'='lance', "
            + "'write.data.path'='"
            + dataPath
            + "', "
            + "'write.target-file-size-bytes'='1048576', "
            + tableStorage
            + "'format-version'='2')");

    spark.sql("INSERT INTO " + table + " SELECT id, CAST(id AS string) FROM range(0, 100)");

    List<Row> dataFiles = spark.sql("SELECT file_path FROM " + table + ".files").collectAsList();
    assertThat(dataFiles).isNotEmpty();
    assertThat(dataFiles.stream().map(row -> row.getString(0)).filter(p -> p.endsWith(".lance")))
        .isNotEmpty();

    // strip the settings from the table: from here on the read may only use the catalog's FileIO
    StringBuilder unset = new StringBuilder();
    for (String property : STORAGE_PROPERTIES) {
      unset.append("'").append(property).append("',");
    }

    spark.sql(
        "ALTER TABLE "
            + table
            + " UNSET TBLPROPERTIES ("
            + unset.substring(0, unset.length() - 1)
            + ")");

    try {
      // count(*) would be answered from the metadata and prove nothing about the data file
      assertThat(spark.sql("SELECT max(id) FROM " + table).collectAsList().get(0).getLong(0))
          .isEqualTo(99L);
    } finally {
      dropAndCleanUp(table);
    }
  }

  /** Drops the table and the data files it wrote, so a run leaves nothing behind in the bucket. */
  private static void dropAndCleanUp(String table) {
    List<String> paths =
        spark.sql("SELECT file_path FROM " + table + ".files").collectAsList().stream()
            .map(row -> row.getString(0))
            .collect(java.util.stream.Collectors.toList());
    spark.sql("DROP TABLE " + table);

    try (org.apache.iceberg.aws.s3.S3FileIO io = new org.apache.iceberg.aws.s3.S3FileIO()) {
      io.initialize(
          org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap.of(
              "s3.endpoint", ENDPOINT,
              "client.region", "us-east-1",
              "s3.access-key-id", ACCESS_KEY,
              "s3.secret-access-key", SECRET_KEY,
              "s3.path-style-access", "true"));
      for (String path : paths) {
        io.deleteFile(path);
      }
    }
  }

  private static boolean minioIsUp() {
    try (Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("127.0.0.1", MINIO_PORT), 500);
      return true;
    } catch (IOException e) {
      return false;
    }
  }
}
