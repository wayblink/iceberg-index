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
import java.util.Map;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end smoke for the channel whose absence made the original report read like a product bug:
 * a Lance-backed Iceberg table on an S3-compatible store whose storage settings exist only in the
 * engine's Hadoop configuration.
 *
 * <p>That is the deployment shape in question: {@code spark.hadoop.fs.s3a.*} is where a submission
 * keeps the endpoint and credentials, while {@code HadoopFileIO} exposes in {@code properties()}
 * only the catalog properties it was initialized with — nothing of the Hadoop configuration. This
 * test uses {@link HidingFileIO}, which opens files through S3FileIO but, like HadoopFileIO,
 * exposes no storage settings of its own; the real HadoopFileIO cannot be used here because its
 * data path needs the S3A client, which this module's test classpath does not carry.
 *
 * <p>The table's storage properties are stripped after the write, so a successful read can only
 * have come from the Hadoop configuration reaching the format through the engine.
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
class TestLanceSparkHadoopStorageConfSmoke {

  private static final int MINIO_PORT = 19000;
  private static final String ENDPOINT = "http://127.0.0.1:" + MINIO_PORT;
  private static final String ACCESS_KEY = "lance3043";
  private static final String SECRET_KEY = "lance3043pw";
  private static final String BUCKET = "lance3043";
  private static final String CATALOG = "confcat";
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
            .appName("lance-hadoop-conf-smoke")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog." + CATALOG, "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog." + CATALOG + ".type", "hadoop")
            .config("spark.sql.catalog." + CATALOG + ".warehouse", warehouse.toString())
            // a catalog FileIO that, like HadoopFileIO, does not expose storage settings
            .config("spark.sql.catalog." + CATALOG + ".io-impl", HidingFileIO.class.getName())
            // the store is configured the way a submission configures it: in the Hadoop
            // configuration, never in the catalog
            .config("spark.hadoop.fs.s3a.endpoint", ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
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
  void readsThroughACatalogFileIoThatExposesNoStorageSettings() {
    spark.sql("CREATE DATABASE IF NOT EXISTS " + CATALOG + ".db");
    String table = CATALOG + ".db.lance_conf_smoke";
    String dataPath = "s3://" + BUCKET + "/spark-smoke/" + java.util.UUID.randomUUID();

    // the write side has no FileIO channel: it reads the table properties and the environment, so
    // the settings have to be on the table while it is written
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

    // strip the settings from the table: from here on the read may only use what the engine
    // carries, which for a HadoopFileIO-shaped FileIO means the Hadoop configuration
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

    HidingFileIO io = new HidingFileIO();
    io.initialize(ImmutableMap.of());
    for (String path : paths) {
      io.deleteFile(path);
    }

    io.close();
  }

  private static boolean minioIsUp() {
    try (Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("127.0.0.1", MINIO_PORT), 500);
      return true;
    } catch (IOException e) {
      return false;
    }
  }

  /**
   * A {@link FileIO} that opens files on the object store but exposes no storage settings, which is
   * what {@code HadoopFileIO.properties()} amounts to under a submission that keeps its store
   * settings in the Hadoop configuration.
   */
  public static class HidingFileIO implements FileIO {
    private final S3FileIO objectStore = new S3FileIO();
    private final HadoopFileIO local = new HadoopFileIO();

    @Override
    public void initialize(Map<String, String> properties) {
      // the settings are deliberately not taken from the properties: this stands in for a FileIO
      // whose configuration lives in a Hadoop Configuration it does not expose
      objectStore.initialize(
          ImmutableMap.of(
              "s3.endpoint",
              ENDPOINT,
              "s3.region",
              "us-east-1",
              "client.region",
              "us-east-1",
              "s3.access-key-id",
              ACCESS_KEY,
              "s3.secret-access-key",
              SECRET_KEY,
              "s3.path-style-access",
              "true"));
    }

    @Override
    public Map<String, String> properties() {
      return ImmutableMap.of();
    }

    @Override
    public InputFile newInputFile(String path) {
      return fileIOFor(path).newInputFile(path);
    }

    @Override
    public InputFile newInputFile(String path, long length) {
      return fileIOFor(path).newInputFile(path, length);
    }

    @Override
    public OutputFile newOutputFile(String path) {
      return fileIOFor(path).newOutputFile(path);
    }

    @Override
    public void deleteFile(String path) {
      fileIOFor(path).deleteFile(path);
    }

    @Override
    public void close() {
      objectStore.close();
      local.close();
    }

    /** The table's metadata stays on the local warehouse; its data files live in the bucket. */
    private FileIO fileIOFor(String path) {
      return path.startsWith("s3://") || path.startsWith("s3a://") ? objectStore : local;
    }
  }
}
