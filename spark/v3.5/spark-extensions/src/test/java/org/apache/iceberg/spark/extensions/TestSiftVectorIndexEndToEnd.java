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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.iceberg.index.vector.VectorIndexTypes;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end vector index test on real SIFT data (128-dimensional SIFT descriptors, Euclidean
 * nearest-neighbor ground truth computed by brute force over the indexed subset).
 *
 * <p>The test drives the whole chain through Spark SQL {@code CALL} procedures: load data, build
 * the index, search it and check recall against brute force, for both EXACT and HNSW modes.
 *
 * <p>It is skipped unless the SIFT tarball is available. Point to it with the {@code sift.dataset}
 * system property or the {@code SIFT_DATASET} environment variable; the default is {@code
 * ~/Downloads/sift.tar.gz}. The number of base vectors and queries is configurable with {@code
 * sift.base} (default 20000) and {@code sift.queries} (default 50).
 */
class TestSiftVectorIndexEndToEnd {

  private static final int DIMENSION = 128;
  private static final int TOP_K = 10;

  private static final int BASE_VECTORS = Integer.getInteger("sift.base", 20_000);
  private static final int QUERIES = Integer.getInteger("sift.queries", 50);

  private static final Pattern ID_PATTERN = Pattern.compile("\"id\":(-?\\d+)");

  private static SparkSession spark;
  private static float[][] baseVectors;
  private static float[][] queryVectors;
  private static int[][] expectedNeighbors;

  @BeforeAll
  static void setup(@TempDir Path tempDir) throws Exception {
    Path tarball = locateTarball();
    assumeThat(tarball).as("SIFT dataset not available; test skipped").isNotNull();

    // read only the first records of each .fvecs member instead of extracting hundreds of MB
    Path baseFile = tempDir.resolve("sift_base.head.fvecs");
    Path queryFile = tempDir.resolve("sift_query.head.fvecs");
    startStage("read sift vectors from tarball");
    headOfMember(tarball, "sift/sift_base.fvecs", BASE_VECTORS * (4 + DIMENSION * 4), baseFile);
    headOfMember(tarball, "sift/sift_query.fvecs", QUERIES * (4 + DIMENSION * 4), queryFile);
    baseVectors = readFvecs(baseFile);
    queryVectors = readFvecs(queryFile);
    endStage();

    startStage("brute-force ground truth (" + QUERIES + " queries x " + BASE_VECTORS + " vectors)");
    expectedNeighbors = bruteForceNearest(baseVectors, queryVectors, TOP_K);
    endStage();

    VectorIndexTypes.register();
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("sift-vector-index")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "4")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config(
                "spark.sql.catalog.local.warehouse",
                tempDir.resolve("warehouse").toUri().toString())
            .getOrCreate();
  }

  @AfterAll
  static void stopSpark() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  public void buildAndSearchVectorIndexOnSiftData() {
    startStage("load data into iceberg tables (x2)");
    loadTable("sift_exact");
    loadTable("sift_hnsw");
    endStage();

    startStage("build EXACT index (spark job + manifest)");
    spark
        .sql(
            "CALL local.system.build_vector_index("
                + "table => 'db.sift_exact', column => 'embedding', "
                + "dimension => 128, metric => 'euclidean')")
        .show(false);
    endStage();

    startStage("build HNSW index (spark job + graph build + manifest)");
    spark
        .sql(
            "CALL local.system.build_vector_index("
                + "table => 'db.sift_hnsw', column => 'embedding', "
                + "dimension => 128, metric => 'euclidean', mode => 'HNSW', "
                + "max_m => 16, ef_construction => 200, ef_search => 64)")
        .show(false);
    endStage();

    // both indexes must cover every loaded row
    startStage("list indexes (x2)");
    for (String table : new String[] {"sift_exact", "sift_hnsw"}) {
      List<Row> indexes =
          spark.sql("CALL local.system.list_indexes(table => 'db." + table + "')").collectAsList();
      assertThat(indexes).hasSize(1);
      assertThat(indexes.get(0).get(1)).isEqualTo("embedding");
      assertThat(indexes.get(0).get(2)).isEqualTo("vector");
      assertThat(indexes.get(0).get(4)).isEqualTo("PUBLISHED");
      assertThat(((Number) indexes.get(0).get(6)).longValue()).isEqualTo(BASE_VECTORS);
    }
    endStage();

    startStage("search EXACT index (" + QUERIES + " queries, incl. row resolution)");
    double exactRecall = measureRecall("sift_exact");
    endStage();

    startStage("search HNSW index (" + QUERIES + " queries, incl. row resolution)");
    double hnswRecall = measureRecall("sift_hnsw");
    endStage();

    // batch search: all queries in one call, one read per distinct hit data file
    startStage("batch search EXACT index (" + QUERIES + " queries, one call)");
    List<Object[]> batchOutput =
        spark
            .sql(
                "CALL local.system.vector_search_batch(table => 'db.sift_exact', "
                    + "column => 'embedding', queries => array("
                    + batchQueriesLiteral()
                    + "), k => "
                    + TOP_K
                    + ")")
            .collectAsList()
            .stream()
            .map(row -> new Object[] {row.get(0), row.get(1), row.get(2), row.get(3), row.get(4)})
            .collect(Collectors.toList());
    endStage();

    // the batch results must reproduce the per-query results and the brute-force reference
    Map<Integer, List<Long>> batchIdsByQuery = Maps.newHashMap();
    for (Object[] row : batchOutput) {
      batchIdsByQuery
          .computeIfAbsent(((Number) row[0]).intValue(), key -> Lists.newArrayList())
          .add(parseId((String) row[4]));
    }
    assertThat(batchIdsByQuery).hasSize(QUERIES);
    double batchRecallSum = 0.0;
    for (int queryId = 0; queryId < QUERIES; queryId++) {
      List<Long> ids = batchIdsByQuery.get(queryId);
      assertThat(ids).hasSize(TOP_K);
      int[] expected = expectedNeighbors[queryId];
      long matches = ids.stream().filter(id -> contains(expected, id)).count();
      batchRecallSum += (double) matches / TOP_K;
    }
    System.out.printf("  batch recall@%d: %.4f%n", TOP_K, batchRecallSum / QUERIES);
    assertThat(batchRecallSum / QUERIES).isGreaterThanOrEqualTo(0.99);

    System.out.println();
    System.out.println(
        "SIFT recall@" + TOP_K + " over " + QUERIES + " queries on " + BASE_VECTORS + " vectors");
    System.out.println("  EXACT : " + exactRecall);
    System.out.println("  HNSW  : " + hnswRecall);

    // exact search must reproduce brute force
    assertThat(exactRecall).isGreaterThanOrEqualTo(0.99);
    // approximate search must be nearly as good on this data size
    assertThat(hnswRecall).isGreaterThanOrEqualTo(0.90);
  }

  /** Runs every query through the SQL procedure and returns the mean recall@TOP_K. */
  private double measureRecall(String table) {
    double recallSum = 0.0;
    long searchNanos = 0L;
    for (int q = 0; q < QUERIES; q++) {
      long queryStart = System.nanoTime();
      List<Row> hits =
          spark
              .sql(
                  "CALL local.system.vector_search("
                      + "table => 'db."
                      + table
                      + "', column => 'embedding', "
                      + "query => array("
                      + toSqlArray(queryVectors[q])
                      + "), k => "
                      + TOP_K
                      + ")")
              .collectAsList();
      searchNanos += System.nanoTime() - queryStart;

      assertThat(hits).hasSize(TOP_K);

      // euclidean scores are distances: results must come back closest first
      for (int i = 1; i < hits.size(); i++) {
        assertThat(((Number) hits.get(i).get(2)).doubleValue())
            .isGreaterThanOrEqualTo(((Number) hits.get(i - 1).get(2)).doubleValue());
      }

      // the resolved row JSON carries the id of the matched row
      List<Long> ids =
          hits.stream().map(hit -> parseId(hit.getString(3))).collect(Collectors.toList());
      int[] expected = expectedNeighbors[q];
      long matches = ids.stream().filter(id -> contains(expected, id)).count();
      recallSum += (double) matches / TOP_K;
    }

    System.out.printf(
        "  [%s] %.1f ms per query (sql call + collect)%n", table, searchNanos / 1e6 / QUERIES);
    return recallSum / QUERIES;
  }

  /** Builds the SQL literal of all query vectors for the batch search call. */
  private static String batchQueriesLiteral() {
    StringBuilder sql = new StringBuilder();
    for (int queryId = 0; queryId < QUERIES; queryId++) {
      if (queryId > 0) {
        sql.append(", ");
      }
      sql.append("'").append(csvQuery(queryVectors[queryId])).append("'");
    }
    return sql.toString();
  }

  private static String csvQuery(float[] vector) {
    StringBuilder csv = new StringBuilder();
    for (int i = 0; i < vector.length; i++) {
      if (i > 0) {
        csv.append(',');
      }
      csv.append(vector[i]);
    }
    return csv.toString();
  }

  private static long stageStartNanos;

  private static void startStage(String name) {
    stageStartNanos = System.nanoTime();
    System.out.println();
    System.out.println(">>> " + name);
  }

  private static void endStage() {
    System.out.printf("<<< %.2f s%n", (System.nanoTime() - stageStartNanos) / 1e9);
  }

  private static void loadTable(String tableName) {
    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("id", DataTypes.LongType, false, Metadata.empty()),
              new StructField(
                  "embedding",
                  DataTypes.createArrayType(DataTypes.FloatType),
                  false,
                  Metadata.empty())
            });
    List<Row> rows = Lists.newArrayListWithExpectedSize(BASE_VECTORS);
    for (int id = 0; id < BASE_VECTORS; id++) {
      float[] vector = baseVectors[id];
      List<Float> values = Lists.newArrayListWithExpectedSize(DIMENSION);
      for (float value : vector) {
        values.add(value);
      }
      rows.add(RowFactory.create((long) id, values));
    }

    spark.createDataFrame(rows, schema).writeTo("local.db." + tableName).createOrReplace();
  }

  private static Path locateTarball() {
    String configured = System.getProperty("sift.dataset", System.getenv("SIFT_DATASET"));
    Path tarball =
        configured != null
            ? Paths.get(configured)
            : Paths.get(System.getProperty("user.home"), "Downloads", "sift.tar.gz");
    return Files.isReadable(tarball) ? tarball : null;
  }

  /** Streams the first {@code bytes} bytes of a tar.gz member into a file. */
  private static void headOfMember(Path tarball, String member, long bytes, Path target)
      throws IOException, InterruptedException {
    Process process =
        new ProcessBuilder(
                "bash",
                "-c",
                "tar -xzf " + tarball + " -O " + member + " | head -c " + bytes + " > " + target)
            // tar reports a broken pipe because head stops reading early; that is expected
            .redirectError(ProcessBuilder.Redirect.DISCARD)
            .start();
    if (process.waitFor() != 0) {
      throw new IllegalStateException("Failed to read " + member + " from " + tarball);
    }
  }

  /** Reads little-endian .fvecs records: int32 dimension followed by that many float32 values. */
  private static float[][] readFvecs(Path file) throws IOException {
    byte[] content = Files.readAllBytes(file);
    ByteBuffer buffer = ByteBuffer.wrap(content).order(ByteOrder.LITTLE_ENDIAN);
    List<float[]> vectors = Lists.newArrayList();
    while (buffer.remaining() >= 4) {
      int dim = buffer.getInt();
      if (dim <= 0 || buffer.remaining() < dim * 4) {
        break;
      }
      float[] vector = new float[dim];
      for (int i = 0; i < dim; i++) {
        vector[i] = buffer.getFloat();
      }
      vectors.add(vector);
    }
    return vectors.toArray(new float[0][]);
  }

  /** Brute-force nearest neighbors (smallest Euclidean distance) for every query. */
  private static int[][] bruteForceNearest(float[][] base, float[][] queries, int neighborCount) {
    int[][] neighbors = new int[queries.length][];
    for (int q = 0; q < queries.length; q++) {
      float[] query = queries[q];
      // max-heap of the k smallest distances, ordered by (distance, id)
      PriorityQueue<long[]> heap =
          new PriorityQueue<>((left, right) -> Long.compare(right[0], left[0]));
      for (int id = 0; id < base.length; id++) {
        double distance = euclidean(query, base[id]);
        long key = Double.doubleToRawLongBits(distance);
        if (heap.size() < neighborCount) {
          heap.add(new long[] {key, id});
        } else if (Double.longBitsToDouble(heap.peek()[0]) > distance) {
          heap.poll();
          heap.add(new long[] {key, id});
        }
      }

      int[] result = new int[heap.size()];
      int index = heap.size() - 1;
      while (!heap.isEmpty()) {
        result[index--] = (int) heap.poll()[1];
      }
      neighbors[q] = result;
    }
    return neighbors;
  }

  private static double euclidean(float[] query, float[] vector) {
    double sum = 0.0;
    for (int i = 0; i < query.length; i++) {
      double difference = (double) query[i] - vector[i];
      sum += difference * difference;
    }
    return Math.sqrt(sum);
  }

  private static String toSqlArray(float[] vector) {
    StringBuilder sql = new StringBuilder(vector.length * 8);
    for (int i = 0; i < vector.length; i++) {
      if (i > 0) {
        sql.append(", ");
      }
      sql.append(String.format(Locale.ROOT, "%sd", vector[i]));
    }
    return sql.toString();
  }

  private static long parseId(String rowJson) {
    Matcher matcher = ID_PATTERN.matcher(rowJson);
    assertThat(matcher.find()).as("row JSON contains an id: %s", rowJson).isTrue();
    return Long.parseLong(matcher.group(1));
  }

  private static boolean contains(int[] values, long value) {
    for (int candidate : values) {
      if (candidate == value) {
        return true;
      }
    }
    return false;
  }
}
