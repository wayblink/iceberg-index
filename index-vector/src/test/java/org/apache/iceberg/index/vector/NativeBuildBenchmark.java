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
package org.apache.iceberg.index.vector;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.iceberg.index.IndexBuildResult;
import org.apache.iceberg.index.IndexEntry;

/**
 * Compares the Java and native build kernels on one raw `f32` file of vectors.
 *
 * <p>This is a measurement harness, not a test: it takes the vectors, builds the same entries with
 * a Java-kernel definition and a native-kernel definition, and prints the wall time, the payload
 * hash and the structure each build reports. It is how the speedup and the byte equality of the two
 * paths are checked on the same rows.
 *
 * <p>Arguments: {@code <vectors.f32> <rows> <dimension> <nlist> <threads> <rounds> <bits>
 * [native-library]}.
 */
public final class NativeBuildBenchmark {

  private NativeBuildBenchmark() {}

  public static void main(String[] args) throws Exception {
    if (args.length < 7) {
      System.err.println(
          "usage: NativeBuildBenchmark <vectors.f32> <rows> <dimension> <nlist> <threads> "
              + "<rounds> <bits> [native-library]");
      System.exit(2);
    }

    VectorIndexTypes.register();
    Path vectorsFile = Path.of(args[0]);
    int rows = Integer.parseInt(args[1]);
    int dimension = Integer.parseInt(args[2]);
    int nlist = Integer.parseInt(args[3]);
    int threads = Integer.parseInt(args[4]);
    int rounds = Integer.parseInt(args[5]);
    int bits = Integer.parseInt(args[6]);
    if (args.length > 7) {
      NativeIvfBuild.load(Path.of(args[7]));
      System.out.println("native library: " + args[7]);
    }

    System.out.printf(
        Locale.ROOT,
        "host: os=%s arch=%s jvm=%s processors=%d%n",
        System.getProperty("os.name"),
        System.getProperty("os.arch"),
        System.getProperty("java.version"),
        Runtime.getRuntime().availableProcessors());
    System.out.printf(
        Locale.ROOT,
        "input: %s rows=%d dimension=%d nlist=%d threads=%d bits=%d%n",
        vectorsFile,
        rows,
        dimension,
        nlist,
        threads,
        bits);

    VectorIndexAdapter encoding = adapter(dimension, bits, nlist, Map.of());
    List<IndexEntry> entries = entries(vectorsFile, rows, dimension, encoding);
    System.out.printf(
        Locale.ROOT, "entries: %d, file bytes %d%n", entries.size(), entriesBytes(entries));

    VectorIndexAdapter javaKernel = adapter(dimension, bits, nlist, Map.of());
    VectorIndexAdapter nativeKernel =
        adapter(dimension, bits, nlist, Map.of(IvfOptions.BUILD_KERNEL, "native"));

    List<Long> javaMillis = new ArrayList<>();
    List<Long> nativeMillis = new ArrayList<>();
    byte[] javaPayload = null;
    byte[] nativePayload = null;
    Map<String, Long> javaStructure = null;
    Map<String, Long> nativeStructure = null;

    for (int round = 0; round < rounds; round++) {
      // alternate which kernel runs first so a warm cache of the entries does not favour one
      if (round % 2 == 0) {
        IndexBuildResult javaBuild = time("java", javaKernel, entries, threads, javaMillis);
        IndexBuildResult nativeBuild = time("native", nativeKernel, entries, threads, nativeMillis);
        javaPayload = bytes(javaBuild.payload());
        nativePayload = bytes(nativeBuild.payload());
        javaStructure = javaBuild.memory().structure();
        nativeStructure = nativeBuild.memory().structure();
      } else {
        IndexBuildResult nativeBuild = time("native", nativeKernel, entries, threads, nativeMillis);
        IndexBuildResult javaBuild = time("java", javaKernel, entries, threads, javaMillis);
        nativePayload = bytes(nativeBuild.payload());
        javaPayload = bytes(javaBuild.payload());
        nativeStructure = nativeBuild.memory().structure();
        javaStructure = javaBuild.memory().structure();
      }

      System.out.printf(
          Locale.ROOT,
          "round %d: payload java=%d bytes sha256=%s, native=%d bytes sha256=%s, equal=%s%n",
          round,
          javaPayload.length,
          sha256(javaPayload),
          nativePayload.length,
          sha256(nativePayload),
          java.util.Arrays.equals(javaPayload, nativePayload));
    }

    System.out.println("java millis:   " + javaMillis);
    System.out.println("native millis: " + nativeMillis);
    double javaMedian = median(javaMillis);
    double nativeMedian = median(nativeMillis);
    System.out.printf(
        Locale.ROOT,
        "median: java %.0f ms, native %.0f ms, speedup %.2fx%n",
        javaMedian,
        nativeMedian,
        javaMedian / nativeMedian);
    System.out.println("java structure:   " + javaStructure);
    System.out.println("native structure: " + nativeStructure);
    if (!java.util.Arrays.equals(javaPayload, nativePayload)) {
      throw new IllegalStateException("The two kernels wrote different payloads");
    }
  }

  private static IndexBuildResult time(
      String kernel,
      VectorIndexAdapter adapter,
      List<IndexEntry> entries,
      int threads,
      List<Long> samples) {
    long started = System.nanoTime();
    IndexBuildResult result = adapter.encodeAuxiliaryPayloadWithMemory(entries, threads);
    long millis = (System.nanoTime() - started) / 1_000_000L;
    samples.add(millis);
    System.out.printf(
        Locale.ROOT,
        "  %s: %d ms, payload %d bytes, structure build-kernel-native=%s%n",
        kernel,
        millis,
        result.payload().remaining(),
        result.memory().structure().get("build-kernel-native"));
    return result;
  }

  private static VectorIndexAdapter adapter(
      int dimension, int bits, int nlist, Map<String, String> extra) {
    Map<String, String> properties = new LinkedHashMap<>(extra);
    properties.put(IvfOptions.NLIST, Integer.toString(nlist));
    properties.put(IvfOptions.LIST_BLOCKS, "true");
    if (bits > 0) {
      properties.put(RaBitQQuantizer.BITS, Integer.toString(bits));
    }

    return new VectorIndexAdapter(
        VectorIndexDefinition.builder()
            .fieldId(1)
            .dimension(dimension)
            .metric(VectorIndexDefinition.Metric.L2)
            .structure(VectorIndexDefinition.VectorStructure.IVF)
            .quantizer(
                bits > 0
                    ? VectorIndexDefinition.VectorQuantizer.RABITQ
                    : VectorIndexDefinition.VectorQuantizer.NONE)
            .properties(properties)
            .build());
  }

  private static List<IndexEntry> entries(
      Path file, int rows, int dimension, VectorIndexAdapter adapter) throws IOException {
    List<IndexEntry> entries = new ArrayList<>(rows);
    try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
      long bytes = (long) rows * dimension * Float.BYTES;
      if (channel.size() < bytes) {
        throw new IllegalArgumentException(
            String.format(
                Locale.ROOT,
                "%s holds %d bytes, %d rows of %d floats need %d",
                file,
                channel.size(),
                rows,
                dimension,
                bytes));
      }

      FloatBuffer floats =
          channel
              .map(FileChannel.MapMode.READ_ONLY, 0, bytes)
              .order(ByteOrder.LITTLE_ENDIAN)
              .asFloatBuffer();
      float[] vector = new float[dimension];
      for (int row = 0; row < rows; row++) {
        floats.get(vector);
        String dataFile = "data-file-" + (row % 3) + ".parquet";
        entries.add(IndexEntry.of(dataFile, row, adapter.encodeVector(vector)));
      }
    }

    return entries;
  }

  private static long entriesBytes(List<IndexEntry> entries) {
    long bytes = 0L;
    for (IndexEntry entry : entries) {
      bytes += entry.payload().length;
    }

    return bytes;
  }

  private static byte[] bytes(ByteBuffer payload) {
    ByteBuffer copy = payload.duplicate();
    byte[] bytes = new byte[copy.remaining()];
    copy.get(bytes);
    return bytes;
  }

  private static String sha256(byte[] bytes) {
    try {
      return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("Every JVM ships SHA-256", e);
    }
  }

  private static double median(List<Long> samples) {
    List<Long> sorted = new ArrayList<>(samples);
    java.util.Collections.sort(sorted);
    int middle = sorted.size() / 2;
    if (sorted.size() % 2 == 1) {
      return sorted.get(middle);
    }

    return (sorted.get(middle - 1) + sorted.get(middle)) / 2.0;
  }
}
