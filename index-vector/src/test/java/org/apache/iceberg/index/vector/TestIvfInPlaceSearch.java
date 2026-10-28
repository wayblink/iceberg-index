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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.index.IndexAdapter;
import org.apache.iceberg.index.IndexEntry;
import org.apache.iceberg.index.IndexSearchResult;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Searching a prepared IVF searcher in place.
 *
 * <p>Two contracts are checked here that the round-trip tests cannot see. The first is ownership:
 * {@code prepare} takes a snapshot, so the search answers the same whatever the caller does to the
 * buffer it passed afterwards -- moving its position, writing over it, or handing in a slice of
 * something larger. The second is the score itself: a record read where it lies must produce the
 * same bits as the exact scan, and a code read in place must produce the same bits as the code
 * array the old representation decoded, which is the arithmetic this change is not allowed to
 * touch.
 */
class TestIvfInPlaceSearch {

  private static final int DIMENSION = 8;
  private static final int ENTRIES = 60;
  private static final int NLIST = 4;

  @BeforeAll
  static void registerVectorIndexType() {
    VectorIndexTypes.register();
  }

  @Test
  void answersTheSameWhateverTheCallerDoesToTheBufferAfterwards() {
    List<IndexEntry> entries = entries(VectorIndexDefinition.Metric.EUCLIDEAN, ENTRIES);
    VectorIndexAdapter ivf = ivfAdapter(VectorIndexDefinition.Metric.EUCLIDEAN, NLIST, NLIST);
    byte[] payload = VectorPayloads.toBytes(ivf.encodeAuxiliaryPayload(entries, 1));
    ByteBuffer query = ivf.encodeQuery(vector(0));

    for (String shape : List.of("heap", "direct", "slice", "read-only")) {
      Source source = source(shape, payload);
      int position = source.buffer.position();
      int limit = source.buffer.limit();

      IndexAdapter.PreparedSearch prepared = ivf.prepareAuxiliary(source.buffer);
      assertThat(source.buffer.position())
          .as("%s position after prepare", shape)
          .isEqualTo(position);
      assertThat(source.buffer.limit()).as("%s limit after prepare", shape).isEqualTo(limit);
      List<IndexSearchResult> before = prepared.search(query, 5);

      // the bytes the caller handed in are not the bytes the search reads: overwriting every one
      // of them leaves the answer, and a second search on the same searcher, alone
      source.corrupt();
      assertThat(prepared.search(query, 5))
          .as("%s search after the source was overwritten", shape)
          .isEqualTo(before);
      assertThat(prepared.search(query, 5))
          .as("%s second search on the same searcher", shape)
          .isEqualTo(before);

      // and a searcher prepared from the overwritten bytes fails rather than answering from them
      assertThatThrownBy(() -> ivf.prepareAuxiliary(source.buffer))
          .as("%s prepare from overwritten bytes", shape)
          .isInstanceOf(IllegalArgumentException.class);
    }
  }

  @Test
  void scoresEveryRecordTheWayTheExactScanDoes() {
    for (VectorIndexDefinition.Metric metric : VectorIndexDefinition.Metric.values()) {
      List<IndexEntry> entries = entries(metric, ENTRIES);
      VectorIndexAdapter ivf = ivfAdapter(metric, NLIST, NLIST);
      VectorIndexAdapter exact = flatAdapter(metric);

      IndexAdapter.PreparedSearch prepared =
          ivf.prepareAuxiliary(ivf.encodeAuxiliaryPayload(entries, 1));
      IndexAdapter.PreparedSearch scan = exact.prepare(exact.encodeEntries(entries));

      for (int q = 0; q < 8; q++) {
        ByteBuffer query = ivf.encodeQuery(vector(q * 5));
        List<IndexSearchResult> fromIvf = prepared.search(query, ENTRIES);
        List<IndexSearchResult> fromScan = scan.search(query, ENTRIES);

        assertThat(fromIvf)
            .as("%s full probe against the scan for query %s", metric, q)
            .hasSameSizeAs(fromScan);
        for (int rank = 0; rank < fromScan.size(); rank++) {
          IndexSearchResult expected = fromScan.get(rank);
          IndexSearchResult actual = fromIvf.get(rank);
          assertThat(actual.dataFile()).isEqualTo(expected.dataFile());
          assertThat(actual.rowPosition()).isEqualTo(expected.rowPosition());
          assertThat(Double.doubleToLongBits(actual.score()))
              .as("%s score bits at rank %s for query %s", metric, rank, q)
              .isEqualTo(Double.doubleToLongBits(expected.score()));
        }
      }
    }
  }

  @Test
  void takesTheSmallerOfTopKAndTheCandidatesItHas() {
    List<IndexEntry> entries = entries(VectorIndexDefinition.Metric.EUCLIDEAN, ENTRIES);
    VectorIndexAdapter ivf = ivfAdapter(VectorIndexDefinition.Metric.EUCLIDEAN, NLIST, NLIST);
    IndexAdapter.PreparedSearch prepared =
        ivf.prepareAuxiliary(ivf.encodeAuxiliaryPayload(entries, 1));
    ByteBuffer query = ivf.encodeQuery(vector(3));

    assertThat(prepared.search(query, 0)).isEmpty();
    assertThat(prepared.search(query, 1)).hasSize(1);
    // asking for more than the index holds returns what it holds rather than failing or padding
    assertThat(prepared.search(query, 1_000_000)).hasSize(ENTRIES);
  }

  @Test
  void readsACodeInPlaceExactlyAsTheCodeArrayWasRead() {
    int bits = 2;
    int seed = 7;
    float[] center = vector(0);
    RaBitQQuantizer quantizer = new RaBitQQuantizer(DIMENSION, seed, bits, center);
    int codeBytes = quantizer.codeBytes();

    List<IvfTestPayloads.Code> records = new ArrayList<>();
    byte[][] codes = new byte[8][];
    float[][] factors = new float[8][];
    for (int record = 0; record < 8; record++) {
      byte[] code = new byte[codeBytes];
      float[] recordFactors = new float[RaBitQQuantizer.FACTORS];
      quantizer.encode(vector(record + 1), code, 0, recordFactors);
      codes[record] = code;
      factors[record] = recordFactors;
      records.add(new IvfTestPayloads.Code(0, record * 3L, code, recordFactors));
    }

    byte[] payload =
        IvfTestPayloads.quantized(
            DIMENSION,
            bits,
            seed,
            new String[] {"data-0.parquet"},
            new float[][] {center},
            8,
            new float[][] {center},
            List.of(records));

    VectorIndexAdapter ivf = quantizedAdapter(bits, 8);
    IndexAdapter.PreparedSearch prepared = ivf.prepareAuxiliary(ByteBuffer.wrap(payload));

    for (int q = 0; q < 5; q++) {
      float[] queryVector = vector(q * 3 + 2);
      List<IndexSearchResult> results = prepared.search(ivf.encodeQuery(queryVector), 8);

      List<IndexSearchResult> expected = reference(queryVector, center, codes, factors);
      assertThat(results).as("records scored for query %s", q).hasSameSizeAs(expected);
      for (int rank = 0; rank < expected.size(); rank++) {
        assertThat(results.get(rank).rowPosition()).isEqualTo(expected.get(rank).rowPosition());
        assertThat(Double.doubleToLongBits(results.get(rank).score()))
            .as("score bits at rank %s for query %s", rank, q)
            .isEqualTo(Double.doubleToLongBits(expected.get(rank).score()));
      }
    }
  }

  @Test
  @Timeout(60)
  void answersTheSameWhenTheSearcherIsSearchedConcurrently() throws Exception {
    List<IndexEntry> entries = entries(VectorIndexDefinition.Metric.EUCLIDEAN, 400);
    VectorIndexAdapter ivf = ivfAdapter(VectorIndexDefinition.Metric.EUCLIDEAN, 8, 8);
    IndexAdapter.PreparedSearch prepared =
        ivf.prepareAuxiliary(ivf.encodeAuxiliaryPayload(entries, 1));

    List<ByteBuffer> queries = Lists.newArrayList();
    List<List<IndexSearchResult>> singleThreaded = Lists.newArrayList();
    for (int q = 0; q < 16; q++) {
      ByteBuffer query = ivf.encodeQuery(vector(q * 11));
      queries.add(query);
      singleThreaded.add(prepared.search(query, 10));
    }

    ExecutorService pool = Executors.newFixedThreadPool(4);
    try {
      List<Callable<List<List<IndexSearchResult>>>> tasks = Lists.newArrayList();
      for (int thread = 0; thread < 4; thread++) {
        tasks.add(
            () -> {
              List<List<IndexSearchResult>> answers = Lists.newArrayList();
              for (ByteBuffer query : queries) {
                answers.add(prepared.search(query, 10));
              }
              return answers;
            });
      }

      for (Future<List<List<IndexSearchResult>>> future : pool.invokeAll(tasks)) {
        List<List<IndexSearchResult>> answers = future.get(60, TimeUnit.SECONDS);
        for (int q = 0; q < queries.size(); q++) {
          assertThat(answers.get(q))
              .as("concurrent answer for query %s", q)
              .isEqualTo(singleThreaded.get(q));
        }
      }
    } finally {
      pool.shutdownNow();
    }
  }

  /** The score the decoded-code representation produced, computed from a code array. */
  private static List<IndexSearchResult> reference(
      float[] queryVector, float[] center, byte[][] codes, float[][] factors) {
    RaBitQQuantizer quantizer = new RaBitQQuantizer(DIMENSION, 7, 2, center);
    float[] rotatedQuery = quantizer.rotateQuery(queryVector);
    double queryLengthSquared = 0.0;
    for (int d = 0; d < DIMENSION; d++) {
      double difference = queryVector[d] - center[d];
      queryLengthSquared += difference * difference;
    }

    double centerSquared = 0.0;
    double querySquared = 0.0;
    for (int d = 0; d < DIMENSION; d++) {
      centerSquared += (double) center[d] * center[d];
      querySquared += (double) queryVector[d] * queryVector[d];
    }

    List<IndexSearchResult> results = Lists.newArrayList();
    for (int record = 0; record < codes.length; record++) {
      double inner =
          quantizer.estimatedInnerProduct(
              codes[record], 0, factors[record], rotatedQuery, queryVector);
      double vectorSquared = RaBitQQuantizer.squaredLength(factors[record], centerSquared);
      double similarity =
          vectorSquared <= 0.0 || querySquared <= 0.0
              ? 0.0
              : inner / (Math.sqrt(vectorSquared) * Math.sqrt(querySquared));
      // the metric is cosine for this fixture, which is the one whose reference needs both terms
      results.add(IndexSearchResult.of("data-0.parquet", record * 3L, similarity));
    }

    results.sort(new VectorScoring(VectorIndexDefinition.Metric.COSINE).comparator());
    return results;
  }

  /** A buffer in one of the shapes a caller can hand in, and a way to overwrite its bytes. */
  private static final class Source {
    private final ByteBuffer buffer;
    private final Runnable overwrite;

    private Source(ByteBuffer buffer, Runnable overwrite) {
      this.buffer = buffer;
      this.overwrite = overwrite;
    }

    private void corrupt() {
      overwrite.run();
    }
  }

  private static Source source(String shape, byte[] bytes) {
    switch (shape) {
      case "heap":
        byte[] heap = bytes.clone();
        return new Source(ByteBuffer.wrap(heap), () -> Arrays.fill(heap, (byte) 0xFF));
      case "direct":
        ByteBuffer direct = ByteBuffer.allocateDirect(bytes.length);
        direct.put(bytes);
        direct.flip();
        return new Source(
            direct,
            () -> {
              ByteBuffer copy = direct.duplicate();
              for (int i = copy.position(); i < copy.limit(); i++) {
                copy.put(i, (byte) 0xFF);
              }
            });
      case "slice":
        ByteBuffer larger = ByteBuffer.allocate(bytes.length + 32);
        larger.position(9);
        larger.put(bytes);
        larger.position(9);
        larger.limit(9 + bytes.length);
        return new Source(
            larger.slice(),
            () -> {
              byte[] storage = larger.array();
              Arrays.fill(storage, 9, 9 + bytes.length, (byte) 0xFF);
            });
      case "read-only":
        byte[] hidden = bytes.clone();
        return new Source(
            ByteBuffer.wrap(hidden).asReadOnlyBuffer(), () -> Arrays.fill(hidden, (byte) 0xFF));
      default:
        throw new IllegalArgumentException("Unknown buffer shape: " + shape);
    }
  }

  private static List<IndexEntry> entries(VectorIndexDefinition.Metric metric, int count) {
    VectorIndexAdapter flat = flatAdapter(metric);
    List<IndexEntry> entries = Lists.newArrayListWithCapacity(count);
    for (int i = 0; i < count; i++) {
      entries.add(IndexEntry.of("data-" + (i / 20) + ".parquet", i, flat.encodeVector(vector(i))));
    }

    return entries;
  }

  private static VectorIndexAdapter flatAdapter(VectorIndexDefinition.Metric metric) {
    return new VectorIndexAdapter(
        VectorIndexDefinition.builder()
            .fieldId(1)
            .dimension(DIMENSION)
            .metric(metric)
            .structure(VectorIndexDefinition.VectorStructure.FLAT)
            .build());
  }

  private static VectorIndexAdapter ivfAdapter(
      VectorIndexDefinition.Metric metric, int nlist, int nprobe) {
    return new VectorIndexAdapter(
        VectorIndexDefinition.builder()
            .fieldId(1)
            .dimension(DIMENSION)
            .metric(metric)
            .structure(VectorIndexDefinition.VectorStructure.IVF)
            .property(IvfOptions.NLIST, Integer.toString(nlist))
            .property(IvfOptions.NPROBE, Integer.toString(nprobe))
            .build());
  }

  private static VectorIndexAdapter quantizedAdapter(int bits, int nprobe) {
    return new VectorIndexAdapter(
        VectorIndexDefinition.builder()
            .fieldId(1)
            .dimension(DIMENSION)
            .metric(VectorIndexDefinition.Metric.COSINE)
            .structure(VectorIndexDefinition.VectorStructure.IVF)
            .quantizer(VectorIndexDefinition.VectorQuantizer.RABITQ)
            .property(RaBitQQuantizer.BITS, Integer.toString(bits))
            .property(IvfOptions.NPROBE, Integer.toString(nprobe))
            .build());
  }

  /** A vector distinct for every entry, so a hit can only be the entry itself. */
  private static float[] vector(int entry) {
    float[] values = new float[DIMENSION];
    for (int d = 0; d < DIMENSION; d++) {
      values[d] = (float) Math.sin(entry * 0.7 + d * 1.3) + 0.5f;
    }

    return values;
  }
}
