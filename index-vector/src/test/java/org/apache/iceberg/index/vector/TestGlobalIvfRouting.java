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

import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;
import java.util.SplittableRandom;
import org.apache.iceberg.index.vector.GlobalIvfRouter.Candidate;
import org.apache.iceberg.index.vector.GlobalIvfRouter.RoutingDecision;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * What global routing changes about a query's fan-out and its recall.
 *
 * <p>The data are synthetic and deliberately dense in clusters: vectors are drawn around true
 * centres, the global centroids are trained on them, and the shards are either packed by cluster
 * (the layout routing is designed for) or dealt round-robin (a layout where a cluster is spread
 * over every shard). The two layouts separate what routing buys from where the clusters are: the
 * candidate count falls with the probed fraction of the centroid space in both, while the number of
 * files touched falls only when a cluster fits in a few shards.
 *
 * <p>Recall is measured against the exact scan over the same vectors. The comparison arm is an
 * independent IVF per shard, which is what the current layout does: every shard probes {@code
 * nprobe} of its own lists and every query pays for every shard.
 */
class TestGlobalIvfRouting {

  private static final int DIMENSION = 16;
  private static final int TRUE_CLUSTERS = 64;
  private static final int ROWS_PER_TRUE_CLUSTER = 512;
  private static final int SHARDS = 32;
  private static final int GLOBAL_CLUSTERS = 128;
  private static final int NPROBE = 8;
  private static final int QUERIES = 64;
  private static final int K = 10;
  private static final double NOISE = 0.25;
  private static final long SEED = 20261118L;

  private static final VectorScoring SCORING = new VectorScoring(VectorIndexDefinition.Metric.L2);

  private static Corpus corpus;
  private static Sharded clusterShards;
  private static GlobalIvfRouter router;
  private static float[][] queries;
  private static int[][] truth;

  @BeforeAll
  static void prepareCorpusAndRouter() {
    corpus = corpus(SEED, TRUE_CLUSTERS, ROWS_PER_TRUE_CLUSTER);
    clusterShards = shardByTrueCluster(corpus, SHARDS);
    router = GlobalIvfRouter.train(clusterShards.shards, GLOBAL_CLUSTERS, SEED, SCORING);
    queries = queries(corpus, QUERIES, SEED);
    truth = new int[QUERIES][];
    for (int query = 0; query < QUERIES; query++) {
      truth[query] = exactTopK(queries[query], corpus.vectors, K);
    }
  }

  @Test
  void aSampleOfTwoKeepsBothEndsOfTheInput() {
    List<float[]> vectors = Lists.newArrayList();
    for (int i = 0; i < 10; i += 1) {
      vectors.add(new float[] {i, 0F});
    }

    float[][] sample = IvfCentroids.sample(vectors, 2);
    assertThat(sample.length).isEqualTo(2);
    assertThat(sample[0][0]).as("the first vector is always sampled").isZero();
    assertThat(sample[1][0]).as("the last vector is always sampled").isEqualTo(9F);
  }

  @Test
  void aSampleLargerThanTheInputKeepsEveryVector() {
    List<float[]> vectors = Lists.newArrayList(new float[] {1F, 2F}, new float[] {3F, 4F});
    assertThat(IvfCentroids.sample(vectors, 10).length).isEqualTo(2);
  }

  @Test
  void trainsTheSameCentroidsForTheSameInputAndSeed() {
    GlobalIvfRouter first = GlobalIvfRouter.train(clusterShards.shards, 32, SEED, SCORING);
    GlobalIvfRouter second = GlobalIvfRouter.train(clusterShards.shards, 32, SEED, SCORING);

    assertThat(first.clusterCount()).isEqualTo(32);
    assertThat(second.clusterCount()).isEqualTo(32);
    for (int centroid = 0; centroid < first.clusterCount(); centroid++) {
      assertThat(second.centroids().centroid(centroid))
          .as("centroid %s", centroid)
          .containsExactly(first.centroids().centroid(centroid));
    }

    for (int query = 0; query < 8; query++) {
      RoutingDecision left = first.route(queries[query], NPROBE);
      RoutingDecision right = second.route(queries[query], NPROBE);
      assertThat(right.clusterIds()).containsExactly(left.clusterIds());
      assertThat(right.candidateEntries()).isEqualTo(left.candidateEntries());
      assertThat(right.touchedShards())
          .extracting(IvfShard::location)
          .containsExactlyElementsOf(Lists.transform(left.touchedShards(), IvfShard::location));
    }
  }

  @Test
  void routesTheSameDecisionForTheSameQuery() {
    RoutingDecision first = router.route(queries[0], NPROBE);
    RoutingDecision second = router.route(queries[0], NPROBE);

    assertThat(second.clusterIds()).containsExactly(first.clusterIds());
    assertThat(second.touchedShards())
        .extracting(IvfShard::location)
        .containsExactlyElementsOf(Lists.transform(first.touchedShards(), IvfShard::location));
    assertThat(second.candidates()).hasSameSizeAs(first.candidates());
    assertThat(second.candidateEntries()).isEqualTo(first.candidateEntries());
  }

  @Test
  void buildsTheSameCoverageFromPublishedCentroids() {
    GlobalIvfRouter rebuilt = GlobalIvfRouter.of(router.centroids(), clusterShards.shards);

    for (int query = 0; query < 8; query++) {
      RoutingDecision expected = router.route(queries[query], NPROBE);
      RoutingDecision actual = rebuilt.route(queries[query], NPROBE);
      assertThat(actual.clusterIds()).containsExactly(expected.clusterIds());
      assertThat(actual.candidateEntries()).isEqualTo(expected.candidateEntries());
      assertThat(actual.touchedShards())
          .extracting(IvfShard::location)
          .containsExactlyElementsOf(Lists.transform(expected.touchedShards(), IvfShard::location));
    }
  }

  @Test
  void recallRisesWithTheNumberOfProbedClusters() {
    LocalIvf local = localIvf(clusterShards);

    double previousRouted = -1.0;
    double previousLocal = -1.0;
    for (int nprobe : new int[] {1, 2, 4, 8, 16}) {
      double routedRecall = 0.0;
      double localRecall = 0.0;
      long routedCandidates = 0L;
      long localCandidates = 0L;
      for (int query = 0; query < QUERIES; query++) {
        RoutingDecision decision = router.route(queries[query], nprobe);
        routedRecall += recall(truth[query], routedTopK(decision, clusterShards, queries[query]));
        localRecall +=
            recall(truth[query], localTopK(local, clusterShards, queries[query], nprobe));
        routedCandidates += decision.candidateEntries();
        localCandidates += local.candidateCount(queries[query], nprobe);
      }

      routedRecall /= QUERIES;
      localRecall /= QUERIES;
      System.out.println(
          "[global-routing] nprobe="
              + nprobe
              + " routed recall="
              + routedRecall
              + " candidates="
              + (routedCandidates / QUERIES)
              + " | local-ivf recall="
              + localRecall
              + " candidates="
              + (localCandidates / QUERIES));

      assertThat(routedRecall).isGreaterThanOrEqualTo(previousRouted);
      assertThat(localRecall).isGreaterThanOrEqualTo(previousLocal);
      previousRouted = routedRecall;
      previousLocal = localRecall;
    }
  }

  @Test
  void touchesOnlyShardsThatHoldAProbedCluster() {
    int[] clusterOfRow = new int[corpus.vectors.length];
    for (int row = 0; row < corpus.vectors.length; row++) {
      clusterOfRow[row] = router.centroids().nearestCentroid(corpus.vectors[row]);
    }

    for (int query = 0; query < 8; query++) {
      RoutingDecision decision = router.route(queries[query], NPROBE);
      boolean[] probed = new boolean[router.clusterCount()];
      for (int cluster : decision.clusterIds()) {
        probed[cluster] = true;
      }

      List<String> expected = Lists.newArrayList();
      long expectedCandidates = 0L;
      for (int shard = 0; shard < clusterShards.shards.size(); shard++) {
        int[] rows = clusterShards.globalRows[shard];
        long inShard = 0L;
        for (int row : rows) {
          if (probed[clusterOfRow[row]]) {
            inShard += 1L;
          }
        }
        if (inShard > 0L) {
          expected.add(clusterShards.shards.get(shard).location());
          expectedCandidates += inShard;
        }
      }

      assertThat(decision.touchedShards())
          .as("query %s touches the shards that cover its probed clusters", query)
          .extracting(IvfShard::location)
          .containsExactlyInAnyOrderElementsOf(expected);
      assertThat(decision.candidateEntries()).isEqualTo(expectedCandidates);
      assertThat(decision.candidates()).hasSize((int) expectedCandidates);
      for (Candidate candidate : decision.candidates()) {
        assertThat(probed[clusterOfRow[rowOf(candidate, clusterShards)]]).isTrue();
      }
    }
  }

  @Test
  void fanoutFallsWhenClustersArePackedIntoShards() {
    double totalTouched = 0.0;
    long totalCandidates = 0L;
    for (int query = 0; query < QUERIES; query++) {
      RoutingDecision decision = router.route(queries[query], NPROBE);
      totalTouched += decision.touchedShards().size();
      totalCandidates += decision.candidateEntries();
    }

    double averageTouched = totalTouched / QUERIES;
    double candidateRatio = (double) totalCandidates / (QUERIES * (double) corpus.vectors.length);
    System.out.println(
        "[global-routing] cluster-aligned shards: avg touched shards="
            + averageTouched
            + "/"
            + SHARDS
            + ", candidate ratio="
            + candidateRatio);

    assertThat(averageTouched).isLessThan(SHARDS);
    assertThat(averageTouched).isLessThanOrEqualTo(NPROBE);
    assertThat(candidateRatio).isLessThan(0.15);
  }

  @Test
  void recallHoldsAgainstTheExactScanWhileCandidatesFall() {
    LocalIvf local = localIvf(clusterShards);

    double routedRecall = 0.0;
    double localRecall = 0.0;
    long routedCandidates = 0L;
    long localCandidates = 0L;
    long allCandidates = 0L;
    for (int query = 0; query < QUERIES; query++) {
      RoutingDecision decision = router.route(queries[query], NPROBE);
      int[] routed = routedTopK(decision, clusterShards, queries[query]);
      int[] localHits = localTopK(local, clusterShards, queries[query], NPROBE);
      routedRecall += recall(truth[query], routed);
      localRecall += recall(truth[query], localHits);
      routedCandidates += decision.candidateEntries();
      localCandidates += local.candidateCount(queries[query], NPROBE);
      allCandidates += corpus.vectors.length;
    }

    routedRecall /= QUERIES;
    localRecall /= QUERIES;
    double candidateRatio = (double) routedCandidates / (QUERIES * (double) corpus.vectors.length);
    System.out.println(
        "[global-routing] recall@"
            + K
            + " cluster-aligned: exact recall=1.000 candidates="
            + (allCandidates / QUERIES)
            + " touched="
            + SHARDS
            + " | local-ivf recall="
            + localRecall
            + " candidates="
            + (localCandidates / QUERIES)
            + " touched="
            + SHARDS
            + " | global recall="
            + routedRecall
            + " candidates="
            + (routedCandidates / QUERIES)
            + " ratio="
            + candidateRatio
            + " avg touched="
            + averageTouched(QUERIES));

    assertThat(routedRecall).isGreaterThan(0.8);
    assertThat(routedCandidates).isLessThan(localCandidates);
    assertThat(localRecall).isGreaterThan(0.8);
  }

  @Test
  void randomShardingLosesFileFanoutButKeepsTheCandidateFall() {
    Sharded dealt = shardRoundRobin(corpus, SHARDS);
    GlobalIvfRouter dealtRouter =
        GlobalIvfRouter.train(dealt.shards, GLOBAL_CLUSTERS, SEED, SCORING);

    double averageTouched = 0.0;
    long totalCandidates = 0L;
    for (int query = 0; query < QUERIES; query++) {
      RoutingDecision decision = dealtRouter.route(queries[query], NPROBE);
      averageTouched += decision.touchedShards().size();
      totalCandidates += decision.candidateEntries();
    }

    averageTouched /= QUERIES;
    double candidateRatio = (double) totalCandidates / (QUERIES * (double) corpus.vectors.length);
    System.out.println(
        "[global-routing] round-robin shards: avg touched shards="
            + averageTouched
            + "/"
            + SHARDS
            + ", candidate ratio="
            + candidateRatio);

    assertThat(averageTouched).isEqualTo(SHARDS);
    assertThat(candidateRatio).isLessThan(0.15);
  }

  @Test
  void probingEveryClusterIsTheExactAnswer() {
    for (int query = 0; query < 4; query++) {
      RoutingDecision decision = router.route(queries[query], router.clusterCount());
      assertThat(decision.clusterIds()).hasSize(router.clusterCount());
      assertThat(decision.touchedShards()).hasSize(SHARDS);
      assertThat(decision.candidateEntries()).isEqualTo(corpus.vectors.length);
      assertThat(routedTopK(decision, clusterShards, queries[query]))
          .as("query %s", query)
          .containsExactly(truth[query]);
    }
  }

  @Test
  void zeroProbesTouchNothing() {
    RoutingDecision decision = router.route(queries[0], 0);

    assertThat(decision.isEmpty()).isTrue();
    assertThat(decision.clusterIds()).isEmpty();
    assertThat(decision.touchedShards()).isEmpty();
    assertThat(decision.candidates()).isEmpty();
    assertThat(decision.candidateEntries()).isZero();
    assertThat(router.route(queries[0], -1).isEmpty()).isTrue();
  }

  @Test
  void oneClusterTouchesEveryShardItCovers() {
    GlobalIvfRouter single = GlobalIvfRouter.train(clusterShards.shards, 1, SEED, SCORING);
    assertThat(single.clusterCount()).isEqualTo(1);

    RoutingDecision decision = single.route(queries[0], 1);
    assertThat(decision.clusterIds()).containsExactly(0);
    assertThat(decision.touchedShards()).hasSize(SHARDS);
    assertThat(decision.candidateEntries()).isEqualTo(corpus.vectors.length);
  }

  @Test
  void nprobeAboveTheClusterCountVisitsEveryCluster() {
    GlobalIvfRouter small = GlobalIvfRouter.train(clusterShards.shards, 4, SEED, SCORING);
    RoutingDecision decision = small.route(queries[0], 1000);

    assertThat(decision.clusterIds()).hasSize(4);
    assertThat(decision.candidateEntries()).isEqualTo(corpus.vectors.length);
  }

  @Test
  void emptyDataRoutesNothing() {
    GlobalIvfRouter empty = GlobalIvfRouter.train(Lists.newArrayList(), 8, SEED, SCORING);
    assertThat(empty.clusterCount()).isZero();
    assertThat(empty.entries()).isZero();
    assertThat(empty.route(queries[0], 8).isEmpty()).isTrue();

    List<IvfShard> onlyEmpty = Lists.newArrayList(IvfShard.of("empty", new float[0][]));
    GlobalIvfRouter emptyShard = GlobalIvfRouter.train(onlyEmpty, 8, SEED, SCORING);
    assertThat(emptyShard.clusterCount()).isZero();
    assertThat(emptyShard.route(queries[0], 8).isEmpty()).isTrue();

    List<IvfShard> withEmpty = Lists.newArrayList(clusterShards.shards);
    withEmpty.add(IvfShard.of("empty", new float[0][]));
    GlobalIvfRouter mixed = GlobalIvfRouter.train(withEmpty, 8, SEED, SCORING);
    for (int query = 0; query < 4; query++) {
      assertThat(mixed.route(queries[query], NPROBE).touchedShards())
          .extracting(IvfShard::location)
          .doesNotContain("empty");
    }
  }

  private static double averageTouched(int queryCount) {
    double total = 0.0;
    for (int query = 0; query < queryCount; query++) {
      total += router.route(queries[query], NPROBE).touchedShards().size();
    }
    return total / queryCount;
  }

  /** The row a candidate points at, through the shard it belongs to. */
  private static int rowOf(Candidate candidate, Sharded sharded) {
    for (int shard = 0; shard < sharded.shards.size(); shard++) {
      if (sharded.shards.get(shard) == candidate.shard()) {
        return sharded.globalRows[shard][candidate.entry()];
      }
    }

    throw new IllegalStateException("Candidate shard is not part of the corpus");
  }

  private static int[] routedTopK(RoutingDecision decision, Sharded sharded, float[] query) {
    TopK top = new TopK(K, SCORING);
    for (Candidate candidate : decision.candidates()) {
      top.offer(rowOf(candidate, sharded), SCORING.score(query, candidate.vector()));
    }
    return top.ids();
  }

  private static int[] localTopK(LocalIvf local, Sharded sharded, float[] query, int nprobe) {
    TopK top = new TopK(K, SCORING);
    for (int shard = 0; shard < sharded.shards.size(); shard++) {
      IvfCentroids centroids = local.centroids.get(shard);
      int[] assignment = local.clusterOf.get(shard);
      boolean[] probed = new boolean[centroids.count()];
      for (int cluster : centroids.nearestCentroids(query, nprobe)) {
        probed[cluster] = true;
      }

      float[] vector;
      int[] rows = sharded.globalRows[shard];
      for (int entry = 0; entry < assignment.length; entry++) {
        if (probed[assignment[entry]]) {
          vector = sharded.shards.get(shard).vector(entry);
          top.offer(rows[entry], SCORING.score(query, vector));
        }
      }
    }
    return top.ids();
  }

  /** The independent IVF of every shard that the current layout searches. */
  private static LocalIvf localIvf(Sharded sharded) {
    LocalIvf local = new LocalIvf();
    for (IvfShard shard : sharded.shards) {
      IvfCentroids centroids =
          IvfCentroids.train(
              Arrays.asList(shard.vectors()), localListCount(shard.entryCount()), SEED, SCORING);
      local.centroids.add(centroids);
      int[] assignment = new int[shard.entryCount()];
      for (int entry = 0; entry < assignment.length; entry++) {
        assignment[entry] = centroids.nearestCentroid(shard.vector(entry));
      }
      local.clusterOf.add(assignment);
    }
    return local;
  }

  private static final class LocalIvf {
    private final List<IvfCentroids> centroids = Lists.newArrayList();
    private final List<int[]> clusterOf = Lists.newArrayList();

    private long candidateCount(float[] query, int nprobe) {
      long count = 0L;
      for (int shard = 0; shard < centroids.size(); shard++) {
        IvfCentroids shardCentroids = centroids.get(shard);
        boolean[] probed = new boolean[shardCentroids.count()];
        for (int cluster : shardCentroids.nearestCentroids(query, nprobe)) {
          probed[cluster] = true;
        }
        for (int cluster : clusterOf.get(shard)) {
          if (probed[cluster]) {
            count += 1L;
          }
        }
      }
      return count;
    }
  }

  private static int localListCount(int entries) {
    return Math.max(1, Math.min(entries, (int) Math.round(Math.sqrt(entries))));
  }

  private static int[] exactTopK(float[] query, float[][] vectors, int k) {
    TopK top = new TopK(k, SCORING);
    for (int row = 0; row < vectors.length; row++) {
      top.offer(row, SCORING.score(query, vectors[row]));
    }
    return top.ids();
  }

  private static double recall(int[] truth, int[] hits) {
    int found = 0;
    for (int expected : truth) {
      for (int hit : hits) {
        if (expected == hit) {
          found += 1;
          break;
        }
      }
    }
    return truth.length == 0 ? 0.0 : (double) found / truth.length;
  }

  private static Corpus corpus(long seed, int trueClusters, int rowsPerCluster) {
    SplittableRandom random = new SplittableRandom(seed);
    float[][] centers = new float[trueClusters][DIMENSION];
    for (float[] center : centers) {
      for (int d = 0; d < DIMENSION; d++) {
        center[d] = (float) (random.nextGaussian() * 2.0);
      }
    }

    int rows = trueClusters * rowsPerCluster;
    int[] order = new int[rows];
    for (int row = 0; row < rows; row++) {
      order[row] = row % trueClusters;
    }

    for (int row = rows - 1; row > 0; row--) {
      int pick = random.nextInt(row + 1);
      int swap = order[row];
      order[row] = order[pick];
      order[pick] = swap;
    }

    int[] trueCluster = new int[rows];
    float[][] vectors = new float[rows][];
    for (int row = 0; row < rows; row++) {
      int cluster = order[row];
      trueCluster[row] = cluster;
      float[] vector = new float[DIMENSION];
      for (int d = 0; d < DIMENSION; d++) {
        vector[d] = centers[cluster][d] + (float) (random.nextGaussian() * NOISE);
      }
      vectors[row] = vector;
    }
    return new Corpus(centers, vectors, trueCluster);
  }

  private static float[][] queries(Corpus corpus, int count, long seed) {
    SplittableRandom random = new SplittableRandom(seed ^ 0x5DEECE66DL);
    float[][] queries = new float[count][];
    for (int query = 0; query < count; query++) {
      float[] center = corpus.centers[random.nextInt(corpus.centers.length)];
      float[] vector = new float[DIMENSION];
      for (int d = 0; d < DIMENSION; d++) {
        vector[d] = center[d] + (float) (random.nextGaussian() * NOISE);
      }
      queries[query] = vector;
    }
    return queries;
  }

  /** Packs whole true clusters into the same shard: the layout a routing build produces. */
  private static Sharded shardByTrueCluster(Corpus corpus, int shardCount) {
    return shard(corpus, shardCount, row -> corpus.trueCluster[row] % shardCount);
  }

  /** Deals rows round-robin, so every shard holds a slice of every cluster. */
  private static Sharded shardRoundRobin(Corpus corpus, int shardCount) {
    return shard(corpus, shardCount, row -> row % shardCount);
  }

  private static Sharded shard(Corpus corpus, int shardCount, ShardOf shardOf) {
    int[] sizes = new int[shardCount];
    for (int row = 0; row < corpus.vectors.length; row++) {
      sizes[shardOf.shardOf(row)] += 1;
    }

    int[][] globalRows = new int[shardCount][];
    for (int shard = 0; shard < shardCount; shard++) {
      globalRows[shard] = new int[sizes[shard]];
    }

    int[] filled = new int[shardCount];
    for (int row = 0; row < corpus.vectors.length; row++) {
      int shard = shardOf.shardOf(row);
      globalRows[shard][filled[shard]] = row;
      filled[shard] += 1;
    }

    List<IvfShard> shards = Lists.newArrayList();
    for (int shard = 0; shard < shardCount; shard++) {
      float[][] vectors = new float[globalRows[shard].length][];
      for (int entry = 0; entry < vectors.length; entry++) {
        vectors[entry] = corpus.vectors[globalRows[shard][entry]];
      }
      shards.add(IvfShard.of("shard-" + shard, vectors));
    }
    return new Sharded(shards, globalRows);
  }

  private interface ShardOf {
    int shardOf(int row);
  }

  private static final class Corpus {
    private final float[][] centers;
    private final float[][] vectors;
    private final int[] trueCluster;

    private Corpus(float[][] centers, float[][] vectors, int[] trueCluster) {
      this.centers = centers;
      this.vectors = vectors;
      this.trueCluster = trueCluster;
    }
  }

  private static final class Sharded {
    private final List<IvfShard> shards;
    private final int[][] globalRows;

    private Sharded(List<IvfShard> shards, int[][] globalRows) {
      this.shards = shards;
      this.globalRows = globalRows;
    }
  }

  private record Hit(int id, double score) {}

  /** A bounded top-k, worst hit first, with the tie-break the exact scan uses. */
  private static final class TopK {
    private final int k;
    private final VectorScoring scoring;
    private final PriorityQueue<Hit> worstFirst;

    private TopK(int k, VectorScoring scoring) {
      this.k = k;
      this.scoring = scoring;
      this.worstFirst =
          new PriorityQueue<>(
              (left, right) -> {
                int byScore = scoring.compareScores(right.score(), left.score());
                return byScore != 0 ? byScore : Integer.compare(right.id(), left.id());
              });
    }

    private void offer(int id, double score) {
      if (worstFirst.size() < k) {
        worstFirst.add(new Hit(id, score));
        return;
      }

      Hit worst = worstFirst.peek();
      int byScore = scoring.compareScores(score, worst.score());
      if (byScore < 0 || (byScore == 0 && id < worst.id())) {
        worstFirst.poll();
        worstFirst.add(new Hit(id, score));
      }
    }

    private int[] ids() {
      Hit[] hits = worstFirst.toArray(new Hit[0]);
      Arrays.sort(
          hits,
          (left, right) -> {
            int byScore = scoring.compareScores(left.score(), right.score());
            return byScore != 0 ? byScore : Integer.compare(left.id(), right.id());
          });
      int[] ids = new int[hits.length];
      for (int i = 0; i < hits.length; i++) {
        ids[i] = hits[i].id();
      }
      return ids;
    }
  }
}
