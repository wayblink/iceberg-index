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

import java.util.Arrays;
import java.util.List;
import java.util.SplittableRandom;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * A set of IVF centroids and the nearest-centroid lookup a router uses.
 *
 * <p>The same class trains both the global coarse centroids of a routing prototype and the local
 * centroids a single shard would train today, so the two can be compared on the same data. The
 * vectors are sampled with a fixed stride and a seed-dependent offset, the starting centroids are a
 * seeded sample of that sample, and the iteration count is fixed: the same vectors and seed produce
 * the same centroids, which is what lets two runs of a test compare their partitions.
 *
 * <p>The centroid of a cluster is the arithmetic mean of its samples. That is the mean the
 * squared-distance partition is built around; a cosine index would additionally have to normalize
 * what it averages, which the prototype does not do.
 *
 * <p>Nearest-centroid lookups order by {@link VectorScoring#compareScores} and break ties by
 * centroid id, so a query that scores two centroids the same visits them in a fixed order rather
 * than in whatever order a sort happens to produce.
 */
final class IvfCentroids {

  /**
   * How many training samples each centroid gets, following the heuristic IVF uses for its lists.
   */
  private static final int SAMPLES_PER_CENTROID = 39;

  private static final int MAX_ITERATIONS = 25;

  private final float[][] centroids;
  private final VectorScoring scoring;

  private IvfCentroids(float[][] centroids, VectorScoring scoring) {
    this.centroids = centroids;
    this.scoring = scoring;
  }

  /**
   * Trains at most {@code requestedCentroids} centroids on a sample of {@code vectors}.
   *
   * <p>A request for more centroids than there are vectors is reduced to one centroid per vector:
   * asking for a partition of an empty cluster would otherwise have no starting point. An empty or
   * zero-centroid input trains nothing, which a router treats as "no routing information".
   */
  static IvfCentroids train(
      List<float[]> vectors, int requestedCentroids, long seed, VectorScoring scoring) {
    Preconditions.checkNotNull(vectors, "Training vectors are null");
    Preconditions.checkNotNull(scoring, "Scoring is null");
    Preconditions.checkArgument(
        requestedCentroids >= 0, "Invalid centroid count: %s", requestedCentroids);

    int count = Math.min(requestedCentroids, vectors.size());
    if (count == 0) {
      return new IvfCentroids(new float[0][], scoring);
    }

    int dimension = dimension(vectors);
    int sampleCount = Math.min(vectors.size(), Math.max(count, count * SAMPLES_PER_CENTROID));
    float[][] sample = sample(vectors, sampleCount);
    float[][] centroids = startingCentroids(sample, count, seed);
    for (int iteration = 0; iteration < MAX_ITERATIONS; iteration++) {
      int[] assignment = new int[sample.length];
      for (int i = 0; i < sample.length; i++) {
        assignment[i] = nearest(centroids, sample[i], scoring);
      }

      float[][] sums = new float[count][dimension];
      int[] sizes = new int[count];
      for (int i = 0; i < sample.length; i++) {
        int centroid = assignment[i];
        sizes[centroid] += 1;
        for (int d = 0; d < dimension; d++) {
          sums[centroid][d] += sample[i][d];
        }
      }

      boolean changed = false;
      for (int centroid = 0; centroid < count; centroid++) {
        if (sizes[centroid] == 0) {
          // a centroid nothing was assigned to keeps its position; the cluster it stands for is
          // empty, and a router skips it because no entry names it
          continue;
        }

        float[] mean = new float[dimension];
        for (int d = 0; d < dimension; d++) {
          mean[d] = sums[centroid][d] / sizes[centroid];
        }
        changed |= !Arrays.equals(mean, centroids[centroid]);
        centroids[centroid] = mean;
      }

      if (!changed) {
        break;
      }
    }

    return new IvfCentroids(centroids, scoring);
  }

  private static int dimension(List<float[]> vectors) {
    int dimension = -1;
    for (float[] vector : vectors) {
      Preconditions.checkNotNull(vector, "Training vector is null");
      if (dimension < 0) {
        dimension = vector.length;
      }

      Preconditions.checkArgument(
          vector.length == dimension,
          "Training vector has %s dimensions, the first has %s",
          vector.length,
          dimension);
    }

    Preconditions.checkArgument(dimension > 0, "Training vectors have no dimensions");
    return dimension;
  }

  /**
   * Takes {@code sampleCount} vectors spread over the whole input, endpoints included.
   *
   * <p>The i-th sample is rounded from {@code i * (size - 1) / (sampleCount - 1)}, so the first and
   * last vector are always in the sample and no tail is left out. A stride would drop the remainder
   * after the last multiple, which on a large snapshot is a whole tail of shards that never vote on
   * a centroid. The seed chooses the starting centroids, not the sample.
   */
  private static float[][] sample(List<float[]> vectors, int sampleCount) {
    if (sampleCount >= vectors.size()) {
      return vectors.toArray(new float[0][]);
    }

    if (sampleCount <= 1) {
      return new float[][] {vectors.get(0)};
    }

    float[][] sample = new float[sampleCount][];
    long span = (long) vectors.size() - 1L;
    for (int i = 0; i < sampleCount; i++) {
      sample[i] = vectors.get((int) Math.round((double) i * span / (sampleCount - 1)));
    }
    return sample;
  }

  /** Picks the starting centroids from the sample with a seeded partial shuffle. */
  private static float[][] startingCentroids(float[][] sample, int count, long seed) {
    int[] order = new int[sample.length];
    for (int i = 0; i < sample.length; i++) {
      order[i] = i;
    }

    SplittableRandom random = new SplittableRandom(seed);
    float[][] centroids = new float[count][];
    for (int i = 0; i < count; i++) {
      int pick = i + random.nextInt(sample.length - i);
      int swap = order[i];
      order[i] = order[pick];
      order[pick] = swap;
      centroids[i] = sample[order[i]];
    }
    return centroids;
  }

  /** The id of the centroid nearest to {@code vector}, ties going to the lower id. */
  int nearestCentroid(float[] vector) {
    Preconditions.checkArgument(
        vector.length == dimension(),
        "Vector has %s dimensions, the centroids have %s",
        vector.length,
        dimension());
    Preconditions.checkState(centroids.length > 0, "There are no centroids to look up");
    return nearest(centroids, vector, scoring);
  }

  /**
   * The ids of the {@code probes} nearest centroids, best first.
   *
   * <p>More probes than centroids is reduced to every centroid; zero or fewer is no centroid at
   * all, which a router treats as a query that touches nothing.
   */
  int[] nearestCentroids(float[] query, int probes) {
    Preconditions.checkArgument(probes >= 0, "Invalid probe count: %s", probes);
    Preconditions.checkArgument(
        query.length == dimension(),
        "Query has %s dimensions, the centroids have %s",
        query.length,
        dimension());

    int visited = Math.min(probes, centroids.length);
    if (visited == 0) {
      return new int[0];
    }

    double[] scores = new double[centroids.length];
    Integer[] order = new Integer[centroids.length];
    for (int centroid = 0; centroid < centroids.length; centroid++) {
      scores[centroid] = scoring.score(query, centroids[centroid]);
      order[centroid] = centroid;
    }

    Arrays.sort(
        order,
        (left, right) -> {
          int byScore = scoring.compareScores(scores[left], scores[right]);
          // equal scores are visited in id order, so a routed search is reproducible rather than
          // depending on the sort's treatment of equal elements
          return byScore != 0 ? byScore : Integer.compare(left, right);
        });

    int[] nearest = new int[visited];
    for (int i = 0; i < visited; i++) {
      nearest[i] = order[i];
    }
    return nearest;
  }

  int count() {
    return centroids.length;
  }

  int dimension() {
    return centroids.length == 0 ? 0 : centroids[0].length;
  }

  float[] centroid(int index) {
    return centroids[index];
  }

  float[][] centroids() {
    return centroids;
  }

  private static int nearest(float[][] centroids, float[] vector, VectorScoring scoring) {
    int nearest = 0;
    double nearestScore = scoring.score(vector, centroids[0]);
    for (int centroid = 1; centroid < centroids.length; centroid++) {
      double score = scoring.score(vector, centroids[centroid]);
      if (scoring.compareScores(score, nearestScore) < 0) {
        nearest = centroid;
        nearestScore = score;
      }
    }
    return nearest;
  }
}
