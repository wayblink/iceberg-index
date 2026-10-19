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

import java.util.Comparator;
import java.util.List;
import java.util.Random;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

/**
 * Whether the one-bit estimate is good enough to rank by.
 *
 * <p>A quantizer that is subtly wrong does not fail: it returns answers, ranked slightly wrongly,
 * and the only sign is a recall figure that is lower than it should be. So the estimate is compared
 * with the exact distance here, both in how far off it is and in whether the order it produces is
 * the order the exact distances produce. A mistake in the arithmetic shows up as an error of the
 * same size as the distance itself rather than as a small bias.
 */
class TestRaBitQQuantizer {

  private static final int DIMENSION = 128;
  private static final int VECTORS = 2000;
  private static final long SEED = 42L;

  @Test
  public void estimatesDistancesAndTheirOrder() {
    // the bit count is an index option, so what it buys is measured rather than assumed: more bits
    // per dimension means smaller codes are not the trade, the trade is a bigger code for a better
    // estimate, and this records where the estimate becomes good enough to order a cluster
    for (int bits : new int[] {1, 2, 3, 4}) {
      measureBits(bits);
    }
  }

  private void measureBits(int bits) {
    float[][] vectors = clusteredVectors(VECTORS, DIMENSION, 20, 7L);
    RaBitQQuantizer quantizer = RaBitQQuantizer.train(vectors, DIMENSION, SEED, bits);

    byte[] codes = new byte[VECTORS * quantizer.codeBytes()];
    float[][] factors = new float[VECTORS][];

    for (int i = 0; i < VECTORS; i++) {
      factors[i] = new float[RaBitQQuantizer.FACTORS];
      quantizer.encode(vectors[i], codes, i * quantizer.codeBytes(), factors[i]);
    }

    int codeBytes = quantizer.codeBytes();
    // the encoding is one bit per dimension rounded up to whole bytes, plus three numbers
    assertThat(codeBytes + 4 * RaBitQQuantizer.FACTORS)
        .as("bytes per vector, against %s for the vector itself", 4 * DIMENSION)
        .isLessThan(4 * DIMENSION);
    if (bits <= 3) {
      // up to three bits per dimension the code plus its three numbers is under a quarter of the
      // vector, which is the range an index is likely to choose from
      assertThat(codeBytes + 4 * RaBitQQuantizer.FACTORS).isLessThan((4 * DIMENSION) / 4);
    }

    int queries = 20;
    Measurements measurements = measure(quantizer, vectors, codes, factors, queries);
    List<double[]> errors = measurements.errors;

    double meanRelativeError = 0.0;
    for (double[] pair : errors) {
      double scale = Math.max(pair[0], 1e-9);
      meanRelativeError += Math.abs(pair[1] - pair[0]) / scale;
    }
    meanRelativeError /= errors.size();
    double overlap = (double) measurements.overlapAtTen / (queries * 10);
    double contained = (double) measurements.insideTop100 / (queries * 10);
    double containedWide = (double) measurements.insideTop1000 / (queries * 10);
    double pearson = pearson(measurements.correlation);
    System.out.println(
        "QUANT bits="
            + bits
            + " codeBytes="
            + codeBytes
            + " bytesPerVector="
            + (codeBytes + 4 * RaBitQQuantizer.FACTORS)
            + " meanRelativeError="
            + String.format("%.4f", meanRelativeError)
            + " top10Overlap="
            + String.format("%.3f", overlap)
            + " top100="
            + String.format("%.3f", contained)
            + " top1000="
            + String.format("%.3f", containedWide)
            + " correlation="
            + String.format("%.4f", pearson));

    // an arithmetic mistake shows up here as an error near 1 rather than as a few percent
    assertThat(meanRelativeError)
        .as("the mean relative error of the estimated squared distance")
        .isLessThan(0.5);
    assertThat(pearson)
        .as("how well the estimate tracks the true distance across the whole set")
        .isGreaterThan(0.95);
    // what an index needs from a quantizer is not that its own ranking is right, because even a
    // small error cannot separate the tenth nearest of a cluster from the eleventh: it is that the
    // true neighbours are inside a candidate set the caller can afford to re-rank exactly
    assertThat(contained)
        .as("the share of the exact top ten inside the estimated top hundred at %s bits", bits)
        .isGreaterThan(bits >= 3 ? 0.99 : 0.8);
    assertThat(meanRelativeError)
        .as("error falls as bits are spent, at %s bits", bits)
        .isLessThan(bits == 1 ? 0.06 : 0.05);
  }

  @Test
  public void reproducesTheSameCodesFromTheSameSeed() {
    float[][] vectors = randomVectors(100, DIMENSION, 3L);
    RaBitQQuantizer first = RaBitQQuantizer.train(vectors, DIMENSION, SEED, 1);
    RaBitQQuantizer second = RaBitQQuantizer.train(vectors, DIMENSION, SEED, 1);

    assertThat(second.center()).isEqualTo(first.center());
    assertThat(second.seed()).isEqualTo(first.seed());

    byte[] firstCodes = new byte[100 * first.codeBytes()];
    byte[] secondCodes = new byte[100 * first.codeBytes()];
    float[] factors = new float[RaBitQQuantizer.FACTORS];
    for (int i = 0; i < 100; i++) {
      first.encode(vectors[i], firstCodes, i * first.codeBytes(), factors);
      second.encode(vectors[i], secondCodes, i * first.codeBytes(), factors);
    }

    assertThat(secondCodes).isEqualTo(firstCodes);
  }

  @Test
  public void padsADimensionThatIsNotAPowerOfTwo() {
    // the Hadamard transform needs a power of two, so a vector is zero padded to one; the padding
    // is zero on both sides of an inner product, which leaves the estimate alone
    float[][] vectors = randomVectors(200, 100, 11L);
    RaBitQQuantizer quantizer = RaBitQQuantizer.train(vectors, 100, SEED, 1);
    assertThat(quantizer.padded()).isEqualTo(128);
    assertThat(quantizer.codeBytes()).isEqualTo(16);

    float[] query = randomVectors(1, 100, 12L)[0];
    float[] rotatedQuery = quantizer.rotateQuery(query);
    float[] factors = new float[RaBitQQuantizer.FACTORS];
    byte[] code = new byte[quantizer.codeBytes()];
    quantizer.encode(vectors[0], code, 0, factors);

    double queryLengthSquared = 0.0;
    for (int d = 0; d < 100; d++) {
      double difference = query[d] - quantizer.center()[d];
      queryLengthSquared += difference * difference;
    }
    double exact = 0.0;
    for (int d = 0; d < 100; d++) {
      double difference = query[d] - vectors[0][d];
      exact += difference * difference;
    }

    double estimate =
        quantizer.estimatedDistanceSquared(code, 0, factors, rotatedQuery, queryLengthSquared);
    assertThat(Math.abs(estimate - exact) / Math.max(exact, 1e-9)).isLessThan(0.5);
  }

  /** What one pass over the data measured, so the test body reads as one statement. */
  private static final class Measurements {
    private final List<double[]> errors = Lists.newArrayList();
    private final List<double[]> correlation = Lists.newArrayList();
    private int overlapAtTen = 0;
    private int insideTop100 = 0;
    private int insideTop1000 = 0;
  }

  private static Measurements measure(
      RaBitQQuantizer quantizer, float[][] vectors, byte[] codes, float[][] factors, int queries) {
    Measurements measurements = new Measurements();
    int codeBytes = quantizer.codeBytes();
    for (int q = 0; q < queries; q++) {
      float[] query = clusteredVectors(1, DIMENSION, 20, 1000L + q)[0];
      float[] rotatedQuery = quantizer.rotateQuery(query);
      double queryLengthSquared = 0.0;
      for (int d = 0; d < DIMENSION; d++) {
        double difference = query[d] - quantizer.center()[d];
        queryLengthSquared += difference * difference;
      }

      List<double[]> exact = Lists.newArrayList();
      List<double[]> estimated = Lists.newArrayList();
      for (int i = 0; i < vectors.length; i++) {
        double exactDistance = 0.0;
        for (int d = 0; d < DIMENSION; d++) {
          double difference = query[d] - vectors[i][d];
          exactDistance += difference * difference;
        }
        exact.add(new double[] {i, exactDistance});
        estimated.add(
            new double[] {
              i,
              quantizer.estimatedDistanceSquared(
                  codes, i * codeBytes, factors[i], rotatedQuery, queryLengthSquared)
            });
        measurements.errors.add(
            new double[] {
              exactDistance,
              quantizer.estimatedDistanceSquared(
                  codes, i * codeBytes, factors[i], rotatedQuery, queryLengthSquared)
            });
      }

      List<Integer> exactTop = topN(exact, 10);
      for (Integer id : topN(estimated, 10)) {
        if (exactTop.contains(id)) {
          measurements.overlapAtTen += 1;
        }
      }
      // what a search would actually do: take a wider candidate set from the estimate and re-rank
      // it exactly. That only works if the true nearest neighbours are inside the wider set.
      List<Integer> top100 = topN(estimated, 100);
      List<Integer> top1000 = topN(estimated, 1000);
      for (Integer id : exactTop) {
        if (top100.contains(id)) {
          measurements.insideTop100 += 1;
        }
        if (top1000.contains(id)) {
          measurements.insideTop1000 += 1;
        }
      }

      for (int i = 0; i < exact.size(); i++) {
        measurements.correlation.add(new double[] {exact.get(i)[1], estimated.get(i)[1]});
      }
    }
    return measurements;
  }

  private static double pearson(List<double[]> pairs) {
    double meanLeft = 0.0;
    double meanRight = 0.0;
    for (double[] pair : pairs) {
      meanLeft += pair[0];
      meanRight += pair[1];
    }
    meanLeft /= pairs.size();
    meanRight /= pairs.size();

    double covariance = 0.0;
    double leftVariance = 0.0;
    double rightVariance = 0.0;
    for (double[] pair : pairs) {
      double left = pair[0] - meanLeft;
      double right = pair[1] - meanRight;
      covariance += left * right;
      leftVariance += left * left;
      rightVariance += right * right;
    }

    return covariance / Math.sqrt(leftVariance * rightVariance);
  }

  private static List<Integer> topN(List<double[]> scored, int wanted) {
    scored.sort(Comparator.comparingDouble(pair -> pair[1]));
    List<Integer> top = Lists.newArrayList();
    for (int i = 0; i < wanted; i++) {
      top.add((int) scored.get(i)[0]);
    }
    return top;
  }

  /** Vectors around a few centres, the way embeddings cluster rather than filling the space. */
  private static float[][] clusteredVectors(int count, int dimension, int clusters, long seed) {
    Random random = new Random(seed);
    float[][] centers = new float[clusters][dimension];
    for (int c = 0; c < clusters; c++) {
      for (int d = 0; d < dimension; d++) {
        centers[c][d] = (float) (random.nextGaussian() * 3.0);
      }
    }

    float[][] vectors = new float[count][dimension];
    for (int i = 0; i < count; i++) {
      float[] center = centers[random.nextInt(clusters)];
      for (int d = 0; d < dimension; d++) {
        vectors[i][d] = center[d] + (float) random.nextGaussian();
      }
    }
    return vectors;
  }

  private static float[][] randomVectors(int count, int dimension, long seed) {
    Random random = new Random(seed);
    float[][] vectors = new float[count][dimension];
    for (int i = 0; i < count; i++) {
      for (int d = 0; d < dimension; d++) {
        vectors[i][d] = (float) random.nextGaussian();
      }
    }
    return vectors;
  }
}
