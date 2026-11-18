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

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.Pair;

/**
 * One bit per dimension, plus three numbers per vector to make the estimate usable.
 *
 * <p>A vector is centred, rotated by an orthogonal transform, and reduced to the sign of each
 * coordinate: one bit per dimension. Two such codes have a dot product that estimates the angle
 * between the vectors, but only after correcting for the fact that a sign is not the coordinate it
 * stands for, and only if the vector's length is kept. That is what the three numbers are for.
 *
 * <p>Writing {@code o'} for the centred data vector and {@code q'} for the centred query, the inner
 * product is estimated as
 *
 * <pre>{@code
 * <o', q'> ~ |o'| * <x, P q'> / <x, u>
 * }</pre>
 *
 * where {@code x} is the stored code, {@code u} is the direction of the rotated data vector, and
 * {@code P} is the rotation. {@code |o'|}, {@code <x, u>} and {@code <o', c>} are what is stored;
 * everything else is computed from the query. The division by {@code <x, u>} is what makes this
 * better than a naive sign quantizer, whose estimate is biased low by a predictable factor.
 *
 * <p>The rotation is {@code P = H D}: a diagonal of random signs followed by a Walsh-Hadamard
 * transform, both orthogonal, so inner products survive it. It costs {@code O(d log d)} per vector
 * rather than the {@code O(d^2)} of a dense rotation, and the transform's length has to be a power
 * of two, so a vector is zero-padded to one. Padding adds coordinates that are zero on both sides
 * of an inner product, which leaves the estimate unchanged.
 *
 * <p>Everything is derived from a seed and the centre, both of which are written with the codes, so
 * a reader reproduces the transform instead of having to be told it.
 */
public final class RaBitQQuantizer implements java.io.Serializable {

  private static final long serialVersionUID = 1L;

  /** The number of numbers kept per vector besides the code. */
  static final int FACTORS = 3;

  /** The property the number of bits per dimension is read from. */
  public static final String BITS = "rabitq-bits";

  private static final int DEFAULT_BITS = 1;

  /**
   * The rotation every quantizer shares by default.
   *
   * <p>Fixed rather than random so that rebuilding a file reproduces its codes; a reader is told it
   * anyway, so the value itself carries no meaning beyond being the one that was used.
   */
  private static final long DEFAULT_SEED = 0x9E3779B97F4A7C15L;

  private static final int MAX_BITS = 8;
  private static final long RETAINED_FIELDS =
      3L * Integer.BYTES + Long.BYTES + 2L * PreparedStructureSize.REFERENCE_BYTES;

  /**
   * The signs in use, by seed and padded length.
   *
   * <p>Every list of one index file carries its own quantizer and they all share one seed, so the
   * same array serves all of them rather than each holding a copy: at a padded length of 4096 a
   * copy is 16 KiB, which a file with a few hundred lists pays a few hundred times over. The
   * quantizers of one file are serialized together, so a round trip rebuilds one shared array
   * rather than one per list, and the accounting counts it once for the same reason.
   */
  private static final Map<Pair<Long, Integer>, float[]> SIGNS = new ConcurrentHashMap<>();

  private final int dimension;
  private final int padded;
  private final long seed;
  private final int bits;
  private final float[] center;
  private final float[] signs;

  RaBitQQuantizer(int dimension, long seed, int bits, float[] center) {
    Preconditions.checkArgument(
        bits >= 1 && bits <= MAX_BITS, "Bits per dimension must be 1 to %s: %s", MAX_BITS, bits);
    this.dimension = dimension;
    this.padded = nextPowerOfTwo(dimension);
    this.seed = seed;
    this.bits = bits;
    this.center = center.clone();
    this.signs = signsFor(seed, padded);
  }

  private static float[] signsFor(long seed, int length) {
    return SIGNS.computeIfAbsent(Pair.of(seed, length), key -> signs(key.first(), key.second()));
  }

  /**
   * The parts of a query that one list's quantizer derives from it, computed once per (query,
   * list).
   *
   * <p>These are the terms of the estimate that depend on the query and on the list's centre but
   * not on which candidate is being scored: the rotated query, the centred query's squared length,
   * the centre's squared length, the query's own squared length, and the dot product between the
   * centre and the centred query. The per-candidate methods derive them on every call; a scoring
   * loop over a list derives them once and passes them to the prepared overloads instead.
   *
   * <p>Each is accumulated in the order and at the precision the per-candidate methods use, so the
   * two produce the same bits, not merely close values.
   */
  static final class ListQuery {
    private final float[] rotated;
    private final double queryLengthSquared;
    private final double querySquared;
    private final double centerSquared;
    private final double queryDotCenter;

    private ListQuery(
        float[] rotated,
        double queryLengthSquared,
        double querySquared,
        double centerSquared,
        double queryDotCenter) {
      this.rotated = rotated;
      this.queryLengthSquared = queryLengthSquared;
      this.querySquared = querySquared;
      this.centerSquared = centerSquared;
      this.queryDotCenter = queryDotCenter;
    }

    /** The squared length of the list's centre, for a caller that reports a cosine. */
    double centerSquared() {
      return centerSquared;
    }

    /** The squared length of the query, for a caller that reports a cosine. */
    double querySquared() {
      return querySquared;
    }
  }

  /** The number of bits per dimension this quantizer was made with. */
  public int bits() {
    return bits;
  }

  /** How many distinct levels a coordinate is quantized to. */
  private int levels() {
    return 1 << bits;
  }

  /** Reads the bit count from the index options. */
  public static int bitsFrom(java.util.Map<String, String> properties) {
    Preconditions.checkNotNull(properties, "Index properties are null");
    String value = properties.get(BITS);
    if (value == null) {
      return DEFAULT_BITS;
    }

    try {
      int bits = Integer.parseInt(value.trim());
      Preconditions.checkArgument(
          bits >= 1 && bits <= MAX_BITS,
          "Index option %s must be 1 to %s: %s",
          BITS,
          MAX_BITS,
          bits);
      return bits;
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Index option " + BITS + " is not a number: " + value, e);
    }
  }

  /**
   * A quantizer carrying only the rotation seed and the bit count.
   *
   * <p>For a caller that centres each of its partitions itself: the seed and the bits are what has
   * to agree between building and searching, while the centre is a property of the partition.
   */
  static RaBitQQuantizer blank(int dimension, int bits) {
    return new RaBitQQuantizer(dimension, DEFAULT_SEED, bits, new float[dimension]);
  }

  /** Trains the quantizer on the vectors a list holds: the centre is their mean. */
  static RaBitQQuantizer train(float[][] vectors, int dimension, long seed, int bits) {
    Preconditions.checkArgument(dimension > 0, "Dimension must be positive: %s", dimension);
    float[] center = new float[dimension];
    if (vectors.length > 0) {
      for (float[] vector : vectors) {
        for (int d = 0; d < dimension; d++) {
          center[d] += vector[d];
        }
      }
      for (int d = 0; d < dimension; d++) {
        center[d] /= vectors.length;
      }
    }

    return new RaBitQQuantizer(dimension, seed, bits, center);
  }

  int dimension() {
    return dimension;
  }

  int padded() {
    return padded;
  }

  long seed() {
    return seed;
  }

  float[] center() {
    return center.clone();
  }

  /** How many bytes a code takes. */
  int codeBytes() {
    try {
      return Math.toIntExact(IvfLayout.codeBytes(padded, bits));
    } catch (ArithmeticException e) {
      throw new IllegalArgumentException(
          "Code size of " + padded + " padded coordinates at " + bits + " bits is not addressable",
          e);
    }
  }

  /** The centre of the vectors, for a caller that has to report it. */
  float[] centerValues() {
    return center.clone();
  }

  /** Quantizes one vector, writing its code into {@code code} from {@code offset}. */
  void encode(float[] vector, byte[] code, int offset, float[] factors) {
    Preconditions.checkArgument(
        vector.length == dimension,
        "Vector has %s dimensions, the quantizer was trained for %s",
        vector.length,
        dimension);
    // the bits of the region this code occupies are cleared first: writing a code sets bits and
    // never clears them, so a caller that hands the same array back for the next vector would get
    // the bits of both
    java.util.Arrays.fill(code, offset, offset + codeBytes(), (byte) 0);

    float[] rotated = rotate(vector);
    // the transform spreads the energy over the padded array, so the length is summed there too:
    // summing over the dimension alone would miss whatever sits in the padding and scale the
    // length, and every estimate with it, by sqrt(dimension / padded)
    double norm = 0.0;
    for (int i = 0; i < padded; i++) {
      norm += (double) rotated[i] * rotated[i];
    }

    // the grid each coordinate is quantized on spans the vector's own largest coordinate, so the
    // levels are spent where the coordinates are rather than on a range they never reach; the scale
    // is not stored because the projection below absorbs it
    double largest = 0.0;
    for (int i = 0; i < padded; i++) {
      largest = Math.max(largest, Math.abs(rotated[i]));
    }

    int levels = levels();
    double scale = largest == 0.0 ? 0.0 : 1.0 / largest;
    double correction = 0.0;
    for (int i = 0; i < padded; i++) {
      double relative = padded == 0 || largest == 0.0 ? 0.0 : rotated[i] * scale;
      int level =
          (int)
              Math.round(
                  Math.max(0.0, Math.min(levels - 1, (relative + 1.0) * (levels - 1) / 2.0)));
      setLevel(code, offset, i, level);
      correction += (double) decodeLevel(level, levels) * (rotated[i] / Math.sqrt(norm));
    }

    factors[0] = (float) Math.sqrt(norm);
    // <x, u>, the projection of the code onto the direction it stands for, which is what makes the
    // estimate unbiased rather than systematically short
    factors[1] = norm == 0.0 ? 1.0f : (float) correction;
    double dotWithCenter = 0.0;
    for (int d = 0; d < dimension; d++) {
      dotWithCenter += (double) (vector[d] - center[d]) * center[d];
    }
    factors[2] = (float) dotWithCenter;
  }

  /**
   * Estimates the square of the distance between a vector and a query.
   *
   * @param code the stored code of the vector
   * @param factors the three numbers stored with it: length, correction, dot with the centre
   * @param rotatedQuery the query, centred and rotated by {@link #rotateQuery}
   * @param queryLengthSquared the squared length of the centred query
   */
  double estimatedDistanceSquared(
      byte[] code, int offset, float[] factors, float[] rotatedQuery, double queryLengthSquared) {
    return estimatedDistanceSquared(code, (long) offset, factors, rotatedQuery, queryLengthSquared);
  }

  /**
   * Estimates the square of the distance from a code stored at {@code codeBase} in {@code payload}.
   *
   * <p>The code is read where it lies rather than copied into an array of its own; the address of
   * each level's bits is computed with {@code long} arithmetic and checked against the code and the
   * payload, so a payload over 256 MiB does not overflow an {@code int} midway.
   */
  double estimatedDistanceSquared(
      byte[] payload,
      long codeBase,
      float[] factors,
      float[] rotatedQuery,
      double queryLengthSquared) {
    double norm = factors[0];
    double correction = factors[1];
    double projection = projection(payload, codeBase, rotatedQuery);

    double estimatedInner = norm == 0.0 || correction == 0.0 ? 0.0 : norm * projection / correction;
    double distanceSquared = norm * norm + queryLengthSquared - 2.0 * estimatedInner;
    // the estimate can come out slightly negative for a vector that is the query itself, and a
    // negative squared distance would be worse than a zero one
    return Math.max(0.0, distanceSquared);
  }

  /** Estimates the inner product between a vector and a query, for a metric that needs it. */
  double estimatedInnerProduct(
      byte[] code, int offset, float[] factors, float[] rotatedQuery, float[] query) {
    return estimatedInnerProduct(code, (long) offset, factors, rotatedQuery, query);
  }

  /** Estimates the inner product from a code stored at {@code codeBase} in {@code payload}. */
  double estimatedInnerProduct(
      byte[] payload, long codeBase, float[] factors, float[] rotatedQuery, float[] query) {
    double norm = factors[0];
    double correction = factors[1];
    double projection = projection(payload, codeBase, rotatedQuery);

    double centeredInner = norm == 0.0 || correction == 0.0 ? 0.0 : norm * projection / correction;
    // <o, q> = <c, c> + <c, q'> + <o', c> + <o', q'>, and the last term is the one estimated
    double queryDotCenter = 0.0;
    for (int d = 0; d < dimension; d++) {
      queryDotCenter += (double) center[d] * (query[d] - center[d]);
    }
    double centerSquared = 0.0;
    for (int d = 0; d < dimension; d++) {
      centerSquared += (double) center[d] * center[d];
    }
    return centerSquared + queryDotCenter + factors[2] + centeredInner;
  }

  /**
   * Estimates the square of the distance from a code, using the query's terms computed once.
   *
   * <p>This is the per-candidate loop of a search: the query's terms are in {@code prepared}, and
   * the only per-candidate arithmetic left is the code's projection and the three factors that
   * scale it.
   */
  double estimatedDistanceSquared(
      byte[] payload, long codeBase, float[] factors, ListQuery prepared) {
    double norm = factors[0];
    double correction = factors[1];
    double projection = projection(payload, codeBase, prepared.rotated);

    double estimatedInner = norm == 0.0 || correction == 0.0 ? 0.0 : norm * projection / correction;
    double distanceSquared = norm * norm + prepared.queryLengthSquared - 2.0 * estimatedInner;
    // the estimate can come out slightly negative for a vector that is the query itself, and a
    // negative squared distance would be worse than a zero one
    return Math.max(0.0, distanceSquared);
  }

  /** Estimates the inner product from a code, using the query's terms computed once. */
  double estimatedInnerProduct(byte[] payload, long codeBase, float[] factors, ListQuery prepared) {
    double norm = factors[0];
    double correction = factors[1];
    double projection = projection(payload, codeBase, prepared.rotated);

    double centeredInner = norm == 0.0 || correction == 0.0 ? 0.0 : norm * projection / correction;
    // <o, q> = <c, c> + <c, q'> + <o', c> + <o', q'>, and the last term is the one estimated
    return prepared.centerSquared + prepared.queryDotCenter + factors[2] + centeredInner;
  }

  /** The squared length of a vector, which the distance estimate needs to report a cosine. */
  float[] rotated(float[] vector) {
    return rotate(vector);
  }

  /**
   * Centres and rotates a query, the expensive part of which is shared by every vector it meets.
   */
  float[] rotateQuery(float[] query) {
    return rotate(query);
  }

  /**
   * Derives the query's terms of the estimate once, for every candidate of one list.
   *
   * <p>The per-candidate methods recompute these terms on every call; a scoring loop over a list
   * calls this once and then the prepared overloads; each term is accumulated in the order and at
   * the precision the per-candidate methods use, so the two produce the same bits.
   *
   * @param query the query vector, centred and rotated here
   */
  ListQuery prepareQuery(float[] query) {
    float[] rotatedQuery = rotate(query);
    double queryLengthSquared = 0.0;
    for (int d = 0; d < dimension; d++) {
      double difference = query[d] - center[d];
      queryLengthSquared += difference * difference;
    }

    double centerSquared = 0.0;
    double querySquared = 0.0;
    for (int d = 0; d < dimension; d++) {
      centerSquared += (double) center[d] * center[d];
      querySquared += (double) query[d] * query[d];
    }

    double queryDotCenter = 0.0;
    for (int d = 0; d < dimension; d++) {
      queryDotCenter += (double) center[d] * (query[d] - center[d]);
    }

    return new ListQuery(
        rotatedQuery, queryLengthSquared, querySquared, centerSquared, queryDotCenter);
  }

  /** The squared length of a vector, from the numbers stored with it. */
  static double squaredLength(float[] factors, double centerSquared) {
    double norm = factors[0];
    return norm * norm + 2.0 * factors[2] + centerSquared;
  }

  private float[] rotate(float[] vector) {
    Preconditions.checkArgument(
        vector.length == dimension,
        "Vector has %s dimensions, the quantizer was trained for %s",
        vector.length,
        dimension);
    float[] data = new float[padded];
    for (int i = 0; i < dimension; i++) {
      data[i] = (vector[i] - center[i]) * signs[i];
    }
    for (int i = dimension; i < padded; i++) {
      // a padded coordinate is zero, which the sign flip leaves alone
      data[i] = 0.0f;
    }

    hadamard(data);
    return data;
  }

  /** The Walsh-Hadamard transform, normalised so that it is its own inverse. */
  private static void hadamard(float[] data) {
    int size = data.length;
    for (int length = 1; length < size; length <<= 1) {
      for (int start = 0; start < size; start += length << 1) {
        for (int i = start; i < start + length; i++) {
          float left = data[i];
          float right = data[i + length];
          data[i] = left + right;
          data[i + length] = left - right;
        }
      }
    }

    float scale = (float) (1.0 / Math.sqrt(size));
    for (int i = 0; i < size; i++) {
      data[i] *= scale;
    }
  }

  private static float[] signs(long seed, int length) {
    Random random = new Random(seed);
    float[] signs = new float[length];
    for (int i = 0; i < length; i++) {
      signs[i] = random.nextBoolean() ? 1.0f : -1.0f;
    }
    return signs;
  }

  /** The value a level stands for: {@code levels} of them spread evenly over the range. */
  private static double decodeLevel(int level, int levels) {
    return levels == 1 ? 0.0 : -1.0 + 2.0 * level / (levels - 1);
  }

  private void setLevel(byte[] code, int offset, int index, int level) {
    long codeBytes = codeBytes();
    int mask = levels() - 1;
    long localBit = (long) index * bits;
    int shift = (int) (localBit & 7);
    int first = IvfLayout.codeByteAddress(offset, index, bits, 0, codeBytes, code.length);
    int word = code[first] & 0xFF;
    int second = -1;
    if (shift + bits > Byte.SIZE) {
      second = IvfLayout.codeByteAddress(offset, index, bits, bits - 1, codeBytes, code.length);
      word |= (code[second] & 0xFF) << 8;
    }

    word &= ~(mask << shift);
    word |= (level & mask) << shift;
    code[first] = (byte) word;
    if (second >= 0) {
      code[second] = (byte) (word >>> 8);
    }
  }

  /**
   * The inner product of a code with a rotated query, over the coordinates the code covers.
   *
   * <p>This is the inner loop of a search: one call per candidate, one iteration per coordinate.
   * The code's extent is therefore checked once instead of twice per coordinate, and a coordinate's
   * byte is reached with a running offset rather than through the generic address arithmetic.
   *
   * <p>Bits within a byte are least-significant first, which is the order {@link #setLevel} writes
   * them in. The padded coordinates are not zero after the transform — it spreads them over the
   * whole array — so they are read like any other; only their sum over the two vectors vanishes.
   */
  private double projection(byte[] payload, long codeBase, float[] rotatedQuery) {
    long codeBytes = codeBytes();
    long end = codeBase + codeBytes;
    Preconditions.checkArgument(
        codeBase >= 0 && end >= codeBase && end <= payload.length,
        "Code at payload byte %s with %s bytes is outside the %s-byte payload",
        codeBase,
        codeBytes,
        payload.length);

    int base = (int) codeBase;
    int mask = levels() - 1;
    int levels = levels();
    double sum = 0.0;
    for (int i = 0; i < padded; i++) {
      long localBit = (long) i * bits;
      int shift = (int) (localBit & 7);
      int byteIndex = base + (int) (localBit >>> 3);
      int word = payload[byteIndex] & 0xFF;
      if (shift + bits > Byte.SIZE) {
        word |= (payload[byteIndex + 1] & 0xFF) << 8;
      }

      sum += decodeLevel((word >>> shift) & mask, levels) * rotatedQuery[i];
    }

    return sum;
  }

  /** Prices the per-list quantizers and the one rotation array shared by all lists of a file. */
  static long estimatedListBytes(int dimension, int listCount) {
    if (listCount == 0) {
      return 0;
    }

    long perList =
        PreparedStructureSize.object(RETAINED_FIELDS) + PreparedStructureSize.floats(dimension);
    return Math.addExact(
        Math.multiplyExact(listCount, perList),
        PreparedStructureSize.floats(nextPowerOfTwo(dimension)));
  }

  /** Counts each retained object or array once within the prepared file. */
  long estimatedBytes(PreparedStructureSize.Receipt receipt) {
    return receipt.object(this, RETAINED_FIELDS) + receipt.array(center) + receipt.array(signs);
  }

  private static int nextPowerOfTwo(int value) {
    Preconditions.checkArgument(value > 0, "Dimension must be positive: %s", value);
    int padded = 1;
    while (padded < value) {
      Preconditions.checkArgument(
          padded <= (Integer.MAX_VALUE >>> 1), "Padded dimension overflows for %s", value);
      padded <<= 1;
    }
    return padded;
  }
}
