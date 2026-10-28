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

import java.util.Random;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

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
    this.signs = signs(seed, padded);
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
    return (padded * bits + 7) / 8;
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
    double norm = 0.0;
    for (int i = 0; i < dimension; i++) {
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
    double norm = factors[0];
    double correction = factors[1];
    // <x, P q'>, over the coordinates the code covers; a zero padded coordinate contributes
    // nothing on either side
    int levels = levels();
    double projection = 0.0;
    for (int i = 0; i < padded; i++) {
      projection += decodeLevel(getLevel(code, offset, i), levels) * rotatedQuery[i];
    }

    double estimatedInner = norm == 0.0 || correction == 0.0 ? 0.0 : norm * projection / correction;
    double distanceSquared = norm * norm + queryLengthSquared - 2.0 * estimatedInner;
    // the estimate can come out slightly negative for a vector that is the query itself, and a
    // negative squared distance would be worse than a zero one
    return Math.max(0.0, distanceSquared);
  }

  /** Estimates the inner product between a vector and a query, for a metric that needs it. */
  double estimatedInnerProduct(
      byte[] code, int offset, float[] factors, float[] rotatedQuery, float[] query) {
    double norm = factors[0];
    double correction = factors[1];
    int levels = levels();
    double projection = 0.0;
    for (int i = 0; i < padded; i++) {
      projection += decodeLevel(getLevel(code, offset, i), levels) * rotatedQuery[i];
    }

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
    int bitOffset = offset * 8 + index * bits;
    for (int bit = 0; bit < bits; bit++) {
      if ((level & (1 << bit)) != 0) {
        int position = bitOffset + bit;
        code[position >> 3] |= (byte) (1 << (position & 7));
      }
    }
  }

  private int getLevel(byte[] code, int offset, int index) {
    int bitOffset = offset * 8 + index * bits;
    int level = 0;
    for (int bit = 0; bit < bits; bit++) {
      int position = bitOffset + bit;
      if ((code[position >> 3] & (1 << (position & 7))) != 0) {
        level |= 1 << bit;
      }
    }
    return level;
  }

  /** Estimates the retained center, padded signs and quantizer fields without copying arrays. */
  long estimatedBytes() {
    return estimatedBytes(new PreparedStructureSize.Receipt());
  }

  long estimatedBytes(PreparedStructureSize.Receipt receipt) {
    return receipt.object(
            this, 3L * Integer.BYTES + Long.BYTES + 2L * PreparedStructureSize.REFERENCE_BYTES)
        + receipt.array(center)
        + receipt.array(signs);
  }

  private static int nextPowerOfTwo(int value) {
    int padded = 1;
    while (padded < value) {
      padded <<= 1;
    }
    return padded;
  }
}
