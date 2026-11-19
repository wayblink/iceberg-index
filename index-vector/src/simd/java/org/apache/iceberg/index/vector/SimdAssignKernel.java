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

import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

/**
 * The nearest-centroid assignment of an IVF build, computed with the vector API.
 *
 * <p>One lane is one dimension: the kernel walks a centroid row and a block of vector rows four
 * lanes at a time, keeping one accumulator per row and reducing it to the row's squared distance at
 * the end of the centroid. The block shares the centroid loads and gives the compiler four
 * independent accumulation chains, which is what keeps the multiply-add units busy; the rows and
 * the accumulators are local variables rather than arrays of vectors, because the vector API only
 * turns the loop into machine instructions when every vector stays in a register.
 *
 * <p>The sums are single-precision and the distances are compared without taking a square root:
 * squared distances are ordered the way their roots are, and the comparison is the one the exact
 * kernel makes. A row keeps an earlier list when two distances are equal, and every row decides its
 * list on its own, so the assignment does not depend on which thread walks the row.
 */
final class SimdAssignKernel implements AssignKernel {
  private static final VectorSpecies<Float> SPECIES = FloatVector.SPECIES_PREFERRED;

  /** How many rows walk one centroid before the kernel moves to the next centroid. */
  private static final int VECTOR_BLOCK = 4;

  @Override
  public void assignRange(
      float[][] vectors, float[][] centroids, int dimension, int[] range, int[] assignment) {
    if (centroids.length == 0) {
      // a structure with no centroid has no list to choose: the exact kernel returns list zero
      // for every row as well, which is what a caller that already dropped the empty lists expects
      for (int i = range[0]; i < range[1]; i++) {
        assignment[i] = 0;
      }

      return;
    }

    int lanes = SPECIES.length();
    int begin = range[0];
    for (; begin + VECTOR_BLOCK <= range[1]; begin += VECTOR_BLOCK) {
      assignBlock(
          vectors[begin],
          vectors[begin + 1],
          vectors[begin + 2],
          vectors[begin + 3],
          centroids,
          dimension,
          lanes,
          assignment,
          begin);
    }

    // the rows a block does not cover are at most VECTOR_BLOCK - 1: they take the one-row kernel
    for (; begin < range[1]; begin++) {
      assignment[begin] = assignRow(vectors[begin], centroids, dimension, lanes);
    }
  }

  /** Assigns four rows against every centroid, keeping one accumulator per row. */
  private static void assignBlock(
      float[] row0,
      float[] row1,
      float[] row2,
      float[] row3,
      float[][] centroids,
      int dimension,
      int lanes,
      int[] assignment,
      int base) {
    float best0 = Float.POSITIVE_INFINITY;
    float best1 = Float.POSITIVE_INFINITY;
    float best2 = Float.POSITIVE_INFINITY;
    float best3 = Float.POSITIVE_INFINITY;
    int nearest0 = 0;
    int nearest1 = 0;
    int nearest2 = 0;
    int nearest3 = 0;

    for (int list = 0; list < centroids.length; list++) {
      float[] centroid = centroids[list];
      FloatVector accumulator0 = FloatVector.zero(SPECIES);
      FloatVector accumulator1 = FloatVector.zero(SPECIES);
      FloatVector accumulator2 = FloatVector.zero(SPECIES);
      FloatVector accumulator3 = FloatVector.zero(SPECIES);

      int d = 0;
      for (; d + lanes <= dimension; d += lanes) {
        FloatVector centre = FloatVector.fromArray(SPECIES, centroid, d);

        FloatVector value0 = FloatVector.fromArray(SPECIES, row0, d);
        FloatVector difference0 = value0.sub(centre);
        accumulator0 = difference0.fma(difference0, accumulator0);

        FloatVector value1 = FloatVector.fromArray(SPECIES, row1, d);
        FloatVector difference1 = value1.sub(centre);
        accumulator1 = difference1.fma(difference1, accumulator1);

        FloatVector value2 = FloatVector.fromArray(SPECIES, row2, d);
        FloatVector difference2 = value2.sub(centre);
        accumulator2 = difference2.fma(difference2, accumulator2);

        FloatVector value3 = FloatVector.fromArray(SPECIES, row3, d);
        FloatVector difference3 = value3.sub(centre);
        accumulator3 = difference3.fma(difference3, accumulator3);
      }

      float distance0 = accumulator0.reduceLanes(VectorOperators.ADD);
      float distance1 = accumulator1.reduceLanes(VectorOperators.ADD);
      float distance2 = accumulator2.reduceLanes(VectorOperators.ADD);
      float distance3 = accumulator3.reduceLanes(VectorOperators.ADD);

      // a dimension that the lanes do not divide is summed one value at a time, as the vector
      // lanes already were: the last dimensions of the row are a suffix of the same sum
      for (; d < dimension; d++) {
        float centre = centroid[d];
        float difference0 = row0[d] - centre;
        float difference1 = row1[d] - centre;
        float difference2 = row2[d] - centre;
        float difference3 = row3[d] - centre;
        distance0 += difference0 * difference0;
        distance1 += difference1 * difference1;
        distance2 += difference2 * difference2;
        distance3 += difference3 * difference3;
      }

      // the lists are compared in increasing order with a strict comparison, so an equal distance
      // keeps the earlier list, exactly as the scalar kernel does
      if (distance0 < best0) {
        best0 = distance0;
        nearest0 = list;
      }

      if (distance1 < best1) {
        best1 = distance1;
        nearest1 = list;
      }

      if (distance2 < best2) {
        best2 = distance2;
        nearest2 = list;
      }

      if (distance3 < best3) {
        best3 = distance3;
        nearest3 = list;
      }
    }

    assignment[base] = nearest0;
    assignment[base + 1] = nearest1;
    assignment[base + 2] = nearest2;
    assignment[base + 3] = nearest3;
  }

  /** Assigns one row, which the last block of a range does for the rows it cannot pair up. */
  private static int assignRow(float[] row, float[][] centroids, int dimension, int lanes) {
    float best = Float.POSITIVE_INFINITY;
    int nearest = 0;

    for (int list = 0; list < centroids.length; list++) {
      float[] centroid = centroids[list];
      FloatVector accumulator = FloatVector.zero(SPECIES);

      int d = 0;
      for (; d + lanes <= dimension; d += lanes) {
        FloatVector value = FloatVector.fromArray(SPECIES, row, d);
        FloatVector centre = FloatVector.fromArray(SPECIES, centroid, d);
        FloatVector difference = value.sub(centre);
        accumulator = difference.fma(difference, accumulator);
      }

      float distance = accumulator.reduceLanes(VectorOperators.ADD);
      for (; d < dimension; d++) {
        float difference = row[d] - centroid[d];
        distance += difference * difference;
      }

      if (distance < best) {
        best = distance;
        nearest = list;
      }
    }

    return nearest;
  }
}
