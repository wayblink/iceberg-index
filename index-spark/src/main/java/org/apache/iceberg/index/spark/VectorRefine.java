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
package org.apache.iceberg.index.spark;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.index.IndexAdapter;
import org.apache.iceberg.index.vector.VectorDistance;
import org.apache.iceberg.index.vector.VectorIndexAdapter;
import org.apache.iceberg.index.vector.VectorIndexDefinition;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Row;

/**
 * The exact re-ranking of the candidates a search fetched back, for the index shapes that order by
 * an estimate.
 *
 * <p>A quantized IVF search orders the rows it finds by the estimate a code gives. The operator
 * above the search measures the exact distance of the rows it keeps, but a task keeps its own k by
 * the estimate before that operator sees them, so a true neighbour the estimate ranks below the
 * k-th row of its own file is never offered. This re-ranks the rows a task fetched back with the
 * same distance function the query asks for, so the window the task keeps is the exact best of the
 * candidates it already holds; no extra read is needed for it, because the rows and their vectors
 * are fetched anyway.
 *
 * <p>It is an internal experiment switch rather than a user setting: two system properties enable
 * it and size the candidate window, and the default is off, which keeps the search exactly as it is
 * without it. The window is a multiple of k and is never narrower than the window the caller
 * already asked for, so enabling it can only widen the candidate set a task considers.
 */
final class VectorRefine implements Serializable {

  private static final long serialVersionUID = 1L;

  /** Whether a search re-ranks its fetched candidates exactly. Internal, for measurement. */
  static final String ENABLED = "iceberg.index.vector.refine";

  /** How many candidates per k a task fetches back before re-ranking. Internal, for measurement. */
  static final String CANDIDATES = "iceberg.index.vector.refine.candidates";

  private static final int DEFAULT_CANDIDATES = 8;

  private final int vectorColumn;
  private final VectorIndexDefinition.Metric metric;
  private final int candidatesPerResult;

  private VectorRefine(
      int vectorColumn, VectorIndexDefinition.Metric metric, int candidatesPerResult) {
    this.vectorColumn = vectorColumn;
    this.metric = metric;
    this.candidatesPerResult = candidatesPerResult;
  }

  /**
   * The refinement a search of this projection performs, or null when it performs none.
   *
   * <p>Only an IVF index of quantized codes orders by an estimate: a plain structure stores the
   * vectors themselves, so its scores are already exact and re-ranking them would only cost time.
   * The vector column has to be one of the columns the task keeps, because the exact distance is
   * measured from the row the task read; a projection that drops it has nothing to re-rank with.
   *
   * @param adapter the index adapter the search runs
   * @param projection the columns the task reads, in the order its rows hold them
   * @param keep how many leading columns of a read row are part of the answer
   * @param resultK the number of rows the query finally keeps, not its candidate budget
   */
  static VectorRefine forSearch(IndexAdapter adapter, Schema projection, int keep, int resultK) {
    if (resultK <= 0 || !Boolean.parseBoolean(System.getProperty(ENABLED, "false"))) {
      return null;
    }

    if (!(adapter instanceof VectorIndexAdapter)) {
      return null;
    }

    VectorIndexDefinition definition = (VectorIndexDefinition) adapter.definition();
    if (definition.structure() != VectorIndexDefinition.VectorStructure.IVF
        || definition.quantizer() != VectorIndexDefinition.VectorQuantizer.RABITQ) {
      return null;
    }

    int column = indexOf(projection, definition.fieldId());
    if (column < 0 || column >= keep) {
      return null;
    }

    return new VectorRefine(
        column, definition.metric(), candidates(System.getProperty(CANDIDATES)));
  }

  /** How many candidates per result the window is sized by. */
  int candidatesPerResult() {
    return candidatesPerResult;
  }

  /**
   * How many candidates a task fetches before it keeps the best of them.
   *
   * <p>The window is at least the caller's opening, and at least {@code resultK} times the number
   * of candidates per result. It is capped by the file's entries, so a file with fewer entries than
   * the caller asked for is searched with fewer: the window is wider than the answer, not than the
   * file.
   */
  int window(int opening, int resultK, long entries) {
    long requested = Math.max((long) opening, (long) Math.max(1, resultK) * candidatesPerResult);
    return (int) Math.max(1L, Math.min(requested, Math.min(entries, Integer.MAX_VALUE)));
  }

  /**
   * The exact distance of a fetched row from the query, smaller meaning closer for every metric.
   */
  double distance(Row row, float[] query) {
    if (row.isNullAt(vectorColumn)) {
      return Double.POSITIVE_INFINITY;
    }

    List<Object> values = row.getList(vectorColumn);
    float[] vector = new float[values.size()];
    for (int i = 0; i < vector.length; i++) {
      vector[i] = ((Number) values.get(i)).floatValue();
    }

    return VectorDistance.distance(metric, vector, query);
  }

  /**
   * The indexes of the best {@code topK} candidates, best first.
   *
   * <p>The order is the one the query's own sort uses: the exact distance first, then the data file
   * and the row position, so two candidates the distance cannot separate are still ordered by where
   * they are rather than by the order the task happened to fetch them in.
   */
  static int[] bestFirst(double[] distances, String[] dataFiles, long[] positions, int topK) {
    Preconditions.checkArgument(
        distances.length == dataFiles.length && distances.length == positions.length,
        "Candidate arrays differ in length: %s distances, %s files, %s positions",
        distances.length,
        dataFiles.length,
        positions.length);

    Integer[] order = new Integer[distances.length];
    for (int i = 0; i < order.length; i++) {
      order[i] = i;
    }

    Arrays.sort(
        order,
        (left, right) -> {
          int byDistance = Double.compare(distances[left], distances[right]);
          if (byDistance != 0) {
            return byDistance;
          }

          int byFile = dataFiles[left].compareTo(dataFiles[right]);
          return byFile != 0 ? byFile : Long.compare(positions[left], positions[right]);
        });

    int[] best = new int[Math.min(Math.max(0, topK), order.length)];
    for (int i = 0; i < best.length; i++) {
      best[i] = order[i];
    }

    return best;
  }

  /** The position of a top-level column of the projection, or -1 when it is not one of them. */
  private static int indexOf(Schema projection, int fieldId) {
    List<Types.NestedField> columns = projection.columns();
    for (int i = 0; i < columns.size(); i++) {
      if (columns.get(i).fieldId() == fieldId) {
        return i;
      }
    }

    return -1;
  }

  /** The configured candidate multiple, which has to be a positive number of candidates per k. */
  private static int candidates(String configured) {
    if (configured == null || configured.trim().isEmpty()) {
      return DEFAULT_CANDIDATES;
    }

    String message =
        "Invalid " + CANDIDATES + ": '" + configured + "', expected a positive integer";
    int value;
    try {
      value = Integer.parseInt(configured.trim());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(message, e);
    }

    Preconditions.checkArgument(value > 0, message);
    return value;
  }
}
