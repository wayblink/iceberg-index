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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import org.apache.iceberg.Schema;
import org.apache.iceberg.index.vector.VectorIndexAdapter;
import org.apache.iceberg.index.vector.VectorIndexDefinition;
import org.apache.iceberg.index.vector.VectorIndexDefinition.Metric;
import org.apache.iceberg.index.vector.VectorIndexDefinition.VectorQuantizer;
import org.apache.iceberg.index.vector.VectorIndexDefinition.VectorStructure;
import org.apache.iceberg.index.vector.VectorIndexTypes;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import scala.collection.JavaConverters;

/** The exact re-ranking of a search's fetched candidates, and when a search performs it. */
class TestVectorRefine {

  private static final Schema PROJECTION =
      new Schema(
          Types.NestedField.optional(
              1, "embedding", Types.ListType.ofOptional(2, Types.FloatType.get())),
          Types.NestedField.optional(3, "id", Types.IntegerType.get()));

  @BeforeAll
  static void registerVectorIndexType() {
    VectorIndexTypes.register();
  }

  @AfterEach
  void disable() {
    System.clearProperty(VectorRefine.ENABLED);
    System.clearProperty(VectorRefine.CANDIDATES);
  }

  @Test
  void ordersByTheExactDistanceAndThenByWhereTheRowIs() {
    double[] distances = new double[] {2.0, 1.0, 1.0, 3.0};
    String[] dataFiles = new String[] {"b.parquet", "b.parquet", "a.parquet", "a.parquet"};
    long[] positions = new long[] {5L, 5L, 9L, 1L};

    // two rows share a distance, and the tie is broken the way the index breaks one: by the data
    // file and then the row position, never by the order the task happened to fetch them in
    assertThat(VectorRefine.bestFirst(distances, dataFiles, positions, 3)).containsExactly(2, 1, 0);
  }

  @Test
  void keepsAtMostTheCandidatesItHas() {
    double[] distances = new double[] {1.0, 2.0};
    String[] dataFiles = new String[] {"a.parquet", "b.parquet"};
    long[] positions = new long[] {0L, 0L};

    assertThat(VectorRefine.bestFirst(distances, dataFiles, positions, 10)).containsExactly(0, 1);
    assertThat(VectorRefine.bestFirst(distances, dataFiles, positions, 0)).isEmpty();
  }

  @Test
  void isOffUnlessTheExperimentIsEnabled() {
    VectorIndexAdapter adapter = adapter(VectorStructure.IVF, VectorQuantizer.RABITQ);

    assertThat(VectorRefine.forSearch(adapter, PROJECTION, 2, 10)).isNull();

    System.setProperty(VectorRefine.ENABLED, "true");
    assertThat(VectorRefine.forSearch(adapter, PROJECTION, 2, 10))
        .as("a quantized IVF index orders by an estimate")
        .isNotNull();
  }

  @Test
  void doesNotRefineAnIndexThatScoresExactly() {
    System.setProperty(VectorRefine.ENABLED, "true");

    assertThat(
            VectorRefine.forSearch(
                adapter(VectorStructure.FLAT, VectorQuantizer.NONE), PROJECTION, 2, 10))
        .as("a flat index of stored vectors already scores what it returns")
        .isNull();
    assertThat(
            VectorRefine.forSearch(
                adapter(VectorStructure.IVF, VectorQuantizer.NONE), PROJECTION, 2, 10))
        .as("an IVF index of stored vectors already scores what it returns")
        .isNull();
  }

  @Test
  void doesNotRefineAProjectionThatDropsTheVectorColumn() {
    System.setProperty(VectorRefine.ENABLED, "true");
    VectorIndexAdapter adapter = adapter(VectorStructure.IVF, VectorQuantizer.RABITQ);

    // the exact distance is measured from the row the task read, so a projection without the
    // vector column has nothing to measure
    assertThat(VectorRefine.forSearch(adapter, PROJECTION, 0, 10)).isNull();

    Schema withoutVector = new Schema(Types.NestedField.optional(3, "id", Types.IntegerType.get()));
    assertThat(VectorRefine.forSearch(adapter, withoutVector, 1, 10)).isNull();
  }

  @Test
  void doesNotRefineASearchThatAsksForNothing() {
    System.setProperty(VectorRefine.ENABLED, "true");

    assertThat(
            VectorRefine.forSearch(
                adapter(VectorStructure.IVF, VectorQuantizer.RABITQ), PROJECTION, 2, 0))
        .isNull();
  }

  @Test
  void sizesTheWindowAsAMultipleOfKAndNeverBelowTheCallersWindow() {
    System.setProperty(VectorRefine.ENABLED, "true");
    System.setProperty(VectorRefine.CANDIDATES, "4");
    VectorRefine refine =
        VectorRefine.forSearch(
            adapter(VectorStructure.IVF, VectorQuantizer.RABITQ), PROJECTION, 2, 10);

    assertThat(refine.window(11, 10, 1_000L)).as("four candidates per result").isEqualTo(40);
    assertThat(refine.window(80, 10, 1_000L))
        .as("a caller's window wider than the refine window is kept")
        .isEqualTo(80);
    assertThat(refine.window(11, 10, 25L))
        .as("a window is never wider than the file")
        .isEqualTo(25);
  }

  @Test
  void tellsHowManyCandidatesPerResultItFetches() {
    System.setProperty(VectorRefine.ENABLED, "true");
    VectorRefine refine =
        VectorRefine.forSearch(
            adapter(VectorStructure.IVF, VectorQuantizer.RABITQ), PROJECTION, 2, 10);

    assertThat(refine.candidatesPerResult()).isEqualTo(8);
  }

  @Test
  void rejectsAnInvalidCandidateCount() {
    System.setProperty(VectorRefine.ENABLED, "true");
    System.setProperty(VectorRefine.CANDIDATES, "0");
    VectorIndexAdapter adapter = adapter(VectorStructure.IVF, VectorQuantizer.RABITQ);

    assertThatThrownBy(() -> VectorRefine.forSearch(adapter, PROJECTION, 2, 10))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(VectorRefine.CANDIDATES);

    System.setProperty(VectorRefine.CANDIDATES, "eight");
    assertThatThrownBy(() -> VectorRefine.forSearch(adapter, PROJECTION, 2, 10))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(VectorRefine.CANDIDATES);
  }

  @Test
  void measuresTheDistanceFromTheVectorTheRowCarries() {
    System.setProperty(VectorRefine.ENABLED, "true");
    VectorRefine refine =
        VectorRefine.forSearch(
            adapter(VectorStructure.IVF, VectorQuantizer.RABITQ), PROJECTION, 2, 10);
    Row row = RowFactory.create(list(1.0f, 2.0f), Integer.valueOf(7));

    assertThat(refine.distance(row, new float[] {1.0f, 2.0f})).isZero();
    assertThat(refine.distance(row, new float[] {4.0f, 6.0f})).isEqualTo(5.0);
    assertThat(
            refine.distance(RowFactory.create(null, Integer.valueOf(7)), new float[] {1.0f, 2.0f}))
        .as("a row without a vector is never a candidate")
        .isEqualTo(Double.POSITIVE_INFINITY);
  }

  private static Object list(Float... values) {
    return JavaConverters.asScalaBuffer(Arrays.asList(values)).toList();
  }

  private static VectorIndexAdapter adapter(VectorStructure structure, VectorQuantizer quantizer) {
    VectorIndexDefinition definition =
        VectorIndexDefinition.builder()
            .fieldId(1)
            .dimension(2)
            .metric(Metric.L2)
            .structure(structure)
            .quantizer(quantizer)
            .build();
    return new VectorIndexAdapter(definition);
  }
}
