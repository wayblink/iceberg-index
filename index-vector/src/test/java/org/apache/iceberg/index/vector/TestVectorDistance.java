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

import org.apache.iceberg.index.vector.VectorIndexDefinition.Metric;
import org.junit.jupiter.api.Test;

class TestVectorDistance {

  @Test
  public void scoreMatchesDistanceForEveryMetric() {
    float[] vector = {1.0f, 0.0f};
    float[] query = {0.0f, 1.0f};

    // orthogonal vectors: cosine similarity is zero, so the distance is one
    assertThat(VectorDistance.score(Metric.COSINE, vector, query)).isEqualTo(0.0);
    assertThat(VectorDistance.distance(Metric.COSINE, vector, query)).isEqualTo(1.0);

    // dot product of orthogonal vectors is zero, so the distance is its negation
    assertThat(VectorDistance.score(Metric.DOT_PRODUCT, vector, query)).isEqualTo(0.0);
    assertThat(VectorDistance.distance(Metric.DOT_PRODUCT, vector, query)).isEqualTo(0.0);

    // euclidean keeps the distance as it is
    assertThat(VectorDistance.score(Metric.EUCLIDEAN, vector, query)).isEqualTo(Math.sqrt(2.0));
    assertThat(VectorDistance.distance(Metric.EUCLIDEAN, vector, query)).isEqualTo(Math.sqrt(2.0));
  }

  @Test
  public void distanceIsSmallerForCloserVectors() {
    float[] query = {1.0f, 0.0f};
    float[] near = {0.9f, 0.1f};
    float[] far = {0.0f, 1.0f};

    for (Metric metric : Metric.values()) {
      assertThat(VectorDistance.distance(metric, near, query))
          .isLessThan(VectorDistance.distance(metric, far, query));
      // and the index score agrees on which one is closer, in its own direction
      boolean closerByScore =
          metric == Metric.EUCLIDEAN
              ? VectorDistance.score(metric, near, query) < VectorDistance.score(metric, far, query)
              : VectorDistance.score(metric, near, query)
                  > VectorDistance.score(metric, far, query);
      assertThat(closerByScore).isTrue();
    }
  }

  @Test
  public void aVectorIsClosestToItself() {
    float[] vector = {0.5f, 0.25f, 0.125f};
    float[] other = {0.1f, 0.2f, 0.3f};

    // cosine and euclidean are zero for identical vectors; dot product is negated, so identical
    // vectors give the smallest (most negative) distance rather than zero
    assertThat(VectorDistance.distance(Metric.COSINE, vector, vector)).isEqualTo(0.0);
    assertThat(VectorDistance.distance(Metric.EUCLIDEAN, vector, vector)).isEqualTo(0.0);
    assertThat(VectorDistance.distance(Metric.DOT_PRODUCT, vector, vector))
        .isLessThan(VectorDistance.distance(Metric.DOT_PRODUCT, vector, other));

    for (Metric metric : Metric.values()) {
      assertThat(VectorDistance.distance(metric, vector, vector))
          .isLessThanOrEqualTo(VectorDistance.distance(metric, vector, other));
    }
  }
}
