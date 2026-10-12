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

import org.apache.iceberg.index.vector.VectorIndexDefinition.Metric;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * The metric math of vector indexes: one implementation, used by the index adapter when it scores
 * entries and by any engine that has to score a query the same way.
 *
 * <p>Two views of the same math are exposed because they are needed for different purposes. {@link
 * #score} is what the index stores and orders by: for {@link Metric#COSINE} and {@link
 * Metric#DOT_PRODUCT} a higher score is closer, for {@link Metric#EUCLIDEAN} a lower one is. {@link
 * #distance} is what a query asks for: smaller always means closer, whatever the metric, so that
 * {@code ORDER BY vec_distance(...)} can be ascending for every metric.
 */
public final class VectorDistance {

  private VectorDistance() {}

  /**
   * Scores a vector against a query the way the index does.
   *
   * @param metric the metric of the index
   * @param vector the stored vector
   * @param query the query vector
   * @return the index score: higher is closer for cosine and dot product, lower for euclidean
   */
  public static double score(Metric metric, float[] vector, float[] query) {
    Preconditions.checkArgument(
        vector.length == query.length,
        "Vector length %s does not match query length %s",
        vector.length,
        query.length);
    switch (metric) {
      case COSINE:
        return cosineSimilarity(vector, query);
      case DOT_PRODUCT:
        return dotProduct(vector, query);
      case EUCLIDEAN:
        return euclideanDistance(vector, query);
      default:
        throw new IllegalArgumentException("Unknown vector index metric: " + metric);
    }
  }

  /**
   * Returns a distance for which smaller always means closer.
   *
   * @param metric the metric of the index
   * @param vector the stored vector
   * @param query the query vector
   * @return the distance; smaller is closer
   */
  public static double distance(Metric metric, float[] vector, float[] query) {
    Preconditions.checkArgument(
        vector.length == query.length,
        "Vector length %s does not match query length %s",
        vector.length,
        query.length);
    switch (metric) {
      case COSINE:
        return 1.0 - cosineSimilarity(vector, query);
      case DOT_PRODUCT:
        return -dotProduct(vector, query);
      case EUCLIDEAN:
        return euclideanDistance(vector, query);
      default:
        throw new IllegalArgumentException("Unknown vector index metric: " + metric);
    }
  }

  private static double cosineSimilarity(float[] vector, float[] query) {
    double dot = 0.0;
    double queryNorm = 0.0;
    double vectorNorm = 0.0;
    for (int i = 0; i < query.length; i++) {
      dot += (double) query[i] * vector[i];
      queryNorm += (double) query[i] * query[i];
      vectorNorm += (double) vector[i] * vector[i];
    }
    return queryNorm == 0.0 || vectorNorm == 0.0
        ? 0.0
        : dot / (Math.sqrt(queryNorm) * Math.sqrt(vectorNorm));
  }

  private static double dotProduct(float[] vector, float[] query) {
    double product = 0.0;
    for (int i = 0; i < query.length; i++) {
      product += (double) query[i] * vector[i];
    }
    return product;
  }

  private static double euclideanDistance(float[] vector, float[] query) {
    double distanceSquared = 0.0;
    for (int i = 0; i < query.length; i++) {
      double difference = (double) query[i] - vector[i];
      distanceSquared += difference * difference;
    }
    return Math.sqrt(distanceSquared);
  }
}
