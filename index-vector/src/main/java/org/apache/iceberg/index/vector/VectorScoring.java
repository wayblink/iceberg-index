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

import java.io.Serializable;
import java.util.Comparator;
import org.apache.iceberg.index.IndexSearchResult;

/**
 * How similar two vectors are, and how results are ordered.
 *
 * <p>One implementation of this is shared by every kernel, because a kernel that measured something
 * else would order its own candidates differently from the exact scan it is compared against, and
 * the two answers merged from several index files would disagree for reasons that have nothing to
 * do with the vectors.
 *
 * <p>A score is a distance for {@link VectorIndexDefinition.Metric#EUCLIDEAN} and a similarity for
 * the others; {@link #comparator()} is what turns that into an order.
 */
public final class VectorScoring implements Serializable {

  private static final long serialVersionUID = 1L;

  private final VectorIndexDefinition.Metric metric;

  public VectorScoring(VectorIndexDefinition.Metric metric) {
    this.metric = metric;
  }

  public VectorIndexDefinition.Metric metric() {
    return metric;
  }

  /** The distance for EUCLIDEAN and the similarity for the others, between two vectors. */
  public double score(float[] query, float[] vector) {
    switch (metric) {
      case COSINE:
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
      case DOT_PRODUCT:
        double product = 0.0;
        for (int i = 0; i < query.length; i++) {
          product += (double) query[i] * vector[i];
        }
        return product;
      case EUCLIDEAN:
        double distanceSquared = 0.0;
        for (int i = 0; i < query.length; i++) {
          double difference = (double) query[i] - vector[i];
          distanceSquared += difference * difference;
        }
        return Math.sqrt(distanceSquared);
      default:
        throw new IllegalStateException("Unknown vector index metric: " + metric);
    }
  }

  /** Orders results best first, breaking ties by the row a hit points at. */
  public Comparator<IndexSearchResult> comparator() {
    Comparator<IndexSearchResult> tieBreaker =
        Comparator.comparing(IndexSearchResult::dataFile)
            .thenComparingLong(IndexSearchResult::rowPosition);
    switch (metric) {
      case EUCLIDEAN:
        return Comparator.comparingDouble(IndexSearchResult::score).thenComparing(tieBreaker);
      case COSINE:
      case DOT_PRODUCT:
        return Comparator.comparingDouble(IndexSearchResult::score)
            .reversed()
            .thenComparing(tieBreaker);
      default:
        throw new IllegalStateException("Unknown vector index metric: " + metric);
    }
  }
}
