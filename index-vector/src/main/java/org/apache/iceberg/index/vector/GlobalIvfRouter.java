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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * Routes a query to the IVF shards that hold the coarse clusters worth probing.
 *
 * <p>Each shard is an index file: it holds entries, and each entry belongs to one coarse cluster of
 * the shared centroid space. The router is trained from all the shards of one snapshot, so the
 * clusters are global to the table rather than local to a file. A query then chooses {@code nprobe}
 * clusters and is answered from the shards that cover those clusters, instead of probing every
 * index file independently.
 *
 * <p>The coverage is the whole routing decision: a shard is touched when at least one probed
 * cluster has an entry in it, and the candidates are exactly the entries whose cluster was probed.
 * A shard that holds no entry of any probed cluster is not touched at all, so the fan-out falls
 * only when clusters are concentrated in a few shards; the candidate count falls by the probed
 * fraction of the centroid space whatever the shard layout. Both are measured separately.
 *
 * <p>The prototype trains on the vectors themselves and keeps the shards in memory. A production
 * router reads the centroids and the cluster-to-shard mapping from an auxiliary metadata blob and
 * reads only the selected list ranges from each touched shard; the entries then carry the data file
 * and row position the index already records, and fetch-back resolves them as it does today.
 */
final class GlobalIvfRouter {

  private final IvfCentroids centroids;
  private final List<IvfShard> shards;

  /** The cluster of every entry, indexed by shard and then by entry. */
  private final int[][] clusterOfEntry;

  /**
   * How many entries of a shard belong to each cluster, indexed by shard and then by cluster id.
   */
  private final long[][] entriesPerCluster;

  private final long entries;

  private GlobalIvfRouter(
      IvfCentroids centroids,
      List<IvfShard> shards,
      int[][] clusterOfEntry,
      long[][] entriesPerCluster,
      long entries) {
    this.centroids = centroids;
    this.shards = shards;
    this.clusterOfEntry = clusterOfEntry;
    this.entriesPerCluster = entriesPerCluster;
    this.entries = entries;
  }

  /**
   * Trains shared coarse centroids on a sample of the shards' vectors and assigns every entry to
   * its nearest one.
   *
   * <p>Training input is the entries of all shards in shard order, which is the single snapshot the
   * prototype routes within. The sampling, starting centroids and iteration count are those of
   * {@link IvfCentroids#train}: the same shards and seed train the same coverage.
   *
   * @param shards the index shards of one snapshot
   * @param clusterCount the number of coarse clusters to train; zero leaves routing unavailable
   * @param seed the seed of the sample offset and of the starting-centroid choice
   * @param scoring the distance function the query uses
   */
  static GlobalIvfRouter train(
      List<IvfShard> shards, int clusterCount, long seed, VectorScoring scoring) {
    Preconditions.checkNotNull(shards, "Shards are null");
    Preconditions.checkNotNull(scoring, "Scoring is null");
    Preconditions.checkArgument(clusterCount >= 0, "Invalid cluster count: %s", clusterCount);

    List<float[]> vectors = Lists.newArrayList();
    for (IvfShard shard : shards) {
      vectors.addAll(Arrays.asList(shard.vectors()));
    }

    return of(IvfCentroids.train(vectors, clusterCount, seed, scoring), shards);
  }

  /**
   * Builds the coverage of a set of shards around already-trained centroids.
   *
   * <p>This is the form a query uses: the centroids and their version are published in the index
   * snapshot, and the router only has to map each shard's entries onto them. It is also what makes
   * the routing decision independent of training, so a test can compare the two halves.
   *
   * @param centroids the shared coarse centroids of the snapshot
   * @param shards the index shards of the same snapshot
   */
  static GlobalIvfRouter of(IvfCentroids centroids, List<IvfShard> shards) {
    Preconditions.checkNotNull(centroids, "Centroids are null");
    Preconditions.checkNotNull(shards, "Shards are null");

    long entries = 0L;
    for (IvfShard shard : shards) {
      entries += shard.entryCount();
    }

    int count = centroids.count();
    int[][] clusterOfEntry = new int[shards.size()][];
    long[][] entriesPerCluster = new long[shards.size()][count];
    if (count > 0) {
      for (int shard = 0; shard < shards.size(); shard++) {
        IvfShard current = shards.get(shard);
        int[] assignment = new int[current.entryCount()];
        for (int entry = 0; entry < current.entryCount(); entry++) {
          int cluster = centroids.nearestCentroid(current.vector(entry));
          assignment[entry] = cluster;
          entriesPerCluster[shard][cluster] += 1L;
        }
        clusterOfEntry[shard] = assignment;
      }
    }

    return new GlobalIvfRouter(
        centroids, ImmutableList.copyOf(shards), clusterOfEntry, entriesPerCluster, entries);
  }

  /**
   * The shards and entries a query probes when it visits the {@code nprobe} nearest clusters.
   *
   * <p>Zero or fewer probes, an empty snapshot, and a router trained with no centroids all answer
   * with the empty decision: nothing is touched, so a caller gets no candidate rather than an
   * error. That is the boundary a query with probing turned off has to handle.
   */
  RoutingDecision route(float[] query, int nprobe) {
    Preconditions.checkNotNull(query, "Query is null");
    if (nprobe <= 0 || entries == 0 || centroids.count() == 0) {
      return RoutingDecision.empty();
    }

    Preconditions.checkArgument(
        query.length == centroids.dimension(),
        "Query has %s dimensions, the centroids have %s",
        query.length,
        centroids.dimension());

    int[] clusterIds = centroids.nearestCentroids(query, nprobe);
    boolean[] probed = new boolean[centroids.count()];
    for (int cluster : clusterIds) {
      probed[cluster] = true;
    }

    List<IvfShard> touched = Lists.newArrayList();
    List<Candidate> candidates = Lists.newArrayList();
    long candidateEntries = 0L;
    for (int shard = 0; shard < shards.size(); shard++) {
      long inShard = 0L;
      for (int cluster : clusterIds) {
        inShard += entriesPerCluster[shard][cluster];
      }
      if (inShard == 0L) {
        // no entry of a probed cluster lives here, so the file is not read at all
        continue;
      }

      touched.add(shards.get(shard));
      candidateEntries = Math.addExact(candidateEntries, inShard);
      int[] assignment = clusterOfEntry[shard];
      for (int entry = 0; entry < assignment.length; entry++) {
        if (probed[assignment[entry]]) {
          candidates.add(new Candidate(shards.get(shard), entry));
        }
      }
    }

    return new RoutingDecision(clusterIds, touched, candidateEntries, candidates);
  }

  IvfCentroids centroids() {
    return centroids;
  }

  List<IvfShard> shards() {
    return shards;
  }

  /** The total number of entries the router covers. */
  long entries() {
    return entries;
  }

  int clusterCount() {
    return centroids.count();
  }

  /** One entry of one shard, which is a candidate a routed search may score. */
  static final class Candidate {
    private final IvfShard shard;
    private final int entry;

    private Candidate(IvfShard shard, int entry) {
      this.shard = shard;
      this.entry = entry;
    }

    IvfShard shard() {
      return shard;
    }

    int entry() {
      return entry;
    }

    float[] vector() {
      return shard.vector(entry);
    }

    long rowPosition() {
      return shard.rowPosition(entry);
    }
  }

  /** The shards and candidate entries of one query, in probe order then shard and entry order. */
  static final class RoutingDecision {

    private static final RoutingDecision EMPTY =
        new RoutingDecision(new int[0], ImmutableList.of(), 0L, ImmutableList.of());

    private final int[] clusterIds;
    private final List<IvfShard> touchedShards;
    private final long candidateEntries;
    private final List<Candidate> candidates;

    private RoutingDecision(
        int[] clusterIds,
        List<IvfShard> touchedShards,
        long candidateEntries,
        List<Candidate> candidates) {
      this.clusterIds = clusterIds;
      this.touchedShards = touchedShards;
      this.candidateEntries = candidateEntries;
      this.candidates = candidates;
    }

    static RoutingDecision empty() {
      return EMPTY;
    }

    /** The probed cluster ids, nearest first. */
    int[] clusterIds() {
      return clusterIds;
    }

    /** The shards a search has to read, in shard order; empty when nothing was probed. */
    List<IvfShard> touchedShards() {
      return touchedShards;
    }

    /** How many entries a routed search scores, which is what the read bytes scale with. */
    long candidateEntries() {
      return candidateEntries;
    }

    /**
     * The candidate entries, in shard and entry order.
     *
     * <p>The prototype materializes them so a test can score each one; a production plan keeps the
     * coverage and streams the list ranges instead, because the candidate list of a real query is
     * what the payload is fetched for rather than a structure held on the driver.
     */
    List<Candidate> candidates() {
      return candidates;
    }

    boolean isEmpty() {
      return touchedShards.isEmpty();
    }
  }
}
