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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerExecutorRemoved;

/**
 * Which executor holds which index file, as the driver knows it.
 *
 * <p>An executor reports a snapshot of what it holds at the end of each task: its id, its host, an
 * epoch that changes when the JVM does, a sequence number, and the identities of the files it can
 * serve. The driver keeps the newest snapshot per executor, drops executors Spark says are gone,
 * and forgets a snapshot that has not been renewed, so the table describes the cluster rather than
 * its history. Nothing here influences a query on its own: a table that is wrong only costs a task
 * that runs somewhere else and reads its file, which is what every task did before the table
 * existed.
 */
final class IndexCacheLocations {

  /** What an executor says it holds, sent back through an accumulator. */
  static final class Report implements Serializable {
    private final String executorId;
    private final String host;
    private final long epoch;
    private final long seq;
    private final long[] keys;
    private final long hits;
    private final long misses;
    private final long loads;
    private final long evictions;
    private final long bytes;
    private final long entries;

    Report(
        String executorId,
        String host,
        long epoch,
        long seq,
        long[] keys,
        long hits,
        long misses,
        long loads,
        long evictions,
        long bytes,
        long entries) {
      this.executorId = executorId;
      this.host = host;
      this.epoch = epoch;
      this.seq = seq;
      this.keys = keys;
      this.hits = hits;
      this.misses = misses;
      this.loads = loads;
      this.evictions = evictions;
      this.bytes = bytes;
      this.entries = entries;
    }

    String executorId() {
      return executorId;
    }

    String host() {
      return host;
    }

    long epoch() {
      return epoch;
    }

    long seq() {
      return seq;
    }

    long[] keys() {
      return keys;
    }

    long hits() {
      return hits;
    }

    long misses() {
      return misses;
    }

    long loads() {
      return loads;
    }

    long evictions() {
      return evictions;
    }

    long bytes() {
      return bytes;
    }

    long entries() {
      return entries;
    }

    @Override
    public String toString() {
      return executorId
          + "@"
          + host
          + " epoch="
          + epoch
          + " seq="
          + seq
          + " entries="
          + entries
          + " bytes="
          + bytes
          + " hits="
          + hits
          + " misses="
          + misses
          + " loads="
          + loads
          + " evictions="
          + evictions;
    }
  }

  /** Where one index file can be searched without reading it again. */
  static final class Holder {
    private final String host;
    private final String executorId;

    private Holder(String host, String executorId) {
      this.host = host;
      this.executorId = executorId;
    }

    String host() {
      return host;
    }

    String executorId() {
      return executorId;
    }
  }

  /** How much the table knows, for a log line or a report. */
  static final class Summary {
    private final int executors;
    private final int files;
    private final long bytes;
    private final double coverage;
    private final long hits;
    private final long misses;
    private final long evictions;

    private Summary(
        int executors,
        int files,
        long bytes,
        double coverage,
        long hits,
        long misses,
        long evictions) {
      this.executors = executors;
      this.files = files;
      this.bytes = bytes;
      this.coverage = coverage;
      this.hits = hits;
      this.misses = misses;
      this.evictions = evictions;
    }

    int executors() {
      return executors;
    }

    int files() {
      return files;
    }

    long bytes() {
      return bytes;
    }

    /** The share of the files a query asked for that at least one executor holds. */
    double coverage() {
      return coverage;
    }

    long hits() {
      return hits;
    }

    long misses() {
      return misses;
    }

    long evictions() {
      return evictions;
    }

    @Override
    public String toString() {
      return String.format(
          "executors=%d files=%d bytes=%d coverage=%.2f hits=%d misses=%d evictions=%d",
          executors, files, bytes, coverage, hits, misses, evictions);
    }
  }

  private static final class View {
    private final String executorId;
    private final String host;
    private final long epoch;
    private final long seq;
    private final long[] keys;
    private final long hits;
    private final long misses;
    private final long evictions;
    private long bytes;
    private long lastSeenMillis;

    private View(Report report) {
      this.executorId = report.executorId();
      this.host = report.host();
      this.epoch = report.epoch();
      this.seq = report.seq();
      this.keys = report.keys();
      this.hits = report.hits();
      this.misses = report.misses();
      this.evictions = report.evictions();
      this.bytes = report.bytes();
      this.lastSeenMillis = System.currentTimeMillis();
    }

    private boolean isNewerThan(View other) {
      return epoch != other.epoch ? epoch > other.epoch : seq > other.seq;
    }
  }

  private static final Map<String, IndexCacheLocations> BY_APPLICATION = new ConcurrentHashMap<>();
  private static final long DEFAULT_TTL_MILLIS = 10L * 60L * 1000L;

  private final Map<String, View> views = new LinkedHashMap<>();
  private final Object lock = new Object();

  static IndexCacheLocations forContext(SparkContext context) {
    String application = context.applicationId();
    return BY_APPLICATION.computeIfAbsent(
        application,
        id -> {
          IndexCacheLocations table = new IndexCacheLocations();
          context.addSparkListener(table.executorListener());
          return table;
        });
  }

  private SparkListener executorListener() {
    return new SparkListener() {
      @Override
      public void onExecutorRemoved(SparkListenerExecutorRemoved event) {
        synchronized (lock) {
          views.remove(event.executorId());
        }
      }
    };
  }

  /** The newest snapshot wins; a report from an older JVM replaces the view outright. */
  void merge(Report report) {
    if (report == null || report.executorId() == null) {
      return;
    }

    synchronized (lock) {
      View current = views.get(report.executorId());
      if (current == null || report.epoch() != current.epoch || report.seq() > current.seq) {
        views.put(report.executorId(), new View(report));
      }
    }
  }

  void mergeAll(Collection<Report> reports) {
    for (Report report : reports) {
      merge(report);
    }
  }

  /**
   * Forgets executors that have not reported for a while, so a dead one cannot keep a hint alive.
   */
  void prune(long ttlMillis) {
    long deadline = System.currentTimeMillis() - ttlMillis;
    synchronized (lock) {
      Iterator<Map.Entry<String, View>> entries = views.entrySet().iterator();
      while (entries.hasNext()) {
        if (entries.next().getValue().lastSeenMillis < deadline) {
          entries.remove();
        }
      }
    }
  }

  void prune() {
    prune(DEFAULT_TTL_MILLIS);
  }

  /**
   * The executors that can serve the given cache key, closest first, at most {@code limit}.
   *
   * <p>Two is the usual limit: a hint that names every executor that holds a popular file would let
   * the task go anywhere, and the point of the hint is to keep it away from the executors that
   * would have to read the file again.
   */
  List<Holder> holders(String cacheKey, int limit) {
    long key = hashKey(cacheKey);
    List<Holder> holders = new ArrayList<>();
    synchronized (lock) {
      for (View view : views.values()) {
        if (contains(view.keys, key)) {
          holders.add(new Holder(view.host, view.executorId));
          if (holders.size() >= limit) {
            break;
          }
        }
      }
    }

    return holders;
  }

  /** How many of the given keys at least one executor holds. */
  int coveredCount(Collection<String> cacheKeys) {
    int covered = 0;
    synchronized (lock) {
      for (String cacheKey : cacheKeys) {
        long key = hashKey(cacheKey);
        boolean found = false;
        for (View view : views.values()) {
          if (contains(view.keys, key)) {
            found = true;
            break;
          }
        }

        if (found) {
          covered += 1;
        }
      }
    }

    return covered;
  }

  Summary summary(Collection<String> cacheKeys) {
    int executors;
    int files = 0;
    long bytes = 0;
    long hits = 0;
    long misses = 0;
    long evictions = 0;
    synchronized (lock) {
      executors = views.size();
      for (View view : views.values()) {
        files += view.keys.length;
        bytes += view.bytes;
        hits += view.hits;
        misses += view.misses;
        evictions += view.evictions;
      }
    }

    double coverage =
        cacheKeys == null || cacheKeys.isEmpty()
            ? 0.0
            : (double) coveredCount(cacheKeys) / cacheKeys.size();
    return new Summary(executors, files, bytes, coverage, hits, misses, evictions);
  }

  int size() {
    synchronized (lock) {
      return views.size();
    }
  }

  void clear() {
    synchronized (lock) {
      views.clear();
    }
  }

  /** A stable 64-bit identity for a cache key, computed the same way on both sides. */
  static long hashKey(String value) {
    long hash = -3750763034362895579L; // FNV-1a offset basis
    for (byte b : value.getBytes(java.nio.charset.StandardCharsets.UTF_8)) {
      hash ^= (b & 0xff);
      hash *= 1099511628211L;
    }

    return hash;
  }

  private static boolean contains(long[] sorted, long key) {
    int low = 0;
    int high = sorted.length - 1;
    while (low <= high) {
      int middle = (low + high) >>> 1;
      long value = sorted[middle];
      if (value < key) {
        low = middle + 1;
      } else if (value > key) {
        high = middle - 1;
      } else {
        return true;
      }
    }

    return false;
  }

  static long[] sortedHashes(Collection<String> cacheKeys) {
    long[] hashes = new long[cacheKeys.size()];
    int index = 0;
    for (String key : cacheKeys) {
      hashes[index] = hashKey(key);
      index += 1;
    }

    Arrays.sort(hashes);
    return hashes;
  }

  static Comparator<Holder> byExecutor() {
    return Comparator.comparing(Holder::executorId);
  }
}
