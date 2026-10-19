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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.iceberg.index.IndexAdapter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * What each executor remembers between queries: the payload of an index file and the search
 * structure prepared from it.
 *
 * <p>Reading an index file and preparing its payload is the work a repeated query would otherwise
 * do again on every one of its files, and it is most of what a search over an index costs. An
 * executor runs several queries, so the result is kept here, in a map that belongs to the JVM and
 * outlives the tasks. The cost is that an executor is no longer stateless: the map has a byte
 * budget, evicts least recently used entries, and is only filled when the caller turns it on.
 *
 * <p>Index files are immutable — a new snapshot refers to new files rather than to a rewritten
 * version of an old one — so an entry is keyed by the file's location and never invalidated. A file
 * that is replaced simply stops being asked for and ages out. The one thing that changes the
 * prepared structure without changing the payload is the adapter reading it, so its fingerprint is
 * part of the key.
 *
 * <p>The prepared structure is not required to be thread safe: a kernel's contract is that it is
 * searched from one thread at a time. An executor has as many task threads as cores, so a single
 * entry can be asked for by two threads at once; {@link CachedFile#searching} holds a lock for the
 * duration of a search rather than duplicating the structure per thread.
 */
final class ExecutorIndexCache {

  private static final Logger LOG = LoggerFactory.getLogger(ExecutorIndexCache.class);

  /** Settings travel to the executors with the job, so they are read once on the driver. */
  static final class Config implements Serializable {
    static final String ENABLED = "spark.iceberg.index.cache.enabled";
    static final String BYTES = "spark.iceberg.index.cache.bytes";
    static final String MAX_ENTRY_BYTES = "spark.iceberg.index.cache.max-entry-bytes";

    private static final Config DISABLED = new Config(false, 0, 0);

    private final boolean enabled;
    private final long maxBytes;
    private final long maxEntryBytes;

    private Config(boolean enabled, long maxBytes, long maxEntryBytes) {
      this.enabled = enabled;
      this.maxBytes = maxBytes;
      this.maxEntryBytes = maxEntryBytes;
    }

    static Config disabled() {
      return DISABLED;
    }

    static Config of(boolean enabled, long maxBytes, long maxEntryBytes) {
      Preconditions.checkArgument(maxBytes > 0, "The cache budget must be positive: %s", maxBytes);
      Preconditions.checkArgument(
          maxEntryBytes > 0, "The per-entry limit must be positive: %s", maxEntryBytes);
      return new Config(enabled, maxBytes, maxEntryBytes);
    }

    static Config from(org.apache.spark.sql.SparkSession spark) {
      boolean enabled = Boolean.parseBoolean(spark.conf().get(ENABLED, "false"));
      if (!enabled) {
        return DISABLED;
      }

      long maxBytes = parseBytes(spark.conf().get(BYTES, "512m"), BYTES);
      long maxEntryBytes = parseBytes(spark.conf().get(MAX_ENTRY_BYTES, "256m"), MAX_ENTRY_BYTES);
      Preconditions.checkArgument(maxBytes > 0, "The cache budget must be positive: %s", maxBytes);
      return new Config(true, maxBytes, maxEntryBytes);
    }

    private static long parseBytes(String value, String key) {
      String trimmed = value.trim().toLowerCase(java.util.Locale.ROOT);
      long scale = 1;
      if (trimmed.endsWith("m")) {
        scale = 1024L * 1024L;
        trimmed = trimmed.substring(0, trimmed.length() - 1);
      } else if (trimmed.endsWith("g")) {
        scale = 1024L * 1024L * 1024L;
        trimmed = trimmed.substring(0, trimmed.length() - 1);
      } else if (trimmed.endsWith("k")) {
        scale = 1024L;
        trimmed = trimmed.substring(0, trimmed.length() - 1);
      }

      try {
        return Long.parseLong(trimmed.trim()) * scale;
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            "Not a size for " + key + ": " + value + " (use 512m, 2g, or a number of bytes)", e);
      }
    }

    boolean enabled() {
      return enabled;
    }

    long maxBytes() {
      return maxBytes;
    }

    long maxEntryBytes() {
      return maxEntryBytes;
    }
  }

  /** One index file as an executor remembers it. */
  static final class CachedFile {
    private final byte[] payload;
    private final IndexAdapter.PreparedSearch prepared;
    private final long bytes;
    private final ReentrantLock searchLock = new ReentrantLock();

    private CachedFile(byte[] payload, IndexAdapter.PreparedSearch prepared, long bytes) {
      this.payload = payload;
      this.prepared = prepared;
      this.bytes = bytes;
    }

    byte[] payload() {
      return payload;
    }

    /** Runs the body against the prepared structure, with this file's search held exclusively. */
    <T> T searching(java.util.function.Function<IndexAdapter.PreparedSearch, T> body) {
      searchLock.lock();
      try {
        return body.apply(prepared);
      } finally {
        searchLock.unlock();
      }
    }

    public long bytes() {
      return bytes;
    }
  }

  /** The counters an executor reports, so a run can say whether the cache did anything. */
  public static final class Stats {
    private final long hits;
    private final long misses;
    private final long loads;
    private final long evictions;
    private final long skippedTooLarge;
    private final long bytes;
    private final long entries;

    private Stats(
        long hits,
        long misses,
        long loads,
        long evictions,
        long skippedTooLarge,
        long bytes,
        long entries) {
      this.hits = hits;
      this.misses = misses;
      this.loads = loads;
      this.evictions = evictions;
      this.skippedTooLarge = skippedTooLarge;
      this.bytes = bytes;
      this.entries = entries;
    }

    public long hits() {
      return hits;
    }

    public long misses() {
      return misses;
    }

    public long loads() {
      return loads;
    }

    public long evictions() {
      return evictions;
    }

    public long skippedTooLarge() {
      return skippedTooLarge;
    }

    public long bytes() {
      return bytes;
    }

    public long entries() {
      return entries;
    }

    @Override
    public String toString() {
      return "hits="
          + hits
          + " misses="
          + misses
          + " loads="
          + loads
          + " evictions="
          + evictions
          + " tooLarge="
          + skippedTooLarge
          + " entries="
          + entries
          + " bytes="
          + bytes;
    }
  }

  private static final ExecutorIndexCache INSTANCE = new ExecutorIndexCache();

  static ExecutorIndexCache get() {
    return INSTANCE;
  }

  // access order, so the first entry of the iteration is the least recently used
  private final Map<String, CachedFile> files = new LinkedHashMap<>(16, 0.75f, true);
  private final Object mapLock = new Object();
  // one monitor per location, so two tasks asking for the same file prepare it once
  private final ConcurrentHashMap<String, Object> loading = new ConcurrentHashMap<>();

  private final AtomicLong hits = new AtomicLong();
  private final AtomicLong misses = new AtomicLong();
  private final AtomicLong loads = new AtomicLong();
  private final AtomicLong evictions = new AtomicLong();
  private final AtomicLong skippedTooLarge = new AtomicLong();
  private long bytes = 0;

  private ExecutorIndexCache() {}

  /**
   * The index file at the location, read and prepared at most once per executor.
   *
   * @param location the index file's location, which identifies its content
   * @param config the budget, read on the driver and carried with the job
   * @param key what the prepared structure depends on besides the payload
   * @param loader reads the payload and prepares it, only called on a miss
   */
  CachedFile file(String location, Config config, String key, Loader loader) {
    if (!config.enabled()) {
      return loader.load();
    }

    String cacheKey = key + "|" + location;
    CachedFile hit = lookup(cacheKey);
    if (hit != null) {
      return hit;
    }

    Object monitor = loading.computeIfAbsent(cacheKey, ignored -> new Object());
    synchronized (monitor) {
      try {
        hit = lookup(cacheKey);
        if (hit != null) {
          return hit;
        }

        long start = System.nanoTime();
        CachedFile loaded = loader.load();
        loads.incrementAndGet();
        if (loaded.bytes() > config.maxEntryBytes()) {
          // a file larger than the per-entry limit is not worth evicting the rest for
          skippedTooLarge.incrementAndGet();
          LOG.debug(
              "Not caching {}: {} bytes is over the {} byte entry limit",
              location,
              loaded.bytes(),
              config.maxEntryBytes());
          return loaded;
        }

        put(cacheKey, loaded, config.maxBytes());
        LOG.debug(
            "Cached {} in {} ms ({} bytes)",
            location,
            java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start),
            loaded.bytes());
        return loaded;
      } finally {
        loading.remove(cacheKey, monitor);
      }
    }
  }

  private CachedFile lookup(String cacheKey) {
    synchronized (mapLock) {
      CachedFile file = files.get(cacheKey);
      if (file == null) {
        misses.incrementAndGet();
        return null;
      }

      hits.incrementAndGet();
      return file;
    }
  }

  private void put(String cacheKey, CachedFile file, long maxBytes) {
    synchronized (mapLock) {
      CachedFile previous = files.put(cacheKey, file);
      if (previous != null) {
        bytes -= previous.bytes();
      }
      bytes += file.bytes();

      Iterator<Map.Entry<String, CachedFile>> oldest = files.entrySet().iterator();
      while (bytes > maxBytes && oldest.hasNext()) {
        Map.Entry<String, CachedFile> evicted = oldest.next();
        bytes -= evicted.getValue().bytes();
        oldest.remove();
        evictions.incrementAndGet();
      }
    }
  }

  Stats stats() {
    synchronized (mapLock) {
      return new Stats(
          hits.get(),
          misses.get(),
          loads.get(),
          evictions.get(),
          skippedTooLarge.get(),
          bytes,
          files.size());
    }
  }

  /** Forgets every entry and every counter, so a test starts from a known executor. */
  void clear() {
    synchronized (mapLock) {
      files.clear();
      bytes = 0;
      hits.set(0);
      misses.set(0);
      loads.set(0);
      evictions.set(0);
      skippedTooLarge.set(0);
    }
  }

  /** Produces an index file's payload and the structure prepared from it. */
  interface Loader {
    CachedFile load();
  }

  /**
   * An entry whose cost is the payload plus whatever the prepared structure adds to it. A searcher
   * that expands the payload says so; one that does not is charged the payload twice, once for its
   * bytes and once for reading them.
   */
  static CachedFile loaded(byte[] payload, IndexAdapter.PreparedSearch prepared) {
    long preparedBytes = prepared.estimatedBytes();
    long bytes = payload.length + (preparedBytes > 0 ? preparedBytes : payload.length);
    return new CachedFile(payload, prepared, bytes);
  }

  static CachedFile loaded(byte[] payload, IndexAdapter.PreparedSearch prepared, long bytes) {
    return new CachedFile(payload, prepared, bytes);
  }
}
