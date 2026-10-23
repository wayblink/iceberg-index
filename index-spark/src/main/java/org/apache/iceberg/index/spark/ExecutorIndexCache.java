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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
 * budget, evicts entries it does not expect to need again, and is only filled when the caller turns
 * it on.
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
 *
 * <h2>Three admission modes</h2>
 *
 * <p>A query over an index whose working set fits the budget can simply keep every file ({@link
 * Mode#PIN_ALL}). When it does not fit, keeping every file makes each query evict what the next one
 * reads, which is worse than not caching at all; then the cache either admits a file only after it
 * has been asked for twice ({@link Mode#PIN_HOT}, so a scan does not displace what repeats) or
 * stays out of the way ({@link Mode#STREAM}). The mode is decided by the driver, which is the only
 * side that knows the working set and the cluster's total budget.
 */
final class ExecutorIndexCache {

  private static final Logger LOG = LoggerFactory.getLogger(ExecutorIndexCache.class);

  /** How an executor treats the entries it is asked for. */
  enum Mode {
    /** Keep every file, and do not evict a kept file to make room. */
    PIN_ALL,
    /** Admit a file only once it has been asked for twice, then protect it from eviction. */
    PIN_HOT,
    /** Do not keep anything; every query reads its files again. */
    STREAM
  }

  /** Settings travel to the executors with the job, so they are read once on the driver. */
  static final class Config implements Serializable {
    static final String ENABLED = "spark.iceberg.index.cache.enabled";
    static final String BYTES = "spark.iceberg.index.cache.bytes";
    static final String MAX_ENTRY_BYTES = "spark.iceberg.index.cache.max-entry-bytes";
    static final String HEAP_FRACTION = "spark.iceberg.index.cache.heap-fraction";
    static final String MODE = "spark.iceberg.index.cache.mode";
    static final String IDLE_TTL = "spark.iceberg.index.cache.idle-ttl";
    static final long MIN_BUDGET_BYTES = 512L * 1024L * 1024L;

    private static final Config DISABLED = new Config(false, 0, 0, Mode.STREAM, 0, "");

    private final boolean enabled;
    private final long maxBytes;
    private final long maxEntryBytes;
    private final Mode mode;
    private final long idleTtlMillis;
    private final String owner;

    private Config(
        boolean enabled,
        long maxBytes,
        long maxEntryBytes,
        Mode mode,
        long idleTtlMillis,
        String owner) {
      this.enabled = enabled;
      this.maxBytes = maxBytes;
      this.maxEntryBytes = maxEntryBytes;
      this.mode = mode;
      this.idleTtlMillis = idleTtlMillis;
      this.owner = owner;
    }

    static Config disabled() {
      return DISABLED;
    }

    static Config of(boolean enabled, long maxBytes, long maxEntryBytes) {
      Preconditions.checkArgument(maxBytes > 0, "The cache budget must be positive: %s", maxBytes);
      Preconditions.checkArgument(
          maxEntryBytes > 0, "The per-entry limit must be positive: %s", maxEntryBytes);
      return new Config(enabled, maxBytes, maxEntryBytes, Mode.PIN_ALL, 0, "");
    }

    static Config of(
        boolean enabled, long maxBytes, long maxEntryBytes, Mode mode, long idleTtlMillis) {
      Preconditions.checkArgument(maxBytes > 0, "The cache budget must be positive: %s", maxBytes);
      Preconditions.checkArgument(
          maxEntryBytes > 0, "The per-entry limit must be positive: %s", maxEntryBytes);
      return new Config(enabled, maxBytes, maxEntryBytes, mode, idleTtlMillis, "");
    }

    /**
     * The cache's settings for one query.
     *
     * @param spark the session
     * @param workingSetBytes the bytes the query's index files hold, which the budget has to cover
     *     for a per-executor share to stay resident; zero when it is not known
     */
    static Config from(org.apache.spark.sql.SparkSession spark, long workingSetBytes) {
      return from(spark, workingSetBytes, "");
    }

    /**
     * The settings for one query of one table.
     *
     * @param owner what the entries belong to, which is what a fair share is divided between
     */
    static Config from(
        org.apache.spark.sql.SparkSession spark, long workingSetBytes, String owner) {
      boolean enabled = Boolean.parseBoolean(spark.conf().get(ENABLED, "false"));
      if (!enabled) {
        return DISABLED;
      }

      String configured = spark.conf().get(BYTES, null);
      long configuredBytes = configured != null ? parseBytes(configured, BYTES) : 0L;
      long maxEntryBytes = parseBytes(spark.conf().get(MAX_ENTRY_BYTES, "256m"), MAX_ENTRY_BYTES);
      int executors = executors(spark);
      long budget =
          derive(
              configured,
              configuredBytes,
              workingSetBytes,
              executors,
              heapBytes(spark),
              heapFraction(spark));
      Mode mode = mode(spark.conf().get(MODE, "auto"), workingSetBytes, budget, executors);
      long idleTtlMillis = durationMillis(spark.conf().get(IDLE_TTL, "30m"), IDLE_TTL);
      long effectiveEntryLimit = Math.min(maxEntryBytes, budget);
      return new Config(
          true, budget, effectiveEntryLimit, mode, idleTtlMillis, owner == null ? "" : owner);
    }

    /**
     * Which admission mode the working set calls for.
     *
     * <p>The cluster's total budget is what a working set has to fit into, since every executor may
     * be asked for any file. Twice the budget is the range where a hot subset is worth keeping;
     * past that the cache would spend its time evicting, so it stays out of the way.
     */
    static Mode mode(
        String configured, long workingSetBytes, long budgetPerExecutor, int executors) {
      if (configured != null && !"auto".equalsIgnoreCase(configured.trim())) {
        return Mode.valueOf(configured.trim().toUpperCase(java.util.Locale.ROOT).replace('-', '_'));
      }

      if (workingSetBytes <= 0) {
        return Mode.PIN_ALL;
      }

      long capacity = budgetPerExecutor * Math.max(1, executors);
      if (workingSetBytes <= capacity) {
        return Mode.PIN_ALL;
      } else if (workingSetBytes <= 2 * capacity) {
        return Mode.PIN_HOT;
      } else {
        return Mode.STREAM;
      }
    }

    /**
     * The budget for one executor.
     *
     * <p>A configured budget is used as it is, because a run that names one wants that number; when
     * it is below an executor's share of the working set the cache can only evict what it just
     * loaded, so that case is reported rather than silently left to thrash.
     *
     * <p>Without a configured budget the budget is the smaller of a share of the executor's heap
     * and the bytes one executor has to hold, which is the whole of what a resident cache needs.
     */
    static long derive(
        String configuredValue,
        long configuredBytes,
        long workingSetBytes,
        int executors,
        long heapBytes,
        double heapFraction) {
      long perExecutor = workingSetBytes > 0 ? ceilDiv(workingSetBytes, Math.max(1, executors)) : 0;
      long heapBudget = (long) (heapBytes * heapFraction);
      if (configuredValue != null) {
        Preconditions.checkArgument(configuredBytes > 0, "The cache budget must be positive");
        if (perExecutor > 0 && configuredBytes < (long) (perExecutor * 0.9)) {
          LOG.warn(
              "The index cache budget of {} bytes is below the {} bytes one executor has to hold "
                  + "(working set {} over {} executors); the cache will evict what it just loaded "
                  + "and a repeated query will not be served from it. Raise {} or leave it unset.",
              configuredBytes,
              perExecutor,
              workingSetBytes,
              executors,
              BYTES);
        }

        return configuredBytes;
      }

      long wanted = perExecutor > 0 ? (long) (perExecutor * 1.2) : MIN_BUDGET_BYTES;
      return Math.min(heapBudget, Math.max(MIN_BUDGET_BYTES, wanted));
    }

    /** The executors a stage runs on, which is what a working set is shared between. */
    private static int executors(org.apache.spark.sql.SparkSession spark) {
      String instances = spark.conf().get("spark.executor.instances", "1");
      try {
        return Math.max(1, Integer.parseInt(instances.trim()));
      } catch (NumberFormatException e) {
        return 1;
      }
    }

    private static long heapBytes(org.apache.spark.sql.SparkSession spark) {
      return parseBytes(spark.conf().get("spark.executor.memory", "1g"), "spark.executor.memory");
    }

    private static double heapFraction(org.apache.spark.sql.SparkSession spark) {
      String value = spark.conf().get(HEAP_FRACTION, "0.25");
      try {
        double fraction = Double.parseDouble(value.trim());
        Preconditions.checkArgument(
            fraction > 0 && fraction <= 1, "The heap fraction must be in (0, 1]: %s", fraction);
        return fraction;
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Not a fraction for " + HEAP_FRACTION + ": " + value, e);
      }
    }

    static long parseBytes(String value, String key) {
      return parse(value, key, 1L);
    }

    private static long durationMillis(String value, String key) {
      return parse(value, key, 1L) * 1000L;
    }

    private static long parse(String value, String key, long multiplier) {
      String trimmed = value.trim().toLowerCase(java.util.Locale.ROOT);
      long scale = 1;
      if (trimmed.endsWith("ms")) {
        scale = 1;
        multiplier *= 1;
        trimmed = trimmed.substring(0, trimmed.length() - 2);
      } else if (trimmed.endsWith("m")) {
        scale = 1024L * 1024L;
        trimmed = trimmed.substring(0, trimmed.length() - 1);
      } else if (trimmed.endsWith("g")) {
        scale = 1024L * 1024L * 1024L;
        trimmed = trimmed.substring(0, trimmed.length() - 1);
      } else if (trimmed.endsWith("k")) {
        scale = 1024L;
        trimmed = trimmed.substring(0, trimmed.length() - 1);
      } else if (trimmed.endsWith("h")) {
        scale = 60L * 60L;
        trimmed = trimmed.substring(0, trimmed.length() - 1);
      } else if (trimmed.endsWith("s")) {
        scale = 1;
        trimmed = trimmed.substring(0, trimmed.length() - 1);
      }

      try {
        return Long.parseLong(trimmed.trim()) * scale * multiplier;
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            "Not a size for " + key + ": " + value + " (use 512m, 2g, 30m, or a number)", e);
      }
    }

    private static long ceilDiv(long dividend, long divisor) {
      return (dividend + divisor - 1) / divisor;
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

    Mode mode() {
      return mode;
    }

    long idleTtlMillis() {
      return idleTtlMillis;
    }

    /** The table these entries belong to, used to divide the budget fairly. */
    String owner() {
      return owner;
    }

    Config withOwner(String newOwner) {
      return new Config(enabled, maxBytes, maxEntryBytes, mode, idleTtlMillis, newOwner);
    }

    /** Only a mode that keeps files is worth the bookkeeping. */
    boolean caching() {
      return enabled && mode != Mode.STREAM;
    }
  }

  /** One index file as an executor remembers it. */
  static final class CachedFile {
    private final String owner;
    private final byte[] payload;
    private final IndexAdapter.PreparedSearch prepared;
    private final long bytes;
    private final ReentrantLock searchLock = new ReentrantLock();
    private int uses;
    // a PIN_ALL entry: the working set fits, so it is never evicted to make room
    private boolean pinned;
    // a 2Q entry: asked for twice, so it outranks an entry seen once
    private boolean protectedRank;
    private long lastAccessNanos = System.nanoTime();

    private CachedFile(
        String owner, byte[] payload, IndexAdapter.PreparedSearch prepared, long bytes) {
      this.owner = owner;
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

    /** Whether a search is running against this file, which makes it unsafe to unload. */
    boolean inUse() {
      if (searchLock.isHeldByCurrentThread()) {
        return true;
      }

      if (searchLock.tryLock()) {
        searchLock.unlock();
        return false;
      }

      return true;
    }

    public long bytes() {
      return bytes;
    }

    String owner() {
      return owner;
    }

    CachedFile withOwner(String newOwner) {
      return new CachedFile(newOwner, payload, prepared, bytes);
    }

    private void touch() {
      uses += 1;
      lastAccessNanos = System.nanoTime();
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

  /** Changes when the JVM does, so a driver can tell a restarted executor from a live one. */
  private static final long EPOCH = new java.security.SecureRandom().nextLong();

  static ExecutorIndexCache get() {
    return INSTANCE;
  }

  static long epoch() {
    return EPOCH;
  }

  // access order, so the first entry of the iteration is the least recently used
  private final Map<String, CachedFile> probation = new LinkedHashMap<>(16, 0.75f, true);
  private final Map<String, CachedFile> protectedFiles = new LinkedHashMap<>(16, 0.75f, true);
  // how often a file has been asked for without being kept, for the PIN_HOT admission
  private final Map<String, Integer> asked =
      new LinkedHashMap<>(16, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Integer> eldest) {
          return size() > 100_000;
        }
      };
  private final Object mapLock = new Object();
  // one monitor per location, so two tasks asking for the same file prepare it once
  private final ConcurrentHashMap<String, Object> loading = new ConcurrentHashMap<>();

  private final AtomicLong hits = new AtomicLong();
  private final AtomicLong misses = new AtomicLong();
  private final AtomicLong loads = new AtomicLong();
  private final AtomicLong evictions = new AtomicLong();
  private final AtomicLong skippedTooLarge = new AtomicLong();
  private long bytes = 0;

  // room promised to loads in flight, and the budget those promises are against
  private long reservedBytes = 0;
  private volatile long budgetBytes = Long.MAX_VALUE;

  // what each table holds, so a share of the budget can be reserved per table
  private final Map<String, Long> bytesByOwner = new LinkedHashMap<>();
  private final Map<String, Integer> entriesByOwner = new LinkedHashMap<>();
  private long lastSweepNanos = System.nanoTime();
  private static long sweepIntervalNanos = java.util.concurrent.TimeUnit.SECONDS.toNanos(30);

  /** Lets a test drive the idle sweep without waiting half a minute. */
  static void sweepIntervalForTests(long nanos) {
    sweepIntervalNanos = nanos;
  }

  private ExecutorIndexCache() {}

  /**
   * The index file at the location, read and prepared at most once per executor.
   *
   * @param location the index file's location, which identifies its content
   * @param config the budget and mode, read on the driver and carried with the job
   * @param key what the prepared structure depends on besides the payload
   * @param loader reads the payload and prepares it, only called on a miss
   */
  CachedFile file(String location, Config config, String key, Loader loader) {
    if (!config.caching()) {
      return loader.load();
    }

    budgetBytes = config.maxBytes();
    String cacheKey = config.owner() + "|" + key + "|" + location;
    sweepIfDue(config);
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

        boolean askedTwice = config.mode() == Mode.PIN_HOT && askedBefore(cacheKey);
        if (config.mode() == Mode.PIN_HOT && !askedTwice) {
          // a scan should not displace what repeats: load it, but wait for a second ask
          return loader.load();
        }

        long start = System.nanoTime();
        // the size is not known until the payload is prepared, so the room held is what the entry
        // limit allows; it goes back as soon as the load finishes
        long reservation = Math.min(config.maxEntryBytes(), config.maxBytes());
        long held = reserve(reservation);

        CachedFile loaded;
        try {
          loaded = loader.load();
        } catch (RuntimeException | Error e) {
          releaseReservation(reservation);
          throw e;
        }

        loads.incrementAndGet();
        // the load has finished, so the reservation goes back whether or not the entry is kept
        releaseReservation(reservation);
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
        CachedFile stored = put(cacheKey, loaded, config, askedTwice);
        if (stored != loaded) {
          // the cached copy carries the owner; a caller should see the entry it will hit next time
          return stored;
        }
        LOG.debug(
            "Cached {} in {} ms ({} bytes, mode {})",
            location,
            java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start),
            loaded.bytes(),
            config.mode());
        return loaded;
      } finally {
        loading.remove(cacheKey, monitor);
      }
    }
  }

  private boolean askedBefore(String cacheKey) {
    synchronized (mapLock) {
      int times = asked.merge(cacheKey, 1, Integer::sum);
      return times > 1;
    }
  }

  private CachedFile lookup(String cacheKey) {
    synchronized (mapLock) {
      CachedFile file = protectedFiles.get(cacheKey);
      if (file == null) {
        file = probation.get(cacheKey);
      }

      if (file == null) {
        misses.incrementAndGet();
        return null;
      }

      hits.incrementAndGet();
      file.touch();
      if (file.uses >= 1 && !file.pinned) {
        // a file asked for twice is what a repeated query looks like, so it outranks one seen once
        probation.remove(cacheKey);
        file.protectedRank = true;
        protectedFiles.put(cacheKey, file);
      }

      return file;
    }
  }

  private CachedFile put(String cacheKey, CachedFile file, Config config, boolean askedTwice) {
    synchronized (mapLock) {
      CachedFile previous = protectedFiles.remove(cacheKey);
      if (previous == null) {
        previous = probation.remove(cacheKey);
      }

      if (previous != null) {
        release(previous);
      }

      // the loader does not know which table it is loading for, so the entry is stamped here
      file = file.withOwner(config.owner());
      file.pinned = config.mode() == Mode.PIN_ALL;
      // an admitted PIN_HOT entry starts on probation: being asked twice earns it a place, being
      // asked again is what earns it protection
      if (file.pinned) {
        protectedFiles.put(cacheKey, file);
      } else {
        probation.put(cacheKey, file);
      }

      bytes += file.bytes();
      bytesByOwner.merge(file.owner(), file.bytes(), Long::sum);
      entriesByOwner.merge(file.owner(), 1, Integer::sum);
      evict(config.maxBytes());

      if (bytes > config.maxBytes()) {
        // everything left is either pinned or being searched, so this file is not kept at all
        // rather than being added to a budget that is already over
        if (protectedFiles.remove(cacheKey) != null || probation.remove(cacheKey) != null) {
          release(file);
        }

        return file;
      }

      return file;
    }
  }

  /**
   * Makes room, evicting the least recently used file that is neither protected nor in use.
   *
   * <p>A PIN_ALL entry is not evicted to make room: the mode exists because the working set fits,
   * so evicting one would mean the fit was wrong, and the caller learns that from a budget warning
   * rather than from a cache that quietly does nothing.
   */
  private void evict(long maxBytes) {
    boolean warned = false;
    while (bytes > maxBytes) {
      // the table holding the most beyond its fair share goes first, so one large index cannot
      // crowd out a small one that is also being queried
      Map.Entry<String, CachedFile> victim = mostOverQuota(maxBytes);
      if (victim == null) {
        victim = oldestEvictable(probation, false);
      }

      if (victim == null) {
        victim = oldestEvictable(protectedFiles, true);
      }

      if (victim == null) {
        // everything resident is protected or being searched; the new file is not kept
        return;
      }

      release(victim.getValue());
      if (protectedFiles.remove(victim.getKey()) == null) {
        probation.remove(victim.getKey());
      }

      if (evictions.incrementAndGet() == 1 && hits.get() == 0 && !warned) {
        warned = true;
        LOG.warn(
            "The index cache evicted an entry before it served a single hit: budget {} bytes. "
                + "The budget is likely below the working set, so raise {}, unset it to derive "
                + "one, or turn the cache off.",
            maxBytes,
            Config.BYTES);
      }
    }
  }

  /**
   * The oldest evictable entry of the table that is furthest beyond its fair share.
   *
   * <p>The share is the budget divided by the number of tables holding anything, recomputed as
   * tables come and go: a table that borrows room another table is not using keeps it until that
   * table asks for it back, which is what "borrowed" means here.
   */
  private Map.Entry<String, CachedFile> mostOverQuota(long maxBytes) {
    int owners = Math.max(1, bytesByOwner.size());
    long quota = maxBytes / owners;
    String worst = null;
    long worstExcess = 0;
    for (Map.Entry<String, Long> entry : bytesByOwner.entrySet()) {
      long excess = entry.getValue() - quota;
      if (excess > worstExcess) {
        worst = entry.getKey();
        worstExcess = excess;
      }
    }

    if (worst == null) {
      return null;
    }

    String owner = worst;
    Map.Entry<String, CachedFile> found = oldestEvictableOf(probation, owner, false);
    return found != null ? found : oldestEvictableOf(protectedFiles, owner, true);
  }

  private Map.Entry<String, CachedFile> oldestEvictableOf(
      Map<String, CachedFile> segment, String owner, boolean skipPinned) {
    for (Map.Entry<String, CachedFile> entry : segment.entrySet()) {
      CachedFile file = entry.getValue();
      if (skipPinned && file.pinned) {
        continue;
      }

      if (owner.equals(file.owner()) && !file.inUse()) {
        return entry;
      }
    }

    return null;
  }

  /**
   * Takes room for an entry that is about to be loaded, or refuses when taking it would exceed the
   * budget.
   *
   * <p>Loading first and measuring afterwards is what makes a budget porous: two tasks that each
   * load a file both see room until one of them finishes. The reservation is the room that has been
   * promised to a load in flight, and it is what makes "we will not exceed the budget" true rather
   * than usually true.
   */
  /**
   * Holds room for an entry that is about to be loaded, taking what is spare.
   *
   * <p>Loading first and measuring afterwards is what makes a budget porous: two tasks that each
   * load a file both see room until one of them finishes, and the second is the one that overflows.
   * The reservation is the room promised to a load in flight, which is the only thing eviction
   * cannot reclaim because neither load has been put yet.
   *
   * <p>Returns what it actually held, which is what has to be handed back.
   *
   * <p>It is deliberately not a refusal. A file whose own size exceeds the budget still gets cached
   * when it is the only entry, which is what a single-index query needs, and the entry limit is
   * what refuses those. When there is no room to spare the load simply reserves nothing and
   * eviction makes room afterwards, as it always did.
   */
  private long reserve(long bytes) {
    synchronized (mapLock) {
      long spare = budgetBytes - this.bytes - reservedBytes;
      long held = Math.min(bytes, Math.max(0L, spare));
      reservedBytes += held;
      return held;
    }
  }

  private void releaseReservation(long bytes) {
    if (bytes <= 0) {
      return;
    }

    synchronized (mapLock) {
      reservedBytes = Math.max(0L, reservedBytes - bytes);
    }
  }

  private void release(CachedFile file) {
    bytes -= file.bytes();
    bytesByOwner.computeIfPresent(
        file.owner(), (owner, held) -> held - file.bytes() <= 0 ? null : held - file.bytes());
    entriesByOwner.computeIfPresent(file.owner(), (owner, held) -> held <= 1 ? null : held - 1);
  }

  private Map.Entry<String, CachedFile> oldestEvictable(
      Map<String, CachedFile> segment, boolean skipPinned) {
    for (Map.Entry<String, CachedFile> entry : segment.entrySet()) {
      CachedFile file = entry.getValue();
      if (skipPinned && file.pinned) {
        continue;
      }

      if (!file.inUse()) {
        return entry;
      }
    }

    return null;
  }

  /** Unloads entries that no query has asked for within the idle window. */
  private void sweepIfDue(Config config) {
    long now = System.nanoTime();
    long ttlNanos =
        config.idleTtlMillis() > 0
            ? java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(config.idleTtlMillis())
            : java.util.concurrent.TimeUnit.MINUTES.toNanos(30);
    if (ttlNanos <= 0 || now - lastSweepNanos < sweepIntervalNanos) {
      return;
    }

    lastSweepNanos = now;
    synchronized (mapLock) {
      int unloaded =
          evictIdle(probation, now - ttlNanos) + evictIdle(protectedFiles, now - ttlNanos);
      if (unloaded > 0) {
        LOG.debug("Unloaded {} idle index cache entries ({} bytes left)", unloaded, bytes);
      }
    }
  }

  private int evictIdle(Map<String, CachedFile> segment, long deadlineNanos) {
    int unloaded = 0;
    Iterator<Map.Entry<String, CachedFile>> entries = segment.entrySet().iterator();
    while (entries.hasNext()) {
      Map.Entry<String, CachedFile> entry = entries.next();
      CachedFile file = entry.getValue();
      if (file.lastAccessNanos > deadlineNanos || file.inUse()) {
        continue;
      }

      bytes -= file.bytes();
      entries.remove();
      unloaded += 1;
    }

    return unloaded;
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
          probation.size() + protectedFiles.size());
    }
  }

  /** The identities of the files this executor can serve, for the driver's location table. */
  long[] keys() {
    synchronized (mapLock) {
      List<String> keys = new ArrayList<>(probation.size() + protectedFiles.size());
      keys.addAll(probation.keySet());
      keys.addAll(protectedFiles.keySet());
      return IndexCacheLocations.sortedHashes(keys);
    }
  }

  /** The report this executor sends back at the end of a task. */
  IndexCacheLocations.Report report(String executorId, String host) {
    Stats stats = stats();
    return new IndexCacheLocations.Report(
        executorId,
        host,
        EPOCH,
        System.nanoTime(),
        keys(),
        stats.hits(),
        stats.misses(),
        stats.loads(),
        stats.evictions(),
        stats.bytes(),
        stats.entries(),
        System.currentTimeMillis());
  }

  /** Forgets every entry and every counter, so a test starts from a known executor. */
  void clear() {
    synchronized (mapLock) {
      probation.clear();
      protectedFiles.clear();
      asked.clear();
      bytes = 0;
      reservedBytes = 0;
      hits.set(0);
      misses.set(0);
      loads.set(0);
      evictions.set(0);
      skippedTooLarge.set(0);
      lastSweepNanos = System.nanoTime();
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
    return new CachedFile("", payload, prepared, bytes);
  }

  static CachedFile loaded(byte[] payload, IndexAdapter.PreparedSearch prepared, long bytes) {
    return new CachedFile("", payload, prepared, bytes);
  }

  Set<String> residentKeys() {
    synchronized (mapLock) {
      Set<String> keys = new java.util.LinkedHashSet<>(probation.keySet());
      keys.addAll(protectedFiles.keySet());
      return keys;
    }
  }
}
