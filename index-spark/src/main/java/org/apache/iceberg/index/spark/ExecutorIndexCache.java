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
import java.util.LinkedHashSet;
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
 * <p>Explicitly managed index targets use controller epochs and query leases. Their entries are
 * released by the controller rather than local file eviction; a lease keeps a file charged until
 * the query relinquishes it, including the interval before its search begins.
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
    static final String HEAP_FRACTION = "spark.iceberg.index.cache.heap-fraction";
    static final String MODE = "spark.iceberg.index.cache.mode";
    static final String IDLE_TTL = "spark.iceberg.index.cache.idle-ttl";
    static final long MIN_BUDGET_BYTES = 512L * 1024L * 1024L;

    /** The executor heap a budget is derived from when the application does not size one. */
    static final String EXECUTOR_MEMORY = "spark.executor.memory";

    /** The heap to assume when the application sizes no executor. */
    static final String DEFAULT_EXECUTOR_MEMORY = "1g";

    /** The default share of the executor heap a budget may use. */
    static final String DEFAULT_HEAP_FRACTION = "0.25";

    private static final Config DISABLED = new Config(false, 0, Mode.STREAM, 0, "");

    private final boolean enabled;
    private final long maxBytes;
    private final Mode mode;
    private final long idleTtlMillis;
    private final String owner;

    private Config(boolean enabled, long maxBytes, Mode mode, long idleTtlMillis, String owner) {
      this.enabled = enabled;
      this.maxBytes = maxBytes;
      this.mode = mode;
      this.idleTtlMillis = idleTtlMillis;
      this.owner = owner;
    }

    /**
     * Reads a setting, from the session configuration or from the one the application was started
     * with.
     *
     * <p>A cache setting is often set with {@code --conf}, which lands in the application's {@link
     * org.apache.spark.SparkConf} and never in the session's SQL configuration. Reading only the
     * session made every such run look like it had the cache turned off, and the coverage this
     * cache reports was then unobservable exactly when it was the thing under test.
     */
    private static String setting(
        org.apache.spark.sql.SparkSession spark, String key, String fallback) {
      String fromSession = spark.conf().get(key, null);
      if (fromSession != null && !fromSession.isEmpty()) {
        return fromSession;
      }

      String fromApplication = spark.sparkContext().getConf().get(key, null);
      return fromApplication != null && !fromApplication.isEmpty() ? fromApplication : fallback;
    }

    static Config disabled() {
      return DISABLED;
    }

    static Config of(boolean enabled, long maxBytes) {
      Preconditions.checkArgument(maxBytes > 0, "The cache budget must be positive: %s", maxBytes);
      return new Config(enabled, maxBytes, Mode.PIN_ALL, 0, "");
    }

    static Config of(boolean enabled, long maxBytes, Mode mode, long idleTtlMillis) {
      Preconditions.checkArgument(maxBytes > 0, "The cache budget must be positive: %s", maxBytes);
      return new Config(enabled, maxBytes, mode, idleTtlMillis, "");
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
      boolean enabled = Boolean.parseBoolean(setting(spark, ENABLED, "false"));
      if (!enabled) {
        return DISABLED;
      }

      String configured = setting(spark, BYTES, null);
      long configuredBytes = configured != null ? parseBytes(configured, BYTES) : 0L;
      int executors = executors(spark);
      long budget =
          derive(
              configured,
              configuredBytes,
              workingSetBytes,
              executors,
              heapBytes(spark),
              heapFraction(spark));
      Mode mode = mode(setting(spark, MODE, "auto"), workingSetBytes, budget, executors);
      long idleTtlMillis = durationMillis(setting(spark, IDLE_TTL, "30m"), IDLE_TTL);
      return new Config(true, budget, mode, idleTtlMillis, owner == null ? "" : owner);
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

      // compared in the same unit as the budget: what the payloads become, not what they weigh
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
     * and what the whole working set becomes once prepared, which is what a resident cache needs to
     * hold every file a query can be sent.
     */
    static long derive(
        String configuredValue,
        long configuredBytes,
        long workingSetBytes,
        int executors,
        long heapBytes,
        double heapFraction) {
      // The whole working set, not a share of it. A file that only some executors hold is served by
      // the ones that do, which is what the location hints are for, but a budget that assumes every
      // executor gets an even slice is one file away from evicting: a search read 33 of 39 files
      // with a budget of one executor's share. Holding the whole set on every executor costs memory
      // the query was not using and makes the cache independent of how the scheduler distributes
      // tasks.
      long perExecutor = workingSetBytes;
      long heapBudget = (long) (heapBytes * heapFraction);
      if (configuredValue != null) {
        Preconditions.checkArgument(configuredBytes > 0, "The cache budget must be positive");
        if (perExecutor > 0 && configuredBytes < (long) (perExecutor * 0.9)) {
          LOG.warn(
              "The index cache budget of {} bytes is below the {} bytes the query's index files "
                  + "become once prepared (a working set of {} bytes over {} executors); the cache "
                  + "will evict what it just loaded and a repeated query will not be served from "
                  + "it. Raise {} or leave it unset.",
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
      String instances = setting(spark, "spark.executor.instances", "1");
      try {
        return Math.max(1, Integer.parseInt(instances.trim()));
      } catch (NumberFormatException e) {
        return 1;
      }
    }

    private static long heapBytes(org.apache.spark.sql.SparkSession spark) {
      return parseBytes(setting(spark, EXECUTOR_MEMORY, DEFAULT_EXECUTOR_MEMORY), EXECUTOR_MEMORY);
    }

    private static double heapFraction(org.apache.spark.sql.SparkSession spark) {
      return fraction(setting(spark, HEAP_FRACTION, DEFAULT_HEAP_FRACTION));
    }

    /**
     * The budget an executor advertises, from the settings the application runs with.
     *
     * <p>An executor has no session, so this takes the values a session would read rather than a
     * session: the configured budget when there is one, and otherwise the same automatic derivation
     * a query makes when the working set is unknown. Advertising nothing while a query derives a
     * budget is what made every load fail as a cache that is off on executors a query was filling.
     *
     * <p>Zero and negative values are how a run says it wants no cache, and they stay distinct from
     * an unset value, which asks for the automatic budget.
     *
     * @param configuredBytes the configured budget, or null when none is configured
     * @param executorMemory the executor heap, or null when the application did not size one
     * @param heapFraction the share of that heap a budget may use, or null for the default
     */
    static long advertisedBudget(
        String configuredBytes, String executorMemory, String heapFraction) {
      boolean configured = configuredBytes != null && !configuredBytes.isBlank();
      long configuredValue = configured ? parseBytes(configuredBytes, BYTES) : 0L;
      if (configured && configuredValue <= 0) {
        return 0L;
      }

      return derive(
          configured ? configuredBytes : null,
          configuredValue,
          // an executor does not know the query's working set, so the budget falls back to the
          // heap share a query uses in the same situation
          0L,
          1,
          parseBytes(
              executorMemory == null || executorMemory.isBlank()
                  ? DEFAULT_EXECUTOR_MEMORY
                  : executorMemory,
              EXECUTOR_MEMORY),
          fraction(
              heapFraction == null || heapFraction.isBlank()
                  ? DEFAULT_HEAP_FRACTION
                  : heapFraction));
    }

    /** The share of the heap a budget may use. */
    private static double fraction(String value) {
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
      return new Config(enabled, maxBytes, mode, idleTtlMillis, newOwner);
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
    // Updated under mapLock; also observed by the search/eviction guards.
    private volatile int leases;
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

    /** Whether a query lease or an active search prevents this file from being released. */
    boolean inUse() {
      if (leases > 0 || searchLock.isHeldByCurrentThread()) {
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
    private final long bytes;
    private final long entries;

    private Stats(long hits, long misses, long loads, long evictions, long bytes, long entries) {
      this.hits = hits;
      this.misses = misses;
      this.loads = loads;
      this.evictions = evictions;
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
  private final IndexCacheOwnership ownership = new IndexCacheOwnership();
  private final Set<String> pendingReleases = new LinkedHashSet<>();
  // Observational associations do not confer residency ownership or protection.
  private final Map<String, Set<String>> attribution = new LinkedHashMap<>();
  private long generation;
  // Registered waiters keep the same monitor alive until every caller has left.
  private final ConcurrentHashMap<String, LoadMonitor> loading = new ConcurrentHashMap<>();

  private static final class LoadMonitor {
    private int callers;
  }

  private record ManagedLoad(String indexUuid, long epoch, String cacheKey, long generation) {}

  private final AtomicLong hits = new AtomicLong();
  private final AtomicLong misses = new AtomicLong();
  private final AtomicLong loads = new AtomicLong();
  private final AtomicLong evictions = new AtomicLong();
  private long bytes = 0;

  // room promised to loads in flight, and the budget those promises are against
  private long reservedBytes = 0;
  private final Set<Reservation> reservations = new LinkedHashSet<>();
  private final Map<String, Long> reservedByOwner = new LinkedHashMap<>();

  private static final class Reservation {
    private final String owner;
    private final long bytes;
    private final long budget;

    private Reservation(String owner, long bytes, long budget) {
      this.owner = owner;
      this.bytes = bytes;
      this.budget = budget;
    }
  }

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
    return file(location, config, key, 0L, loader);
  }

  /**
   * The same, for a caller that knows how large the file is before reading it.
   *
   * <p>What one entry costs cannot be known from a configured ceiling any more, and reserving the
   * whole budget for every load would let one load hold the cache while it prepares, serialising a
   * query's loads. The length of the file is a cheap lower bound and keeps the reservations in
   * proportion to the files, without a setting to keep in step with the data.
   */
  CachedFile file(String location, Config config, String key, long sizeHint, Loader loader) {
    if (!config.caching()) {
      return loader.load();
    }

    String cacheKey = config.owner() + "|" + key + "|" + location;
    sweepIfDue(config);
    boolean managed;
    CachedFile hit;
    synchronized (mapLock) {
      managed = ownership.isManagedKey(cacheKey);
      hit = managed ? null : lookup(cacheKey);
    }
    // This legacy API has no index epoch or lease; managed queries must use acquireManaged.
    if (managed) {
      return loader.load();
    }

    if (hit != null) {
      return hit;
    }

    LoadMonitor monitor = loadingMonitor(cacheKey);
    synchronized (monitor) {
      try {
        synchronized (mapLock) {
          managed = ownership.isManagedKey(cacheKey);
          hit = managed ? null : lookup(cacheKey);
        }
        if (managed) {
          return loader.load();
        }
        if (hit != null) {
          return hit;
        }

        boolean askedTwice = config.mode() == Mode.PIN_HOT && askedBefore(cacheKey);
        if (config.mode() == Mode.PIN_HOT && !askedTwice) {
          // a scan should not displace what repeats: load it, but wait for a second ask
          return loader.load();
        }

        Reservation reservation = reserve(config, null, sizeHint);
        try {
          CachedFile loaded = loader.load();
          loads.incrementAndGet();
          return put(cacheKey, loaded, config, reservation);
        } finally {
          releaseReservation(reservation);
        }
      } finally {
        releaseLoadingMonitor(cacheKey, monitor);
      }
    }
  }

  /** Records optional index associations for files that are currently resident. */
  void recordIndexAttribution(String indexUuid, Set<String> cacheKeys) {
    Preconditions.checkArgument(
        indexUuid != null && !indexUuid.isBlank(), "Index identity is empty");
    Preconditions.checkArgument(cacheKeys != null, "Index attribution keys are null");
    synchronized (mapLock) {
      for (String key : cacheKeys) {
        if (probation.containsKey(key) || protectedFiles.containsKey(key)) {
          attribution.computeIfAbsent(key, ignored -> new LinkedHashSet<>()).add(indexUuid);
        }
      }
    }
  }

  /** Returns recorded associations, not managed ownership or load readiness. */
  Map<String, Set<String>> indexAttribution() {
    synchronized (mapLock) {
      Map<String, Set<String>> snapshot = new LinkedHashMap<>();
      attribution.forEach((key, indexes) -> snapshot.put(key, Set.copyOf(indexes)));
      return Map.copyOf(snapshot);
    }
  }

  /** Applies a controller-supplied, table-qualified index identity and target atomically. */
  void applyTarget(String indexUuid, long epoch, Set<String> cacheKeys) {
    synchronized (mapLock) {
      Set<String> previous = ownership.keys(indexUuid);
      ownership.applyTarget(indexUuid, epoch, cacheKeys);
      for (String key : previous) {
        releaseIfUnowned(key);
      }
      mapLock.notifyAll();
    }
  }

  /** Returns only a retained current-epoch entry; rejection never constitutes a load ACK. */
  CachedFile installManaged(
      String indexUuid, long epoch, String cacheKey, Config config, Loader loader) {
    return installManaged(indexUuid, epoch, cacheKey, config, 0L, loader);
  }

  CachedFile installManaged(
      String indexUuid, long epoch, String cacheKey, Config config, long sizeHint, Loader loader) {
    if (!config.enabled()) {
      return null;
    }

    ManagedLoad load;
    synchronized (mapLock) {
      load = new ManagedLoad(indexUuid, epoch, cacheKey, generation);
      if (!current(load)) {
        return null;
      }
    }

    LoadMonitor monitor = loadingMonitor(cacheKey);
    synchronized (monitor) {
      try {
        synchronized (mapLock) {
          if (!current(load)) {
            return null;
          }
          CachedFile existing = find(cacheKey);
          if (existing != null) {
            if (!canReuse(existing, config, null)) {
              return null;
            }
            pendingReleases.remove(cacheKey);
            return existing;
          }
        }
        Reservation reservation = reserve(config, load, sizeHint);
        if (reservation == null) {
          return null;
        }
        try {
          CachedFile loaded = loader.load();
          loads.incrementAndGet();
          CachedFile retained = put(cacheKey, loaded, config, reservation, load);
          synchronized (mapLock) {
            return current(load) && find(cacheKey) == retained ? retained : null;
          }
        } finally {
          releaseReservation(reservation);
        }
      } finally {
        releaseLoadingMonitor(cacheKey, monitor);
      }
    }
  }

  /**
   * Leases a resident managed copy by cache key alone, for a query that does not know the index
   * epoch. Returns null when the key is not managed, no index admits loads, or nothing is resident,
   * and the caller then reads the file without retaining it.
   */
  Lease acquireManagedKey(String cacheKey) {
    synchronized (mapLock) {
      if (!ownership.admits(cacheKey)) {
        return null;
      }

      CachedFile file = lookup(cacheKey);
      if (file == null) {
        return null;
      }

      file.leases = Math.incrementExact(file.leases);
      return new Lease(cacheKey, file);
    }
  }

  Lease acquireManaged(String indexUuid, long epoch, String cacheKey) {
    synchronized (mapLock) {
      if (!ownership.mayAdmit(indexUuid, epoch, cacheKey)) {
        return null;
      }
      CachedFile file = lookup(cacheKey);
      if (file == null) {
        return null;
      }
      file.leases = Math.incrementExact(file.leases);
      return new Lease(cacheKey, file);
    }
  }

  /**
   * True while any cached file is held by a running search or lease. A managed target may only be
   * applied once the references a query already received have finished.
   */
  boolean hasReferencesInUse() {
    synchronized (mapLock) {
      for (CachedFile file : probation.values()) {
        if (file.inUse()) {
          return true;
        }
      }

      for (CachedFile file : protectedFiles.values()) {
        if (file.inUse()) {
          return true;
        }
      }

      return false;
    }
  }

  /** Adds keys to an index's current target, so a plan can arrive file by file. */
  void addManagedKeys(String indexUuid, long epoch, Set<String> cacheKeys) {
    synchronized (mapLock) {
      ownership.addKeys(indexUuid, epoch, cacheKeys);
      mapLock.notifyAll();
    }
  }

  /** Releases one file of an index; other files and other indexes keep their ownership. */
  boolean releaseCopy(String indexUuid, String cacheKey) {
    synchronized (mapLock) {
      boolean released = ownership.releaseKey(indexUuid, cacheKey);
      releaseIfUnowned(cacheKey);
      mapLock.notifyAll();
      return released;
    }
  }

  void releaseIndex(String indexUuid, long epoch) {
    synchronized (mapLock) {
      Set<String> keys = ownership.keys(indexUuid);
      ownership.offload(indexUuid, epoch);
      ownership.release(indexUuid, epoch);
      for (String key : keys) {
        releaseIfUnowned(key);
      }
      mapLock.notifyAll();
    }
  }

  final class Lease implements AutoCloseable {
    private final String cacheKey;
    private CachedFile file;
    private boolean closed;

    private Lease(String cacheKey, CachedFile file) {
      this.cacheKey = cacheKey;
      this.file = file;
    }

    CachedFile file() {
      synchronized (mapLock) {
        Preconditions.checkState(!closed, "Index file lease is closed");
        return file;
      }
    }

    @Override
    public void close() {
      synchronized (mapLock) {
        if (!closed) {
          closed = true;
          CachedFile released = file;
          this.file = null;
          released.leases -= 1;
          if (pendingReleases.contains(cacheKey)) {
            releaseIfUnowned(cacheKey);
          }
          mapLock.notifyAll();
        }
      }
    }
  }

  private LoadMonitor loadingMonitor(String cacheKey) {
    return loading.compute(
        cacheKey,
        (ignored, existing) -> {
          LoadMonitor monitor = existing == null ? new LoadMonitor() : existing;
          monitor.callers = Math.incrementExact(monitor.callers);
          return monitor;
        });
  }

  private void releaseLoadingMonitor(String cacheKey, LoadMonitor monitor) {
    loading.computeIfPresent(
        cacheKey,
        (ignored, existing) -> {
          Preconditions.checkState(
              existing == monitor, "Index loading monitor changed: %s", cacheKey);
          existing.callers -= 1;
          return existing.callers == 0 ? null : existing;
        });
  }

  private boolean current(ManagedLoad load) {
    return generation == load.generation()
        && ownership.mayAdmit(load.indexUuid(), load.epoch(), load.cacheKey());
  }

  private boolean canReuse(CachedFile file, Config config, Reservation reservation) {
    long otherReservations = reservedBytes - (reservation == null ? 0 : reservation.bytes);
    return bytes <= budget(config.maxBytes()) - otherReservations;
  }

  private CachedFile find(String cacheKey) {
    CachedFile file = protectedFiles.get(cacheKey);
    return file == null ? probation.get(cacheKey) : file;
  }

  private void releaseIfUnowned(String cacheKey) {
    if (ownership.isOwned(cacheKey)) {
      pendingReleases.remove(cacheKey);
      return;
    }
    CachedFile file = find(cacheKey);
    if (file == null) {
      pendingReleases.remove(cacheKey);
    } else if (file.inUse()) {
      pendingReleases.add(cacheKey);
    } else {
      protectedFiles.remove(cacheKey);
      probation.remove(cacheKey);
      attribution.remove(cacheKey);
      pendingReleases.remove(cacheKey);
      release(file);
    }
  }

  private void drainPendingReleases() {
    if (!pendingReleases.isEmpty()) {
      for (String key : new ArrayList<>(pendingReleases)) {
        releaseIfUnowned(key);
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

  private CachedFile put(String cacheKey, CachedFile file, Config config, Reservation reservation) {
    return put(cacheKey, file, config, reservation, null);
  }

  private CachedFile put(
      String cacheKey, CachedFile file, Config config, Reservation reservation, ManagedLoad load) {
    synchronized (mapLock) {
      if (load == null ? ownership.isManagedKey(cacheKey) : !current(load)) {
        return file;
      }
      CachedFile existing = find(cacheKey);
      if (existing != null) {
        return load == null || canReuse(existing, config, reservation) ? existing : null;
      }
      long capacity = budget(config.maxBytes());
      // Reclaim any extra room before admitting the measured entry. Other loads keep their
      // promises.
      long otherReservations = reservedBytes - reservation.bytes;
      long available = Math.max(0L, capacity - otherReservations);
      evict(Math.max(0L, available - file.bytes()), capacity, reservation, file.bytes());
      if (file.bytes() > available - bytes) {
        LOG.debug("Not caching {}: no room within the {} byte budget", cacheKey, capacity);
        return file;
      }

      releaseReservation(reservation);

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
      LOG.debug("Cached {} ({} bytes, mode {})", cacheKey, file.bytes(), config.mode());
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
  private void evict(
      long maxBytes, long fairnessBudget, Reservation reservation, long incomingBytes) {
    boolean warned = false;
    while (bytes > maxBytes) {
      // the table holding the most beyond its fair share goes first, so one large index cannot
      // crowd out a small one that is also being queried
      Map.Entry<String, CachedFile> victim =
          mostOverQuota(fairnessBudget, reservation, incomingBytes);
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
      attribution.remove(victim.getKey());
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
  private Map.Entry<String, CachedFile> mostOverQuota(
      long fairnessBudget, Reservation reservation, long incomingBytes) {
    Map<String, Long> occupied = new LinkedHashMap<>(bytesByOwner);
    reservedByOwner.forEach((owner, held) -> occupied.merge(owner, held, Long::sum));
    // The arriving entry replaces its own promise, not another table's share.
    occupied.merge(reservation.owner, incomingBytes - reservation.bytes, Long::sum);
    int owners = Math.max(1, occupied.size());
    long quota = fairnessBudget / owners;
    String worst = null;
    long worstExcess = 0;
    for (Map.Entry<String, Long> entry : occupied.entrySet()) {
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
      if (ownership.isOwned(entry.getKey()) || (skipPinned && file.pinned)) {
        continue;
      }

      if (owner.equals(file.owner()) && !file.inUse()) {
        return entry;
      }
    }

    return null;
  }

  /**
   * Reserves unused cache room for one load. Loads wait rather than spend another load's promise.
   *
   * <p>The loader's size is unknown until preparation finishes. When only resident entries remain,
   * a load may hold less than the entry limit; admission then reclaims evictable entries or serves
   * the file without retaining it. This bounds cache accounting, not the heap used by arbitrary
   * loaders or uncached searches.
   */
  private Reservation reserve(Config config) {
    return reserve(config, null, 0L);
  }

  private Reservation reserve(Config config, ManagedLoad load, long sizeHint) {
    synchronized (mapLock) {
      long requested = expectedEntryBytes(config, sizeHint);
      while (true) {
        if (load != null && !current(load)) {
          return null;
        }
        drainPendingReleases();
        long capacity = budget(config.maxBytes());
        long spare = Math.max(0L, capacity - bytes - reservedBytes);
        if (spare >= requested || reservations.isEmpty()) {
          long held = Math.min(requested, spare);
          Reservation reservation = new Reservation(config.owner(), held, config.maxBytes());
          reservations.add(reservation);
          reservedBytes += held;
          reservedByOwner.merge(config.owner(), held, Long::sum);
          return reservation;
        }

        try {
          mapLock.wait();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException("Interrupted while reserving index cache room", e);
        }
      }
    }
  }

  /**
   * What one entry is expected to cost, so a load can reserve its room without a configured
   * ceiling.
   *
   * <p>The cache has no size to go by before a file is read, and reserving the whole budget would
   * let one load hold the cache while it runs, serialising the loads of a query. What it does know
   * is what the entries it already holds cost, which is the same structure for the same index: the
   * largest of them is the estimate, and until one is resident the whole budget is reserved, which
   * is safe and only costs parallelism on the first load of an executor.
   */
  private long expectedEntryBytes(Config config, long sizeHint) {
    long largest = Math.max(0L, sizeHint);
    for (CachedFile file : protectedFiles.values()) {
      largest = Math.max(largest, file.bytes());
    }
    for (CachedFile file : probation.values()) {
      largest = Math.max(largest, file.bytes());
    }

    return largest > 0L ? largest : config.maxBytes();
  }

  private long budget(long configured) {
    long capacity = configured;
    for (Reservation reservation : reservations) {
      capacity = Math.min(capacity, reservation.budget);
    }
    return capacity;
  }

  private void releaseReservation(Reservation reservation) {
    synchronized (mapLock) {
      if (reservations.remove(reservation)) {
        reservedBytes -= reservation.bytes;
        reservedByOwner.computeIfPresent(
            reservation.owner,
            (owner, held) -> held - reservation.bytes == 0 ? null : held - reservation.bytes);
        mapLock.notifyAll();
      }
    }
  }

  private void release(CachedFile file) {
    bytes -= file.bytes();
    bytesByOwner.computeIfPresent(
        file.owner(), (owner, held) -> held - file.bytes() <= 0 ? null : held - file.bytes());
    entriesByOwner.computeIfPresent(file.owner(), (owner, held) -> held <= 1 ? null : held - 1);
    mapLock.notifyAll();
  }

  private Map.Entry<String, CachedFile> oldestEvictable(
      Map<String, CachedFile> segment, boolean skipPinned) {
    for (Map.Entry<String, CachedFile> entry : segment.entrySet()) {
      CachedFile file = entry.getValue();
      if (ownership.isOwned(entry.getKey()) || (skipPinned && file.pinned)) {
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
      if (ownership.isOwned(entry.getKey())
          || file.lastAccessNanos > deadlineNanos
          || file.inUse()) {
        continue;
      }

      release(file);
      attribution.remove(entry.getKey());
      entries.remove();
      unloaded += 1;
    }

    return unloaded;
  }

  Stats stats() {
    synchronized (mapLock) {
      drainPendingReleases();
      return new Stats(
          hits.get(),
          misses.get(),
          loads.get(),
          evictions.get(),
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

  /** Resets the cache for tests, deferring release of entries that are still in use. */
  void clear() {
    synchronized (mapLock) {
      generation = Math.incrementExact(generation);
      ownership.clear();
      Set<String> keys = new LinkedHashSet<>(probation.keySet());
      keys.addAll(protectedFiles.keySet());
      for (String key : keys) {
        releaseIfUnowned(key);
      }
      asked.clear();
      hits.set(0);
      misses.set(0);
      loads.set(0);
      evictions.set(0);
      lastSweepNanos = System.nanoTime();
      mapLock.notifyAll();
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
    if (preparedBytes <= 0) {
      LOG.debug(
          "Index cache size is estimated: prepared structure did not report its size; "
              + "using {} payload bytes as the fallback",
          payload.length);
    }
    long bytes = IndexCacheMemory.residentBytes(payload.length, preparedBytes);
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
