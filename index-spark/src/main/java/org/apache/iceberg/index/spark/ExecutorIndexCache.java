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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import org.apache.iceberg.index.IndexAdapter;
import org.apache.iceberg.index.IndexFileMemory;
import org.apache.iceberg.index.IndexSearchResult;
import org.apache.iceberg.index.spark.IndexCacheMemory.ChargeProjection;
import org.apache.iceberg.index.spark.IndexCacheMetadata.Component;
import org.apache.iceberg.index.spark.IndexCacheMetadata.Components;
import org.apache.iceberg.index.spark.IndexCacheMetadata.MetadataState;
import org.apache.iceberg.index.spark.IndexCacheMetadata.OwnedMap;
import org.apache.iceberg.index.spark.IndexCacheMetadata.OwnedSet;
import org.apache.iceberg.index.spark.IndexCacheMetadata.Texts;
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

      if (workingSetBytes < 0) {
        // Unknown capacity cannot promise full residency. Legacy queries may still admit a hot
        // entry using the explicitly inexact per-file fallback; managed loads require a bound.
        return Mode.PIN_HOT;
      }
      if (workingSetBytes == 0) {
        return Mode.PIN_ALL;
      }

      long capacity;
      try {
        capacity = Math.multiplyExact(budgetPerExecutor, Math.max(1, executors));
      } catch (ArithmeticException e) {
        capacity = Long.MAX_VALUE;
      }
      if (workingSetBytes <= capacity) {
        return Mode.PIN_ALL;
      } else if (workingSetBytes - capacity <= capacity) {
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
      return parseBytes(setting(spark, "spark.executor.memory", "1g"), "spark.executor.memory");
    }

    private static double heapFraction(org.apache.spark.sql.SparkSession spark) {
      String value = setting(spark, HEAP_FRACTION, "0.25");
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
  static final class CachedFile implements Component, Serializable {
    private final String owner;
    private final IndexAdapter.PreparedSearch prepared;
    private final long bytes;
    private final long preparedBytes;
    private final byte[] context;
    private final ReentrantLock searchLock = new ReentrantLock();
    private int uses;
    // Updated under mapLock; also observed by the search/eviction guards.
    private volatile int leases;
    // Marking a release uses existing wrapper storage, never allocates under budget pressure.
    private boolean releasing;
    private boolean modelInUse;
    // a PIN_ALL entry: the working set fits, so it is never evicted to make room
    private boolean pinned;
    // a 2Q entry: asked for twice, so it outranks an entry seen once
    private boolean protectedRank;
    private long lastAccessNanos = System.nanoTime();

    private CachedFile(String owner, IndexAdapter.PreparedSearch prepared, long bytes) {
      this(owner, prepared, bytes, null, prepared.estimatedBytes());
    }

    private CachedFile(
        String owner,
        IndexAdapter.PreparedSearch prepared,
        long bytes,
        byte[] context,
        long preparedBytes) {
      this.owner = owner;
      this.prepared = prepared;
      this.bytes = bytes;
      this.context = context;
      this.preparedBytes = preparedBytes;
    }

    @Override
    public void charge(Components components) {
      if (components.object(this, 64)) {
        // ReentrantLock: one compressed synchronizer reference (16); its AQS state (32).
        components.object(searchLock, 16 + 32);
        if (context != null) {
          components.object(context, IndexCacheMetadata.align(16L + context.length));
        }
        components.object(prepared, preparedBytes > 0 ? preparedBytes : bytes);
        components.add(owner);
      }
    }

    long preparedBytes() {
      return preparedBytes;
    }

    boolean hasKnownContext() {
      return context != null;
    }

    boolean matchesContext(IndexAdapter adapter) {
      byte[] current = IndexCacheMemory.contextFingerprint(adapter);
      return context != null && current != null && Arrays.equals(context, current);
    }

    CachedFile withContext(IndexAdapter adapter) {
      return new CachedFile(
          owner,
          prepared,
          bytes,
          preparedBytes > 0 ? IndexCacheMemory.contextFingerprint(adapter) : null,
          preparedBytes);
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
      if (modelInUse || leases > 0 || searchLock.isHeldByCurrentThread()) {
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
      return new CachedFile(newOwner, prepared, bytes, context, preparedBytes);
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
  private Texts texts = new Texts();
  private OwnedMap<String, CachedFile> probation = new OwnedMap<>(true);
  private OwnedMap<String, CachedFile> protectedFiles = new OwnedMap<>(true);
  // how often a file has been asked for without being kept, for the PIN_HOT admission
  private OwnedMap<String, Integer> asked = new OwnedMap<>(true);
  private final Object mapLock = new Object();
  private IndexCacheOwnership ownership = new IndexCacheOwnership(texts);
  // Observational associations do not confer residency ownership or protection.
  private OwnedMap<String, Set<String>> attribution = new OwnedMap<>();
  private long metadataBudget = Long.MAX_VALUE;
  private long generation;
  private long chargeSequence;
  // Registered waiters keep the same monitor alive until every caller has left.
  private OwnedMap<String, LoadMonitor> loading = new OwnedMap<>();

  private static final class LoadMonitor implements Component, Serializable {
    private int callers;
    private String owner;

    @Override
    public void charge(Components components) {
      if (components.object(this, 24)) {
        components.add(owner);
      }
    }
  }

  private record ManagedLoad(String indexUuid, long epoch, String cacheKey, long generation) {}

  private final AtomicLong hits = new AtomicLong();
  private final AtomicLong misses = new AtomicLong();
  private final AtomicLong loads = new AtomicLong();
  private final AtomicLong evictions = new AtomicLong();
  private long bytes = 0;

  // room promised to loads in flight, and the budget those promises are against
  private long reservedBytes = 0;
  private OwnedSet<Reservation> reservations = new OwnedSet<>();
  private OwnedMap<String, Long> reservedByOwner = new OwnedMap<>();

  private static final class Reservation implements Component, Serializable {
    private String owner;
    private String cacheKey;
    private long bytes;
    private final long budget;

    @Override
    public void charge(Components components) {
      if (components.object(this, 40)) {
        components.add(owner);
        components.add(cacheKey);
      }
    }

    private Reservation(String owner, long bytes, long budget) {
      this.owner = owner;
      this.bytes = bytes;
      this.budget = budget;
    }
  }

  // what each table holds, so a share of the budget can be reserved per table
  private OwnedMap<String, Long> bytesByOwner = new OwnedMap<>();
  private OwnedMap<String, Integer> entriesByOwner = new OwnedMap<>();
  private long lastSweepNanos = System.nanoTime();
  private static long sweepIntervalNanos = java.util.concurrent.TimeUnit.SECONDS.toNanos(30);

  /** Lets a test drive the idle sweep without waiting half a minute. */
  static void sweepIntervalForTests(long nanos) {
    sweepIntervalNanos = nanos;
  }

  private ExecutorIndexCache() {}

  private record State(
      Texts texts,
      OwnedMap<String, CachedFile> probation,
      OwnedMap<String, CachedFile> protectedFiles,
      OwnedMap<String, Integer> asked,
      IndexCacheOwnership ownership,
      OwnedMap<String, Set<String>> attribution,
      OwnedMap<String, LoadMonitor> loading,
      OwnedSet<Reservation> reservations,
      OwnedMap<String, Long> reservedByOwner,
      OwnedMap<String, Long> bytesByOwner,
      OwnedMap<String, Integer> entriesByOwner,
      long reservedBytes,
      long bytes)
      implements Serializable {}

  private State state() {
    return new State(
        texts,
        probation,
        protectedFiles,
        asked,
        ownership,
        attribution,
        loading,
        reservations,
        reservedByOwner,
        bytesByOwner,
        entriesByOwner,
        reservedBytes,
        bytes);
  }

  private void restore(State state) {
    this.texts = state.texts();
    this.probation = state.probation();
    this.protectedFiles = state.protectedFiles();
    this.asked = state.asked();
    this.ownership = state.ownership();
    this.attribution = state.attribution();
    this.loading = state.loading();
    this.reservations = state.reservations();
    this.reservedByOwner = state.reservedByOwner();
    this.bytesByOwner = state.bytesByOwner();
    this.entriesByOwner = state.entriesByOwner();
    this.reservedBytes = state.reservedBytes();
    this.bytes = state.bytes();
  }

  private void detachState() {
    texts = texts.copy();
    probation = new OwnedMap<>(probation);
    protectedFiles = new OwnedMap<>(protectedFiles);
    asked = new OwnedMap<>(asked);
    ownership = ownership.copy(texts);
    attribution = new OwnedMap<>(attribution);
    attribution.replaceAll((key, indexes) -> new OwnedSet<>(indexes));
    loading = new OwnedMap<>(loading);
    reservations = new OwnedSet<>(reservations);
    reservedByOwner = new OwnedMap<>(reservedByOwner);
    bytesByOwner = new OwnedMap<>(bytesByOwner);
    entriesByOwner = new OwnedMap<>(entriesByOwner);
  }

  /** All growing mutations publish one capacity-visible after-state under mapLock. */
  private boolean admit(long capacity, Runnable mutation) {
    return admit(capacity, mutation, false, ignored -> {});
  }

  private record OwnerCharges(Map<String, Long> effectiveBytes, long controlBytes) {}

  private record AdmissionDelta(
      Map<String, Long> beforeOwners,
      Map<String, Long> afterOwners,
      long beforeEffective,
      long afterEffective,
      long controlBytes) {
    long ownerIncrement(String owner) {
      return afterOwners.getOrDefault(owner, 0L) - beforeOwners.getOrDefault(owner, 0L);
    }
  }

  private OwnerCharges ownerCharges() {
    Components components = new Components();
    Map<String, Long> charges = new LinkedHashMap<>();
    Set<String> owners = new LinkedHashSet<>(entriesByOwner.keySet());
    owners.addAll(reservedByOwner.keySet());
    loading.values().forEach(monitor -> owners.add(monitor.owner));
    for (String owner : owners) {
      long before = components.bytes();
      Set<String> keys = new LinkedHashSet<>();
      probation.forEach(
          (key, file) -> {
            if (owner.equals(file.owner())) {
              keys.add(key);
            }
          });
      protectedFiles.forEach(
          (key, file) -> {
            if (owner.equals(file.owner())) {
              keys.add(key);
            }
          });
      probation.chargeEntries(components, keys);
      protectedFiles.chargeEntries(components, keys);
      attribution.chargeEntries(components, keys);
      bytesByOwner.chargeEntries(components, Set.of(owner));
      entriesByOwner.chargeEntries(components, Set.of(owner));
      Set<String> loadingKeys = new LinkedHashSet<>();
      loading.forEach(
          (key, monitor) -> {
            if (owner.equals(monitor.owner)) {
              loadingKeys.add(key);
            }
          });
      loading.chargeEntries(components, loadingKeys);
      Set<Reservation> promises = new LinkedHashSet<>();
      for (Reservation promise : reservations) {
        if (owner.equals(promise.owner)) {
          promises.add(promise);
        }
      }
      reservations.chargeEntries(components, promises);
      reservedByOwner.chargeEntries(components, Set.of(owner));
      texts.chargeResident(components);
      charges.put(
          owner,
          Math.addExact(components.bytes() - before, reservedByOwner.getOrDefault(owner, 0L)));
    }
    long resident = components.bytes();
    components.root(ownership);
    components.root(asked);
    texts.charge(components);
    return new OwnerCharges(charges, components.bytes() - resident);
  }

  private boolean admit(
      long capacity, Runnable mutation, boolean shrinking, Consumer<AdmissionDelta> increment) {
    State before = state();
    MetadataState previous = metadataState(texts, attribution);
    OwnerCharges beforeCharges = ownerCharges();
    detachState();
    try {
      mutation.run();
      ChargeProjection projection =
          IndexCacheMemory.project(previous, ignored -> metadataState(texts, attribution));
      long previousEffective =
          IndexCacheMemory.project(previous, unchanged -> unchanged).effectiveBytes();
      long afterEffective = projection.effectiveBytes();
      texts.replace(projection.afterState().texts());
      OwnerCharges afterCharges = ownerCharges();
      increment.accept(
          new AdmissionDelta(
              beforeCharges.effectiveBytes(),
              afterCharges.effectiveBytes(),
              previousEffective,
              afterEffective,
              afterCharges.controlBytes()));
      if (afterEffective > capacity && (!shrinking || afterEffective > previousEffective)) {
        restore(before);
        return false;
      }
      texts.replace(projection.afterState().texts());
      return true;
    } catch (ArithmeticException e) {
      restore(before);
      return false;
    } catch (RuntimeException e) {
      restore(before);
      throw e;
    }
  }

  private record SizedPrepared(long estimatedBytes)
      implements IndexAdapter.PreparedSearch, Serializable {
    @Override
    public List<IndexSearchResult> search(ByteBuffer query, int topK) {
      throw new UnsupportedOperationException("Metadata shape is not a searcher");
    }
  }

  /**
   * Detached capacity-visible roots, with prepared state replaced by identity-preserving quotes.
   */
  static final class Shape implements Serializable {
    private final State state;
    private final ChargeSnapshot charge;
    private final String runtime;
    private final Set<String> pendingLoads;

    private Shape(State state, ChargeSnapshot charge) {
      this(state, charge, Set.of());
    }

    private Shape(State state, ChargeSnapshot charge, Set<String> pendingLoads) {
      this.state = state;
      this.charge = charge;
      this.runtime = IndexCacheMemory.runtimeIdentity();
      this.pendingLoads = Set.copyOf(pendingLoads);
    }

    boolean compatibleRuntime() {
      return charge.known()
          && runtime.equals(IndexCacheMemory.runtimeIdentity())
          && IndexCacheMemory.supportedLayout();
    }

    Shape unknownCharge() {
      return new Shape(
          state,
          new ChargeSnapshot(
              charge.version(),
              false,
              charge.usedBytes(),
              charge.residentBytes(),
              charge.controlBytes(),
              charge.reservedBytes(),
              charge.releasingBytes(),
              charge.holders(),
              charge.groupRefs()),
          pendingLoads);
    }

    ChargeSnapshot charge() {
      return charge;
    }

    static Shape empty() {
      return new ExecutorIndexCache().shape();
    }

    Shape project(String indexUuid, long epoch, List<IndexCacheProtocol.File> files) {
      Preconditions.checkArgument(compatibleRuntime(), "Index cache runtime profile is unknown");
      ExecutorIndexCache model = new ExecutorIndexCache();
      model.restore(state);
      model.detachModel();
      Set<String> keys = new LinkedHashSet<>();
      for (IndexCacheProtocol.File file : files) {
        Preconditions.checkArgument(
            file.owner() != null && !file.owner().isBlank(), "Index owner is unknown");
        IndexFileMemory memory = file.memory();
        Preconditions.checkArgument(
            memory != null && memory.modelVersion() == 2 && memory.preparedBytes() > 0,
            "Index memory is unknown");
        keys.add(file.cacheKey());
        if (model.find(file.cacheKey()) == null) {
          for (Reservation reservation : new ArrayList<>(model.reservations)) {
            if (file.cacheKey().equals(reservation.cacheKey)) {
              model.releaseReservation(reservation);
            }
          }
          String owner = model.texts.own(file.owner());
          CachedFile retained =
              new CachedFile(
                  owner,
                  new SizedPrepared(memory.preparedBytes()),
                  IndexCacheMemory.residentBytes(memory),
                  new byte[32],
                  memory.preparedBytes());
          retained.pinned = true;
          model.protectedFiles.put(model.texts.own(file.cacheKey()), retained);
          model.bytes += retained.bytes();
          model.bytesByOwner.merge(owner, retained.bytes(), Long::sum);
          model.entriesByOwner.merge(owner, 1, Integer::sum);
        }
      }
      if (indexUuid != null) {
        if (model.ownership.hasTarget(indexUuid)) {
          Set<String> existing = new LinkedHashSet<>(model.ownership.keys(indexUuid));
          existing.addAll(keys);
          model.ownership.applyTarget(indexUuid, epoch, existing);
          model.ownership.addKeys(indexUuid, epoch, keys);
        } else {
          model.ownership.applyTarget(indexUuid, epoch, keys);
        }
      }
      ChargeSnapshot next = model.chargeSnapshot();
      return new Shape(
          model.state(),
          new ChargeSnapshot(
              next.version(),
              charge.known() && next.known(),
              next.usedBytes(),
              next.residentBytes(),
              next.controlBytes(),
              next.reservedBytes(),
              next.releasingBytes(),
              next.holders(),
              next.groupRefs()),
          pendingLoads);
    }

    Shape projectAdmission(String indexUuid, long epoch, List<IndexCacheProtocol.File> files) {
      Set<String> pending = new LinkedHashSet<>(pendingLoads);
      for (IndexCacheProtocol.File file : files) {
        if (!state.probation().containsKey(file.cacheKey())
            && !state.protectedFiles().containsKey(file.cacheKey())) {
          pending.add(file.cacheKey());
        }
      }
      Shape after = project(indexUuid, epoch, files);
      long peak = after.effectiveAdmissionBytes(pending);
      ChargeSnapshot charge = after.charge();
      return new Shape(
          after.state,
          new ChargeSnapshot(
              charge.version(),
              charge.known(),
              charge.usedBytes(),
              charge.residentBytes(),
              charge.controlBytes(),
              peak - charge.usedBytes(),
              charge.releasingBytes(),
              charge.holders(),
              charge.groupRefs()),
          pending);
    }

    /** Peak metadata for the specified cold publications; no prepared estimate is re-invoked. */
    long effectiveAdmissionBytes(Set<String> keys) {
      ExecutorIndexCache model = new ExecutorIndexCache();
      model.restore(state);
      model.detachModel();
      for (String key : keys) {
        CachedFile file = model.find(key);
        if (file != null && !model.loading.containsKey(key)) {
          LoadMonitor monitor = new LoadMonitor();
          monitor.owner = file.owner();
          monitor.callers = 1;
          model.loading.put(model.texts.own(key), monitor);
        }
      }
      ChargeSnapshot publication = model.chargeSnapshot();
      for (String key : keys) {
        CachedFile file = model.find(key);
        if (file == null) {
          continue;
        }
        model.protectedFiles.remove(key);
        model.probation.remove(key);
        model.attribution.remove(key);
        model.bytes -= file.bytes();
        model.bytesByOwner.computeIfPresent(
            file.owner(), (owner, held) -> held - file.bytes() <= 0 ? null : held - file.bytes());
        model.entriesByOwner.computeIfPresent(
            file.owner(), (owner, held) -> held <= 1 ? null : held - 1);
        boolean promised =
            model.reservations.stream().anyMatch(reservation -> key.equals(reservation.cacheKey));
        if (!promised) {
          Reservation reservation = new Reservation(file.owner(), file.bytes(), Long.MAX_VALUE);
          reservation.cacheKey = model.texts.own(key);
          model.reservations.add(reservation);
          model.reservedByOwner.merge(file.owner(), file.bytes(), Long::sum);
        }
      }
      ChargeSnapshot preparation = model.chargeSnapshot();
      return Math.addExact(
          Math.max(publication.usedBytes(), preparation.usedBytes()), state.reservedBytes());
    }

    Shape releaseKeys(String indexUuid, Set<String> keys) {
      ExecutorIndexCache model = new ExecutorIndexCache();
      model.restore(state);
      model.detachModel();
      for (String key : keys) {
        model.releaseCopy(indexUuid, key);
      }
      return new Shape(model.state(), model.chargeSnapshot());
    }

    Map<String, Long> holders() {
      Set<String> keys = new LinkedHashSet<>(state.probation().keySet());
      keys.addAll(state.protectedFiles().keySet());
      Map<String, Long> result = new LinkedHashMap<>();
      keys.forEach(key -> result.put(key, residentBytes(Set.of(key))));
      return result;
    }

    long residentBytes(Set<String> keys) {
      Components components = new Components();
      state.probation().chargeEntries(components, keys);
      state.protectedFiles().chargeEntries(components, keys);
      state.attribution().chargeEntries(components, keys);
      Set<String> owners = new LinkedHashSet<>();
      for (String key : keys) {
        CachedFile file = state.probation().peek(key);
        if (file == null) {
          file = state.protectedFiles().peek(key);
        }
        if (file != null) {
          owners.add(file.owner());
        }
      }
      state.bytesByOwner().chargeEntries(components, owners);
      state.entriesByOwner().chargeEntries(components, owners);
      state.texts().chargeResident(components);
      return components.bytes();
    }
  }

  private void detachModel() {
    detachState();
    IdentityHashMap<IndexAdapter.PreparedSearch, SizedPrepared> prepared = new IdentityHashMap<>();
    probation.replaceAll((key, file) -> modeled(file, prepared));
    protectedFiles.replaceAll((key, file) -> modeled(file, prepared));
  }

  Shape shape() {
    synchronized (mapLock) {
      ChargeSnapshot charge = chargeSnapshot();
      ExecutorIndexCache model = new ExecutorIndexCache();
      model.restore(state());
      model.detachState();
      IdentityHashMap<IndexAdapter.PreparedSearch, SizedPrepared> prepared =
          new IdentityHashMap<>();
      model.probation.replaceAll((key, file) -> modeled(file, prepared));
      model.protectedFiles.replaceAll((key, file) -> modeled(file, prepared));
      return new Shape(model.state(), charge);
    }
  }

  private static CachedFile modeled(
      CachedFile file, IdentityHashMap<IndexAdapter.PreparedSearch, SizedPrepared> prepared) {
    SizedPrepared quote =
        prepared.computeIfAbsent(
            file.prepared,
            ignored -> new SizedPrepared(file.preparedBytes > 0 ? file.preparedBytes : file.bytes));
    CachedFile copy =
        new CachedFile(file.owner, quote, file.bytes, file.context, file.preparedBytes);
    copy.pinned = file.pinned;
    copy.releasing = file.releasing;
    copy.leases = file.leases;
    copy.modelInUse = file.inUse();
    return copy;
  }

  void configureBudget(long capacity) {
    synchronized (mapLock) {
      metadataBudget = capacity;
    }
  }

  long managedEpoch(String indexUuid) {
    synchronized (mapLock) {
      return ownership.epoch(indexUuid);
    }
  }

  Set<String> managedKeys(String indexUuid) {
    synchronized (mapLock) {
      return ownership.keys(indexUuid);
    }
  }

  boolean mayAdmit(String indexUuid, long epoch, String cacheKey) {
    synchronized (mapLock) {
      return ownership.mayAdmit(indexUuid, epoch, cacheKey);
    }
  }

  private long usedBytes() {
    ChargeProjection projection =
        IndexCacheMemory.project(metadataState(texts, attribution), before -> before);
    texts.replace(projection.afterState().texts());
    return projection.usedBytes();
  }

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
      metadataBudget = config.maxBytes();
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

    LoadMonitor monitor = loadingMonitor(cacheKey, config.owner());
    if (monitor == null) {
      return loader.load();
    }
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

        Reservation reservation = reserve(config, null, sizeHint, cacheKey);
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
      Texts afterTexts = texts.copy();
      OwnedMap<String, Set<String>> afterAttribution = new OwnedMap<>(attribution);
      for (String key : cacheKeys) {
        if (probation.containsKey(key) || protectedFiles.containsKey(key)) {
          Set<String> indexes = new OwnedSet<>(afterAttribution.getOrDefault(key, Set.of()));
          indexes.add(afterTexts.own(indexUuid));
          afterAttribution.put(afterTexts.own(key), indexes);
        }
      }
      ChargeProjection projection =
          IndexCacheMemory.project(
              metadataState(texts, attribution),
              before -> metadataState(afterTexts, afterAttribution));
      try {
        if (projection.incrementBytes() > 0
            && projection.effectiveBytes() > budget(metadataBudget)) {
          return;
        }
      } catch (ArithmeticException e) {
        return;
      }
      attribution = afterAttribution;
      texts.replace(projection.afterState().texts());
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
      if (!admit(
          budget(metadataBudget), () -> ownership.applyTarget(indexUuid, epoch, cacheKeys),
          previous.containsAll(cacheKeys), ignored -> {})) {
        return;
      }
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

    loader.validate();
    ManagedLoad load;
    synchronized (mapLock) {
      metadataBudget = config.maxBytes();
      load = new ManagedLoad(indexUuid, epoch, cacheKey, generation);
      if (!current(load)) {
        return null;
      }
      CachedFile existing = find(cacheKey);
      if (existing != null) {
        loader.validate(existing);
        if (!canReuse(existing, config, null)) {
          return null;
        }
        existing.releasing = false;
        return existing;
      }
    }

    LoadMonitor monitor = loadingMonitor(cacheKey, config.owner());
    if (monitor == null) {
      return null;
    }
    synchronized (monitor) {
      try {
        synchronized (mapLock) {
          if (!current(load)) {
            return null;
          }
          CachedFile existing = find(cacheKey);
          if (existing != null) {
            loader.validate(existing);
            if (!canReuse(existing, config, null)) {
              return null;
            }
            existing.releasing = false;
            return existing;
          }
        }
        Reservation reservation = reserve(config, load, sizeHint, cacheKey);
        if (reservation == null) {
          return null;
        }
        try {
          CachedFile loaded = loader.load();
          loader.validate(loaded);
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
      admit(budget(metadataBudget), () -> ownership.addKeys(indexUuid, epoch, cacheKeys));
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
      if (!ownership.hasTarget(indexUuid)) {
        return;
      }
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
          if (released.releasing) {
            releaseIfUnowned(cacheKey);
          }
          mapLock.notifyAll();
        }
      }
    }
  }

  private LoadMonitor loadingMonitor(String cacheKey, String owner) {
    synchronized (mapLock) {
      LoadMonitor monitor = loading.get(cacheKey);
      if (monitor == null) {
        LoadMonitor created = new LoadMonitor();
        if (!admit(
            budget(metadataBudget),
            () -> {
              created.owner = texts.own(owner);
              loading.put(texts.own(cacheKey), created);
            })) {
          return null;
        }
        monitor = created;
      }
      monitor.callers = Math.incrementExact(monitor.callers);
      return monitor;
    }
  }

  private void releaseLoadingMonitor(String cacheKey, LoadMonitor monitor) {
    synchronized (mapLock) {
      LoadMonitor existing = loading.get(cacheKey);
      Preconditions.checkState(existing == monitor, "Index loading monitor changed: %s", cacheKey);
      existing.callers -= 1;
      if (existing.callers == 0) {
        loading.remove(cacheKey);
      }
    }
  }

  private boolean current(ManagedLoad load) {
    return generation == load.generation()
        && ownership.mayAdmit(load.indexUuid(), load.epoch(), load.cacheKey());
  }

  private boolean canReuse(CachedFile file, Config config, Reservation reservation) {
    long otherReservations = reservedBytes - (reservation == null ? 0 : reservation.bytes);
    return usedBytes() <= budget(config.maxBytes()) - otherReservations;
  }

  private CachedFile find(String cacheKey) {
    CachedFile file = protectedFiles.get(cacheKey);
    return file == null ? probation.get(cacheKey) : file;
  }

  private void releaseIfUnowned(String cacheKey) {
    CachedFile file = find(cacheKey);
    if (file == null) {
      return;
    }
    if (ownership.isOwned(cacheKey)) {
      file.releasing = false;
    } else if (file.inUse()) {
      file.releasing = true;
    } else {
      protectedFiles.remove(cacheKey);
      probation.remove(cacheKey);
      attribution.remove(cacheKey);
      release(file);
    }
  }

  private void drainPendingReleases() {
    List<String> keys = new ArrayList<>();
    probation.forEach(
        (key, file) -> {
          if (file.releasing) {
            keys.add(key);
          }
        });
    protectedFiles.forEach(
        (key, file) -> {
          if (file.releasing) {
            keys.add(key);
          }
        });
    keys.forEach(this::releaseIfUnowned);
  }

  private boolean askedBefore(String cacheKey) {
    synchronized (mapLock) {
      int previous = asked.getOrDefault(cacheKey, 0);
      boolean accepted =
          admit(
              budget(metadataBudget),
              () -> {
                asked.put(texts.own(cacheKey), Math.incrementExact(previous));
                if (asked.size() > 100_000) {
                  Iterator<String> oldest = asked.keySet().iterator();
                  oldest.next();
                  oldest.remove();
                }
              });
      return accepted && previous > 0;
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
        CachedFile retained = file;
        admit(
            budget(metadataBudget),
            () -> {
              probation.remove(cacheKey);
              protectedFiles.put(texts.own(cacheKey), retained);
            });
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
      if (reservation == null || file.bytes() > budget(config.maxBytes())) {
        return file;
      }
      long capacity = budget(config.maxBytes());
      CachedFile stamped = file.withOwner(config.owner());
      stamped.pinned = config.mode() == Mode.PIN_ALL;
      AdmissionDelta[] projected = new AdmissionDelta[1];
      while (!admit(
          capacity,
          () -> {
            releaseReservation(reservation);
            String ownedKey = texts.own(cacheKey);
            CachedFile retained = stamped.withOwner(texts.own(config.owner()));
            retained.pinned = stamped.pinned;
            if (retained.pinned) {
              protectedFiles.put(ownedKey, retained);
            } else {
              probation.put(ownedKey, retained);
            }
            bytes += retained.bytes();
            bytesByOwner.merge(retained.owner(), retained.bytes(), Long::sum);
            entriesByOwner.merge(retained.owner(), 1, Integer::sum);
          },
          false,
          delta -> projected[0] = delta)) {
        if (projected[0] == null) {
          return file;
        }
        Map.Entry<String, CachedFile> victim = mostOverQuota(capacity, projected[0]);
        if (victim == null) {
          victim = oldestEvictable(probation, false);
        }
        if (victim == null) {
          victim = oldestEvictable(protectedFiles, true);
        }
        if (victim == null) {
          LOG.debug("Not caching {}: no room within the {} byte budget", cacheKey, capacity);
          return file;
        }
        release(victim.getValue());
        attribution.remove(victim.getKey());
        protectedFiles.remove(victim.getKey());
        probation.remove(victim.getKey());
        if (evictions.incrementAndGet() == 1 && hits.get() == 0) {
          LOG.warn(
              "The index cache evicted an entry before serving a hit: budget {} bytes", capacity);
        }
      }
      return find(cacheKey);
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
      long fairnessBudget, AdmissionDelta projection) {
    Map<String, Long> occupied = new LinkedHashMap<>(projection.beforeOwners());
    for (String owner : projection.afterOwners().keySet()) {
      occupied.put(
          owner, Math.addExact(occupied.getOrDefault(owner, 0L), projection.ownerIncrement(owner)));
    }
    occupied.keySet().retainAll(projection.afterOwners().keySet());
    int owners = Math.max(1, occupied.size());
    long quota = Math.max(0L, fairnessBudget - projection.controlBytes()) / owners;
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
  private Reservation reserve(Config config, ManagedLoad load, long sizeHint, String cacheKey) {
    synchronized (mapLock) {
      long requested = expectedEntryBytes(config, sizeHint);
      while (true) {
        if (load != null && !current(load)) {
          return null;
        }
        drainPendingReleases();
        long capacity = budget(config.maxBytes());
        long spare = Math.max(0L, capacity - usedBytes() - reservedBytes);
        if (spare >= requested || reservations.isEmpty()) {
          Reservation reservation = new Reservation(config.owner(), 0, config.maxBytes());
          boolean accepted =
              admit(
                  capacity,
                  () -> {
                    String owner = texts.own(config.owner());
                    reservation.owner = owner;
                    reservation.cacheKey = texts.own(cacheKey);
                    reservations.add(reservation);
                    reservedByOwner.merge(owner, 0L, Long::sum);
                    long available = Math.max(0L, capacity - usedBytes() - reservedBytes);
                    reservation.bytes = Math.min(requested, available);
                    reservedBytes += reservation.bytes;
                    reservedByOwner.merge(owner, reservation.bytes, Long::sum);
                  });
          return accepted ? reservation : null;
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

  record ChargeSnapshot(
      long version,
      boolean known,
      long usedBytes,
      long residentBytes,
      long controlBytes,
      long reservedBytes,
      long releasingBytes,
      int holders,
      int groupRefs)
      implements Serializable {}

  private MetadataState metadataState(
      Texts ownedTexts, OwnedMap<String, Set<String>> associations) {
    return new MetadataState(
        List.of(
            probation,
            protectedFiles,
            associations,
            bytesByOwner,
            entriesByOwner,
            loading,
            reservations,
            reservedByOwner),
        List.of(ownership, asked),
        ownedTexts,
        reservedBytes);
  }

  ChargeSnapshot chargeSnapshot() {
    synchronized (mapLock) {
      drainPendingReleases();
      ChargeProjection projection =
          IndexCacheMemory.project(metadataState(texts, attribution), before -> before);
      texts.replace(projection.afterState().texts());
      Components releasing = new Components();
      probation.forEach(
          (key, file) -> {
            if (file.releasing) {
              releasing.add(key);
              releasing.add(file);
            }
          });
      protectedFiles.forEach(
          (key, file) -> {
            if (file.releasing) {
              releasing.add(key);
              releasing.add(file);
            }
          });
      int holders = 0;
      boolean known = IndexCacheMemory.supportedLayout();
      for (CachedFile file : probation.values()) {
        known &= file.hasKnownContext() && file.owner() != null && !file.owner().isBlank();
        holders = Math.addExact(holders, file.leases);
      }
      for (CachedFile file : protectedFiles.values()) {
        known &= file.hasKnownContext() && file.owner() != null && !file.owner().isBlank();
        holders = Math.addExact(holders, file.leases);
      }
      // Snapshot sequence is monotonic even across clear; it is not the controller policy epoch.
      chargeSequence = Math.incrementExact(chargeSequence);
      return new ChargeSnapshot(
          chargeSequence,
          known,
          projection.usedBytes(),
          projection.residentBytes(),
          projection.controlBytes(),
          reservedBytes,
          releasing.bytes(),
          holders,
          ownership.groupRefs());
    }
  }

  Stats stats() {
    synchronized (mapLock) {
      drainPendingReleases();
      return new Stats(
          hits.get(),
          misses.get(),
          loads.get(),
          evictions.get(),
          chargeSnapshot().residentBytes(),
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
    synchronized (mapLock) {
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
              System.currentTimeMillis())
          .withMetadata(shape());
    }
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
      metadataBudget = Long.MAX_VALUE;
      chargeSnapshot();
      hits.set(0);
      misses.set(0);
      loads.set(0);
      evictions.set(0);
      lastSweepNanos = System.nanoTime();
      mapLock.notifyAll();
    }
  }

  /** Produces the independent state prepared from an index file. */
  interface Loader {
    CachedFile load();

    /**
     * Control readers validate before both reuse and payload I/O. Policy-only loaders may opt out.
     */
    default void validate() {}

    /** A positive policy fixture charge is not a reader/model proof. */
    default void validate(CachedFile file) {}
  }

  /**
   * Keeps only prepared state. Input bytes contribute to the explicitly inexact legacy fallback
   * when the searcher cannot report its retained memory, but the cache does not retain that input.
   */
  static CachedFile loaded(byte[] payload, IndexAdapter.PreparedSearch prepared) {
    return loaded(payload.length, prepared);
  }

  static CachedFile loaded(long inputBytes, IndexAdapter.PreparedSearch prepared) {
    long preparedBytes = prepared.estimatedBytes();
    if (preparedBytes <= 0 || !IndexCacheMemory.supportedLayout()) {
      LOG.debug(
          "Index cache size is estimated: prepared size is unknown or the wrapper JVM layout "
              + "is unsupported; using {} input bytes as the inexact fallback",
          inputBytes);
    }
    long bytes = IndexCacheMemory.residentBytes(inputBytes, preparedBytes);
    return new CachedFile("", prepared, bytes, null, preparedBytes);
  }

  static CachedFile loaded(byte[] payload, IndexAdapter.PreparedSearch prepared, long bytes) {
    return new CachedFile("", prepared, bytes);
  }

  Set<String> residentKeys() {
    synchronized (mapLock) {
      Set<String> keys = new java.util.LinkedHashSet<>(probation.keySet());
      keys.addAll(protectedFiles.keySet());
      return keys;
    }
  }
}
