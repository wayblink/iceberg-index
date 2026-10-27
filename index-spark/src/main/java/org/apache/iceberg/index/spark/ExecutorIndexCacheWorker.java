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

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Executes cache commands on one executor. Loading happens outside the state lock, so a slow
 * prepare never blocks a status report; commands are idempotent by identity and a copy is only
 * released by a command at least as new as the one that installed it.
 */
final class ExecutorIndexCacheWorker implements AutoCloseable {
  /** Reason of a refusal that the driver may retry; the same word is used by the coordinator. */
  static final String BUSY = "BUSY";

  /** A reference is still in use; the driver retries without spending a transient attempt. */
  static final String WAIT_REFERENCES = "WAIT_REFERENCES";

  interface Loader {
    ExecutorIndexCache.CachedFile load(IndexCacheProtocol.Command command);
  }

  private record Installed(long epoch, long bytes) {}

  private enum TargetDecision {
    APPLIED,
    STALE,
    WAIT_REFERENCES
  }

  private final String session;
  private final String executorId;
  private final String incarnation;
  private final ExecutorIndexCache cache;
  private final ExecutorIndexCache.Config cacheConfig;
  private final Loader loader;
  private final IndexCacheWorkerConfig config;
  private final Map<String, IndexCacheProtocol.Ack> completed = new ConcurrentHashMap<>();
  private final Map<String, Installed> installed = new LinkedHashMap<>();
  private final Map<String, Set<String>> targetKeys = new LinkedHashMap<>();
  private final Map<String, Long> targetEpoch = new LinkedHashMap<>();
  private final Map<String, Long> promised = new LinkedHashMap<>();
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final Object stateLock = new Object();
  private final Map<String, Object> locks = new ConcurrentHashMap<>();

  ExecutorIndexCacheWorker(
      String session,
      String executorId,
      String incarnation,
      ExecutorIndexCache cache,
      ExecutorIndexCache.Config cacheConfig,
      Loader loader,
      IndexCacheWorkerConfig config) {
    Preconditions.checkArgument(session != null && !session.isEmpty(), "Session is missing");
    Preconditions.checkArgument(
        executorId != null && !executorId.isEmpty(), "Executor identity is missing");
    Preconditions.checkArgument(
        incarnation != null && !incarnation.isEmpty(), "Executor incarnation is missing");
    this.session = session;
    this.executorId = executorId;
    this.incarnation = incarnation;
    this.cache = Objects.requireNonNull(cache, "Cache is null");
    this.cacheConfig = Objects.requireNonNull(cacheConfig, "Cache config is null");
    this.loader = Objects.requireNonNull(loader, "Loader is null");
    this.config = Objects.requireNonNull(config, "Config is null");
  }

  IndexCacheProtocol.Ack execute(IndexCacheProtocol.Command command) {
    Objects.requireNonNull(command, "Command is null");
    Preconditions.checkArgument(command.file() != null, "Command file is missing");

    if (closed.get()) {
      return reject(command, "CLOSED");
    }

    IndexCacheProtocol.Ack previous = completed.get(command.commandId());
    if (previous != null) {
      return previous;
    }

    // Only retries of the same command serialize; a slow load must not block reports or other
    // commands, so the state lock is taken around bookkeeping and never around the loader.
    Object commandLock = locks.computeIfAbsent(command.commandId(), ignored -> new Object());
    try {
      synchronized (commandLock) {
        previous = completed.get(command.commandId());
        if (previous != null) {
          return previous;
        }

        if (!session.equals(command.session())) {
          return remember(command, reject(command, "STALE_SESSION"));
        }

        if (!incarnation.equals(command.incarnation())) {
          return remember(command, reject(command, "STALE_INCARNATION"));
        }

        IndexCacheProtocol.Ack ack =
            command.type() == IndexCacheProtocol.CommandType.RELEASE
                ? release(command)
                : load(command);
        return remember(command, ack);
      }
    } finally {
      locks.remove(command.commandId(), commandLock);
    }
  }

  private IndexCacheProtocol.Ack remember(
      IndexCacheProtocol.Command command, IndexCacheProtocol.Ack ack) {
    if (!BUSY.equals(ack.reason()) && !WAIT_REFERENCES.equals(ack.reason())) {
      // A transient refusal must stay retryable: caching it would make every re-send a no-op.
      completed.put(command.commandId(), ack);
    }

    return ack;
  }

  /**
   * Makes the cache accept this key for the plan's epoch before the load is admitted.
   *
   * <p>An executor only ever sees individual files, so the target per index is the union of the
   * keys it was told about; files the new plan no longer wants are removed by the explicit release
   * commands, not by guessing a whole target from one command. A delayed older epoch is refused so
   * it cannot roll the target back.
   */
  private TargetDecision ensureTarget(String indexUuid, long epoch, String cacheKey) {
    synchronized (stateLock) {
      Long applied = targetEpoch.get(indexUuid);
      if (applied != null && epoch < applied) {
        return TargetDecision.STALE;
      }

      // Only the first takeover has to wait: later epochs keep the same managed copies valid, so
      // re-applying a target does not invalidate what a query already holds.
      if (applied == null && cache.hasReferencesInUse()) {
        return TargetDecision.WAIT_REFERENCES;
      }

      Set<String> keys = targetKeys.computeIfAbsent(indexUuid, ignored -> new LinkedHashSet<>());
      keys.add(cacheKey);
      if (applied == null || epoch > applied) {
        cache.applyTarget(indexUuid, epoch, keys);
        targetEpoch.put(indexUuid, epoch);
      } else {
        cache.addManagedKeys(indexUuid, epoch, Set.of(cacheKey));
      }

      return TargetDecision.APPLIED;
    }
  }

  private IndexCacheProtocol.Ack load(IndexCacheProtocol.Command command) {
    String cacheKey = command.file().cacheKey();
    Long estimate = command.file().estimatedBytes();
    if (estimate == null) {
      // Unknown size is not zero: without a bound the executor cannot promise the space.
      return reject(command, "SIZE_UNKNOWN");
    }

    if (command.epoch() <= 0) {
      return reject(command, "STALE_EPOCH");
    }

    TargetDecision decision = ensureTarget(command.indexUuid(), command.epoch(), cacheKey);
    if (decision == TargetDecision.STALE) {
      return reject(command, "STALE_EPOCH");
    } else if (decision == TargetDecision.WAIT_REFERENCES) {
      return reject(command, WAIT_REFERENCES);
    }

    // Free space is checked before this command promises its own estimate: a first load must not
    // see its own reservation as if it were already used.
    long free = cacheConfig.maxBytes() - cache.stats().bytes() - reservedBytes();
    if (estimate > free) {
      // Refuse before reading the file: a prepare that cannot be admitted only wastes I/O and the
      // temporary memory the driver is trying to protect.
      return reject(command, BUSY);
    }

    synchronized (stateLock) {
      promised.put(command.commandId(), estimate);
    }

    long startedAt = System.nanoTime();
    RuntimeException last = null;
    for (int attempt = 1; attempt <= config.maxAttempts(); attempt++) {
      try {
        // Admission, reservation, and publication stay in the cache; this worker only owns the
        // control loop and the epoch bookkeeping a release needs. A retry is safe: admission is
        // idempotent for a command and a published copy is reused instead of loaded twice.
        ExecutorIndexCache.CachedFile file =
            cache.installManaged(
                command.indexUuid(),
                command.epoch(),
                cacheKey,
                cacheConfig,
                () -> loader.load(command));
        if (file == null) {
          // Admission refused: either this entry can never fit, or the space is momentarily taken.
          synchronized (stateLock) {
            promised.remove(command.commandId());
          }

          return reject(command, "BUSY");
        }

        synchronized (stateLock) {
          installed.put(cacheKey, new Installed(command.epoch(), file.bytes()));
          promised.remove(command.commandId());
        }

        return ack(command, IndexCacheProtocol.AckStatus.LOADED, file.bytes(), null);
      } catch (RuntimeException e) {
        last = e;
      }
    }

    synchronized (stateLock) {
      promised.remove(command.commandId());
    }

    long elapsedMillis = (System.nanoTime() - startedAt) / 1_000_000L;
    if (elapsedMillis > config.operationTimeoutMillis()) {
      // The driver may already have fenced this plan; reporting the deadline keeps the reason
      // accurate instead of blaming the last read error.
      return ack(command, IndexCacheProtocol.AckStatus.FAILED, 0, "TIMEOUT");
    }

    return ack(command, IndexCacheProtocol.AckStatus.FAILED, 0, message(last));
  }

  private IndexCacheProtocol.Ack release(IndexCacheProtocol.Command command) {
    String cacheKey = command.file().cacheKey();
    Installed copy;
    synchronized (stateLock) {
      copy = installed.get(cacheKey);
    }

    if (copy != null && copy.epoch() > command.epoch()) {
      // A newer load installed this copy; an older release must not delete it.
      return reject(command, "STALE_EPOCH");
    }

    // A release withdraws this index's claim at whatever epoch the executor currently holds, and
    // also removes a pending admission so a load that is still preparing cannot publish afterwards.
    cache.releaseCopy(command.indexUuid(), cacheKey);

    synchronized (stateLock) {
      Set<String> keys = targetKeys.get(command.indexUuid());
      if (keys != null) {
        keys.remove(cacheKey);
      }
    }

    synchronized (stateLock) {
      // The physical copy may still be shared with another index, so only stop reporting it when
      // the cache no longer holds it at all.
      if (!cache.residentKeys().contains(cacheKey)) {
        installed.remove(cacheKey);
      }
    }

    return ack(
        command, IndexCacheProtocol.AckStatus.RELEASED, copy == null ? 0 : copy.bytes(), null);
  }

  /**
   * Builds the report for the driver. It reads only confirmed state, so it answers while a load is
   * still running.
   */
  IndexCacheProtocol.NodeReport report(
      String profile,
      long sequence,
      long budgetBytes,
      long reservedBytes,
      long releasingBytes,
      Map<String, Long> accessAgeNanos) {
    Map<String, Long> holders = holders();
    long used = 0;
    for (long bytes : holders.values()) {
      used = Math.addExact(used, bytes);
    }

    return IndexCacheProtocol.NodeReport.builder()
        .session(session)
        .executorId(executorId)
        .incarnation(incarnation)
        .sequence(sequence)
        .budgetBytes(budgetBytes)
        .usedBytes(used)
        .reservedBytes(reservedBytes)
        .releasingBytes(releasingBytes)
        .holders(holders)
        .profile(profile)
        .accessAgeNanos(accessAgeNanos == null ? Map.of() : accessAgeNanos)
        .build();
  }

  /** Bytes promised to loads that have not finished yet. */
  long reservedBytes() {
    synchronized (stateLock) {
      long reserved = 0;
      for (long bytes : promised.values()) {
        reserved = Math.addExact(reserved, bytes);
      }

      return reserved;
    }
  }

  /**
   * Files this executor still holds, from its own bookkeeping filtered by what the cache really
   * has. A copy evicted by the cache, or released after its last lease closed, stops being
   * reported.
   */
  Map<String, Long> holders() {
    Set<String> resident = cache.residentKeys();
    synchronized (stateLock) {
      Map<String, Long> holders = new LinkedHashMap<>();
      for (Map.Entry<String, Installed> entry : installed.entrySet()) {
        if (resident.contains(entry.getKey())) {
          holders.put(entry.getKey(), entry.getValue().bytes());
        }
      }

      return holders;
    }
  }

  Set<String> residentKeys() {
    return holders().keySet();
  }

  long usedBytes() {
    long used = 0;
    for (long bytes : holders().values()) {
      used = Math.addExact(used, bytes);
    }

    return used;
  }

  IndexCacheWorkerConfig config() {
    return config;
  }

  boolean closed() {
    return closed.get();
  }

  @Override
  public void close() {
    closed.set(true);
    completed.clear();
    synchronized (stateLock) {
      // Nothing will be admitted any more, so the promised space must not keep inflating reports.
      promised.clear();
    }
  }

  /** Acknowledgement for a command that failed outside the loader, for example admission code. */
  IndexCacheProtocol.Ack failure(IndexCacheProtocol.Command command, RuntimeException error) {
    return ack(command, IndexCacheProtocol.AckStatus.FAILED, 0, message(error));
  }

  private IndexCacheProtocol.Ack reject(IndexCacheProtocol.Command command, String reason) {
    return ack(command, IndexCacheProtocol.AckStatus.REJECTED, 0, reason);
  }

  private IndexCacheProtocol.Ack ack(
      IndexCacheProtocol.Command command,
      IndexCacheProtocol.AckStatus status,
      long bytes,
      String reason) {
    return IndexCacheProtocol.Ack.builder()
        .session(session)
        .planId(command.planId())
        .commandId(command.commandId())
        .executorId(executorId)
        .incarnation(incarnation)
        .epoch(command.epoch())
        .sequence(command.sequence())
        .status(status)
        .bytes(bytes)
        .reason(reason)
        .build();
  }

  private static String message(RuntimeException e) {
    String message = e.getMessage();
    return message == null || message.isEmpty() ? e.getClass().getSimpleName() : message;
  }
}
