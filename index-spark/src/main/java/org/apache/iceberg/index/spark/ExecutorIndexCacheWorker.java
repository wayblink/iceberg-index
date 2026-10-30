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

import java.util.LinkedHashSet;
import java.util.List;
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

    default void validate(IndexCacheProtocol.Command command) {}

    default void validate(IndexCacheProtocol.Command command, ExecutorIndexCache.CachedFile file) {}
  }

  private enum TargetDecision {
    APPLIED,
    STALE,
    WAIT_REFERENCES,
    BUSY
  }

  private final String session;
  private final String executorId;
  private final String incarnation;
  private final ExecutorIndexCache cache;
  private final ExecutorIndexCache.Config cacheConfig;
  private final Loader loader;
  private final IndexCacheWorkerConfig config;
  private final Map<String, IndexCacheProtocol.Ack> completed = new ConcurrentHashMap<>();
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
    cache.configureBudget(cacheConfig.maxBytes());
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
      return withCurrentCharge(previous);
    }

    // Only retries of the same command serialize; a slow load must not block reports or other
    // commands, so the state lock is taken around bookkeeping and never around the loader.
    Object commandLock = locks.computeIfAbsent(command.commandId(), ignored -> new Object());
    try {
      synchronized (commandLock) {
        previous = completed.get(command.commandId());
        if (previous != null) {
          return withCurrentCharge(previous);
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

  /** A repeated answer states the cache as it is now, because the first one may be stale. */
  private IndexCacheProtocol.Ack withCurrentCharge(IndexCacheProtocol.Ack previous) {
    return previous
        .copy()
        .chargeVersion(cache.chargeVersion())
        .controlBytes(cache.controlBytes())
        .build();
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
      long applied = cache.managedEpoch(indexUuid);
      if (applied >= 0 && epoch < applied) {
        return TargetDecision.STALE;
      }

      // Only the first takeover has to wait: later epochs keep the same managed copies valid, so
      // re-applying a target does not invalidate what a query already holds.
      if (applied < 0 && cache.hasReferencesInUse()) {
        return TargetDecision.WAIT_REFERENCES;
      }

      Set<String> keys = new LinkedHashSet<>(cache.managedKeys(indexUuid));
      keys.add(cacheKey);
      if (applied < 0 || epoch > applied) {
        cache.applyTarget(indexUuid, epoch, keys);
      } else {
        cache.addManagedKeys(indexUuid, epoch, Set.of(cacheKey));
      }
      if (!cache.mayAdmit(indexUuid, epoch, cacheKey)) {
        return TargetDecision.BUSY;
      }
      return TargetDecision.APPLIED;
    }
  }

  private IndexCacheProtocol.Ack load(IndexCacheProtocol.Command command) {
    String cacheKey = command.file().cacheKey();
    Long estimate = command.file().estimatedBytes();
    if (estimate == null
        || command.file().owner() == null
        || command.file().owner().isBlank()
        || command.file().memory() == null) {
      // Unknown size is not zero: without a bound the executor cannot promise the space.
      return reject(command, "SIZE_UNKNOWN");
    }

    if (command.epoch() <= 0) {
      return reject(command, "STALE_EPOCH");
    }

    try {
      ExecutorIndexCache.Shape projected =
          cache
              .shape()
              .projectAdmission(command.indexUuid(), command.epoch(), List.of(command.file()));
      if (projected.charge().usedBytes() + projected.charge().reservedBytes()
          > cacheConfig.maxBytes()) {
        return reject(command, BUSY);
      }
    } catch (RuntimeException e) {
      return reject(command, "SIZE_UNKNOWN");
    }
    TargetDecision decision = ensureTarget(command.indexUuid(), command.epoch(), cacheKey);
    if (decision == TargetDecision.STALE) {
      return reject(command, "STALE_EPOCH");
    } else if (decision == TargetDecision.WAIT_REFERENCES) {
      return reject(command, WAIT_REFERENCES);
    } else if (decision == TargetDecision.BUSY) {
      return reject(command, BUSY);
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
                cacheConfig.withOwner(command.file().owner()),
                estimate,
                command.planId(),
                command.commandId(),
                new ExecutorIndexCache.Loader() {
                  @Override
                  public ExecutorIndexCache.CachedFile load() {
                    return loader.load(command);
                  }

                  @Override
                  public void validate() {
                    loader.validate(command);
                  }

                  @Override
                  public void validate(ExecutorIndexCache.CachedFile cached) {
                    loader.validate(command, cached);
                  }
                });
        if (file == null) {
          // Admission refused: either this entry can never fit, or the space is momentarily taken.
          return reject(command, "BUSY");
        }

        return ack(
            command,
            IndexCacheProtocol.AckStatus.LOADED,
            cache.shape().residentBytes(Set.of(cacheKey)),
            null);
      } catch (RuntimeException e) {
        last = e;
      }
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
    long appliedEpoch = cache.managedEpoch(command.indexUuid());
    if (appliedEpoch > command.epoch()) {
      // A newer load installed this copy; an older release must not delete it.
      return reject(command, "STALE_EPOCH");
    }

    // A release withdraws this index's claim at whatever epoch the executor currently holds, and
    // also removes a pending admission so a load that is still preparing cannot publish afterwards.
    cache.releaseCopy(command.indexUuid(), cacheKey);

    return ack(command, IndexCacheProtocol.AckStatus.RELEASED, 0, null);
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
    IndexCacheProtocol.NodeReport report =
        report(profile, sequence, budgetBytes, releasingBytes, accessAgeNanos);
    return report.copy().reservedBytes(Math.max(report.reservedBytes(), reservedBytes)).build();
  }

  IndexCacheProtocol.NodeReport report(
      String profile,
      long sequence,
      long budgetBytes,
      long releasingBytes,
      Map<String, Long> accessAgeNanos) {
    ExecutorIndexCache.Shape shape = cache.shape();
    ExecutorIndexCache.MemoryUsage memory = cache.memoryUsage();

    return IndexCacheProtocol.NodeReport.builder()
        .session(session)
        .executorId(executorId)
        .incarnation(incarnation)
        .sequence(sequence)
        .budgetBytes(budgetBytes)
        .usedBytes(memory.usedBytes())
        .reservedBytes(memory.reservedBytes())
        .releasingBytes(releasingBytes)
        .holders(memory.residentFiles())
        .reservedByPlan(memory.reservedByPlan())
        .reservedByCommand(memory.reservedByCommand())
        .metadataShape(shape)
        .profile(profile)
        .accessAgeNanos(accessAgeNanos == null ? Map.of() : accessAgeNanos)
        .build();
  }

  /** Bytes promised to loads that have not finished yet. */
  long reservedBytes() {
    return cache.chargeSnapshot().reservedBytes();
  }

  /**
   * Files this executor still holds, from its own bookkeeping filtered by what the cache really
   * has. A copy evicted by the cache, or released after its last lease closed, stops being
   * reported.
   */
  Map<String, Long> holders() {
    return cache.shape().holders();
  }

  Set<String> residentKeys() {
    return holders().keySet();
  }

  long usedBytes() {
    return cache.chargeSnapshot().usedBytes();
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
    // In-flight cache reservations are released by their load's finally block.
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
    // The charge counters travel, not the charge state: the driver projects this command onto the
    // state its last report described, so one answer costs a few numbers instead of a copy of every
    // entry the executor holds.
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
        .chargeVersion(cache.chargeVersion())
        .controlBytes(cache.controlBytes())
        .reason(reason)
        .build();
  }

  private static String message(RuntimeException e) {
    String message = e.getMessage();
    return message == null || message.isEmpty() ? e.getClass().getSimpleName() : message;
  }
}
