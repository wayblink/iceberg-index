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

import java.math.BigInteger;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Driver-side residency bookkeeping: protection policy, confirmed coverage, index LRU and plan
 * materialization.
 *
 * <p>Confirmed coverage only comes from an accepted ACK of the current session, incarnation and
 * epoch. Executor reports contribute capacity and diagnostics, never confirmation. This class is
 * not thread safe; callers serialize capacity-affecting calls while reads remain unaffected.
 */
final class IndexResidencyCoordinator {
  /** Gap between the driver and an executor report before its capacity is unknown. */
  static final long STALE_AFTER_NANOS = Duration.ofSeconds(10).toNanos();

  interface Clock {
    long nowNanos();
  }

  private record Holder(
      String location, String cacheKey, String executorId, String incarnation, long bytes) {}

  private static final class IndexState {
    private final String indexUuid;
    private long epoch;
    private boolean paused;
    private IndexCacheProtocol.Protection protection;
    private long accessOrder;
    private java.util.Set<String> targetCacheKeys = new LinkedHashSet<>();

    private IndexState(String indexUuid) {
      this.indexUuid = indexUuid;
    }

    private void accessed(long order) {
      this.accessOrder = order;
    }

    private boolean retains(String cacheKey) {
      return targetCacheKeys.contains(cacheKey);
    }

    private boolean protectedNow(long nowNanos) {
      return protection == null || protection.permanent() || protection.deadlineNanos() > nowNanos;
    }
  }

  private static final class PlanState {
    private final String planId;
    private final String indexUuid;
    private final long epoch;
    private final IndexCacheProtocol.Status terminalStatus;
    private final List<IndexCacheProtocol.File> files = new ArrayList<>();
    private final Map<String, String> rejected = new LinkedHashMap<>();
    private final Map<String, String> failed = new LinkedHashMap<>();
    private final Map<String, PlanFile> commands = new LinkedHashMap<>();
    private final Map<String, PlanFile> loaded = new LinkedHashMap<>();
    private final Set<String> fenced = new LinkedHashSet<>();
    private final Set<String> dropped = new LinkedHashSet<>();
    private final Set<String> reused = new LinkedHashSet<>();
    private final Map<String, String> migrationTargets = new LinkedHashMap<>();
    private final Map<String, String> migrationNotices = new LinkedHashMap<>();
    private final Map<String, String> migrationSources = new LinkedHashMap<>();
    private final Set<String> released = new LinkedHashSet<>();
    private final List<String> evicted = new ArrayList<>();
    private int nextAttempt = 1;
    private boolean expired;

    private PlanState(
        String planId, String indexUuid, long epoch, IndexCacheProtocol.Status terminalStatus) {
      this.planId = planId;
      this.indexUuid = indexUuid;
      this.epoch = epoch;
      this.terminalStatus = terminalStatus;
    }

    private boolean finalized() {
      return expired || terminalStatus != null;
    }

    private int attempted() {
      return loaded.size() + failed.size();
    }

    private Set<String> loadExecutors() {
      Set<String> executors = new LinkedHashSet<>();
      for (PlanFile planned : commands.values()) {
        if (planned.type != null && planned.type == IndexCacheProtocol.CommandType.LOAD) {
          executors.add(planned.executorId);
        }
      }

      return executors;
    }

    private PlanFile command(String commandId) {
      return commands.get(commandId);
    }
  }

  private static final class PlanFile {
    private final String location;
    private final String cacheKey;
    private final String executorId;
    private final String incarnation;
    private final long bytes;
    private final IndexCacheProtocol.CommandType type;

    private PlanFile(
        String location,
        String cacheKey,
        String executorId,
        String incarnation,
        long bytes,
        IndexCacheProtocol.CommandType type) {
      this.location = location;
      this.cacheKey = cacheKey;
      this.executorId = executorId;
      this.incarnation = incarnation;
      this.bytes = bytes;
      this.type = type;
    }
  }

  private static final class NodeState {
    private IndexCacheProtocol.NodeReport report;
    private long reportTimeNanos;
    private final List<IndexCacheProtocol.Command> queue = new ArrayList<>();
    private final List<IndexCacheProtocol.Command> issued = new ArrayList<>();
    private final Map<String, Long> reserved = new LinkedHashMap<>();
    private final Set<String> commands = new LinkedHashSet<>();

    private void issued(String commandId) {
      commands.add(commandId);
    }

    private NodeState(IndexCacheProtocol.NodeReport report, long nowNanos) {
      this.report = report;
      this.reportTimeNanos = nowNanos;
    }

    private void observe(IndexCacheProtocol.NodeReport next, long nowNanos) {
      this.report = next;
      this.reportTimeNanos = nowNanos;
    }

    private boolean holds(String cacheKey) {
      return report.holders().containsKey(cacheKey);
    }

    private long reservedBytes() {
      return reserved.values().stream().mapToLong(Long::longValue).sum();
    }

    private void reserve(String planId, long bytes) {
      Long current = reserved.get(planId);
      reserved.put(planId, Math.addExact(current == null ? 0 : current, bytes));
    }

    private void settle(String planId, long bytes) {
      Long current = reserved.get(planId);
      if (current == null) {
        return;
      }

      long remaining = current - bytes;
      if (remaining > 0) {
        reserved.put(planId, remaining);
      } else {
        reserved.remove(planId);
      }
    }

    /** Acknowledged work is delivered: it is no longer pending in the reply queue. */
    private void delivered(String commandId) {
      queue.removeIf(command -> command.commandId().equals(commandId));
    }

    private void enqueue(IndexCacheProtocol.Command command) {
      if (commands.add(command.commandId())) {
        queue.add(command);
        issued.add(command);
      }
    }
  }

  private final Clock clock;
  private final IndexPlacementPlanner planner = new IndexPlacementPlanner();
  private final Map<String, NodeState> nodes = new LinkedHashMap<>();
  private final Map<String, IndexState> indexes = new HashMap<>();
  private final Map<String, PlanState> plans = new LinkedHashMap<>();
  private long planCount;
  private long accessOrder;
  private String lastPlanId;

  IndexResidencyCoordinator(Clock clock) {
    this.clock = Objects.requireNonNull(clock, "Clock is null");
  }

  String acceptLoad(IndexCacheProtocol.Target target, IndexResidencyPolicy policy, long nowNanos) {
    Objects.requireNonNull(target, "Index target is null");
    IndexState index = indexes.computeIfAbsent(target.indexUuid(), IndexState::new);
    index.epoch = index.epoch + 1;
    index.paused = false;
    index.accessed(++accessOrder);
    Set<String> previous = new LinkedHashSet<>(index.targetCacheKeys);
    // A supplied policy always restarts protection; a repeated load without one keeps its deadline.
    if (policy != null) {
      index.protection = protection(policy, nowNanos);
    }

    for (PlanState open : plans.values()) {
      if (open.indexUuid.equals(target.indexUuid()) && !open.finalized()) {
        open.expired = true;
      }
    }

    index.targetCacheKeys.clear();
    for (IndexCacheProtocol.File file : target.files()) {
      index.targetCacheKeys.add(file.cacheKey());
    }

    PlanState plan = new PlanState("plan-" + (++planCount), target.indexUuid(), index.epoch, null);
    plan.files.addAll(target.files());
    plans.put(plan.planId, plan);
    lastPlanId = plan.planId;
    materialize(plan, target, index, previous);
    return plan.planId;
  }

  String acceptOffload(String indexUuid, long nowNanos) {
    Objects.requireNonNull(indexUuid, "Index identity is null");
    IndexState index = indexes.computeIfAbsent(indexUuid, IndexState::new);
    index.epoch = index.epoch + 1;
    // Withdraw the target so the index stops accepting new files, but keep the old keys until the
    // holders are collected: they are what the release commands are built from.
    Set<String> previous = new LinkedHashSet<>(index.targetCacheKeys);
    index.targetCacheKeys.clear();
    for (PlanState open : plans.values()) {
      if (open.indexUuid.equals(indexUuid) && !open.finalized()) {
        open.expired = true;
      }
    }

    PlanState plan =
        new PlanState(
            "plan-" + (++planCount), indexUuid, index.epoch, IndexCacheProtocol.Status.OFFLOADED);
    plans.put(plan.planId, plan);
    lastPlanId = plan.planId;
    List<Holder> toRelease = holders(indexUuid);
    for (String cacheKey : previous) {
      for (Holder holder : holdersOfKey(cacheKey)) {
        if (toRelease.stream()
            .noneMatch(
                h -> h.cacheKey().equals(cacheKey) && h.executorId().equals(holder.executorId()))) {
          toRelease.add(holder);
        }
      }
    }

    index.paused = true;
    // The paused index keeps no confirmed copy: the pending release commands are the only owner.
    for (PlanState open : plans.values()) {
      if (open.indexUuid.equals(indexUuid)) {
        open.loaded.clear();
      }
    }

    for (Holder holder : toRelease) {
      NodeState node = nodes.get(holder.executorId());
      String location = holder.location();
      String cacheKey = holder.cacheKey();
      enqueue(
          plan,
          node,
          "RELEASE",
          IndexCacheProtocol.CommandType.RELEASE,
          indexUuid,
          modeFor(cacheKey),
          IndexCacheProtocol.File.builder().location(location).cacheKey(cacheKey).build(),
          holder.bytes());
    }

    return plan.planId;
  }

  String lastPlanId() {
    return lastPlanId;
  }

  IndexCacheProtocol.Reply report(IndexCacheProtocol.NodeReport report) {
    Objects.requireNonNull(report, "Executor report is null");
    NodeState node = nodes.get(report.executorId());
    if (node == null) {
      node = new NodeState(report, clock.nowNanos());
      nodes.put(report.executorId(), node);
    } else if (!node.report.incarnation().equals(report.incarnation())) {
      // A new incarnation replaces the old node and all of its outstanding work.
      node = new NodeState(report, clock.nowNanos());
      nodes.put(report.executorId(), node);
    } else if (report.sequence() >= node.report.sequence()) {
      node.observe(report, clock.nowNanos());
    }

    for (Map.Entry<String, Long> access : report.accessAgeNanos().entrySet()) {
      IndexState index = indexes.get(access.getKey());
      if (index != null) {
        index.accessed(++accessOrder);
      }
    }

    return IndexCacheProtocol.Reply.builder()
        .commands(new ArrayList<>(node.queue))
        .nextSequence(report.sequence())
        .build();
  }

  /**
   * Records an executor acknowledgement. A {@code LOADED} ACK confirms coverage only for a command
   * issued to that executor incarnation by the current plan epoch; duplicates and late ACKs for a
   * superseded or expired plan change nothing.
   */
  void ack(IndexCacheProtocol.Ack ack) {
    Objects.requireNonNull(ack, "ACK is null");
    NodeState node = nodes.get(ack.executorId());
    if (node == null || !node.report.incarnation().equals(ack.incarnation())) {
      return;
    }

    PlanState plan = plans.get(ack.planId());
    if (plan == null) {
      return;
    }

    IndexState index = indexes.get(plan.indexUuid);
    if (index == null || index.epoch != plan.epoch) {
      return;
    }

    PlanFile command = plan.command(ack.commandId());
    if (command == null || command.bytes < 0) {
      return;
    }

    node.delivered(ack.commandId());
    if (plan.terminalStatus != null) {
      node.settle(plan.planId, command.bytes);
      return;
    }

    // A late ACK for a timed-out plan keeps its success; files already fenced stay uncovered.
    if (plan.expired && plan.fenced.contains(command.cacheKey)) {
      node.settle(plan.planId, command.bytes);
      return;
    }

    if (command.type == IndexCacheProtocol.CommandType.RELEASE) {
      if (ack.status() == IndexCacheProtocol.AckStatus.RELEASED) {
        plan.dropped.add(command.cacheKey);
        plan.loaded.remove(command.cacheKey);
      }

      node.settle(plan.planId, command.bytes);
      return;
    }

    if (ack.status() == IndexCacheProtocol.AckStatus.LOADED) {
      if (plan.loaded.containsKey(command.cacheKey)) {
        // Retries and duplicate ACKs must not charge the same copy twice.
        return;
      }

      Map<String, Long> holders = new LinkedHashMap<>(node.report.holders());
      holders.put(command.cacheKey, command.bytes);
      node.report =
          node.report
              .copy()
              .holders(holders)
              .usedBytes(Math.addExact(node.report.usedBytes(), command.bytes))
              .build();
      plan.loaded.put(command.cacheKey, command);
      plan.failed.remove(command.location);
      releaseMigratedCopy(plan, command, ack.executorId());
    } else if (ack.status() == IndexCacheProtocol.AckStatus.REJECTED
        || ack.status() == IndexCacheProtocol.AckStatus.FAILED) {
      plan.failed.put(command.location, ack.reason() == null ? ack.status().name() : ack.reason());
      node.settle(plan.planId, command.bytes);
    }
  }

  IndexCacheProtocol.Result result(String planId) {
    IndexCacheProtocol.Result r = result0(planId);
    PlanState dbg = plans.get(planId);
    return r;
  }

  private IndexCacheProtocol.Result result0(String planId) {
    PlanState plan = planId == null ? null : plans.get(planId);
    if (plan == null) {
      throw new IllegalArgumentException("Unknown residency plan: " + planId);
    }

    IndexState index = indexes.get(plan.indexUuid);
    IndexCacheProtocol.Protection protection = index == null ? null : index.protection;
    if (plan.terminalStatus != null) {
      return resultOf(plan, plan.terminalStatus, protection, Map.of(), plan.evicted);
    }

    Set<String> lost = lostIncarnations();
    Map<String, String> uncovered = new LinkedHashMap<>(plan.rejected);
    Map<String, Long> counted = new LinkedHashMap<>();
    for (IndexCacheProtocol.File file : plan.files) {
      if (plan.reused.contains(file.cacheKey()) && confirmedAnywhere(file.cacheKey())) {
        counted.putIfAbsent(file.cacheKey(), bytesOf(file.cacheKey()));
        continue;
      }

      PlanFile loaded = plan.loaded.get(file.cacheKey());
      if (loaded != null
          && !lost.contains(loaded.incarnation)
          && !plan.dropped.contains(file.cacheKey())) {
        counted.putIfAbsent(file.cacheKey(), loaded.bytes);
      } else if (!plan.loaded.containsKey(file.cacheKey())
          || lost.contains(loaded.incarnation)
          || plan.dropped.contains(file.cacheKey())) {
        uncovered.putIfAbsent(file.location(), reason(plan, file, lost));
      }
    }

    long resident = 0;
    for (long bytes : counted.values()) {
      resident = Math.addExact(resident, bytes);
    }

    IndexCacheProtocol.Status status;
    if (!indexes.containsKey(plan.indexUuid) || index == null) {
      status = IndexCacheProtocol.Status.FAILED;
    } else if (uncovered.isEmpty()) {
      status = IndexCacheProtocol.Status.COMPLETE;
    } else if (plan.expired) {
      status = resident == 0 ? IndexCacheProtocol.Status.FAILED : IndexCacheProtocol.Status.PARTIAL;
    } else if (queued(plan.planId)) {
      status = IndexCacheProtocol.Status.QUEUED;
    } else if (plan.loaded.isEmpty() && allIssuedLoadsAnswered(plan, lost)) {
      // Every issued load already has a terminal result (failure or a lost node); nothing is left.
      status = IndexCacheProtocol.Status.FAILED;
    } else if (!plan.loaded.isEmpty()) {
      status = IndexCacheProtocol.Status.PARTIAL;
    } else {
      status = IndexCacheProtocol.Status.PENDING;
    }

    return resultOf(plan, status, protection, uncovered, plan.evicted);
  }

  IndexCacheProtocol.NodeReport state(String executorId, String incarnation) {
    NodeState node = nodes.get(executorId);
    return node == null || !node.report.incarnation().equals(incarnation)
        ? null
        : node.report.copy().reservedBytes(node.reservedBytes()).build();
  }

  /** Marks nodes whose reports are older than {@link #STALE_AFTER_NANOS} as unknown. */
  void refresh(long nowNanos) {
    for (NodeState node : nodes.values()) {
      // Outstanding commands mean the node simply has not answered yet, not that it left.
      if (nowNanos - node.reportTimeNanos <= STALE_AFTER_NANOS) {
        continue;
      }

      boolean abandoned = node.queue.isEmpty() && !node.report.holders().isEmpty();
      if (!abandoned) {
        continue;
      }

      for (PlanState plan : plans.values()) {
        if (plan.finalized()) {
          continue;
        }

        for (PlanFile loaded : new ArrayList<>(plan.loaded.values())) {
          if (loaded.incarnation.equals(node.report.incarnation())) {
            plan.loaded.remove(loaded.cacheKey);
            plan.failed.put(loaded.location, "NODE_LOST");
          }
        }
      }
    }
  }

  /**
   * Ends a plan that exceeded its deadline: success stays confirmed, unfinished files are fenced
   * and cancelled so no late ACK can backfill them.
   */
  void expire(String planId, long nowNanos) {
    PlanState plan = plans.get(planId);
    if (plan == null || plan.finalized()) {
      return;
    }

    plan.expired = true;
    // Already confirmed files stay; only work without an answer is fenced.
    Set<String> answered = new LinkedHashSet<>(plan.failed.keySet());
    for (PlanFile planned : plan.commands.values()) {
      if (planned.type != IndexCacheProtocol.CommandType.LOAD
          || plan.loaded.containsKey(planned.cacheKey)
          || answered.contains(planned.location)) {
        continue;
      }

      plan.fenced.add(planned.cacheKey);
      plan.failed.put(planned.location, "CANCELLED");
    }
  }

  List<String> expiredProtections() {
    List<String> expired = new ArrayList<>();
    for (IndexState index : indexedStates()) {
      if (index.protection != null
          && !index.protection.permanent()
          && index.protection.deadlineNanos() <= clock.nowNanos()) {
        expired.add(index.indexUuid);
      }
    }

    Collections.sort(expired);
    return expired;
  }

  List<IndexCacheProtocol.Plan> pendingCommands(String executorId, String incarnation) {
    NodeState node = nodes.get(executorId);
    if (node == null || !node.report.incarnation().equals(incarnation)) {
      return List.of();
    }

    Map<String, List<IndexCacheProtocol.Command>> byPlan = new LinkedHashMap<>();
    for (IndexCacheProtocol.Command command : node.queue) {
      byPlan.computeIfAbsent(command.planId(), ignored -> new ArrayList<>()).add(command);
    }

    List<IndexCacheProtocol.Plan> pending = new ArrayList<>();
    for (Map.Entry<String, List<IndexCacheProtocol.Command>> entry : byPlan.entrySet()) {
      IndexCacheProtocol.Command first = entry.getValue().get(0);
      List<IndexCacheProtocol.File> files = new ArrayList<>();
      for (IndexCacheProtocol.Command command : entry.getValue()) {
        files.add(command.file());
      }

      pending.add(
          IndexCacheProtocol.Plan.builder()
              .planId(entry.getKey())
              .indexUuid(first.indexUuid())
              .epoch(first.epoch())
              .files(files)
              .build());
    }

    return pending;
  }

  List<IndexCacheProtocol.Command> commands(String executorId, String incarnation) {
    NodeState node = nodes.get(executorId);
    return node == null || !node.report.incarnation().equals(incarnation)
        ? List.of()
        : new ArrayList<>(node.issued);
  }

  private void materialize(
      PlanState plan,
      IndexCacheProtocol.Target target,
      IndexState index,
      Set<String> previousKeys) {
    Set<String> reusable = new LinkedHashSet<>();
    for (IndexCacheProtocol.File file : target.files()) {
      // Only files carrying an index-scoped identity issued by this plan are routable.
      if (target.indexUuid() == null || !cacheKeyBelongsTo(file, target.indexUuid())) {
        plan.rejected.put(file.location(), "CACHE_KEY_NOT_OWNED");
      } else if (index.retains(file.cacheKey()) && confirmedAnywhere(file.cacheKey())) {
        // A shared file already has one confirmed copy in this target; no second load is needed.
        plan.reused.add(file.cacheKey());
        reusable.add(file.location());
      }
    }

    IndexCacheProtocol.Target routable =
        IndexCacheProtocol.Target.builder()
            .indexUuid(target.indexUuid())
            .snapshotId(target.snapshotId())
            .files(
                target.files().stream()
                    .filter(file -> !plan.rejected.containsKey(file.location()))
                    .toList())
            .definition(target.definition())
            .io(target.io())
            .build();
    List<IndexCacheProtocol.File> pendingFiles =
        routable.files().stream().filter(file -> !reusable.contains(file.location())).toList();
    IndexCacheProtocol.Target pending =
        IndexCacheProtocol.Target.builder()
            .indexUuid(target.indexUuid())
            .snapshotId(target.snapshotId())
            .files(pendingFiles)
            .definition(target.definition())
            .io(target.io())
            .build();
    IndexCacheProtocol.Plan placement = planner.plan(pending, reportsFor(plan.planId));
    plan.rejected.putAll(placement.rejected());
    for (Map.Entry<String, String> entry : placement.placements().entrySet()) {
      IndexCacheProtocol.File file = file(target, entry.getKey());
      String executorId = entry.getValue();
      NodeState node = nodes.get(executorId);
      // Only an acknowledged copy counts; a report on its own grants nothing.
      boolean shared = confirmedOn(node.report.executorId(), file.cacheKey());
      if (node == null) {
        plan.rejected.put(file.location(), "NODE_LOST");
      } else if (shared) {
        // A confirmed copy on this node already covers the file; no second load is planned.
        continue;
      } else if (file.estimatedBytes() == null) {
        plan.rejected.put(file.location(), "SIZE_UNKNOWN");
      } else if (node.report.budgetBytes() <= 0) {
        plan.rejected.put(file.location(), "CACHE_DISABLED");
      } else {
        enqueue(
            plan,
            node,
            "LOAD",
            IndexCacheProtocol.CommandType.LOAD,
            target.indexUuid(),
            modeFor(file.cacheKey()),
            file,
            file.estimatedBytes());
      }
    }

    planMigrations(plan, target, index, reusable);

    // Evict whole indexes only while the local plan still has uncovered routable files.
    tryEvict(plan, placement);

    for (Holder holder : holdersOfKeys(previousKeys)) {
      if (!stillHeldBy(holder.cacheKey(), target.files())) {
        NodeState node = nodes.get(holder.executorId());
        if (node != null) {
          enqueue(
              plan,
              node,
              "RELEASE",
              IndexCacheProtocol.CommandType.RELEASE,
              index.indexUuid,
              "OTHER",
              IndexCacheProtocol.File.builder()
                  .location(holder.location())
                  .cacheKey(holder.cacheKey())
                  .build(),
              holder.bytes());
        }
      }
    }
  }

  /**
   * Rebalances files that already have exactly one confirmed copy. A second copy is loaded on a
   * colder node first and the old one is released only after that ACK; without room for both copies
   * the file stays where it is and the reason is reported.
   */
  private void planMigrations(
      PlanState plan, IndexCacheProtocol.Target target, IndexState index, Set<String> reusable) {
    for (String location : reusable) {
      IndexCacheProtocol.File file = file(target, location);
      List<Holder> holders = holdersOfKey(file.cacheKey());
      if (holders.isEmpty() || file.estimatedBytes() == null) {
        continue;
      }

      Holder current = holders.get(0);
      NodeState holderNode = nodes.get(current.executorId());
      if (holderNode == null || holderNode.report.budgetBytes() <= 0) {
        continue;
      }

      NodeState destination = null;
      boolean colderWithoutRoom = false;
      for (NodeState node : nodes.values()) {
        if (node.report.executorId().equals(current.executorId())
            || node.holds(file.cacheKey())
            || node.report.budgetBytes() <= 0) {
          continue;
        }

        if (lessOccupied(node, holderNode)) {
          if (hasRoomForSecondCopy(node, file)) {
            if (destination == null || lessOccupied(node, destination)) {
              destination = node;
            }
          } else {
            colderWithoutRoom = true;
          }
        }
      }

      if (destination == null) {
        if (colderWithoutRoom) {
          plan.migrationNotices.put(location, "NEEDS_DOUBLE_SPACE");
        }
        continue;
      }

      enqueue(
          plan,
          destination,
          "MIGRATE",
          IndexCacheProtocol.CommandType.LOAD,
          index.indexUuid,
          modeFor(file.cacheKey()),
          file,
          file.estimatedBytes());
      plan.migrationTargets.put(location, destination.report.executorId());
      plan.migrationSources.put(file.cacheKey(), current.executorId());
    }
  }

  private boolean lessOccupied(NodeState left, NodeState right) {
    return BigInteger.valueOf(left.report.usedBytes())
            .multiply(BigInteger.valueOf(right.report.budgetBytes()))
            .compareTo(
                BigInteger.valueOf(right.report.usedBytes())
                    .multiply(BigInteger.valueOf(left.report.budgetBytes())))
        < 0;
  }

  private boolean hasRoomForSecondCopy(NodeState node, IndexCacheProtocol.File file) {
    long free = node.report.budgetBytes() - node.report.usedBytes() - node.reservedBytes();
    return free >= file.estimatedBytes();
  }

  /** Releases the previous node's copy of a migrated file once the replacement is confirmed. */
  private void releaseMigratedCopy(PlanState plan, PlanFile migrated, String executorId) {
    String source = plan.migrationSources.get(migrated.cacheKey);
    if (source == null || source.equals(executorId) || !plan.released.add(migrated.cacheKey)) {
      return;
    }

    NodeState node = nodes.get(source);
    if (node == null) {
      return;
    }

    enqueue(
        plan,
        node,
        "MIGRATE_RELEASE",
        IndexCacheProtocol.CommandType.RELEASE,
        plan.indexUuid,
        "OTHER",
        IndexCacheProtocol.File.builder()
            .location(migrated.location)
            .cacheKey(migrated.cacheKey)
            .build(),
        migrated.bytes);
  }

  /**
   * Starts an index-level eviction when placement lacked capacity. Only an eligible index (expired
   * protection, no in-flight load) is chosen, and its space is not treated as free until release is
   * acknowledged, so rejected files stay uncovered here.
   */
  private void tryEvict(PlanState plan, IndexCacheProtocol.Plan placement) {
    if (needsEviction(placement) && !plan.evicted.isEmpty()) {
      return;
    }

    Set<String> before = new LinkedHashSet<>(plan.rejected.keySet());
    if (!needsEviction(placement)) {
      return;
    }

    long nowNanos = clock.nowNanos();
    List<IndexState> candidates = new ArrayList<>();
    for (IndexState other : indexedStates()) {
      if (other.indexUuid.equals(plan.indexUuid) || other.protectedNow(nowNanos)) {
        continue;
      }

      // Only resident indexes that this plan can actually reclaim are victims.
      if (!hasInFlightLoad(other.indexUuid) && !holders(other.indexUuid).isEmpty()) {
        candidates.add(other);
      }
    }

    candidates.sort(
        Comparator.comparingLong((IndexState state) -> state.accessOrder)
            .thenComparing(state -> state.indexUuid));
    if (candidates.isEmpty()) {
      return;
    }

    IndexState victim = candidates.get(0);
    List<Holder> victims = holders(victim.indexUuid);
    if (victims.isEmpty()) {
      return;
    }

    for (Holder holder : victims) {
      NodeState node = nodes.get(holder.executorId());
      if (node != null) {
        enqueue(
            plan,
            node,
            "EVICT",
            IndexCacheProtocol.CommandType.RELEASE,
            victim.indexUuid,
            "OTHER",
            IndexCacheProtocol.File.builder()
                .location(holder.location())
                .cacheKey(holder.cacheKey())
                .build(),
            holder.bytes());
      }
    }

    plan.evicted.add(victim.indexUuid);
    for (String location : before) {
      plan.rejected.put(location, "INSUFFICIENT_CAPACITY_PENDING_EVICTION");
    }
  }

  /**
   * A plan is queued while another plan still owns capacity-affecting work on every node it needs.
   */
  /**
   * A load is queued when capacity-affecting work of an earlier, still unfinished plan is
   * outstanding on any node this plan was placed on. The two never reserve the same space.
   */
  private Set<String> queuedTargets(PlanState plan) {
    Set<String> executors = new LinkedHashSet<>();
    for (NodeState node : nodes.values()) {
      if (node.report.budgetBytes() > 0) {
        executors.add(node.report.executorId());
      }
    }

    return executors;
  }

  private boolean queued(String planId) {
    PlanState plan = plans.get(planId);
    if (plan == null) {
      return false;
    }

    Set<String> executors = new LinkedHashSet<>();
    for (String executorId : plan.loadExecutors()) {
      executors.add(executorId);
    }

    if (executors.isEmpty()) {
      // A plan blocked before placement was never issued; it still waits for work already in
      // flight on any node it would have used, so it cannot jump the queue on a re-plan.
      executors = queuedTargets(plan);
      if (executors.isEmpty()) {
        return false;
      }
    }

    for (PlanState other : plans.values()) {
      if (other.planId.equals(planId) || other.finalized()) {
        continue;
      }

      for (PlanFile inFlight : other.commands.values()) {
        // Ownership of a node starts at planning time, not when a copy is confirmed.
        if (inFlight.type == IndexCacheProtocol.CommandType.LOAD
            && executors.contains(inFlight.executorId)) {
          return true;
        }
      }
    }

    return false;
  }

  private static boolean allIssuedLoadsAnswered(PlanState plan, Set<String> lost) {
    for (PlanFile planned : plan.commands.values()) {
      if (planned.type != IndexCacheProtocol.CommandType.LOAD) {
        continue;
      }

      if (plan.loaded.containsKey(planned.cacheKey)
          || lost.contains(planned.incarnation)
          || plan.fenced.contains(planned.cacheKey)
          || plan.failed.containsKey(planned.location)) {
        continue;
      }

      return false;
    }

    return true;
  }

  private static boolean needsEviction(IndexCacheProtocol.Plan placement) {
    for (String reason : placement.rejected().values()) {
      if (!"SIZE_UNKNOWN".equals(reason)) {
        return true;
      }
    }

    return false;
  }

  /** True while an index still has unconfirmed load commands, so it is never evicted mid-load. */
  private boolean hasInFlightLoad(String indexUuid) {
    for (PlanState plan : plans.values()) {
      if (!plan.indexUuid.equals(indexUuid) || plan.finalized()) {
        continue;
      }

      for (PlanFile planned : plan.commands.values()) {
        if (planned.type == IndexCacheProtocol.CommandType.LOAD
            && !plan.loaded.containsKey(planned.cacheKey)
            && !plan.failed.containsKey(planned.location)
            && !plan.fenced.contains(planned.cacheKey)) {
          return true;
        }
      }
    }

    return false;
  }

  private void enqueue(
      PlanState plan,
      NodeState node,
      String label,
      IndexCacheProtocol.CommandType type,
      String indexUuid,
      String mode,
      IndexCacheProtocol.File file,
      long bytes) {
    String commandId = plan.planId + "#" + (plan.nextAttempt++) + "-" + label;
    IndexCacheProtocol.Command command =
        IndexCacheProtocol.Command.builder()
            .session(node.report.session())
            .executorId(node.report.executorId())
            .incarnation(node.report.incarnation())
            .indexUuid(indexUuid)
            .epoch(plan.epoch)
            .planId(plan.planId)
            .commandId(commandId)
            .type(type)
            .mode(mode)
            .file(file)
            .sequence(plan.nextAttempt)
            .build();
    plan.commands.put(
        commandId,
        new PlanFile(
            file.location(),
            file.cacheKey(),
            node.report.executorId(),
            node.report.incarnation(),
            bytes,
            type));
    if (type == IndexCacheProtocol.CommandType.LOAD) {
      node.reserve(plan.planId, bytes);
    }

    // Registration is by the queue itself, so a command cannot be marked issued before it exists.
    node.enqueue(command);
  }

  private List<IndexCacheProtocol.NodeReport> reports() {
    List<IndexCacheProtocol.NodeReport> reports = new ArrayList<>();
    for (NodeState node : nodes.values()) {
      reports.add(node.report.copy().reservedBytes(node.reservedBytes()).build());
    }

    return reports;
  }

  /**
   * Reports as the planner sees them: space promised to the current plan is not double booked, but
   * commitments of other in-flight plans stay unavailable to this one.
   */
  private List<IndexCacheProtocol.NodeReport> reportsFor(String planId) {
    List<IndexCacheProtocol.NodeReport> reports = new ArrayList<>();
    for (NodeState node : nodes.values()) {
      long others = 0;
      for (Map.Entry<String, Long> reservation : node.reserved.entrySet()) {
        if (!reservation.getKey().equals(planId)) {
          others = Math.addExact(others, reservation.getValue());
        }
      }

      // Occupancy includes retained copies plus every other plan's unresolved promise; the
      // planner adds this plan's own placements to that baseline.
      reports.add(
          node.report
              .copy()
              .usedBytes(Math.addExact(node.report.usedBytes(), others))
              .reservedBytes(0)
              .build());
    }

    return reports;
  }

  private Set<String> lostIncarnations() {
    Set<String> lost = new LinkedHashSet<>();
    for (NodeState node : nodes.values()) {
      // A report older than the staleness bound cannot confirm coverage; outstanding commands are
      // tracked separately and never count as answers.
      if (node.reportTimeNanos == 0
          || clock.nowNanos() - node.reportTimeNanos > STALE_AFTER_NANOS) {
        lost.add(node.report.incarnation());
      }
    }

    return lost;
  }

  private List<IndexState> indexedStates() {
    List<IndexState> states = new ArrayList<>(indexes.values());
    states.sort(Comparator.comparing(state -> state.indexUuid));
    return states;
  }

  private List<Holder> holders(String indexUuid) {
    List<Holder> holders = new ArrayList<>();
    for (Map.Entry<String, List<Holder>> entry : holdersByCacheKey().entrySet()) {
      if (entry.getKey().contains("|" + indexUuid + "|")) {
        holders.addAll(entry.getValue());
      }
    }

    return holders;
  }

  /**
   * Holders that still belong to an index's current target, so a released or superseded file is not
   * handed to offload or eviction twice.
   */
  /**
   * Copies belonging to an index, including those in plans that have already finished. Without this
   * an offload or target replacement would never see copies loaded by an earlier plan.
   */
  private Map<String, List<Holder>> holdersByCacheKey() {
    Map<String, List<Holder>> byKey = new LinkedHashMap<>();
    for (PlanState plan : plans.values()) {
      IndexState index = indexes.get(plan.indexUuid);
      if (index == null) {
        continue;
      }

      for (PlanFile loaded : plan.loaded.values()) {
        // A copy released by a later plan, or no longer part of the current target, is not a
        // holder any more; a paused index still holds its files until release is acknowledged.
        if (plan.dropped.contains(loaded.cacheKey) || !index.retains(loaded.cacheKey)) {
          continue;
        }

        byKey
            .computeIfAbsent(loaded.cacheKey, ignored -> new ArrayList<>())
            .add(
                new Holder(
                    loaded.location,
                    loaded.cacheKey,
                    loaded.executorId,
                    loaded.incarnation,
                    loaded.bytes));
      }
    }

    return byKey;
  }

  private String locationOfKey(String cacheKey) {
    List<Holder> holders = holdersOfKey(cacheKey);
    if (!holders.isEmpty()) {
      return holders.get(0).location();
    }

    int separator = cacheKey.lastIndexOf('|');
    return separator >= 0 ? cacheKey.substring(separator + 1) : cacheKey;
  }

  private boolean confirmedOn(String executorId, String cacheKey) {
    for (Holder holder : holdersOfKey(cacheKey)) {
      if (holder.executorId().equals(executorId)) {
        return true;
      }
    }

    return false;
  }

  private boolean confirmedAnywhere(String cacheKey) {
    return !holdersOfKey(cacheKey).isEmpty();
  }

  private long bytesOf(String cacheKey) {
    List<Holder> holders = holdersOfKey(cacheKey);
    return holders.isEmpty() ? 0 : holders.get(0).bytes();
  }

  private List<Holder> holdersOfKeys(Set<String> cacheKeys) {
    List<Holder> holders = new ArrayList<>();
    for (String cacheKey : cacheKeys) {
      holders.addAll(holdersOfKey(cacheKey));
    }

    return holders;
  }

  private List<Holder> holdersOfKey(String cacheKey) {
    List<Holder> holders = new ArrayList<>();
    for (PlanState plan : plans.values()) {
      PlanFile loaded = plan.loaded.get(cacheKey);
      if (loaded != null && !plan.dropped.contains(cacheKey)) {
        holders.add(
            new Holder(
                loaded.location,
                loaded.cacheKey,
                loaded.executorId,
                loaded.incarnation,
                loaded.bytes));
      }
    }

    return holders;
  }

  private static boolean stillHeldBy(String cacheKey, List<IndexCacheProtocol.File> files) {
    for (IndexCacheProtocol.File file : files) {
      if (file.cacheKey().equals(cacheKey)) {
        return true;
      }
    }

    return false;
  }

  private static boolean cacheKeyBelongsTo(IndexCacheProtocol.File file, String indexUuid) {
    String[] parts = file.cacheKey().split("\\|", 3);
    return parts.length == 3 && parts[1].equals(indexUuid) && parts[2].equals(file.location());
  }

  private static String modeFor(String cacheKey) {
    String[] parts = cacheKey.split("\\|", 3);
    return parts.length == 3 ? parts[0] : "OTHER";
  }

  private static IndexCacheProtocol.File file(IndexCacheProtocol.Target target, String location) {
    return target.files().stream()
        .filter(file -> file.location().equals(location))
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("Unknown planned file: " + location));
  }

  private static String reason(PlanState plan, IndexCacheProtocol.File file, Set<String> lost) {
    String reason = plan.failed.get(file.location());
    if (reason != null) {
      return reason;
    }

    PlanFile loaded = plan.loaded.get(file.cacheKey());
    if (loaded != null && lost.contains(loaded.incarnation)) {
      return "NODE_LOST";
    }

    return plan.fenced.contains(file.cacheKey()) ? "CANCELLED" : "PENDING";
  }

  private IndexCacheProtocol.Result resultOf(
      PlanState plan,
      IndexCacheProtocol.Status status,
      IndexCacheProtocol.Protection protection,
      Map<String, String> uncovered,
      List<String> evicted) {
    long resident = 0;
    for (PlanFile loaded : plan.loaded.values()) {
      resident = Math.addExact(resident, loaded.bytes);
    }

    return IndexCacheProtocol.Result.builder()
        .planId(plan.planId)
        .indexUuid(plan.indexUuid)
        .epoch(plan.epoch)
        .status(status)
        .protection(protection)
        .targetFiles(plan.files.stream().map(IndexCacheProtocol.File::location).toList())
        .uncovered(uncovered)
        .residentBytes(resident)
        .evictedIndexes(evicted)
        .reusedFiles(plan.reused.stream().map(this::locationOfKey).toList())
        .migrations(plan.migrationTargets)
        .migrationNotices(plan.migrationNotices)
        .build();
  }

  private static IndexCacheProtocol.Protection protection(
      IndexResidencyPolicy policy, long nowNanos) {
    if (policy.forever()) {
      return IndexCacheProtocol.Protection.builder().permanent(true).text("forever").build();
    }

    if (policy.durationNanos() == 0) {
      return IndexCacheProtocol.Protection.builder().text("0").build();
    }

    return IndexCacheProtocol.Protection.builder()
        .text(nanos(policy.durationNanos()))
        .deadlineNanos(Math.addExact(nowNanos, policy.durationNanos()))
        .build();
  }

  private static String nanos(long durationNanos) {
    if (durationNanos % Duration.ofDays(1).toNanos() == 0) {
      return durationNanos / Duration.ofDays(1).toNanos() + "d";
    } else if (durationNanos % Duration.ofHours(1).toNanos() == 0) {
      return durationNanos / Duration.ofHours(1).toNanos() + "h";
    } else if (durationNanos % Duration.ofMinutes(1).toNanos() == 0) {
      return durationNanos / Duration.ofMinutes(1).toNanos() + "m";
    }

    return durationNanos / Duration.ofSeconds(1).toNanos() + "s";
  }
}
