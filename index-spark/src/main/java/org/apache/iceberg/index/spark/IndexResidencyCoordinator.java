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
    private long accessAgeNanos = Long.MAX_VALUE;
    private long accessObservedAtNanos;
    private Set<String> targetCacheKeys = new LinkedHashSet<>();

    private IndexState(String indexUuid) {
      this.indexUuid = indexUuid;
    }

    private void accessed(long ageNanos, long observedAtNanos) {
      this.accessAgeNanos = ageNanos;
      this.accessObservedAtNanos = observedAtNanos;
    }

    /** Age at {@code nowNanos}; larger means staler. Independent of any absolute clock epoch. */
    private long ageAt(long nowNanos) {
      return accessAgeNanos == Long.MAX_VALUE
          ? Long.MAX_VALUE
          : Math.addExact(accessAgeNanos, Math.max(0, nowNanos - accessObservedAtNanos));
    }

    private boolean retains(String cacheKey) {
      return targetCacheKeys.contains(cacheKey);
    }

    private boolean protectedNow(long nowNanos) {
      return protection != null
          && (protection.permanent() || protection.deadlineNanos() > nowNanos);
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
    private final Set<String> orphansReleased = new LinkedHashSet<>();
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
    private final long epoch;

    private PlanFile(
        String location,
        String cacheKey,
        String executorId,
        String incarnation,
        long bytes,
        IndexCacheProtocol.CommandType type,
        long epoch) {
      this.location = location;
      this.cacheKey = cacheKey;
      this.executorId = executorId;
      this.incarnation = incarnation;
      this.bytes = bytes;
      this.type = type;
      this.epoch = epoch;
    }
  }

  private static final class NodeState {
    private IndexCacheProtocol.NodeReport report;
    private long reportTimeNanos;
    private final List<IndexCacheProtocol.Command> queue = new ArrayList<>();
    private final List<IndexCacheProtocol.Command> issued = new ArrayList<>();
    private final Map<String, Long> reservedByCommand = new LinkedHashMap<>();
    private final Map<String, String> planOfCommand = new LinkedHashMap<>();
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
      return reservedByCommand.values().stream().mapToLong(Long::longValue).sum();
    }

    private long reservedBytesFor(String planId) {
      long total = 0;
      for (Map.Entry<String, Long> reservation : reservedByCommand.entrySet()) {
        if (planId.equals(planOfCommand.get(reservation.getKey()))) {
          total = Math.addExact(total, reservation.getValue());
        }
      }

      return total;
    }

    private Map<String, Long> reservedByPlan() {
      Map<String, Long> byPlan = new LinkedHashMap<>();
      for (Map.Entry<String, Long> reservation : reservedByCommand.entrySet()) {
        String planId = planOfCommand.get(reservation.getKey());
        if (planId != null) {
          byPlan.merge(planId, reservation.getValue(), Long::sum);
        }
      }

      return byPlan;
    }

    private void reserve(String planId, String commandId, long bytes) {
      reservedByCommand.put(commandId, bytes);
      planOfCommand.put(commandId, planId);
    }

    /** Releases exactly the promise made by this command; other commands keep theirs. */
    private void settle(String commandId) {
      reservedByCommand.remove(commandId);
      planOfCommand.remove(commandId);
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
  private String lastPlanId;

  IndexResidencyCoordinator(Clock clock) {
    this.clock = Objects.requireNonNull(clock, "Clock is null");
  }

  String acceptLoad(IndexCacheProtocol.Target target, IndexResidencyPolicy policy, long nowNanos) {
    Objects.requireNonNull(target, "Index target is null");
    IndexState index = indexes.computeIfAbsent(target.indexUuid(), IndexState::new);
    index.epoch = index.epoch + 1;
    index.paused = false;
    index.accessed(0, nowNanos);
    Set<String> previous = new LinkedHashSet<>(index.targetCacheKeys);
    // A supplied policy always restarts protection; a repeated load without one keeps its deadline.
    if (policy != null) {
      index.protection = protection(policy, nowNanos);
    } else if (index.protection == null) {
      // Omitting retention on the first load means no protection, not permanent protection.
      index.protection = protection(IndexResidencyPolicy.parse("0"), nowNanos);
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
    } else if (!node.report.session().equals(report.session())) {
      // Another application reusing the executor ID starts from scratch.
      forgetIncarnation(node.report.executorId(), node.report.incarnation());
      node = new NodeState(report, clock.nowNanos());
      nodes.put(report.executorId(), node);
    } else if (!node.report.incarnation().equals(report.incarnation())) {
      // A restarted executor has an empty cache, so its old copies stop being confirmed coverage.
      forgetIncarnation(node.report.executorId(), node.report.incarnation());
      node = new NodeState(report, clock.nowNanos());
      nodes.put(report.executorId(), node);
    } else if (report.sequence() >= node.report.sequence()) {
      node.observe(report, clock.nowNanos());
    }

    // Reports carry relative ages, so the driver converts them to its own clock. The report arrival
    // time must not reset the order; only an actual access does.
    long receivedAt = clock.nowNanos();
    for (Map.Entry<String, Long> access : report.accessAgeNanos().entrySet()) {
      IndexState index = indexes.get(access.getKey());
      if (index != null) {
        index.accessed(Math.max(0, access.getValue()), receivedAt);
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
    if (node == null
        || !node.report.incarnation().equals(ack.incarnation())
        || !node.report.session().equals(ack.session())) {
      return;
    }

    PlanState plan = plans.get(ack.planId());
    if (plan == null) {
      return;
    }

    PlanFile command = plan.command(ack.commandId());
    if (command == null || command.bytes < 0) {
      return;
    }

    node.delivered(ack.commandId());
    IndexState index = indexes.get(plan.indexUuid);
    boolean ownsCommand =
        plan.terminalStatus == null
            && !plan.expired
            && !plan.fenced.contains(command.cacheKey)
            && command.incarnation.equals(ack.incarnation())
            && index != null
            && index.epoch == plan.epoch;

    if (command.type == IndexCacheProtocol.CommandType.RELEASE) {
      if (ack.status() == IndexCacheProtocol.AckStatus.RELEASED) {
        PlanFile copy = plan.loaded.get(command.cacheKey);
        plan.dropped.add(command.cacheKey);
        // A migration releases the same key on the old node; the new copy must stay confirmed.
        if (copy != null
            && copy.executorId.equals(command.executorId)
            && copy.incarnation.equals(command.incarnation)) {
          plan.loaded.remove(command.cacheKey);
        }
      }

      node.settle(ack.commandId());
      return;
    }

    if (!ownsCommand) {
      // A copy whose plan or epoch no longer exists has no owner: release it instead of leaking it.
      if (ack.status() == IndexCacheProtocol.AckStatus.LOADED) {
        releaseOrphan(plan, node, command);
      }

      node.settle(ack.commandId());
      return;
    }

    if (ack.status() == IndexCacheProtocol.AckStatus.LOADED) {
      if (plan.loaded.containsKey(command.cacheKey)) {
        // Retries and duplicate ACKs must not charge the same copy twice.
        return;
      }

      // A report may already list the copy; only the part the report is missing gets charged here.
      long delta = node.report.holders().containsKey(command.cacheKey) ? 0 : command.bytes;
      Map<String, Long> holders = new LinkedHashMap<>(node.report.holders());
      holders.put(command.cacheKey, command.bytes);
      node.report =
          node.report
              .copy()
              .holders(holders)
              .usedBytes(Math.addExact(node.report.usedBytes(), delta))
              .build();
      plan.loaded.put(command.cacheKey, command);
      plan.failed.remove(command.location);
      // The reservation becomes retained bytes; keeping it would charge the copy twice.
      node.settle(ack.commandId());
      releaseMigratedCopy(plan, command, ack.executorId());
    } else if (ack.status() == IndexCacheProtocol.AckStatus.REJECTED
        || ack.status() == IndexCacheProtocol.AckStatus.FAILED) {
      plan.failed.put(command.location, ack.reason() == null ? ack.status().name() : ack.reason());
      node.settle(ack.commandId());
    }
  }

  IndexCacheProtocol.Result result(String planId) {
    return result0(planId);
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
        : node.report
            .copy()
            .reservedBytes(node.reservedBytes())
            .reservedByPlan(node.reservedByPlan())
            .build();
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

    for (NodeState node : nodes.values()) {
      for (PlanFile planned : plan.commands.values()) {
        // Reservations of fenced commands are released here; their ACKs may never arrive.
        if (plan.fenced.contains(planned.cacheKey)) {
          node.settle(commandIdOf(plan, planned));
        }
      }
    }
  }

  /** Command identity is derived from the plan and the command's stable position in it. */
  private static String commandIdOf(PlanState plan, PlanFile command) {
    for (Map.Entry<String, PlanFile> entry : plan.commands.entrySet()) {
      if (entry.getValue() == command) {
        return entry.getKey();
      }
    }

    return "";
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
      } else if (confirmedAnywhere(file.cacheKey())) {
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
    for (Map.Entry<String, String> rejection : placement.rejected().entrySet()) {
      plan.rejected.put(
          rejection.getKey(),
          "NO_EXECUTORS".equals(rejection.getValue()) && !nodes.isEmpty()
              ? "CACHE_DISABLED"
              : rejection.getValue());
    }
    for (Map.Entry<String, String> entry : placement.placements().entrySet()) {
      IndexCacheProtocol.File file = file(target, entry.getKey());
      String executorId = entry.getValue();
      NodeState node = nodes.get(executorId);
      // Only an acknowledged copy counts; a report on its own grants nothing.
      if (node == null) {
        plan.rejected.put(file.location(), "NODE_LOST");
      } else if (confirmedOn(node.report.executorId(), file.cacheKey())) {
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
    tryEvict(plan, placement, pendingFiles);

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
    List<IndexCacheProtocol.File> files = new ArrayList<>();
    for (String location : reusable) {
      files.add(file(target, location));
    }

    // Large files first, matching the placement order, so the same target set is chosen twice.
    files.sort(
        Comparator.comparing(
                IndexCacheProtocol.File::estimatedBytes,
                Comparator.nullsLast(Comparator.reverseOrder()))
            .thenComparing(IndexCacheProtocol.File::location));
    for (IndexCacheProtocol.File file : files) {
      List<Holder> holders = holdersOfKey(file.cacheKey());
      if (holders.isEmpty() || file.estimatedBytes() == null) {
        continue;
      }

      holders.sort(Comparator.comparing(Holder::executorId));
      Holder current = holders.get(0);
      String location = file.location();
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
    return BigInteger.valueOf(occupied(left))
            .multiply(BigInteger.valueOf(right.report.budgetBytes()))
            .compareTo(
                BigInteger.valueOf(occupied(right))
                    .multiply(BigInteger.valueOf(left.report.budgetBytes())))
        < 0;
  }

  /** Occupancy used for comparisons; the planner charges the same two fields. */
  private long occupied(NodeState node) {
    return Math.addExact(node.report.usedBytes(), node.reservedBytes());
  }

  private boolean hasRoomForSecondCopy(NodeState node, IndexCacheProtocol.File file) {
    long free = node.report.budgetBytes() - occupied(node);
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
  private void tryEvict(
      PlanState plan, IndexCacheProtocol.Plan placement, List<IndexCacheProtocol.File> pending) {
    if (!plan.evicted.isEmpty()) {
      return;
    }

    Map<String, Long> deficits = capacityDeficits(placement, pending);
    if (deficits.isEmpty()) {
      // Either nothing was rejected for capacity, or the gap is a file no node could ever hold.
      return;
    }

    long nowNanos = clock.nowNanos();
    List<IndexState> candidates = new ArrayList<>();
    for (IndexState other : indexedStates()) {
      if (other.indexUuid.equals(plan.indexUuid) || other.protectedNow(nowNanos)) {
        continue;
      }

      // Only an index whose copies sit on a short node and cover its gap can help this plan.
      if (!hasInFlightLoad(other.indexUuid) && canFree(other, deficits)) {
        candidates.add(other);
      }
    }

    long schedulingNanos = clock.nowNanos();
    candidates.sort(
        Comparator.comparingLong((IndexState state) -> state.ageAt(schedulingNanos))
            .reversed()
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
    for (Map.Entry<String, String> rejection : placement.rejected().entrySet()) {
      // Only capacity gaps wait for the eviction; other reasons stay as they are.
      if ("INSUFFICIENT_CAPACITY".equals(rejection.getValue())) {
        plan.rejected.put(rejection.getKey(), "INSUFFICIENT_CAPACITY_PENDING_EVICTION");
      }
    }
  }

  /** Free bytes each short node still needs, derived from the files that could not be placed. */
  private Map<String, Long> capacityDeficits(
      IndexCacheProtocol.Plan placement, List<IndexCacheProtocol.File> pending) {
    Map<String, Long> deficits = new LinkedHashMap<>();
    for (Map.Entry<String, String> rejection : placement.rejected().entrySet()) {
      if (!"INSUFFICIENT_CAPACITY".equals(rejection.getValue())) {
        continue;
      }

      Long bytes = bytesOfPending(pending, rejection.getKey());
      if (bytes == null) {
        continue;
      }

      for (NodeState node : nodes.values()) {
        long budget = node.report.budgetBytes();
        if (budget <= 0 || bytes > budget) {
          continue;
        }

        long free = budget - node.report.usedBytes() - node.report.reservedBytes();
        if (free < bytes) {
          deficits.merge(node.report.executorId(), bytes - free, Math::max);
        }
      }
    }

    return deficits;
  }

  private static Long bytesOfPending(List<IndexCacheProtocol.File> pending, String location) {
    for (IndexCacheProtocol.File file : pending) {
      if (file.location().equals(location)) {
        return file.estimatedBytes();
      }
    }

    return null;
  }

  private boolean canFree(IndexState victim, Map<String, Long> deficits) {
    Map<String, Long> reclaimable = new LinkedHashMap<>();
    for (Holder holder : holders(victim.indexUuid)) {
      reclaimable.merge(holder.executorId(), holder.bytes(), Long::sum);
    }

    for (Map.Entry<String, Long> deficit : deficits.entrySet()) {
      if (reclaimable.getOrDefault(deficit.getKey(), 0L) >= deficit.getValue()) {
        return true;
      }
    }

    return false;
  }

  /** Nodes that could take part in a plan that has not been placed yet. */
  private Set<String> queuedTargets(PlanState plan) {
    Set<String> executors = new LinkedHashSet<>();
    for (NodeState node : nodes.values()) {
      if (node.report.budgetBytes() > 0) {
        executors.add(node.report.executorId());
      }
    }

    return executors;
  }

  /**
   * A load is queued when capacity-affecting work of an earlier, still unfinished plan is
   * outstanding on any node this plan was placed on. The two never reserve the same space.
   */
  private boolean queued(String planId) {
    PlanState plan = plans.get(planId);
    if (plan == null) {
      return false;
    }

    Set<String> executors = new LinkedHashSet<>(plan.loadExecutors());
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
        // Ownership of a node starts at planning time, not when a copy is confirmed, but a command
        // that already failed, was fenced, or is confirmed holds no space any more.
        if (inFlight.type == IndexCacheProtocol.CommandType.LOAD
            && executors.contains(inFlight.executorId)
            && !other.loaded.containsKey(inFlight.cacheKey)
            && !other.fenced.contains(inFlight.cacheKey)
            && !other.failed.containsKey(inFlight.location)) {
          return true;
        }
      }
    }

    return false;
  }

  /** True when every issued load already has a terminal result, so nothing is still awaited. */
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
    enqueue(plan, node, label, type, indexUuid, mode, file, bytes, plan.epoch);
  }

  private void enqueue(
      PlanState plan,
      NodeState node,
      String label,
      IndexCacheProtocol.CommandType type,
      String indexUuid,
      String mode,
      IndexCacheProtocol.File file,
      long bytes,
      long epoch) {
    String commandId = plan.planId + "#" + (plan.nextAttempt++) + "-" + label;
    IndexCacheProtocol.Command command =
        IndexCacheProtocol.Command.builder()
            .session(node.report.session())
            .executorId(node.report.executorId())
            .incarnation(node.report.incarnation())
            .indexUuid(indexUuid)
            .epoch(epoch)
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
            type,
            epoch));
    if (type == IndexCacheProtocol.CommandType.LOAD) {
      node.reserve(plan.planId, commandId, bytes);
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
      long others = Math.subtractExact(node.reservedBytes(), node.reservedBytesFor(planId));

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

  /**
   * Drops every trace of an incarnation that no longer exists: confirmed copies stop counting and
   * outstanding commands can never be delivered, so they are fenced instead of leaving the plan
   * waiting forever.
   */
  private void forgetIncarnation(String executorId, String incarnation) {
    for (PlanState plan : plans.values()) {
      for (PlanFile loaded : new ArrayList<>(plan.loaded.values())) {
        if (loaded.executorId.equals(executorId) && loaded.incarnation.equals(incarnation)) {
          plan.loaded.remove(loaded.cacheKey);
          plan.failed.put(loaded.location, "NODE_LOST");
        }
      }

      for (PlanFile planned : plan.commands.values()) {
        if (!planned.executorId.equals(executorId)
            || !planned.incarnation.equals(incarnation)
            || planned.type != IndexCacheProtocol.CommandType.LOAD
            || plan.loaded.containsKey(planned.cacheKey)
            || plan.failed.containsKey(planned.location)) {
          continue;
        }

        plan.fenced.add(planned.cacheKey);
        plan.failed.put(planned.location, "NODE_LOST");
      }
    }
  }

  /**
   * Releases a copy whose owning plan can no longer use it. The tombstone keeps the epoch under
   * which the copy was installed, so an executor can never confuse it with authority over a newer
   * load of the same key, and the same directory may only be released once per plan.
   */
  private void releaseOrphan(PlanState plan, NodeState node, PlanFile orphan) {
    if (!plan.orphansReleased.add(orphan.cacheKey)) {
      return;
    }

    enqueue(
        plan,
        node,
        "ORPHAN_RELEASE",
        IndexCacheProtocol.CommandType.RELEASE,
        plan.indexUuid,
        "ORPHAN",
        IndexCacheProtocol.File.builder()
            .location(orphan.location)
            .cacheKey(orphan.cacheKey)
            .build(),
        orphan.bytes,
        orphan.epoch);
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
    Set<String> lost = lostIncarnations();
    for (PlanState plan : plans.values()) {
      PlanFile loaded = plan.loaded.get(cacheKey);
      if (loaded != null
          && !plan.dropped.contains(cacheKey)
          && !lost.contains(loaded.incarnation)) {
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
