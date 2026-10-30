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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Handles control messages on the driver. It is Spark-free so the protocol can be exercised without
 * a cluster; the plugin only forwards messages to it.
 */
final class IndexCacheDriverControl {
  private final IndexResidencyCoordinator coordinator;

  IndexCacheDriverControl(IndexResidencyCoordinator coordinator) {
    this.coordinator = Objects.requireNonNull(coordinator, "Coordinator is null");
  }

  /**
   * Returns a reply for a report, and an empty reply for an ACK.
   *
   * <p>Spark delivers plugin messages from a pool of driver threads, so this is the serialization
   * point for the coordinator's state machine.
   */
  synchronized IndexCacheProtocol.Reply receive(Object message) {
    Preconditions.checkArgument(message != null, "Control message is null");
    if (message instanceof IndexCacheProtocol.NodeReport report) {
      // a report is the only moment a node says it is still there, so this is where the ones that
      // stopped are marked as lost: a copy on an executor that went away must stop counting as
      // covered before the next plan is placed from it
      coordinator.refresh(nowNanos());
      return coordinator.report(report);
    } else if (message instanceof IndexCacheProtocol.Ack ack) {
      coordinator.ack(ack);
      return IndexCacheProtocol.Reply.builder().commands(List.of()).build();
    }

    throw new IllegalArgumentException(
        "Unsupported control message: " + message.getClass().getName());
  }

  // Every state-machine entry point is serialized here: Spark delivers reports and ACKs on a pool
  // of driver threads while a SQL call or a query may touch the same coordinator.

  synchronized String acceptLoad(
      IndexCacheProtocol.Target target, IndexResidencyPolicy policy, long nowNanos) {
    return coordinator.acceptLoad(target, policy, nowNanos);
  }

  synchronized String acceptOffload(String indexUuid, long nowNanos) {
    return coordinator.acceptOffload(indexUuid, nowNanos);
  }

  synchronized IndexCacheProtocol.Result result(String planId) {
    return coordinator.result(planId);
  }

  /**
   * Ends a plan whose deadline passed, and answers with its final state. A caller that stops
   * waiting calls this instead of simply giving up, so the plan does not keep the space it promised
   * nor the promises an executor may still answer.
   */
  synchronized IndexCacheProtocol.Result expire(String planId, long nowNanos) {
    coordinator.expire(planId, nowNanos);
    return coordinator.result(planId);
  }

  synchronized void recordAccess(String indexUuid, long nowNanos) {
    coordinator.recordAccess(indexUuid, nowNanos);
  }

  synchronized Map<String, String> placements(String planId) {
    return coordinator.placements(planId);
  }

  synchronized Map<String, Long> plannedBytes(String planId) {
    return coordinator.plannedBytes(planId);
  }

  synchronized Map<String, Long> accessAges(long nowNanos) {
    return coordinator.accessAges(nowNanos);
  }

  synchronized Map<String, IndexCacheProtocol.Protection> protectedIndexes() {
    return coordinator.protectedIndexes();
  }

  synchronized List<IndexCacheProtocol.NodeReport> nodeReports() {
    return List.copyOf(coordinator.nodeReports().values());
  }

  synchronized Map<String, Long> reportAges(long nowNanos) {
    return coordinator.nodeReportAgeNanos(nowNanos);
  }

  synchronized long nowNanos() {
    return coordinator.nowNanos();
  }

  /** True when at least one executor reported recently enough to answer a command. */
  synchronized boolean hasLiveExecutors(long nowNanos) {
    for (long age : coordinator.nodeReportAgeNanos(nowNanos).values()) {
      if (age <= IndexResidencyCoordinator.STALE_AFTER_NANOS) {
        return true;
      }
    }

    return false;
  }

  /** Only for tests: production paths must use the synchronized entry points above. */
  IndexResidencyCoordinator coordinator() {
    return coordinator;
  }
}
