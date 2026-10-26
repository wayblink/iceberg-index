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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

final class IndexPlacementPlanner {
  IndexCacheProtocol.Plan plan(
      IndexCacheProtocol.Target target, List<IndexCacheProtocol.NodeReport> reports) {
    Preconditions.checkArgument(target != null, "Index target is missing");
    Preconditions.checkArgument(reports != null, "Executor reports are missing");
    List<WorkingNode> nodes = new ArrayList<>();
    Set<String> identities = new HashSet<>();
    for (IndexCacheProtocol.NodeReport report : reports) {
      Preconditions.checkArgument(report != null, "Executor report is null");
      Preconditions.checkArgument(
          identities.add(report.executorId()), "Duplicate executor: %s", report.executorId());
      if (report.budgetBytes() > 0) {
        nodes.add(new WorkingNode(report));
      }
    }

    nodes.sort(Comparator.comparing(node -> node.report.executorId()));
    List<IndexCacheProtocol.File> files = new ArrayList<>(target.files());
    files.sort(
        Comparator.comparing(
                IndexCacheProtocol.File::estimatedBytes,
                Comparator.nullsLast(Comparator.reverseOrder()))
            .thenComparing(IndexCacheProtocol.File::location));
    Map<String, String> placements = new LinkedHashMap<>();
    Map<String, String> rejected = new LinkedHashMap<>();
    Map<String, WorkingNode> chosen = new LinkedHashMap<>();
    for (IndexCacheProtocol.File file : files) {
      if (file.estimatedBytes() == null) {
        rejected.put(file.location(), "SIZE_UNKNOWN");
        continue;
      }

      WorkingNode node = best(file, nodes, true);
      if (node == null) {
        node = best(file, nodes, false);
      }

      if (node != null) {
        node.reserve(file);
        placements.put(file.location(), node.report.executorId());
        chosen.put(file.location(), node);
      } else {
        rejected.put(file.location(), reason(file, nodes));
      }
    }

    // Reserve uncovered files before spending space on a second copy of an existing file.
    for (IndexCacheProtocol.File file : files) {
      WorkingNode current = chosen.get(file.location());
      if (current == null || !current.holds(file)) {
        continue;
      }

      WorkingNode destination = best(file, nodes, false);
      if (destination != null && destination != current) {
        destination.reserve(file);
        placements.put(file.location(), destination.report.executorId());
        // The old copy remains charged until an actual release is acknowledged.
      }
    }

    return IndexCacheProtocol.Plan.builder()
        .indexUuid(target.indexUuid())
        .snapshotId(target.snapshotId())
        .files(files)
        .placements(placements)
        .rejected(rejected)
        .build();
  }

  private static WorkingNode best(
      IndexCacheProtocol.File file, List<WorkingNode> nodes, boolean existingOnly) {
    WorkingNode best = null;
    for (WorkingNode node : nodes) {
      if ((!existingOnly || node.holds(file))
          && node.fits(file)
          && (best == null || compare(file, node, best) < 0)) {
        best = node;
      }
    }

    return best;
  }

  private static int compare(IndexCacheProtocol.File file, WorkingNode left, WorkingNode right) {
    int ratio =
        BigInteger.valueOf(left.after(file))
            .multiply(BigInteger.valueOf(right.report.budgetBytes()))
            .compareTo(
                BigInteger.valueOf(right.after(file))
                    .multiply(BigInteger.valueOf(left.report.budgetBytes())));
    if (ratio != 0) {
      return ratio;
    }

    if (left.holds(file) != right.holds(file)) {
      return left.holds(file) ? -1 : 1;
    }

    return left.report.executorId().compareTo(right.report.executorId());
  }

  private static String reason(IndexCacheProtocol.File file, List<WorkingNode> nodes) {
    if (nodes.isEmpty()) {
      return "NO_EXECUTORS";
    }

    for (WorkingNode node : nodes) {
      if (file.estimatedBytes() <= node.report.budgetBytes()) {
        return "INSUFFICIENT_CAPACITY";
      }
    }

    return "FILE_TOO_LARGE";
  }

  private static final class WorkingNode {
    private final IndexCacheProtocol.NodeReport report;
    private long occupied;

    private WorkingNode(IndexCacheProtocol.NodeReport report) {
      this.report = report;
      this.occupied = Math.addExact(report.usedBytes(), report.reservedBytes());
    }

    private boolean holds(IndexCacheProtocol.File file) {
      return report.holders().containsKey(file.cacheKey());
    }

    private boolean fits(IndexCacheProtocol.File file) {
      return occupied <= report.budgetBytes() && extra(file) <= report.budgetBytes() - occupied;
    }

    private long extra(IndexCacheProtocol.File file) {
      return holds(file) ? 0 : file.estimatedBytes();
    }

    private long after(IndexCacheProtocol.File file) {
      return Math.addExact(occupied, extra(file));
    }

    private void reserve(IndexCacheProtocol.File file) {
      this.occupied = after(file);
    }
  }
}
