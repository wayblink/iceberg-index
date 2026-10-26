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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/** Index target ownership, guarded by the executor cache's map lock. */
final class IndexCacheOwnership {
  private record Target(long epoch, boolean paused, Set<String> keys) {}

  private final Map<String, Target> targets = new HashMap<>();
  private final Map<String, Set<String>> ownersByKey = new HashMap<>();
  private final Set<String> managedKeys = new HashSet<>();

  void applyTarget(String indexUuid, long epoch, Set<String> cacheKeys) {
    validate(indexUuid, epoch);
    Preconditions.checkArgument(cacheKeys != null, "Index target keys are null");
    Target previous = targets.get(indexUuid);
    if (previous != null && epoch <= previous.epoch()) {
      return;
    }

    Set<String> keys = Set.copyOf(cacheKeys);
    removeOwner(indexUuid, previous);
    targets.put(indexUuid, new Target(epoch, false, keys));
    managedKeys.addAll(keys);
    for (String key : keys) {
      ownersByKey.computeIfAbsent(key, ignored -> new HashSet<>()).add(indexUuid);
    }
  }

  void offload(String indexUuid, long epoch) {
    validate(indexUuid, epoch);
    Target previous = targets.get(indexUuid);
    if (previous != null && epoch < previous.epoch()) {
      return;
    }

    removeOwner(indexUuid, previous);
    targets.put(indexUuid, new Target(epoch, true, previous == null ? Set.of() : previous.keys()));
  }

  boolean mayAdmit(String indexUuid, long epoch, String cacheKey) {
    Target target = targets.get(indexUuid);
    return target != null
        && !target.paused()
        && target.epoch() == epoch
        && target.keys().contains(cacheKey);
  }

  void release(String indexUuid, long epoch) {
    validate(indexUuid, epoch);
    Target target = targets.get(indexUuid);
    if (target != null && target.paused() && target.epoch() == epoch) {
      targets.put(indexUuid, new Target(epoch, true, Set.of()));
    }
  }

  /**
   * Adds keys to an existing target at the same epoch. A target replacement uses a newer epoch; a
   * growing target is how one plan's files arrive as separate commands.
   */
  void addKeys(String indexUuid, long epoch, Set<String> cacheKeys) {
    validate(indexUuid, epoch);
    Preconditions.checkArgument(cacheKeys != null, "Index target keys are null");
    Target target = targets.get(indexUuid);
    if (target == null || target.epoch() != epoch) {
      return;
    }

    Set<String> keys = new HashSet<>(target.keys());
    keys.addAll(cacheKeys);
    targets.put(indexUuid, new Target(epoch, target.paused(), Set.copyOf(keys)));
    managedKeys.addAll(cacheKeys);
    for (String key : cacheKeys) {
      ownersByKey.computeIfAbsent(key, ignored -> new HashSet<>()).add(indexUuid);
    }
  }

  /**
   * Withdraws one key from an index's current target; other keys and owners stay untouched.
   *
   * <p>The caller names the index and the file, not the epoch: a release is about ownership of a
   * file, and the target the executor holds may already be newer than the plan that asked for the
   * release.
   */
  boolean releaseKey(String indexUuid, String cacheKey) {
    validate(indexUuid, 0L);
    Target target = targets.get(indexUuid);
    if (target == null || !target.keys().contains(cacheKey)) {
      return false;
    }

    long epoch = target.epoch();

    Set<String> keys = new HashSet<>(target.keys());
    keys.remove(cacheKey);
    targets.put(indexUuid, new Target(epoch, target.paused(), Set.copyOf(keys)));
    ownersByKey.computeIfPresent(
        cacheKey,
        (ignored, owners) -> {
          owners.remove(indexUuid);
          return owners.isEmpty() ? null : owners;
        });
    return true;
  }

  Set<String> owners(String cacheKey) {
    return Set.copyOf(ownersByKey.getOrDefault(cacheKey, Set.of()));
  }

  boolean isOwned(String cacheKey) {
    return ownersByKey.containsKey(cacheKey);
  }

  /** True when some index owns this key and none of them has paused admission. */
  boolean admits(String cacheKey) {
    for (String owner : ownersByKey.getOrDefault(cacheKey, Set.of())) {
      Target target = targets.get(owner);
      if (target != null && !target.paused()) {
        return true;
      }
    }

    return false;
  }

  boolean isManagedKey(String cacheKey) {
    return managedKeys.contains(cacheKey);
  }

  Set<String> keys(String indexUuid) {
    Target target = targets.get(indexUuid);
    return target == null ? Set.of() : target.keys();
  }

  void clear() {
    targets.clear();
    ownersByKey.clear();
    managedKeys.clear();
  }

  private void removeOwner(String indexUuid, Target target) {
    if (target == null) {
      return;
    }

    for (String key : target.keys()) {
      ownersByKey.computeIfPresent(
          key,
          (ignored, owners) -> {
            owners.remove(indexUuid);
            return owners.isEmpty() ? null : owners;
          });
    }
  }

  private static void validate(String indexUuid, long epoch) {
    Preconditions.checkArgument(
        indexUuid != null && !indexUuid.isBlank(), "Index identity is empty");
    Preconditions.checkArgument(epoch >= 0, "Index policy epoch is negative: %s", epoch);
  }
}
