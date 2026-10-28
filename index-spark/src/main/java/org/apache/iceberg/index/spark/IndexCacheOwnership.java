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

import java.util.Map;
import java.util.Set;
import org.apache.iceberg.index.spark.IndexCacheMetadata.Component;
import org.apache.iceberg.index.spark.IndexCacheMetadata.Components;
import org.apache.iceberg.index.spark.IndexCacheMetadata.OwnedMap;
import org.apache.iceberg.index.spark.IndexCacheMetadata.OwnedSet;
import org.apache.iceberg.index.spark.IndexCacheMetadata.Texts;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/** Index target ownership, guarded by the executor cache's map lock. */
final class IndexCacheOwnership implements Component {
  private record Target(long epoch, boolean paused, Set<String> keys) implements Component {
    @Override
    public void charge(Components components) {
      if (components.object(this, 32)) {
        components.add(keys);
      }
    }
  }

  private final Texts texts;
  private final Map<String, Target> targets = new OwnedMap<>();
  private final Map<String, Set<String>> ownersByKey = new OwnedMap<>();
  private final Set<String> managedKeys = new OwnedSet<>();

  IndexCacheOwnership() {
    this(new Texts());
  }

  IndexCacheOwnership(Texts texts) {
    this.texts = texts;
  }

  private Set<String> own(Set<String> source) {
    Set<String> result = new OwnedSet<>();
    for (String key : source) {
      result.add(texts.own(key));
    }
    return result;
  }

  @Override
  public void charge(Components components) {
    if ((!targets.isEmpty() || !managedKeys.isEmpty()) && components.object(this, 32)) {
      components.root(targets);
      components.root(ownersByKey);
      components.root(managedKeys);
    }
  }

  int groupRefs() {
    int result = 0;
    for (Set<String> owners : ownersByKey.values()) {
      result = Math.addExact(result, owners.size());
    }
    return result;
  }

  void applyTarget(String indexUuid, long epoch, Set<String> cacheKeys) {
    validate(indexUuid, epoch);
    Preconditions.checkArgument(cacheKeys != null, "Index target keys are null");
    Target previous = targets.get(indexUuid);
    if (previous != null && epoch <= previous.epoch()) {
      return;
    }
    Set<String> keys = own(cacheKeys);
    String owner = texts.own(indexUuid);
    removeOwner(owner, previous);
    targets.put(owner, new Target(epoch, false, keys));
    managedKeys.addAll(keys);
    for (String key : keys) {
      ownersByKey.computeIfAbsent(key, ignored -> new OwnedSet<>()).add(owner);
    }
  }

  void offload(String indexUuid, long epoch) {
    validate(indexUuid, epoch);
    Target previous = targets.get(indexUuid);
    if (previous != null && epoch < previous.epoch()) {
      return;
    }
    String owner = texts.own(indexUuid);
    removeOwner(owner, previous);
    targets.put(
        owner, new Target(epoch, true, previous == null ? new OwnedSet<>() : previous.keys()));
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
      targets.put(texts.own(indexUuid), new Target(epoch, true, new OwnedSet<>()));
    }
  }

  /** Adds keys at the same epoch; replacement requires a newer epoch. */
  void addKeys(String indexUuid, long epoch, Set<String> cacheKeys) {
    validate(indexUuid, epoch);
    Preconditions.checkArgument(cacheKeys != null, "Index target keys are null");
    Target target = targets.get(indexUuid);
    if (target == null || target.epoch() != epoch) {
      return;
    }
    Set<String> keys = own(target.keys());
    Set<String> added = own(cacheKeys);
    keys.addAll(added);
    String owner = texts.own(indexUuid);
    targets.put(owner, new Target(epoch, target.paused(), keys));
    managedKeys.addAll(added);
    for (String key : added) {
      ownersByKey.computeIfAbsent(key, ignored -> new OwnedSet<>()).add(owner);
    }
  }

  /** Withdraws one key without changing other files or owners. */
  boolean releaseKey(String indexUuid, String cacheKey) {
    validate(indexUuid, 0L);
    Target target = targets.get(indexUuid);
    if (target == null || !target.keys().contains(cacheKey)) {
      return false;
    }
    Set<String> keys = own(target.keys());
    keys.remove(cacheKey);
    targets.put(texts.own(indexUuid), new Target(target.epoch(), target.paused(), keys));
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

  /** True when at least one owning index admits loads. */
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
    return target == null ? Set.of() : Set.copyOf(target.keys());
  }

  void clear() {
    targets.clear();
    ownersByKey.clear();
    managedKeys.clear();
  }

  private void removeOwner(String indexUuid, Target target) {
    if (target != null) {
      for (String key : target.keys()) {
        ownersByKey.computeIfPresent(
            key,
            (ignored, owners) -> {
              owners.remove(indexUuid);
              return owners.isEmpty() ? null : owners;
            });
      }
    }
  }

  private static void validate(String indexUuid, long epoch) {
    Preconditions.checkArgument(
        indexUuid != null && !indexUuid.isBlank(), "Index identity is empty");
    Preconditions.checkArgument(epoch >= 0, "Index policy epoch is negative: %s", epoch);
  }
}
