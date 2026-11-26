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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/** Driver-visible settings of the control channel; every value must be positive. */
final class IndexCacheWorkerConfig {
  static final String PREFIX = "spark.iceberg.index.cache.";
  static final String POLL_INTERVAL = PREFIX + "control.poll.interval";
  static final String LEGACY_POLL_INTERVAL = PREFIX + "control.poll-interval";
  static final String MAX_COMMANDS = PREFIX + "control.max.commands";
  static final String LEGACY_MAX_COMMANDS = PREFIX + "control.max-commands";
  static final String MAX_CONCURRENT = PREFIX + "load.max.concurrent";
  static final String LEGACY_MAX_CONCURRENT = PREFIX + "load.max-concurrent";
  static final String TIMEOUT = PREFIX + "load.timeout";
  static final String MAX_ATTEMPTS = PREFIX + "load.max.attempts";
  static final String LEGACY_MAX_ATTEMPTS = PREFIX + "load.max-attempts";

  private final long pollMillis;
  private final int maxCommands;
  private final int maxConcurrentLoads;
  private final long operationTimeoutMillis;
  private final int maxAttempts;

  private IndexCacheWorkerConfig(
      long pollMillis,
      int maxCommands,
      int maxConcurrentLoads,
      long operationTimeoutMillis,
      int maxAttempts) {
    this.pollMillis = pollMillis;
    this.maxCommands = maxCommands;
    this.maxConcurrentLoads = maxConcurrentLoads;
    this.operationTimeoutMillis = operationTimeoutMillis;
    this.maxAttempts = maxAttempts;
  }

  static IndexCacheWorkerConfig parse(Map<String, String> settings) {
    Map<String, String> values = settings == null ? Map.of() : settings;
    return new IndexCacheWorkerConfig(
        seconds(values, POLL_INTERVAL, LEGACY_POLL_INTERVAL, 1),
        (int) positive(values, MAX_COMMANDS, LEGACY_MAX_COMMANDS, 64),
        (int) positive(values, MAX_CONCURRENT, LEGACY_MAX_CONCURRENT, 1),
        seconds(values, TIMEOUT, null, 600),
        (int) positive(values, MAX_ATTEMPTS, LEGACY_MAX_ATTEMPTS, 3));
  }

  /**
   * The value of a setting and the name it was set under.
   *
   * <p>A renamed setting is still read, so a run that carries the old name is obeyed; the name is
   * kept so an error can quote the key the operator actually wrote. Within one configuration the
   * new name answers when both are set.
   */
  private static final class Setting {
    private final String value;
    private final String key;

    private Setting(String value, String key) {
      this.value = value;
      this.key = key;
    }

    private String value() {
      return value;
    }

    private String key() {
      return key;
    }
  }

  private static Setting find(Map<String, String> values, String key, String legacyKey) {
    Setting fromNew = nonBlank(values, key);
    return fromNew != null ? fromNew : nonBlank(values, legacyKey);
  }

  private static Setting nonBlank(Map<String, String> values, String key) {
    if (key == null) {
      return null;
    }

    String raw = values.get(key);
    return raw == null || raw.isBlank() ? null : new Setting(raw, key);
  }

  private static long seconds(
      Map<String, String> values, String key, String legacyKey, long fallbackSeconds) {
    Setting setting = find(values, key, legacyKey);
    if (setting == null) {
      return fallbackSeconds * 1_000L;
    }

    long seconds;
    try {
      seconds = Long.parseLong(setting.value().trim());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          String.format("Invalid %s: %s (expected whole seconds)", setting.key(), setting.value()),
          e);
    }

    Preconditions.checkArgument(
        seconds > 0, "Invalid %s: %s (must be positive)", setting.key(), setting.value());
    return Math.multiplyExact(seconds, 1_000L);
  }

  private static long positive(
      Map<String, String> values, String key, String legacyKey, long fallback) {
    Setting setting = find(values, key, legacyKey);
    if (setting == null) {
      return fallback;
    }

    long value;
    try {
      value = Long.parseLong(setting.value().trim());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          String.format("Invalid %s: %s", setting.key(), setting.value()), e);
    }

    Preconditions.checkArgument(
        value > 0, "Invalid %s: %s (must be positive)", setting.key(), setting.value());
    Preconditions.checkArgument(
        value <= Integer.MAX_VALUE, "Invalid %s: %s (too large)", setting.key(), setting.value());
    return value;
  }

  long pollMillis() {
    return pollMillis;
  }

  int maxCommands() {
    return maxCommands;
  }

  int maxConcurrentLoads() {
    return maxConcurrentLoads;
  }

  long operationTimeoutMillis() {
    return operationTimeoutMillis;
  }

  int maxAttempts() {
    return maxAttempts;
  }
}
