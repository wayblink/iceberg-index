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
  static final String POLL_INTERVAL = PREFIX + "control.poll-interval";
  static final String MAX_COMMANDS = PREFIX + "control.max-commands";
  static final String MAX_CONCURRENT = PREFIX + "load.max-concurrent";
  static final String TIMEOUT = PREFIX + "load.timeout";
  static final String MAX_ATTEMPTS = PREFIX + "load.max-attempts";

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
        seconds(values, POLL_INTERVAL, 1),
        (int) positive(values, MAX_COMMANDS, 64),
        (int) positive(values, MAX_CONCURRENT, 1),
        seconds(values, TIMEOUT, 600),
        (int) positive(values, MAX_ATTEMPTS, 3));
  }

  private static long seconds(Map<String, String> values, String key, long fallbackSeconds) {
    String raw = values.get(key);
    if (raw == null || raw.isBlank()) {
      return fallbackSeconds * 1_000L;
    }

    long seconds;
    try {
      seconds = Long.parseLong(raw.trim());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          String.format("Invalid %s: %s (expected whole seconds)", key, raw), e);
    }

    Preconditions.checkArgument(seconds > 0, "Invalid %s: %s (must be positive)", key, raw);
    return Math.multiplyExact(seconds, 1_000L);
  }

  private static long positive(Map<String, String> values, String key, long fallback) {
    String raw = values.get(key);
    if (raw == null || raw.isBlank()) {
      return fallback;
    }

    long value;
    try {
      value = Long.parseLong(raw.trim());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(String.format("Invalid %s: %s", key, raw), e);
    }

    Preconditions.checkArgument(value > 0, "Invalid %s: %s (must be positive)", key, raw);
    Preconditions.checkArgument(value <= Integer.MAX_VALUE, "Invalid %s: %s (too large)", key, raw);
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
