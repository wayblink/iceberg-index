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

import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/** The protection duration requested for one index; the driver assigns its deadline. */
final class IndexResidencyPolicy {
  private static final Pattern FINITE = Pattern.compile("([0-9]+)([smhd])");
  private static final IndexResidencyPolicy ZERO = new IndexResidencyPolicy(false, 0);
  private static final IndexResidencyPolicy FOREVER = new IndexResidencyPolicy(true, 0);

  private final boolean forever;
  private final long durationNanos;

  private IndexResidencyPolicy(boolean forever, long durationNanos) {
    this.forever = forever;
    this.durationNanos = durationNanos;
  }

  static IndexResidencyPolicy parse(String value) {
    if (value == null) {
      return null;
    }

    String normalized = value.trim().toLowerCase(Locale.ROOT);
    if ("0".equals(normalized)) {
      return ZERO;
    } else if ("forever".equals(normalized)) {
      return FOREVER;
    }

    Matcher matcher = FINITE.matcher(normalized);
    Preconditions.checkArgument(matcher.matches(), "Invalid index retention period: %s", value);
    try {
      long amount = Long.parseLong(matcher.group(1));
      Preconditions.checkArgument(amount > 0, "Invalid index retention period: %s", value);
      long scale =
          switch (matcher.group(2)) {
            case "s" -> TimeUnit.SECONDS.toNanos(1);
            case "m" -> TimeUnit.MINUTES.toNanos(1);
            case "h" -> TimeUnit.HOURS.toNanos(1);
            case "d" -> TimeUnit.DAYS.toNanos(1);
            default ->
                throw new IllegalArgumentException("Invalid index retention period: " + value);
          };
      return new IndexResidencyPolicy(false, Math.multiplyExact(amount, scale));
    } catch (NumberFormatException | ArithmeticException e) {
      throw new IllegalArgumentException("Invalid index retention period: " + value, e);
    }
  }

  boolean forever() {
    return forever;
  }

  /** Returns the finite duration, or zero for indefinite protection. */
  long durationNanos() {
    return durationNanos;
  }
}
