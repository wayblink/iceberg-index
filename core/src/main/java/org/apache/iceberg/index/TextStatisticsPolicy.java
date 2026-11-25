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
package org.apache.iceberg.index;

import java.util.Locale;

/**
 * What a text index build does about the corpus-wide statistics BM25 scores with.
 *
 * <p>A snapshot's statistics are the document frequency of every term in the corpus and the corpus
 * totals. They can be merged once, at build time, into one file that every query reads; or every
 * shard's dictionary can contribute its own counts to the terms a query carries, at query time.
 * Merging is cheaper per query but costs one pass over every shard dictionary while the index is
 * built, which is a size the build cannot always hold: a corpus of a hundred million documents
 * measures tens of gigabytes of shard dictionaries. Summing at query time holds no corpus-wide
 * structure at all, and pays per query instead.
 *
 * <p>The policy is a property of the index, chosen when the index is built: it describes the
 * artifact rather than switching a behaviour on at query time. A query reads the merged file when
 * the snapshot has one and sums its own terms when it does not.
 */
public enum TextStatisticsPolicy {
  /** Merge the statistics when their size stays within the build's own limit. */
  AUTO,
  /** Merge the statistics whatever their size; a large corpus can run out of memory doing it. */
  ALWAYS,
  /** Never merge them; every query sums the document frequency of its own terms. */
  NEVER;

  /** The canonical name, which is also the value stored in the index properties. */
  public String value() {
    return name().toLowerCase(Locale.ROOT);
  }

  /**
   * Parses the value a caller passes to a build, accepting any case and surrounding space.
   *
   * @throws IllegalArgumentException when the value names none of the policies
   */
  public static TextStatisticsPolicy parse(String value) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(
          "Invalid text statistics policy: " + value + " (expected auto, always or never)");
    }

    return switch (value.trim().toLowerCase(Locale.ROOT)) {
      case "auto" -> AUTO;
      case "always" -> ALWAYS;
      case "never" -> NEVER;
      default ->
          throw new IllegalArgumentException(
              "Invalid text statistics policy: " + value + " (expected auto, always or never)");
    };
  }
}
