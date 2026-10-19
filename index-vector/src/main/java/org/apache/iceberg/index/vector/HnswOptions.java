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
package org.apache.iceberg.index.vector;

import java.util.Locale;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

/**
 * The parameters an HNSW graph is built and searched with.
 *
 * <p>These are properties of the definition rather than fields on it: a definition describes what
 * kind of index is wanted, and the parameters belong to the structure that was chosen. Keeping them
 * as fields on the definition means every structure added later has to add its own fields to a
 * class they all share.
 *
 * <p>The keys are kebab-case, like every other property in the index layer. They are read through
 * this class so that a missing one takes its default and a malformed one fails here, naming the
 * key, rather than somewhere deep in a build.
 */
public final class HnswOptions implements java.io.Serializable {

  private static final long serialVersionUID = 1L;

  /** The graph degree: how many neighbours each node keeps on the layer it was inserted at. */
  public static final String MAX_M = "max-m";

  /** How many candidates the build considers while wiring a new node into the graph. */
  public static final String EF_CONSTRUCTION = "ef-construction";

  /** How many candidates a search keeps while descending, which trades recall for latency. */
  public static final String EF_SEARCH = "ef-search";

  private static final int DEFAULT_MAX_M = 16;
  private static final int DEFAULT_EF_CONSTRUCTION = 200;
  private static final int DEFAULT_EF_SEARCH = 50;

  private static final ImmutableSet<String> KEYS =
      ImmutableSet.of(MAX_M, EF_CONSTRUCTION, EF_SEARCH);

  private final int maxM;
  private final int efConstruction;
  private final int efSearch;

  private HnswOptions(int maxM, int efConstruction, int efSearch) {
    Preconditions.checkArgument(maxM > 0, "Invalid HNSW %s: %s", MAX_M, maxM);
    Preconditions.checkArgument(
        efConstruction > 0, "Invalid HNSW %s: %s", EF_CONSTRUCTION, efConstruction);
    Preconditions.checkArgument(efSearch > 0, "Invalid HNSW %s: %s", EF_SEARCH, efSearch);
    this.maxM = maxM;
    this.efConstruction = efConstruction;
    this.efSearch = efSearch;
  }

  /** The property keys this structure reads, for a caller that has to report what it supports. */
  public static ImmutableSet<String> keys() {
    return KEYS;
  }

  /** Reads the options, taking the default for each one the properties do not set. */
  public static HnswOptions from(Map<String, String> properties) {
    Preconditions.checkNotNull(properties, "Index properties are null");
    // a key this structure does not read is a mistake worth naming: it is either a typo or an
    // option of a different structure, and silently ignoring it hides both
    for (String key : properties.keySet()) {
      Preconditions.checkArgument(
          KEYS.contains(key),
          "Unknown HNSW option %s: this structure reads %s",
          key,
          Sets.newTreeSet(KEYS));
    }

    return new HnswOptions(
        intOption(properties, MAX_M, DEFAULT_MAX_M),
        intOption(properties, EF_CONSTRUCTION, DEFAULT_EF_CONSTRUCTION),
        intOption(properties, EF_SEARCH, DEFAULT_EF_SEARCH));
  }

  public int maxM() {
    return maxM;
  }

  public int efConstruction() {
    return efConstruction;
  }

  public int efSearch() {
    return efSearch;
  }

  private static int intOption(Map<String, String> properties, String key, int defaultValue) {
    String value = properties.get(key);
    if (value == null) {
      return defaultValue;
    }

    try {
      return Integer.parseInt(value.trim());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          String.format(Locale.ROOT, "HNSW option %s is not a number: %s", key, value), e);
    }
  }
}
