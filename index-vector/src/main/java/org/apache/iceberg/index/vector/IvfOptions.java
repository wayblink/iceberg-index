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
 * The parameters an IVF index is built and searched with.
 *
 * <p>Both are properties of the definition, read through this class so that a missing one takes its
 * default and a malformed one fails here, naming the key.
 */
public final class IvfOptions implements java.io.Serializable {

  private static final long serialVersionUID = 1L;

  /** How many lists the vectors are partitioned into; zero means derive it from the entry count. */
  public static final String NLIST = "nlist";

  /** How many of those lists a search visits. */
  public static final String NPROBE = "nprobe";

  private static final int DEFAULT_NPROBE = 8;

  /**
   * How many training samples each list gets, following the usual heuristic.
   *
   * <p>More samples fit the centroids better and cost a pass over the sample per iteration; this
   * many per list is enough to estimate the partitions without training on the whole table.
   */
  private static final int SAMPLES_PER_LIST = 39;

  private static final ImmutableSet<String> KEYS = ImmutableSet.of(NLIST, NPROBE);

  private final int nlist;
  private final int nprobe;

  private IvfOptions(int nlist, int nprobe) {
    Preconditions.checkArgument(nlist >= 0, "Invalid IVF %s: %s", NLIST, nlist);
    Preconditions.checkArgument(nprobe > 0, "Invalid IVF %s: %s", NPROBE, nprobe);
    this.nlist = nlist;
    this.nprobe = nprobe;
  }

  /** The property keys this structure reads, for a caller that has to report what it supports. */
  public static ImmutableSet<String> keys() {
    return KEYS;
  }

  /** Reads the options, taking the default for each one the properties do not set. */
  public static IvfOptions from(Map<String, String> properties) {
    return from(properties, VectorIndexDefinition.VectorQuantizer.NONE);
  }

  /**
   * Reads the options of a structure that is going to store its vectors with {@code quantizer}.
   *
   * <p>The quantizer is a separate axis and has options of its own, so those are accepted here too:
   * a property is rejected when nothing about this combination reads it, not when this class alone
   * does not.
   */
  public static IvfOptions from(
      Map<String, String> properties, VectorIndexDefinition.VectorQuantizer quantizer) {
    Preconditions.checkNotNull(properties, "Index properties are null");
    java.util.Set<String> readable = Sets.newLinkedHashSet(KEYS);
    if (quantizer == VectorIndexDefinition.VectorQuantizer.RABITQ) {
      readable.add(RaBitQQuantizer.BITS);
    }

    for (String key : properties.keySet()) {
      Preconditions.checkArgument(
          readable.contains(key),
          "Unknown IVF option %s: this structure reads %s",
          key,
          Sets.newTreeSet(readable));
    }

    return new IvfOptions(
        intOption(properties, NLIST, 0), intOption(properties, NPROBE, DEFAULT_NPROBE));
  }

  /** The list count asked for, or zero to derive it from the entry count. */
  public int nlist() {
    return nlist;
  }

  public int nprobe() {
    return nprobe;
  }

  /**
   * The list count to use for a group of {@code entries} vectors.
   *
   * <p>Derived from the size of the group rather than fixed, because a group is one data file and
   * data files differ in size: a fixed count would fragment a small file into lists of a few
   * vectors each, and leave a large one with lists too long to scan. Never more lists than vectors,
   * so a tiny file degrades to a single list rather than to empty ones.
   */
  public int nlistFor(int entries) {
    if (entries <= 0) {
      return 0;
    }

    int wanted = nlist > 0 ? nlist : (int) Math.round(Math.sqrt(entries));
    return Math.max(1, Math.min(wanted, entries));
  }

  /** How many vectors to train the centroids on, for a group of {@code entries} vectors. */
  public static int samplesFor(int entries, int listCount) {
    return Math.min(entries, Math.max(listCount, listCount * SAMPLES_PER_LIST));
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
          String.format(Locale.ROOT, "IVF option %s is not a number: %s", key, value), e);
    }
  }
}
