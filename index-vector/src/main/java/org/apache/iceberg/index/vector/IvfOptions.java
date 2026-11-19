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

  /** Historical property kept for reading existing index definitions. */
  public static final String NPROBE = "nprobe";

  static final int DEFAULT_NPROBE = 8;

  /** How many training samples each list gets, following the usual heuristic. */
  static final String TRAIN_SAMPLES_PER_LIST = "ivf-train-samples-per-list";

  /** The most rounds the centroids are trained for. */
  static final String TRAIN_MAX_ITERATIONS = "ivf-train-max-iterations";

  /**
   * The relative movement of the centroids below which training stops.
   *
   * <p>Zero keeps the bitwise criterion: training ends once a round leaves every centroid where it
   * was. A positive value stops as soon as a round moves them by less than that fraction of their
   * own length, which is what lets a build trade quality for time.
   */
  static final String TRAIN_CONVERGENCE_TOLERANCE = "ivf-train-convergence-tolerance";

  /** More samples fit the centroids better and cost a pass over the sample per iteration. */
  static final int DEFAULT_TRAIN_SAMPLES_PER_LIST = 39;

  static final int DEFAULT_TRAIN_MAX_ITERATIONS = 25;

  static final double DEFAULT_TRAIN_CONVERGENCE_TOLERANCE = 0.0;

  private static final int MAX_TRAIN_ITERATIONS = 100;

  private static final double MAX_TRAIN_CONVERGENCE_TOLERANCE = 0.1;

  private static final double PARTS_PER_MILLION = 1_000_000.0;

  static final String LIST_BLOCKS = "ivf-list-blocks";

  /**
   * The nearest-centroid kernel a build runs: {@code exact} or {@code simd}.
   *
   * <p>The {@code simd} kernel is an optional alternative whose classes come from the vector API,
   * an incubator module that is not in the JDK's default module graph; a JVM that cannot load it
   * falls back to {@code exact} rather than failing the build. Only the build reads this option.
   */
  static final String ASSIGN_KERNEL = "ivf-assign-kernel";

  /** The kernels {@link #ASSIGN_KERNEL} names. */
  enum AssignKernelKind {
    EXACT,
    SIMD
  }

  /**
   * How a build turns the vectors into a payload: {@code java} or {@code native}.
   *
   * <p>The native kernel is an optional implementation. It runs only when its library loads on this
   * platform and the metric is L2; in every other case the build falls back to the Java kernel. The
   * two kernels write the same payload byte for byte, so the choice does not change what the index
   * contains, only the path that wrote it and the structure the build records.
   */
  static final String BUILD_KERNEL = "ivf-build-kernel";

  /** The kernels {@link #BUILD_KERNEL} names. */
  enum BuildKernel {
    JAVA,
    NATIVE
  }

  private static final ImmutableSet<String> KEYS =
      ImmutableSet.of(
          NLIST,
          LIST_BLOCKS,
          TRAIN_SAMPLES_PER_LIST,
          TRAIN_MAX_ITERATIONS,
          TRAIN_CONVERGENCE_TOLERANCE,
          ASSIGN_KERNEL,
          BUILD_KERNEL);

  private final int nlist;
  private final boolean listBlocks;
  private final int trainSamplesPerList;
  private final int trainMaxIterations;
  private final double trainConvergenceTolerance;
  private final AssignKernelKind assignKernel;
  private final BuildKernel buildKernel;

  private IvfOptions(
      int nlist,
      boolean listBlocks,
      int trainSamplesPerList,
      int trainMaxIterations,
      double trainConvergenceTolerance,
      AssignKernelKind assignKernel,
      BuildKernel buildKernel) {
    Preconditions.checkArgument(nlist >= 0, "Invalid IVF %s: %s", NLIST, nlist);
    Preconditions.checkArgument(
        trainSamplesPerList >= 1,
        "Invalid IVF %s: %s (at least one sample per list)",
        TRAIN_SAMPLES_PER_LIST,
        trainSamplesPerList);
    Preconditions.checkArgument(
        trainMaxIterations >= 1 && trainMaxIterations <= MAX_TRAIN_ITERATIONS,
        "Invalid IVF %s: %s (between 1 and %s)",
        TRAIN_MAX_ITERATIONS,
        trainMaxIterations,
        MAX_TRAIN_ITERATIONS);
    Preconditions.checkArgument(
        trainConvergenceTolerance >= 0.0
            && trainConvergenceTolerance <= MAX_TRAIN_CONVERGENCE_TOLERANCE,
        "Invalid IVF %s: %s (between 0 and %s)",
        TRAIN_CONVERGENCE_TOLERANCE,
        trainConvergenceTolerance,
        MAX_TRAIN_CONVERGENCE_TOLERANCE);
    this.nlist = nlist;
    this.listBlocks = listBlocks;
    this.trainSamplesPerList = trainSamplesPerList;
    this.trainMaxIterations = trainMaxIterations;
    this.trainConvergenceTolerance = trainConvergenceTolerance;
    this.assignKernel = assignKernel;
    this.buildKernel = buildKernel;
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
          NPROBE.equals(key) || readable.contains(key),
          "Unknown IVF option %s: this structure reads %s",
          key,
          Sets.newTreeSet(readable));
    }

    String blocks = properties.getOrDefault(LIST_BLOCKS, "false");
    Preconditions.checkArgument(
        "true".equalsIgnoreCase(blocks) || "false".equalsIgnoreCase(blocks),
        "Invalid IVF %s: %s",
        LIST_BLOCKS,
        blocks);
    return new IvfOptions(
        intOption(properties, NLIST, 0),
        Boolean.parseBoolean(blocks),
        intOption(properties, TRAIN_SAMPLES_PER_LIST, DEFAULT_TRAIN_SAMPLES_PER_LIST),
        intOption(properties, TRAIN_MAX_ITERATIONS, DEFAULT_TRAIN_MAX_ITERATIONS),
        doubleOption(properties, TRAIN_CONVERGENCE_TOLERANCE, DEFAULT_TRAIN_CONVERGENCE_TOLERANCE),
        assignKernelOption(properties),
        buildKernelOption(properties));
  }

  boolean listBlocks() {
    return listBlocks;
  }

  /**
   * Whether the build was asked to run the vector API assignment kernel.
   *
   * <p>A JVM that cannot load the kernel still reports {@code true} here; the kernel is the build's
   * choice, and the build records whether it could act on it.
   */
  boolean simdAssignKernel() {
    return assignKernel == AssignKernelKind.SIMD;
  }

  /** Whether the definition asked for the native build kernel. */
  boolean nativeBuildKernel() {
    return buildKernel == BuildKernel.NATIVE;
  }

  int trainSamplesPerList() {
    return trainSamplesPerList;
  }

  int trainMaxIterations() {
    return trainMaxIterations;
  }

  double trainConvergenceTolerance() {
    return trainConvergenceTolerance;
  }

  /** The tolerance in millionths, which is the scale the build records it in. */
  long tolerancePartsPerMillion() {
    return Math.round(trainConvergenceTolerance * PARTS_PER_MILLION);
  }

  /** The list count asked for, or zero to derive it from the entry count. */
  public int nlist() {
    return nlist;
  }

  /**
   * @deprecated Configure the probe count on the query.
   */
  @Deprecated
  public int nprobe() {
    return DEFAULT_NPROBE;
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

  /**
   * How many vectors to train the centroids on, for a group of {@code entries} vectors.
   *
   * <p>The sample is the list count times the samples per list the definition asked for, so a
   * smaller budget trains on fewer vectors; the file's own entries always cap it.
   */
  int samplesFor(int entries, int listCount) {
    long wanted = (long) listCount * trainSamplesPerList;
    return (int) Math.min(entries, Math.max(listCount, wanted));
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

  private static AssignKernelKind assignKernelOption(Map<String, String> properties) {
    String value = properties.getOrDefault(ASSIGN_KERNEL, AssignKernelKind.EXACT.name());
    return switch (value.trim().toLowerCase(Locale.ROOT)) {
      case "exact" -> AssignKernelKind.EXACT;
      case "simd" -> AssignKernelKind.SIMD;
      default ->
          throw new IllegalArgumentException(
              String.format(
                  Locale.ROOT,
                  "Invalid IVF %s: %s (expected exact or simd)",
                  ASSIGN_KERNEL,
                  value));
    };
  }

  private static BuildKernel buildKernelOption(Map<String, String> properties) {
    String value = properties.getOrDefault(BUILD_KERNEL, BuildKernel.JAVA.name());
    return switch (value.trim().toLowerCase(Locale.ROOT)) {
      case "java" -> BuildKernel.JAVA;
      case "native" -> BuildKernel.NATIVE;
      default ->
          throw new IllegalArgumentException(
              String.format(
                  Locale.ROOT,
                  "Invalid IVF %s: %s (expected java or native)",
                  BUILD_KERNEL,
                  value));
    };
  }

  private static double doubleOption(
      Map<String, String> properties, String key, double defaultValue) {
    String value = properties.get(key);
    if (value == null) {
      return defaultValue;
    }

    try {
      return Double.parseDouble(value.trim());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          String.format(Locale.ROOT, "IVF option %s is not a number: %s", key, value), e);
    }
  }
}
