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

import java.util.Map;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

/**
 * The kernels this build knows, by name.
 *
 * <p>Registration is explicit and failing loudly, like the index type registry: a kernel that was
 * not registered is reported by name rather than replaced by a default, because a build that
 * quietly used a different kernel would produce an index the caller did not ask for.
 */
public final class VectorKernels {

  private static final Map<String, VectorKernelFactory> FACTORIES = Maps.newConcurrentMap();

  private VectorKernels() {}

  public static void register(VectorKernelFactory factory) {
    Preconditions.checkNotNull(factory, "Vector kernel factory is null");
    Preconditions.checkArgument(
        factory.name() != null && !factory.name().isEmpty(), "Vector kernel has no name");
    VectorKernelFactory existing = FACTORIES.putIfAbsent(factory.name(), factory);
    if (existing != null) {
      // built by hand rather than by Preconditions, whose arguments are evaluated before the check
      // and would dereference the very thing that turned out to be there
      throw new IllegalArgumentException(
          "A vector kernel named "
              + factory.name()
              + " is already registered: "
              + existing.getClass().getName());
    }
  }

  /** The factory of that name, or a failure that lists the ones there are. */
  public static VectorKernelFactory fromName(String name) {
    Preconditions.checkArgument(name != null && !name.isEmpty(), "Vector kernel name is empty");
    VectorKernelFactory factory = FACTORIES.get(name);
    Preconditions.checkArgument(
        factory != null, "Unknown vector kernel: %s (registered kernels: %s)", name, names());
    return factory;
  }

  public static Set<String> names() {
    return Sets.newTreeSet(FACTORIES.keySet());
  }

  /**
   * The kernel that serves a definition, refusing one no registered kernel can build.
   *
   * <p>The refusal names what is available, because the alternative is an index nothing can search.
   */
  static VectorKernel forDefinition(VectorIndexDefinition definition) {
    for (VectorKernelFactory factory : factoriesInNameOrder()) {
      KernelCapabilities capabilities = factory.capabilities();
      if (capabilities.structures().contains(definition.structure())
          && capabilities.quantizers().contains(definition.quantizer())
          && capabilities.metrics().contains(definition.metric())) {
        return factory.create(definition);
      }
    }

    throw new IllegalArgumentException(
        String.format(
            "No vector kernel builds a %s index with the %s quantizer for the %s metric: %s",
            definition.structure(), definition.quantizer(), definition.metric(), supported()));
  }

  /** What the registered kernels can build, in a form an error message can print. */
  static String supported() {
    if (FACTORIES.isEmpty()) {
      return "no kernels are registered";
    }

    return Lists.newArrayList(
            factoriesInNameOrder().stream()
                .map(factory -> factory.name() + " (" + factory.capabilities() + ")")
                .toList())
        .toString();
  }

  /**
   * Registers the kernels that ship with this build.
   *
   * <p>Idempotent, and safe to reach concurrently: every registration path ends here, so it has to
   * tolerate a kernel already being present rather than fail on the second caller.
   */
  public static void registerDefaults() {
    FACTORIES.putIfAbsent(BruteforceKernel.NAME, new BruteforceKernel.Factory());
    FACTORIES.putIfAbsent(HnswlibKernel.NAME, new HnswlibKernel.Factory());
    FACTORIES.putIfAbsent(IvfKernel.NAME, new IvfKernel.Factory());
  }

  private static java.util.List<VectorKernelFactory> factoriesInNameOrder() {
    return FACTORIES.entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .map(Map.Entry::getValue)
        .toList();
  }
}
