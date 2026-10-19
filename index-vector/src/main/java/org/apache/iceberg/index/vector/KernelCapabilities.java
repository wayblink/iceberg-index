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

import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

/**
 * What a kernel can do.
 *
 * <p>A kernel states this rather than leaving a caller to discover it by trying: a request for a
 * structure or a quantizer a kernel does not implement is refused at the point it is made, naming
 * what is available, instead of an index being built that nothing can search.
 */
public final class KernelCapabilities {

  private final Set<VectorIndexDefinition.VectorStructure> structures;
  private final Set<VectorIndexDefinition.VectorQuantizer> quantizers;
  private final Set<VectorIndexDefinition.Metric> metrics;
  private final boolean nativeLibrary;
  private final boolean incrementalAdd;

  private KernelCapabilities(Builder builder) {
    this.structures = ImmutableSet.copyOf(builder.structures);
    this.quantizers = ImmutableSet.copyOf(builder.quantizers);
    this.metrics = ImmutableSet.copyOf(builder.metrics);
    this.nativeLibrary = builder.nativeLibrary;
    this.incrementalAdd = builder.incrementalAdd;
  }

  public static Builder builder() {
    return new Builder();
  }

  public Set<VectorIndexDefinition.VectorStructure> structures() {
    return structures;
  }

  public Set<VectorIndexDefinition.VectorQuantizer> quantizers() {
    return quantizers;
  }

  public Set<VectorIndexDefinition.Metric> metrics() {
    return metrics;
  }

  /** Whether the kernel loads a native library, which a deployment has to put on the image. */
  public boolean nativeLibrary() {
    return nativeLibrary;
  }

  /**
   * Whether the kernel can add vectors to an existing payload without rebuilding it.
   *
   * <p>A refresh rebuilds the index files of the data files that changed, so a kernel that cannot
   * add is still refreshed file by file; what it cannot do is grow one payload in place.
   */
  public boolean incrementalAdd() {
    return incrementalAdd;
  }

  @Override
  public String toString() {
    return String.format(
        "structures=%s quantizers=%s metrics=%s nativeLibrary=%s incrementalAdd=%s",
        Sets.newTreeSet(structures.stream().map(Enum::name).toList()),
        Sets.newTreeSet(quantizers.stream().map(Enum::name).toList()),
        Sets.newTreeSet(metrics.stream().map(Enum::name).toList()),
        nativeLibrary,
        incrementalAdd);
  }

  public static final class Builder {
    private final Set<VectorIndexDefinition.VectorStructure> structures = Sets.newLinkedHashSet();
    private final Set<VectorIndexDefinition.VectorQuantizer> quantizers = Sets.newLinkedHashSet();
    private final Set<VectorIndexDefinition.Metric> metrics = Sets.newLinkedHashSet();
    private boolean nativeLibrary = false;
    private boolean incrementalAdd = false;

    private Builder() {}

    public Builder structure(VectorIndexDefinition.VectorStructure structure) {
      structures.add(structure);
      return this;
    }

    public Builder quantizer(VectorIndexDefinition.VectorQuantizer quantizer) {
      quantizers.add(quantizer);
      return this;
    }

    public Builder metric(VectorIndexDefinition.Metric metric) {
      metrics.add(metric);
      return this;
    }

    public Builder nativeLibrary(boolean isNative) {
      this.nativeLibrary = isNative;
      return this;
    }

    public Builder incrementalAdd(boolean canAdd) {
      this.incrementalAdd = canAdd;
      return this;
    }

    public KernelCapabilities build() {
      return new KernelCapabilities(this);
    }
  }
}
