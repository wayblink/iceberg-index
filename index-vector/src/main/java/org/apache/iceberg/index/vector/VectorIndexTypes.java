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

import org.apache.iceberg.index.IndexType;
import org.apache.iceberg.index.IndexTypes;

/**
 * Registration of the vector index type with the core {@link IndexTypes} registry.
 *
 * <p>The vector adapter lives in this module so that core carries no ANN dependency. Core fails
 * fast on unknown index types, so a process that reads or builds vector indexes must register the
 * type first, typically once at startup:
 *
 * <pre>{@code
 * VectorIndexTypes.register();
 * }</pre>
 *
 * <p>Registration is idempotent.
 */
public final class VectorIndexTypes {

  private VectorIndexTypes() {}

  /**
   * Registers the vector index type in the core registry, and the kernels it ships with.
   *
   * <p>The kernels are registered here because the type is useless without one: a process that
   * registered the type but no kernel would accept a definition and then fail to build it.
   */
  public static void register() {
    IndexTypes.register(new VectorIndexType());
    VectorKernels.registerDefaults();
  }

  /** Returns a new instance of the vector index type. */
  public static IndexType vectorIndexType() {
    return new VectorIndexType();
  }
}
