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

import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * Registry of known {@link IndexType index types}.
 *
 * <p>The registry maps a type identifier (for example {@code "vector"}) to the contract that parses
 * its definition and creates its adapter. Built-in types are registered automatically; additional
 * types can be registered by any module. Unknown types fail fast so that an engine never silently
 * misinterprets an index type it does not support.
 */
public final class IndexTypes {
  private static final Map<String, IndexType> REGISTRY = Maps.newConcurrentMap();

  static {
    register(new VectorIndexType());
  }

  private IndexTypes() {}

  /**
   * Registers an index type, replacing any existing registration for the same type identifier.
   *
   * @param type the index type to register
   */
  public static void register(IndexType type) {
    Preconditions.checkNotNull(type, "Index type is null");
    REGISTRY.put(type.type(), type);
  }

  /**
   * Returns the index type registered for the given identifier.
   *
   * @param type the type identifier, for example {@code "vector"}
   * @throws IllegalArgumentException if the type is not registered
   */
  public static IndexType fromType(String type) {
    Preconditions.checkArgument(type != null && !type.isEmpty(), "Index type is empty");
    IndexType indexType = REGISTRY.get(type);
    Preconditions.checkArgument(
        indexType != null,
        "Unknown index type: %s (registered types: %s)",
        type,
        REGISTRY.keySet());
    return indexType;
  }

  /** Returns an immutable view of all registered index types keyed by identifier. */
  public static Map<String, IndexType> registered() {
    return ImmutableMap.copyOf(REGISTRY);
  }
}
