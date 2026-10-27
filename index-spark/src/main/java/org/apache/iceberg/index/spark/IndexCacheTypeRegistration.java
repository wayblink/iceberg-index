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

import org.apache.iceberg.index.text.TextIndexTypes;
import org.apache.iceberg.index.vector.VectorIndexTypes;

/**
 * Registers the index types a cache command may name.
 *
 * <p>The driver registers them while planning a query, but an executor is a separate JVM with its
 * own registries: it rebuilds the adapter from the definition carried by the command. Registering
 * here, on both sides of the plugin, keeps a load from failing with "Unknown index type".
 */
final class IndexCacheTypeRegistration {
  static void registerAll() {
    VectorIndexTypes.register();
    TextIndexTypes.register();
  }

  private IndexCacheTypeRegistration() {}
}
