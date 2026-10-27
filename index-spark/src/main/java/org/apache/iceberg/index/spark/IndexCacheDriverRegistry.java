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

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.spark.SparkContext;

/**
 * The control channel of each running application, as registered by the driver side of {@link
 * IndexCachePlugin}.
 *
 * <p>It is keyed by the {@link SparkContext} itself, not by the application id: Spark creates the
 * driver plugin before it assigns that id, so a registry keyed by the id cannot be filled in at
 * init. A driver JVM runs one context, and the procedures reach it through the same session.
 */
final class IndexCacheDriverRegistry {
  private static final Map<SparkContext, IndexCacheDriverControl> CONTROLS =
      Collections.synchronizedMap(new IdentityHashMap<>());

  private IndexCacheDriverRegistry() {}

  static void register(SparkContext context, IndexCacheDriverControl control) {
    Preconditions.checkNotNull(context, "Spark context is null");
    CONTROLS.put(context, Preconditions.checkNotNull(control, "Control is null"));
  }

  static void unregister(SparkContext context) {
    if (context != null) {
      CONTROLS.remove(context);
    }
  }

  /** The channel of this application, or null when the plugin is not enabled. */
  static IndexCacheDriverControl find(SparkContext context) {
    return context == null ? null : CONTROLS.get(context);
  }

  /** The control channel of this application, or a message that says the plugin is not enabled. */
  static IndexCacheDriverControl require(SparkContext context) {
    IndexCacheDriverControl control = find(context);
    Preconditions.checkArgument(
        control != null,
        "The index cache control channel is not available: start the application with "
            + "spark.plugins=%s",
        IndexCachePlugin.class.getName());
    return control;
  }

  static void clear() {
    CONTROLS.clear();
  }
}
