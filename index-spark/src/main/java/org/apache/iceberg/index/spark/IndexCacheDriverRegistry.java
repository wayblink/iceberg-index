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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * The control channel of each running application, as registered by the driver side of {@link
 * IndexCachePlugin}. A control command belongs to one application, so the registry is keyed by the
 * application identity rather than by a table.
 */
final class IndexCacheDriverRegistry {
  private static final Map<String, IndexCacheDriverControl> CONTROLS = new ConcurrentHashMap<>();

  private IndexCacheDriverRegistry() {}

  static void register(String applicationId, IndexCacheDriverControl control) {
    Preconditions.checkArgument(
        applicationId != null && !applicationId.isEmpty(), "Application identity is missing");
    CONTROLS.put(applicationId, Preconditions.checkNotNull(control, "Control is null"));
  }

  static void unregister(String applicationId) {
    if (applicationId != null) {
      CONTROLS.remove(applicationId);
    }
  }

  /** The control channel of this application, or a message that says the plugin is not enabled. */
  static IndexCacheDriverControl require(String applicationId) {
    IndexCacheDriverControl control = applicationId == null ? null : CONTROLS.get(applicationId);
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
