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

import java.util.Objects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Handles control messages on the driver. It is Spark-free so the protocol can be exercised without
 * a cluster; the plugin only forwards messages to it.
 */
final class IndexCacheDriverControl {
  private final IndexResidencyCoordinator coordinator;

  IndexCacheDriverControl(IndexResidencyCoordinator coordinator) {
    this.coordinator = Objects.requireNonNull(coordinator, "Coordinator is null");
  }

  /** Returns a reply for a report, and nothing for an ACK. */
  IndexCacheProtocol.Reply receive(Object message) {
    Preconditions.checkArgument(message != null, "Control message is null");
    if (message instanceof IndexCacheProtocol.NodeReport report) {
      return coordinator.report(report);
    } else if (message instanceof IndexCacheProtocol.Ack ack) {
      coordinator.ack(ack);
      return IndexCacheProtocol.Reply.builder().commands(java.util.List.of()).build();
    }

    throw new IllegalArgumentException(
        "Unsupported control message: " + message.getClass().getName());
  }

  IndexResidencyCoordinator coordinator() {
    return coordinator;
  }
}
