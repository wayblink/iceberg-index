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
import org.apache.spark.SparkContext;
import org.apache.spark.api.plugin.DriverPlugin;
import org.apache.spark.api.plugin.ExecutorPlugin;
import org.apache.spark.api.plugin.PluginContext;
import org.apache.spark.api.plugin.SparkPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Enables the index cache control channel: {@code
 * spark.plugins=org.apache.iceberg.index.spark.IndexCachePlugin}.
 *
 * <p>The plugin carries no index payload and no credentials in status messages. The driver only
 * decides placement; executors read storage themselves with the read context carried by a command.
 */
public class IndexCachePlugin implements SparkPlugin {
  @Override
  public DriverPlugin driverPlugin() {
    return new IndexCacheDriverPlugin();
  }

  @Override
  public ExecutorPlugin executorPlugin() {
    return new IndexCacheExecutorPlugin();
  }

  /** Driver side: forwards control messages to the residency coordinator. */
  static final class IndexCacheDriverPlugin implements DriverPlugin {
    private static final Logger LOG = LoggerFactory.getLogger(IndexCacheDriverPlugin.class);
    private IndexCacheDriverControl control;

    @Override
    public Map<String, String> init(SparkContext sc, PluginContext pluginContext) {
      control = new IndexCacheDriverControl(new IndexResidencyCoordinator(System::nanoTime));
      return Map.of();
    }

    @Override
    public Object receive(Object message) {
      return control == null ? null : control.receive(message);
    }

    @Override
    public void shutdown() {
      control = null;
    }
  }

  /** Executor side: polls for commands and executes them outside the control path. */
  static final class IndexCacheExecutorPlugin implements ExecutorPlugin {
    private static final Logger LOG = LoggerFactory.getLogger(IndexCacheExecutorPlugin.class);
    private IndexCacheExecutorLoop loop;

    @Override
    public void init(PluginContext pluginContext, Map<String, String> extraConf) {
      IndexCacheWorkerConfig config = IndexCacheWorkerConfig.parse(extraConf);
      IndexCacheControlChannel channel = pluginContext::ask;
      ExecutorIndexCacheWorker worker =
          new ExecutorIndexCacheWorker(
              pluginContext.conf().get("spark.app.id", ""),
              pluginContext.executorID(),
              pluginContext.executorID()
                  + "#"
                  + pluginContext.conf().get("spark.app.startTime", ""),
              ExecutorIndexCache.get(),
              ExecutorIndexCache.Config.of(true, Long.MAX_VALUE / 4, Long.MAX_VALUE / 8),
              new IndexCacheReadContext(),
              config);
      loop = new IndexCacheExecutorLoop(channel, worker, config, Long.MAX_VALUE / 4);
      loop.start();
    }

    @Override
    public void shutdown() {
      if (loop != null) {
        loop.close();
        loop = null;
      }
    }
  }
}
