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

import java.util.List;
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
    private SparkContext context;

    @Override
    public Map<String, String> init(SparkContext sc, PluginContext pluginContext) {
      IndexCacheTypeRegistration.registerAll();
      IndexCacheWorkerConfig config =
          IndexCacheWorkerConfig.parse(pluginSettings(pluginContext.conf()));
      control =
          new IndexCacheDriverControl(
              new IndexResidencyCoordinator(
                  System::nanoTime, config.maxCommands(), config.maxAttempts()));
      // The facade and the procedures reach this application's channel through the registry, keyed
      // by the context: Spark has not assigned the application id when a driver plugin starts.
      context = sc;
      IndexCacheDriverRegistry.register(sc, control);
      return Map.of();
    }

    /**
     * Spark stores per-plugin configuration under {@code spark.plugins.internal.conf.<plugin
     * class>.<key>} for the executor; the driver reads the same names and falls back to the plain
     * key for applications that set it directly.
     */
    private static Map<String, String> pluginSettings(org.apache.spark.SparkConf conf) {
      Map<String, String> settings = new java.util.LinkedHashMap<>();
      for (String key :
          List.of(
              IndexCacheWorkerConfig.POLL_INTERVAL,
              IndexCacheWorkerConfig.STALE_AFTER,
              IndexCacheWorkerConfig.MAX_COMMANDS,
              IndexCacheWorkerConfig.MAX_CONCURRENT,
              IndexCacheWorkerConfig.TIMEOUT,
              IndexCacheWorkerConfig.MAX_ATTEMPTS)) {
        String prefixed =
            "spark.plugins.internal.conf." + IndexCachePlugin.class.getName() + "." + key;
        String value = conf.get(prefixed, null);
        if (value == null) {
          value = conf.get(key, null);
        }

        if (value != null) {
          settings.put(key, value);
        }
      }

      return settings;
    }

    @Override
    public Object receive(Object message) {
      return control == null ? null : control.receive(message);
    }

    @Override
    public void shutdown() {
      IndexCacheDriverRegistry.unregister(context);
      context = null;
      control = null;
    }
  }

  /** Executor side: polls for commands and executes them outside the control path. */
  static final class IndexCacheExecutorPlugin implements ExecutorPlugin {
    private IndexCacheExecutorLoop loop;
    private ExecutorIndexCache.ExecutorBudget budgetBinding;

    /**
     * Spark fills {@code extraConf} from {@code spark.plugins.internal.conf.<plugin class>.<key>},
     * so the settings documented as {@code spark.iceberg.index.cache.*} arrive here under that
     * prefix. Nothing is derived from a session, because executors do not have one.
     */
    @Override
    public void init(PluginContext pluginContext, Map<String, String> extraConf) {
      IndexCacheTypeRegistration.registerAll();
      IndexCacheWorkerConfig config = IndexCacheWorkerConfig.parse(extraConf);
      long budget = advertisedBudget(pluginContext.conf());
      ExecutorIndexCache cache = ExecutorIndexCache.get();
      budgetBinding = cache.bindExecutorBudget(budget);
      try {
        IndexCacheControlChannel channel = pluginContext::ask;
        ExecutorIndexCacheWorker worker =
            new ExecutorIndexCacheWorker(
                pluginContext.conf().get("spark.app.id", ""),
                pluginContext.executorID(),
                incarnation(pluginContext),
                cache,
                // Query-specific budgets cannot expand this executor's advertised capacity.
                budget > 0
                    ? ExecutorIndexCache.Config.of(true, budget)
                    : ExecutorIndexCache.Config.disabled(),
                new IndexCacheReadContext(),
                config);
        loop = new IndexCacheExecutorLoop(channel, worker, config, budget);
        loop.start();
      } catch (RuntimeException e) {
        shutdown();
        throw e;
      }
    }

    /**
     * The budget this executor advertises, which is the one a query derives from the same heap when
     * the application configures none.
     *
     * <p>The executor is where a load is refused, so this number is what turns a configured cache
     * into a usable one: advertising nothing while a query derives a budget refused every load.
     */
    private static long advertisedBudget(org.apache.spark.SparkConf conf) {
      return ExecutorIndexCache.Config.advertisedBudget(
          conf.get(ExecutorIndexCache.Config.BYTES, null),
          conf.get(ExecutorIndexCache.Config.EXECUTOR_MEMORY, null),
          conf.get(ExecutorIndexCache.Config.HEAP_FRACTION, null));
    }

    private static String incarnation(PluginContext pluginContext) {
      // The application identity keeps a reused executor id from two applications apart.
      return pluginContext.executorID()
          + "#"
          + pluginContext.conf().get("spark.app.id", "")
          + "#"
          + pluginContext.conf().get("spark.app.startTime", "");
    }

    @Override
    public void shutdown() {
      if (loop != null) {
        loop.close();
        loop = null;
      }
      if (budgetBinding != null) {
        budgetBinding.close();
        budgetBinding = null;
      }
    }
  }
}
