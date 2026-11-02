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
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Polls the driver for commands on one executor and executes them off the control path, so a slow
 * prepare never delays a status report.
 */
final class IndexCacheExecutorLoop implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(IndexCacheExecutorLoop.class);

  private final IndexCacheControlChannel channel;
  private final ExecutorIndexCacheWorker worker;
  private final IndexCacheWorkerConfig config;
  private final long budgetBytes;
  private final AtomicBoolean running = new AtomicBoolean(false);
  private final AtomicBoolean polling = new AtomicBoolean(false);
  private final Queue<IndexCacheProtocol.Ack> pendingAcks = new ConcurrentLinkedQueue<>();
  private final java.util.Set<String> submitted =
      java.util.concurrent.ConcurrentHashMap.newKeySet();
  private ScheduledExecutorService poller;
  private ExecutorService loaders;
  private long sequence;

  IndexCacheExecutorLoop(
      IndexCacheControlChannel channel,
      ExecutorIndexCacheWorker worker,
      IndexCacheWorkerConfig config,
      long budgetBytes) {
    this.channel = Objects.requireNonNull(channel, "Channel is null");
    this.worker = Objects.requireNonNull(worker, "Worker is null");
    this.config = Objects.requireNonNull(config, "Config is null");
    this.budgetBytes = budgetBytes;
    // Created eagerly so a manual poll works without the scheduler; both threads stay daemon.
    this.loaders =
        Executors.newFixedThreadPool(
            config.maxConcurrentLoads(),
            runnable -> {
              Thread thread = new Thread(runnable, "iceberg-index-cache-load");
              thread.setDaemon(true);
              return thread;
            });
  }

  void start() {
    if (!running.compareAndSet(false, true)) {
      return;
    }

    poller =
        Executors.newSingleThreadScheduledExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "iceberg-index-cache-control");
              thread.setDaemon(true);
              return thread;
            });
    poller.scheduleWithFixedDelay(this::pollOnce, 0, config.pollMillis(), TimeUnit.MILLISECONDS);
  }

  /** One control round trip: report state, then execute whatever the driver sends back. */
  void pollOnce() {
    // A manual tick is allowed before start(); only a closed worker or an overlapping poll stops
    // it.
    if (worker.closed() || !polling.compareAndSet(false, true)) {
      return;
    }

    try {
      // Acknowledgements are sent from the control thread: Spark's ask must run there.
      IndexCacheProtocol.Ack ack;
      // Peek first: if the round trip fails the acknowledgement stays queued and the command stays
      // in flight, so the driver's re-send is answered instead of being skipped forever.
      while ((ack = pendingAcks.peek()) != null) {
        if (!send(ack)) {
          // the report is what says this executor is alive and what it holds, so it must not queue
          // up behind an acknowledgement that keeps failing
          break;
        }

        pendingAcks.poll();
        submitted.remove(ack.commandId());
      }

      IndexCacheProtocol.NodeReport report =
          worker.report(IndexCacheMemory.runtimeIdentity(), ++sequence, budgetBytes, 0L, Map.of());
      Object response = channel.ask(report);
      if (!(response instanceof IndexCacheProtocol.Reply reply)) {
        return;
      }

      int batchCount = 0;
      for (IndexCacheProtocol.Command command : reply.commands()) {
        if (batchCount >= config.maxCommands()) {
          // The rest stays queued on the driver; a bounded batch keeps one reply from flooding
          // the loading pool or the temporary memory of this executor.
          break;
        }

        if (!submitted.add(command.commandId())) {
          continue;
        }

        batchCount += 1;

        // Loading runs on its own bounded pool so a slow prepare never delays the next poll.
        loaders.submit(
            () -> {
              IndexCacheProtocol.Ack done;
              try {
                done = worker.execute(command);
              } catch (RuntimeException e) {
                done = worker.failure(command, e);
              }

              // The driver decides coverage; the executor only reports what it did.
              pendingAcks.offer(done);
            });
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      LOG.debug("Index cache control round trip failed; the next poll retries", e);
    } finally {
      polling.set(false);
    }
  }

  /**
   * Sends one acknowledgement, and says whether the queue may move past it.
   *
   * <p>A failure is retried: the usual cause is a driver that is briefly unreachable, and the
   * driver answers a lost acknowledgement by re-sending the command. The message stays at the head
   * of the queue so the retry answers that re-send instead of skipping the command forever.
   */
  private boolean send(IndexCacheProtocol.Ack ack) throws InterruptedException {
    try {
      channel.ask(ack);
      return true;
    } catch (InterruptedException e) {
      throw e;
    } catch (Exception e) {
      LOG.debug("Index cache acknowledgement {} did not reach the driver; retrying", ack.commandId(), e);
      return false;
    }
  }

  boolean running() {
    return running.get();
  }

  @Override
  public void close() {
    running.set(false);
    if (poller != null) {
      poller.shutdownNow();
      poller = null;
    }

    if (loaders != null) {
      loaders.shutdownNow();
      loaders = null;
    }

    pendingAcks.clear();
    worker.close();
  }
}
