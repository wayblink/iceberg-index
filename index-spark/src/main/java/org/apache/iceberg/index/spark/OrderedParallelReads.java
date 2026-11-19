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

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * Reads a list of row streams concurrently and hands their rows back in stream order.
 *
 * <p>A group of data files is read by the one Spark task that writes its index file, and a task is
 * one thread: reading the files one after another leaves the task waiting on the tail of each
 * request instead of overlapping the tails, which is what makes the read of a group slow. A few
 * streams at once overlaps them; the rows still come back in the order the streams were given,
 * which is the order the index entries and the files an index file covers are written in.
 *
 * <p>Each stream runs on its own thread of a fixed pool and buffers its rows in a bounded queue, so
 * a stream that runs ahead of the reader costs a bounded amount of memory rather than the whole
 * file: at most {@code streamsInFlight * queueChunks * chunkRows} rows are held at once. A stream
 * that fails ends the read with its failure and stops the others; the rows produced before it are
 * not handed out as if the stream had ended, because a silently short group would be written as a
 * complete index file.
 *
 * <p>The rows are consumed by the caller's thread, so anything a caller does per row (encoding, for
 * instance) keeps running on that one thread.
 */
final class OrderedParallelReads<T> implements Iterator<T> {

  /** Opens one stream of rows; called on the reading thread. */
  interface Stream<T> {
    CloseableIterator<T> open() throws Exception;
  }

  private static final long POLL_MILLIS = 100L;

  private final List<Stream<T>> streams;
  private final List<ArrayBlockingQueue<Object>> queues;
  private final int chunkRows;
  private final int streamsInFlight;
  private final ExecutorService pool;
  private final AtomicBoolean stopped = new AtomicBoolean();
  private final AtomicReference<Throwable> failure = new AtomicReference<>();

  private int submitted = 0;
  private int current = 0;
  private CloseableIterator<T> inline = null;
  private Iterator<T> buffer = null;
  private T next = null;
  private boolean ready = false;
  private boolean finished = false;

  private static final Object END = new Object();

  private OrderedParallelReads(
      List<Stream<T>> streams, int streamsInFlight, int queueChunks, int chunkRows) {
    Preconditions.checkArgument(streamsInFlight > 0, "Streams in flight must be positive");
    Preconditions.checkArgument(queueChunks > 0, "Queue chunks must be positive");
    Preconditions.checkArgument(chunkRows > 0, "Chunk rows must be positive");
    this.streams = streams;
    this.chunkRows = chunkRows;
    this.streamsInFlight = Math.min(streamsInFlight, streams.size());
    this.finished = streams.isEmpty();

    if (streams.size() <= 1) {
      // one stream is read by the caller's thread: the common layout is one data file per index
      // file, and a group of one has nothing to overlap
      this.queues = null;
      this.pool = null;
    } else {
      List<ArrayBlockingQueue<Object>> newQueues = Lists.newArrayListWithCapacity(streams.size());
      for (int i = 0; i < streams.size(); i++) {
        newQueues.add(new ArrayBlockingQueue<>(queueChunks));
      }

      this.queues = newQueues;
      this.pool =
          Executors.newFixedThreadPool(
              this.streamsInFlight,
              runnable -> {
                Thread thread = new Thread(runnable, "index-group-read");
                thread.setDaemon(true);
                return thread;
              });

      submitReady();
    }
  }

  /**
   * Reads the given streams, at most {@code streamsInFlight} at a time, and hands their rows back
   * in stream order. Each stream may buffer up to {@code queueChunks} chunks of {@code chunkRows}
   * rows.
   */
  static <T> OrderedParallelReads<T> of(
      List<Stream<T>> streams, int streamsInFlight, int queueChunks, int chunkRows) {
    return new OrderedParallelReads<>(streams, streamsInFlight, queueChunks, chunkRows);
  }

  @Override
  public boolean hasNext() {
    if (ready) {
      return true;
    }

    if (finished) {
      return false;
    }

    while (true) {
      if (buffer != null && buffer.hasNext()) {
        next = buffer.next();
        ready = true;
        return true;
      }

      buffer = null;

      if (pool == null) {
        return inlineNext();
      }

      if (current >= streams.size()) {
        finish();
        return false;
      }

      Object chunk = take();
      if (chunk == END) {
        current += 1;
        submitReady();
        continue;
      }

      @SuppressWarnings("unchecked")
      List<T> rows = (List<T>) chunk;
      buffer = rows.iterator();
    }
  }

  @Override
  public T next() {
    if (!hasNext()) {
      throw new NoSuchElementException("No more rows to read");
    }

    ready = false;
    return next;
  }

  /**
   * Stops the streams that are still reading. Called when the rows are no longer wanted, as a task
   * that ends early does.
   */
  void close() {
    if (stopped.compareAndSet(false, true)) {
      if (inline != null) {
        closeQuietly(inline);
        inline = null;
      }

      if (pool != null) {
        pool.shutdownNow();
      }
    }
  }

  /**
   * The index of the stream the row most recently returned by {@link #next()} came from, so a
   * caller can tell whose row it is. Only meaningful on the consuming thread.
   */
  int currentStream() {
    return current;
  }

  private boolean inlineNext() {
    try {
      if (inline == null) {
        inline = streams.get(0).open();
      }

      if (inline.hasNext()) {
        next = inline.next();
        ready = true;
        return true;
      }
    } catch (Exception e) {
      // the stream is done with: a caller that asks again gets the end rather than a second attempt
      // to open the same file
      finished = true;
      close();
      throw asRuntimeException(e);
    }

    finish();
    return false;
  }

  private Object take() {
    ArrayBlockingQueue<Object> queue = queues.get(current);
    while (true) {
      throwIfFailed();

      if (stopped.get()) {
        // the read was stopped under this iteration, by a failure of another stream or by a caller
        // that no longer wants the rows: no end of the queue is coming
        throw new RuntimeException("The read of the data files of an index group was stopped");
      }

      Object chunk;
      try {
        chunk = queue.poll(POLL_MILLIS, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        close();
        throw new RuntimeException("Interrupted while reading the data files of an index", e);
      }

      if (chunk != null) {
        return chunk;
      }
    }
  }

  private void submitReady() {
    if (pool.isShutdown()) {
      // the read was stopped between the end of one stream and the start of the next
      return;
    }

    while (submitted < streams.size() && submitted < current + streamsInFlight) {
      int index = submitted;
      submitted += 1;
      pool.execute(() -> read(index));
    }
  }

  private void read(int index) {
    ArrayBlockingQueue<Object> queue = queues.get(index);
    CloseableIterator<T> rows = null;
    try {
      rows = streams.get(index).open();
      List<T> chunk = Lists.newArrayListWithCapacity(chunkRows);
      while (rows.hasNext()) {
        chunk.add(rows.next());
        if (chunk.size() >= chunkRows) {
          if (!offer(queue, chunk)) {
            return;
          }

          chunk = Lists.newArrayListWithCapacity(chunkRows);
        }
      }

      if (!chunk.isEmpty() && !offer(queue, chunk)) {
        return;
      }

      offer(queue, END);
    } catch (Throwable t) {
      if (failure.compareAndSet(null, t)) {
        // the failure ends the read rather than being handed to the caller's thread as a missing
        // row; the rows already queued for this stream are abandoned with the rest
        close();
      }
    } finally {
      closeQuietly(rows);
    }
  }

  /** Offers a chunk, waiting while the queue is full; false when the read was stopped. */
  private boolean offer(ArrayBlockingQueue<Object> queue, Object chunk)
      throws InterruptedException {
    while (!stopped.get()) {
      if (queue.offer(chunk, POLL_MILLIS, TimeUnit.MILLISECONDS)) {
        return true;
      }
    }

    return false;
  }

  private void throwIfFailed() {
    Throwable cause = failure.get();
    if (cause != null) {
      close();
      throw asRuntimeException(cause);
    }
  }

  private void finish() {
    finished = true;
    close();
  }

  private static void closeQuietly(AutoCloseable closeable) {
    if (closeable != null) {
      try {
        closeable.close();
      } catch (Exception e) {
        // a reader that is being abandoned cannot do anything about its close failing, and masking
        // the failure that ended the read with this one would hide the reason the task failed
      }
    }
  }

  private static RuntimeException asRuntimeException(Throwable cause) {
    if (cause instanceof RuntimeException) {
      return (RuntimeException) cause;
    }

    if (cause instanceof Error) {
      throw (Error) cause;
    }

    return new RuntimeException("Failed to read the data files of an index group", cause);
  }
}
