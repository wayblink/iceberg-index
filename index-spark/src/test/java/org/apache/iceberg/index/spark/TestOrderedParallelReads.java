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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

/**
 * A group's data files are read on several streams at once, and their rows still come back in the
 * order the files were given, which is what the entries and the covered files depend on.
 */
class TestOrderedParallelReads {

  @Test
  void handsRowsBackInStreamOrderWhicheverStreamFinishesFirst() {
    List<OrderedParallelReads.Stream<Integer>> streams = Lists.newArrayList();
    // the streams after the first are ready before it is, so the order cannot be the completion
    // order
    for (int i = 0; i < 6; i++) {
      int index = i;
      streams.add(() -> new SlowRows(List.of(index * 10, index * 10 + 1), index == 0 ? 40L : 0L));
    }

    OrderedParallelReads<Integer> reads = OrderedParallelReads.of(streams, 3, 2, 2);
    List<Integer> rows = Lists.newArrayList();
    List<Integer> streamOfRow = Lists.newArrayList();
    while (reads.hasNext()) {
      rows.add(reads.next());
      streamOfRow.add(reads.currentStream());
    }

    assertThat(rows).containsExactly(0, 1, 10, 11, 20, 21, 30, 31, 40, 41, 50, 51);
    assertThat(streamOfRow).containsExactly(0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5);
  }

  @Test
  void producesTheSameRowsOnOneStreamAsOnMany() {
    List<Integer> all = Lists.newArrayList();
    for (int i = 0; i < 100; i++) {
      all.add(i);
    }

    List<OrderedParallelReads.Stream<Integer>> oneStream = Lists.newArrayList();
    oneStream.add(() -> rows(all));
    assertThat(drain(OrderedParallelReads.of(oneStream, 4, 2, 8))).containsExactlyElementsOf(all);

    List<OrderedParallelReads.Stream<Integer>> tenStreams = Lists.newArrayList();
    for (int start = 0; start < 100; start += 10) {
      List<Integer> chunk = all.subList(start, start + 10);
      tenStreams.add(() -> rows(chunk));
    }

    assertThat(drain(OrderedParallelReads.of(tenStreams, 4, 2, 8))).containsExactlyElementsOf(all);
  }

  @Test
  void skipsStreamsWithoutRows() {
    List<OrderedParallelReads.Stream<Integer>> streams =
        List.of(
            TestOrderedParallelReads::emptyRows,
            () -> rows(List.of(1)),
            TestOrderedParallelReads::emptyRows);

    assertThat(drain(OrderedParallelReads.of(streams, 3, 2, 2))).containsExactly(1);
  }

  @Test
  void keepsTheStreamOfEachRowWhenAStreamInTheMiddleIsEmpty() {
    List<Integer> many = Lists.newArrayList();
    for (int i = 0; i < 1000; i++) {
      many.add(i);
    }

    List<OrderedParallelReads.Stream<Integer>> streams =
        List.of(() -> rows(many), TestOrderedParallelReads::emptyRows, () -> rows(List.of(1000)));
    OrderedParallelReads<Integer> reads = OrderedParallelReads.of(streams, 3, 2, 8);
    List<Integer> delivered = Lists.newArrayList();
    List<Integer> streamOfRow = Lists.newArrayList();
    while (reads.hasNext()) {
      delivered.add(reads.next());
      streamOfRow.add(reads.currentStream());
    }

    assertThat(delivered).hasSize(1001);
    assertThat(delivered.subList(0, 1000)).containsExactlyElementsOf(many);
    assertThat(delivered.get(1000)).isEqualTo(1000);
    // the entries of a group are attributed to a data file by the stream of the row, so the empty
    // stream must not take a row and the row after it must carry its own stream rather than the
    // empty one's
    assertThat(streamOfRow.subList(0, 1000)).containsOnly(0);
    assertThat(streamOfRow.get(1000)).isEqualTo(2);
  }

  @Test
  void handsBackEveryChunkOfEveryStreamInOrder() {
    List<OrderedParallelReads.Stream<Integer>> streams = Lists.newArrayList();
    List<Integer> expected = Lists.newArrayList();
    for (int stream = 0; stream < 5; stream++) {
      List<Integer> values = Lists.newArrayList();
      for (int row = 0; row < 10; row++) {
        values.add(stream * 10 + row);
      }

      expected.addAll(values);
      streams.add(() -> rows(values));
    }

    // a chunk is smaller than a stream, so each stream spans several chunks and the read has to
    // hand them back in the order of the stream they belong to
    assertThat(drain(OrderedParallelReads.of(streams, 3, 2, 4)))
        .containsExactlyElementsOf(expected);
  }

  @Test
  void closesTheStreamsWhenTheConsumerFailsOnARow() {
    AtomicInteger opened = new AtomicInteger();
    List<AtomicInteger> closes = Lists.newArrayList();
    List<OrderedParallelReads.Stream<Integer>> streams = Lists.newArrayList();
    for (int i = 0; i < 4; i++) {
      AtomicInteger closed = new AtomicInteger();
      closes.add(closed);
      streams.add(() -> tracked(opened, closed, new GeneratedRows(10_000, new AtomicLong())));
    }

    OrderedParallelReads<Integer> reads = OrderedParallelReads.of(streams, 3, 2, 8);
    // a consumer that fails on a row stops asking for more, which is what the entry loop does when
    // it cannot encode a value: the streams it started have to be closed with the read rather than
    // left waiting on a queue nobody drains again
    assertThatThrownBy(
            () -> {
              reads.next();
              throw new IllegalStateException("the value of the row could not be encoded");
            })
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("could not be encoded");
    reads.close();

    awaitTrue(() -> closedStreams(closes) == opened.get() && readerThreads().isEmpty(), 5_000L);
    assertThat(opened.get()).isPositive();
    for (AtomicInteger closed : closes) {
      assertThat(closed.get()).as("a stream that was opened is closed once").isLessThanOrEqualTo(1);
    }

    assertThat(closedStreams(closes)).isEqualTo(opened.get());
    assertThat(readerThreads()).as("no reader thread outlives the read").isEmpty();
  }

  @Test
  void failsTheReadWhenAStreamFailsWhileItsNextChunkIsRead() {
    CountDownLatch chunksDrained = new CountDownLatch(1);
    List<OrderedParallelReads.Stream<Integer>> streams = Lists.newArrayList();
    streams.add(
        () ->
            new FailingAfterRows(
                4, chunksDrained, "the second chunk of the file could not be read"));
    streams.add(() -> rows(List.of(8, 9)));

    OrderedParallelReads<Integer> reads = OrderedParallelReads.of(streams, 2, 2, 2);
    List<Integer> delivered = Lists.newArrayList();
    // the four rows of the first two chunks are delivered, and the failure that follows must reach
    // the consumer as that failure rather than as the end of the rows, because a silently short
    // group would be written as a complete index file
    assertThatThrownBy(
            () -> {
              while (reads.hasNext()) {
                delivered.add(reads.next());
                if (delivered.size() == 4) {
                  chunksDrained.countDown();
                }
              }
            })
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("the second chunk of the file could not be read");
    assertThat(delivered).containsExactly(0, 1, 2, 3);
  }

  @Test
  void yieldsNothingWhenThereAreNoStreams() {
    assertThat(drain(OrderedParallelReads.<Integer>of(Lists.newArrayList(), 4, 2, 2))).isEmpty();
  }

  @Test
  void readsASingleStreamOnTheCallingThread() {
    AtomicReference<String> openedOn = new AtomicReference<>();
    List<OrderedParallelReads.Stream<Integer>> streams = Lists.newArrayList();
    streams.add(
        () -> {
          openedOn.set(Thread.currentThread().getName());
          return rows(List.of(1, 2, 3));
        });

    assertThat(drain(OrderedParallelReads.of(streams, 4, 2, 2))).containsExactly(1, 2, 3);
    assertThat(openedOn.get()).isEqualTo(Thread.currentThread().getName());
  }

  @Test
  void readsNoMoreThanTheStreamsInFlightAtOnce() {
    AtomicInteger active = new AtomicInteger();
    AtomicInteger mostActive = new AtomicInteger();
    List<OrderedParallelReads.Stream<Integer>> streams = Lists.newArrayList();
    for (int i = 0; i < 6; i++) {
      int index = i;
      streams.add(
          () -> {
            mostActive.accumulateAndGet(active.incrementAndGet(), Math::max);
            return new SlowRows(List.of(index, index), 20L, active::decrementAndGet);
          });
    }

    assertThat(drain(OrderedParallelReads.of(streams, 4, 2, 2))).hasSize(12);
    assertThat(mostActive.get()).isEqualTo(4);
  }

  @Test
  void boundsTheRowsHeldWhileTheConsumerIsSlow() throws Exception {
    int rowsPerStream = 10_000;
    AtomicLong produced = new AtomicLong();
    List<OrderedParallelReads.Stream<Integer>> streams = Lists.newArrayList();
    for (int i = 0; i < 2; i++) {
      streams.add(() -> new GeneratedRows(rowsPerStream, produced));
    }

    OrderedParallelReads<Integer> reads = OrderedParallelReads.of(streams, 2, 2, 8);
    assertThat(reads.hasNext()).isTrue();
    reads.next();

    // nothing is consumed for a while, so the readers fill their queues and stop: a stream may hold
    // its queued chunks and the chunk it is offering, and no more
    Thread.sleep(250L);
    long buffered = produced.get();
    assertThat(buffered).isLessThanOrEqualTo(2L * (2 + 2) * 8);
    assertThat(buffered).isLessThan(rowsPerStream / 4L);

    assertThat(drain(reads)).hasSize(2 * rowsPerStream - 1);
  }

  @Test
  void failsTheReadWhenAStreamFails() {
    AtomicInteger opened = new AtomicInteger();
    AtomicInteger closed = new AtomicInteger();
    List<OrderedParallelReads.Stream<Integer>> streams = Lists.newArrayList();
    streams.add(() -> tracked(opened, closed, new SlowRows(List.of(0, 1, 2, 3), 20L)));
    streams.add(() -> tracked(opened, closed, new FailingRows(2, "file 2 could not be read")));
    streams.add(() -> tracked(opened, closed, new SlowRows(List.of(4, 5, 6, 7), 20L)));
    streams.add(() -> tracked(opened, closed, new SlowRows(List.of(8, 9), 20L)));

    OrderedParallelReads<Integer> reads = OrderedParallelReads.of(streams, 3, 2, 2);
    List<Integer> delivered = Lists.newArrayList();
    assertThatThrownBy(
            () -> {
              while (reads.hasNext()) {
                delivered.add(reads.next());
              }
            })
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("file 2 could not be read");

    // the streams that were still reading when the failure arrived are stopped rather than left
    // waiting on a queue nobody drains again
    awaitTrue(() -> opened.get() > 0 && closed.get() == opened.get(), 5_000L);
    assertThat(closed.get()).isEqualTo(opened.get());
  }

  @Test
  void failsASingleStreamThatFails() {
    AtomicInteger opened = new AtomicInteger();
    List<OrderedParallelReads.Stream<Integer>> streams = Lists.newArrayList();
    streams.add(
        () -> {
          opened.incrementAndGet();
          return new FailingRows(1, "file could not be read");
        });

    OrderedParallelReads<Integer> reads = OrderedParallelReads.of(streams, 4, 2, 2);
    assertThatThrownBy(() -> drain(reads))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("file could not be read");

    // the failed stream is not opened a second time by a caller that asks for more rows
    assertThat(reads.hasNext()).isFalse();
    assertThat(opened.get()).isEqualTo(1);
  }

  @Test
  void failsTheReadWhenAStreamCannotBeOpened() {
    List<OrderedParallelReads.Stream<Integer>> streams = Lists.newArrayList();
    streams.add(() -> rows(List.of(1)));
    streams.add(
        () -> {
          throw new IOException("no such file");
        });

    assertThatThrownBy(() -> drain(OrderedParallelReads.of(streams, 2, 2, 2)))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Failed to read the data files of an index group")
        .hasCauseInstanceOf(IOException.class);
  }

  @Test
  void stopsTheStreamsWhenTheReadIsClosed() {
    AtomicInteger opened = new AtomicInteger();
    AtomicInteger closed = new AtomicInteger();
    List<OrderedParallelReads.Stream<Integer>> streams = Lists.newArrayList();
    for (int i = 0; i < 3; i++) {
      streams.add(() -> tracked(opened, closed, new GeneratedRows(10_000, new AtomicLong())));
    }

    OrderedParallelReads<Integer> reads = OrderedParallelReads.of(streams, 3, 2, 8);
    assertThat(reads.hasNext()).isTrue();
    reads.next();
    reads.close();

    awaitTrue(() -> closed.get() == opened.get(), 5_000L);
    assertThat(closed.get()).isEqualTo(opened.get());
  }

  private static CloseableIterator<Integer> emptyRows() {
    return rows(List.of());
  }

  private static CloseableIterator<Integer> rows(List<Integer> values) {
    return CloseableIterator.withClose(values.iterator());
  }

  private static CloseableIterator<Integer> tracked(
      AtomicInteger opened, AtomicInteger closed, CloseableIterator<Integer> rows) {
    opened.incrementAndGet();
    return new CloseableIterator<Integer>() {
      @Override
      public boolean hasNext() {
        return rows.hasNext();
      }

      @Override
      public Integer next() {
        return rows.next();
      }

      @Override
      public void close() throws IOException {
        closed.incrementAndGet();
        rows.close();
      }
    };
  }

  private static List<Integer> drain(OrderedParallelReads<Integer> reads) {
    List<Integer> rows = Lists.newArrayList();
    while (reads.hasNext()) {
      rows.add(reads.next());
    }
    return rows;
  }

  private static int closedStreams(List<AtomicInteger> closes) {
    int closed = 0;
    for (AtomicInteger close : closes) {
      closed += close.get();
    }

    return closed;
  }

  /** The reader threads of the reads that are still running. */
  private static List<String> readerThreads() {
    List<String> names = Lists.newArrayList();
    for (Thread thread : Thread.getAllStackTraces().keySet()) {
      if (thread.getName().startsWith("index-group-read")) {
        names.add(thread.getName());
      }
    }

    return names;
  }

  private static void awaitTrue(BooleanSupplier condition, long waitMillis) {
    long deadline = System.currentTimeMillis() + waitMillis;
    while (!condition.getAsBoolean() && System.currentTimeMillis() < deadline) {
      try {
        Thread.sleep(10L);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  /** Rows that take their time, so a reader lands in between two of them. */
  private static final class SlowRows implements CloseableIterator<Integer> {
    private final Iterator<Integer> rows;
    private final long millis;
    private final Runnable onClose;

    private SlowRows(List<Integer> rows, long millis) {
      this(rows, millis, () -> {});
    }

    private SlowRows(List<Integer> rows, long millis, Runnable onClose) {
      this.rows = rows.iterator();
      this.millis = millis;
      this.onClose = onClose;
    }

    @Override
    public boolean hasNext() {
      if (millis > 0L) {
        try {
          Thread.sleep(millis);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }

      return rows.hasNext();
    }

    @Override
    public Integer next() {
      return rows.next();
    }

    @Override
    public void close() {
      onClose.run();
    }
  }

  /**
   * Rows produced as they are asked for, counting how far ahead of the consumer the reader goes.
   */
  private static final class GeneratedRows implements CloseableIterator<Integer> {
    private final int count;
    private final AtomicLong produced;
    private int next = 0;

    private GeneratedRows(int count, AtomicLong produced) {
      this.count = count;
      this.produced = produced;
    }

    @Override
    public boolean hasNext() {
      return next < count;
    }

    @Override
    public Integer next() {
      if (next >= count) {
        throw new NoSuchElementException("No more rows");
      }

      produced.incrementAndGet();
      return next++;
    }

    @Override
    public void close() {}
  }

  /**
   * Rows that yield their first chunks and then wait for the consumer before failing, so the
   * failure lands while the read is filling the chunk after them rather than before any row reached
   * the consumer.
   */
  private static final class FailingAfterRows implements CloseableIterator<Integer> {
    private final int readable;
    private final CountDownLatch chunksDrained;
    private final String message;
    private int next = 0;

    private FailingAfterRows(int readable, CountDownLatch chunksDrained, String message) {
      this.readable = readable;
      this.chunksDrained = chunksDrained;
      this.message = message;
    }

    @Override
    public boolean hasNext() {
      if (next < readable) {
        return true;
      }

      try {
        if (!chunksDrained.await(10L, TimeUnit.SECONDS)) {
          throw new IllegalStateException("the consumer did not read the rows before the failure");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("interrupted while the file was failing to read", e);
      }

      throw new IllegalStateException(message);
    }

    @Override
    public Integer next() {
      return next++;
    }

    @Override
    public void close() {}
  }

  /** A stream that yields the given rows and then fails, as an unreadable file would. */
  private static final class FailingRows implements CloseableIterator<Integer> {
    private final int readable;
    private final String message;
    private int next = 0;

    private FailingRows(int readable, String message) {
      this.readable = readable;
      this.message = message;
    }

    @Override
    public boolean hasNext() {
      if (next >= readable) {
        throw new IllegalStateException(message);
      }

      return true;
    }

    @Override
    public Integer next() {
      return next++;
    }

    @Override
    public void close() {}
  }
}
