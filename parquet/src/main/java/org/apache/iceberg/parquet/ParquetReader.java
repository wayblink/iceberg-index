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
package org.apache.iceberg.parquet;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.filter2.columnindex.RowRanges;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetReader<T> extends CloseableGroup implements CloseableIterable<T> {
  private final InputFile input;
  private final Schema expectedSchema;
  private final ParquetReadOptions options;
  private final Function<MessageType, ParquetValueReader<?>> readerFunc;
  private final Expression filter;
  private final boolean reuseContainers;
  private final boolean caseSensitive;
  private final NameMapping nameMapping;
  private final List<Long> positions;

  public ParquetReader(
      InputFile input,
      Schema expectedSchema,
      ParquetReadOptions options,
      Function<MessageType, ParquetValueReader<?>> readerFunc,
      NameMapping nameMapping,
      Expression filter,
      boolean reuseContainers,
      boolean caseSensitive) {
    this(
        input,
        expectedSchema,
        options,
        readerFunc,
        nameMapping,
        filter,
        reuseContainers,
        caseSensitive,
        null);
  }

  public ParquetReader(
      InputFile input,
      Schema expectedSchema,
      ParquetReadOptions options,
      Function<MessageType, ParquetValueReader<?>> readerFunc,
      NameMapping nameMapping,
      Expression filter,
      boolean reuseContainers,
      boolean caseSensitive,
      List<Long> positions) {
    this.input = input;
    this.expectedSchema = expectedSchema;
    this.options = options;
    this.readerFunc = readerFunc;
    // replace alwaysTrue with null to avoid extra work evaluating a trivial filter
    this.filter = filter == Expressions.alwaysTrue() ? null : filter;
    this.reuseContainers = reuseContainers;
    this.caseSensitive = caseSensitive;
    this.nameMapping = nameMapping;
    // the requested positions, ascending and distinct: a lookup reads the pages that hold them
    this.positions = positions;
  }

  private ReadConf<T> conf = null;

  private ReadConf<T> init() {
    if (conf == null) {
      ReadConf<T> readConf =
          new ReadConf<>(
              input,
              options,
              expectedSchema,
              filter,
              readerFunc,
              null,
              nameMapping,
              reuseContainers,
              caseSensitive,
              null);
      this.conf = readConf.copy();
      return readConf;
    }
    return conf;
  }

  @Override
  public CloseableIterator<T> iterator() {
    FileIterator<T> iter = new FileIterator<>(init(), positions);
    addCloseable(iter);
    return iter;
  }

  private static class FileIterator<T> implements CloseableIterator<T> {
    private static final Logger LOG = LoggerFactory.getLogger(FileIterator.class);

    private final ParquetFileReader reader;
    private final boolean[] shouldSkip;
    private final ParquetValueReader<T> model;
    private final long totalValues;
    private final boolean reuseContainers;
    // the rows this lookup asked for, ascending and distinct, or null to read the whole file
    private final List<Long> positions;
    private final long fileRowCount;

    private int nextRowGroup = 0;
    private long nextRowGroupStart = 0;
    private long valuesRead = 0;
    private T last = null;

    // where the next row the model returns sits in the file, when only some rows are read
    private long nextRowPosition = 0;
    // the pages of the row group being read, as [start, end) file positions, in ascending order
    private final java.util.Deque<long[]> selectedPages = Lists.newLinkedList();
    private int positionsRead = 0;

    FileIterator(ReadConf<T> conf, List<Long> positions) {
      this.reader = conf.reader();
      this.shouldSkip = conf.shouldSkip();
      this.model = conf.model();
      this.totalValues = conf.totalValues();
      this.reuseContainers = conf.reuseContainers();
      this.positions = positions;
      this.fileRowCount = positions == null ? 0L : rowCount(reader);
    }

    @Override
    public boolean hasNext() {
      if (positions != null) {
        // a position the file does not hold names no row, so the answer is the ones it does hold
        return positionsRead < readablePositions();
      }

      return valuesRead < totalValues;
    }

    @Override
    public T next() {
      try {
        if (positions != null) {
          advanceToNextPosition();
        } else if (valuesRead >= nextRowGroupStart) {
          advance();
        }

        if (reuseContainers) {
          this.last = model.read(last);
        } else {
          this.last = model.read(null);
        }

        if (positions != null) {
          nextRowPosition += 1;
          positionsRead += 1;
          dropPagesBehind();
        } else {
          valuesRead += 1;
        }

        return last;
      } catch (ParquetDecodingException e) {
        if (reader != null) {
          // Knowing the exact parquet file is essential for tracing bad nodes
          // that produced the corrupt file, parquet lib doesn't do this today.
          LOG.error("Error decoding Parquet file {}", reader.getFile(), e);
        }

        throw e;
      }
    }

    private void advance() {
      while (shouldSkip[nextRowGroup]) {
        nextRowGroup += 1;
        reader.skipNextRowGroup();
      }

      PageReadStore pages;
      try {
        pages = reader.readNextRowGroup();
      } catch (IOException e) {
        throw new RuntimeIOException(e);
      }

      nextRowGroupStart += pages.getRowCount();
      nextRowGroup += 1;

      model.setPageSource(pages);
    }

    /** How many of the requested positions this file actually holds. */
    private int readablePositions() {
      int readable = 0;
      for (Long position : positions) {
        if (position < fileRowCount) {
          readable += 1;
        } else {
          break;
        }
      }

      return readable;
    }

    /**
     * Moves the reader onto the next requested row, reading the pages that hold the rows it wants
     * and none of the others.
     */
    private void advanceToNextPosition() {
      while (positionsRead < readablePositions()) {
        long wanted = positions.get(positionsRead);
        if (selectedPages.isEmpty()) {
          PageReadStore pages = readPagesHolding(wanted);
          if (pages == null) {
            return;
          }

          model.setPageSource(pages);
        }

        // the rows of the selected pages that come before the wanted one are decoded and dropped:
        // the page is the unit a columnar format can skip, not the row
        while (!selectedPages.isEmpty() && nextRowPosition < wanted) {
          model.read(null);
          nextRowPosition += 1;
          dropPagesBehind();
        }

        if (selectedPages.isEmpty()) {
          // the pages of this row group are behind us; the next round reads the group that holds it
          continue;
        }

        return;
      }
    }

    /**
     * Drops the pages the reader has finished and moves the cursor to the start of the next one.
     *
     * <p>The pages a filtered read returns are laid out one after another, so the row after the
     * last row of one page is the first row of the next selected page, not the row that follows it
     * in the file.
     */
    private void dropPagesBehind() {
      while (!selectedPages.isEmpty() && nextRowPosition >= selectedPages.peekFirst()[1]) {
        selectedPages.pollFirst();
        if (!selectedPages.isEmpty()) {
          nextRowPosition = selectedPages.peekFirst()[0];
        }
      }
    }

    /**
     * The pages of the next row group that hold wanted rows, or null when no row group left holds
     * one. A format without an offset index cannot say where a row sits, so its row groups are read
     * whole.
     */
    private PageReadStore readPagesHolding(long wanted) {
      List<BlockMetaData> blocks = reader.getRowGroups();
      while (nextRowGroup < blocks.size()) {
        boolean skip = shouldSkip[nextRowGroup];
        BlockMetaData block = blocks.get(nextRowGroup);
        long start = nextRowGroupStart;
        long end = start + block.getRowCount();
        int blockIndex = nextRowGroup;
        nextRowGroup += 1;
        nextRowGroupStart = end;

        if (skip || end <= wanted) {
          continue;
        }

        long lastWanted = positions.get(readablePositions() - 1);
        if (start > lastWanted) {
          return null;
        }

        try {
          if (block.getColumns().isEmpty()) {
            return null;
          }

          OffsetIndex offsetIndex = reader.readOffsetIndex(block.getColumns().get(0));
          List<Integer> pages = Lists.newArrayList();
          selectedPages.clear();
          int cursor = positionsRead;
          for (int page = 0; page < offsetIndex.getPageCount(); page += 1) {
            long pageStart = offsetIndex.getFirstRowIndex(page);
            long pageEnd = offsetIndex.getLastRowIndex(page, block.getRowCount()) + 1;
            boolean holds = false;
            while (cursor < positions.size() && start + positions.get(cursor) < pageEnd) {
              long position = start + positions.get(cursor);
              holds |= position >= pageStart;
              cursor += 1;
            }

            if (holds) {
              pages.add(page);
              selectedPages.add(new long[] {start + pageStart, start + pageEnd});
            }
          }

          if (pages.isEmpty()) {
            continue;
          }

          RowRanges ranges =
              RowRanges.create(
                  block.getRowCount(),
                  pages.stream().mapToInt(Integer::intValue).iterator(),
                  offsetIndex);
          PageReadStore pagesRead = reader.readFilteredRowGroup(blockIndex, ranges);
          nextRowPosition = selectedPages.peekFirst()[0];
          return pagesRead;
        } catch (IOException e) {
          throw new RuntimeIOException(e);
        }
      }

      return null;
    }

    private static long rowCount(ParquetFileReader reader) {
      long rows = 0L;
      for (BlockMetaData block : reader.getRowGroups()) {
        rows += block.getRowCount();
      }

      return rows;
    }

    @Override
    public void close() throws IOException {
      reader.close();
    }
  }
}
