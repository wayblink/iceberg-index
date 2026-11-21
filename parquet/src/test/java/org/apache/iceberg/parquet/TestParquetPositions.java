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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Reading only the rows a lookup names, by file position.
 *
 * <p>A lookup knows which rows it wants, and a file holds millions of them, so the read has to land
 * on the row groups that hold the requested rows and skip the rest of the file. What says it did is
 * how much of the file was read: a reader that answers the requested rows correctly after decoding
 * everything has not made the lookup cheap, which is the problem this exists for.
 *
 * <p>The rows asked for come back with their own values. A filtered page read decodes the pages
 * that hold a wanted row, and the pages of two columns do not start and end on the same rows, so
 * one column's values can come back with another column's rows without anything failing. The reader
 * therefore reads whole row groups, and the checks here read a table with a narrow column and a
 * wide one, which is the shape that drifts apart when pages are read in part.
 */
class TestParquetPositions {
  private static final int ROW_COUNT = 100_000;
  private static final int DIMENSION = 128;

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()),
          optional(2, "vector", Types.ListType.ofRequired(3, Types.FloatType.get())));

  @TempDir private Path temp;

  @Test
  void readsTheRequestedRowsWithTheirOwnValues() throws IOException {
    File file = writeVectors();

    List<Long> requested = ImmutableList.of(50_000L, 1L, 99_999L);
    CountingInputFile counting = new CountingInputFile(Files.localInput(file));

    List<Long> ids = Lists.newArrayList();
    try (CloseableIterable<Record> rows = reader(counting).positions(requested).build()) {
      for (Record row : rows) {
        long id = (Long) row.getField("id");
        ids.add(id);
        assertThat(firstValue(row))
            .as("the first value of the vector of the row at position %d", id)
            .isEqualTo((float) id);
        assertThat((List<?>) row.getField("vector"))
            .as("the vector read with the row at position %d", id)
            .hasSize(DIMENSION);
      }
    }

    assertThat(ids)
        .as("the rows asked for, in file order, however the request was ordered")
        .containsExactly(1L, 50_000L, 99_999L);
  }

  @Test
  void readsOnlyTheRowGroupsThatHoldTheRows() throws IOException {
    File file = writeVectors();

    // three rows of the first row group, out of the several groups the file holds
    List<Long> requested = ImmutableList.of(1L, 100L, 200L);
    CountingInputFile counting = new CountingInputFile(Files.localInput(file));

    List<Long> ids = Lists.newArrayList();
    try (CloseableIterable<Record> rows = reader(counting).positions(requested).build()) {
      for (Record row : rows) {
        ids.add((Long) row.getField("id"));
      }
    }

    assertThat(ids).as("the rows asked for").containsExactly(1L, 100L, 200L);
    assertThat(counting.bytesRead())
        .as("a read of three rows of one row group does not decode the whole file")
        .isLessThan(file.length() / 2);
  }

  @Test
  void readsNothingForPositionsBeyondTheFile() throws IOException {
    File file = writeVectors();

    List<Long> ids = Lists.newArrayList();
    try (CloseableIterable<Record> rows =
        reader(Files.localInput(file)).positions(ImmutableList.of(ROW_COUNT + 100L)).build()) {
      for (Record row : rows) {
        ids.add((Long) row.getField("id"));
      }
    }

    assertThat(ids).as("a position the file does not hold names no row").isEmpty();
  }

  private static Object firstValue(Record row) {
    return ((List<?>) row.getField("vector")).get(0);
  }

  private Parquet.ReadBuilder reader(InputFile input) {
    return Parquet.read(input)
        .project(SCHEMA)
        .createReaderFunc(
            messageType ->
                org.apache.iceberg.data.parquet.GenericParquetReaders.buildReader(
                    SCHEMA, messageType));
  }

  private File writeVectors() throws IOException {
    File file = new File(temp.toFile(), "positions-" + java.util.UUID.randomUUID() + ".parquet");

    List<Record> records = Lists.newArrayListWithCapacity(ROW_COUNT);
    for (long row = 0; row < ROW_COUNT; row += 1) {
      long id = row;
      GenericRecord record = GenericRecord.create(SCHEMA);
      record.setField("id", id);
      List<Float> vector =
          LongStream.range(0, DIMENSION)
              .mapToObj(offset -> (float) (offset + id))
              .collect(Collectors.toList());
      record.setField("vector", vector);
      records.add(record);
    }

    try (org.apache.iceberg.io.FileAppender<Record> writer =
        Parquet.write(Files.localOutput(file))
            .schema(SCHEMA)
            .createWriterFunc(org.apache.iceberg.data.parquet.GenericParquetWriter::create)
            // small row groups, so the rows asked for are a small part of the file: the point of a
            // positional read is what it does not decode
            .set("write.parquet.row-group-size-bytes", "262144")
            .set("write.parquet.page-size-bytes", "16384")
            .build()) {
      writer.addAll(records);
    }

    return file;
  }

  /** An input file that counts the bytes the reader asked for. */
  private static class CountingInputFile implements InputFile {
    private final InputFile delegate;
    private long bytesRead = 0L;

    private CountingInputFile(InputFile delegate) {
      this.delegate = delegate;
    }

    long bytesRead() {
      return bytesRead;
    }

    @Override
    public long getLength() {
      return delegate.getLength();
    }

    @Override
    public SeekableInputStream newStream() {
      return new CountingStream(delegate.newStream());
    }

    @Override
    public String location() {
      return delegate.location();
    }

    @Override
    public boolean exists() {
      return delegate.exists();
    }

    private class CountingStream extends SeekableInputStream {
      private final SeekableInputStream delegateStream;

      private CountingStream(SeekableInputStream delegateStream) {
        this.delegateStream = delegateStream;
      }

      @Override
      public long getPos() throws IOException {
        return delegateStream.getPos();
      }

      @Override
      public void seek(long newPos) throws IOException {
        delegateStream.seek(newPos);
      }

      @Override
      public int read() throws IOException {
        int value = delegateStream.read();
        if (value >= 0) {
          bytesRead += 1;
        }

        return value;
      }

      @Override
      public int read(byte[] bytes, int off, int len) throws IOException {
        int read = delegateStream.read(bytes, off, len);
        if (read > 0) {
          bytesRead += read;
        }

        return read;
      }

      @Override
      public void close() throws IOException {
        delegateStream.close();
      }
    }
  }
}
