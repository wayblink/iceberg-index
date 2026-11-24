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
package org.apache.iceberg.index;

import static org.apache.iceberg.Files.localInput;
import static org.apache.iceberg.Files.localOutput;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinReader;
import org.apache.iceberg.util.JsonUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestPuffinIndexFileContainer {

  // the fixed tail of a Puffin file: the footer payload length, the flags and the magic
  private static final int FOOTER_STRUCT_LENGTH = 3 * Integer.BYTES;

  private final IndexFileContainer container = new PuffinIndexFileContainer();

  @Test
  public void aBlobTakesOwnershipOfThePayloadItIsGiven() {
    // a container hands over the bytes it has just read, and a reader hands them straight to an
    // adapter: every copy on that path is another copy of a file that can be hundreds of megabytes
    byte[] payload = {1, 2, 3};

    IndexBlob blob = IndexBlob.of("iceberg-testing-entries-v1", payload);

    assertThat(blob.payloadUnsafe())
        .as("the payload is not copied on construction")
        .isSameAs(payload);
    assertThat(blob.payload()).as("the public accessor still copies").isNotSameAs(payload);
    assertThat(blob.payload()).containsExactly(1, 2, 3);
    assertThat(blob.takePayload()).as("a reader takes the array without a copy").isSameAs(payload);
  }

  @Test
  public void aReaderTakesTheArrayThePuffinReaderProduced() {
    byte[] payload = {1, 2, 3, 4};
    assertThat(PuffinIndexFileContainer.payloadOf(ByteBuffer.wrap(payload)))
        .as("a whole array-backed buffer is handed over, not copied")
        .isSameAs(payload);

    ByteBuffer view = ByteBuffer.wrap(payload, 1, 2);
    assertThat(PuffinIndexFileContainer.payloadOf(view))
        .as("a buffer that does not cover its array is copied")
        .containsExactly(2, 3)
        .isNotSameAs(payload);
  }

  @Test
  public void roundTripsMultipleBlobs(@TempDir Path tempDir) throws IOException {
    String location = tempDir.resolve("index-file-1.puffin").toString();
    IndexBlob first = IndexBlob.of("iceberg-vector-entries-v1", new byte[] {1, 2, 3, 4});
    IndexBlob second = IndexBlob.of("iceberg-vector-graph-v1", new byte[] {9});

    container.write(localOutput(location), List.of(first, second));
    assertThat(container.read(localInput(location))).containsExactly(first, second);
  }

  @Test
  void preservesRangesInBlobsThatRequireUncompressedStorage(@TempDir Path tempDir)
      throws IOException {
    String location = tempDir.resolve("range-readable.puffin").toString();
    IndexBlob blob = IndexBlob.uncompressed("iceberg-text-bm25-v1", new byte[] {1, 2, 3});
    container.write(localOutput(location), List.of(blob));
    try (PuffinReader reader = Puffin.read(localInput(location)).build()) {
      assertThat(reader.fileMetadata().blobs())
          .singleElement()
          .satisfies(
              metadata -> {
                assertThat(metadata.compressionCodec()).isNull();
                assertThat(metadata.length()).isEqualTo(3);
              });
    }
  }

  @Test
  public void compressesBlobsAndReadsThemBack(@TempDir Path tempDir) throws IOException {
    String location = tempDir.resolve("index-file-1.puffin").toString();
    // an index payload is many entries over few data files, which repeats long runs of the same
    // bytes, so it compresses about as well as the column the index was built from
    byte[] payload = new byte[1 << 20];
    for (int i = 0; i < payload.length; i++) {
      payload[i] = (byte) (i % 97);
    }
    IndexBlob blob = IndexBlob.of("iceberg-vector-entries-v2", payload);

    container.write(localOutput(location), List.of(blob));

    assertThat(container.read(localInput(location))).containsExactly(blob);
    assertThat(java.nio.file.Files.size(java.nio.file.Paths.get(location)))
        .isLessThan(payload.length / 2);
  }

  @Test
  public void canBeToldToWriteBlobsUncompressed(@TempDir Path tempDir) throws IOException {
    String location = tempDir.resolve("index-file-1.puffin").toString();
    byte[] payload = new byte[1 << 20];
    for (int i = 0; i < payload.length; i++) {
      payload[i] = (byte) (i % 97);
    }
    IndexBlob blob = IndexBlob.of("iceberg-vector-entries-v2", payload);

    System.setProperty(PuffinIndexFileContainer.INDEX_BLOB_COMPRESSION_PROPERTY, "none");
    try {
      container.write(localOutput(location), List.of(blob));
      assertThat(container.read(localInput(location))).containsExactly(blob);
      assertThat(java.nio.file.Files.size(java.nio.file.Paths.get(location)))
          .isGreaterThan(payload.length);
    } finally {
      System.clearProperty(PuffinIndexFileContainer.INDEX_BLOB_COMPRESSION_PROPERTY);
    }
  }

  @Test
  void readsOnlyTheHeadOfAnUncompressedBlob(@TempDir Path tempDir) throws IOException {
    String location = tempDir.resolve("uncompressed.puffin").toString();
    // an uncompressed blob is the case that costs the driver heap today: a text index payload of a
    // large table is read whole only to look at its first four bytes
    byte[] payload = new byte[1 << 20];
    payload[3] = 2;
    for (int i = Integer.BYTES; i < payload.length; i++) {
      payload[i] = (byte) (i % 97);
    }
    IndexBlob blob = IndexBlob.uncompressed("iceberg-text-bm25-v1", payload);
    container.write(localOutput(location), List.of(blob));

    CountingInputFile file = new CountingInputFile(localInput(location));
    byte[] prefix = PuffinIndexFileContainer.payloadPrefix(file, blob.type(), 16);

    assertThat(prefix).as("the head of the payload").isEqualTo(Arrays.copyOf(payload, 16));
    assertThat(file.bytesRead())
        .as("a header-sized prefix must not cost the megabyte the payload is")
        .isLessThan(64 * 1024)
        .isLessThan(localInput(location).getLength());
  }

  @Test
  void readsOneCompressedBlobAndCutsItToThePrefix(@TempDir Path tempDir) throws IOException {
    String location = tempDir.resolve("compressed.puffin").toString();
    // a compressed blob is decoded from its start, but the other blob of the file is not read
    IndexBlob wanted = IndexBlob.of("iceberg-vector-entries-v2", new byte[] {0, 0, 0, 7, 42, 43});
    byte[] filler = new byte[1 << 20];
    new Random(7).nextBytes(filler);
    IndexBlob other = IndexBlob.of("iceberg-vector-graph-v2", filler);
    container.write(localOutput(location), List.of(wanted, other));

    CountingInputFile file = new CountingInputFile(localInput(location));
    assertThat(PuffinIndexFileContainer.payloadPrefix(file, wanted.type(), 4))
        .containsExactly(0, 0, 0, 7);
    assertThat(file.bytesRead())
        .as("the matching blob is decoded, the other megabyte is never read")
        .isLessThan(64 * 1024);
  }

  @Test
  void decodesOnlyTheHeadOfACompressedBlob(@TempDir Path tempDir) throws IOException {
    String location = tempDir.resolve("compressed-large.puffin").toString();
    // a prefix of a compressed blob has to be decoded from its start, but the blocks behind the
    // head
    // are not needed: a FLAT vector index can write the vectors of the whole table as one blob, and
    // decoding them into one array is what a check of the index must not do
    byte[] payload = new byte[8 << 20];
    new Random(7).nextBytes(payload);
    payload[0] = 0;
    payload[1] = 0;
    payload[2] = 0;
    payload[3] = 7;
    IndexBlob blob = IndexBlob.of("iceberg-vector-entries-v2", payload);
    container.write(localOutput(location), List.of(blob));

    CountingInputFile file = new CountingInputFile(localInput(location));
    assertThat(PuffinIndexFileContainer.payloadPrefix(file, blob.type(), 16))
        .isEqualTo(Arrays.copyOf(payload, 16));
    assertThat(file.bytesRead())
        .as("a header-sized prefix must not read the payload it is a prefix of")
        .isLessThan(2 * 1024 * 1024)
        .isLessThan(payload.length);
  }

  @Test
  void namesABlobBeforeTheStartOfTheFile(@TempDir Path tempDir) throws IOException {
    Path file = fileWithBlobMetadata(tempDir.resolve("negative-offset.puffin"), "offset", -1L);

    assertThatThrownBy(
            () ->
                PuffinIndexFileContainer.payloadPrefix(
                    localInput(file.toString()), "iceberg-testing-entries-v1", 16))
        .as("a footer that names a blob before the start of the file says which blob it is")
        .isInstanceOf(IOException.class)
        .hasMessageContaining("offset -1");
  }

  @Test
  void namesABlobOfNoLength(@TempDir Path tempDir) throws IOException {
    Path file = fileWithBlobMetadata(tempDir.resolve("zero-length.puffin"), "length", 0L);

    assertThatThrownBy(
            () ->
                PuffinIndexFileContainer.payloadPrefix(
                    localInput(file.toString()), "iceberg-testing-entries-v1", 16))
        .as("a footer that names a blob of no length says which blob it is")
        .isInstanceOf(IOException.class)
        .hasMessageContaining("length 0");
  }

  /**
   * A file whose first blob is named with the given offset or length.
   *
   * <p>A damaged footer usually fails the magic or the size it names; one that still parses can
   * name metadata no file can contain, which is the case these tests are about. The footer is
   * rewritten rather than the file body, because the metadata is what makes the difference.
   */
  private Path fileWithBlobMetadata(Path file, String field, long value) throws IOException {
    container.write(
        localOutput(file.toString()),
        List.of(IndexBlob.uncompressed("iceberg-testing-entries-v1", new byte[] {1, 2, 3})));
    byte[] content = Files.readAllBytes(file);
    int footerStruct = content.length - FOOTER_STRUCT_LENGTH;
    int payloadLength =
        ByteBuffer.wrap(content, footerStruct, Integer.BYTES)
            .order(ByteOrder.LITTLE_ENDIAN)
            .getInt();
    int payloadStart = footerStruct - payloadLength;
    ObjectNode footer =
        JsonUtil.parse(
            new String(content, payloadStart, payloadLength, StandardCharsets.UTF_8),
            node -> (ObjectNode) node);
    ((ObjectNode) footer.get("blobs").get(0)).put(field, value);
    byte[] payload = JsonUtil.mapper().writeValueAsString(footer).getBytes(StandardCharsets.UTF_8);

    ByteArrayOutputStream rewritten = new ByteArrayOutputStream();
    rewritten.write(content, 0, payloadStart);
    rewritten.write(payload);
    rewritten.write(
        ByteBuffer.allocate(Integer.BYTES)
            .order(ByteOrder.LITTLE_ENDIAN)
            .putInt(payload.length)
            .array());
    rewritten.write(
        content, footerStruct + Integer.BYTES, content.length - footerStruct - Integer.BYTES);
    Files.write(file, rewritten.toByteArray());
    return file;
  }

  @Test
  void readsTheWholeBlobWhenItIsShorterThanThePrefix(@TempDir Path tempDir) throws IOException {
    String location = tempDir.resolve("short.puffin").toString();
    byte[] payload = {1, 2, 3, 4, 5};
    IndexBlob blob = IndexBlob.uncompressed("iceberg-testing-entries-v1", payload);
    container.write(localOutput(location), List.of(blob));

    assertThat(PuffinIndexFileContainer.payloadPrefix(localInput(location), blob.type(), 1024))
        .containsExactly(payload);
  }

  @Test
  void returnsNullWhenNoBlobHasTheType(@TempDir Path tempDir) throws IOException {
    String location = tempDir.resolve("other-type.puffin").toString();
    container.write(
        localOutput(location), List.of(IndexBlob.of("iceberg-testing-entries-v1", new byte[] {1})));

    assertThat(
            PuffinIndexFileContainer.payloadPrefix(
                localInput(location), "iceberg-testing-graph-v1", 16))
        .isNull();
  }

  @Test
  void returnsNullWhenTheFileHasNoBlobs(@TempDir Path tempDir) throws IOException {
    String location = tempDir.resolve("empty.puffin").toString();
    container.write(localOutput(location), List.of());

    assertThat(PuffinIndexFileContainer.payloadPrefix(localInput(location), "iceberg-any-v1", 16))
        .isNull();
  }

  /** An input file that records how many bytes were read, which is what a whole-file read shows. */
  private static class CountingInputFile implements InputFile {
    private final InputFile delegate;
    private long bytesRead;

    private CountingInputFile(InputFile delegate) {
      this.delegate = delegate;
    }

    private long bytesRead() {
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
      private final SeekableInputStream stream;

      private CountingStream(SeekableInputStream stream) {
        this.stream = stream;
      }

      @Override
      public long getPos() throws IOException {
        return stream.getPos();
      }

      @Override
      public void seek(long newPos) throws IOException {
        stream.seek(newPos);
      }

      @Override
      public int read() throws IOException {
        int value = stream.read();
        if (value >= 0) {
          bytesRead++;
        }
        return value;
      }

      @Override
      public int read(byte[] target, int offset, int length) throws IOException {
        int read = stream.read(target, offset, length);
        if (read > 0) {
          bytesRead += read;
        }
        return read;
      }

      @Override
      public int available() throws IOException {
        return stream.available();
      }

      @Override
      public void close() throws IOException {
        stream.close();
      }
    }
  }
}
