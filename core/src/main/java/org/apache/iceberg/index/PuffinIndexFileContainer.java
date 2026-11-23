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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.BlobMetadata;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinCompressionCodec;
import org.apache.iceberg.puffin.PuffinReader;
import org.apache.iceberg.puffin.PuffinWriter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.Pair;

/**
 * {@link IndexFileContainer} backed by the Puffin format.
 *
 * <p>Each index blob becomes one Puffin blob with its index blob type preserved. Puffin's snapshot
 * and sequence number fields are not used by the index layer; the binding between an index file and
 * a table snapshot lives in {@link IndexSnapshot}.
 */
public final class PuffinIndexFileContainer implements IndexFileContainer {

  private static final List<Integer> NO_INPUT_FIELDS = ImmutableList.of();

  /** The codec an index blob is written with, overridable for a measurement or a rollback. */
  public static final String INDEX_BLOB_COMPRESSION_PROPERTY =
      "iceberg.index.blob-compression-codec";

  private static final String DEFAULT_INDEX_BLOB_COMPRESSION =
      PuffinCompressionCodec.ZSTD.codecName();

  private static final long NO_SNAPSHOT_ID = -1L;
  private static final long NO_SEQUENCE_NUMBER = -1L;

  @Override
  public void write(OutputFile file, List<IndexBlob> blobs) throws IOException {
    Preconditions.checkNotNull(file, "Output file is null");
    Preconditions.checkNotNull(blobs, "Blobs are null");
    // blobs are compressed by default: an index payload is the vectors of a table plus a row
    // reference per entry, and neither the vectors nor the references compress on their own, but
    // together, and as a blob, they compress about as well as the column the index was built from.
    // Reading needs no change: a Puffin reader decompresses a blob by the codec in its metadata.
    String codecName =
        System.getProperty(INDEX_BLOB_COMPRESSION_PROPERTY, DEFAULT_INDEX_BLOB_COMPRESSION);
    // the uncompressed codec has no name of its own, so it is spelled out rather than looked up
    PuffinCompressionCodec compression =
        codecName == null || codecName.isEmpty() || "none".equalsIgnoreCase(codecName)
            ? PuffinCompressionCodec.NONE
            : PuffinCompressionCodec.forName(codecName);
    try (PuffinWriter writer =
        Puffin.write(file).createdBy("iceberg-index").compressBlobs(compression).build()) {
      for (IndexBlob blob : blobs) {
        writer.add(
            new Blob(
                blob.type(),
                NO_INPUT_FIELDS,
                NO_SNAPSHOT_ID,
                NO_SEQUENCE_NUMBER,
                ByteBuffer.wrap(blob.payload()),
                blob.requiresUncompressedStorage() ? PuffinCompressionCodec.NONE : null,
                Map.of()));
      }
      writer.finish();
    }
  }

  @Override
  public List<IndexBlob> read(InputFile file) throws IOException {
    return read(file, 0L);
  }

  /**
   * Reads the blobs of a file whose length is already known.
   *
   * <p>The length is passed to the Puffin reader, so the reader does not ask the file for it: a
   * known length is what keeps the open from probing storage for a size the index manifest already
   * recorded.
   */
  public List<IndexBlob> read(InputFile file, long knownFileSize) throws IOException {
    Preconditions.checkNotNull(file, "Input file is null");
    Puffin.ReadBuilder builder = Puffin.read(file);
    if (knownFileSize > 0) {
      builder = builder.withFileSize(knownFileSize);
    }

    try (PuffinReader reader = builder.build()) {
      List<BlobMetadata> blobMetadata = reader.fileMetadata().blobs();
      List<IndexBlob> blobs = Lists.newArrayList();
      for (Pair<BlobMetadata, ByteBuffer> read : reader.readAll(blobMetadata)) {
        blobs.add(IndexBlob.of(read.first().type(), payloadOf(read.second())));
      }
      return ImmutableList.copyOf(blobs);
    }
  }

  /**
   * The first bytes of the first blob of a file that has the given type.
   *
   * <p>A payload names the format version that wrote it in its first bytes, so a caller that only
   * needs that version must not materialize a payload that can be hundreds of megabytes: this reads
   * at most {@code maxBytes} of the blob. An uncompressed blob is read in place from its offset. A
   * compressed blob cannot be decompressed from a prefix, so it is read and decompressed whole -
   * still one blob rather than the file - and cut to {@code maxBytes}.
   *
   * @return the first {@code maxBytes} bytes of that blob, or null when the file has no blob of the
   *     given type
   */
  public static byte[] payloadPrefix(InputFile file, String blobType, int maxBytes)
      throws IOException {
    Preconditions.checkNotNull(file, "Input file is null");
    Preconditions.checkNotNull(blobType, "Blob type is null");
    Preconditions.checkArgument(maxBytes > 0, "Max bytes must be positive: %s", maxBytes);

    try (PuffinReader reader = Puffin.read(file).build()) {
      BlobMetadata blob = null;
      for (BlobMetadata metadata : reader.fileMetadata().blobs()) {
        if (blobType.equals(metadata.type())) {
          blob = metadata;
          break;
        }
      }

      if (blob == null) {
        return null;
      }

      if (blob.compressionCodec() == null) {
        // the file is opened a second time because a reader does not expose the stream it holds,
        // and seeking to the blob is still one range of maxBytes rather than the whole file
        return readPrefix(file, blob.offset(), (int) Math.min(blob.length(), maxBytes));
      }

      // a compressed blob is decompressed into one array by the codec, so the prefix is cut from
      // that array; the cost is one blob of the file, not the file
      byte[] payload = payloadOf(Lists.newArrayList(reader.readAll(List.of(blob))).get(0).second());
      return payload.length <= maxBytes ? payload : Arrays.copyOf(payload, maxBytes);
    }
  }

  private static byte[] readPrefix(InputFile file, long offset, int length) throws IOException {
    byte[] prefix = new byte[length];
    try (SeekableInputStream in = file.newStream()) {
      in.seek(offset);
      IOUtil.readFully(in, prefix, 0, length);
    }
    return prefix;
  }

  /**
   * The bytes of a buffer the Puffin reader has just produced, without a second copy.
   *
   * <p>{@link PuffinReader#readAll} wraps a freshly read or decompressed array in every buffer it
   * returns, so the array is already the only copy of the blob. A buffer that does not cover its
   * whole array is copied instead: a caller may not assume a view is the blob.
   */
  static byte[] payloadOf(ByteBuffer data) {
    if (data.hasArray() && data.arrayOffset() == 0 && data.remaining() == data.array().length) {
      return data.array();
    }

    byte[] payload = new byte[data.remaining()];
    data.duplicate().get(payload);
    return payload;
  }
}
