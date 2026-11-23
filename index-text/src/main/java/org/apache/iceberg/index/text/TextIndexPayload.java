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
package org.apache.iceberg.index.text;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;
import org.apache.iceberg.index.text.TextIndexAdapter.CorpusCounts;
import org.apache.iceberg.index.text.TextIndexAdapter.Document;
import org.apache.iceberg.index.text.TextIndexAdapter.FileIndex;
import org.apache.iceberg.index.text.TextIndexAdapter.Posting;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

/**
 * The payload layout of a text index file: the corpus counts, the file table, one dense
 * document-length column, and the dictionary with its postings.
 *
 * <p>A document's data file and row position are not stored per document. An index file covers the
 * rows of its data files in order, so a file's rows are consecutive: the file table records where
 * each file's first document starts, which makes the position of any document a subtraction. Only
 * the lengths are per document, because scoring needs them and they follow from nothing else; they
 * are a fixed-width column that a query reads in one range instead of the per-document records v2
 * decoded one table chunk at a time.
 */
final class TextIndexPayload {
  /** The layout version this build writes; it is part of the payload signature. */
  static final int FORMAT_VERSION = 1;

  static final int MAGIC = 0x424D3100 | FORMAT_VERSION;

  private static final int BUCKET_COUNT = 64;

  /** A length at or above this value is stored in the overflow list instead of the column. */
  static final int MAX_INLINE_LENGTH = 0xFFFF;

  static final int HEADER_SIZE = 4 + 8 + 8 + 4 * 7 + (BUCKET_COUNT + 1) * 4;

  private TextIndexPayload() {}

  /**
   * Encodes the payload of one index file, or the snapshot statistics when {@code index} is null.
   *
   * @param counts the corpus counts of the payload: documents, total length and document frequency
   * @param index the local postings and documents, or null to write a statistics dictionary
   */
  static ByteBuffer encode(CorpusCounts counts, FileIndex index) throws IOException {
    List<String> files = index == null ? List.of() : files(index);
    Map<String, Integer> fileIds = Maps.newHashMap();
    for (int id = 0; id < files.size(); id++) {
      fileIds.put(files.get(id), id);
    }

    Documents documents = documents(fileIds, index);
    ByteArrayOutputStream fileSection = fileSection(files, documents.fileStart, documents.fileRow);
    ByteArrayOutputStream lengthsSection = lengthsSection(documents.lengths);
    ByteArrayOutputStream overflowSection = overflowSection(documents.overflow);
    ByteArrayOutputStream exceptionsSection = exceptionsSection(documents.exceptions);
    Dictionary dictionary = dictionary(counts, index);

    int lengthsStart = HEADER_SIZE + fileSection.size();
    int overflowStart = Math.addExact(lengthsStart, lengthsSection.size());
    int exceptionsStart = Math.addExact(overflowStart, overflowSection.size());
    int dictionaryStart = Math.addExact(exceptionsStart, exceptionsSection.size());
    int postingsStart = dictionaryStart;
    for (ByteArrayOutputStream bucket : dictionary.buckets) {
      if (bucket != null) {
        postingsStart = Math.addExact(postingsStart, bucket.size());
      }
    }

    ByteArrayOutputStream bytes =
        new ByteArrayOutputStream(Math.addExact(postingsStart, dictionary.postings.length));
    writeIntBE(bytes, MAGIC);
    writeLongBE(bytes, counts.documents);
    writeLongBE(bytes, counts.tokens);
    writeIntBE(bytes, files.size());
    writeIntBE(bytes, fileSection.size());
    writeIntBE(bytes, lengthsSection.size());
    writeIntBE(bytes, overflowSection.size());
    writeIntBE(bytes, exceptionsSection.size());
    writeIntBE(bytes, dictionaryStart);
    writeIntBE(bytes, postingsStart);
    int position = dictionaryStart;
    for (ByteArrayOutputStream bucket : dictionary.buckets) {
      writeIntBE(bytes, position);
      if (bucket != null) {
        position = Math.addExact(position, bucket.size());
      }
    }

    writeIntBE(bytes, position);
    fileSection.writeTo(bytes);
    lengthsSection.writeTo(bytes);
    overflowSection.writeTo(bytes);
    exceptionsSection.writeTo(bytes);
    for (ByteArrayOutputStream bucket : dictionary.buckets) {
      if (bucket != null) {
        bucket.writeTo(bytes);
      }
    }

    bytes.write(dictionary.postings);
    return ByteBuffer.wrap(bytes.toByteArray());
  }

  /** The per-document column and the tables that recover each document's data file and row. */
  private static Documents documents(Map<String, Integer> fileIds, FileIndex index)
      throws IOException {
    int count = index == null ? 0 : index.documents.size();
    int[] fileStart = new int[fileIds.size()];
    long[] fileRow = new long[fileIds.size()];
    for (int file = 0; file < fileStart.length; file++) {
      fileStart[file] = -1;
    }

    byte[] column = new byte[Math.multiplyExact(count, Short.BYTES)];
    List<long[]> overflow = Lists.newArrayList();
    List<long[]> exceptions = Lists.newArrayList();
    // the decoder maps a document to its file by the first docId of each file, so a file that
    // appears again after another one would be read back as belonging to that other file. The
    // writer sorts documents by file today, but the payload is a long-lived format that must not
    // depend on a caller doing so.
    boolean[] closed = new boolean[fileIds.size()];
    int currentFile = -1;
    for (int doc = 0; doc < count; doc++) {
      Document document = index.documents.get(doc);
      int file = fileIds.get(document.dataFile);
      if (file != currentFile) {
        if (currentFile >= 0) {
          closed[currentFile] = true;
        }

        if (closed[file]) {
          throw new IOException(
              "Invalid BM25 document order: data file "
                  + document.dataFile
                  + " appears again after another file, so its documents are not contiguous");
        }

        currentFile = file;
      }

      if (fileStart[file] < 0) {
        fileStart[file] = doc;
        fileRow[file] = document.rowPosition;
      }

      long expected = Math.addExact(fileRow[file], doc - fileStart[file]);
      if (document.rowPosition != expected) {
        // the rows of a file are almost always consecutive; a file with a gap records the rows
        // that do not follow from its start
        exceptions.add(new long[] {doc, document.rowPosition});
      }

      if (document.length >= MAX_INLINE_LENGTH) {
        column[2 * doc] = (byte) 0xFF;
        column[2 * doc + 1] = (byte) 0xFF;
        overflow.add(new long[] {doc, document.length});
      } else {
        column[2 * doc] = (byte) document.length;
        column[2 * doc + 1] = (byte) (document.length >>> 8);
      }
    }

    return new Documents(column, fileStart, fileRow, overflow, exceptions);
  }

  private static ByteArrayOutputStream fileSection(
      List<String> files, int[] fileStart, long[] fileRow) throws IOException {
    ByteArrayOutputStream section = new ByteArrayOutputStream();
    for (int file = 0; file < files.size(); file++) {
      writeString(section, files.get(file));
      writeVarint(section, fileStart[file]);
      writeLongVarint(section, fileRow[file]);
    }

    return section;
  }

  private static ByteArrayOutputStream lengthsSection(byte[] column) throws IOException {
    ByteArrayOutputStream section = new ByteArrayOutputStream();
    try (DeflaterOutputStream out = new DeflaterOutputStream(section)) {
      out.write(column);
    }

    return section;
  }

  private static ByteArrayOutputStream overflowSection(List<long[]> overflow) throws IOException {
    ByteArrayOutputStream section = new ByteArrayOutputStream();
    writeVarint(section, overflow.size());
    int previous = 0;
    for (long[] entry : overflow) {
      writeVarint(section, Math.toIntExact(entry[0]) - previous);
      writeVarint(section, Math.toIntExact(entry[1]));
      previous = Math.toIntExact(entry[0]);
    }

    return section;
  }

  private static ByteArrayOutputStream exceptionsSection(List<long[]> exceptions)
      throws IOException {
    ByteArrayOutputStream section = new ByteArrayOutputStream();
    writeVarint(section, exceptions.size());
    int previous = 0;
    for (long[] entry : exceptions) {
      writeVarint(section, Math.toIntExact(entry[0]) - previous);
      writeLongVarint(section, entry[1]);
      previous = Math.toIntExact(entry[0]);
    }

    return section;
  }

  /** The dictionary of every term, with each term's posting range written ahead of its bytes. */
  private static Dictionary dictionary(CorpusCounts counts, FileIndex index) throws IOException {
    List<String> terms = Lists.newArrayList(counts.documentFrequency.keySet());
    terms.sort(String::compareTo);

    ByteArrayOutputStream postings = new ByteArrayOutputStream();
    ByteArrayOutputStream[] buckets = new ByteArrayOutputStream[BUCKET_COUNT];
    byte[][] previousTerms = new byte[BUCKET_COUNT][];
    long[] previousRangeOffset = new long[BUCKET_COUNT];
    for (String term : terms) {
      // a term's range is what its posting stream occupies, known before and after writing it, so
      // the dictionary entry is written here instead of holding one range entry per term
      long rangeOffset = postings.size();
      if (index != null) {
        encodePostings(postings, index, term);
      }

      int bucket = bucket(term);
      if (buckets[bucket] == null) {
        buckets[bucket] = new ByteArrayOutputStream();
      }

      byte[] termBytes = term.getBytes(StandardCharsets.UTF_8);
      int shared = sharedPrefix(previousTerms[bucket], termBytes);
      writeVarint(buckets[bucket], shared);
      writeVarint(buckets[bucket], termBytes.length - shared);
      buckets[bucket].write(termBytes, shared, termBytes.length - shared);
      writeLongVarint(buckets[bucket], counts.documentFrequency.get(term));
      writeVarint(buckets[bucket], Math.toIntExact(rangeOffset - previousRangeOffset[bucket]));
      writeVarint(buckets[bucket], Math.toIntExact(postings.size() - rangeOffset));
      previousRangeOffset[bucket] = rangeOffset;
      previousTerms[bucket] = termBytes;
    }

    return new Dictionary(postings.toByteArray(), buckets);
  }

  /** The data files of an index file, in the order their documents appear. */
  private static List<String> files(FileIndex index) {
    Map<String, Boolean> seen = Maps.newLinkedHashMap();
    for (Document document : index.documents) {
      seen.putIfAbsent(document.dataFile, true);
    }

    return Lists.newArrayList(seen.keySet());
  }

  private static void encodePostings(ByteArrayOutputStream bytes, FileIndex index, String term)
      throws IOException {
    List<Posting> postings = index.postingsByTerm.get(term);
    ByteArrayOutputStream compressed = new ByteArrayOutputStream();
    try (DeflaterOutputStream out = new DeflaterOutputStream(compressed)) {
      int previous = -1;
      for (Posting posting : postings) {
        int docId = posting.docId;
        writeVarint(out, previous < 0 ? docId : docId - previous);
        writeVarint(out, posting.termFrequency);
        previous = docId;
      }
    }

    compressed.writeTo(bytes);
  }

  static TextIndexSource.Source source(ByteBuffer bytes) {
    return TextIndexSource.source(bytes);
  }

  static boolean matches(ByteBuffer payload) {
    return payload.remaining() >= Integer.BYTES && payload.getInt(payload.position()) == MAGIC;
  }

  static TextIndexSource.Source source(SeekableInputStream stream, long base) {
    return TextIndexSource.source(stream, base);
  }

  static Reader open(TextIndexSource.Source source, int size) throws IOException {
    check(size >= HEADER_SIZE, "Invalid BM25 payload size");
    ByteBuffer header = source.read(0, HEADER_SIZE);
    check(header.getInt() == MAGIC, "Not a BM25 payload");
    long documents = header.getLong();
    long tokens = header.getLong();
    int fileCount = header.getInt();
    int filesLength = header.getInt();
    int lengthsLength = header.getInt();
    int overflowLength = header.getInt();
    int exceptionsLength = header.getInt();
    int dictionaryStart = header.getInt();
    int postingsStart = header.getInt();
    int[] bucketOffsets = new int[BUCKET_COUNT + 1];
    for (int bucket = 0; bucket <= BUCKET_COUNT; bucket++) {
      bucketOffsets[bucket] = header.getInt();
    }

    checkLayout(
        documents,
        tokens,
        fileCount,
        filesLength,
        lengthsLength,
        overflowLength,
        exceptionsLength,
        dictionaryStart,
        postingsStart,
        bucketOffsets,
        size);
    return new Reader(
        source,
        size,
        documents,
        tokens,
        fileCount,
        filesLength,
        lengthsLength,
        overflowLength,
        exceptionsLength,
        postingsStart,
        bucketOffsets);
  }

  private static void checkLayout(
      long documents,
      long tokens,
      int fileCount,
      int filesLength,
      int lengthsLength,
      int overflowLength,
      int exceptionsLength,
      int dictionaryStart,
      int postingsStart,
      int[] bucketOffsets,
      int size)
      throws IOException {
    check(
        documents >= 0
            && tokens >= 0
            && fileCount >= 0
            && filesLength >= 0
            && lengthsLength >= 0
            && overflowLength >= 0
            && exceptionsLength >= 0,
        "Invalid BM25 payload header");
    long exceptionsStart = (long) HEADER_SIZE + filesLength + lengthsLength + overflowLength;
    check(
        exceptionsStart + exceptionsLength == dictionaryStart
            && dictionaryStart <= postingsStart
            && postingsStart <= size,
        "Invalid BM25 payload layout");
    for (int bucket = 0; bucket <= BUCKET_COUNT; bucket++) {
      check(
          bucketOffsets[bucket] >= dictionaryStart && bucketOffsets[bucket] <= postingsStart,
          "Invalid BM25 dictionary offset");
      check(
          bucket == 0 || bucketOffsets[bucket] >= bucketOffsets[bucket - 1],
          "Invalid BM25 dictionary offset order");
    }
  }

  private static int bucket(String term) {
    return (term.hashCode() & Integer.MAX_VALUE) % BUCKET_COUNT;
  }

  private static void check(boolean condition, String message) throws IOException {
    if (!condition) {
      throw new IOException(message);
    }
  }

  private static int sharedPrefix(byte[] previous, byte[] term) {
    if (previous == null) {
      return 0;
    }

    int max = Math.min(previous.length, term.length);
    int shared = 0;
    while (shared < max && previous[shared] == term[shared]) {
      shared += 1;
    }

    return shared;
  }

  static final class Reader implements TextIndexShard {
    private final TextIndexSource.Source source;
    private final int size;
    private final long documents;
    private final long tokens;
    private final int fileCount;
    private final int filesLength;
    private final int lengthsLength;
    private final int overflowLength;
    private final int exceptionsLength;
    private final int postingsStart;
    private final int[] bucketOffsets;

    private List<String> files;
    private int[] fileStart;
    private long[] fileRow;
    private byte[] column;
    private Map<Integer, Integer> overflow;
    private Map<Integer, Long> exceptions;

    private Reader(
        TextIndexSource.Source source,
        int size,
        long documents,
        long tokens,
        int fileCount,
        int filesLength,
        int lengthsLength,
        int overflowLength,
        int exceptionsLength,
        int postingsStart,
        int[] bucketOffsets) {
      this.source = source;
      this.size = size;
      this.documents = documents;
      this.tokens = tokens;
      this.fileCount = fileCount;
      this.filesLength = filesLength;
      this.lengthsLength = lengthsLength;
      this.overflowLength = overflowLength;
      this.exceptionsLength = exceptionsLength;
      this.postingsStart = postingsStart;
      this.bucketOffsets = bucketOffsets;
    }

    @Override
    public CorpusCounts counts(Set<String> terms) throws IOException {
      Map<String, Long> frequencies = Maps.newHashMap();
      directory(terms).forEach((term, entry) -> frequencies.put(term, entry.frequency()));
      return new CorpusCounts(documents, tokens, frequencies);
    }

    @Override
    public FileIndex shard(Set<String> terms, boolean statisticsOnly) throws IOException {
      check(documents <= Integer.MAX_VALUE, "Invalid BM25 shard document count");
      int localDocuments = Math.toIntExact(documents);
      Map<String, DirectoryEntry> directory = directory(terms);
      if (statisticsOnly) {
        Map<String, Long> frequencies = Maps.newHashMap();
        directory.forEach((term, entry) -> frequencies.put(term, entry.frequency()));
        return new FileIndex(localDocuments, tokens, frequencies, Map.of(), List.of());
      }

      check(lengthsLength > 0 || localDocuments == 0, "BM25 shard has no document table");
      // a term this shard does not hold must not pay for the document table or the file table:
      // both are read when a hit first needs one, not before the term loop
      byte[] decoded = null;
      List<String> fileNames = null;
      Map<String, Long> frequencies = Maps.newHashMap();
      Map<String, List<Posting>> byTerm = Maps.newHashMap();
      List<Document> documentsList = Lists.newArrayList();
      for (Map.Entry<String, DirectoryEntry> term : directory.entrySet()) {
        PostingList list = postings(term.getValue());
        List<Posting> postings = Lists.newArrayListWithCapacity(list.docIds.length);
        int file = 0;
        for (int posting = 0; posting < list.docIds.length; posting++) {
          int docId = list.docIds[posting];
          check(docId >= 0 && docId < localDocuments, "Invalid BM25 posting");
          if (fileNames == null) {
            fileNames = files();
          }

          while (file + 1 < fileStart.length && docId >= fileStart[file + 1]) {
            file += 1;
          }

          if (decoded == null) {
            decoded = column();
          }

          int length = length(docId, decoded);
          check(list.termFrequencies[posting] <= length, "Invalid BM25 posting term frequency");
          long row = Math.addExact(fileRow[file], docId - fileStart[file]);
          Long exception = exceptions().get(docId);
          if (exception != null) {
            row = exception;
          }

          documentsList.add(new Document(fileNames.get(file), row, length));
          postings.add(new Posting(documentsList.size() - 1, list.termFrequencies[posting]));
        }

        frequencies.put(term.getKey(), (long) list.docIds.length);
        byTerm.put(term.getKey(), postings);
      }

      return new FileIndex(localDocuments, tokens, frequencies, byTerm, documentsList);
    }

    private int length(int docId, byte[] decoded) throws IOException {
      int low = decoded[2 * docId] & 0xFF;
      int high = decoded[2 * docId + 1] & 0xFF;
      int value = low | (high << 8);
      if (value != MAX_INLINE_LENGTH) {
        return value;
      }

      Integer overflowed = overflow().get(docId);
      return overflowed == null ? MAX_INLINE_LENGTH : overflowed;
    }

    /** The document lengths as a little-endian column, decoded once per reader. */
    private byte[] column() throws IOException {
      if (column == null) {
        byte[] compressed = bytes(source.read(HEADER_SIZE + filesLength, lengthsLength));
        int expected = Math.multiplyExact(Math.toIntExact(documents), Short.BYTES);
        byte[] decoded = new byte[expected];
        try (DataInputStream in =
            new DataInputStream(new InflaterInputStream(new ByteArrayInputStream(compressed)))) {
          in.readFully(decoded);
          check(in.read() < 0, "Unexpected trailing BM25 lengths");
        }

        column = decoded;
      }

      return column;
    }

    private Map<Integer, Integer> overflow() throws IOException {
      if (overflow == null) {
        int start = HEADER_SIZE + filesLength + lengthsLength;
        Map<Integer, Integer> entries = Maps.newHashMap();
        try (DataInputStream in =
            new DataInputStream(
                new ByteArrayInputStream(bytes(source.read(start, overflowLength))))) {
          if (in.available() > 0) {
            int count = readVarint(in);
            int docId = 0;
            for (int entry = 0; entry < count; entry++) {
              docId = Math.addExact(docId, readVarint(in));
              entries.put(docId, readVarint(in));
            }
          }
        }

        overflow = entries;
      }

      return overflow;
    }

    private Map<Integer, Long> exceptions() throws IOException {
      if (exceptions == null) {
        int start = HEADER_SIZE + filesLength + lengthsLength + overflowLength;
        Map<Integer, Long> entries = Maps.newHashMap();
        try (DataInputStream in =
            new DataInputStream(
                new ByteArrayInputStream(bytes(source.read(start, exceptionsLength))))) {
          if (in.available() > 0) {
            int count = readVarint(in);
            int docId = 0;
            for (int entry = 0; entry < count; entry++) {
              docId = Math.addExact(docId, readVarint(in));
              entries.put(docId, readLongVarint(in));
            }
          }
        }

        exceptions = entries;
      }

      return exceptions;
    }

    private PostingList postings(DirectoryEntry entry) throws IOException {
      int postingCount = Math.toIntExact(entry.frequency());
      byte[] compressed =
          bytes(
              source.read(
                  postingsStart + Math.toIntExact(entry.offset()),
                  Math.toIntExact(entry.length())));
      int[] docIds = new int[postingCount];
      int[] termFrequencies = new int[postingCount];
      try (DataInputStream in =
          new DataInputStream(new InflaterInputStream(new ByteArrayInputStream(compressed)))) {
        int previousDoc = -1;
        for (int posting = 0; posting < postingCount; posting++) {
          int delta = readVarint(in);
          int docId = previousDoc < 0 ? delta : Math.addExact(previousDoc, delta);
          int frequency = readVarint(in);
          check(
              docId >= 0 && frequency > 0 && (previousDoc < 0 || docId > previousDoc),
              "Invalid BM25 posting");
          docIds[posting] = docId;
          termFrequencies[posting] = frequency;
          previousDoc = docId;
        }

        check(in.read() < 0, "Unexpected trailing BM25 postings");
      }

      return new PostingList(docIds, termFrequencies);
    }

    private List<String> files() throws IOException {
      if (files == null) {
        if (fileCount == 0) {
          files = List.of();
          fileStart = new int[0];
          fileRow = new long[0];
        } else {
          List<String> names = Lists.newArrayListWithCapacity(fileCount);
          int[] starts = new int[fileCount];
          long[] rows = new long[fileCount];
          try (DataInputStream in =
              new DataInputStream(
                  new ByteArrayInputStream(bytes(source.read(HEADER_SIZE, filesLength))))) {
            for (int file = 0; file < fileCount; file++) {
              names.add(readString(in));
              starts[file] = readVarint(in);
              rows[file] = readLongVarint(in);
              check(
                  starts[file] >= 0 && (file == 0 || starts[file] > starts[file - 1]),
                  "Invalid BM25 file start");
            }

            check(in.read() < 0, "Unexpected trailing BM25 file table");
          }

          files = names;
          fileStart = starts;
          fileRow = rows;
        }
      }

      return files;
    }

    private Map<String, DirectoryEntry> directory(Set<String> terms) throws IOException {
      Map<String, DirectoryEntry> result = Maps.newHashMap();
      if (terms == null) {
        // the range offsets of the entries are deltas within their bucket, so a bucket is read on
        // its own even when every term is wanted
        for (int bucketIndex = 0; bucketIndex < BUCKET_COUNT; bucketIndex++) {
          readDirectory(result, null, bucketOffsets[bucketIndex], bucketOffsets[bucketIndex + 1]);
        }

        return result;
      }

      Set<Integer> buckets = Sets.newHashSet();
      for (String term : terms) {
        buckets.add(bucket(term));
      }

      for (int bucketIndex = 0; bucketIndex < BUCKET_COUNT; bucketIndex++) {
        if (buckets.contains(bucketIndex)) {
          readDirectory(result, terms, bucketOffsets[bucketIndex], bucketOffsets[bucketIndex + 1]);
        }
      }

      return result;
    }

    private void readDirectory(
        Map<String, DirectoryEntry> result, Set<String> terms, int start, int end)
        throws IOException {
      if (start == end) {
        return;
      }

      long previousRangeOffset = 0L;
      byte[] previous = new byte[0];
      try (DataInputStream in =
          new DataInputStream(new ByteArrayInputStream(bytes(source.read(start, end - start))))) {
        while (in.available() > 0) {
          int shared = readVarint(in);
          check(shared <= previous.length, "Invalid BM25 dictionary prefix");
          int suffix = readVarint(in);
          byte[] termBytes = new byte[shared + suffix];
          System.arraycopy(previous, 0, termBytes, 0, shared);
          in.readFully(termBytes, shared, suffix);
          String term = new String(termBytes, StandardCharsets.UTF_8);
          long frequency = readLongVarint(in);
          long rangeOffset = Math.addExact(previousRangeOffset, readVarint(in));
          long rangeLength = readVarint(in);
          check(
              frequency > 0 && frequency <= documents && rangeOffset >= 0 && rangeLength >= 0,
              "Invalid BM25 dictionary entry");
          check(
              rangeOffset + rangeLength <= size - (long) postingsStart,
              "Invalid BM25 posting range: offset="
                  + rangeOffset
                  + " length="
                  + rangeLength
                  + " postings="
                  + (size - (long) postingsStart));
          if (terms == null || terms.contains(term)) {
            result.put(term, new DirectoryEntry(frequency, rangeOffset, rangeLength));
          }

          previous = termBytes;
          previousRangeOffset = rangeOffset;
        }
      }
    }
  }

  private record Documents(
      byte[] lengths,
      int[] fileStart,
      long[] fileRow,
      List<long[]> overflow,
      List<long[]> exceptions) {}

  private record Dictionary(byte[] postings, ByteArrayOutputStream[] buckets) {}

  private static final class PostingList {
    private final int[] docIds;
    private final int[] termFrequencies;

    private PostingList(int[] docIds, int[] termFrequencies) {
      this.docIds = docIds;
      this.termFrequencies = termFrequencies;
    }
  }

  private static byte[] bytes(ByteBuffer buffer) {
    ByteBuffer copy = buffer.duplicate();
    byte[] bytes = new byte[copy.remaining()];
    copy.get(bytes);
    return bytes;
  }

  private static void writeVarint(OutputStream out, int value) throws IOException {
    int remaining = value;
    while ((remaining & ~0x7F) != 0) {
      out.write((remaining & 0x7F) | 0x80);
      remaining >>>= 7;
    }

    out.write(remaining);
  }

  private static void writeLongVarint(OutputStream out, long value) throws IOException {
    long remaining = value;
    while ((remaining & ~0x7FL) != 0) {
      out.write((int) ((remaining & 0x7F) | 0x80));
      remaining >>>= 7;
    }

    out.write((int) remaining);
  }

  private static int readVarint(DataInputStream in) throws IOException {
    int value = 0;
    int shift = 0;
    while (true) {
      int next = in.readUnsignedByte();
      value |= (next & 0x7F) << shift;
      if ((next & 0x80) == 0) {
        return value;
      }

      shift += 7;
      if (shift > 28 && (shift != 28 || (next & 0xF0) != 0)) {
        throw new IOException("Invalid BM25 varint");
      }
    }
  }

  private static long readLongVarint(DataInputStream in) throws IOException {
    long value = 0;
    int shift = 0;
    while (true) {
      int next = in.readUnsignedByte();
      value |= (long) (next & 0x7F) << shift;
      if ((next & 0x80) == 0) {
        return value;
      }

      shift += 7;
      if (shift > 63) {
        throw new IOException("Invalid BM25 long varint");
      }
    }
  }

  private static void writeString(OutputStream out, String value) throws IOException {
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    writeVarint(out, bytes.length);
    out.write(bytes);
  }

  private static String readString(DataInputStream in) throws IOException {
    int length = readVarint(in);
    byte[] bytes = new byte[length];
    in.readFully(bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  private static void writeIntBE(ByteArrayOutputStream out, int value) {
    out.write((value >>> 24) & 0xFF);
    out.write((value >>> 16) & 0xFF);
    out.write((value >>> 8) & 0xFF);
    out.write(value & 0xFF);
  }

  private static void writeLongBE(ByteArrayOutputStream out, long value) {
    out.write((int) (value >>> 56) & 0xFF);
    out.write((int) (value >>> 48) & 0xFF);
    out.write((int) (value >>> 40) & 0xFF);
    out.write((int) (value >>> 32) & 0xFF);
    out.write((int) (value >>> 24) & 0xFF);
    out.write((int) (value >>> 16) & 0xFF);
    out.write((int) (value >>> 8) & 0xFF);
    out.write((int) value & 0xFF);
  }

  private record DirectoryEntry(long frequency, long offset, long length) {}
}
