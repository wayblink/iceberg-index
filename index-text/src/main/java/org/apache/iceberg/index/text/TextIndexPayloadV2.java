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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;
import org.apache.iceberg.index.text.TextIndexAdapter.CorpusCounts;
import org.apache.iceberg.index.text.TextIndexAdapter.Document;
import org.apache.iceberg.index.text.TextIndexAdapter.FileIndex;
import org.apache.iceberg.index.text.TextIndexAdapter.Posting;
import org.apache.iceberg.index.text.TextIndexPayload.Source;
import org.apache.iceberg.io.SeekableInputStream;

/**
 * BM25 v2 payload: one index file's postings and statistics, or the snapshot statistics dictionary.
 *
 * <p>The v1 payload writes the data file path, the absolute row position and the document length in
 * every posting. All three are properties of the document, not of the posting, so a document with
 * {@code u} distinct terms pays for them {@code u} times; at 137 terms per document that is most of
 * the payload.
 *
 * <p>This layout keeps a per-document value once:
 *
 * <ul>
 *   <li>data file paths are written once, as a file table, and a document carries a file id;
 *   <li>row position and document length live in a document table, split into independently
 *       compressed chunks, and a posting carries nothing for them;
 *   <li>a posting is {@code (docId delta, tf)}, two varints;
 *   <li>the term dictionary is front coded, and its numeric fields are varints, with the posting
 *       range offset a delta from the previous term of the same bucket.
 * </ul>
 *
 * <p>Everything a query needs stays addressable by range: the header names the bucket offsets, a
 * bucket names the posting ranges of its terms, and the document table directory names the chunks.
 * A query reads the buckets of its terms, their posting ranges, and only the document table chunks
 * that hold a matched document, so a sparse query stays sparse and a common term costs one pass
 * over the chunks it touches instead of a path per posting.
 *
 * <p>The layout has no ordering requirement beyond postings of one term being in ascending document
 * order, which the builder produces by construction.
 *
 * <p>Layout, all integers big endian:
 *
 * <pre>
 * header (308 bytes)
 *   int    magic                     {@link #MAGIC}
 *   long   documents
 *   long   tokens                    total token count, for the average document length
 *   int    fileCount
 *   int    chunkCount                document table chunks
 *   int    chunkDocs                 documents per chunk, the last chunk may be shorter
 *   int    filesLength               file table bytes
 *   int    docDirectoryLength        chunkCount * 4
 *   int    dictionaryStart           absolute offset
 *   int    postingsStart             absolute offset
 *   int[65] bucketOffsets            absolute offsets, bucketOffsets[64] == postingsStart
 * file table                          fileCount x (varint length, UTF-8 bytes)
 * document table directory            chunkCount x int offset, relative to the chunk section
 * document table chunks               each chunk is a zlib stream of
 *                                     varint fileId, varint rowPosition, varint length
 * dictionary                          64 buckets; entries of one bucket are sorted and are
 *                                     (byte sharedPrefixLength, varint suffixLength, suffix bytes,
 *                                      varint documentFrequency, varint rangeOffsetDelta,
 *                                      varint rangeLength)
 * postings                            per term a zlib stream of (varint docId delta, varint tf);
 *                                     the first posting writes the document id absolute
 * </pre>
 *
 * <p>The snapshot statistics file uses the same layout with no data files, no document table and no
 * postings, so a reader needs one format for both.
 */
final class TextIndexPayloadV2 {
  /** Format version of this layout; it is part of the magic, so a reader dispatches on one int. */
  static final int FORMAT_VERSION = 1;

  /** First bytes of a v2 payload, the differentiator against the v1 payload. */
  static final int MAGIC = 0x424D3200 | FORMAT_VERSION;

  private static final int BUCKET_COUNT = 64;

  /** Documents per independently compressed document table chunk. */
  static final int CHUNK_DOCS = 4096;

  static final int HEADER_SIZE = 4 + 8 + 8 + 4 + 4 + 4 + 4 + 4 + 4 + 4 + (BUCKET_COUNT + 1) * 4;

  private TextIndexPayloadV2() {}

  /**
   * Encodes the payload of one index file, or the snapshot statistics when {@code index} is null.
   *
   * @param counts the corpus counts of the payload: documents, total length and document frequency
   * @param index the local postings and documents, or null to write a statistics dictionary
   */
  static ByteBuffer encode(CorpusCounts counts, FileIndex index) throws IOException {
    List<String> files = index == null ? List.of() : files(index);
    Map<String, Integer> fileIds = new HashMap<>();
    for (int id = 0; id < files.size(); id++) {
      fileIds.put(files.get(id), id);
    }

    ByteArrayOutputStream fileSection = new ByteArrayOutputStream();
    for (String file : files) {
      writeString(fileSection, file);
    }

    ByteArrayOutputStream docSection = new ByteArrayOutputStream();
    ByteArrayOutputStream docDirectory = new ByteArrayOutputStream();
    int chunkCount = 0;
    if (index != null && !index.documents.isEmpty()) {
      int documents = index.documents.size();
      chunkCount = (documents + CHUNK_DOCS - 1) / CHUNK_DOCS;
      for (int chunk = 0; chunk < chunkCount; chunk++) {
        ByteArrayOutputStream compressed = new ByteArrayOutputStream();
        try (DeflaterOutputStream out = new DeflaterOutputStream(compressed)) {
          int start = chunk * CHUNK_DOCS;
          int end = Math.min(start + CHUNK_DOCS, documents);
          for (int doc = start; doc < end; doc++) {
            Document document = index.documents.get(doc);
            writeVarint(out, fileIds.get(document.dataFile));
            writeLongVarint(out, document.rowPosition);
            writeVarint(out, document.length);
          }
        }

        writeIntBE(docDirectory, docSection.size());
        compressed.writeTo(docSection);
      }
    }

    List<String> terms = new ArrayList<>(counts.documentFrequency.keySet());
    terms.sort(String::compareTo);

    ByteArrayOutputStream postings = new ByteArrayOutputStream();
    Map<String, long[]> ranges = new HashMap<>();
    if (index != null) {
      for (String term : terms) {
        int offset = postings.size();
        encodePostings(postings, index, term);
        ranges.put(term, new long[] {offset, postings.size() - offset});
      }
    }

    ByteArrayOutputStream[] dictionaries = new ByteArrayOutputStream[BUCKET_COUNT];
    byte[][] previousTerms = new byte[BUCKET_COUNT][];
    long[] previousRangeOffset = new long[BUCKET_COUNT];
    for (String term : terms) {
      int bucket = bucket(term);
      if (dictionaries[bucket] == null) {
        dictionaries[bucket] = new ByteArrayOutputStream();
      }

      long[] range = ranges.getOrDefault(term, new long[] {0L, 0L});
      long rangeOffset = range[0];
      byte[] termBytes = term.getBytes(StandardCharsets.UTF_8);
      int shared = sharedPrefix(previousTerms[bucket], termBytes);
      ByteArrayOutputStream dictionary = dictionaries[bucket];
      dictionary.write(shared);
      writeVarint(dictionary, termBytes.length - shared);
      dictionary.write(termBytes, shared, termBytes.length - shared);
      writeLongVarint(dictionary, counts.documentFrequency.get(term));
      writeVarint(dictionary, Math.toIntExact(rangeOffset - previousRangeOffset[bucket]));
      writeVarint(dictionary, Math.toIntExact(range[1]));
      previousRangeOffset[bucket] = rangeOffset;
      previousTerms[bucket] = termBytes;
    }

    int dictionaryStart =
        HEADER_SIZE + fileSection.size() + docDirectory.size() + docSection.size();
    int postingsStart = dictionaryStart;
    for (ByteArrayOutputStream dictionary : dictionaries) {
      if (dictionary != null) {
        postingsStart = Math.addExact(postingsStart, dictionary.size());
      }
    }

    ByteArrayOutputStream bytes =
        new ByteArrayOutputStream(Math.addExact(postingsStart, postings.size()));
    writeIntBE(bytes, MAGIC);
    writeLongBE(bytes, counts.documents);
    writeLongBE(bytes, counts.tokens);
    writeIntBE(bytes, files.size());
    writeIntBE(bytes, chunkCount);
    writeIntBE(bytes, CHUNK_DOCS);
    writeIntBE(bytes, fileSection.size());
    writeIntBE(bytes, docDirectory.size());
    writeIntBE(bytes, dictionaryStart);
    writeIntBE(bytes, postingsStart);
    int position = dictionaryStart;
    for (ByteArrayOutputStream dictionary : dictionaries) {
      writeIntBE(bytes, position);
      if (dictionary != null) {
        position = Math.addExact(position, dictionary.size());
      }
    }

    writeIntBE(bytes, position);
    fileSection.writeTo(bytes);
    docDirectory.writeTo(bytes);
    docSection.writeTo(bytes);
    for (ByteArrayOutputStream dictionary : dictionaries) {
      if (dictionary != null) {
        dictionary.writeTo(bytes);
      }
    }

    postings.writeTo(bytes);
    return ByteBuffer.wrap(bytes.toByteArray());
  }

  /** The data files of an index file, in the order their documents appear. */
  private static List<String> files(FileIndex index) {
    Map<String, Boolean> seen = new LinkedHashMap<>();
    for (Document document : index.documents) {
      seen.putIfAbsent(document.dataFile, Boolean.TRUE);
    }

    return new ArrayList<>(seen.keySet());
  }

  private static void encodePostings(ByteArrayOutputStream bytes, FileIndex index, String term)
      throws IOException {
    List<Posting> postings = index.postingsByTerm.get(term);
    if (postings == null) {
      throw new IllegalArgumentException("Missing postings for term " + term);
    }

    ByteArrayOutputStream records = new ByteArrayOutputStream();
    int previousDoc = -1;
    try (DeflaterOutputStream out = new DeflaterOutputStream(records)) {
      for (Posting posting : postings) {
        check(posting.docId > previousDoc, "BM25 v2 postings are not in document order");
        writeVarint(out, previousDoc < 0 ? posting.docId : posting.docId - previousDoc);
        writeVarint(out, posting.termFrequency);
        previousDoc = posting.docId;
      }
    }

    records.writeTo(bytes);
  }

  static Source source(ByteBuffer bytes) {
    return TextIndexPayload.source(bytes);
  }

  static Source source(SeekableInputStream stream, long base) {
    return TextIndexPayload.source(stream, base);
  }

  /** Whether a payload starts with this format, without opening it. */
  static boolean matches(ByteBuffer payload) {
    return payload.remaining() >= Integer.BYTES && payload.getInt(payload.position()) == MAGIC;
  }

  static Reader open(Source source, int size) throws IOException {
    check(size >= Integer.BYTES, "Truncated BM25 v2 signature");
    ByteBuffer header = source.read(0, Math.min(size, HEADER_SIZE));
    if (header.getInt() != MAGIC) {
      throw new IllegalArgumentException("Not a BM25 v2 payload");
    }

    check(size >= HEADER_SIZE, "Truncated BM25 v2 header");
    long documents = header.getLong();
    long tokens = header.getLong();
    int fileCount = header.getInt();
    int chunkCount = header.getInt();
    int chunkDocs = header.getInt();
    int filesLength = header.getInt();
    int docDirectoryLength = header.getInt();
    int dictionaryStart = header.getInt();
    int postingsStart = header.getInt();
    check(
        documents >= 0
            && tokens >= 0
            && fileCount >= 0
            && chunkCount >= 0
            && chunkDocs > 0
            && filesLength >= 0
            && docDirectoryLength >= 0,
        "Invalid BM25 v2 header");
    int[] bucketOffsets = new int[BUCKET_COUNT + 1];
    int previous = HEADER_SIZE;
    for (int index = 0; index < bucketOffsets.length; index++) {
      int offset = header.getInt();
      check(offset >= previous && offset <= size, "Invalid BM25 v2 bucket offset");
      bucketOffsets[index] = offset;
      previous = offset;
    }

    check(bucketOffsets[0] == dictionaryStart, "Invalid BM25 v2 dictionary start");
    check(bucketOffsets[BUCKET_COUNT] == postingsStart, "Invalid BM25 v2 postings start");
    check(postingsStart >= dictionaryStart, "Invalid BM25 v2 section order");
    long chunksStart = (long) HEADER_SIZE + filesLength + docDirectoryLength;
    check(chunksStart <= dictionaryStart, "Invalid BM25 v2 document table");
    check(
        chunkCount == 0 || (long) chunkCount * Integer.BYTES == docDirectoryLength,
        "Invalid BM25 v2 document table directory");
    if (documents == 0) {
      check(chunkCount == 0, "BM25 v2 empty payload has document chunks");
    } else if (chunkCount > 0) {
      check((long) chunkCount * chunkDocs >= documents, "BM25 v2 document chunks cover too few");
      check(
          (long) (chunkCount - 1) * chunkDocs < documents,
          "BM25 v2 has a trailing empty document chunk");
    }

    return new Reader(
        source,
        size,
        documents,
        tokens,
        fileCount,
        chunkCount,
        chunkDocs,
        filesLength,
        docDirectoryLength,
        dictionaryStart,
        postingsStart,
        bucketOffsets);
  }

  /** The bucket of a term; the same function as v1, so a term keeps its bucket across formats. */
  private static int bucket(String term) {
    return term.hashCode() & (BUCKET_COUNT - 1);
  }

  private static void check(boolean condition, String message) throws IOException {
    if (!condition) {
      throw new IOException(message);
    }
  }

  /** Shared prefix bytes of two terms of one sorted bucket. */
  private static int sharedPrefix(byte[] previous, byte[] term) {
    if (previous == null) {
      return 0;
    }

    int shared = 0;
    int limit = Math.min(previous.length, term.length);
    while (shared < limit && previous[shared] == term[shared]) {
      shared += 1;
    }

    return shared;
  }

  static final class Reader implements TextIndexShard {
    private final Source source;
    private final int size;
    private final long documents;
    private final long tokens;
    private final int fileCount;
    private final int chunkCount;
    private final int chunkDocs;
    private final int filesLength;
    private final int docDirectoryLength;
    private final int dictionaryStart;
    private final int postingsStart;
    private final int[] bucketOffsets;

    private List<String> files;
    private int[] chunkOffsets;

    private Reader(
        Source source,
        int size,
        long documents,
        long tokens,
        int fileCount,
        int chunkCount,
        int chunkDocs,
        int filesLength,
        int docDirectoryLength,
        int dictionaryStart,
        int postingsStart,
        int[] bucketOffsets) {
      this.source = source;
      this.size = size;
      this.documents = documents;
      this.tokens = tokens;
      this.fileCount = fileCount;
      this.chunkCount = chunkCount;
      this.chunkDocs = chunkDocs;
      this.filesLength = filesLength;
      this.docDirectoryLength = docDirectoryLength;
      this.dictionaryStart = dictionaryStart;
      this.postingsStart = postingsStart;
      this.bucketOffsets = bucketOffsets;
    }

    @Override
    public CorpusCounts counts(Set<String> terms) throws IOException {
      Map<String, Long> frequencies = new HashMap<>();
      directory(terms).forEach((term, entry) -> frequencies.put(term, entry.frequency()));
      return new CorpusCounts(documents, tokens, frequencies);
    }

    @Override
    public FileIndex shard(Set<String> terms, boolean statisticsOnly) throws IOException {
      check(documents <= Integer.MAX_VALUE, "Invalid BM25 shard document count");
      int localDocuments = Math.toIntExact(documents);
      Map<String, DirectoryEntry> directory = directory(terms);
      if (statisticsOnly) {
        Map<String, Long> frequencies = new HashMap<>();
        directory.forEach((term, entry) -> frequencies.put(term, entry.frequency()));
        return new FileIndex(localDocuments, tokens, frequencies, Map.of(), List.of());
      }

      check(chunkCount > 0 || localDocuments == 0, "BM25 v2 shard has no document table");

      // decode every requested posting list, and remember which documents are referenced
      boolean[] needed = new boolean[localDocuments];
      boolean[] chunkNeeded = new boolean[chunkCount];
      Map<String, PostingList> lists = new LinkedHashMap<>();
      for (Map.Entry<String, DirectoryEntry> term : directory.entrySet()) {
        PostingList list = postings(term.getValue());
        lists.put(term.getKey(), list);
        for (int docId : list.docIds) {
          check(docId >= 0 && docId < localDocuments, "Invalid BM25 v2 posting");
          if (!needed[docId]) {
            needed[docId] = true;
            chunkNeeded[docId / chunkDocs] = true;
          }
        }
      }

      // read only the document table chunks holding a referenced document
      int[] slotByDoc = new int[localDocuments];
      Arrays.fill(slotByDoc, -1);
      int[] matchFile = new int[0];
      long[] matchRow = new long[0];
      int[] matchLength = new int[0];
      int matches = 0;
      for (int chunk = 0; chunk < chunkCount; chunk++) {
        if (!chunkNeeded[chunk]) {
          continue;
        }

        int start = chunk * chunkDocs;
        int count = Math.min(chunkDocs, localDocuments - start);
        if (matches + count > matchFile.length) {
          int capacity = Math.max(matches + count, Math.max(16, matchFile.length * 2));
          matchFile = Arrays.copyOf(matchFile, capacity);
          matchRow = Arrays.copyOf(matchRow, capacity);
          matchLength = Arrays.copyOf(matchLength, capacity);
        }

        try (DataInputStream in =
            new DataInputStream(
                new InflaterInputStream(new ByteArrayInputStream(chunkBytes(chunk))))) {
          for (int index = 0; index < count; index++) {
            int fileId = readVarint(in);
            long row = readLongVarint(in);
            int length = readVarint(in);
            int docId = start + index;
            check(fileId >= 0 && row >= 0 && length >= 0, "Invalid BM25 v2 document table entry");
            if (needed[docId]) {
              slotByDoc[docId] = matches;
              matchFile[matches] = fileId;
              matchRow[matches] = row;
              matchLength[matches] = length;
              matches += 1;
            }
          }

          check(in.read() < 0, "Unexpected trailing BM25 v2 document table");
        }
      }

      List<String> files = files();
      Map<String, Long> frequencies = new HashMap<>();
      Map<String, List<Posting>> byTerm = new HashMap<>();
      List<Document> documentsList = new ArrayList<>();
      for (Map.Entry<String, PostingList> term : lists.entrySet()) {
        PostingList list = term.getValue();
        frequencies.put(term.getKey(), (long) list.docIds.length);
        List<Posting> postings = new ArrayList<>(list.docIds.length);
        for (int posting = 0; posting < list.docIds.length; posting++) {
          int slot = slotByDoc[list.docIds[posting]];
          check(slot >= 0, "Missing BM25 v2 document table entry");
          check(fileIdsValid(slot, matchFile, files), "Invalid BM25 v2 document file id");
          check(
              list.termFrequencies[posting] <= matchLength[slot],
              "Invalid BM25 v2 posting term frequency");
          documentsList.add(
              new Document(files.get(matchFile[slot]), matchRow[slot], matchLength[slot]));
          postings.add(new Posting(documentsList.size() - 1, list.termFrequencies[posting]));
        }

        byTerm.put(term.getKey(), postings);
      }

      return new FileIndex(localDocuments, tokens, frequencies, byTerm, documentsList);
    }

    private static boolean fileIdsValid(int slot, int[] matchFile, List<String> files) {
      return matchFile[slot] >= 0 && matchFile[slot] < files.size();
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
              "Invalid BM25 v2 posting");
          docIds[posting] = docId;
          termFrequencies[posting] = frequency;
          previousDoc = docId;
        }

        check(in.read() < 0, "Unexpected trailing BM25 v2 postings");
      }

      return new PostingList(docIds, termFrequencies);
    }

    /** The compressed bytes of one document table chunk. */
    private byte[] chunkBytes(int chunk) throws IOException {
      int[] offsets = chunkOffsets();
      int start = offsets[chunk];
      int end = chunk + 1 < offsets.length ? offsets[chunk + 1] : chunksLength();
      return bytes(source.read(chunksStart() + start, end - start));
    }

    private List<String> files() throws IOException {
      if (files == null) {
        if (fileCount == 0) {
          files = List.of();
        } else {
          List<String> names = new ArrayList<>(fileCount);
          try (DataInputStream in =
              new DataInputStream(
                  new ByteArrayInputStream(bytes(source.read(HEADER_SIZE, filesLength))))) {
            for (int file = 0; file < fileCount; file++) {
              names.add(readString(in));
            }

            check(in.read() < 0, "Unexpected trailing BM25 v2 file table");
          }

          files = names;
        }
      }

      return files;
    }

    private int[] chunkOffsets() throws IOException {
      if (chunkOffsets == null) {
        ByteBuffer directory = source.read(HEADER_SIZE + filesLength, docDirectoryLength);
        int[] offsets = new int[chunkCount];
        for (int chunk = 0; chunk < chunkCount; chunk++) {
          offsets[chunk] = directory.getInt();
          check(
              offsets[chunk] >= 0 && (chunk == 0 || offsets[chunk] >= offsets[chunk - 1]),
              "Invalid BM25 v2 document table directory");
        }

        chunkOffsets = offsets;
      }

      return chunkOffsets;
    }

    private int chunksStart() {
      return HEADER_SIZE + filesLength + docDirectoryLength;
    }

    /** The document table section ends where the dictionary starts. */
    private int chunksLength() {
      return dictionaryStart - chunksStart();
    }

    private Map<String, DirectoryEntry> directory(Set<String> terms) throws IOException {
      Map<String, DirectoryEntry> result = new HashMap<>();
      if (terms == null) {
        // the range offsets of the entries are deltas within their bucket, so a bucket is read on
        // its own even when every term is wanted
        for (int bucketIndex = 0; bucketIndex < BUCKET_COUNT; bucketIndex++) {
          readDirectory(result, null, bucketOffsets[bucketIndex], bucketOffsets[bucketIndex + 1]);
        }

        return result;
      }

      Set<Integer> buckets = new HashSet<>();
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
          int shared = in.readUnsignedByte();
          check(shared <= previous.length, "Invalid BM25 v2 dictionary prefix");
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
              "Invalid BM25 v2 dictionary entry");
          check(
              rangeOffset + rangeLength <= size - (long) postingsStart,
              "Invalid BM25 v2 posting range: offset="
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

  private static final class PostingList {
    private final int[] docIds;
    private final int[] termFrequencies;

    private PostingList(int[] docIds, int[] termFrequencies) {
      this.docIds = docIds;
      this.termFrequencies = termFrequencies;
    }
  }

  private static byte[] bytes(ByteBuffer buffer) {
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
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
      out.write((int) (remaining & 0x7F) | 0x80);
      remaining >>>= 7;
    }

    out.write((int) remaining);
  }

  private static int readVarint(DataInputStream in) throws IOException {
    int result = 0;
    int shift = 0;
    while (true) {
      int next = in.read();
      check(next >= 0, "Truncated BM25 v2 varint");
      result |= (next & 0x7F) << shift;
      if ((next & 0x80) == 0) {
        return result;
      }

      shift += 7;
      check(shift <= 28, "Invalid BM25 v2 varint");
    }
  }

  private static long readLongVarint(DataInputStream in) throws IOException {
    long result = 0L;
    int shift = 0;
    while (true) {
      int next = in.read();
      check(next >= 0, "Truncated BM25 v2 varint");
      result |= (long) (next & 0x7F) << shift;
      if ((next & 0x80) == 0) {
        return result;
      }

      shift += 7;
      check(shift <= 63, "Invalid BM25 v2 varint");
    }
  }

  private static void writeString(OutputStream out, String value) throws IOException {
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    writeVarint(out, bytes.length);
    out.write(bytes, 0, bytes.length);
  }

  private static String readString(DataInputStream in) throws IOException {
    int length = readVarint(in);
    check(length >= 0, "Invalid BM25 v2 string length");
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
    writeIntBE(out, (int) (value >>> 32));
    writeIntBE(out, (int) value);
  }

  private record DirectoryEntry(long frequency, long offset, long length) {}
}
