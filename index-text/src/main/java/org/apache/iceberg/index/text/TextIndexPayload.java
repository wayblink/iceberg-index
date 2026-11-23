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
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
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

/** Sorted bucket directories followed by independently compressed term postings. */
final class TextIndexPayload {
  static final int FORMAT_VERSION = 1;
  static final int MAGIC = 0x424D3100 | FORMAT_VERSION;
  private static final int BUCKET_COUNT = 64;
  private static final int HEADER_SIZE =
      Integer.BYTES + 2 * Long.BYTES + (BUCKET_COUNT + 1) * Integer.BYTES;

  private TextIndexPayload() {}

  static ByteBuffer encode(CorpusCounts counts, FileIndex index) throws IOException {
    ByteArrayOutputStream postings = new ByteArrayOutputStream();
    ByteArrayOutputStream[] dictionaries = new ByteArrayOutputStream[BUCKET_COUNT];
    List<String> terms = new ArrayList<>(counts.documentFrequency.keySet());
    terms.sort(String::compareTo);
    for (String term : terms) {
      int bucket = bucket(term);
      if (dictionaries[bucket] == null) {
        dictionaries[bucket] = new ByteArrayOutputStream();
      }

      int offset = postings.size();
      if (index != null) {
        // The zlib stream repeats paths cheaply and keeps the doc reference and length in the
        // term range. Sparse queries do not have to load a shard-wide document table.
        ByteArrayOutputStream termBytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(new DeflaterOutputStream(termBytes))) {
          for (Posting posting : index.postingsByTerm.get(term)) {
            Document document = index.documents.get(posting.docId);
            out.writeUTF(document.dataFile);
            out.writeLong(document.rowPosition);
            out.writeInt(document.length);
            out.writeInt(posting.termFrequency);
          }
        }
        termBytes.writeTo(postings);
      }

      DataOutputStream dictionary = new DataOutputStream(dictionaries[bucket]);
      writeString(dictionary, term);
      dictionary.writeLong(counts.documentFrequency.get(term));
      dictionary.writeInt(offset);
      dictionary.writeInt(postings.size() - offset);
    }

    int dictionaryEnd = HEADER_SIZE;
    for (ByteArrayOutputStream dictionary : dictionaries) {
      if (dictionary != null) {
        dictionaryEnd = Math.addExact(dictionaryEnd, dictionary.size());
      }
    }

    ByteArrayOutputStream bytes =
        new ByteArrayOutputStream(Math.addExact(dictionaryEnd, postings.size()));
    try (DataOutputStream out = new DataOutputStream(bytes)) {
      out.writeInt(MAGIC);
      out.writeLong(counts.documents);
      out.writeLong(counts.tokens);
      int position = HEADER_SIZE;
      for (ByteArrayOutputStream dictionary : dictionaries) {
        out.writeInt(position);
        if (dictionary != null) {
          position += dictionary.size();
        }
      }
      out.writeInt(position);
      for (ByteArrayOutputStream dictionary : dictionaries) {
        if (dictionary != null) {
          dictionary.writeTo(out);
        }
      }
      postings.writeTo(out);
    }
    return ByteBuffer.wrap(bytes.toByteArray());
  }

  static Reader open(TextIndexSource.Source source, int size) throws IOException {
    check(size >= Integer.BYTES, "Truncated BM25 v1 signature");
    ByteBuffer header = source.read(0, Math.min(size, HEADER_SIZE));
    if (header.getInt() != MAGIC) {
      throw new IllegalArgumentException("Not a text index payload");
    }

    check(size >= HEADER_SIZE, "Truncated BM25 v1 header");
    long documents = header.getLong();
    long tokens = header.getLong();
    check(documents >= 0 && tokens >= 0, "Invalid BM25 v1 corpus counts");
    int[] offsets = new int[BUCKET_COUNT + 1];
    int previous = HEADER_SIZE;
    for (int index = 0; index < offsets.length; index++) {
      int offset = header.getInt();
      check(offset >= previous && offset <= size, "Invalid BM25 v1 dictionary offset");
      offsets[index] = offset;
      previous = offset;
    }
    check(offsets[0] == HEADER_SIZE, "Invalid BM25 v1 dictionary start");
    return new Reader(source, size, documents, tokens, offsets);
  }

  private static int bucket(String term) {
    return term.hashCode() & (BUCKET_COUNT - 1);
  }

  private static void writeString(DataOutputStream out, String value) throws IOException {
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    out.writeInt(bytes.length);
    out.write(bytes);
  }

  private static String readString(DataInputStream in) throws IOException {
    int length = in.readInt();
    if (length < 0) {
      throw new IOException("Invalid BM25 v1 string length: " + length);
    }

    byte[] bytes = new byte[length];
    in.readFully(bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  private static void check(boolean condition, String message) throws IOException {
    if (!condition) {
      throw new IOException(message);
    }
  }

  static final class Reader implements TextIndexShard {
    private final TextIndexSource.Source source;
    private final int size;
    private final long documents;
    private final long tokens;
    private final int[] offsets;

    private Reader(TextIndexSource.Source source, int size, long documents, long tokens, int[] offsets) {
      this.source = source;
      this.size = size;
      this.documents = documents;
      this.tokens = tokens;
      this.offsets = offsets;
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
      if (statisticsOnly) {
        CorpusCounts counts = counts(terms);
        return new FileIndex(localDocuments, tokens, counts.documentFrequency, Map.of(), List.of());
      }

      Map<String, Long> frequencies = new HashMap<>();
      Map<String, List<Posting>> byTerm = new HashMap<>();
      List<Document> matches = new ArrayList<>();
      for (Map.Entry<String, DirectoryEntry> term : directory(terms).entrySet()) {
        DirectoryEntry entry = term.getValue();
        frequencies.put(term.getKey(), entry.frequency());
        int postingCount = Math.toIntExact(entry.frequency());
        List<Posting> postings = new ArrayList<>();
        ByteBuffer compressed = source.read(entry.offset(), entry.length());
        try (DataInputStream in =
            new DataInputStream(
                new InflaterInputStream(new ByteArrayInputStream(bytes(compressed))))) {
          for (int row = 0; row < postingCount; row++) {
            String file = in.readUTF();
            long position = in.readLong();
            int length = in.readInt();
            int frequency = in.readInt();
            check(position >= 0 && frequency > 0 && frequency <= length, "Invalid BM25 v1 posting");
            postings.add(new Posting(matches.size(), frequency));
            matches.add(new Document(file, position, length));
          }
          check(in.read() < 0, "Unexpected trailing BM25 v1 postings");
        }
        byTerm.put(term.getKey(), postings);
      }
      return new FileIndex(localDocuments, tokens, frequencies, byTerm, matches);
    }

    private Map<String, DirectoryEntry> directory(Set<String> terms) throws IOException {
      Map<String, DirectoryEntry> result = new HashMap<>();
      if (terms == null) {
        readDirectory(result, null, offsets[0], offsets[BUCKET_COUNT]);
        return result;
      }

      boolean[] needed = new boolean[BUCKET_COUNT];
      terms.forEach(term -> needed[bucket(term)] = true);
      for (int bucketIndex = 0; bucketIndex < BUCKET_COUNT; bucketIndex++) {
        if (needed[bucketIndex]) {
          readDirectory(result, terms, offsets[bucketIndex], offsets[bucketIndex + 1]);
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

      int postingsStart = offsets[BUCKET_COUNT];
      try (DataInputStream in =
          new DataInputStream(new ByteArrayInputStream(bytes(source.read(start, end - start))))) {
        while (in.available() > 0) {
          String term = readString(in);
          long frequency = in.readLong();
          int offset = in.readInt();
          int postingLength = in.readInt();
          check(
              frequency > 0
                  && frequency <= documents
                  && offset >= 0
                  && postingLength >= 0
                  && (long) postingsStart + offset + postingLength <= size,
              "Invalid BM25 v1 dictionary entry");
          if (terms == null || terms.contains(term)) {
            result.put(term, new DirectoryEntry(frequency, postingsStart + offset, postingLength));
          }
        }
      }
    }
  }

  private static byte[] bytes(ByteBuffer buffer) {
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    return bytes;
  }

  private record DirectoryEntry(long frequency, int offset, int length) {}
}
