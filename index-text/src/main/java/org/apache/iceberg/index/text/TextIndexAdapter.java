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
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.index.IndexAdapter;
import org.apache.iceberg.index.IndexDefinition;
import org.apache.iceberg.index.IndexEntry;
import org.apache.iceberg.index.IndexSearchResult;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * BM25 text index adapter.
 *
 * <p>The index is table-wide: one auxiliary index file holds the inverted index of the whole
 * snapshot, because BM25 scoring needs corpus-wide document frequency and average document length,
 * which a per-data-file index could not compute. See {@link IndexAdapter#isTableWide()}.
 *
 * <p>The payload format is a versioned binary blob: the BM25 parameters, corpus statistics
 * (document count and total length), the term dictionary with postings (document id and term
 * frequency), and the mapping from document id to row reference and document length.
 */
public final class TextIndexAdapter implements IndexAdapter {

  static final String ENTRY_BLOB_TYPE = "iceberg-text-entries-v1";
  static final String AUXILIARY_BLOB_TYPE = "iceberg-text-bm25-v1";

  private static final byte FORMAT_VERSION = 1;
  private static final byte[] MAGIC = "ICEBM25".getBytes(StandardCharsets.US_ASCII);

  private final TextIndexDefinition definition;

  public TextIndexAdapter(TextIndexDefinition definition) {
    this.definition = definition;
  }

  @Override
  public IndexDefinition definition() {
    return definition;
  }

  @Override
  public String entryBlobType() {
    return ENTRY_BLOB_TYPE;
  }

  @Override
  public String auxiliaryBlobType() {
    return AUXILIARY_BLOB_TYPE;
  }

  @Override
  public boolean writesAuxiliaryFiles() {
    return true;
  }

  @Override
  public boolean isTableWide() {
    return true;
  }

  /** Analyzes a document into the tokens this index stores. */
  public byte[] encodeDocument(String text) {
    return encodeTokens(definition.analyze(text));
  }

  /** Encodes a query string for {@link #searchAuxiliary(ByteBuffer, ByteBuffer, int)}. */
  public ByteBuffer encodeQuery(String query) {
    return ByteBuffer.wrap(query.getBytes(StandardCharsets.UTF_8));
  }

  /** Decodes a document payload into its tokens. */
  public List<String> decodeDocument(ByteBuffer payload) {
    return decodeTokens(payload);
  }

  @Override
  public ByteBuffer encodeEntries(List<IndexEntry> entries) {
    throw new UnsupportedOperationException(
        "Text indexes write a single table-wide auxiliary file, not entry-bearing files");
  }

  @Override
  public List<IndexEntry> decodeEntries(ByteBuffer payload) {
    throw new UnsupportedOperationException(
        "Text indexes write a single table-wide auxiliary file, not entry-bearing files");
  }

  @Override
  public List<IndexSearchResult> search(ByteBuffer payload, ByteBuffer query, int topK) {
    throw new UnsupportedOperationException(
        "Text indexes write a single table-wide auxiliary file, not entry-bearing files");
  }

  /**
   * Builds the table-wide index payload from the analyzed documents of a snapshot.
   *
   * @param entries one entry per indexed row; the payload is the analyzed document
   */
  @Override
  public ByteBuffer encodeAuxiliaryPayload(List<IndexEntry> entries) {
    // document ids are assigned in entry order; documents keep their length and row reference
    List<Document> documents = Lists.newArrayListWithCapacity(entries.size());
    Map<String, List<Posting>> postingsByTerm = Maps.newHashMap();
    long totalLength = 0;
    for (int docId = 0; docId < entries.size(); docId++) {
      IndexEntry entry = entries.get(docId);
      List<String> tokens = decodeTokens(ByteBuffer.wrap(entry.payload()));
      documents.add(new Document(entry.dataFile(), entry.rowPosition(), tokens.size()));
      totalLength += tokens.size();
      Map<String, Integer> termFrequency = Maps.newHashMap();
      for (String token : tokens) {
        termFrequency.merge(token, 1, Integer::sum);
      }
      for (Map.Entry<String, Integer> term : termFrequency.entrySet()) {
        postingsByTerm
            .computeIfAbsent(term.getKey(), key -> Lists.newArrayList())
            .add(new Posting(docId, term.getValue()));
      }
    }

    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      try (DataOutputStream out = new DataOutputStream(bytes)) {
        out.write(MAGIC);
        out.writeByte(FORMAT_VERSION);
        out.writeDouble(definition.k1());
        out.writeDouble(definition.b());
        out.writeInt(documents.size());
        out.writeLong(totalLength);

        out.writeInt(postingsByTerm.size());
        for (Map.Entry<String, List<Posting>> term : postingsByTerm.entrySet()) {
          byte[] termBytes = term.getKey().getBytes(StandardCharsets.UTF_8);
          out.writeShort(termBytes.length);
          out.write(termBytes);
          out.writeInt(term.getValue().size());
          for (Posting posting : term.getValue()) {
            out.writeInt(posting.docId);
            out.writeInt(posting.termFrequency);
          }
        }

        out.writeInt(documents.size());
        for (Document document : documents) {
          byte[] dataFile = document.dataFile.getBytes(StandardCharsets.UTF_8);
          out.writeShort(dataFile.length);
          out.write(dataFile);
          out.writeLong(document.rowPosition);
          out.writeInt(document.length);
        }
      }

      return ByteBuffer.wrap(bytes.toByteArray());
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to write the text index payload", e);
    }
  }

  @Override
  public List<IndexSearchResult> searchAuxiliary(
      ByteBuffer payload, ByteBuffer query, int topK) {
    List<String> queryTokens = definition.analyze(decodeQuery(query));
    if (queryTokens.isEmpty() || topK <= 0) {
      return Lists.newArrayList();
    }

    IndexPayload index = readPayload(payload);
    Map<String, Integer> queryTermFrequency = Maps.newHashMap();
    for (String token : queryTokens) {
      queryTermFrequency.merge(token, 1, Integer::sum);
    }

    double averageLength = index.totalDocuments == 0 ? 0.0 : (double) index.totalLength / index.totalDocuments;
    Map<Integer, Double> scores = new HashMap<>();
    for (Map.Entry<String, Integer> queryTerm : queryTermFrequency.entrySet()) {
      List<Posting> postings = index.postingsByTerm.get(queryTerm.getKey());
      if (postings == null || postings.isEmpty()) {
        continue;
      }

      double idf =
          Math.log(
              1.0
                  + (index.totalDocuments - postings.size() + 0.5)
                      / (postings.size() + 0.5));
      for (Posting posting : postings) {
        Document document = index.documents.get(posting.docId);
        double frequency = (double) posting.termFrequency;
        double lengthNorm =
            averageLength == 0.0
                ? 1.0
                : definition.k1()
                        * (1.0 - definition.b() + definition.b() * document.length / averageLength)
                    + frequency;
        double score = idf * frequency * (definition.k1() + 1.0) / lengthNorm;
        scores.merge(posting.docId, score, Double::sum);
      }
    }

    List<IndexSearchResult> results = Lists.newArrayListWithCapacity(scores.size());
    for (Map.Entry<Integer, Double> score : scores.entrySet()) {
      Document document = index.documents.get(score.getKey());
      results.add(
          IndexSearchResult.of(document.dataFile, document.rowPosition, score.getValue()));
    }

    results.sort(resultComparator());
    return results.size() > topK
        ? Lists.newArrayList(results.subList(0, topK))
        : results;
  }

  @Override
  public Comparator<IndexSearchResult> resultComparator() {
    return Comparator.comparingDouble(IndexSearchResult::score)
        .reversed()
        .thenComparing(IndexSearchResult::dataFile)
        .thenComparingLong(IndexSearchResult::rowPosition);
  }

  private static String decodeQuery(ByteBuffer query) {
    return StandardCharsets.UTF_8.decode(query.duplicate()).toString();
  }

  private static IndexPayload readPayload(ByteBuffer buffer) {
    try {
      byte[] bytes = new byte[buffer.remaining()];
      buffer.duplicate().get(bytes);
      try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
        byte[] magic = new byte[MAGIC.length];
        in.readFully(magic);
        if (!java.util.Arrays.equals(magic, MAGIC)) {
          throw new IllegalArgumentException("Not a text index payload");
        }
        byte version = in.readByte();
        if (version != FORMAT_VERSION) {
          throw new IllegalArgumentException("Unsupported text index format version: " + version);
        }
        in.readDouble(); // k1, already in the definition
        in.readDouble(); // b, already in the definition
        int totalDocuments = in.readInt();
        long totalLength = in.readLong();

        int termCount = in.readInt();
        Map<String, List<Posting>> postingsByTerm = Maps.newHashMap();
        for (int i = 0; i < termCount; i++) {
          byte[] termBytes = new byte[in.readUnsignedShort()];
          in.readFully(termBytes);
          String term = new String(termBytes, StandardCharsets.UTF_8);
          int postingCount = in.readInt();
          List<Posting> postings = Lists.newArrayListWithCapacity(postingCount);
          for (int p = 0; p < postingCount; p++) {
            postings.add(new Posting(in.readInt(), in.readInt()));
          }
          postingsByTerm.put(term, postings);
        }

        int documentCount = in.readInt();
        List<Document> documents = Lists.newArrayListWithCapacity(documentCount);
        for (int i = 0; i < documentCount; i++) {
          byte[] dataFile = new byte[in.readUnsignedShort()];
          in.readFully(dataFile);
          documents.add(
              new Document(
                  new String(dataFile, StandardCharsets.UTF_8), in.readLong(), in.readInt()));
        }

        return new IndexPayload(totalDocuments, totalLength, postingsByTerm, documents);
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read the text index payload", e);
    }
  }

  private static byte[] encodeTokens(List<String> tokens) {
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      try (DataOutputStream out = new DataOutputStream(bytes)) {
        out.writeInt(tokens.size());
        for (String token : tokens) {
          byte[] tokenBytes = token.getBytes(StandardCharsets.UTF_8);
          out.writeShort(tokenBytes.length);
          out.write(tokenBytes);
        }
      }
      return bytes.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to encode tokens", e);
    }
  }

  private static List<String> decodeTokens(ByteBuffer buffer) {
    try {
      byte[] bytes = new byte[buffer.remaining()];
      buffer.duplicate().get(bytes);
      try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
        int count = in.readInt();
        List<String> tokens = Lists.newArrayListWithCapacity(count);
        for (int i = 0; i < count; i++) {
          byte[] tokenBytes = new byte[in.readUnsignedShort()];
          in.readFully(tokenBytes);
          tokens.add(new String(tokenBytes, StandardCharsets.UTF_8));
        }
        return tokens;
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to decode tokens", e);
    }
  }

  /** A document of the indexed snapshot: its row reference and its token count. */
  private static final class Document {
    private final String dataFile;
    private final long rowPosition;
    private final int length;

    private Document(String dataFile, long rowPosition, int length) {
      this.dataFile = dataFile;
      this.rowPosition = rowPosition;
      this.length = length;
    }
  }

  /** One term occurrence: the document id and the term frequency in that document. */
  private static final class Posting {
    private final int docId;
    private final int termFrequency;

    private Posting(int docId, int termFrequency) {
      this.docId = docId;
      this.termFrequency = termFrequency;
    }
  }

  /** The decoded table-wide index. */
  private static final class IndexPayload {
    private final int totalDocuments;
    private final long totalLength;
    private final Map<String, List<Posting>> postingsByTerm;
    private final List<Document> documents;

    private IndexPayload(
        int totalDocuments,
        long totalLength,
        Map<String, List<Posting>> postingsByTerm,
        List<Document> documents) {
      this.totalDocuments = totalDocuments;
      this.totalLength = totalLength;
      this.postingsByTerm = postingsByTerm;
      this.documents = documents;
    }
  }
}
