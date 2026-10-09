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
 * <p>Scoring a document needs corpus-wide statistics: the document frequency of each query term and
 * the average document length. Those are sums over the whole snapshot, so this adapter searches all
 * index file payloads of a snapshot together ({@link #searchesAllPayloadsTogether()}). Each index
 * file carries the <b>local</b> postings and statistics of the data files it covers, and the
 * statistics are merged at search time, exactly like Lucene merges per-segment statistics. That
 * keeps index files per data file, so incremental refresh, coverage, lineage and delete semantics
 * are unchanged.
 *
 * <p>Score of document d for query q: {@code Σ_t∈q idf(t) · tf(t,d)·(k1+1) / (tf(t,d) + k1·(1 − b +
 * b·|d|/avgdl))}, with {@code idf(t) = ln(1 + (N − df(t) + 0.5)/(df(t) + 0.5))}. A query term that
 * occurs several times contributes once per occurrence.
 */
public final class TextIndexAdapter implements IndexAdapter {

  static final String ENTRY_BLOB_TYPE = "iceberg-text-entries-v1";
  static final String AUXILIARY_BLOB_TYPE = "iceberg-text-bm25-v1";

  private static final byte[] MAGIC = "ICEBM25".getBytes(StandardCharsets.US_ASCII);
  private static final byte FORMAT_VERSION = 1;

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
  public boolean searchesAllPayloadsTogether() {
    return true;
  }

  /** Analyzes a document into the tokens this index stores. */
  public byte[] encodeDocument(String text) {
    return encodeTokens(definition.analyze(text));
  }

  /** Encodes a query string; it is analyzed at search time with the same analyzer. */
  public ByteBuffer encodeQuery(String query) {
    return ByteBuffer.wrap(query.getBytes(StandardCharsets.UTF_8));
  }

  /** Decodes a document payload into its tokens. */
  public List<String> decodeDocument(ByteBuffer payload) {
    return decodeTokens(payload);
  }

  @Override
  public byte[] encodeEntryValue(Object columnValue) {
    return encodeDocument((String) columnValue);
  }

  @Override
  public ByteBuffer encodeEntries(List<IndexEntry> entries) {
    throw new UnsupportedOperationException(
        "Text indexes write auxiliary index files, not entry-bearing files");
  }

  @Override
  public List<IndexEntry> decodeEntries(ByteBuffer payload) {
    throw new UnsupportedOperationException(
        "Text indexes write auxiliary index files, not entry-bearing files");
  }

  @Override
  public List<IndexSearchResult> search(ByteBuffer payload, ByteBuffer query, int topK) {
    throw new UnsupportedOperationException(
        "Text indexes search all payloads of a snapshot together; see searchAuxiliaryAll");
  }

  /**
   * Builds the payload of one index file: the local postings and local statistics of the given
   * entries, plus the mapping from local document id to row reference and document length.
   */
  @Override
  public ByteBuffer encodeAuxiliaryPayload(List<IndexEntry> entries) {
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
        out.writeDouble(definition.lengthNorm());
        // local statistics header: merged across payloads at search time
        out.writeInt(documents.size());
        out.writeLong(totalLength);

        out.writeInt(postingsByTerm.size());
        for (Map.Entry<String, List<Posting>> term : postingsByTerm.entrySet()) {
          byte[] termBytes = term.getKey().getBytes(StandardCharsets.UTF_8);
          out.writeShort(termBytes.length);
          out.write(termBytes);
          // the document frequency doubles as the posting count: one posting per document
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
  public List<IndexSearchResult> searchAuxiliary(ByteBuffer payload, ByteBuffer query, int topK) {
    throw new UnsupportedOperationException(
        "Text indexes search all payloads of a snapshot together; see searchAuxiliaryAll");
  }

  /**
   * Searches all index file payloads of a snapshot: merges the local statistics into corpus-wide
   * document frequency and average document length, then scores the candidate documents.
   */
  @Override
  public List<IndexSearchResult> searchAuxiliaryAll(
      List<ByteBuffer> payloads, ByteBuffer query, int topK) {
    List<String> queryTokens = definition.analyze(decodeQuery(query));
    if (queryTokens.isEmpty() || topK <= 0 || payloads.isEmpty()) {
      return Lists.newArrayList();
    }

    List<FileIndex> indexes = Lists.newArrayListWithCapacity(payloads.size());
    for (ByteBuffer payload : payloads) {
      indexes.add(readPayload(payload));
    }

    // merge the local statistics into corpus-wide statistics, then score the candidates with them
    CorpusStatistics statistics = mergeStatistics(indexes);
    Map<String, Integer> queryTermFrequency = Maps.newHashMap();
    for (String token : queryTokens) {
      queryTermFrequency.merge(token, 1, Integer::sum);
    }

    Map<String, Map<Long, Double>> scoresByFile =
        scoreCandidates(indexes, queryTermFrequency, statistics);

    List<IndexSearchResult> results = Lists.newArrayList();
    for (Map.Entry<String, Map<Long, Double>> fileScores : scoresByFile.entrySet()) {
      for (Map.Entry<Long, Double> score : fileScores.getValue().entrySet()) {
        results.add(IndexSearchResult.of(fileScores.getKey(), score.getKey(), score.getValue()));
      }
    }

    results.sort(resultComparator());
    return results.size() > topK ? Lists.newArrayList(results.subList(0, topK)) : results;
  }

  /** The corpus-wide statistics merged from the local statistics of every index file. */
  private static CorpusStatistics mergeStatistics(List<FileIndex> indexes) {
    long totalDocuments = 0;
    long totalLength = 0;
    Map<String, Integer> documentFrequency = Maps.newHashMap();
    for (FileIndex index : indexes) {
      totalDocuments += index.localDocuments;
      totalLength += index.localTotalLength;
      for (Map.Entry<String, Integer> term : index.localDocumentFrequency.entrySet()) {
        documentFrequency.merge(term.getKey(), term.getValue(), Integer::sum);
      }
    }

    return new CorpusStatistics(
        totalDocuments,
        totalLength == 0 || totalDocuments == 0 ? 0.0 : (double) totalLength / totalDocuments,
        documentFrequency);
  }

  /** Scores every candidate document with the corpus-wide statistics, keyed by row reference. */
  private Map<String, Map<Long, Double>> scoreCandidates(
      List<FileIndex> indexes,
      Map<String, Integer> queryTermFrequency,
      CorpusStatistics statistics) {
    Map<String, Map<Long, Double>> scoresByFile = Maps.newHashMap();
    for (FileIndex index : indexes) {
      for (Map.Entry<String, Integer> queryTerm : queryTermFrequency.entrySet()) {
        List<Posting> postings = index.postingsByTerm.get(queryTerm.getKey());
        if (postings == null) {
          continue;
        }

        int frequency = statistics.documentFrequency.getOrDefault(queryTerm.getKey(), 0);
        double idf =
            Math.log(1.0 + (statistics.totalDocuments - frequency + 0.5) / (frequency + 0.5));
        for (Posting posting : postings) {
          Document document = index.documents.get(posting.docId);
          double score =
              queryTerm.getValue()
                  * documentScore(
                      idf, posting.termFrequency, document.length, statistics.averageLength);
          scoresByFile
              .computeIfAbsent(document.dataFile, key -> Maps.newHashMap())
              .merge(document.rowPosition, score, Double::sum);
        }
      }
    }

    return scoresByFile;
  }

  private double documentScore(double idf, int termFrequency, int length, double averageLength) {
    double frequency = termFrequency;
    // with no corpus length to normalize against, only the saturation applies
    double lengthNorm = averageLength == 0.0 ? 1.0 : (double) length / averageLength;
    double normalization =
        definition.k1() * (1.0 - definition.lengthNorm() + definition.lengthNorm() * lengthNorm)
            + frequency;
    return idf * frequency * (definition.k1() + 1.0) / normalization;
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

  private FileIndex readPayload(ByteBuffer buffer) {
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
        int localDocuments = in.readInt();
        long localTotalLength = in.readLong();

        int termCount = in.readInt();
        Map<String, Integer> localDocumentFrequency = Maps.newHashMap();
        Map<String, List<Posting>> postingsByTerm = Maps.newHashMap();
        for (int i = 0; i < termCount; i++) {
          byte[] termBytes = new byte[in.readUnsignedShort()];
          in.readFully(termBytes);
          String term = new String(termBytes, StandardCharsets.UTF_8);
          // the document frequency doubles as the posting count: one posting per document
          int postingCount = in.readInt();
          localDocumentFrequency.put(term, postingCount);
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

        return new FileIndex(
            localDocuments, localTotalLength, localDocumentFrequency, postingsByTerm, documents);
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

  /** A document of one index file: its row reference and its token count. */
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

  /** One term occurrence: the local document id and the term frequency in that document. */
  private static final class Posting {
    private final int docId;
    private final int termFrequency;

    private Posting(int docId, int termFrequency) {
      this.docId = docId;
      this.termFrequency = termFrequency;
    }
  }

  /** Corpus-wide statistics merged from the local statistics of every index file. */
  private static final class CorpusStatistics {
    private final long totalDocuments;
    private final double averageLength;
    private final Map<String, Integer> documentFrequency;

    private CorpusStatistics(
        long totalDocuments, double averageLength, Map<String, Integer> documentFrequency) {
      this.totalDocuments = totalDocuments;
      this.averageLength = averageLength;
      this.documentFrequency = documentFrequency;
    }
  }

  /** The decoded content of one index file payload. */
  private static final class FileIndex {
    private final int localDocuments;
    private final long localTotalLength;
    private final Map<String, Integer> localDocumentFrequency;
    private final Map<String, List<Posting>> postingsByTerm;
    private final List<Document> documents;

    private FileIndex(
        int localDocuments,
        long localTotalLength,
        Map<String, Integer> localDocumentFrequency,
        Map<String, List<Posting>> postingsByTerm,
        List<Document> documents) {
      this.localDocuments = localDocuments;
      this.localTotalLength = localTotalLength;
      this.localDocumentFrequency = localDocumentFrequency;
      this.postingsByTerm = postingsByTerm;
      this.documents = documents;
    }
  }
}
