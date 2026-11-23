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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import org.apache.iceberg.index.IndexAdapter;
import org.apache.iceberg.index.IndexBlob;
import org.apache.iceberg.index.IndexBuildResult;
import org.apache.iceberg.index.IndexCandidates;
import org.apache.iceberg.index.IndexDefinition;
import org.apache.iceberg.index.IndexEntry;
import org.apache.iceberg.index.IndexFile;
import org.apache.iceberg.index.IndexQuery;
import org.apache.iceberg.index.IndexSearchResult;
import org.apache.iceberg.index.IndexSnapshot;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.puffin.BlobMetadata;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinReader;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.ByteBuffers;

/**
 * BM25 text index adapter.
 *
 * <p>Scoring a document needs corpus-wide statistics: the document frequency of each query term and
 * the average document length. Those are sums over the whole snapshot, so this adapter searches all
 * index file payloads of a snapshot together ({@link #searchesAllPayloadsTogether()}). Each index
 * file carries the local postings and statistics of the data files it covers. The shared statistics
 * can be published with an index snapshot so queries only need the requested terms.
 *
 * <p>Score of document d for query q: {@code Σ_t∈q idf(t) · tf(t,d)·(k1+1) / (tf(t,d) + k1·(1 − b +
 * b·|d|/avgdl))}, with {@code idf(t) = ln(1 + (N − df(t) + 0.5)/(df(t) + 0.5))}. A query term that
 * occurs several times contributes once per occurrence.
 */
public final class TextIndexAdapter implements IndexAdapter, java.io.Serializable {

  private static final long serialVersionUID = 1L;

  static final String ENTRY_BLOB_TYPE = "iceberg-text-entries-v1";

  /**
   * The payload layout: a length column, the file table and the postings.
   *
   * <p>The blob type names the role a payload plays, not the version that wrote it; the version is
   * the first four bytes of the payload. A layout change bumps that version instead of the blob
   * type, so a build that cannot read an index still finds it under the type it looks for.
   */
  static final String AUXILIARY_BLOB_TYPE = "iceberg-text-bm25-v1";

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
  public Set<String> readableAuxiliaryBlobTypes() {
    return Set.of(AUXILIARY_BLOB_TYPE);
  }

  @Override
  public IndexBlob auxiliaryBlob(ByteBuffer payload) {
    return IndexBlob.uncompressed(auxiliaryBlobType(), ByteBuffers.toByteArray(payload));
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
  private static FileIndex indexEntries(List<IndexEntry> entries) {
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

    Map<String, Long> frequencies = Maps.newHashMap();
    postingsByTerm.forEach((term, postings) -> frequencies.put(term, (long) postings.size()));
    return new FileIndex(documents.size(), totalLength, frequencies, postingsByTerm, documents);
  }

  @Override
  public ByteBuffer encodeAuxiliaryPayload(List<IndexEntry> entries) {
    FileIndex index = indexEntries(entries);
    try {
      return TextIndexPayload.encode(counts(index), index);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to write BM25 shard", e);
    }
  }

  @Override
  public IndexBuildResult encodeAuxiliaryPayloadWithMemory(
      List<IndexEntry> entries, int parallelism) {
    return TextMemoryEstimates.result(encodeAuxiliaryPayload(entries), parallelism, readerId());
  }

  /**
   * The reader that measures a payload of the layout this definition selects.
   *
   * <p>The identity carries the payload format version, which is bumped whenever the bytes a build
   * writes would be misread by an older reader: a cached payload measured by an older reader is not
   * the same payload, and the version is what tells the two apart. This is the same rule a vector
   * kernel follows with {@code vector/<kernel>/<version>}.
   */
  private String readerId() {
    return "text/bm25/" + TextIndexPayload.FORMAT_VERSION;
  }

  /**
   * The text payload format version this build writes and reads.
   *
   * <p>Part of the payload rather than of the blob type, for the same reason a vector kernel keeps
   * its version in the payload: the blob type says which role a payload plays, and the payload says
   * which of its formats wrote it.
   */
  public int payloadFormatVersion() {
    return TextIndexPayload.FORMAT_VERSION;
  }

  /**
   * Reads the format version from the first four bytes of a payload, without reading the rest.
   *
   * @throws IllegalArgumentException when the payload is truncated or is not a BM25 payload
   */
  public int payloadVersion(ByteBuffer payload) {
    Preconditions.checkNotNull(payload, "Payload is null");
    Preconditions.checkArgument(
        payload.remaining() >= Integer.BYTES, "Truncated BM25 payload header");
    int version = TextIndexPayload.versionOf(payload.getInt(payload.position()));
    Preconditions.checkArgument(version >= 0, "Not a BM25 payload");
    return version;
  }

  @Override
  public Long retainedBytes(long payloadBytes, Map<String, Long> structure) {
    return TextMemoryEstimates.retainedBytes(payloadBytes, structure);
  }

  @Override
  public PreparedSearch prepareAuxiliary(ByteBuffer payload) {
    byte[] bytes = ByteBuffers.toByteArray(payload.asReadOnlyBuffer());
    PreparedShard shard = new PreparedShard(bytes);
    try {
      shard.open();
      return shard;
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid BM25 shard payload", e);
    }
  }

  /** Reads shard statistics from a prepared payload without opening its file. */
  public CorpusCounts shardStatistics(PreparedSearch prepared, Set<String> terms) {
    try {
      return ((PreparedShard) prepared).open().counts(terms);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read prepared BM25 shard statistics", e);
    }
  }

  /** Reads only the requested term frequencies from an uncompressed Puffin blob. */
  public CorpusCounts shardStatistics(InputFile file, Set<String> terms) {
    BlobMetadata blob = rangeBlob(file);
    try (SeekableInputStream stream = file.newStream()) {
      return openRange(stream, blob).counts(terms);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read BM25 shard statistics " + file.location(), e);
    }
  }

  /** Encodes the merged counts for a snapshot's term dictionary. */
  public ByteBuffer encodeSnapshotStatistics(CorpusCounts counts) {
    try {
      return TextIndexPayload.encode(counts, null);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to write BM25 snapshot statistics", e);
    }
  }

  @Override
  public Object readSnapshotStatistics(InputFile file, ByteBuffer query) {
    return snapshotStatistics(file, query.hasRemaining() ? queryTerms(query) : null);
  }

  @Override
  public List<IndexSearchResult> searchAuxiliaryFiles(
      FileIO io, IndexSnapshot snapshot, List<IndexFile> files, IndexQuery query) {
    Map<String, Integer> frequencies = queryTermFrequency(query.payload());
    if (files.isEmpty() || frequencies.isEmpty() || query.topK() <= 0) {
      return List.of();
    }

    CorpusStatistics statistics =
        snapshotStatistics(io.newInputFile(snapshot.statisticsLocation()), frequencies.keySet());
    PriorityQueue<IndexSearchResult> best = new PriorityQueue<>(resultComparator().reversed());
    List<byte[]> queries = List.of(ByteBuffers.toByteArray(query.payload()));
    int[] limits = {query.topK()};
    for (IndexFile file : files) {
      InputFile input = io.newInputFile(file.location());
      for (IndexSearchResult hit :
          searchShardBatch(input, queries, limits, statistics, null).get(0)) {
        best.add(hit);
        if (best.size() > query.topK()) {
          best.poll();
        }
      }
    }
    List<IndexSearchResult> results = Lists.newArrayList(best);
    results.sort(resultComparator());
    return List.copyOf(results);
  }

  /** Opens a snapshot dictionary and reads statistics only for the supplied query terms. */
  public CorpusStatistics snapshotStatistics(InputFile file, Set<String> terms) {
    try (SeekableInputStream stream = file.newStream()) {
      return decode(TextIndexSource.source(stream, 0), Math.toIntExact(file.getLength()))
          .counts(terms)
          .corpus();
    } catch (IOException e) {
      throw new UncheckedIOException(
          "Failed to read BM25 snapshot statistics " + file.location(), e);
    }
  }

  /** Scores a batch with the posting ranges of its distinct terms. */
  public List<List<IndexSearchResult>> searchShardBatch(
      InputFile file,
      List<byte[]> queries,
      int[] limits,
      CorpusStatistics statistics,
      IndexCandidates candidates) {
    BlobMetadata blob = rangeBlob(file);
    try (SeekableInputStream stream = file.newStream()) {
      TextIndexShard reader = openRange(stream, blob);
      return searchShardBatch(queries, limits, statistics, candidates, reader::shard);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read BM25 shard " + file.location(), e);
    }
  }

  /** Scores a batch from a prepared shard using statistics supplied by the caller. */
  public List<List<IndexSearchResult>> searchShardBatch(
      PreparedSearch prepared,
      List<byte[]> queries,
      int[] limits,
      CorpusStatistics statistics,
      IndexCandidates candidates) {
    try {
      return searchShardBatch(
          queries, limits, statistics, candidates, ((PreparedShard) prepared).open()::shard);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read prepared BM25 shard", e);
    }
  }

  private static final class PreparedShard implements PreparedSearch {
    private final byte[] payload;

    private PreparedShard(byte[] payload) {
      this.payload = payload;
    }

    private TextIndexShard open() throws IOException {
      return decode(TextIndexSource.source(ByteBuffer.wrap(payload)), payload.length);
    }

    @Override
    public List<IndexSearchResult> search(ByteBuffer query, int topK) {
      throw new UnsupportedOperationException(
          "BM25 requires corpus statistics; use searchShardBatch");
    }

    @Override
    public long estimatedBytes() {
      return TextMemoryEstimates.preparedBytes(payload.length);
    }
  }

  private static BlobMetadata rangeBlob(InputFile file) {
    try (PuffinReader reader = Puffin.read(file).build()) {
      for (BlobMetadata blob : reader.fileMetadata().blobs()) {
        if (AUXILIARY_BLOB_TYPE.equals(blob.type())) {
          if (blob.compressionCodec() != null
              || blob.offset() < 0
              || blob.length() < 0
              || blob.length() > file.getLength() - blob.offset()) {
            throw new IOException("Invalid BM25 blob range");
          }
          return blob;
        }
      }
      throw new IOException("Missing BM25 blob");
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to open BM25 shard " + file.location(), e);
    }
  }

  private static TextIndexShard openRange(SeekableInputStream stream, BlobMetadata blob)
      throws IOException {
    return decode(TextIndexSource.source(stream, blob.offset()), Math.toIntExact(blob.length()));
  }

  /**
   * Opens the payload, refusing a version this build does not read instead of decoding its bytes at
   * the offsets of another layout.
   */
  private static TextIndexShard decode(TextIndexSource.Source source, int size) throws IOException {
    int version =
        size >= Integer.BYTES
            ? TextIndexPayload.versionOf(source.read(0, Integer.BYTES).getInt())
            : -1;
    if (version >= 0) {
      if (version != TextIndexPayload.FORMAT_VERSION) {
        // a rebuild, not a search: the payload cannot be decoded by this reader, and saying so here
        // keeps a cached shard or a published one from being scored at the wrong offsets
        throw new IllegalArgumentException(TextIndexPayload.unsupportedVersionMessage(version));
      }

      return TextIndexPayload.open(source, size);
    }

    throw new IllegalArgumentException("Not a text index payload");
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
    Map<String, Integer> queryTerms = queryTermFrequency(query);
    if (queryTerms.isEmpty() || topK <= 0 || payloads.isEmpty()) {
      return Lists.newArrayList();
    }

    List<FileIndex> indexes = Lists.newArrayListWithCapacity(payloads.size());
    for (ByteBuffer payload : payloads) {
      indexes.add(readPayload(payload, queryTerms.keySet(), false));
    }

    return searchIndexes(indexes, queryTerms, mergeStatistics(indexes), topK, null);
  }

  /** Returns the terms whose document frequencies a query needs. */
  public Set<String> queryTerms(ByteBuffer query) {
    return new HashSet<>(definition.analyze(decodeQuery(query)));
  }

  private static CorpusCounts counts(FileIndex index) {
    return new CorpusCounts(
        index.localDocuments, index.localTotalLength, index.localDocumentFrequency);
  }

  private List<List<IndexSearchResult>> searchShardBatch(
      List<byte[]> queries,
      int[] limits,
      CorpusStatistics statistics,
      IndexCandidates candidates,
      ShardReader reader)
      throws IOException {
    List<Map<String, Integer>> frequencies = Lists.newArrayListWithCapacity(queries.size());
    Set<String> needed = new HashSet<>();
    for (int queryId = 0; queryId < queries.size(); queryId++) {
      Map<String, Integer> frequency =
          limits[queryId] > 0
              ? queryTermFrequency(ByteBuffer.wrap(queries.get(queryId)))
              : Map.of();
      frequencies.add(frequency);
      needed.addAll(frequency.keySet());
    }

    FileIndex index = reader.read(needed, false);
    List<List<IndexSearchResult>> hits = Lists.newArrayListWithCapacity(queries.size());
    for (int queryId = 0; queryId < queries.size(); queryId++) {
      hits.add(
          searchIndexes(
              List.of(index), frequencies.get(queryId), statistics, limits[queryId], candidates));
    }
    return hits;
  }

  @FunctionalInterface
  private interface ShardReader {
    FileIndex read(Set<String> terms, boolean statisticsOnly) throws IOException;
  }

  private Map<String, Integer> queryTermFrequency(ByteBuffer query) {
    Map<String, Integer> frequency = Maps.newHashMap();
    for (String token : definition.analyze(decodeQuery(query))) {
      frequency.merge(token, 1, Integer::sum);
    }
    return frequency;
  }

  private List<IndexSearchResult> searchIndexes(
      List<FileIndex> indexes,
      Map<String, Integer> queryTerms,
      CorpusStatistics statistics,
      int topK,
      IndexCandidates candidates) {
    if (queryTerms.isEmpty() || topK <= 0) {
      return List.of();
    }

    Map<String, Map<Long, Double>> scoresByFile =
        scoreCandidates(indexes, queryTerms, statistics, candidates);
    Comparator<IndexSearchResult> order = resultComparator();
    PriorityQueue<IndexSearchResult> best = new PriorityQueue<>(order.reversed());
    for (Map.Entry<String, Map<Long, Double>> fileScores : scoresByFile.entrySet()) {
      for (Map.Entry<Long, Double> score : fileScores.getValue().entrySet()) {
        IndexSearchResult hit =
            IndexSearchResult.of(fileScores.getKey(), score.getKey(), score.getValue());
        if (best.size() < topK) {
          best.add(hit);
        } else if (order.compare(hit, best.element()) < 0) {
          best.remove();
          best.add(hit);
        }
      }
    }

    List<IndexSearchResult> results = Lists.newArrayList(best);
    results.sort(order);
    return results;
  }

  /** The corpus-wide statistics merged from the local statistics of every index file. */
  private static CorpusStatistics mergeStatistics(List<FileIndex> indexes) {
    long totalDocuments = 0;
    long totalLength = 0;
    Map<String, Long> documentFrequency = Maps.newHashMap();
    for (FileIndex index : indexes) {
      totalDocuments = Math.addExact(totalDocuments, index.localDocuments);
      totalLength = Math.addExact(totalLength, index.localTotalLength);
      index.localDocumentFrequency.forEach(
          (term, count) -> documentFrequency.merge(term, count, Math::addExact));
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
      CorpusStatistics statistics,
      IndexCandidates candidates) {
    Map<String, Map<Long, Double>> scoresByFile = Maps.newHashMap();
    for (FileIndex index : indexes) {
      for (Map.Entry<String, Integer> queryTerm : queryTermFrequency.entrySet()) {
        List<Posting> postings = index.postingsByTerm.get(queryTerm.getKey());
        if (postings == null) {
          continue;
        }

        double idf = inverseDocumentFrequency(statistics, queryTerm.getKey());
        for (Posting posting : postings) {
          Document document = index.documents.get(posting.docId);
          if (candidates != null && !candidates.contains(document.dataFile, document.rowPosition)) {
            continue;
          }

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
    return documentScore(definition, idf, termFrequency, length, averageLength);
  }

  @Override
  public Object searchStatistics(List<ByteBuffer> payloads) {
    List<FileIndex> indexes = Lists.newArrayListWithCapacity(payloads.size());
    for (ByteBuffer payload : payloads) {
      indexes.add(readPayload(payload, null, true));
    }
    return mergeStatistics(indexes);
  }

  /**
   * Scores a document against a query the way the index does, with the corpus statistics of one
   * snapshot. This is what a query expression uses, so that a score computed outside the index is
   * the score the index would have given.
   *
   * @param definition the index definition, which carries the analyzer and the BM25 parameters
   * @param statistics the corpus statistics of the snapshot being read
   * @param document the text of the document
   * @param query the query text
   * @return the BM25 score; zero when the document shares no term with the query
   */
  public static double score(
      TextIndexDefinition definition, CorpusStatistics statistics, String document, String query) {
    List<String> documentTokens = definition.analyze(document);
    List<String> queryTokens = definition.analyze(query);
    if (documentTokens.isEmpty() || queryTokens.isEmpty() || statistics == null) {
      return 0.0;
    }

    Map<String, Integer> documentTerms = termFrequency(documentTokens);
    Map<String, Integer> queryTerms = termFrequency(queryTokens);
    double score = 0.0;
    for (Map.Entry<String, Integer> queryTerm : queryTerms.entrySet()) {
      int frequency = documentTerms.getOrDefault(queryTerm.getKey(), 0);
      if (frequency == 0) {
        continue;
      }

      double idf = inverseDocumentFrequency(statistics, queryTerm.getKey());
      score +=
          queryTerm.getValue()
              * documentScore(
                  definition, idf, frequency, documentTokens.size(), statistics.averageLength());
    }

    return score;
  }

  private static double inverseDocumentFrequency(CorpusStatistics statistics, String term) {
    long frequency = statistics.documentFrequency(term);
    return Math.log1p((statistics.totalDocuments() - frequency + 0.5) / (frequency + 0.5));
  }

  private static Map<String, Integer> termFrequency(List<String> tokens) {
    Map<String, Integer> frequency = Maps.newHashMap();
    for (String token : tokens) {
      frequency.merge(token, 1, Integer::sum);
    }
    return frequency;
  }

  private static double documentScore(
      TextIndexDefinition definition,
      double idf,
      int termFrequency,
      int length,
      double averageLength) {
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

  private FileIndex readPayload(ByteBuffer buffer, Set<String> terms, boolean statisticsOnly) {
    try {
      return decode(TextIndexSource.source(buffer), buffer.remaining())
          .shard(terms, statisticsOnly);
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
          out.writeInt(tokenBytes.length);
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
        int remaining = bytes.length - Integer.BYTES;
        if (count < 0 || count > remaining) {
          throw new IOException("Invalid token count: " + count);
        }

        List<String> tokens = Lists.newArrayListWithCapacity(count);
        for (int i = 0; i < count; i++) {
          int length = in.readInt();
          remaining -= Integer.BYTES;
          if (length < 0 || length > remaining) {
            throw new IOException("Invalid token length: " + length);
          }

          byte[] tokenBytes = new byte[length];
          in.readFully(tokenBytes);
          remaining -= length;
          tokens.add(new String(tokenBytes, StandardCharsets.UTF_8));
        }
        return tokens;
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to decode tokens", e);
    }
  }

  /** A document of one index file: its row reference and its token count. */
  static final class Document {
    final String dataFile;
    final long rowPosition;
    final int length;

    Document(String dataFile, long rowPosition, int length) {
      this.dataFile = dataFile;
      this.rowPosition = rowPosition;
      this.length = length;
    }
  }

  /** One term occurrence: the local document id and the term frequency in that document. */
  static final class Posting {
    final int docId;
    final int termFrequency;

    Posting(int docId, int termFrequency) {
      this.docId = docId;
      this.termFrequency = termFrequency;
    }
  }

  /** Additive corpus statistics that can be reduced across shards. */
  public static final class CorpusCounts implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    final long documents;
    final long tokens;
    final Map<String, Long> documentFrequency;

    CorpusCounts(long documents, long tokens, Map<String, Long> documentFrequency) {
      this.documents = documents;
      this.tokens = tokens;
      this.documentFrequency = ImmutableMap.copyOf(documentFrequency);
    }

    public static CorpusCounts empty() {
      return new CorpusCounts(0, 0, Map.of());
    }

    public CorpusCounts merge(CorpusCounts other) {
      Map<String, Long> frequency = Maps.newHashMap(documentFrequency);
      other.documentFrequency.forEach(
          (term, count) -> frequency.merge(term, count, Math::addExact));
      return new CorpusCounts(
          Math.addExact(documents, other.documents),
          Math.addExact(tokens, other.tokens),
          frequency);
    }

    public CorpusStatistics corpus() {
      double average = documents == 0 ? 0.0 : (double) tokens / documents;
      return new CorpusStatistics(documents, average, documentFrequency);
    }
  }

  /**
   * Corpus-wide statistics of one index snapshot, merged from the local statistics of every index
   * file. A BM25 score is a function of these, which is why an engine that scores rows outside the
   * index, a query expression for instance, needs them.
   */
  public static final class CorpusStatistics implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    private final long totalDocuments;
    private final double averageLength;
    private final Map<String, Long> documentFrequency;

    private CorpusStatistics(
        long totalDocuments, double averageLength, Map<String, Long> documentFrequency) {
      this.totalDocuments = totalDocuments;
      this.averageLength = averageLength;
      this.documentFrequency = ImmutableMap.copyOf(documentFrequency);
    }

    /**
     * Statistics with nothing in them, for an expression that has not been bound to an index yet.
     */
    public static CorpusStatistics empty() {
      return new CorpusStatistics(0, 0.0, Maps.newHashMap());
    }

    /** The number of documents in the snapshot the statistics were merged from. */
    public long totalDocuments() {
      return totalDocuments;
    }

    /** The average document length of that snapshot, in tokens. */
    public double averageLength() {
      return averageLength;
    }

    /**
     * How many documents of that snapshot contain each term.
     *
     * @throws ArithmeticException if a term occurs in more than {@link Integer#MAX_VALUE} documents
     */
    public Map<String, Integer> documentFrequency() {
      ImmutableMap.Builder<String, Integer> frequencies = ImmutableMap.builder();
      documentFrequency.forEach((term, count) -> frequencies.put(term, Math.toIntExact(count)));
      return frequencies.build();
    }

    /** How many documents of that snapshot contain the term, or zero if absent. */
    public long documentFrequency(String term) {
      return documentFrequency.getOrDefault(term, 0L);
    }
  }

  /** The decoded content of one index file payload. */
  static final class FileIndex {
    final int localDocuments;
    final long localTotalLength;
    final Map<String, Long> localDocumentFrequency;
    final Map<String, List<Posting>> postingsByTerm;
    final List<Document> documents;

    FileIndex(
        int localDocuments,
        long localTotalLength,
        Map<String, Long> localDocumentFrequency,
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
