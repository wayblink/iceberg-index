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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.api.Assertions.within;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.iceberg.index.IndexEntry;
import org.apache.iceberg.index.IndexSearchResult;
import org.apache.iceberg.index.text.TextIndexAdapter.CorpusCounts;
import org.apache.iceberg.index.text.TextIndexAdapter.CorpusStatistics;
import org.apache.iceberg.index.text.TextIndexAdapter.Document;
import org.apache.iceberg.index.text.TextIndexAdapter.FileIndex;
import org.apache.iceberg.index.text.TextIndexAdapter.Posting;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;

/**
 * Measures BM25 payload sizes for a deterministic synthetic corpus and checks that two payload
 * formats answer the same queries identically.
 *
 * <p>The corpus models the megamath corpus the v1 baseline was measured on: 599 tokens per document
 * on average, 137 postings per document, a Zipf term distribution with exponent 1.05 and about 45%
 * hapax terms. Word forms are random alphanumeric strings about 12 characters long and every shard
 * covers four data files with 123-character paths, because the v1 posting ranges repeat those paths
 * and their length sets the compression cost per posting.
 *
 * <p>Both formats are encoded from the same decoded content, one {@link FileIndex} and its {@link
 * CorpusCounts} per shard, so the reported difference is the layout and not a difference in the
 * documents, postings or statistics. Each report names the layout sections and the cost per posting
 * of every document frequency bucket, and the test asserts that the two formats answer every query
 * with the same hits, scores and order, which is the contract a format change keeps.
 *
 * <p>The probe does not assert absolute byte counts: those belong to a measured baseline, and a
 * format change should move them. It asserts the stable invariants instead, that the per-shard
 * statistics merge back to the generated corpus and that the answers agree bit for bit.
 *
 * <p>The probe is a scale model, not a reproduction: at 20k documents a term can appear in at most
 * 20k documents (5k in one shard), so the {@code >=65536} document frequency bucket stays empty and
 * every term sits lower than in the production corpus. Read the per-bucket cost with the production
 * bucket sizes when extrapolating, and do not compare the reported bytes per document with the
 * production number directly.
 */
class Bm25FormatProbe {

  // Corpus shape, taken from the production corpus: 6,456,636,544 tokens over 10,784,907 documents.
  private static final int DEFAULT_DOCUMENTS = 20_000;
  private static final int DEFAULT_SHARD_DOCUMENTS = 5_000;
  private static final int DATA_FILES_PER_SHARD = 4;
  private static final int POSTINGS_PER_DOCUMENT = 137;
  private static final double MEAN_DOCUMENT_TOKENS = 599.0;
  private static final double DOCUMENT_LENGTH_SIGMA = 0.35;
  private static final double MEAN_TERM_LENGTH = 11.9;
  private static final double TERM_LENGTH_SIGMA = 2.5;
  private static final double ZIPF_EXPONENT = 1.05;
  // With 137 postings per document, 515k candidates put about 45% of the observed vocabulary at a
  // document frequency of one; the test asserts the fraction stays in a narrow band.
  private static final int VOCABULARY_CANDIDATES = 515_000;
  private static final long SEED = 20261120L;
  private static final String ALPHABET = "abcdefghijklmnopqrstuvwxyz0123456789";
  private static final double LOG_MEAN_DOCUMENT_TOKENS =
      Math.log(MEAN_DOCUMENT_TOKENS) - DOCUMENT_LENGTH_SIGMA * DOCUMENT_LENGTH_SIGMA / 2.0;
  // one query per order of magnitude of document frequency, so every bucket is exercised
  private static final int QUERY_COUNT = 20;

  // BM25 v1 payload layout, mirrored from TextIndexPayload: corpus counts and 64+1 dictionary
  // offsets in the header, one dictionary per hash bucket, then the term posting ranges.
  private static final int V1_MAGIC = 0x424D3101;
  private static final int V1_BUCKETS = 64;
  private static final int V1_HEADER_BYTES =
      Integer.BYTES + 2 * Long.BYTES + (V1_BUCKETS + 1) * Integer.BYTES;
  private static final int V1_OFFSETS_AT = Integer.BYTES + 2 * Long.BYTES;

  // BM25 v2 payload layout, mirrored from TextIndexPayloadV2: counts and the section offsets in
  // the header, then the file table, the document table, the front coded dictionaries and the
  // term posting ranges.
  private static final int V2_MAGIC = 0x424D3201;
  private static final int V2_BUCKETS = 64;
  private static final int V2_HEADER_BYTES =
      Integer.BYTES + 2 * Long.BYTES + 7 * Integer.BYTES + (V2_BUCKETS + 1) * Integer.BYTES;
  private static final int V2_FILES_LENGTH_AT = Integer.BYTES + 2 * Long.BYTES + 3 * Integer.BYTES;
  private static final int V2_DOC_DIRECTORY_LENGTH_AT = V2_FILES_LENGTH_AT + Integer.BYTES;
  private static final int V2_DICTIONARY_START_AT = V2_DOC_DIRECTORY_LENGTH_AT + Integer.BYTES;
  private static final int V2_POSTINGS_START_AT = V2_DICTIONARY_START_AT + Integer.BYTES;
  private static final int V2_OFFSETS_AT = V2_POSTINGS_START_AT + Integer.BYTES;

  private static final int[] DF_BUCKET_LIMITS = {1, 3, 15, 1023, 65535, Integer.MAX_VALUE};
  private static final String[] DF_BUCKET_LABELS = {
    "1", "2-3", "4-15", "16-1023", "1024-65535", ">=65536"
  };

  private final TextIndexAdapter adapter =
      new TextIndexAdapter(TextIndexDefinition.builder().fieldId(1).build());

  @Test
  void reportsSizeAndEquivalence() {
    int documents = Integer.getInteger("bm25.probe.documents", DEFAULT_DOCUMENTS);
    int shardDocuments = Integer.getInteger("bm25.probe.shard-documents", DEFAULT_SHARD_DOCUMENTS);
    long start = System.nanoTime();

    Corpus corpus = Corpus.generate(documents);
    List<List<IndexEntry>> shards = shards(corpus, shardDocuments);
    List<ShardContent> contents = contents(shards);
    List<ByteBuffer> v1Payloads = encodeV1(contents);
    List<ByteBuffer> v2Payloads = encodeV2(contents);

    System.out.println();
    System.out.printf(
        Locale.ROOT,
        "synth inputs: %d documents, %d shards, %d data files per shard, %d rows per file,"
            + " %d-character data file path%n",
        documents,
        shards.size(),
        DATA_FILES_PER_SHARD,
        shards.get(0).size() / DATA_FILES_PER_SHARD,
        shards.get(0).get(0).dataFile().length());
    System.out.println(corpus.describe());
    System.out.println(measure("BM25 v1 baseline", v1Payloads, documents));
    System.out.println(measure("BM25 v2", v2Payloads, documents));
    System.out.printf(
        Locale.ROOT,
        "adapter blob type: %s; v2/v1 size: %.3f (%,d -> %,d bytes, %.1f -> %.1f B/document)%n",
        adapter.auxiliaryBlobType(),
        (double) totalBytes(v2Payloads) / totalBytes(v1Payloads),
        totalBytes(v1Payloads),
        totalBytes(v2Payloads),
        (double) totalBytes(v1Payloads) / documents,
        (double) totalBytes(v2Payloads) / documents);

    List<String> queries = corpus.queries();
    StringBuilder queryFrequencies = new StringBuilder();
    for (String query : queries) {
      if (queryFrequencies.length() > 0) {
        queryFrequencies.append(", ");
      }
      queryFrequencies.append(corpus.documentFrequencyOf(query));
    }
    System.out.printf(
        Locale.ROOT, "queries: %d, document frequencies [%s]%n", queries.size(), queryFrequencies);
    for (List<ByteBuffer> payloads : List.of(v1Payloads, v2Payloads)) {
      int answered = 0;
      for (String query : queries) {
        if (!adapter.searchAuxiliaryAll(payloads, adapter.encodeQuery(query), 10).isEmpty()) {
          answered++;
        }
      }
      assertThat(answered).isEqualTo(queries.size());
    }

    assertThat(v1Payloads)
        .hasSize(shards.size())
        .allSatisfy(payload -> assertThat(payload.remaining()).isPositive());
    assertThat(v2Payloads)
        .hasSize(shards.size())
        .allSatisfy(payload -> assertThat(payload.remaining()).isPositive());
    assertThat(shards).allSatisfy(shard -> assertThat(shard).isNotEmpty());

    // corpus shape: the generator has to produce the workload the probe reports on
    assertThat(corpus.postings()).isEqualTo((long) documents * POSTINGS_PER_DOCUMENT);
    assertThat(corpus.meanLength()).isBetween(560.0, 640.0);
    assertThat(corpus.hapaxFraction()).isBetween(0.40, 0.50);

    // the per-shard local statistics of both formats merge back to the generated corpus
    assertCorpusStatistics("BM25 v1", corpus, v1Payloads);
    assertCorpusStatistics("BM25 v2", corpus, v2Payloads);

    // the answers of both formats have to agree hit for hit, score bit for score bit
    assertSameTopK(adapter, v1Payloads, v2Payloads, queries, 100);
    System.out.printf(
        Locale.ROOT,
        "equivalence: %d queries x top-100, answers and scores are bit-identical%n",
        queries.size());
    // the comparison has to notice a difference, otherwise it cannot guard a format change
    System.out.println("negative control, a dropped payload has to be reported:");
    assertThatThrownBy(
            () ->
                assertSameTopK(
                    adapter, v1Payloads, v2Payloads.subList(1, v2Payloads.size()), queries, 100))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("Query");

    // the format change exists to shrink the payload
    assertThat(totalBytes(v2Payloads)).isLessThan(totalBytes(v1Payloads));
    System.out.printf(Locale.ROOT, "probe elapsed: %.1f s%n", (System.nanoTime() - start) / 1.0e9);
  }

  /**
   * Reports the size of a payload list, its layout sections and, for the payloads this probe
   * writes, the share and per-posting cost of every document frequency bucket.
   */
  static String measure(String label, List<ByteBuffer> payloads, int documents) {
    StringBuilder report = new StringBuilder();
    report
        .append("payload size report [")
        .append(label)
        .append("]: ")
        .append(payloads.size())
        .append(" payloads, ")
        .append(documents)
        .append(" documents\n");
    int documentsPerPayload = payloads.isEmpty() ? 0 : documents / payloads.size();
    long totalBytes = totalBytes(payloads);
    for (int payload = 0; payload < payloads.size(); payload++) {
      int bytes = payloads.get(payload).remaining();
      report.append(
          String.format(
              Locale.ROOT,
              "  payload[%d]: %,d bytes (%.1f B/document)%n",
              payload,
              bytes,
              documentsPerPayload == 0 ? 0.0 : (double) bytes / documentsPerPayload));
    }
    report.append(
        String.format(
            Locale.ROOT,
            "  total: %,d bytes, %.1f B/document%n",
            totalBytes,
            documents == 0 ? 0.0 : (double) totalBytes / documents));

    Breakdown breakdown = breakdown(payloads);
    if (breakdown == null) {
      report.append("  layout breakdown: skipped, the payloads are not a BM25 v1 or v2 payload\n");
      return report.toString();
    }

    report.append(String.format(Locale.ROOT, "  format: %s%n", breakdown.format));
    report.append(String.format(Locale.ROOT, "  layout: %s%n", breakdown.sections()));
    report.append(
        String.format(
            Locale.ROOT,
            "  postings: %,d (%.1f/document), %.2f B/posting of ranges, %.2f B/posting of all"
                + " bytes%n",
            breakdown.postings,
            documents == 0 ? 0.0 : (double) breakdown.postings / documents,
            breakdown.postings == 0 ? 0.0 : (double) breakdown.rangeBytes / breakdown.postings,
            breakdown.postings == 0 ? 0.0 : (double) breakdown.totalBytes() / breakdown.postings));
    report.append("  df bucket, per-payload local document frequency:\n");
    report.append(
        String.format(
            Locale.ROOT,
            "    %-12s %10s %12s %15s %12s %8s%n",
            "bucket",
            "terms",
            "postings",
            "range bytes",
            "B/posting",
            "share"));
    for (int bucket = 0; bucket < DF_BUCKET_LABELS.length; bucket++) {
      report.append(
          String.format(
              Locale.ROOT,
              "    %-12s %,10d %,12d %,15d %12.2f %7.1f%%%n",
              DF_BUCKET_LABELS[bucket],
              breakdown.termsByBucket[bucket],
              breakdown.postingsByBucket[bucket],
              breakdown.rangeBytesByBucket[bucket],
              breakdown.postingsByBucket[bucket] == 0
                  ? 0.0
                  : (double) breakdown.rangeBytesByBucket[bucket]
                      / breakdown.postingsByBucket[bucket],
              breakdown.rangeBytes == 0
                  ? 0.0
                  : 100.0 * breakdown.rangeBytesByBucket[bucket] / breakdown.rangeBytes));
    }
    report.append(
        String.format(
            Locale.ROOT,
            "  max local document frequency: %,d, so the >=65536 bucket cannot fill with %,d"
                + " documents per payload%n",
            breakdown.maxLocalDocumentFrequency,
            documentsPerPayload));
    return report.toString();
  }

  /**
   * Asserts that two payload lists answer every query with the same hits in the same order, with
   * bit-identical scores, and prints the first difference when they do not.
   */
  static void assertSameTopK(
      TextIndexAdapter adapter,
      List<ByteBuffer> expectedPayloads,
      List<ByteBuffer> actualPayloads,
      List<String> queries,
      int topK) {
    for (String query : queries) {
      ByteBuffer encoded = adapter.encodeQuery(query);
      List<IndexSearchResult> expected =
          adapter.searchAuxiliaryAll(expectedPayloads, encoded, topK);
      List<IndexSearchResult> actual = adapter.searchAuxiliaryAll(actualPayloads, encoded, topK);
      if (expected.size() != actual.size()) {
        String message =
            String.format(
                Locale.ROOT,
                "Query \"%s\" returned %d hits for the expected payloads and %d for the actual"
                    + " payloads",
                query,
                expected.size(),
                actual.size());
        System.out.println(message);
        fail(message);
      }

      for (int rank = 0; rank < expected.size(); rank++) {
        IndexSearchResult first = expected.get(rank);
        IndexSearchResult second = actual.get(rank);
        boolean sameScore =
            Double.doubleToLongBits(first.score()) == Double.doubleToLongBits(second.score());
        if (!first.dataFile().equals(second.dataFile())
            || first.rowPosition() != second.rowPosition()
            || !sameScore) {
          String message =
              String.format(
                  Locale.ROOT,
                  "Query \"%s\" differs at rank %d: expected (%s, %d, %s) but got (%s, %d, %s)",
                  query,
                  rank,
                  first.dataFile(),
                  first.rowPosition(),
                  Double.toHexString(first.score()),
                  second.dataFile(),
                  second.rowPosition(),
                  Double.toHexString(second.score()));
          System.out.println(message);
          fail(message);
        }
      }
    }
  }

  /** Encodes one payload per shard with the v1 writer, the baseline v2 is compared with. */
  private static List<ByteBuffer> encodeV1(List<ShardContent> shards) {
    List<ByteBuffer> payloads = Lists.newArrayListWithCapacity(shards.size());
    for (ShardContent shard : shards) {
      try {
        payloads.add(TextIndexPayload.encode(shard.counts, shard.index));
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to write a BM25 v1 shard", e);
      }
    }
    return payloads;
  }

  /**
   * Encodes one payload per shard with the v2 writer. A third format adds a method like this one;
   * {@link #measure} and {@link #assertSameTopK} stay independent of the format.
   */
  private static List<ByteBuffer> encodeV2(List<ShardContent> shards) {
    List<ByteBuffer> payloads = Lists.newArrayListWithCapacity(shards.size());
    for (ShardContent shard : shards) {
      try {
        payloads.add(TextIndexPayloadV2.encode(shard.counts, shard.index));
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to write a BM25 v2 shard", e);
      }
    }
    return payloads;
  }

  private List<ShardContent> contents(List<List<IndexEntry>> shards) {
    List<ShardContent> contents = Lists.newArrayListWithCapacity(shards.size());
    for (List<IndexEntry> shard : shards) {
      contents.add(new ShardContent(shardContent(shard)));
    }
    return contents;
  }

  private FileIndex shardContent(List<IndexEntry> entries) {
    List<Document> documents = Lists.newArrayListWithCapacity(entries.size());
    Map<String, List<Posting>> postingsByTerm = Maps.newHashMap();
    long totalTokens = 0;
    for (int docId = 0; docId < entries.size(); docId++) {
      IndexEntry entry = entries.get(docId);
      List<String> tokens = adapter.decodeDocument(ByteBuffer.wrap(entry.payload()));
      documents.add(new Document(entry.dataFile(), entry.rowPosition(), tokens.size()));
      totalTokens += tokens.size();

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

    Map<String, Long> documentFrequency = Maps.newHashMap();
    postingsByTerm.forEach((term, postings) -> documentFrequency.put(term, (long) postings.size()));
    return new FileIndex(
        documents.size(), totalTokens, documentFrequency, postingsByTerm, documents);
  }

  private void assertCorpusStatistics(String label, Corpus corpus, List<ByteBuffer> payloads) {
    CorpusStatistics statistics = (CorpusStatistics) adapter.searchStatistics(payloads);
    assertThat(statistics.totalDocuments()).as("%s documents", label).isEqualTo(corpus.documents());
    assertThat(statistics.averageLength())
        .as("%s average length", label)
        .isCloseTo(corpus.meanLength(), within(1.0e-12));
    Map<String, Integer> merged = statistics.documentFrequency();
    assertThat(merged).as("%s distinct terms", label).hasSize(corpus.observedTerms());
    int mismatchedFrequencies = 0;
    for (int term = 0; term < corpus.vocabularyTerms(); term++) {
      if (merged.getOrDefault(corpus.term(term), 0) != corpus.documentFrequency(term)) {
        mismatchedFrequencies++;
      }
    }
    assertThat(mismatchedFrequencies)
        .as("%s document frequencies that differ from the corpus", label)
        .isZero();
  }

  private List<List<IndexEntry>> shards(Corpus corpus, int shardDocuments) {
    int documents = corpus.documents();
    int shardCount = (documents + shardDocuments - 1) / shardDocuments;
    List<List<IndexEntry>> shards = Lists.newArrayListWithCapacity(shardCount);
    for (int shard = 0; shard < shardCount; shard++) {
      int first = shard * shardDocuments;
      int last = Math.min(documents, first + shardDocuments);
      int rowsPerFile = (last - first + DATA_FILES_PER_SHARD - 1) / DATA_FILES_PER_SHARD;
      List<IndexEntry> entries = Lists.newArrayListWithCapacity(last - first);
      for (int document = first; document < last; document++) {
        int row = document - first;
        int file = Math.min(DATA_FILES_PER_SHARD - 1, row / rowsPerFile);
        entries.add(
            corpus.entry(
                adapter,
                document,
                dataFile(shard * DATA_FILES_PER_SHARD + file),
                row % rowsPerFile));
      }
      shards.add(entries);
    }
    return shards;
  }

  private static String dataFile(int file) {
    long salt = (file + 1) * 0x9E3779B97F4A7C15L;
    return String.format(
        Locale.ROOT,
        "s3://warehouse-prod/datasets/megamath-parquet-v7/data/train/part-%05d-%016x%016x-c000"
            + ".snappy.parquet",
        file,
        salt,
        ~salt);
  }

  private static long totalBytes(List<ByteBuffer> payloads) {
    long bytes = 0;
    for (ByteBuffer payload : payloads) {
      bytes += payload.remaining();
    }
    return bytes;
  }

  private static Breakdown breakdown(List<ByteBuffer> payloads) {
    if (payloads.isEmpty() || payloads.get(0).remaining() < Integer.BYTES) {
      return null;
    }
    int magic = payloads.get(0).slice().getInt(0);
    if (magic == V1_MAGIC) {
      return v1Breakdown(payloads);
    }
    if (magic == V2_MAGIC) {
      return v2Breakdown(payloads);
    }
    return null;
  }

  /** Reads the v1 layout: header, the 64 dictionaries and the per-term posting ranges. */
  private static Breakdown v1Breakdown(List<ByteBuffer> payloads) {
    Breakdown breakdown = new Breakdown("BM25 v1", "header", "dictionary", "posting ranges");
    for (ByteBuffer payload : payloads) {
      ByteBuffer bytes = payload.slice();
      int size = bytes.remaining();
      if (size < V1_HEADER_BYTES || bytes.getInt(0) != V1_MAGIC) {
        return null;
      }
      open(payload);

      long documents = bytes.getLong(Integer.BYTES);
      int[] offsets = offsets(bytes, V1_BUCKETS, V1_OFFSETS_AT);
      int postingsStart = offsets[V1_BUCKETS];
      breakdown.section(0, V1_HEADER_BYTES);
      breakdown.section(1, postingsStart - V1_HEADER_BYTES);

      long rangeBytes = v1RangeBytes(breakdown, bytes, offsets, postingsStart, size, documents);
      if ((long) V1_HEADER_BYTES + (postingsStart - V1_HEADER_BYTES) + rangeBytes != size) {
        throw new IllegalArgumentException(
            "Inconsistent BM25 v1 layout: header and dictionaries do not cover the payload");
      }
      breakdown.section(2, rangeBytes);
    }
    return breakdown;
  }

  private static int[] offsets(ByteBuffer bytes, int buckets, int offsetAt) {
    int[] offsets = new int[buckets + 1];
    for (int bucket = 0; bucket <= buckets; bucket++) {
      offsets[bucket] = bytes.getInt(offsetAt + bucket * Integer.BYTES);
    }
    return offsets;
  }

  private static long v1RangeBytes(
      Breakdown breakdown,
      ByteBuffer bytes,
      int[] offsets,
      int postingsStart,
      int size,
      long documents) {
    long rangeBytes = 0;
    for (int bucket = 0; bucket < V1_BUCKETS; bucket++) {
      int start = offsets[bucket];
      int end = offsets[bucket + 1];
      if (start == end) {
        continue;
      }
      try (DataInputStream in =
          new DataInputStream(new ByteArrayInputStream(toArray(bytes.slice(start, end - start))))) {
        while (in.available() > 0) {
          in.readUTF();
          long frequency = in.readLong();
          int offset = in.readInt();
          int length = in.readInt();
          if (frequency <= 0
              || frequency > documents
              || offset < 0
              || length < 0
              || (long) postingsStart + offset + length > size) {
            throw new IllegalArgumentException("Inconsistent BM25 v1 dictionary entry");
          }
          breakdown.addTerm(frequency, length);
          rangeBytes += length;
        }
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to read the BM25 v1 dictionary", e);
      }
    }
    return rangeBytes;
  }

  /**
   * Reads the v2 layout. The posting ranges are the section from {@code postingsStart} to the end
   * of the payload; the file table and the document table that precede the dictionaries are not
   * part of it even though they address the same documents.
   */
  private static Breakdown v2Breakdown(List<ByteBuffer> payloads) {
    Breakdown breakdown =
        new Breakdown(
            "BM25 v2",
            "header",
            "file table",
            "document table directory",
            "document table chunks",
            "dictionary",
            "posting ranges");
    for (ByteBuffer payload : payloads) {
      ByteBuffer bytes = payload.slice();
      int size = bytes.remaining();
      if (size < V2_HEADER_BYTES || bytes.getInt(0) != V2_MAGIC) {
        return null;
      }
      open(payload);

      long documents = bytes.getLong(Integer.BYTES);
      int filesLength = bytes.getInt(V2_FILES_LENGTH_AT);
      int docDirectoryLength = bytes.getInt(V2_DOC_DIRECTORY_LENGTH_AT);
      int dictionaryStart = bytes.getInt(V2_DICTIONARY_START_AT);
      int postingsStart = bytes.getInt(V2_POSTINGS_START_AT);
      int[] offsets = offsets(bytes, V2_BUCKETS, V2_OFFSETS_AT);
      long documentChunks =
          (long) dictionaryStart - V2_HEADER_BYTES - filesLength - docDirectoryLength;
      if (documentChunks < 0
          || offsets[0] != dictionaryStart
          || offsets[V2_BUCKETS] != postingsStart) {
        throw new IllegalArgumentException("Inconsistent BM25 v2 header sections");
      }
      breakdown.section(0, V2_HEADER_BYTES);
      breakdown.section(1, filesLength);
      breakdown.section(2, docDirectoryLength);
      breakdown.section(3, documentChunks);
      breakdown.section(4, postingsStart - dictionaryStart);

      long rangeBytes = v2RangeBytes(breakdown, bytes, offsets, documents);

      if (rangeBytes != size - (long) postingsStart) {
        throw new IllegalArgumentException(
            "Inconsistent BM25 v2 postings: ranges do not cover the posting section");
      }
      long sections =
          (long) V2_HEADER_BYTES
              + filesLength
              + docDirectoryLength
              + documentChunks
              + (postingsStart - (long) dictionaryStart)
              + rangeBytes;
      if (sections != size) {
        throw new IllegalArgumentException(
            "Inconsistent BM25 v2 layout: sections do not cover the payload");
      }
      breakdown.section(5, rangeBytes);
    }
    return breakdown;
  }

  private static long v2RangeBytes(
      Breakdown breakdown, ByteBuffer bytes, int[] offsets, long documents) {
    long rangeBytes = 0;
    for (int bucket = 0; bucket < V2_BUCKETS; bucket++) {
      int start = offsets[bucket];
      int end = offsets[bucket + 1];
      if (start == end) {
        continue;
      }
      try (DataInputStream in =
          new DataInputStream(new ByteArrayInputStream(toArray(bytes.slice(start, end - start))))) {
        while (in.available() > 0) {
          in.readUnsignedByte();
          int suffix = readVarint(in);
          in.readFully(new byte[suffix]);
          long frequency = readLongVarint(in);
          readVarint(in);
          int length = readVarint(in);
          if (frequency <= 0 || frequency > documents || length < 0) {
            throw new IllegalArgumentException("Inconsistent BM25 v2 dictionary entry");
          }
          breakdown.addTerm(frequency, length);
          rangeBytes += length;
        }
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to read the BM25 v2 dictionary", e);
      }
    }
    return rangeBytes;
  }

  private static int readVarint(DataInputStream in) throws IOException {
    int result = 0;
    int shift = 0;
    while (true) {
      int next = in.read();
      if (next < 0) {
        throw new IOException("Truncated BM25 v2 varint");
      }
      result |= (next & 0x7F) << shift;
      if ((next & 0x80) == 0) {
        return result;
      }
      shift += 7;
      if (shift > 28) {
        throw new IOException("Invalid BM25 v2 varint");
      }
    }
  }

  private static long readLongVarint(DataInputStream in) throws IOException {
    long result = 0L;
    int shift = 0;
    while (true) {
      int next = in.read();
      if (next < 0) {
        throw new IOException("Truncated BM25 v2 varint");
      }
      result |= (long) (next & 0x7F) << shift;
      if ((next & 0x80) == 0) {
        return result;
      }
      shift += 7;
      if (shift > 63) {
        throw new IOException("Invalid BM25 v2 varint");
      }
    }
  }

  /** Opens the payload layout its signature names, so a bad payload fails here, not in a report. */
  private static TextIndexShard open(ByteBuffer payload) {
    ByteBuffer bytes = payload.slice();
    try {
      if (bytes.remaining() >= Integer.BYTES && bytes.getInt(0) == V2_MAGIC) {
        return TextIndexPayloadV2.open(TextIndexPayload.source(bytes), bytes.remaining());
      }
      return TextIndexPayload.open(TextIndexPayload.source(bytes), bytes.remaining());
    } catch (IOException e) {
      throw new UncheckedIOException("Invalid BM25 payload", e);
    }
  }

  private static byte[] toArray(ByteBuffer buffer) {
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    return bytes;
  }

  private static int dfBucket(long documentFrequency) {
    for (int bucket = 0; bucket < DF_BUCKET_LIMITS.length; bucket++) {
      if (documentFrequency <= DF_BUCKET_LIMITS[bucket]) {
        return bucket;
      }
    }
    return DF_BUCKET_LIMITS.length - 1;
  }

  /** The decoded content of one shard: the input of every payload format. */
  private static final class ShardContent {
    private final FileIndex index;
    private final CorpusCounts counts;

    private ShardContent(FileIndex index) {
      this.index = index;
      this.counts =
          new CorpusCounts(
              index.localDocuments, index.localTotalLength, index.localDocumentFrequency);
    }
  }

  /** One payload format's size by layout section and by document frequency bucket. */
  private static final class Breakdown {
    private final String format;
    private final String[] sectionNames;
    private final long[] sectionBytes;
    private long postings;
    private long rangeBytes;
    private long maxLocalDocumentFrequency;
    private final long[] termsByBucket = new long[DF_BUCKET_LABELS.length];
    private final long[] postingsByBucket = new long[DF_BUCKET_LABELS.length];
    private final long[] rangeBytesByBucket = new long[DF_BUCKET_LABELS.length];

    private Breakdown(String format, String... sectionNames) {
      this.format = format;
      this.sectionNames = sectionNames;
      this.sectionBytes = new long[sectionNames.length];
    }

    private void section(int section, long bytes) {
      sectionBytes[section] = Math.addExact(sectionBytes[section], bytes);
    }

    private void addTerm(long documentFrequency, long bytes) {
      int bucket = dfBucket(documentFrequency);
      termsByBucket[bucket]++;
      postingsByBucket[bucket] += documentFrequency;
      rangeBytesByBucket[bucket] += bytes;
      postings = Math.addExact(postings, documentFrequency);
      rangeBytes = Math.addExact(rangeBytes, bytes);
      maxLocalDocumentFrequency = Math.max(maxLocalDocumentFrequency, documentFrequency);
    }

    private long totalBytes() {
      long bytes = 0;
      for (long section : sectionBytes) {
        bytes = Math.addExact(bytes, section);
      }
      return bytes;
    }

    private String sections() {
      StringBuilder text = new StringBuilder();
      for (int section = 0; section < sectionNames.length; section++) {
        if (section > 0) {
          text.append(" + ");
        }
        text.append(sectionNames[section])
            .append(' ')
            .append(String.format(Locale.ROOT, "%,d", sectionBytes[section]))
            .append(" B");
      }
      return text.append(" = ")
          .append(String.format(Locale.ROOT, "%,d B", totalBytes()))
          .toString();
    }
  }

  /**
   * A deterministic synthetic corpus: per-document term postings with term frequencies drawn from a
   * Zipf vocabulary, so the index sees the same document frequency shape as a real corpus.
   */
  private static final class Corpus {
    private final int documents;
    private final String[] vocabulary;
    private final int[] termIds;
    private final int[] termFrequencies;
    private final int[] lengths;
    private final int[] documentFrequency;

    private Corpus(
        int documents,
        String[] vocabulary,
        int[] termIds,
        int[] termFrequencies,
        int[] lengths,
        int[] documentFrequency) {
      this.documents = documents;
      this.vocabulary = vocabulary;
      this.termIds = termIds;
      this.termFrequencies = termFrequencies;
      this.lengths = lengths;
      this.documentFrequency = documentFrequency;
    }

    private static Corpus generate(int documents) {
      Random random = new Random(SEED);
      String[] vocabulary = vocabulary(random);
      double[] rankWeights = new double[vocabulary.length];
      double[] cumulative = new double[vocabulary.length];
      double totalWeight = 0.0;
      for (int rank = 1; rank <= vocabulary.length; rank++) {
        rankWeights[rank - 1] = Math.pow(rank, -ZIPF_EXPONENT);
        totalWeight += rankWeights[rank - 1];
        cumulative[rank - 1] = totalWeight;
      }

      int[] termIds = new int[documents * POSTINGS_PER_DOCUMENT];
      int[] termFrequencies = new int[termIds.length];
      int[] lengths = new int[documents];
      int[] documentFrequency = new int[vocabulary.length];
      int[] seen = new int[vocabulary.length];
      double[] weights = new double[POSTINGS_PER_DOCUMENT];
      long[] ranking = new long[POSTINGS_PER_DOCUMENT];

      for (int document = 0; document < documents; document++) {
        int length = documentLength(random);
        lengths[document] = length;
        int offset = document * POSTINGS_PER_DOCUMENT;
        int stamp = document + 1;
        double weightSum = 0.0;
        for (int posting = 0; posting < POSTINGS_PER_DOCUMENT; posting++) {
          int term;
          do {
            term = sample(cumulative, totalWeight, random);
          } while (seen[term] == stamp);
          seen[term] = stamp;
          termIds[offset + posting] = term;
          documentFrequency[term]++;
          weights[posting] = rankWeights[term];
          weightSum += weights[posting];
        }
        assignTermFrequencies(offset, length, weights, weightSum, termFrequencies, ranking);
      }

      return new Corpus(
          documents, vocabulary, termIds, termFrequencies, lengths, documentFrequency);
    }

    /**
     * Splits a document length over its postings: every posting occurs at least once and the rest
     * of the tokens go to the frequent terms of the document, which reproduces the skewed term
     * frequency of real text.
     */
    private static void assignTermFrequencies(
        int offset,
        int length,
        double[] weights,
        double weightSum,
        int[] termFrequencies,
        long[] ranking) {
      int extra = length - POSTINGS_PER_DOCUMENT;
      int assigned = 0;
      for (int posting = 0; posting < POSTINGS_PER_DOCUMENT; posting++) {
        double exact = extra * weights[posting] / weightSum;
        int base = (int) exact;
        termFrequencies[offset + posting] = 1 + base;
        // the largest remainder decides which postings get the tokens that do not divide evenly
        ranking[posting] = (((long) ((exact - base) * (1L << 30))) << 8) | posting;
        assigned += base;
      }
      Arrays.sort(ranking);
      int remainder = extra - assigned;
      for (int selection = 0; selection < remainder; selection++) {
        int posting = (int) (ranking[POSTINGS_PER_DOCUMENT - 1 - selection] & 0xFF);
        termFrequencies[offset + posting]++;
      }
    }

    private static int documentLength(Random random) {
      int length =
          (int)
              Math.round(
                  Math.exp(
                      LOG_MEAN_DOCUMENT_TOKENS + DOCUMENT_LENGTH_SIGMA * random.nextGaussian()));
      return Math.max(POSTINGS_PER_DOCUMENT + 1, length);
    }

    private static int sample(double[] cumulative, double totalWeight, Random random) {
      int index = Arrays.binarySearch(cumulative, random.nextDouble() * totalWeight);
      return Math.min(index >= 0 ? index : -index - 1, cumulative.length - 1);
    }

    private static String[] vocabulary(Random random) {
      String[] vocabulary = new String[VOCABULARY_CANDIDATES];
      Set<String> used = Sets.newHashSetWithExpectedSize(VOCABULARY_CANDIDATES * 2);
      for (int term = 0; term < vocabulary.length; term++) {
        String word;
        do {
          word = randomWord(random);
        } while (!used.add(word));
        vocabulary[term] = word;
      }
      return vocabulary;
    }

    private static String randomWord(Random random) {
      int length =
          Math.max(
              2, (int) Math.round(MEAN_TERM_LENGTH + TERM_LENGTH_SIGMA * random.nextGaussian()));
      StringBuilder word = new StringBuilder(length);
      for (int character = 0; character < length; character++) {
        word.append(ALPHABET.charAt(random.nextInt(ALPHABET.length())));
      }
      return word.toString();
    }

    private IndexEntry entry(
        TextIndexAdapter adapter, int document, String dataFile, long rowPosition) {
      StringBuilder text = new StringBuilder(lengths[document] * (int) (MEAN_TERM_LENGTH + 1.0));
      int offset = document * POSTINGS_PER_DOCUMENT;
      for (int posting = 0; posting < POSTINGS_PER_DOCUMENT; posting++) {
        String term = vocabulary[termIds[offset + posting]];
        int occurrences = termFrequencies[offset + posting];
        for (int occurrence = 0; occurrence < occurrences; occurrence++) {
          text.append(term).append(' ');
        }
      }
      return IndexEntry.of(dataFile, rowPosition, adapter.encodeEntryValue(text.toString()));
    }

    private int documents() {
      return documents;
    }

    private int vocabularyTerms() {
      return vocabulary.length;
    }

    private String term(int index) {
      return vocabulary[index];
    }

    private int documentFrequency(int index) {
      return documentFrequency[index];
    }

    private int documentFrequencyOf(String term) {
      for (int index = 0; index < vocabulary.length; index++) {
        if (vocabulary[index].equals(term)) {
          return documentFrequency[index];
        }
      }
      return 0;
    }

    private long postings() {
      long postings = 0;
      for (int frequency : documentFrequency) {
        postings += frequency;
      }
      return postings;
    }

    private int observedTerms() {
      int observed = 0;
      for (int frequency : documentFrequency) {
        if (frequency > 0) {
          observed++;
        }
      }
      return observed;
    }

    private double meanLength() {
      long tokens = 0;
      for (int length : lengths) {
        tokens += length;
      }
      return documents == 0 ? 0.0 : (double) tokens / documents;
    }

    private double hapaxFraction() {
      int observed = 0;
      int hapax = 0;
      for (int frequency : documentFrequency) {
        if (frequency == 1) {
          hapax++;
        }
        if (frequency > 0) {
          observed++;
        }
      }
      return observed == 0 ? 0.0 : (double) hapax / observed;
    }

    /** One query per order of magnitude of document frequency, all of them distinct terms. */
    private List<String> queries() {
      List<String> queries = Lists.newArrayListWithCapacity(QUERY_COUNT);
      boolean[] used = new boolean[vocabulary.length];
      for (int query = 0; query < QUERY_COUNT; query++) {
        double fraction = Math.pow((double) documents, -query / (double) (QUERY_COUNT - 1));
        int target = (int) Math.max(1.0, Math.round(documents * fraction));
        int best = -1;
        for (int term = 0; term < documentFrequency.length; term++) {
          if (used[term] || documentFrequency[term] == 0) {
            continue;
          }
          if (best < 0
              || Math.abs(documentFrequency[term] - target)
                  < Math.abs(documentFrequency[best] - target)) {
            best = term;
          }
        }
        if (best >= 0) {
          used[best] = true;
          queries.add(vocabulary[best]);
        }
      }
      return queries;
    }

    private String describe() {
      long tokens = 0;
      for (int length : lengths) {
        tokens += length;
      }
      long postings = postings();
      int observed = observedTerms();
      int hapax = 0;
      int maxDocumentFrequency = 0;
      for (int frequency : documentFrequency) {
        if (frequency == 1) {
          hapax++;
        }
        maxDocumentFrequency = Math.max(maxDocumentFrequency, frequency);
      }
      long termCharacters = 0;
      for (String term : vocabulary) {
        termCharacters += term.length();
      }
      return String.format(
          Locale.ROOT,
          "corpus: %,d documents, %,d tokens (%.1f/document), %,d postings (%.1f/document),"
              + " %,d distinct terms, %.1f%% hapax, max df %,d, mean df %.2f, mean term length"
              + " %.2f characters%n",
          documents,
          tokens,
          (double) tokens / documents,
          postings,
          (double) postings / documents,
          observed,
          100.0 * hapax / observed,
          maxDocumentFrequency,
          observed == 0 ? 0.0 : (double) postings / observed,
          (double) termCharacters / vocabulary.length);
    }
  }
}
