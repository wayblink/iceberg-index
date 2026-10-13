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
package org.apache.iceberg.index.spark;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import org.apache.iceberg.Table;
import org.apache.iceberg.index.IndexAdapter;
import org.apache.iceberg.index.IndexBlob;
import org.apache.iceberg.index.IndexFile;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.IndexQuery;
import org.apache.iceberg.index.IndexReader;
import org.apache.iceberg.index.IndexSearchResult;
import org.apache.iceberg.index.IndexTypes;
import org.apache.iceberg.index.PuffinIndexFileContainer;
import org.apache.iceberg.inmemory.InMemoryInputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;

/**
 * Searches an index with the payloads read and searched on the executors, and the per-file results
 * merged by a distributed stage rather than on the driver.
 *
 * <p>The index search itself is cheap per index file: it scans the entries of one file, or
 * traverses one HNSW graph, and keeps only that file's top-k. What does not scale is doing that for
 * every file on one thread, because both the reads and the compute are sequential, and the payload
 * of a file has to be in memory at once. Reading the payloads as files and searching them per
 * partition moves both to the executors, where they are parallel and where a large payload only
 * occupies one task.
 *
 * <p>What does not scale either is merging the per-file results on the driver, because that is one
 * top-k per index file, so a query for a large top-k over many files brings every one of those
 * results to the driver to be sorted there. The merge is a bounded top-k instead: each task keeps
 * its own file's best hits, a tree reduce keeps the best of what the tasks produced, and only the
 * final top-k comes back. A file is never split, so an index file, which carries a graph or a
 * posting list of its own, always travels whole into one task.
 *
 * <p>Only the small part stays on the driver: the manifest list and the manifests, which name the
 * index files.
 *
 * <p>An index type whose search needs all payloads together, BM25 with its corpus statistics for
 * instance, cannot be searched file by file and is not supported here; those are searched on the
 * driver, where the payloads are merged before scoring. Such a search also cannot say whether more
 * hits exist than the top-k it returned, because it only ever saw the top-k.
 */
public final class DistributedIndexSearch {

  private static final String PATH_COLUMN = "path";
  private static final String CONTENT_COLUMN = "content";

  private DistributedIndexSearch() {}

  /**
   * The hits of a search, and whether the index holds more of them than the query asked for.
   *
   * <p>The latter is what lets a caller fill the holes delete files leave in a result: it asks for
   * one more hit than it needs, and when it gets one it knows there is another round to ask for.
   */
  public static final class SearchOutcome {
    private final List<IndexSearchResult> hits;
    private final boolean moreHits;

    private SearchOutcome(List<IndexSearchResult> hits, boolean moreHits) {
      this.hits = hits;
      this.moreHits = moreHits;
    }

    /** The hits of the whole index, best first, at most the query's top-k. */
    public List<IndexSearchResult> hits() {
      return hits;
    }

    /** Whether the index holds more hits than the ones returned. */
    public boolean moreHits() {
      return moreHits;
    }
  }

  /**
   * Searches the index snapshot a query is answered from, on the executors.
   *
   * @param spark the Spark session
   * @param metadata the index to search
   * @param table the table the index belongs to, used to resolve the snapshot and its file IO
   * @param query the query to answer
   * @return the hits of the whole index, best first, and whether the index holds more of them
   */
  public static SearchOutcome search(
      SparkSession spark, IndexMetadata metadata, Table table, IndexQuery query) {
    IndexAdapter adapter =
        IndexTypes.fromType(metadata.indexType()).createAdapter(metadata.definition());
    if (adapter.searchesAllPayloadsTogether()) {
      // the payloads have to be merged before any of them can be scored, and such a search only
      // ever sees its own top-k, so it cannot report whether more hits exist
      return new SearchOutcome(new IndexReader(metadata, table).search(query), false);
    }

    IndexReader reader = new IndexReader(metadata, table);
    List<IndexFile> files = reader.indexFiles(query);
    if (files.isEmpty()) {
      return new SearchOutcome(ImmutableList.of(), false);
    }

    // every index file of a snapshot carries the payload the adapter searches, so an executor knows
    // which blob of a file to take from the adapter alone
    List<String> locations = Lists.newArrayListWithCapacity(files.size());
    for (IndexFile file : files) {
      locations.add(file.location());
    }

    JavaSparkContext context = JavaSparkContext.fromSparkContext(spark.sparkContext());
    Broadcast<IndexAdapter> adapterRef = context.broadcast(adapter);
    // a ByteBuffer is not serializable, so the payload travels as bytes
    Broadcast<byte[]> queryRef = context.broadcast(toBytes(query.payload().duplicate()));

    // the limit is captured as a primitive: the query itself is not serializable. One more than the
    // query asks for is kept, because that is what says whether there is another round to ask for.
    int topK = query.topK();
    int localTopK = topK + 1;

    // whole files as rows: the binary file data source reads a whole file as one row, which is what
    // keeps the payload of an index file inside a single task; its own schema is fixed, so the
    // columns are selected after
    Dataset<Row> blobs =
        spark
            .read()
            .format("binaryFile")
            .load(locations.toArray(new String[0]))
            .select(PATH_COLUMN, CONTENT_COLUMN);

    List<Tuple3<String, Long, Double>> top =
        blobs
            .toJavaRDD()
            .map(row -> Tuple2.apply(row.getString(0), row.<byte[]>getAs(1)))
            .mapPartitions(
                (FlatMapFunction<Iterator<Tuple2<String, byte[]>>, Tuple3<String, Long, Double>>)
                    iterator -> {
                      List<Tuple3<String, Long, Double>> fileHits = Lists.newArrayList();
                      while (iterator.hasNext()) {
                        Tuple2<String, byte[]> blob = iterator.next();
                        for (IndexSearchResult hit :
                            searchFile(
                                blob._2(),
                                adapterRef.getValue(),
                                ByteBuffer.wrap(queryRef.getValue()),
                                localTopK)) {
                          fileHits.add(
                              new Tuple3<>(hit.dataFile(), hit.rowPosition(), hit.score()));
                        }
                      }
                      return fileHits.iterator();
                    })
            .takeOrdered(localTopK, hitOrder(adapter));

    List<IndexSearchResult> hits = Lists.newArrayListWithCapacity(top.size());
    for (Tuple3<String, Long, Double> hit : top) {
      hits.add(IndexSearchResult.of(hit._1(), hit._2(), hit._3()));
    }

    boolean moreHits = hits.size() > topK;
    if (moreHits) {
      hits = Lists.newArrayList(hits.subList(0, topK));
    }

    return new SearchOutcome(ImmutableList.copyOf(hits), moreHits);
  }

  /**
   * Orders hits the way the adapter orders its results, for the merge of the per-file results.
   *
   * <p>The order of an adapter is a function of the score and of the data file and row position
   * that break ties, and all three travel with a hit, so the direction of the score is all that has
   * to be learned from the adapter: a distance sorts the smallest score first, a similarity the
   * largest.
   */
  private static Comparator<Tuple3<String, Long, Double>> hitOrder(IndexAdapter adapter) {
    // two results that differ only in their score, so the tie breaker has nothing to say and
    // the comparator can only order them by the direction of the score
    IndexSearchResult higher = IndexSearchResult.of("a", 0L, 1.0);
    IndexSearchResult lower = IndexSearchResult.of("a", 0L, 0.0);
    boolean higherScoreFirst = adapter.resultComparator().compare(higher, lower) < 0;
    return (Comparator<Tuple3<String, Long, Double>> & Serializable)
        (left, right) -> {
          int byScore = Double.compare(left._3(), right._3());
          int primary = higherScoreFirst ? -byScore : byScore;
          if (primary != 0) {
            return primary;
          }
          int byFile = left._1().compareTo(right._1());
          if (byFile != 0) {
            return byFile;
          }
          return Long.compare(left._2(), right._2());
        };
  }

  private static byte[] toBytes(ByteBuffer buffer) {
    byte[] bytes = new byte[buffer.remaining()];
    buffer.duplicate().get(bytes);
    return bytes;
  }

  /** Searches one index file, which an executor has read whole. */
  private static List<IndexSearchResult> searchFile(
      byte[] content, IndexAdapter adapter, ByteBuffer query, int topK) {
    String blobType =
        adapter.writesAuxiliaryFiles() ? adapter.auxiliaryBlobType() : adapter.entryBlobType();

    List<IndexBlob> blobs;
    try {
      blobs = new PuffinIndexFileContainer().read(new InMemoryInputFile(content));
    } catch (java.io.IOException e) {
      throw new java.io.UncheckedIOException("Failed to read an index file", e);
    }

    for (IndexBlob blob : blobs) {
      if (!blobType.equals(blob.type())) {
        continue;
      }

      if (adapter.writesAuxiliaryFiles()) {
        return adapter.searchAuxiliary(ByteBuffer.wrap(blob.payload()), query, topK);
      }
      return adapter.search(ByteBuffer.wrap(blob.payload()), query, topK);
    }

    return ImmutableList.of();
  }
}
