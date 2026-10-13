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

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import org.apache.iceberg.Table;
import org.apache.iceberg.index.IndexAdapter;
import org.apache.iceberg.index.IndexFile;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.IndexQuery;
import org.apache.iceberg.index.IndexReader;
import org.apache.iceberg.index.IndexSearchResult;
import org.apache.iceberg.index.IndexTypes;
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
 * Searches an index with the payloads read and searched on the executors.
 *
 * <p>The index search itself is cheap per index file: it scans the entries of one file, or
 * traverses one HNSW graph, and keeps only that file's top-k. What does not scale is doing that for
 * every file on one thread, because both the reads and the compute are sequential, and the payload
 * of a file has to be in memory at once. Reading the payloads as files and searching them per
 * partition moves both to the executors, where they are parallel and where a large payload only
 * occupies one task.
 *
 * <p>Only the small part stays on the driver: the manifest list and the manifests, which name the
 * index files, and the merge of the per-file results, which is one top-k per file.
 *
 * <p>An index type whose search needs all payloads together, BM25 with its corpus statistics for
 * instance, cannot be searched file by file and is not supported here; those are searched on the
 * driver, where the payloads are merged before scoring.
 */
public final class DistributedIndexSearch {

  private static final String PATH_COLUMN = "path";
  private static final String CONTENT_COLUMN = "content";

  private DistributedIndexSearch() {}

  /**
   * Searches the index snapshot a query is answered from, on the executors.
   *
   * @param spark the Spark session
   * @param metadata the index to search
   * @param table the table the index belongs to, used to resolve the snapshot and its file IO
   * @param query the query to answer
   * @return the hits of the whole index, best first, at most the query's top-k
   */
  public static List<IndexSearchResult> search(
      SparkSession spark, IndexMetadata metadata, Table table, IndexQuery query) {
    IndexAdapter adapter =
        IndexTypes.fromType(metadata.indexType()).createAdapter(metadata.definition());
    if (adapter.searchesAllPayloadsTogether()) {
      // the payloads have to be merged before any of them can be scored
      return new IndexReader(metadata, table).search(query);
    }

    List<IndexFile> files = new IndexReader(metadata, table).indexFiles(query);
    if (files.isEmpty()) {
      return ImmutableList.of();
    }

    // every index file of a snapshot carries the payload the adapter searches, so an executor knows
    // which blob of a file to take from the adapter alone
    List<String> locations = Lists.newArrayListWithCapacity(files.size());
    for (IndexFile file : files) {
      locations.add(file.location());
    }

    Broadcast<IndexAdapter> adapterRef =
        JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(adapter);
    // a ByteBuffer is not serializable, so the payload travels as bytes
    Broadcast<byte[]> queryRef =
        JavaSparkContext.fromSparkContext(spark.sparkContext())
            .broadcast(toBytes(query.payload().duplicate()));

    // the limit is captured as a primitive: the query itself is not serializable
    int topK = query.topK();

    // whole files as rows: one index file is one row, which is what keeps a payload in one task
    // the binary file data source reads a whole file as one row, which is what keeps the payload of
    // an index file inside a single task; its own schema is fixed, so the columns are selected
    // after
    Dataset<Row> blobs =
        spark
            .read()
            .format("binaryFile")
            .load(locations.toArray(new String[0]))
            .select(PATH_COLUMN, CONTENT_COLUMN);
    List<Tuple3<String, Long, Double>> triples =
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
                                topK)) {
                          fileHits.add(
                              new Tuple3<>(hit.dataFile(), hit.rowPosition(), hit.score()));
                        }
                      }
                      return fileHits.iterator();
                    })
            .collect();

    List<IndexSearchResult> hits = Lists.newArrayListWithCapacity(triples.size());
    for (Tuple3<String, Long, Double> triple : triples) {
      hits.add(IndexSearchResult.of(triple._1(), triple._2(), triple._3()));
    }

    hits.sort(adapter.resultComparator());
    if (hits.size() > query.topK()) {
      hits = Lists.newArrayList(hits.subList(0, query.topK()));
    }
    return ImmutableList.copyOf(hits);
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

    List<org.apache.iceberg.index.IndexBlob> blobs;
    try {
      blobs =
          new org.apache.iceberg.index.PuffinIndexFileContainer()
              .read(new InMemoryInputFile(content));
    } catch (java.io.IOException e) {
      throw new java.io.UncheckedIOException("Failed to read an index file", e);
    }

    for (org.apache.iceberg.index.IndexBlob blob : blobs) {
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
