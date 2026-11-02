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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.Table;
import org.apache.iceberg.index.IndexAdapter;
import org.apache.iceberg.index.IndexBlob;
import org.apache.iceberg.index.IndexFile;
import org.apache.iceberg.index.IndexFileMemory;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.IndexQuery;
import org.apache.iceberg.index.IndexReader;
import org.apache.iceberg.index.IndexSearchResult;
import org.apache.iceberg.index.IndexTypes;
import org.apache.iceberg.index.PuffinIndexFileContainer;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

  private static final Logger LOG = LoggerFactory.getLogger(DistributedIndexSearch.class);

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
    // A search is what makes an index hot, so this is where the LRU learns about it.
    IndexCacheControl.recordAccess(spark, metadata.indexUuid());
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
    // the file IO travels whole, so an executor reads a payload the way the table is read, with the
    // properties of its catalog rather than with whatever the session happens to be configured with
    Broadcast<FileIO> ioRef = context.broadcast(table.io());
    // whether an executor remembers index files between queries, and how much it may keep
    long workingSet = workingSetBytes(table.io(), files, adapter, table.location());
    Broadcast<ExecutorIndexCache.Config> cacheRef =
        context.broadcast(ExecutorIndexCache.Config.from(spark, workingSet, table.location()));
    String cacheKey = adapterKey(adapter);
    List<String> cacheKeys = cacheKeys(table.location(), cacheKey, locations);
    // what each executor holds, so a task can be sent to one that already has its file
    IndexCacheLocations locationsTable = IndexCacheLocations.forContext(context.sc());
    boolean locality = localityEnabled(spark);
    // captured by the task directly: an accumulator only reports through the copy it was created
    // from, so a broadcast copy of it would update nothing
    org.apache.spark.util.CollectionAccumulator<IndexCacheLocations.Report> cacheReports =
        cacheReports(context, cacheRef.getValue());
    // identifies this search, so its tasks report this executor's cache once instead of once each
    String queryRun = java.util.UUID.randomUUID().toString();

    // the limit is captured as a primitive: the query itself is not serializable. One more than the
    // query asks for is kept, because that is what says whether there is another round to ask for.
    int topK = query.topK();
    int localTopK = topK + 1;

    // one index file per partition, because an index file is the unit of work: it carries a graph
    // or
    // a posting list of its own, so it is read whole and searched as a whole, and the number of
    // partitions is the number of index files rather than whatever a byte budget would make of them
    List<Tuple3<String, Long, Double>> top =
        IndexFileRdds.javaRdd(context.sc(), locations, cacheKeys, locationsTable, locality)
            .mapPartitions(
                (FlatMapFunction<Iterator<String>, Tuple3<String, Long, Double>>)
                    locations1 -> {
                      IndexAdapter fileAdapter = adapterRef.getValue();
                      ByteBuffer fileQuery = ByteBuffer.wrap(queryRef.getValue());
                      FileIO fileIO = ioRef.getValue();
                      ExecutorIndexCache.Config cacheConfig = cacheRef.getValue();
                      List<Tuple3<String, Long, Double>> fileHits = Lists.newArrayList();
                      while (locations1.hasNext()) {
                        String location = locations1.next();
                        long readStart = System.nanoTime();
                        ExecutorIndexCache cache = ExecutorIndexCache.get();
                        String managedKey = cacheKey(cacheConfig.owner(), cacheKey, location);
                        // A file residency management owns is leased, so a release waits for this
                        // query; anything else keeps the old lazy behaviour.
                        ExecutorIndexCache.Lease lease = cache.acquireManagedKey(managedKey);
                        ExecutorIndexCache.CachedFile file =
                            lease != null
                                ? lease.file()
                                : cache.file(
                                    location,
                                    cacheConfig,
                                    cacheKey,
                                    sizeHint(fileIO, location),
                                    () -> loadFile(fileIO, location, fileAdapter));
                        long readNanos = System.nanoTime() - readStart;
                        long searchStart = System.nanoTime();
                        try {
                          for (IndexSearchResult hit :
                              file.searching(
                                  prepared -> prepared.search(fileQuery.duplicate(), localTopK))) {
                            fileHits.add(
                                new Tuple3<>(hit.dataFile(), hit.rowPosition(), hit.score()));
                          }
                        } finally {
                          if (lease != null) {
                            lease.close();
                          }
                        }
                        LOG.debug(
                            "index file {} read {} ms, search {} ms",
                            location.substring(location.lastIndexOf('/') + 1),
                            TimeUnit.MILLISECONDS.convert(readNanos, TimeUnit.NANOSECONDS),
                            TimeUnit.MILLISECONDS.convert(
                                System.nanoTime() - searchStart, TimeUnit.NANOSECONDS));
                      }
                      reportCache(cacheReports, queryRun);
                      return fileHits.iterator();
                    })
            .takeOrdered(localTopK, hitOrder(adapter));
    mergeCacheReports(cacheReports, locationsTable, cacheKeys, cacheRef.getValue(), workingSet);

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
   * Searches the index snapshot a batch of queries is answered from, loading each index file once.
   *
   * <p>A query on its own reads and decodes every index file of the snapshot, so N queries cost N
   * times that work. This searches each file once for the whole batch: the payload is read once,
   * prepared once, and then scored once per query, so the batch pays one load and N searches.
   *
   * @param spark the Spark session
   * @param metadata the index to search
   * @param table the table the index belongs to, used to resolve the snapshot and its file IO
   * @param queries the queries to answer; all are answered from the snapshot of the first one
   * @return the hits of each query, best first, in the order the queries were given
   */
  public static List<List<IndexSearchResult>> searchBatch(
      SparkSession spark, IndexMetadata metadata, Table table, List<IndexQuery> queries) {
    if (queries.isEmpty()) {
      return ImmutableList.of();
    }

    IndexCacheControl.recordAccess(spark, metadata.indexUuid());
    IndexAdapter adapter =
        IndexTypes.fromType(metadata.indexType()).createAdapter(metadata.definition());
    if (adapter.searchesAllPayloadsTogether()) {
      // each query needs the merged payloads, and such a search has no per-file form to batch
      List<List<IndexSearchResult>> perQuery = Lists.newArrayListWithCapacity(queries.size());
      IndexReader togetherReader = new IndexReader(metadata, table);
      for (IndexQuery query : queries) {
        perQuery.add(togetherReader.search(query));
      }
      return perQuery;
    }

    IndexReader reader = new IndexReader(metadata, table);
    List<IndexFile> files = reader.indexFiles(queries.get(0));
    List<List<IndexSearchResult>> perQuery = Lists.newArrayListWithCapacity(queries.size());
    if (files.isEmpty()) {
      for (int i = 0; i < queries.size(); i++) {
        perQuery.add(ImmutableList.of());
      }
      return perQuery;
    }

    List<String> locations = Lists.newArrayListWithCapacity(files.size());
    for (IndexFile file : files) {
      locations.add(file.location());
    }

    JavaSparkContext context = JavaSparkContext.fromSparkContext(spark.sparkContext());
    Broadcast<IndexAdapter> adapterRef = context.broadcast(adapter);
    // a ByteBuffer is not serializable, so the payloads travel as bytes
    List<byte[]> queryPayloads = Lists.newArrayListWithCapacity(queries.size());
    int widestTopK = 0;
    for (IndexQuery query : queries) {
      queryPayloads.add(toBytes(query.payload().duplicate()));
      widestTopK = Math.max(widestTopK, query.topK());
    }
    Broadcast<List<byte[]>> queriesRef = context.broadcast(queryPayloads);
    Broadcast<FileIO> ioRef = context.broadcast(table.io());
    long workingSet = workingSetBytes(table.io(), files, adapter, table.location());
    Broadcast<ExecutorIndexCache.Config> cacheRef =
        context.broadcast(ExecutorIndexCache.Config.from(spark, workingSet, table.location()));
    String cacheKey = adapterKey(adapter);
    List<String> cacheKeys = cacheKeys(table.location(), cacheKey, locations);
    IndexCacheLocations locationsTable = IndexCacheLocations.forContext(context.sc());
    boolean locality = localityEnabled(spark);
    // captured by the task directly: an accumulator only reports through the copy it was created
    // from, so a broadcast copy of it would update nothing
    org.apache.spark.util.CollectionAccumulator<IndexCacheLocations.Report> cacheReports =
        cacheReports(context, cacheRef.getValue());
    // identifies this search, so its tasks report this executor's cache once instead of once each
    String queryRun = java.util.UUID.randomUUID().toString();

    // one more than the widest query asks for is kept per query, because that is what says whether
    // there is another round to ask for
    int localTopK = widestTopK + 1;

    JavaPairRDD<Integer, Tuple3<String, Long, Double>> fileHits =
        IndexFileRdds.javaRdd(context.sc(), locations, cacheKeys, locationsTable, locality)
            .mapPartitionsToPair(
                (PairFlatMapFunction<Iterator<String>, Integer, Tuple3<String, Long, Double>>)
                    locations1 -> {
                      IndexAdapter fileAdapter = adapterRef.getValue();
                      List<byte[]> fileQueries = queriesRef.getValue();
                      FileIO fileIO = ioRef.getValue();
                      ExecutorIndexCache.Config cacheConfig = cacheRef.getValue();
                      List<Tuple2<Integer, Tuple3<String, Long, Double>>> batchHits =
                          Lists.newArrayList();
                      while (locations1.hasNext()) {
                        String location = locations1.next();
                        long readStart = System.nanoTime();
                        ExecutorIndexCache cache = ExecutorIndexCache.get();
                        String managedKey = cacheKey(cacheConfig.owner(), cacheKey, location);
                        ExecutorIndexCache.Lease lease = cache.acquireManagedKey(managedKey);
                        ExecutorIndexCache.CachedFile file =
                            lease != null
                                ? lease.file()
                                : cache.file(
                                    location,
                                    cacheConfig,
                                    cacheKey,
                                    sizeHint(fileIO, location),
                                    () -> loadFile(fileIO, location, fileAdapter));
                        long readNanos = System.nanoTime() - readStart;

                        long searchStart = System.nanoTime();
                        try {
                          file.searching(
                              prepared -> {
                                for (int queryId = 0; queryId < fileQueries.size(); queryId++) {
                                  for (IndexSearchResult hit :
                                      prepared.search(
                                          ByteBuffer.wrap(fileQueries.get(queryId)), localTopK)) {
                                    batchHits.add(
                                        new Tuple2<>(
                                            queryId,
                                            new Tuple3<>(
                                                hit.dataFile(), hit.rowPosition(), hit.score())));
                                  }
                                }
                                return null;
                              });
                        } finally {
                          if (lease != null) {
                            lease.close();
                          }
                        }

                        LOG.debug(
                            "index file {} read {} ms, batch search of {} queries {} ms",
                            location.substring(location.lastIndexOf('/') + 1),
                            TimeUnit.MILLISECONDS.convert(readNanos, TimeUnit.NANOSECONDS),
                            fileQueries.size(),
                            TimeUnit.MILLISECONDS.convert(
                                System.nanoTime() - searchStart, TimeUnit.NANOSECONDS));
                      }
                      reportCache(cacheReports, queryRun);
                      return batchHits.iterator();
                    });

    // the per-query top-k is merged with a bounded accumulator, so the shuffle carries at most one
    // entry per (query, index file) instead of every hit
    Comparator<Tuple3<String, Long, Double>> order = hitOrder(adapter);
    int mergeLimit = localTopK;
    Function2<
            List<Tuple3<String, Long, Double>>,
            Tuple3<String, Long, Double>,
            List<Tuple3<String, Long, Double>>>
        mergeHit =
            (hits, hit) -> {
              hits.add(hit);
              return bounded(hits, order, mergeLimit);
            };
    Function2<
            List<Tuple3<String, Long, Double>>,
            List<Tuple3<String, Long, Double>>,
            List<Tuple3<String, Long, Double>>>
        mergeHits =
            (left, right) -> {
              left.addAll(right);
              return bounded(left, order, mergeLimit);
            };
    Map<Integer, List<Tuple3<String, Long, Double>>> topByQuery =
        fileHits
            .aggregateByKey(Lists.<Tuple3<String, Long, Double>>newArrayList(), mergeHit, mergeHits)
            .collectAsMap();
    mergeCacheReports(cacheReports, locationsTable, cacheKeys, cacheRef.getValue(), workingSet);

    for (int queryId = 0; queryId < queries.size(); queryId++) {
      List<Tuple3<String, Long, Double>> top =
          topByQuery.getOrDefault(queryId, Lists.newArrayList());
      top = bounded(top, order, queries.get(queryId).topK());
      List<IndexSearchResult> hits = Lists.newArrayListWithCapacity(top.size());
      for (Tuple3<String, Long, Double> hit : top) {
        hits.add(IndexSearchResult.of(hit._1(), hit._2(), hit._3()));
      }
      perQuery.add(ImmutableList.copyOf(hits));
    }

    return perQuery;
  }

  /** Keeps the best {@code limit} hits of a query and drops the rest. */
  private static List<Tuple3<String, Long, Double>> bounded(
      List<Tuple3<String, Long, Double>> hits,
      Comparator<Tuple3<String, Long, Double>> order,
      int limit) {
    if (hits.size() <= limit) {
      return hits;
    }
    List<Tuple3<String, Long, Double>> sorted = Lists.newArrayList(hits);
    sorted.sort(order);
    return Lists.newArrayList(sorted.subList(0, limit));
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

  /**
   * Collects what each executor's cache holds, when asked to.
   *
   * <p>The counters belong to the executor, so they travel back through an accumulator: an
   * executor's own output is in a pod log that a run may not keep, and its logger is often above
   * the level a counter line is written at.
   */
  private static org.apache.spark.util.CollectionAccumulator<IndexCacheLocations.Report>
      cacheReports(JavaSparkContext context, ExecutorIndexCache.Config config) {
    // reported whenever the cache is on: a run that turned it on is a run that wants to know
    // whether it did anything
    return config.enabled() ? context.sc().collectionAccumulator() : null;
  }

  /**
   * Sends this executor's view of its cache back, so the driver's location table stays current.
   *
   * <p>Called from every task of a query, but only the first task of one query on one executor
   * reports: the view is the same on all of them, and a query over a thousand index files would
   * otherwise push a thousand copies of it through the accumulator.
   */
  static void reportCache(
      org.apache.spark.util.CollectionAccumulator<IndexCacheLocations.Report> reports,
      String queryId) {
    if (reports == null) {
      return;
    }

    String executor = "unknown";
    String host = "unknown";
    try {
      if (org.apache.spark.SparkEnv.get() != null) {
        executor = org.apache.spark.SparkEnv.get().executorId();
        host = org.apache.spark.SparkEnv.get().blockManager().blockManagerId().host();
      }
    } catch (RuntimeException e) {
      // a host name is only used to place a task, so an unknown one falls back to no hint
      host = "unknown";
    }

    IndexCacheLocations.Report report =
        ExecutorIndexCache.get().reportOnce(queryId, executor, host);
    if (report != null) {
      reports.add(report);
    }
  }

  /** Folds the executors' reports into the location table and says what the query had cached. */
  private static void mergeCacheReports(
      org.apache.spark.util.CollectionAccumulator<IndexCacheLocations.Report> reports,
      IndexCacheLocations table,
      List<String> cacheKeys,
      ExecutorIndexCache.Config config,
      long workingSetBytes) {
    if (reports == null) {
      return;
    }

    table.mergeAll(reports.value());
    table.prune();
    IndexCacheLocations.Summary summary = table.summary(cacheKeys);
    table.recordQuery(config.mode().name(), workingSetBytes, summary.coverage());
    // the driver's logger is often above INFO, and this line is how a run says whether the cache
    // did anything, so it goes to standard out
    System.out.println("INDEX_CACHE mode=" + config.mode() + " this query: " + summary);

    if (summary.coverage() < 1.0 && config.mode() == ExecutorIndexCache.Mode.PIN_ALL) {
      LOG.info(
          "index cache holds {} of the {} files this query read; the rest will be read again",
          Math.round(summary.coverage() * cacheKeys.size()),
          cacheKeys.size());
    }
  }

  /**
   * The keys the cache stores an index file under, which is how a covered file is counted.
   *
   * <p>The key is the owner (the table location), the adapter identity, and the file. Residency
   * management plans its targets in exactly this shape, so a query has to look up the same string;
   * the plan and the query both build it here.
   */
  static String cacheKey(String owner, String adapterKey, String location) {
    return owner + "|" + adapterKey + "|" + location;
  }

  private static List<String> cacheKeys(String owner, String adapterKey, List<String> locations) {
    List<String> keys = Lists.newArrayListWithCapacity(locations.size());
    for (String location : locations) {
      keys.add(cacheKey(owner, adapterKey, location));
    }

    return keys;
  }

  private static boolean localityEnabled(SparkSession spark) {
    return Boolean.parseBoolean(spark.conf().get("spark.iceberg.index.schedule.locality", "true"));
  }

  /**
   * The retained-state estimate is only known under the table that owns the cache keys, so the
   * owner-less form is always unknown (-1). Use {@link #workingSetBytes(FileIO, List, IndexAdapter,
   * String)} with the table location.
   */
  static long workingSetBytes(FileIO io, List<IndexFile> files, IndexAdapter adapter) {
    return -1;
  }

  /**
   * The prepared-state bytes of one index file under this reader and runtime, or -1 when unknown.
   *
   * <p>A manifest record matching the current model, reader and runtime is trusted without a read;
   * otherwise the file is read and prepared once to re-estimate it. The value is the retained
   * prepared state P, not the container or encoded payload lengths, which are diagnostic only.
   */
  /**
   * Reports an unknown working set with its reason, once per reason.
   *
   * <p>An unknown working set is not a query failure: the cache falls back to its smallest budget,
   * which keeps it from claiming the query fits. What it does mean is that repeated queries will
   * not be served from the cache, and that is worth one line rather than a debug message per query.
   */
  private static long unknownWorkingSet(String reason) {
    if (WORKING_SET_REASONS.add(reason)) {
      LOG.warn(
          "The index cache working set is unknown because {}; a derived budget cannot size this"
              + " query and the cache keeps only its minimum. Raise the reporting if this table is"
              + " meant to be served from the cache.",
          reason);
    }

    return -1;
  }

  private static final java.util.Set<String> WORKING_SET_REASONS = ConcurrentHashMap.newKeySet();

  static long preparedBytesOf(FileIO io, IndexFile file, IndexAdapter adapter) {
    IndexFileMemory memory = IndexCacheMemory.resolveMemory(io, file, adapter);
    return memory == null ? -1 : memory.preparedBytes();
  }

  static long workingSetBytes(
      FileIO io, List<IndexFile> files, IndexAdapter adapter, String owner) {
    if (owner == null || owner.isBlank()) {
      return unknownWorkingSet("the table has no location to own the entries");
    }

    if (IndexCacheMemory.currentDescriptor(adapter) == null) {
      return unknownWorkingSet(
          "reader " + adapter.getClass().getName() + " has no retained-state descriptor");
    }

    List<IndexCacheProtocol.File> planned = new ArrayList<>();
    for (IndexFile file : files) {
      IndexFileMemory memory = IndexCacheMemory.resolveMemory(io, file, adapter);
      if (memory == null) {
        return unknownWorkingSet("the retained size of one of the files is unknown");
      }
      planned.add(
          IndexCacheProtocol.File.builder()
              .location(file.location())
              .cacheKey(cacheKey(owner, adapterKey(adapter), file.location()))
              .owner(owner)
              .memory(memory)
              .estimatedBytes(IndexCacheMemory.residentBytes(memory))
              .build());
    }
    try {
      return ExecutorIndexCache.Shape.empty().project(null, 0, planned).charge().usedBytes();
    } catch (RuntimeException e) {
      return -1;
    }
  }

  static final class LoadedFile {
    private final ExecutorIndexCache.CachedFile file;
    private final long fileBytes;
    private final long payloadBytes;
    private final long preparedBytes;

    LoadedFile(
        ExecutorIndexCache.CachedFile file, long fileBytes, long payloadBytes, long preparedBytes) {
      this.file = file;
      this.fileBytes = fileBytes;
      this.payloadBytes = payloadBytes;
      this.preparedBytes = preparedBytes;
    }

    ExecutorIndexCache.CachedFile file() {
      return file;
    }

    long fileBytes() {
      return fileBytes;
    }

    long payloadBytes() {
      return payloadBytes;
    }

    long preparedBytes() {
      return preparedBytes;
    }
  }

  private static final class PreparedFile {
    private final IndexAdapter.PreparedSearch searcher;
    private final long payloadBytes;

    private PreparedFile(IndexAdapter.PreparedSearch searcher, long payloadBytes) {
      this.searcher = searcher;
      this.payloadBytes = payloadBytes;
    }

    IndexAdapter.PreparedSearch searcher() {
      return searcher;
    }

    long payloadBytes() {
      return payloadBytes;
    }
  }

  static ExecutorIndexCache.CachedFile loadFile(FileIO io, String location, IndexAdapter adapter) {
    return loadFileState(io, location, adapter).file();
  }

  static LoadedFile loadFileState(FileIO io, String location, IndexAdapter adapter) {
    InputFile input = io.newInputFile(location);
    long length = input.getLength();
    if (length > IndexPayloadLimits.MAX_PAYLOAD_BYTES) {
      throw new IllegalArgumentException(IndexPayloadLimits.tooLargeToRead(length, location));
    }
    PreparedFile prepared = prepareFile(input, adapter);
    ExecutorIndexCache.CachedFile file =
        ExecutorIndexCache.loaded(prepared.payloadBytes(), prepared.searcher())
            .withContext(adapter);
    return new LoadedFile(file, length, prepared.payloadBytes(), file.preparedBytes());
  }

  /**
   * The length of a file, which a load reserves room against before it reads anything.
   *
   * <p>It is a lower bound of what the prepared structure costs, and it is free to ask for. Without
   * it a load would have to reserve the whole budget, which would let one load hold the cache while
   * it prepares and serialise the loads of a single query.
   */
  private static long sizeHint(FileIO io, String location) {
    try {
      return io.newInputFile(location).getLength();
    } catch (RuntimeException e) {
      // a length is a hint for sizing, never a reason to fail a read that will report the error
      return 0L;
    }
  }

  /** Reads the payload of one index file, which an executor reads whole. */
  private static byte[] readPayload(FileIO io, String location) {
    InputFile file = io.newInputFile(location);
    long length = file.getLength();
    if (length > IndexPayloadLimits.MAX_PAYLOAD_BYTES) {
      throw new IllegalArgumentException(IndexPayloadLimits.tooLargeToRead(length, location));
    }

    byte[] content = new byte[(int) length];
    try (java.io.InputStream in = file.newStream()) {
      IOUtil.readFully(in, content, 0, content.length);
    } catch (java.io.IOException e) {
      throw new java.io.UncheckedIOException("Failed to read index file " + location, e);
    }
    return content;
  }

  /** Prepares one index file, which an executor has read whole, for any number of searches. */
  private static PreparedFile prepareFile(InputFile input, IndexAdapter adapter) {
    // every type the adapter can still read, not just the one it writes: an index built before a
    // rename is still on tables, and a reader that only knew the current name would skip it
    java.util.Set<String> blobTypes =
        adapter.writesAuxiliaryFiles()
            ? adapter.readableAuxiliaryBlobTypes()
            : adapter.readableEntryBlobTypes();

    List<IndexBlob> blobs;
    try {
      blobs = new PuffinIndexFileContainer().read(input);
    } catch (java.io.IOException e) {
      throw new java.io.UncheckedIOException("Failed to read an index file", e);
    }

    for (IndexBlob blob : blobs) {
      if (!blobTypes.contains(blob.type())) {
        continue;
      }

      ByteBuffer payload = ByteBuffer.wrap(blob.payload());
      long payloadBytes = payload.remaining();
      try {
        IndexAdapter.PreparedSearch searcher =
            adapter.writesAuxiliaryFiles()
                ? adapter.prepareAuxiliary(payload)
                : adapter.prepare(payload);
        return new PreparedFile(searcher, payloadBytes);
      } catch (ClassCastException | IllegalArgumentException e) {
        // a payload an index was written with can stop being readable when the entries it holds
        // change shape; that is a rebuild, and the message has to say so rather than look like a
        // cache or scheduling problem
        throw new IllegalStateException(
            "Cannot read the "
                + blob.type()
                + " payload of an index file: it was written by a version whose payload this "
                + "build does not read. Rebuild the index.",
            e);
      }
    }

    // a file the adapter cannot read is not an empty result: saying so is the difference between
    // "this index has nothing" and "this build cannot read that index", and the second one is what
    // a payload written by another version looks like
    List<String> present = Lists.newArrayList();
    for (IndexBlob blob : blobs) {
      present.add(blob.type());
    }

    throw new IllegalStateException("Index file has no " + blobTypes + " blob, only " + present);
  }

  /**
   * What a prepared index file depends on besides its payload: the adapter reading it, and which
   * blob of the file it takes. Two definitions that read different payload formats produce
   * different payloads, so they do not need to be told apart here.
   */
  static String adapterKey(IndexAdapter adapter) {
    return adapter.getClass().getName()
        + "|"
        + (adapter.writesAuxiliaryFiles() ? adapter.auxiliaryBlobType() : adapter.entryBlobType())
        + ":"
        + IndexCacheMemory.readerKey(adapter);
  }
}
