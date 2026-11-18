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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.encryption.EncryptingFileIO;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.InclusiveMetricsEvaluator;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.hadoop.SerializableConfiguration;
import org.apache.iceberg.index.IndexAdapter;
import org.apache.iceberg.index.IndexAdapter.PreparedSearch;
import org.apache.iceberg.index.IndexBlob;
import org.apache.iceberg.index.IndexCandidates;
import org.apache.iceberg.index.IndexFile;
import org.apache.iceberg.index.IndexFileMemory;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.IndexQuery;
import org.apache.iceberg.index.IndexReader;
import org.apache.iceberg.index.IndexSearchResult;
import org.apache.iceberg.index.IndexTypes;
import org.apache.iceberg.index.PuffinIndexFileContainer;
import org.apache.iceberg.index.text.TextIndexAdapter;
import org.apache.iceberg.index.text.TextIndexAdapter.CorpusCounts;
import org.apache.iceberg.index.text.TextIndexAdapter.CorpusStatistics;
import org.apache.iceberg.index.text.TextIndexDefinition;
import org.apache.iceberg.index.vector.VectorIndexAdapter;
import org.apache.iceberg.index.vector.VectorIndexDefinition;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.metric.SQLMetric;
import org.apache.spark.sql.types.StructType;
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
   * What a query's tasks counted while they searched.
   *
   * <p>The rows never reach the driver, so neither do the counts that describe them: each task adds
   * to these as it goes, and the caller reads them once the query has run. Anything not handed a
   * metric is simply not reported, which keeps the fused search usable from a test.
   */
  public static final class RowCounters implements Serializable {
    private static final long serialVersionUID = 1L;

    private final SQLMetric requested;
    private final SQLMetric lastRequest;
    private final SQLMetric live;
    private final SQLMetric fetched;
    private final SQLMetric unresolved;
    private final SQLMetric outsideSnapshot;
    private final SQLMetric files;
    private final SQLMetric rounds;
    private final SQLMetric underfilled;
    private final SQLMetric scanTasks;

    public RowCounters(
        SQLMetric requested,
        SQLMetric lastRequest,
        SQLMetric live,
        SQLMetric fetched,
        SQLMetric unresolved,
        SQLMetric outsideSnapshot,
        SQLMetric files,
        SQLMetric rounds,
        SQLMetric underfilled,
        SQLMetric scanTasks) {
      this.requested = requested;
      this.lastRequest = lastRequest;
      this.live = live;
      this.fetched = fetched;
      this.unresolved = unresolved;
      this.outsideSnapshot = outsideSnapshot;
      this.files = files;
      this.rounds = rounds;
      this.underfilled = underfilled;
      this.scanTasks = scanTasks;
    }

    /** Adds what one index file's task found and what it cost. */
    void report(
        int opening,
        int last,
        int liveRows,
        int fetchedRows,
        int unresolvedHits,
        int outsideHits,
        int filesRead,
        int roundsRun,
        int scanTasksShipped,
        boolean shortOfTopK) {
      add(requested, opening);
      add(lastRequest, last);
      add(live, liveRows);
      add(fetched, fetchedRows);
      add(unresolved, unresolvedHits);
      add(outsideSnapshot, outsideHits);
      add(files, filesRead);
      add(rounds, roundsRun);
      add(scanTasks, scanTasksShipped);
      if (shortOfTopK) {
        add(underfilled, 1L);
      }
    }

    private static void add(SQLMetric metric, long value) {
      if (metric != null) {
        metric.add(value);
      }
    }
  }

  /**
   * What a search's tasks spent their time on, and what reading the index cost them.
   *
   * <p>A search is one job whose tasks each read an index file, score its records, fetch the rows
   * their hits name, and build their own answer. The four stages are wall clock and together cover
   * the time a task spent on its index files, so a query that took seconds can be told apart from
   * one that waited, and a stage that is missing is a stage that cost nothing rather than one that
   * was not measured.
   *
   * <p>The names are the contract: a spark event log records these by the description of the
   * accumulator, so a name changed here silently drops the metric from every log that reads it.
   * They follow the names the lance reader publishes, so the two formats can be compared.
   */
  public static final class SearchStages implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final String INDEX_READ_TIME = "indexReadTimeNs";
    public static final String PROBE_SCORE_TIME = "probeScoreTimeNs";
    public static final String FETCH_BACK_TIME = "fetchBackTimeNs";
    public static final String MERGE_TIME = "mergeTimeNs";
    public static final String TASK_TIME = "taskTimeNs";
    public static final String INDEX_FILE_BYTES_READ = "numIndexFileBytesRead";
    public static final String INDEX_RANGE_BYTES_READ = "numIndexRangeBytesRead";
    public static final String INDEX_RANGE_READ_REQUESTS = "numIndexRangeReadRequests";
    public static final String INDEX_LIST_BLOCKS_READ = "numIndexListBlocksRead";
    public static final String INDEX_WHOLE_LIST_AREA_READS = "numIndexWholeListAreaReads";
    public static final String INDEX_CACHE_HITS = "numIndexCacheHits";
    public static final String INDEX_CACHE_MISSES = "numIndexCacheMisses";

    private static final Map<String, String> DESCRIPTIONS;

    static {
      Map<String, String> descriptions = new java.util.LinkedHashMap<>();
      descriptions.put(INDEX_READ_TIME, "time a task spent opening and reading index files (ns)");
      descriptions.put(
          PROBE_SCORE_TIME, "time a task spent probing lists and scoring their records (ns)");
      descriptions.put(FETCH_BACK_TIME, "time a task spent fetching the rows its hits name (ns)");
      descriptions.put(MERGE_TIME, "time a task spent merging and building its answer (ns)");
      descriptions.put(TASK_TIME, "time a task spent on its index files (ns)");
      descriptions.put(INDEX_FILE_BYTES_READ, "bytes a task read to load index files whole");
      descriptions.put(INDEX_RANGE_BYTES_READ, "bytes a task read from index files in ranges");
      descriptions.put(INDEX_RANGE_READ_REQUESTS, "range requests a task made to index files");
      descriptions.put(INDEX_LIST_BLOCKS_READ, "index list blocks a task read through ranges");
      descriptions.put(
          INDEX_WHOLE_LIST_AREA_READS,
          "searches that read the whole list area instead of their ranges");
      descriptions.put(INDEX_CACHE_HITS, "index files an executor already held");
      descriptions.put(INDEX_CACHE_MISSES, "index file opens that read from storage");
      DESCRIPTIONS = java.util.Collections.unmodifiableMap(descriptions);
    }

    private final Map<String, SQLMetric> metrics;

    public SearchStages(Map<String, SQLMetric> metrics) {
      this.metrics =
          metrics == null ? java.util.Collections.emptyMap() : new java.util.HashMap<>(metrics);
    }

    /** The metric names a caller creates SQL metrics for, keyed by the description Spark uses. */
    public static Map<String, String> descriptions() {
      return DESCRIPTIONS;
    }

    /** Adds what one task spent, if any of its metrics were handed in. */
    void report(Stages stages) {
      add(INDEX_READ_TIME, stages.indexReadNanos);
      add(PROBE_SCORE_TIME, stages.probeScoreNanos);
      add(FETCH_BACK_TIME, stages.fetchBackNanos);
      add(MERGE_TIME, stages.mergeNanos);
      add(TASK_TIME, stages.taskNanos);
      add(INDEX_FILE_BYTES_READ, stages.fileBytesRead);
      add(INDEX_RANGE_BYTES_READ, stages.rangeBytesRead);
      add(INDEX_RANGE_READ_REQUESTS, stages.rangeReadRequests);
      add(INDEX_LIST_BLOCKS_READ, stages.listBlocksRead);
      add(INDEX_WHOLE_LIST_AREA_READS, stages.wholeListAreaReads);
      add(INDEX_CACHE_HITS, stages.cacheHits);
      add(INDEX_CACHE_MISSES, stages.cacheMisses);
    }

    private void add(String name, long value) {
      SQLMetric metric = metrics.get(name);
      if (metric != null) {
        metric.add(value);
      }
    }
  }

  /** What one task spent, gathered in the stage a caller reports it for. */
  static final class Stages {
    private long indexReadNanos;
    private long probeScoreNanos;
    private long fetchBackNanos;
    private long mergeNanos;
    private long taskNanos;
    private long fileBytesRead;
    private long rangeBytesRead;
    private long rangeReadRequests;
    private long listBlocksRead;
    private long wholeListAreaReads;
    private int cacheHits;
    private int cacheMisses;
  }

  /**
   * Searches the index snapshot a query is answered from, and reads the rows it found, on the
   * executors.
   *
   * <p>A hit names a data file and a position, and this reads the row there where the hit was made:
   * the task that searched an index file also reads the rows its own hits name, through the format
   * model, applies the snapshot's deletes and the query's filter, and keeps its own top-k. Only the
   * rows it keeps leave the task, and they leave as the answer itself rather than as something to
   * be collected and sent out again, so a query costs one job instead of three.
   *
   * <p>A task's top-k is sound because a row among the k nearest of the whole answer is also among
   * the k nearest of its own file. When the rows a file turned up do not fill the window, the task
   * asks its own index file for more: a payload it already holds is cheap to search again, which is
   * what makes widening a local read rather than another job.
   *
   * @param spark the Spark session
   * @param metadata the index to search
   * @param table the table the index belongs to, used to resolve the snapshot and its file IO
   * @param query the query to answer, whose snapshot and top-k the search uses
   * @param encodedQueries the query payloads, already encoded by the index's adapter
   * @param projection the columns to read, in this order
   * @param expected the schema to read the data files with, or null to infer it
   * @param keep how many leading columns of a read row are part of the answer
   * @param resultK the number of rows the query finally keeps, which sizes the refinement window
   * @param opening the number of candidates a task asks for before it has to widen
   * @param scanTasks the scan tasks of the snapshot being answered, where a hit's file is found
   * @param filter the filter a row has to pass after it is read, or null when the query has none or
   *     when the search is restricted to the admitted rows instead
   * @param candidateFilter the filter that decides which rows may be scored at all, or null when
   *     the search scores every row of its file
   * @param filterColumns the columns {@code candidateFilter} names, which is what a task reads to
   *     know which rows of its own files it may score
   * @param counters where the tasks report what they found, or null to report nothing
   * @param stages where the tasks report what they spent, or null to report nothing
   * @return the rows the index found, at most the query's top-k per index file, best first within a
   *     task
   */
  public static JavaRDD<Row> searchWithRows(
      SparkSession spark,
      IndexMetadata metadata,
      Table table,
      IndexQuery query,
      List<ByteBuffer> encodedQueries,
      Schema projection,
      StructType expected,
      int keep,
      int resultK,
      int opening,
      List<FileScanTask> scanTasks,
      IndexRowFilter filter,
      IndexRowFilter candidateFilter,
      List<String> filterColumns,
      RowCounters counters,
      SearchStages stages) {
    // A search is what makes an index hot, so this is where the LRU learns about it.
    IndexCacheControl.recordAccess(spark, metadata.indexUuid());
    IndexAdapter adapter =
        IndexTypes.fromType(metadata.indexType()).createAdapter(metadata.definition());
    Preconditions.checkArgument(
        !adapter.searchesAllPayloadsTogether(),
        "A search that merges payloads before scoring cannot read rows per task: %s",
        metadata.indexUuid());

    JavaSparkContext context = JavaSparkContext.fromSparkContext(spark.sparkContext());
    IndexReader reader = new IndexReader(metadata, table);
    List<IndexFile> files = reader.indexFiles(query);
    if (files.isEmpty() || encodedQueries.isEmpty()) {
      return context.emptyRDD();
    }

    // a task is handed its own index file and the scan tasks of the files that file covers, so the
    // table's file list does not travel with every task: what it needs is what it reads
    Map<String, FileScanTask> tasksByLocation = Maps.newHashMap();
    for (FileScanTask task : scanTasks) {
      tasksByLocation.put(task.file().location().toString(), task);
    }

    List<String> locations = Lists.newArrayListWithCapacity(files.size());
    List<IndexFileTask> shipped = Lists.newArrayListWithCapacity(files.size());
    for (IndexFile file : files) {
      List<FileScanTask> covered = Lists.newArrayList();
      for (String dataFile : file.coveredDataFiles()) {
        FileScanTask task = tasksByLocation.get(dataFile);
        if (task != null) {
          // a file the frozen snapshot no longer has has entries the index still names, and a hit
          // of one is counted as outside the snapshot rather than read
          covered.add(task);
        }
      }

      locations.add(file.location());
      shipped.add(
          new IndexFileTask(file.location(), file.entryCount(), recordedLength(file), covered));
    }

    JavaSparkContext sparkContext = context;
    // a ByteBuffer is not serializable, so the payloads travel as bytes
    byte[][] payloads = new byte[encodedQueries.size()][];
    for (int i = 0; i < encodedQueries.size(); i++) {
      payloads[i] = toBytes(encodedQueries.get(i).duplicate());
    }
    Broadcast<IndexAdapter> adapterRef = sparkContext.broadcast(adapter);
    // the file IO travels whole, so an executor reads a payload and a data file the way the table
    // is
    // read, with the properties of its catalog rather than with whatever the session is configured
    // with
    Broadcast<FileIO> ioRef = sparkContext.broadcast(table.io());
    Broadcast<byte[][]> payloadsRef = sparkContext.broadcast(payloads);
    Broadcast<RowReadPlan> planRef =
        sparkContext.broadcast(
            RowReadPlan.of(
                projection,
                expected,
                IndexEntries.SnapshotDeletes.read(
                    spark, table, coveredTasks(shipped), table.schema()),
                new SerializableConfiguration(spark.sessionState().newHadoopConf()),
                filter,
                candidateFilter,
                filterColumns,
                table.schema(),
                keep));
    long workingSet = workingSetBytes(table.io(), files, adapter, table.location());
    Broadcast<ExecutorIndexCache.Config> cacheRef =
        sparkContext.broadcast(ExecutorIndexCache.Config.from(spark, workingSet, table.location()));
    // an index that keeps vectors scores a similarity, so which end of the score is the better row
    // is the index's own decision and not something to assume
    IndexSearchResult higher = IndexSearchResult.of("a", 0L, 1.0);
    IndexSearchResult lower = IndexSearchResult.of("a", 0L, 0.0);
    boolean higherScoreFirst = adapter.resultComparator().compare(higher, lower) < 0;
    String cacheKey = adapterKey(adapter);
    List<String> cacheKeys = cacheKeys(table.location(), cacheKey, locations);
    // what each executor holds, so a task can be sent to one that already has its file
    IndexCacheLocations locationsTable = IndexCacheLocations.forContext(sparkContext.sc());
    boolean locality = localityEnabled(spark);
    org.apache.spark.util.CollectionAccumulator<IndexCacheLocations.Report> cacheReports =
        cacheReports(sparkContext, cacheRef.getValue());
    // identifies this search, so its tasks report this executor's cache once instead of once each
    String queryRun = java.util.UUID.randomUUID().toString();
    boolean rangeRead = rangeReadEnabled(spark, adapter);
    RowCounters tally = counters;

    int topK = query.topK();
    int initial = Math.max(opening, topK + 1);
    // refinement re-ranks one query's fetched candidates, so a task that answers several queries
    // has no single window to refine; it is off by default and an experiment switch turns it on
    VectorRefine refine =
        payloads.length == 1 ? VectorRefine.forSearch(adapter, projection, keep, resultK) : null;
    if (refine != null) {
      LOG.info(
          "Refining vector candidates by exact distance: {} candidates per result, metric={}",
          refine.candidatesPerResult(),
          ((VectorIndexDefinition) adapter.definition()).metric());
    }

    // Each index file carries an independent search structure and is one unit of work.
    return IndexFileRdds.tasks(sparkContext.sc(), shipped, cacheKeys, locationsTable, locality)
        .mapPartitions(
            (FlatMapFunction<Iterator<IndexFileTask>, Row>)
                indexFiles -> {
                  IndexAdapter fileAdapter = adapterRef.getValue();
                  byte[][] filePayloads = payloadsRef.getValue();
                  // the exact re-ranking measures the query's own distance, so it decodes the query
                  // the payload already carries rather than shipping a second copy of it
                  float[] refinedQuery =
                      refine == null
                          ? null
                          : ((VectorIndexAdapter) fileAdapter)
                              .decodeQuery(ByteBuffer.wrap(filePayloads[0]));
                  FileIO fileIO = ioRef.getValue();
                  ExecutorIndexCache.Config cacheConfig = cacheRef.getValue();
                  RowReadPlan plan = planRef.getValue();
                  List<Row> partitionRows = Lists.newArrayList();
                  Stages stageTally = stages == null ? null : new Stages();
                  long taskStart = System.nanoTime();
                  while (indexFiles.hasNext()) {
                    IndexFileTask indexFile = indexFiles.next();
                    long candidatesStart = System.nanoTime();
                    IndexCandidates candidates = plan.candidatesOf(fileIO, indexFile.tasks());
                    if (stageTally != null) {
                      // reading the files a prefilter admits is a fetch-back before the search
                      stageTally.fetchBackNanos += System.nanoTime() - candidatesStart;
                    }
                    if (candidates != null && candidates.isEmpty()) {
                      if (tally != null) {
                        tally.report(0, 0, 0, 0, 0, 0, 0, 0, indexFile.tasks().size(), true);
                      }

                      continue;
                    }

                    String location = indexFile.location();
                    long readStart = System.nanoTime();
                    OpenedSearch opened =
                        openSearch(
                            fileIO,
                            location,
                            fileAdapter,
                            cacheConfig,
                            cacheKey,
                            rangeRead,
                            indexFile.fileLength());
                    long readNanos = System.nanoTime() - readStart;
                    if (stageTally != null) {
                      stageTally.indexReadNanos += readNanos;
                      stageTally.fileBytesRead += opened.fileBytesRead();
                      if (opened.servedFromCache()) {
                        stageTally.cacheHits++;
                      } else {
                        stageTally.cacheMisses++;
                      }

                      VectorIndexAdapter.RangeSearch range = opened.rangeSearch();
                      if (range != null) {
                        VectorIndexAdapter.OpenReadMetrics open = range.openReadMetrics();
                        if (open != null) {
                          stageTally.rangeBytesRead +=
                              open.footerBytes() + open.descriptionBytes() + open.listBytes();
                          stageTally.rangeReadRequests +=
                              open.footerRequests()
                                  + open.descriptionRequests()
                                  + open.listRequests();
                          stageTally.listBlocksRead += open.listRequests();
                        }
                      }
                    }

                    long searchStart = System.nanoTime();
                    // the refined window is asked for in the first round rather than grown towards:
                    // an extra round would fetch and read the same candidates twice
                    int requested =
                        refine == null
                            ? initial
                            : refine.window(initial, resultK, indexFile.entryCount());
                    // what the search was originally asked for is what it reports as its opening
                    // when nothing re-ranked the window, so a run without refinement reports the
                    // same numbers it always has
                    int reported = refine == null ? opening : requested;
                    try (opened) {
                      ExecutorIndexCache.CachedFile file = opened.file;
                      FileOutcome outcome =
                          rowsOfIndexFile(
                              file,
                              fileIO,
                              filePayloads,
                              topK,
                              resultK,
                              requested,
                              indexFile.entryCount(),
                              higherScoreFirst,
                              candidates,
                              plan,
                              refine,
                              refinedQuery,
                              indexFile.tasks(),
                              opened.rangeSearch(),
                              stageTally);
                      if (tally != null) {
                        tally.report(
                            reported,
                            outcome.lastRequest,
                            outcome.rows.size(),
                            outcome.fetched,
                            outcome.unresolved,
                            outcome.outsideSnapshot,
                            outcome.filesRead,
                            outcome.rounds,
                            indexFile.tasks().size(),
                            outcome.rows.size() < (refine == null ? topK : resultK));
                      }
                      long mergeStart = System.nanoTime();
                      partitionRows.addAll(outcome.rows);
                      if (stageTally != null) {
                        stageTally.mergeNanos += System.nanoTime() - mergeStart;
                      }
                    }

                    if (stageTally != null && opened.rangeSearch() != null) {
                      stageTally.wholeListAreaReads += opened.rangeSearch().wholeListAreaReads();
                    }

                    if (opened.rangeSearch != null) {
                      logReadTimes(location, opened, readNanos, System.nanoTime() - searchStart);
                    }
                  }

                  if (stageTally != null) {
                    stageTally.taskNanos = System.nanoTime() - taskStart;
                    stages.report(stageTally);
                  }

                  reportCache(cacheReports, queryRun);
                  return partitionRows.iterator();
                });
  }

  /** Every scan task a task may read, which is what the snapshot's deletes are read for. */
  private static List<FileScanTask> coveredTasks(List<IndexFileTask> files) {
    List<FileScanTask> tasks = Lists.newArrayList();
    for (IndexFileTask file : files) {
      tasks.addAll(file.tasks());
    }

    return tasks;
  }

  /** What one index file's task found, and what it cost to find it. */
  private static final class FileOutcome {
    private final List<Row> rows;
    private final int fetched;
    private final int lastRequest;
    private final int unresolved;
    private final int outsideSnapshot;
    private final int filesRead;
    private final int rounds;

    private FileOutcome(
        List<Row> rows,
        int fetched,
        int lastRequest,
        int unresolved,
        int outsideSnapshot,
        int filesRead,
        int rounds) {
      this.rows = rows;
      this.fetched = fetched;
      this.lastRequest = lastRequest;
      this.unresolved = unresolved;
      this.outsideSnapshot = outsideSnapshot;
      this.filesRead = filesRead;
      this.rounds = rounds;
    }
  }

  /**
   * The rows one index file found, read where the file was searched.
   *
   * <p>The window is filled from live rows that pass the filter, so a hit whose row a delete file
   * removed, or that the filter turns down, does not count towards it: the file is asked for more
   * until the window is full or the file has nothing left to offer. Each round asks for more than
   * the last, because a kernel may return a different set for a wider search rather than a
   * superset.
   */
  private static FileOutcome rowsOfIndexFile(
      ExecutorIndexCache.CachedFile file,
      FileIO io,
      byte[][] payloads,
      int topK,
      int resultK,
      int opening,
      long entries,
      boolean higherScoreFirst,
      IndexCandidates candidates,
      RowReadPlan plan,
      VectorRefine refine,
      float[] refinedQuery,
      List<FileScanTask> ownTasks,
      VectorIndexAdapter.RangeSearch range,
      Stages stageTally) {
    // a kernel that filters while it searches scores the admitted rows and nothing else, so the
    // window is filled from them; one that cannot do that answers the unfiltered question instead,
    // and the hits outside the set are dropped below rather than counted as rejected
    // a searcher that scores everything and then keeps what the set admits cannot say by its answer
    // size that it has nothing left: a selective filter makes its answer small however much it
    // still
    // holds. What it can do is score more, so the widening goes on until the file's entries have
    // all
    // been asked for.
    boolean filtersWhileSearching =
        candidates == null || file.searching(prepared -> prepared.joinsCandidateSet());
    // a refined search fills the window it asked for and lets the exact ranking pick the k rows
    // from it; without refinement the window is k, filled by the estimate's order
    int target = refine == null ? topK : Math.max(opening, topK);
    Map<String, FetchedRow> found = Maps.newLinkedHashMap();
    int limit = Math.max(opening, target);
    int last = 0;
    int round = 0;
    int unresolved = 0;
    int fetchedRows = 0;
    int rejected = 0;
    int outside = 0;
    Set<String> filesRead = new java.util.HashSet<>();
    boolean more = true;
    while (found.size() < target && round < MAX_ROUNDS && more) {
      round++;
      final int asked = (int) Math.min(entries, limit);
      last = asked;
      more = false;
      for (byte[] payload : payloads) {
        long listNanosBefore = 0L;
        long listBytesBefore = 0L;
        long listRequestsBefore = 0L;
        if (stageTally != null && range != null) {
          VectorIndexAdapter.RangeReadMetrics before = range.listReadMetrics();
          listNanosBefore = before.nanos();
          listBytesBefore = before.bytes();
          listRequestsBefore = before.requests();
        }

        long searchStart = System.nanoTime();
        List<IndexSearchResult> hits =
            file.searching(
                prepared -> searchFile(prepared, ByteBuffer.wrap(payload), asked, candidates));
        long searchNanos = System.nanoTime() - searchStart;
        if (stageTally != null) {
          // a range search reads the lists it visits inside the search, so that read is index-read
          // time and not scoring time
          long listNanos = 0L;
          long listBytes = 0L;
          long listRequests = 0L;
          if (range != null) {
            VectorIndexAdapter.RangeReadMetrics after = range.listReadMetrics();
            listNanos = after.nanos() - listNanosBefore;
            listBytes = after.bytes() - listBytesBefore;
            listRequests = after.requests() - listRequestsBefore;
          }

          stageTally.indexReadNanos += Math.max(0L, listNanos);
          stageTally.probeScoreNanos += Math.max(0L, searchNanos - listNanos);
          stageTally.rangeBytesRead += Math.max(0L, listBytes);
          stageTally.rangeReadRequests += Math.max(0L, listRequests);
          stageTally.listBlocksRead += Math.max(0L, listRequests);
        }

        // a kernel that returned the whole window may hold more; one that returned less has nothing
        // left to offer, which is what stops the widening. A kernel that keeps the set afterwards
        // is asked for everything its file holds instead, because its answer size says nothing
        more = filtersWhileSearching ? hits.size() >= asked : asked < entries;

        long fetchStart = System.nanoTime();
        RowsAtHits read = plan.rowsOf(hits, io, ownTasks);
        if (stageTally != null) {
          stageTally.fetchBackNanos += System.nanoTime() - fetchStart;
        }
        fetchedRows += read.rows.size();
        unresolved += read.unresolved;
        outside += read.outsideSnapshot;
        filesRead.addAll(read.dataFiles);
        for (FetchedRow row : read.rows) {
          if (candidates != null && !candidates.contains(row.dataFile, row.position)) {
            // a hit the search was not allowed to score: it is not part of the answer this task may
            // give, and counting it as rejected would widen a search that has nothing to find
            continue;
          }

          if (!plan.admits(row.row)) {
            rejected++;
            continue;
          }

          // the columns read only to judge the row are dropped once it has been judged
          found.putIfAbsent(
              row.dataFile + "#" + row.position,
              new FetchedRow(row.dataFile, row.position, plan.narrow(row.row), row.score));
        }
      }

      if (found.size() >= target || !more) {
        break;
      }

      int wanted = target + rejected + unresolved;
      int next = (int) Math.min(entries, Math.max((long) limit * 2, (long) wanted));
      if (next <= limit) {
        break;
      }

      limit = next;
    }

    long mergeStart = System.nanoTime();
    List<FetchedRow> fetched = Lists.newArrayList(found.values());
    List<Row> rows;
    if (refine == null) {
      fetched.sort(byScore(higherScoreFirst));
      rows = Lists.newArrayListWithCapacity(Math.min(topK, fetched.size()));
      for (int i = 0; i < fetched.size() && i < topK; i++) {
        rows.add(fetched.get(i).row);
      }
    } else {
      rows = refinedTopK(fetched, refine, refinedQuery, resultK);
    }

    if (stageTally != null) {
      stageTally.mergeNanos += System.nanoTime() - mergeStart;
    }

    return new FileOutcome(rows, fetchedRows, last, unresolved, outside, filesRead.size(), round);
  }

  /**
   * The exact best {@code topK} of the fetched rows.
   *
   * <p>The window was filled by the index's estimate, so its rows are the candidates and this is
   * where their order and their distance are decided by the metric the query asked for. Every row
   * was fetched back anyway; re-ranking it costs one distance over its vector, not another read.
   */
  private static List<Row> refinedTopK(
      List<FetchedRow> fetched, VectorRefine refine, float[] query, int topK) {
    double[] distances = new double[fetched.size()];
    String[] dataFiles = new String[fetched.size()];
    long[] positions = new long[fetched.size()];
    for (int i = 0; i < fetched.size(); i++) {
      FetchedRow row = fetched.get(i);
      distances[i] = refine.distance(row.row, query);
      dataFiles[i] = row.dataFile;
      positions[i] = row.position;
    }

    int[] best = VectorRefine.bestFirst(distances, dataFiles, positions, topK);
    List<Row> rows = Lists.newArrayListWithCapacity(best.length);
    for (int index : best) {
      rows.add(fetched.get(index).row);
    }

    return rows;
  }

  /** The order of two rows of a search: the better row first, ties broken by file and position. */
  private static Comparator<FetchedRow> byScore(boolean higherScoreFirst) {
    return (Comparator<FetchedRow> & Serializable)
        (left, right) -> {
          int byScore = Double.compare(left.score, right.score);
          int primary = higherScoreFirst ? -byScore : byScore;
          if (primary != 0) {
            return primary;
          }

          int byFile = left.dataFile.compareTo(right.dataFile);
          if (byFile != 0) {
            return byFile;
          }

          return Long.compare(left.position, right.position);
        };
  }

  /** The rows of a set of hits, and how many hits pointed at nothing a task could read. */
  private static final class RowsAtHits {
    private final List<FetchedRow> rows;
    private final int unresolved;
    private final int outsideSnapshot;
    private final Set<String> dataFiles;

    private RowsAtHits(
        List<FetchedRow> rows, int unresolved, int outsideSnapshot, Set<String> dataFiles) {
      this.rows = rows;
      this.unresolved = unresolved;
      this.outsideSnapshot = outsideSnapshot;
      this.dataFiles = dataFiles;
    }
  }

  /** A row a hit pointed at, read where the hit was made. */
  static final class FetchedRow {
    private final String dataFile;
    private final long position;
    private final Row row;
    private final double score;

    private FetchedRow(String dataFile, long position, Row row, double score) {
      this.dataFile = dataFile;
      this.position = position;
      this.row = row;
      this.score = score;
    }

    double score() {
      return score;
    }
  }

  /**
   * What a task needs to turn the hits it found into rows.
   *
   * <p>It holds the files of the snapshot the hits may name, what to project, and the filters a row
   * is held to: the snapshot's deletes, and the query's own filter. It is broadcast once for the
   * search, so a task carries a reference rather than a copy of the table's file list.
   */
  static final class RowReadPlan implements Serializable {
    private static final long serialVersionUID = 1L;

    private final Schema projection;
    private final StructType expected;
    private final IndexEntries.SnapshotDeletes deletes;
    private final SerializableConfiguration conf;
    private final IndexRowFilter filter;
    private final IndexRowFilter candidateFilter;
    private final List<String> filterColumns;
    private final Schema tableSchema;
    private final int keep;

    private RowReadPlan(
        Schema projection,
        StructType expected,
        IndexEntries.SnapshotDeletes deletes,
        SerializableConfiguration conf,
        IndexRowFilter filter,
        IndexRowFilter candidateFilter,
        List<String> filterColumns,
        Schema tableSchema,
        int keep) {
      this.projection = projection;
      this.expected = expected;
      this.deletes = deletes;
      this.conf = conf;
      this.filter = filter;
      this.candidateFilter = candidateFilter;
      this.filterColumns = new ArrayList<>(filterColumns);
      this.tableSchema = tableSchema;
      this.keep = keep;
    }

    static RowReadPlan of(
        Schema projection,
        StructType expected,
        IndexEntries.SnapshotDeletes deletes,
        SerializableConfiguration conf,
        IndexRowFilter filter,
        IndexRowFilter candidateFilter,
        List<String> filterColumns,
        Schema tableSchema,
        int keep) {
      return new RowReadPlan(
          projection,
          expected,
          deletes,
          conf,
          filter,
          candidateFilter,
          filterColumns,
          tableSchema,
          keep);
    }

    /** The live rows the hits name, with the score the index gave each of them. */
    RowsAtHits rowsOf(List<IndexSearchResult> hits, FileIO io, List<FileScanTask> ownTasks) {
      if (hits.isEmpty()) {
        return new RowsAtHits(ImmutableList.of(), 0, 0, new java.util.HashSet<>());
      }

      Map<String, List<IndexSearchResult>> byFile = Maps.newLinkedHashMap();
      for (IndexSearchResult hit : hits) {
        byFile.computeIfAbsent(hit.dataFile(), key -> Lists.newArrayList()).add(hit);
      }

      List<FetchedRow> rows = Lists.newArrayList();
      Set<String> files = new java.util.HashSet<>();
      int unresolved = 0;
      int outside = 0;
      Map<String, FileScanTask> byLocation = tasksByLocation(ownTasks);
      for (Map.Entry<String, List<IndexSearchResult>> entry : byFile.entrySet()) {
        String dataFile = entry.getKey();
        FileScanTask task = byLocation.get(dataFile);
        if (task == null) {
          // a hit of a file this snapshot no longer has cannot be read: a compaction or rewrite
          // after the build leaves entries pointing at files the snapshot replaced
          outside += entry.getValue().size();
          continue;
        }

        List<Long> positions = Lists.newArrayListWithCapacity(entry.getValue().size());
        Map<Long, Double> scores = Maps.newHashMap();
        for (IndexSearchResult hit : entry.getValue()) {
          positions.add(hit.rowPosition());
          scores.put(hit.rowPosition(), hit.score());
        }

        java.util.Collections.sort(positions);
        List<Tuple2<Row, Long>> read =
            IndexEntries.liveRowsAt(io, task, deletes, positions, projection, expected, conf);
        files.add(dataFile);
        for (Tuple2<Row, Long> row : read) {
          Double score = scores.get(row._2());
          if (score == null) {
            continue;
          }

          rows.add(new FetchedRow(dataFile, row._2(), row._1(), score.doubleValue()));
        }

        // a hit whose position has no live row was removed by a delete file of the snapshot, or the
        // file no longer holds a row there
        unresolved += positions.size() - read.size();
      }

      return new RowsAtHits(rows, unresolved, outside, files);
    }

    /**
     * The rows of the given data files that may be scored at all, or null when every row of the
     * search may be.
     *
     * <p>The set is built here, in the task, from the files this index file covers: a filter that
     * keeps a small share of the table is what makes restricting the search worth it, and a task
     * knows its own share without asking the driver for the table's.
     */
    IndexCandidates candidatesOf(FileIO io, List<FileScanTask> ownTasks) {
      if (candidateFilter == null || ownTasks.isEmpty()) {
        return null;
      }

      Map<String, long[]> positions = Maps.newLinkedHashMap();
      long count = 0;
      for (FileScanTask task : ownTasks) {
        String dataFile = task.file().location().toString();
        long[] admitted =
            IndexEntries.positionsMatching(
                io, task, deletes, candidateFilter, tableSchema, filterColumns, expected, conf);
        if (admitted.length > 0) {
          positions.put(dataFile, admitted);
          count += admitted.length;
        }
      }

      if (count == 0) {
        return IndexCandidates.empty();
      }

      return IndexCandidates.of(positions);
    }

    /** Whether a row passes the query's own filter, when the query has one. */
    boolean admits(Row row) {
      return filter == null || filter.matches(row);
    }

    /** The row without the columns that were read only to judge it. */
    Row narrow(Row row) {
      if (row.size() == keep) {
        return row;
      }

      Object[] values = new Object[keep];
      for (int i = 0; i < keep; i++) {
        values[i] = row.get(i);
      }

      return org.apache.spark.sql.RowFactory.create(values);
    }

    private static Map<String, FileScanTask> tasksByLocation(List<FileScanTask> tasks) {
      Map<String, FileScanTask> byLocation = Maps.newHashMap();
      for (FileScanTask task : tasks) {
        byLocation.put(task.file().location().toString(), task);
      }

      return byLocation;
    }
  }

  /** The rounds a task may widen its own search before it reports what it has. */
  private static final int MAX_ROUNDS = 10;

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
    return search(spark, metadata, table, query, null);
  }

  /**
   * Searches the index for the rows a scalar filter admitted, and only those.
   *
   * <p>Without a candidate set a filtered query cannot be answered from the index: the search picks
   * the rows nearest the query out of everything the index holds, and the rows the filter would
   * have kept may be anywhere in that order. Handing the admitted rows to the search lets the
   * kernel decide what to score with the filter in hand, which is what makes a filtered query a
   * question an index can answer at all.
   *
   * <p>A kernel that cannot take the set while it searches is asked the unfiltered question and
   * filtered afterwards, which returns fewer rows than asked for when the filter is selective. That
   * is a smaller answer to the same question, never a wrong one.
   *
   * @param spark the Spark session
   * @param metadata the index to search
   * @param table the table the index belongs to, used to resolve the snapshot and its file IO
   * @param query the query to answer
   * @param candidates the rows a scalar filter admitted, or null when the query has no filter
   * @return the hits of the whole index, best first, and whether the index holds more of them
   */
  public static SearchOutcome search(
      SparkSession spark,
      IndexMetadata metadata,
      Table table,
      IndexQuery query,
      IndexCandidates candidates) {
    // A search is what makes an index hot, so this is where the LRU learns about it.
    IndexCacheControl.recordAccess(spark, metadata.indexUuid());
    IndexAdapter adapter =
        IndexTypes.fromType(metadata.indexType()).createAdapter(metadata.definition());
    if (adapter instanceof TextIndexAdapter text && candidates == null) {
      int limit = query.topK();
      if (limit == 0) {
        return new SearchOutcome(ImmutableList.of(), false);
      }

      int window = limit == Integer.MAX_VALUE ? limit : limit + 1;
      IndexQuery widened =
          IndexQuery.of(query.sourceTableSnapshotId(), query.payload(), window)
              .withConsistency(query.consistency());
      List<IndexSearchResult> hits =
          searchText(spark, metadata, table, text, List.of(widened), null, null).get(0);
      boolean more = hits.size() > limit;
      return new SearchOutcome(more ? ImmutableList.copyOf(hits.subList(0, limit)) : hits, more);
    }

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

    // a search of one query does not read rows through its scan tasks, but the tasks carry what
    // each index file's open needs besides its location
    List<IndexFileTask> shards = indexFileTasks(files);

    JavaSparkContext context = JavaSparkContext.fromSparkContext(spark.sparkContext());
    Broadcast<IndexAdapter> adapterRef = context.broadcast(adapter);
    // a ByteBuffer is not serializable, so the payload travels as bytes
    Broadcast<byte[]> queryRef = context.broadcast(toBytes(query.payload().duplicate()));
    // the admitted rows travel once for the whole search rather than with each task's closure
    Broadcast<IndexCandidates> candidatesRef =
        candidates == null ? null : context.broadcast(candidates);
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
    boolean rangeRead = rangeReadEnabled(spark, adapter);

    // the limit is captured as a primitive: the query itself is not serializable. One more than the
    // query asks for is kept, because that is what says whether there is another round to ask for.
    int topK = query.topK();
    int localTopK = topK + 1;

    // Each index file carries an independent search structure and is one unit of work.
    List<Tuple3<String, Long, Double>> top =
        IndexFileRdds.tasks(context.sc(), shards, cacheKeys, locationsTable, locality)
            .mapPartitions(
                (FlatMapFunction<Iterator<IndexFileTask>, Tuple3<String, Long, Double>>)
                    indexFiles -> {
                      IndexAdapter fileAdapter = adapterRef.getValue();
                      ByteBuffer fileQuery = ByteBuffer.wrap(queryRef.getValue());
                      IndexCandidates fileCandidates =
                          candidatesRef == null ? null : candidatesRef.getValue();
                      FileIO fileIO = ioRef.getValue();
                      ExecutorIndexCache.Config cacheConfig = cacheRef.getValue();
                      List<Tuple3<String, Long, Double>> fileHits = Lists.newArrayList();
                      while (indexFiles.hasNext()) {
                        IndexFileTask indexFile = indexFiles.next();
                        String location = indexFile.location();
                        long readStart = System.nanoTime();
                        OpenedSearch opened =
                            openSearch(
                                fileIO,
                                location,
                                fileAdapter,
                                cacheConfig,
                                cacheKey,
                                rangeRead,
                                indexFile.fileLength());
                        long readNanos = System.nanoTime() - readStart;
                        long searchStart = System.nanoTime();
                        try (opened) {
                          ExecutorIndexCache.CachedFile file = opened.file;
                          for (IndexSearchResult hit :
                              file.searching(
                                  prepared ->
                                      searchFile(
                                          prepared,
                                          fileQuery.duplicate(),
                                          localTopK,
                                          fileCandidates))) {
                            fileHits.add(
                                new Tuple3<>(hit.dataFile(), hit.rowPosition(), hit.score()));
                          }
                        }
                        logReadTimes(location, opened, readNanos, System.nanoTime() - searchStart);
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
   * Searches one index file for the rows the candidate set admits.
   *
   * <p>A kernel that filters while it searches fills the window from the admitted rows, so the file
   * still contributes as many hits as it has. Any other kernel answers the unfiltered question and
   * the hits outside the set are dropped, which is the best that can be done without asking it a
   * question it cannot answer.
   */
  static List<IndexSearchResult> searchFile(
      PreparedSearch prepared, ByteBuffer query, int topK, IndexCandidates candidates) {
    return candidates == null
        ? prepared.search(query, topK)
        : prepared.search(query, topK, candidates);
  }

  /**
   * Searches the index snapshot a batch of queries is answered from, loading each index file once.
   *
   * <p>A query on its own reads and decodes every index file of the snapshot, so N queries cost N
   * times that work. This searches each file once for the whole batch: the payload is read once,
   * prepared once, and then scored once per query, so the batch pays one load and N searches.
   *
   * <p>This is the hits-only form; use {@link #searchBatchWithOutcomes} when the caller also needs
   * to know whether the index holds more hits than a query asked for.
   *
   * @param spark the Spark session
   * @param metadata the index to search
   * @param table the table the index belongs to, used to resolve the snapshot and its file IO
   * @param queries the queries to answer; all are answered from the snapshot of the first one
   * @return the hits of each query, best first, in the order the queries were given
   */
  public static List<List<IndexSearchResult>> searchBatch(
      SparkSession spark, IndexMetadata metadata, Table table, List<IndexQuery> queries) {
    List<SearchOutcome> outcomes = searchBatchOutcomes(spark, metadata, table, queries);
    List<List<IndexSearchResult>> perQuery = Lists.newArrayListWithCapacity(outcomes.size());
    for (SearchOutcome outcome : outcomes) {
      perQuery.add(outcome.hits());
    }

    return perQuery;
  }

  /**
   * Searches the index snapshot a batch of queries is answered from, loading each index file once,
   * and reports for each query the hits and whether the index holds more of them.
   *
   * <p>This is the batch form of {@link #search}. Each query asks each file for one more hit than
   * its own window. The merge uses that extra hit to report {@code moreHits} and limits the
   * returned hits to the query's window.
   *
   * <p>An index whose search merges the payloads before scoring only ever sees the top-k it
   * returns, so it cannot say whether more hits exist; the outcomes of such an index report no more
   * hits.
   *
   * @param spark the Spark session
   * @param metadata the index to search
   * @param table the table the index belongs to, used to resolve the snapshot and its file IO
   * @param queries the queries to answer, with the same source table snapshot and consistency
   * @return the hits of each query, best first, and whether the index holds more of them, in the
   *     order the queries were given
   */
  public static List<SearchOutcome> searchBatchWithOutcomes(
      SparkSession spark, IndexMetadata metadata, Table table, List<IndexQuery> queries) {
    if (!queries.isEmpty()) {
      long snapshotId = queries.get(0).sourceTableSnapshotId();
      IndexQuery.Consistency consistency = queries.get(0).consistency();
      for (IndexQuery query : queries) {
        Preconditions.checkArgument(
            query.sourceTableSnapshotId() == snapshotId,
            "Batch queries must be answered from the same source table snapshot: expected %s but"
                + " found %s",
            snapshotId,
            query.sourceTableSnapshotId());
        Preconditions.checkArgument(
            query.consistency() == consistency,
            "Batch queries must have the same consistency: expected %s but found %s",
            consistency,
            query.consistency());
      }
    }

    return searchBatchOutcomes(spark, metadata, table, queries);
  }

  private static List<SearchOutcome> searchBatchOutcomes(
      SparkSession spark, IndexMetadata metadata, Table table, List<IndexQuery> queries) {
    if (queries.isEmpty()) {
      return ImmutableList.of();
    }

    IndexCacheControl.recordAccess(spark, metadata.indexUuid());
    IndexAdapter adapter =
        IndexTypes.fromType(metadata.indexType()).createAdapter(metadata.definition());
    if (adapter instanceof TextIndexAdapter text) {
      // the text batch path scores against merged corpus statistics and only ever sees its own
      // top-k, so it cannot report whether more hits exist
      return outcomesWithoutMore(searchText(spark, metadata, table, text, queries, null, null));
    }

    if (adapter.searchesAllPayloadsTogether()) {
      // each query needs the merged payloads, and such a search has no per-file form to batch
      List<SearchOutcome> perQuery = Lists.newArrayListWithCapacity(queries.size());
      IndexReader togetherReader = new IndexReader(metadata, table);
      for (IndexQuery query : queries) {
        perQuery.add(new SearchOutcome(togetherReader.search(query), false));
      }
      return perQuery;
    }

    IndexReader reader = new IndexReader(metadata, table);
    List<IndexFile> files = reader.indexFiles(queries.get(0));
    List<SearchOutcome> perQuery = Lists.newArrayListWithCapacity(queries.size());
    if (files.isEmpty()) {
      for (int i = 0; i < queries.size(); i++) {
        perQuery.add(new SearchOutcome(ImmutableList.of(), false));
      }
      return perQuery;
    }

    List<String> locations = Lists.newArrayListWithCapacity(files.size());
    for (IndexFile file : files) {
      locations.add(file.location());
    }

    // a batch search does not read rows through its scan tasks, but the tasks carry what each
    // index file's open needs besides its location
    List<IndexFileTask> shards = indexFileTasks(files);

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
    // a query asks its own file for one more than its own window: the window is part of the
    // question for a structure whose traversal depends on it, so the widest query must not answer
    // for a narrower one
    int[] askedPerQuery = new int[queries.size()];
    for (int i = 0; i < queries.size(); i++) {
      askedPerQuery[i] = (int) Math.min(Integer.MAX_VALUE, (long) queries.get(i).topK() + 1L);
    }
    Broadcast<int[]> askedRef = context.broadcast(askedPerQuery);
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
    boolean rangeRead = rangeReadEnabled(spark, adapter);

    // one more than the widest query asks for is kept per query, because that is what says whether
    // there is another round to ask for
    int localTopK = widestTopK + 1;

    JavaPairRDD<Integer, Tuple3<String, Long, Double>> fileHits =
        IndexFileRdds.tasks(context.sc(), shards, cacheKeys, locationsTable, locality)
            .mapPartitionsToPair(
                (PairFlatMapFunction<
                        Iterator<IndexFileTask>, Integer, Tuple3<String, Long, Double>>)
                    indexFiles -> {
                      IndexAdapter fileAdapter = adapterRef.getValue();
                      List<byte[]> fileQueries = queriesRef.getValue();
                      FileIO fileIO = ioRef.getValue();
                      ExecutorIndexCache.Config cacheConfig = cacheRef.getValue();
                      List<Tuple2<Integer, Tuple3<String, Long, Double>>> batchHits =
                          Lists.newArrayList();
                      while (indexFiles.hasNext()) {
                        IndexFileTask indexFile = indexFiles.next();
                        String location = indexFile.location();
                        long readStart = System.nanoTime();
                        OpenedSearch opened =
                            openSearch(
                                fileIO,
                                location,
                                fileAdapter,
                                cacheConfig,
                                cacheKey,
                                rangeRead,
                                indexFile.fileLength());
                        long readNanos = System.nanoTime() - readStart;

                        long searchStart = System.nanoTime();
                        try (opened) {
                          ExecutorIndexCache.CachedFile file = opened.file;
                          file.searching(
                              prepared -> {
                                int[] asked = askedRef.getValue();
                                for (int queryId = 0; queryId < fileQueries.size(); queryId++) {
                                  for (IndexSearchResult hit :
                                      prepared.search(
                                          ByteBuffer.wrap(fileQueries.get(queryId)),
                                          asked[queryId])) {
                                    batchHits.add(
                                        new Tuple2<>(
                                            queryId,
                                            new Tuple3<>(
                                                hit.dataFile(), hit.rowPosition(), hit.score())));
                                  }
                                }
                                return null;
                              });
                        }

                        logReadTimes(location, opened, readNanos, System.nanoTime() - searchStart);
                      }
                      reportCache(cacheReports, queryRun);
                      return batchHits.iterator();
                    });

    // the per-query top-k is merged with a bounded accumulator, so the shuffle carries at most one
    // entry per (query, index file) instead of every hit
    Comparator<Tuple3<String, Long, Double>> order = hitOrder(adapter);
    int mergeLimit = localTopK;
    Map<Integer, List<Tuple3<String, Long, Double>>> topByQuery =
        aggregateHits(fileHits, order, mergeLimit);
    mergeCacheReports(cacheReports, locationsTable, cacheKeys, cacheRef.getValue(), workingSet);

    for (int queryId = 0; queryId < queries.size(); queryId++) {
      int limit = queries.get(queryId).topK();
      List<Tuple3<String, Long, Double>> candidates =
          topByQuery.getOrDefault(queryId, Lists.newArrayList());
      // the merge keeps one more than the widest window, so a query whose merged candidates fit its
      // own window is the only case where the index has nothing more to offer it
      perQuery.add(new SearchOutcome(hitsOf(candidates, limit, order), candidates.size() > limit));
    }

    return perQuery;
  }

  /** Searches the full text corpus while ranking only rows admitted by the predicate. */
  public static List<IndexSearchResult> searchFilteredText(
      SparkSession spark,
      IndexMetadata metadata,
      Table table,
      IndexQuery query,
      List<FileScanTask> tasks,
      Schema projection,
      Expression pruning,
      Predicate<Row> predicate) {
    Preconditions.checkArgument(
        query.consistency() == IndexQuery.Consistency.EXACT_SNAPSHOT,
        "A filtered text search requires an exact source snapshot");
    Preconditions.checkArgument(
        predicate instanceof Serializable, "Text predicate is not serializable");
    IndexCacheControl.recordAccess(spark, metadata.indexUuid());
    TextIndexAdapter adapter = new TextIndexAdapter((TextIndexDefinition) metadata.definition());
    Map<String, String> properties =
        Maps.newHashMap(IndexEntries.storageProperties(spark.sessionState().newHadoopConf()));
    properties.putAll(IndexEntries.storageProperties(table.io()));
    properties.putAll(table.properties());
    TextFilterRead filter =
        new TextFilterRead(
            EncryptingFileIO.combine(table.io(), table.encryption()),
            projection,
            predicate,
            properties);
    return searchText(
            spark,
            metadata,
            table,
            adapter,
            List.of(query),
            filter,
            textTasks(table.schema(), tasks, pruning))
        .get(0);
  }

  private static Map<String, FileScanTask> textTasks(
      Schema schema, List<FileScanTask> tasks, Expression pruning) {
    InclusiveMetricsEvaluator metrics = new InclusiveMetricsEvaluator(schema, pruning);
    Map<Integer, Evaluator> partitions = Maps.newHashMap();
    Map<String, FileScanTask> admitted = Maps.newHashMap();
    for (FileScanTask task : tasks) {
      Evaluator partition =
          partitions.computeIfAbsent(
              task.spec().specId(),
              id ->
                  new Evaluator(
                      task.spec().partitionType(),
                      Projections.inclusive(task.spec()).project(pruning)));
      if (partition.eval(task.file().partition()) && metrics.eval(task.file())) {
        admitted.put(task.file().location(), task);
      }
    }
    return admitted;
  }

  private static final class TextFilterRead implements Serializable {
    private final FileIO io;
    private final Schema projection;
    private final Predicate<Row> predicate;
    private final Map<String, String> properties;

    private TextFilterRead(
        FileIO io, Schema projection, Predicate<Row> predicate, Map<String, String> properties) {
      this.io = io;
      this.projection = projection;
      this.predicate = predicate;
      this.properties = properties;
    }

    IndexCandidates candidates(List<FileScanTask> tasks) {
      Map<String, long[]> admitted = Maps.newHashMap();
      for (FileScanTask task : tasks) {
        long[] positions =
            IndexEntries.textFilterPositions(io, task, projection, predicate, properties);
        if (positions.length > 0) {
          admitted.put(task.file().location(), positions);
        }
      }
      return IndexCandidates.of(admitted);
    }
  }

  private static List<List<IndexSearchResult>> searchText(
      SparkSession spark,
      IndexMetadata metadata,
      Table table,
      TextIndexAdapter adapter,
      List<IndexQuery> queries,
      TextFilterRead filter,
      Map<String, FileScanTask> filteredTasks) {
    int[] limits = queries.stream().mapToInt(IndexQuery::topK).toArray();

    IndexQuery first = queries.get(0);
    IndexReader reader = new IndexReader(metadata, table);
    List<IndexFile> files = reader.indexFiles(first);
    Set<String> terms = new HashSet<>();
    List<byte[]> payloads = Lists.newArrayListWithCapacity(queries.size());
    for (int i = 0; i < queries.size(); i++) {
      IndexQuery query = queries.get(i);
      Preconditions.checkArgument(
          query.sourceTableSnapshotId() == first.sourceTableSnapshotId()
              && query.consistency() == first.consistency(),
          "BM25 batch queries must use the same source snapshot and consistency");
      payloads.add(toBytes(query.payload()));
      if (limits[i] > 0) {
        terms.addAll(adapter.queryTerms(query.payload()));
      }
    }

    Comparator<Tuple3<String, Long, Double>> order = hitOrder(adapter);
    if (files.isEmpty() || terms.isEmpty()) {
      return orderedHits(Map.of(), limits, order);
    }

    List<IndexFileTask> searchShards = Lists.newArrayListWithCapacity(files.size());
    for (IndexFile file : files) {
      List<FileScanTask> shardTasks = Lists.newArrayList();
      if (filteredTasks != null) {
        for (String location : file.coveredDataFiles()) {
          FileScanTask task = filteredTasks.get(location);
          if (task != null) {
            shardTasks.add(task);
          }
        }
      }

      if (filteredTasks == null || !shardTasks.isEmpty()) {
        searchShards.add(
            new IndexFileTask(
                file.location(), file.entryCount(), recordedLength(file), shardTasks));
      }
    }

    if (searchShards.isEmpty()) {
      return orderedHits(Map.of(), limits, order);
    }

    JavaSparkContext context = JavaSparkContext.fromSparkContext(spark.sparkContext());
    String owner = table.location();
    String readerKey = adapterKey(adapter);
    // Query cache lookup uses only the manifest and residency metadata. A shard that predates
    // memory records can still be loaded explicitly, with its actual charge measured by the worker.
    long workingSet =
        ExecutorIndexCache.Config.enabled(spark)
                && files.stream()
                    .allMatch(file -> IndexCacheMemory.compatible(file.memory(), adapter))
            ? workingSetBytes(table.io(), files, adapter, owner)
            : -1L;
    ExecutorIndexCache.Config config = ExecutorIndexCache.Config.from(spark, workingSet, owner);
    IndexCacheLocations locationsTable = IndexCacheLocations.forContext(context.sc());
    boolean useResidentCache = config.enabled();
    boolean locality = useResidentCache && localityEnabled(spark);
    List<String> keys =
        cacheKeys(owner, readerKey, searchShards.stream().map(IndexFileTask::location).toList());
    var cacheReports = cacheReports(context, config);
    String queryRun = java.util.UUID.randomUUID().toString();
    Broadcast<FileIO> io = context.broadcast(table.io());
    Broadcast<TextIndexAdapter> scorer = context.broadcast(adapter);
    Broadcast<TextFilterRead> filterRef = filter != null ? context.broadcast(filter) : null;
    try {
      String publishedStatistics = reader.statisticsLocation(first);
      CorpusStatistics corpus =
          publishedStatistics == null
              ? mergeTextStatistics(
                  context,
                  files,
                  io,
                  scorer,
                  terms,
                  owner,
                  readerKey,
                  locationsTable,
                  locality,
                  useResidentCache)
              : adapter.snapshotStatistics(table.io().newInputFile(publishedStatistics), terms);
      Broadcast<CorpusStatistics> statistics = context.broadcast(corpus);
      Broadcast<List<byte[]>> encodedQueries = context.broadcast(payloads);
      try {
        JavaPairRDD<Integer, Tuple3<String, Long, Double>> hits =
            IndexFileRdds.tasks(context.sc(), searchShards, keys, locationsTable, locality)
                .flatMapToPair(
                    file -> {
                      IndexCandidates candidates =
                          filterRef == null ? null : filterRef.value().candidates(file.tasks());
                      if (candidates != null && candidates.isEmpty()) {
                        return Collections.emptyIterator();
                      }

                      List<Tuple2<Integer, Tuple3<String, Long, Double>>> shardHits =
                          Lists.newArrayList();
                      TextIndexAdapter text = scorer.value();
                      List<byte[]> queriesForShard = encodedQueries.value();
                      CorpusStatistics corpusForShard = statistics.value();
                      List<List<IndexSearchResult>> results;
                      try (ExecutorIndexCache.Lease lease =
                          useResidentCache
                              ? ExecutorIndexCache.get()
                                  .acquireManagedKey(cacheKey(owner, readerKey, file.location()))
                              : null) {
                        results =
                            lease == null
                                ? text.searchShardBatch(
                                    inputFile(io.value(), file.location(), file.fileLength()),
                                    queriesForShard,
                                    limits,
                                    corpusForShard,
                                    candidates)
                                : lease
                                    .file()
                                    .searching(
                                        prepared ->
                                            text.searchShardBatch(
                                                prepared,
                                                queriesForShard,
                                                limits,
                                                corpusForShard,
                                                candidates));
                      }
                      reportCache(cacheReports, queryRun);
                      for (int queryId = 0; queryId < results.size(); queryId++) {
                        for (IndexSearchResult hit : results.get(queryId)) {
                          shardHits.add(
                              new Tuple2<>(
                                  queryId,
                                  new Tuple3<>(hit.dataFile(), hit.rowPosition(), hit.score())));
                        }
                      }
                      return shardHits.iterator();
                    });

        int widest = Arrays.stream(limits).max().orElseThrow();
        Map<Integer, List<Tuple3<String, Long, Double>>> top = aggregateHits(hits, order, widest);
        mergeCacheReports(cacheReports, locationsTable, keys, config, workingSet);
        return orderedHits(top, limits, order);
      } finally {
        encodedQueries.destroy();
        statistics.destroy();
      }
    } finally {
      if (filterRef != null) {
        filterRef.destroy();
      }
      scorer.destroy();
      io.destroy();
    }
  }

  private static CorpusStatistics mergeTextStatistics(
      JavaSparkContext context,
      List<IndexFile> files,
      Broadcast<FileIO> io,
      Broadcast<TextIndexAdapter> scorer,
      Set<String> terms,
      String owner,
      String readerKey,
      IndexCacheLocations locationsTable,
      boolean locality,
      boolean useResidentCache) {
    List<String> locations = files.stream().map(IndexFile::location).toList();
    Broadcast<Set<String>> queryTerms = context.broadcast(terms);
    try {
      return IndexFileRdds.javaRdd(
              context.sc(),
              locations,
              cacheKeys(owner, readerKey, locations),
              locationsTable,
              locality)
          .map(
              file -> {
                TextIndexAdapter text = scorer.value();
                Set<String> selectedTerms = queryTerms.value();
                try (ExecutorIndexCache.Lease lease =
                    useResidentCache
                        ? ExecutorIndexCache.get()
                            .acquireManagedKey(cacheKey(owner, readerKey, file))
                        : null) {
                  return lease == null
                      ? text.shardStatistics(io.value().newInputFile(file), selectedTerms)
                      : lease
                          .file()
                          .searching(prepared -> text.shardStatistics(prepared, selectedTerms));
                }
              })
          .treeReduce(CorpusCounts::merge, 2)
          .corpus();
    } finally {
      queryTerms.destroy();
    }
  }

  private static Map<Integer, List<Tuple3<String, Long, Double>>> aggregateHits(
      JavaPairRDD<Integer, Tuple3<String, Long, Double>> fileHits,
      Comparator<Tuple3<String, Long, Double>> order,
      int mergeLimit) {
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
    return fileHits
        .aggregateByKey(Lists.<Tuple3<String, Long, Double>>newArrayList(), mergeHit, mergeHits)
        .collectAsMap();
  }

  private static List<List<IndexSearchResult>> orderedHits(
      Map<Integer, List<Tuple3<String, Long, Double>>> topByQuery,
      int[] limits,
      Comparator<Tuple3<String, Long, Double>> order) {
    List<List<IndexSearchResult>> perQuery = Lists.newArrayListWithCapacity(limits.length);
    for (int queryId = 0; queryId < limits.length; queryId++) {
      List<Tuple3<String, Long, Double>> top =
          topByQuery.getOrDefault(queryId, Lists.newArrayList());
      perQuery.add(hitsOf(top, limits[queryId], order));
    }

    return perQuery;
  }

  /** The best hits of a query, at most {@code limit} of them, in the order of the merge. */
  private static List<IndexSearchResult> hitsOf(
      List<Tuple3<String, Long, Double>> candidates,
      int limit,
      Comparator<Tuple3<String, Long, Double>> order) {
    List<Tuple3<String, Long, Double>> top = bounded(candidates, order, limit);
    List<IndexSearchResult> hits = Lists.newArrayListWithCapacity(top.size());
    for (Tuple3<String, Long, Double> hit : top) {
      hits.add(IndexSearchResult.of(hit._1(), hit._2(), hit._3()));
    }

    return ImmutableList.copyOf(hits);
  }

  /** Wraps the hits of a search that only ever sees its own top-k, so it cannot report more. */
  private static List<SearchOutcome> outcomesWithoutMore(List<List<IndexSearchResult>> perQuery) {
    List<SearchOutcome> outcomes = Lists.newArrayListWithCapacity(perQuery.size());
    for (List<IndexSearchResult> hits : perQuery) {
      outcomes.add(new SearchOutcome(hits, false));
    }

    return outcomes;
  }

  /**
   * Keeps the best {@code limit} hits of a query and drops the rest.
   *
   * <p>The result is ordered even when nothing is dropped: a list shorter than the limit arrives in
   * whatever order the merge produced, and two hits with the same score would then reach the caller
   * in an order that depends on how the tasks were scheduled. The order of a merge is a function of
   * the scores and of the data file and position that break ties, so a query has one answer.
   */
  private static List<Tuple3<String, Long, Double>> bounded(
      List<Tuple3<String, Long, Double>> hits,
      Comparator<Tuple3<String, Long, Double>> order,
      int limit) {
    List<Tuple3<String, Long, Double>> sorted = Lists.newArrayList(hits);
    sorted.sort(order);
    return Lists.newArrayList(sorted.subList(0, Math.min(limit, sorted.size())));
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
   * <p>Called from every task of a query, because each task is what puts one more index file in the
   * cache: a report is a snapshot, so a report from the first task of a query describes a cache
   * that is still filling, and the coverage the driver derives from it is too low. The cached keys
   * travel with every report (eight bytes per file); the charge projection travels once per query,
   * which is what keeps a query over a thousand files from pushing a thousand copies of a map.
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

    reports.add(ExecutorIndexCache.get().reportForTask(queryId, executor, host));
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

  /**
   * The retained-state estimate of one index file under this reader and runtime, or -1 when
   * unknown.
   *
   * <p>A manifest record matching the current model, reader and runtime is trusted as it is;
   * otherwise the legacy rule bounds the estimate from the length of the file, which costs a stat
   * rather than a read of its payload. The value is the retained prepared state P, not the
   * container or encoded payload lengths, which are diagnostic only.
   */
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

    IndexCacheMemory.Estimates estimates = new IndexCacheMemory.Estimates();
    String readerKey = adapterKey(adapter);
    try {
      List<IndexCacheProtocol.File> planned = new ArrayList<>();
      for (IndexFile file : files) {
        IndexFileMemory memory = estimates.resolve(io, file, adapter);
        if (memory == null) {
          return unknownWorkingSet("the retained size of one of the files is unknown");
        }
        planned.add(
            IndexCacheProtocol.File.builder()
                .location(file.location())
                .cacheKey(cacheKey(owner, readerKey, file.location()))
                .owner(owner)
                .memory(memory)
                .estimatedBytes(IndexCacheMemory.residentBytes(memory))
                .build());
      }

      return ExecutorIndexCache.Shape.empty().project(null, 0, planned).charge().usedBytes();
    } catch (RuntimeException e) {
      return -1;
    } finally {
      estimates.log("query working set");
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

  private static boolean rangeReadEnabled(SparkSession spark, IndexAdapter adapter) {
    String key = "spark.iceberg.index.vector.ivf.range-read.enabled";
    return adapter instanceof VectorIndexAdapter
        && adapter.definition() instanceof VectorIndexDefinition definition
        && definition.structure() == VectorIndexDefinition.VectorStructure.IVF
        && Boolean.parseBoolean(definition.properties().get("ivf-list-blocks"))
        && Boolean.parseBoolean(
            spark.conf().get(key, spark.sparkContext().getConf().get(key, "false")));
  }

  private static void logReadTimes(
      String location, OpenedSearch opened, long openNanos, long searchNanos) {
    if (!LOG.isDebugEnabled()) {
      return;
    }

    String name = location.substring(location.lastIndexOf('/') + 1);
    long listNanos = 0L;
    if (opened.rangeSearch() != null) {
      VectorIndexAdapter.RangeReadMetrics metrics = opened.rangeSearch().listReadMetrics();
      listNanos = metrics.nanos();
      LOG.debug(
          "index file {} IVF list I/O {} ms, {} bytes, {} reads, {} whole-list-area reads",
          name,
          TimeUnit.NANOSECONDS.toMillis(listNanos),
          metrics.bytes(),
          metrics.requests(),
          opened.rangeSearch().wholeListAreaReads());
      VectorIndexAdapter.OpenReadMetrics open = opened.rangeSearch().openReadMetrics();
      if (open != null) {
        LOG.debug(
            "index file {} open read {} opens, {} length probes, footer {} bytes/{} reads,"
                + " description {} bytes/{} reads, lists {} bytes/{} reads",
            name,
            open.opens(),
            open.lengthRequests(),
            open.footerBytes(),
            open.footerRequests(),
            open.descriptionBytes(),
            open.descriptionRequests(),
            open.listBytes(),
            open.listRequests());
      }
    }

    LOG.debug(
        "index file {} read {} ms, search {} ms",
        name,
        TimeUnit.NANOSECONDS.toMillis(openNanos + listNanos),
        TimeUnit.NANOSECONDS.toMillis(searchNanos - listNanos));
  }

  static final class OpenedSearch implements AutoCloseable {
    private final ExecutorIndexCache.CachedFile file;
    private final Runnable release;
    private final VectorIndexAdapter.RangeSearch rangeSearch;
    private final long fileBytesRead;
    private final boolean servedFromCache;

    private OpenedSearch(
        ExecutorIndexCache.CachedFile file,
        Runnable release,
        VectorIndexAdapter.RangeSearch rangeSearch,
        long fileBytesRead,
        boolean servedFromCache) {
      this.file = file;
      this.release = release;
      this.rangeSearch = rangeSearch;
      this.fileBytesRead = fileBytesRead;
      this.servedFromCache = servedFromCache;
    }

    ExecutorIndexCache.CachedFile file() {
      return file;
    }

    VectorIndexAdapter.RangeSearch rangeSearch() {
      return rangeSearch;
    }

    /** The bytes this open read to load an index file whole, and zero when it read none. */
    long fileBytesRead() {
      return fileBytesRead;
    }

    /** Whether an executor already held the file, so this open went to no storage. */
    boolean servedFromCache() {
      return servedFromCache;
    }

    @Override
    public void close() {
      release.run();
    }
  }

  static OpenedSearch openSearch(
      FileIO io,
      String location,
      IndexAdapter adapter,
      ExecutorIndexCache.Config config,
      String readerKey,
      boolean rangeRead,
      long knownLength) {
    ExecutorIndexCache cache = ExecutorIndexCache.get();
    String key = cacheKey(config.owner(), readerKey, location);
    // the switch is read by the load side and by the query side or by neither: a resident copy
    // that an earlier load installed must not be leased after the switch was turned off, which is
    // what makes a session that disables the cache return to reading its index files
    ExecutorIndexCache.Lease lease = config.enabled() ? cache.acquireManagedKey(key) : null;
    if (lease != null) {
      return new OpenedSearch(lease.file(), lease::close, null, 0L, true);
    }

    if (rangeRead && adapter instanceof VectorIndexAdapter vector) {
      ExecutorIndexCache.CachedFile hit = cache.unmanagedHit(key, config);
      if (hit != null) {
        return new OpenedSearch(hit, () -> {}, null, 0L, true);
      }

      VectorIndexAdapter.RangeSearch search =
          vector.prepareRange(inputFile(io, location, knownLength), knownLength);
      if (search != null) {
        return new OpenedSearch(
            ExecutorIndexCache.loaded(0L, search), search::close, search, 0L, false);
      }
    }

    // the load is where a miss pays for the file; a hit never calls it, which is what tells the
    // two apart without asking the cache
    long[] loadedBytes = new long[] {0L};
    ExecutorIndexCache.CachedFile file =
        cache.file(
            location,
            config,
            readerKey,
            sizeHint(io, location, knownLength),
            () -> {
              LoadedFile loaded = loadFileState(io, location, adapter, knownLength);
              loadedBytes[0] = loaded.fileBytes();
              return loaded.file();
            });
    return new OpenedSearch(file, () -> {}, null, loadedBytes[0], loadedBytes[0] == 0L);
  }

  static ExecutorIndexCache.CachedFile loadFile(FileIO io, String location, IndexAdapter adapter) {
    return loadFile(io, location, adapter, 0L);
  }

  static ExecutorIndexCache.CachedFile loadFile(
      FileIO io, String location, IndexAdapter adapter, long knownLength) {
    return loadFileState(io, location, adapter, knownLength).file();
  }

  static LoadedFile loadFileState(FileIO io, String location, IndexAdapter adapter) {
    return loadFileState(io, location, adapter, 0L);
  }

  /**
   * Reads and prepares one index file, using a length the manifest already recorded when there is
   * one.
   *
   * <p>Without the length a load asks storage for it three times: once to reserve room in the
   * cache, once to bound the payload size before reading, and once inside the container reader to
   * locate the footer. A known length answers all three without a request.
   */
  static LoadedFile loadFileState(
      FileIO io, String location, IndexAdapter adapter, long knownLength) {
    InputFile input = inputFile(io, location, knownLength);
    long length = knownLength > 0 ? knownLength : input.getLength();
    if (length > IndexPayloadLimits.MAX_PAYLOAD_BYTES) {
      throw new IllegalArgumentException(IndexPayloadLimits.tooLargeToRead(length, location));
    }
    PreparedFile prepared = prepareFile(input, adapter, knownLength);
    ExecutorIndexCache.CachedFile file =
        ExecutorIndexCache.loaded(prepared.payloadBytes(), prepared.searcher())
            .withContext(adapter);
    return new LoadedFile(file, length, prepared.payloadBytes(), file.preparedBytes());
  }

  /** Opens an index file, using the length the manifest recorded when it recorded one. */
  private static InputFile inputFile(FileIO io, String location, long knownLength) {
    return knownLength > 0 ? io.newInputFile(location, knownLength) : io.newInputFile(location);
  }

  /**
   * The length of a file, which a load reserves room against before it reads anything.
   *
   * <p>It is a lower bound of what the prepared structure costs, and it is free to ask for when the
   * manifest recorded it. Without it a load would have to reserve the whole budget, which would let
   * one load hold the cache while it prepares and serialise the loads of a single query.
   */
  private static long sizeHint(FileIO io, String location, long knownLength) {
    if (knownLength > 0) {
      return knownLength;
    }

    try {
      return io.newInputFile(location).getLength();
    } catch (RuntimeException e) {
      // a length is a hint for sizing, never a reason to fail a read that will report the error
      return 0L;
    }
  }

  /**
   * The length the index manifest recorded for an index file, which is where a search learns it
   * without asking storage.
   */
  static long recordedLength(IndexFile file) {
    IndexFileMemory memory = file.memory();
    return memory != null && memory.fileBytes() != null ? memory.fileBytes() : 0L;
  }

  /**
   * One task per index file, carrying what an open needs. A search that does not read rows still
   * ships an empty task list rather than a second kind of partition.
   */
  private static List<IndexFileTask> indexFileTasks(List<IndexFile> files) {
    List<IndexFileTask> tasks = Lists.newArrayListWithCapacity(files.size());
    for (IndexFile file : files) {
      tasks.add(
          new IndexFileTask(file.location(), file.entryCount(), recordedLength(file), List.of()));
    }

    return tasks;
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
  private static PreparedFile prepareFile(InputFile input, IndexAdapter adapter, long knownLength) {
    // every type the adapter can still read, not just the one it writes: an index built before a
    // rename is still on tables, and a reader that only knew the current name would skip it
    java.util.Set<String> blobTypes =
        adapter.writesAuxiliaryFiles()
            ? adapter.readableAuxiliaryBlobTypes()
            : adapter.readableEntryBlobTypes();

    List<IndexBlob> blobs;
    try {
      blobs = new PuffinIndexFileContainer().read(input, knownLength);
    } catch (java.io.IOException e) {
      throw new java.io.UncheckedIOException("Failed to read an index file", e);
    }

    for (IndexBlob blob : blobs) {
      if (!blobTypes.contains(blob.type())) {
        continue;
      }

      byte[] payload = blob.takePayload();
      long payloadBytes = payload.length;
      try {
        IndexAdapter.PreparedSearch searcher =
            adapter.writesAuxiliaryFiles()
                ? adapter.prepareAuxiliaryOwned(payload)
                : adapter.prepareOwned(payload);
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
