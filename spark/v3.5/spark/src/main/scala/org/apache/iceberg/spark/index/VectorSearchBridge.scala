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
package org.apache.iceberg.spark.index

import java.lang.{Long => JLong}
import java.nio.ByteBuffer
import java.util.{Comparator, List => JList, PriorityQueue}
import org.apache.iceberg.index.IndexQuery
import org.apache.iceberg.index.IndexQuery.Consistency
import org.apache.iceberg.index.IndexSearchResult
import org.apache.iceberg.index.spark.DistributedIndexSearch
import org.apache.iceberg.index.spark.VectorIndexSearch
import org.apache.iceberg.index.spark.IndexEntries
import org.apache.iceberg.index.spark.IndexSearch
import org.apache.iceberg.index.vector.VectorDistance
import org.apache.iceberg.index.vector.VectorIndexAdapter
import org.apache.iceberg.index.vector.VectorIndexDefinition
import org.apache.spark.sql.Row
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * The shared entry point for a vector search whose result keeps the index's own hits: {@code CALL
 * vector_search} and the vector table functions.
 *
 * <p>Planning a request does what the ordinary SQL path does: it binds the table and the index,
 * freezes both snapshots, and decides only the parameters the caller left unset. Execution then
 * searches the frozen index snapshot and resolves the rows from the frozen table snapshot, so a
 * refresh or an append after planning cannot make one query read two snapshots.
 *
 * <p>This contract is not the complete-table one. The rows are the index's own ranked hits with
 * their raw scores, a hit whose row was removed still appears with a null row, and the files the
 * index does not cover are not scanned. Sharing the parameter decision does not change what a
 * caller gets back.
 */
private[spark] object VectorSearchBridge {

  /** The least a row can cost, for a column whose type declares no size of its own. */
  private[spark] val BytesPerRowFloor = 8L

  /** The largest number of queries one shared execution answers. */
  private val MaxQueriesPerBlock = 128

  /**
   * The system property that overrides the block size, for measurement and tests only.
   *
   * <p>It is read from the JVM rather than from the session's configuration, so it is not a
   * user-facing setting: a caller that wants a different block size measures it, it does not
   * configure it.
   */
  private val QueriesPerBlockProperty = "iceberg.index.batch.queries-per-block"

  /** What one query payload may take of the broadcast a block sends to its tasks. */
  private val QueryPayloadBudget = 8L * 1024 * 1024

  /** What the driver may hold of one block's candidates and rows. */
  private val DriverBudget = 8L * 1024 * 1024

  /** What one block's candidates may take of the shuffle. */
  private val ShuffleBudget = 128L * 1024 * 1024

  /** The least a candidate and its bounded-heap entry are estimated to take. */
  private val CandidateBytes = 128L

  /** What one fetched row costs besides its vector: the row wrapper and its other columns. */
  private val RowOverheadBytes = 256L

  /**
   * What serialising one vector element to JSON costs.
   *
   * <p>A block holds the JSON of the rows it answers with, not just their rows, so the driver bound
   * counts both. A double is written as at most about 18 characters plus a separator, so 20 bytes
   * per element is a conservative estimate for either a double or a float column.
   */
  private val JsonBytesPerDimension = 20L

  /** The bytes of a JSON object besides its values: keys, braces and separators. */
  private val JsonOverheadBytes = 32L

  /** The bytes one query vector takes in a broadcast. */
  private val BytesPerFloat = 4L

  /** The rounds a query may widen its candidate window before it reports what it has. */
  private val MaxRounds = 10

  /** A candidate of one query: where it is, how far it is, and its row once it was read. */
  private case class Candidate(dataFile: String, position: Long, distance: Double, row: Row)

  /**
   * Nearest first, and by where the row is when two distances are equal.
   *
   * <p>A distance alone leaves two hits in whatever order they arrived in, which is the order the
   * tasks happened to produce: a cached query and an uncached one then answer the same question
   * differently, and so do two runs of the same query. The data file and the row position make the
   * order a function of the question rather than of the scheduling.
   */
  private val CandidateOrdering: Ordering[Candidate] =
    Ordering.by[Candidate, (Double, String, Long)](candidate =>
      (candidate.distance, candidate.dataFile, candidate.position))

  /** The same order for a candidate on its way back from the cluster, before it is a row. */
  private val ScoredOrdering: Ordering[(Double, String, Long)] =
    Ordering.by[(Double, String, Long), (Double, String, Long)](scored => scored)

  /** The reverse order, so a bounded heap of candidates polls its worst one first. */
  private object WorstFirst extends Comparator[(Double, String, Long)] with Serializable {
    override def compare(left: (Double, String, Long), right: (Double, String, Long)): Int =
      -ScoredOrdering.compare(left, right)
  }

  /**
   * How many queries one shared execution answers.
   *
   * <p>Estimates the broadcast, driver and shuffle costs of a block. At least one query is
   * processed at a time, even if its estimated cost exceeds one of the budgets.
   */
  /**
   * The probe count the call resolved, or null when it named none and the encoder reads the
   * session's.
   *
   * <p>The parameters are decided once per call, so the encoding of every query of the call uses
   * the same count. A query planned by the rewrite names none, so it keeps the session's setting.
   */
  private def nprobeOf(context: VectorSearchContext): Integer = {
    if (context.parameters == null || context.parameters.nprobe <= 0L) {
      null
    } else {
      Integer.valueOf(context.parameters.nprobe.toInt)
    }
  }

  private def queriesPerBlock(context: VectorSearchContext): Int = {
    val configured = System.getProperty(QueriesPerBlockProperty, null)
    if (configured != null && configured.trim.nonEmpty) {
      val value =
        try {
          configured.trim.toInt
        } catch {
          case _: NumberFormatException =>
            throw new IllegalArgumentException(
              s"Invalid $QueriesPerBlockProperty: '$configured', expected a positive integer")
        }
      require(
        value > 0,
        s"Invalid $QueriesPerBlockProperty: '$configured', expected a positive integer")
      // the override is for measurement, and a block of the whole batch would defeat the bound it
      // is there to measure
      return math.min(value, MaxQueriesPerBlock)
    }

    val inputs = context.inputs
    derivedQueriesPerBlock(
      context.request.input.contract,
      context.request.input.topK,
      if (context.budget == null) 0 else context.budget.initial,
      inputs.definition.dimension(),
      inputs.indexFiles.size,
      inputs.uncoveredDataFiles.size)
  }

  /**
   * Estimates how many queries a block may answer under the broadcast, driver and shuffle budgets.
   *
   * <p>The parameters describe one batch, in the order the estimates use them: the contract (only a
   * whole-table contract reads the files the index does not cover), the result count, the query's
   * initial candidate width, the vector dimension, and the index-file and uncovered-file counts.
   * The result is at least one: a single query whose estimate exceeds a budget is still answered.
   */
  private[index] def derivedQueriesPerBlock(
      contract: VectorResultContract,
      k: Int,
      initialWindow: Int,
      vectorDimension: Int,
      indexFileCount: Int,
      uncoveredFileCount: Int): Int = {
    val topK = math.max(1, k).toLong
    // the width a query starts with, which is k times the multiplier: sizing a block by k alone
    // would let a caller with over-sample raise the real per-block memory behind the bound
    val window = math.max(1, initialWindow).toLong
    val dimension = math.max(1, vectorDimension).toLong
    // a block of positions holds neither rows nor their JSON: the answer is the index's own hits
    val holdsRows = contract != VectorResultContract.IndexPositions
    val rowBytes = if (holdsRows) dimension * java.lang.Double.BYTES + RowOverheadBytes else 0L
    // the block also keeps the JSON of the rows it answers with, which is not in their row size
    val jsonBytes =
      if (holdsRows) dimension * JsonBytesPerDimension + JsonOverheadBytes else 0L
    val indexFiles = math.max(1, indexFileCount).toLong
    val uncoveredFiles =
      if (contract == VectorResultContract.TableCovered) uncoveredFileCount.toLong else 0L
    val byPayload = QueryPayloadBudget / (dimension * BytesPerFloat)
    // Indexed rows are still live when the winners from the uncovered scan are fetched, and the
    // answered rows of the block also carry their JSON
    val scanRows = if (uncoveredFiles > 0L) topK else 0L
    val perQuery = (window + scanRows) * rowBytes + topK * jsonBytes
    val byDriver = if (perQuery <= 0L) Long.MaxValue else DriverBudget / perQuery
    // The two stages shuffle separately. The index asks for one extra hit to detect more hits.
    val hitsPerIndexFile = math.min(Int.MaxValue.toLong, window + 1L)
    val byIndexShuffle = ShuffleBudget / CandidateBytes / indexFiles / hitsPerIndexFile
    val byScanShuffle =
      if (uncoveredFiles > 0L) ShuffleBudget / CandidateBytes / uncoveredFiles / topK
      else Long.MaxValue
    val byShuffle = math.min(byIndexShuffle, byScanShuffle)
    val derived = math.min(byPayload, math.min(byDriver, byShuffle))
    math.max(1L, math.min(MaxQueriesPerBlock.toLong, derived)).toInt
  }

  /** A request whose contract is the index's own hits, planned but not searched. */
  def plan(
      spark: SparkSession,
      table: org.apache.iceberg.Table,
      column: String,
      indexArgument: String,
      queries: JList[Array[Float]],
      k: Int,
      batch: Boolean,
      coverage: String): VectorBindingResult =
    plan(spark, table, column, indexArgument, queries, k, batch, coverage, null, null, null)

  def plan(
      spark: SparkSession,
      table: org.apache.iceberg.Table,
      column: String,
      indexArgument: String,
      queries: JList[Array[Float]],
      k: Int,
      batch: Boolean,
      coverage: String,
      nprobeArgument: String,
      overSampleArgument: String = null,
      filterModeArgument: String = null): VectorBindingResult = {
    val contract = VectorResultContract.forCoverage(coverage)
    val request = VectorSearchRequest.forTable(
      table,
      column,
      queries,
      k,
      // the request is planned against the table's current snapshot; a caller that has already
      // frozen one hands it to the search, not to the plan
      null,
      contract.name,
      indexArgument,
      batch,
      nprobeArgument,
      overSampleArgument,
      filterModeArgument)
    BindVectorSearch.bind(spark, request) match {
      case BoundVectorSearch(context) =>
        // a request that asks for nothing is answered with nothing: its parameters are not judged
        // against a search that will not happen
        if (k <= 0 || queries.isEmpty) {
          return BoundVectorSearch(
            context.copy(
              parameters = null,
              budget = VectorCandidateBudget(0L, 0, context.inputs.entryUpperBound)))
        }
        val parameters =
          VectorQueryParameters.decide(context.rawParameters, context.inputs.definition)
        BoundVectorSearch(
          context.copy(
            parameters = parameters,
            budget = VectorCandidateBudget
              .derive(k, parameters.overSample, context.inputs.entryUpperBound)))
      case other => other
    }
  }

  /**
   * Searches a planned request, and returns what its contract promises.
   *
   * <p>The hit view returns the index's own hits; the two coverage modes return the live rows of the
   * requested snapshot, scored with the metric the query asks for, which is the number
   * {@code ORDER BY vec_distance} would produce for the same row.
   */
  def execute(spark: SparkSession, context: VectorSearchContext): JList[Row] = {
    val contract = context.request.input.contract
    if (contract == VectorResultContract.IndexPositions) {
      return indexPositions(spark, context)
    }

    if (contract != VectorResultContract.IndexHits) {
      return nearestRows(spark, context)
    }

    val request = context.request.input
    val batch = request.batch
    val schema = if (batch) IndexSearch.RESULT_SCHEMA else IndexSearch.SINGLE_RESULT_SCHEMA
    if (request.topK <= 0 || request.queries.isEmpty) {
      return spark.createDataFrame(new java.util.ArrayList[Row](), schema).collectAsList()
    }

    // one block of queries at a time: the block shares one load of every index file and one row
    // resolution, so a data file several queries hit is read once for the whole block
    spark.createDataFrame(hitBlocks(spark, context).flatten.toSeq.asJava, schema).collectAsList()
  }

  /**
   * The index's own hits of a request, one block at a time.
   *
   * <p>Each block is searched, resolved and serialised when the iterator's next element is asked
   * for, so a caller that consumes the blocks as they arrive holds one block's rows rather than the
   * whole batch's. The procedure collects them because its answer is a result set; the table
   * function's operator turns each block into a partition instead.
   */
  private[spark] def hitBlocks(
      spark: SparkSession,
      context: VectorSearchContext): Iterator[Seq[Row]] = {
    val request = context.request.input
    if (request.topK <= 0 || request.queries.isEmpty) {
      return Iterator.empty
    }

    val blockSize = queriesPerBlock(context)
    (0 until request.queries.size by blockSize).iterator.map { start =>
      val end = math.min(start + blockSize, request.queries.size)
      val block = (start until end).map(queryId => (queryId, request.queries(queryId)))
      indexHitsInBlock(spark, context, block)
    }
  }

  /**
   * The index's own hits of every query, as positions and scores only.
   *
   * <p>No data file is read and no hit is judged against the snapshot: the answer is what the index
   * ranked, so a row deleted after the build is still reported here. That is the trade the coverage
   * mode makes: the caller gets the index's own answer and pays for neither the row nor its JSON,
   * which is what lets one block hold many more queries.
   */
  private def indexPositions(spark: SparkSession, context: VectorSearchContext): JList[Row] = {
    val request = context.request.input
    val batch = request.batch
    val schema = if (batch) IndexSearch.RESULT_SCHEMA else IndexSearch.SINGLE_RESULT_SCHEMA
    if (request.topK <= 0 || request.queries.isEmpty) {
      return spark.createDataFrame(new java.util.ArrayList[Row](), schema).collectAsList()
    }

    val inputs = context.inputs
    val adapter = new VectorIndexAdapter(inputs.definition)
    val k = request.topK
    val blockSize = queriesPerBlock(context)
    val answered = mutable.ListBuffer.empty[Row]
    var start = 0
    while (start < request.queries.size) {
      val end = math.min(start + blockSize, request.queries.size)
      val block = (start until end).map(queryId => (queryId, request.queries(queryId)))
      val payloads = block.map { case (_, query) =>
        VectorIndexSearch.encodeQuery(spark, adapter, query, nprobeOf(context))
      }

      val hitsPerQuery = hitsForBlock(spark, context, block, payloads)
      hitsPerQuery.zipWithIndex.foreach { case (hits, query) =>
        val queryId = block(query)._1
        hits.take(k).foreach { hit =>
          answered += (if (batch) {
                         RowFactory.create(
                           Integer.valueOf(queryId),
                           hit.dataFile(),
                           JLong.valueOf(hit.rowPosition()),
                           java.lang.Double.valueOf(hit.score()),
                           null)
                       } else {
                         RowFactory.create(
                           hit.dataFile(),
                           JLong.valueOf(hit.rowPosition()),
                           java.lang.Double.valueOf(hit.score()),
                           null)
                       })
        }
      }

      start = end
    }

    spark.createDataFrame(answered.asJava, schema).collectAsList()
  }

  /**
   * The index's own hits of one block of queries, with their rows resolved in one pass.
   *
   * <p>A hit whose row was removed is kept: this contract reports what the index found and lets the
   * caller see the missing row, which is what makes the score comparable with the one the index
   * produced. A data file two queries hit is read once for the whole block.
   */
  private def indexHitsInBlock(
      spark: SparkSession,
      context: VectorSearchContext,
      block: Seq[(Int, Array[Float])]): Seq[Row] = {
    val request = context.request.input
    val batch = request.batch
    val inputs = context.inputs
    val adapter = new VectorIndexAdapter(inputs.definition)
    val payloads =
      block.map { case (_, query) =>
        VectorIndexSearch.encodeQuery(spark, adapter, query, nprobeOf(context))
      }

    val hitsPerQuery = hitsForBlock(spark, context, block, payloads)
    val json = resolveRows(spark, context, hitsPerQuery.flatten)

    val rows = mutable.ListBuffer.empty[Row]
    hitsPerQuery.zipWithIndex.foreach { case (hits, query) =>
      val queryId = block(query)._1
      hits.foreach { hit =>
        val row = Option(json.get(hit.dataFile()))
          .flatMap(byPosition => Option(byPosition.get(JLong.valueOf(hit.rowPosition()))))
          .orNull
        val values: Array[Object] =
          if (batch) {
            Array[Object](
              Integer.valueOf(queryId),
              hit.dataFile(),
              JLong.valueOf(hit.rowPosition()),
              java.lang.Double.valueOf(hit.score()),
              row)
          } else {
            Array[Object](
              hit.dataFile(),
              JLong.valueOf(hit.rowPosition()),
              java.lang.Double.valueOf(hit.score()),
              row)
          }
        rows += RowFactory.create(values: _*)
      }
    }

    rows
  }

  /**
   * The hits of each query of a block, asking for a wider window when a query has fewer than k.
   *
   * <p>Only the queries that are still short take part in the next round; the window grows by the
   * hits still missing, and the search stops as soon as the kernel says it has nothing more to give.
   */
  private def hitsForBlock(
      spark: SparkSession,
      context: VectorSearchContext,
      block: Seq[(Int, Array[Float])],
      payloads: Seq[ByteBuffer]): Seq[Seq[IndexSearchResult]] = {
    val inputs = context.inputs
    val k = context.request.input.topK
    val hits =
      Array.fill(block.size)(mutable.LinkedHashMap.empty[(String, Long), IndexSearchResult])
    val widths = Array.fill(block.size)(context.budget.initial)
    val done = Array.fill(block.size)(false)
    var round = 0
    while (round < MaxRounds && done.exists(complete => !complete)) {
      round += 1
      val active = block.indices.filterNot(done)
      val queries = active.map { query =>
        IndexQuery
          .of(
            inputs.indexSnapshot.sourceTableSnapshotId(),
            payloads(query).duplicate(),
            widths(query))
          .withConsistency(Consistency.AT_OR_BEFORE)
      }
      val outcomes = DistributedIndexSearch
        .searchBatchWithOutcomes(spark, inputs.indexMetadata, inputs.table, queries.asJava)
        .asScala

      active.zip(outcomes).foreach { case (query, outcome) =>
        // a hit whose file this snapshot no longer has is kept: the caller sees the hit with a null
        // row, which is what this contract reports, instead of a shorter answer
        outcome.hits().asScala.foreach { hit =>
          hits(query).getOrElseUpdate((hit.dataFile(), hit.rowPosition()), hit)
        }

        if (!outcome.moreHits() || hits(query).size >= k || widths(query) == 0) {
          done(query) = true
        } else {
          val next = VectorCandidateBudget.nextOrNull(
            widths(query),
            hits(query).size,
            k,
            inputs.entryUpperBound)
          if (next == null) {
            done(query) = true
          } else {
            widths(query) = next.initial
          }
        }
      }
    }

    // the search asked for k times the multiplier, but this contract still answers one query with
    // at most k hits, in the index's own order
    block.indices.map(query => hits(query).values.take(k).toSeq)
  }

  /**
   * The nearest live rows of the requested snapshot, under the contract's coverage mode.
   *
   * <p>Every candidate is resolved against the frozen table snapshot before it can be answered with:
   * a hit whose data file the snapshot no longer has, or whose position no longer holds a live row,
   * is dropped. That is the same rule the rewritten query applies, and it is what makes a deleted
   * row impossible to return from either mode.
   *
   * <p>The table mode adds the rows of the files the index does not cover, scored the same way, and
   * takes the k nearest of both together, so its answer is the answer a scan would give. The indexed
   * mode stops at the index, so its answer can be shorter than k.
   *
   * <p>A batch is answered one block of queries at a time: the block shares one load of every index
   * file, one fetch-back for its hits and one pass over the files the index does not cover.
   */
  private def nearestRows(spark: SparkSession, context: VectorSearchContext): JList[Row] = {
    val request = context.request.input
    val schema = structOf(context.request.output)
    if (request.topK <= 0 || request.queries.isEmpty) {
      return spark.createDataFrame(new java.util.ArrayList[Row](), schema).collectAsList()
    }

    val blockSize = queriesPerBlock(context)
    val answered = mutable.ListBuffer.empty[Row]
    var start = 0
    while (start < request.queries.size) {
      val end = math.min(start + blockSize, request.queries.size)
      val block = (start until end).map(queryId => (queryId, request.queries(queryId)))
      answered ++= nearestRowsInBlock(spark, context, block)
      start = end
    }

    spark.createDataFrame(answered.asJava, schema).collectAsList()
  }

  /**
   * The nearest live rows of one block of queries, from one index scan and one row resolution.
   *
   * <p>The index candidates of the block are searched and fetched back together; the rows of the
   * files the index does not cover are scanned once for the whole block. Then each query takes the k
   * nearest of its own candidates, ordered the way a single query orders them.
   */
  private def nearestRowsInBlock(
      spark: SparkSession,
      context: VectorSearchContext,
      block: Seq[(Int, Array[Float])]): Seq[Row] = {
    val request = context.request.input
    val inputs = context.inputs
    val topK = request.topK
    val metric = inputs.definition.metric()
    val names = columnNames(context)
    val vectorIndex = names.indexOf(request.column)
    val adapter = new VectorIndexAdapter(inputs.definition)
    val payloads = block.map { case (_, query) =>
      VectorIndexSearch.encodeQuery(spark, adapter, query, nprobeOf(context))
    }

    val indexed =
      indexCandidatesForBlock(spark, context, block, payloads, names, vectorIndex, metric)
    val scanned =
      if (request.contract == VectorResultContract.IndexCovered) {
        Seq.fill(block.size)(Seq.empty[(String, Long, Double)])
      } else {
        scanCandidatesForBlock(spark, context, block, names, vectorIndex, metric)
      }

    val chosen = block.indices.map { query =>
      val candidates =
        indexed(query) ++ scanned(query).map { case (dataFile, position, distance) =>
          Candidate(dataFile, position, distance, null)
        }
      candidates.sorted(CandidateOrdering).take(topK)
    }

    // the scan kept only where its candidates are: the rows of the ones that made the answer are
    // read here, in the same pass for the whole block
    val missing = chosen.flatten
      .filter(candidate => candidate.row == null)
      .map(candidate => (candidate.dataFile, candidate.position))
      .distinct
    val fetched = rowsOfSelected(spark, context, missing)
    val resolved = chosen.map(_.map { candidate =>
      if (candidate.row != null) {
        candidate
      } else {
        fetched.get((candidate.dataFile, candidate.position)) match {
          case Some(row) => candidate.copy(row = row)
          case None => candidate
        }
      }
    })

    // one JSON per row of the block, not per query: a row two queries selected is serialised once
    val uniqueRows = mutable.LinkedHashMap.empty[(String, Long), Row]
    resolved.foreach(_.foreach { candidate =>
      if (candidate.row != null) {
        uniqueRows.getOrElseUpdate((candidate.dataFile, candidate.position), candidate.row)
      }
    })
    val json = jsonOfRows(spark, context, uniqueRows.toSeq)

    val answered = mutable.ListBuffer.empty[Row]
    resolved.zipWithIndex.foreach { case (candidates, query) =>
      val queryId = block(query)._1
      candidates.foreach { candidate =>
        val rowJson = json.getOrElse((candidate.dataFile, candidate.position), null)
        answered += (if (request.batch) {
                       RowFactory.create(
                         Integer.valueOf(queryId),
                         candidate.dataFile,
                         JLong.valueOf(candidate.position),
                         java.lang.Double.valueOf(candidate.distance),
                         rowJson)
                     } else {
                       RowFactory.create(
                         candidate.dataFile,
                         JLong.valueOf(candidate.position),
                         java.lang.Double.valueOf(candidate.distance),
                         rowJson)
                     })
      }
    }

    answered
  }

  /**
   * The live rows the index covers that are nearest to each query of a block, with their exact
   * distance.
   *
   * <p>One search answers the whole block: each index file is loaded and prepared once, then scored
   * once per query. The hits of the block are fetched back together, so a data file two queries hit
   * is read once. A query whose window fell short of k live rows asks for a wider window; only the
   * queries that are still short take part in the next round.
   */
  private def indexCandidatesForBlock(
      spark: SparkSession,
      context: VectorSearchContext,
      block: Seq[(Int, Array[Float])],
      payloads: Seq[ByteBuffer],
      names: Seq[String],
      vectorIndex: Int,
      metric: VectorIndexDefinition.Metric): Seq[Seq[Candidate]] = {
    val inputs = context.inputs
    val topK = context.request.input.topK
    val found = Array.fill(block.size)(mutable.LinkedHashMap.empty[(String, Long), Row])
    val widths = Array.fill(block.size)(context.budget.initial)
    val more = Array.fill(block.size)(true)
    val done = Array.fill(block.size)(false)
    var round = 0
    while (round < MaxRounds && done.exists(complete => !complete)) {
      round += 1
      val active = block.indices.filterNot(done)
      val queries = active.map { query =>
        IndexQuery
          .of(
            inputs.indexSnapshot.sourceTableSnapshotId(),
            payloads(query).duplicate(),
            widths(query))
          .withConsistency(Consistency.AT_OR_BEFORE)
      }
      val outcomes = DistributedIndexSearch
        .searchBatchWithOutcomes(spark, inputs.indexMetadata, inputs.table, queries.asJava)
        .asScala

      // a hit of a file the snapshot no longer has is not a candidate at all: the row it names is
      // gone, and scoring it would let a rewritten-away file occupy a place in the answer. The
      // fetch-back below drops these too — a scan task is only planned for a file the snapshot has —
      // so this is what keeps them out before the work of resolving them, not the only guard
      val positions = mutable.LinkedHashMap.empty[String, mutable.LinkedHashSet[JLong]]
      active.zip(outcomes).foreach { case (query, outcome) =>
        outcome.hits().asScala.foreach { hit =>
          val key = (hit.dataFile(), hit.rowPosition())
          if (inputs.dataFiles.contains(hit.dataFile()) && !found(query).contains(key)) {
            positions
              .getOrElseUpdate(hit.dataFile(), mutable.LinkedHashSet.empty)
              .add(JLong.valueOf(hit.rowPosition()))
          }
        }
      }

      val rows =
        if (positions.isEmpty) {
          Map.empty[String, java.util.Map[JLong, Row]]
        } else {
          IndexEntries
            .rowsAt(
              spark,
              inputs.table,
              positions.map { case (dataFile, positions) => dataFile -> positions.asJava }.asJava,
              names.toList.asJava,
              inputs.readSchema,
              inputs.scanTasks)
            .asScala
            .toMap
        }

      active.zip(outcomes).foreach { case (query, outcome) =>
        outcome.hits().asScala.foreach { hit =>
          val key = (hit.dataFile(), hit.rowPosition())
          if (inputs.dataFiles.contains(hit.dataFile()) && !found(query).contains(key)) {
            val byPosition = rows.getOrElse(hit.dataFile(), null)
            val row: Row =
              if (byPosition == null) null
              else byPosition.get(JLong.valueOf(hit.rowPosition()))
            if (row != null && (vectorIndex < 0 || !row.isNullAt(vectorIndex))) {
              found(query).put(key, row)
            }
          }
        }

        more(query) = outcome.moreHits()
        if (found(query).size >= topK || !more(query) || widths(query) == 0) {
          done(query) = true
        } else {
          val next = VectorCandidateBudget.nextOrNull(
            widths(query),
            found(query).size,
            topK,
            inputs.entryUpperBound)
          if (next == null) {
            done(query) = true
          } else {
            widths(query) = next.initial
          }
        }
      }
    }

    block.indices.map { query =>
      found(query).iterator.map { case ((dataFile, position), row) =>
        Candidate(dataFile, position, distanceOf(metric, row, vectorIndex, block(query)._2), row)
      }.toSeq
    }
  }

  /**
   * The k nearest live rows of the files the index does not cover, for every query of a block.
   *
   * <p>One pass reads the uncovered files and scores each row against every query of the block; a
   * partition keeps only the k best per query, and a bounded merge turns those into the k best of
   * each query. A row whose vector is missing, or whose vector has another dimension than the query,
   * is not a candidate for that query — the same rule the single-query scan applies.
   */
  private def scanCandidatesForBlock(
      spark: SparkSession,
      context: VectorSearchContext,
      block: Seq[(Int, Array[Float])],
      names: Seq[String],
      vectorIndex: Int,
      metric: VectorIndexDefinition.Metric): Seq[Seq[(String, Long, Double)]] = {
    val inputs = context.inputs
    val topK = context.request.input.topK
    val uncovered = inputs.uncoveredDataFiles
    if (uncovered.isEmpty || vectorIndex < 0 || topK <= 0) {
      return Seq.fill(block.size)(Seq.empty)
    }

    val rows = IndexEntries.liveRowsWithPositions(
      spark,
      inputs.table,
      uncovered.asJava,
      names.toList.asJava,
      inputs.readSchema,
      inputs.scanTasks)
    if (rows == null) {
      return Seq.fill(block.size)(Seq.empty)
    }

    val queriesRef = spark.sparkContext.broadcast(block.map(query => query._2).toArray)
    val scored = rows.rdd.mapPartitions { entries =>
      val queries = queriesRef.value
      // the heap holds at most topK entries, but pre-allocating that many for every query of every
      // partition is wasted when the file has fewer rows: a bounded start grows as needed
      val capacity = math.max(1, math.min(topK, 1024))
      val heaps =
        Array.fill(queries.length)(new PriorityQueue[(Double, String, Long)](capacity, WorstFirst))
      entries.foreach { case (dataFile, entry) =>
        val position = entry._1
        val row = entry._2
        if (!row.isNullAt(vectorIndex)) {
          val vector = vectorOf(row, vectorIndex)
          var query = 0
          while (query < queries.length) {
            val queryVector = queries(query)
            if (vector.length == queryVector.length) {
              val heap = heaps(query)
              heap.offer((VectorDistance.distance(metric, vector, queryVector), dataFile, position))
              if (heap.size > topK) {
                heap.poll()
              }
            }
            query += 1
          }
        }
      }
      heaps.indices.iterator.flatMap { query =>
        heaps(query).iterator().asScala.map(candidate => (query, candidate))
      }
    }

    val merged = scored
      .aggregateByKey(List.empty[(Double, String, Long)])(
        (best, candidate) => boundedScored(candidate :: best, topK),
        (left, right) => boundedScored(left ++ right, topK))
      .collectAsMap()

    block.indices.map { query =>
      merged
        .getOrElse(query, List.empty[(Double, String, Long)])
        .sorted(ScoredOrdering)
        .take(topK)
        .map { case (distance, dataFile, position) => (dataFile, position, distance) }
    }
  }

  /** The best {@code limit} of the candidates, in the order the answer uses. */
  private def boundedScored(
      candidates: List[(Double, String, Long)],
      limit: Int): List[(Double, String, Long)] = {
    candidates.sorted(ScoredOrdering).take(limit)
  }

  /** Reads the live rows of the given positions, keyed by data file and then by position. */
  private def rowsOfSelected(
      spark: SparkSession,
      context: VectorSearchContext,
      positions: Seq[(String, Long)]): Map[(String, Long), Row] = {
    if (positions.isEmpty) {
      return Map.empty
    }

    val inputs = context.inputs
    val byFile = mutable.LinkedHashMap.empty[String, mutable.LinkedHashSet[JLong]]
    positions.foreach { case (dataFile, position) =>
      byFile.getOrElseUpdate(dataFile, mutable.LinkedHashSet.empty).add(JLong.valueOf(position))
    }
    val rowsByFile = IndexEntries.rowsAt(
      spark,
      inputs.table,
      byFile.map { case (dataFile, byPosition) => dataFile -> byPosition.asJava }.asJava,
      columnNames(context).toList.asJava,
      inputs.readSchema,
      inputs.scanTasks)

    val rows = mutable.LinkedHashMap.empty[(String, Long), Row]
    positions.foreach { case (dataFile, position) =>
      val byPosition = rowsByFile.get(dataFile)
      val row: Row = if (byPosition == null) null else byPosition.get(JLong.valueOf(position))
      if (row != null) {
        rows.put((dataFile, position), row)
      }
    }

    rows.toMap
  }

  /** The JSON of the given rows, keyed by where the row is. */
  private def jsonOfRows(
      spark: SparkSession,
      context: VectorSearchContext,
      rows: Seq[((String, Long), Row)]): Map[(String, Long), String] = {
    if (rows.isEmpty) {
      return Map.empty
    }

    // the rows travel as one value of the read schema, so their field order and count have to be
    // that schema's; a subset or a widened projection here would serialise the wrong columns
    val rowSchema = rows.head._2.schema
    require(
      rowSchema.fieldNames.sameElements(context.inputs.readSchema.fieldNames),
      s"Cannot serialise rows of ${rowSchema.simpleString} as ${context.inputs.readSchema.simpleString}")
    val frame =
      spark.createDataFrame(rows.map(entry => entry._2).asJava, context.inputs.readSchema)
    // the JSON is paired back by position, so the count has to match the rows it was asked for;
    // a mismatch would rename every row after it without any other symptom
    val json = frame.toJSON.collectAsList()
    require(
      json.size == rows.size,
      s"Expected one JSON per row of ${rows.size}, but serialised ${json.size}")
    rows.map(entry => entry._1).zip(json.asScala).toMap
  }

  /** The schema a request answers with, which is what its output attributes declare. */
  private def structOf(
      output: Seq[org.apache.spark.sql.catalyst.expressions.Attribute]): StructType = {
    new StructType(
      output
        .map(attribute => StructField(attribute.name, attribute.dataType, attribute.nullable))
        .toArray)
  }

  private def columnNames(context: VectorSearchContext): Seq[String] = {
    context.inputs.table.schema().columns().asScala.map(column => column.name()).toSeq
  }

  /** The distance of one row's vector from the query, which is what the rewrite sorts by. */
  private def distanceOf(
      metric: VectorIndexDefinition.Metric,
      row: Row,
      vectorIndex: Int,
      query: Array[Float]): Double = {
    if (vectorIndex < 0 || row.isNullAt(vectorIndex)) {
      return Double.PositiveInfinity
    }

    VectorDistance.distance(metric, vectorOf(row, vectorIndex), query)
  }

  /** The vector of one row, as the floats the distance function takes. */
  private def vectorOf(row: Row, vectorIndex: Int): Array[Float] = {
    val values = row.getList[AnyRef](vectorIndex)
    val vector = new Array[Float](values.size())
    var i = 0
    while (i < vector.length) {
      vector(i) = values.get(i).asInstanceOf[java.lang.Number].floatValue()
      i += 1
    }

    vector
  }

  /**
   * The JSON of each hit's live row, keyed by data file and then by position.
   *
   * <p>The JSON is serialised where the row is read: a lookup resolves a whole record per hit, and
   * collecting the records only to parallelise them again to serialise them would move them across
   * the driver barrier twice.
   */
  private def resolveRows(
      spark: SparkSession,
      context: VectorSearchContext,
      hits: Seq[IndexSearchResult]): java.util.Map[String, java.util.Map[JLong, String]] = {
    val inputs = context.inputs
    if (hits.isEmpty) {
      return java.util.Collections.emptyMap()
    }

    val names = inputs.table.schema().columns().asScala.map(_.name()).toSeq
    val positions = hits
      .groupBy(hit => hit.dataFile())
      .map { case (dataFile, fileHits) =>
        dataFile -> fileHits.map(hit => JLong.valueOf(hit.rowPosition())).toSet.asJava
      }
      .asJava

    IndexEntries.rowsAtJson(
      spark,
      inputs.table,
      positions,
      names.toList.asJava,
      inputs.readSchema,
      inputs.scanTasks)
  }
}

/**
 * The request a table function returns: the names, the vectors and the contract form, and nothing
 * that has been bound or read yet.
 *
 * <p>It is a serializable description rather than a frozen plan, because a physical node travels to
 * the driver that runs it and a snapshot, a manifest list or a scan task is not what should travel
 * with it. Binding still happens once, at the start of execution, and fixes both snapshots before
 * anything is read.
 */
private[spark] case class VectorSearchHitsRequest(
    tableName: String,
    column: String,
    indexArgument: String,
    queries: Seq[Array[Float]],
    topK: Int,
    batch: Boolean,
    nprobeArgument: String,
    overSampleArgument: String,
    filterModeArgument: String)(
    override val output: Seq[org.apache.spark.sql.catalyst.expressions.Attribute])
    extends LeafNode {

  override protected def otherCopyArgs: Seq[AnyRef] = output :: Nil

  /**
   * The hits this request can answer with, which is all it ever returns.
   *
   * <p>A leaf of a plan has to estimate itself: an operator that sizes its input — a join does, a
   * filter or a projection does not — asks for one while planning, and the default estimate for a
   * leaf is not implemented. The contract bounds the answer tightly: one hit per query for each of
   * the {@code topK} results asked for, and no residual scan to add uncovered rows, unlike the
   * rewritten source.
   */
  override def computeStats: Statistics = {
    val perQuery = math.max(0, topK).toLong
    val rows = if (batch) perQuery * queries.size else perQuery
    val bytesPerRow = output.foldLeft(VectorSearchBridge.BytesPerRowFloor) { (bytes, attribute) =>
      bytes + attribute.dataType.defaultSize
    }
    Statistics(sizeInBytes = bytesPerRow * rows, rowCount = Some(rows))
  }

  def queryVectors: JList[Array[Float]] = {
    val vectors = new java.util.ArrayList[Array[Float]](queries.size)
    queries.foreach(query => vectors.add(query.toArray))
    vectors
  }

  override def simpleString(maxFields: Int): String =
    s"VectorSearchHits[contract=INDEX_HITS (table functions answer for the whole table), " +
      s"batch=$batch, table=$tableName, column=$column, k=$topK, queries=${queries.size}, " +
      s"driverMaterialization=true]"
}

/** A planned index-hit search whose request has already been bound to its snapshots. */
private[spark] case class VectorSearchHitsPlan(context: VectorSearchContext, batch: Boolean)
    extends LeafNode {

  override val output: Seq[org.apache.spark.sql.catalyst.expressions.Attribute] =
    context.request.output

  override def simpleString(maxFields: Int): String =
    s"VectorSearchHits[contract=${context.request.input.contract.name}, " +
      s"batch=$batch, k=${context.request.input.topK}, " +
      s"index=${context.inputs.indexMetadata.indexUuid()}, " +
      s"indexSnapshot=${context.inputs.indexSnapshot.snapshotId}, " +
      s"tableSnapshot=${context.inputs.tableSnapshot.snapshotId()}, " +
      s"coveredFiles=${context.inputs.dataFiles.size - context.inputs.uncoveredDataFiles.size}, " +
      s"uncoveredFiles=${context.inputs.uncoveredDataFiles.size}, driverMaterialization=true]"
}
