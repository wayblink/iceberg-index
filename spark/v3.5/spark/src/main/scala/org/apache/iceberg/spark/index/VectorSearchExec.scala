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
import org.apache.iceberg.index.IndexQuery
import org.apache.iceberg.index.IndexQuery.Consistency
import org.apache.iceberg.index.IndexSearchResult
import org.apache.iceberg.index.spark.DistributedIndexSearch
import org.apache.iceberg.index.spark.IndexEntries
import org.apache.iceberg.index.vector.VectorIndexAdapter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * How a lowered vector source produces rows.
 *
 * <p>Two branches share the decisions the optimizer already made: an index branch returns the live
 * rows its candidates point at, and, for a complete-table query, a scan branch returns the live rows
 * of the files the index does not cover. The operator above the source re-scores and sorts exactly,
 * so this class never decides which candidate is nearest.
 *
 * <p>An approximate kernel does not promise that a wider search keeps its smaller answer as a
 * prefix, so each round keeps the live rows already found and asks for a cumulative number of
 * candidates. Asking only for the difference would let a re-ranked candidate set drop rows that were
 * already resolved.
 *
 * <p>The candidate branch runs on the driver, because the search API returns driver-side state. The
 * plan says so, and the metrics record what crossed it; it is not a promise about peak memory.
 */
private[spark] object VectorSearchExec {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  /** The rounds a delete-aware search may take before it returns what it has. */
  private val MaxRounds = 10

  def rows(
      spark: SparkSession,
      source: VectorSearchSource): (RDD[InternalRow], VectorSearchStats) = {
    val context = source.context
    val names = source.output.map(_.name)
    val readSchema = schemaOf(source.output)
    val candidateStart = System.nanoTime()
    val found = candidateRows(spark, context, names, readSchema)
    val candidateNanos = System.nanoTime() - candidateStart
    val indexBranch = rowsToRdd(spark, found.rows, readSchema)
    val residualStart = System.nanoTime()
    val residual = residualRows(spark, context, names, readSchema)
    val residualNanos = System.nanoTime() - residualStart
    val residualFiles = if (residual == null) 0 else context.inputs.uncoveredDataFiles.size
    // the parameters are decided before a source is ever built, but the candidate loop treats them
    // as possibly absent, so the log reads them the same way instead of failing where that guard
    // would have returned an empty request
    val overSample =
      if (context.parameters == null) {
        "unset[unset]"
      } else {
        s"${context.parameters.overSample}[${context.parameters.overSampleOrigin}]"
      }
    logger.info(
      "vector search contract={} k={} overSample={} candidates={} lastRequest={} live={} "
        + "unresolved={} outsideSnapshot={} rounds={} rowsAtFiles={} residualFiles={} reason={} "
        + "driverMaterialization=true candidatesMs={} residualMs={}",
      Array[Object](
        context.request.input.contract.name,
        Integer.valueOf(context.request.input.topK),
        overSample,
        JLong.valueOf(found.initial),
        JLong.valueOf(found.requested),
        Integer.valueOf(found.rows.size),
        Integer.valueOf(found.unresolved),
        Integer.valueOf(found.outsideSnapshot),
        Integer.valueOf(found.rounds),
        Integer.valueOf(found.rowsAtFiles),
        Integer.valueOf(residualFiles),
        found.reason,
        JLong.valueOf(candidateNanos / 1000000L),
        JLong.valueOf(residualNanos / 1000000L)): _*)
    val rows =
      if (residual == null) indexBranch else spark.sparkContext.union(Seq(indexBranch, residual))
    val stats = VectorSearchStats(
      found.initial,
      found.requested,
      found.rows.size,
      found.unresolved,
      found.outsideSnapshot,
      found.rounds,
      found.rowsAtFiles,
      residualFiles,
      found.rows.size < context.request.input.topK)
    (rows, stats)
  }

  private case class Candidates(
      rows: Seq[Row],
      initial: Long,
      requested: Long,
      unresolved: Int,
      outsideSnapshot: Int,
      rounds: Int,
      rowsAtFiles: Int,
      reason: String)

  /** The live rows the index candidates point at, asking for more while the top-k is short. */
  private def candidateRows(
      spark: SparkSession,
      context: VectorSearchContext,
      names: Seq[String],
      readSchema: StructType): Candidates = {
    val inputs = context.inputs
    val k = context.request.input.topK
    val queries = context.request.input.queries
    if (k <= 0 || queries.isEmpty || context.parameters == null) {
      return Candidates(Nil, 0L, 0L, 0, 0, 0, 0, "empty_request")
    }
    val opening = if (context.budget == null) {
      VectorCandidateBudget.derive(k, context.parameters.overSample, inputs.entryUpperBound)
    } else {
      context.budget
    }

    val adapter = new VectorIndexAdapter(inputs.definition)
    val found = mutable.LinkedHashMap.empty[(String, Long), Row]
    // a position is counted once, whether it resolved to a live row or not: a wider round returns
    // the earlier candidates again, and an unresolved one would otherwise be counted on every round
    val seen = mutable.Set.empty[(String, Long)]
    var budget = opening
    var unresolved = 0
    var outsideSnapshot = 0
    val readFiles = mutable.Set.empty[String]
    var round = 0
    var reason = "underfilled_retry_limit"
    var done = false
    while (!done && round < MaxRounds) {
      round += 1
      queries.foreach { query =>
        val payload = adapter.encodeQuery(query.toArray)
        val outcome = DistributedIndexSearch.search(
          spark,
          inputs.indexMetadata,
          inputs.table,
          IndexQuery
            .of(inputs.indexSnapshot.sourceTableSnapshotId(), payload, budget.initial)
            .withConsistency(Consistency.AT_OR_BEFORE))
        // hits of data files the frozen snapshot no longer has cannot be resolved: a compaction or
        // rewrite after the build leaves entries pointing at files the snapshot replaced. They are
        // dropped instead of failing the query, and counted once so the loss is visible
        outcome.hits().asScala.foreach { hit =>
          val key = (hit.dataFile(), hit.rowPosition())
          if (!inputs.dataFiles.contains(hit.dataFile()) && !seen.contains(key)) {
            seen += key
            outsideSnapshot += 1
          }
        }
        val hits = outcome.hits().asScala.filter(hit => inputs.dataFiles.contains(hit.dataFile()))
        // a position seen by an earlier round is not a new candidate, so it is neither counted nor
        // asked for again
        val fresh = hits.filterNot(hit => seen.contains((hit.dataFile(), hit.rowPosition())))
        fresh.foreach(hit => seen += (hit.dataFile() -> hit.rowPosition()))
        val positions = positionsOf(fresh)
        if (!positions.isEmpty) {
          val rowsByFile = IndexEntries.rowsAt(
            spark,
            inputs.table,
            positions,
            names.toList.asJava,
            readSchema,
            inputs.scanTasks)
          readFiles ++= rowsByFile.keySet().asScala
          fresh.foreach { hit =>
            val byPosition = rowsByFile.get(hit.dataFile())
            val row =
              if (byPosition == null) null else byPosition.get(JLong.valueOf(hit.rowPosition()))
            if (row == null) {
              // a deleted, no longer visible or otherwise unresolved hit: the index still ranks it,
              // but the frozen table snapshot has no live row at that position any more
              unresolved += 1
            } else {
              found.put((hit.dataFile(), hit.rowPosition()), row)
            }
          }
        }
        if (found.size >= k) {
          done = true
          reason = "satisfied"
        } else if (!outcome.moreHits()) {
          done = true
          reason = "kernel_exhausted"
        } else if (budget.entryUpperBound != null && budget.initial >= budget.entryUpperBound
            .longValue()) {
          done = true
          reason = "entry_bound_reached"
        }
      }
      if (!done) {
        val next =
          VectorCandidateBudget.nextOrNull(budget.initial, found.size, k, budget.entryUpperBound)
        if (next == null) {
          done = true
          reason = "budget_not_representable"
        } else {
          budget = next
        }
      }
    }
    Candidates(
      found.values.toSeq,
      opening.initial,
      budget.initial,
      unresolved,
      outsideSnapshot,
      round,
      readFiles.size,
      reason)
  }

  /** The live rows of the files the index does not cover, as a distributed branch. */
  private def residualRows(
      spark: SparkSession,
      context: VectorSearchContext,
      names: Seq[String],
      readSchema: StructType): RDD[InternalRow] = {
    val uncovered = context.inputs.uncoveredDataFiles
    if (uncovered.isEmpty || context.request.input.contract == VectorResultContract.IndexHits) {
      return null
    }

    val vectorIndex = names.indexOf(context.request.input.column)
    IndexEntries
      .liveRows(
        spark,
        context.inputs.table,
        uncovered.asJava,
        names.toList.asJava,
        readSchema,
        context.inputs.scanTasks)
      .queryExecution
      .toRdd
      // a row without a vector is not a candidate, and the index branch never returns one: the two
      // branches have to answer the same question, or the same row would appear or not depending on
      // whether its file happens to be covered
      .filter(row => vectorIndex < 0 || !row.isNullAt(vectorIndex))
      .map(row => row.copy())
  }

  private def positionsOf(
      hits: Seq[IndexSearchResult]): java.util.Map[String, java.util.Set[JLong]] = {
    hits
      .groupBy(hit => hit.dataFile())
      .map { case (dataFile, fileHits) =>
        dataFile -> fileHits.map(hit => JLong.valueOf(hit.rowPosition())).toSet.asJava
      }
      .asJava
  }

  private def rowsToRdd(
      spark: SparkSession,
      rows: Seq[Row],
      readSchema: StructType): RDD[InternalRow] = {
    if (rows.isEmpty) {
      spark.sparkContext.emptyRDD[InternalRow]
    } else {
      spark.createDataFrame(rows.asJava, readSchema).queryExecution.toRdd.map(row => row.copy())
    }
  }

  def schemaOf(output: Seq[Attribute]): StructType =
    StructType(output.map { attribute =>
      StructField(attribute.name, attribute.dataType, attribute.nullable, attribute.metadata)
    }.toArray)
}

/**
 * What one execution of a vector source crossed the driver barrier with: the candidates it asked
 * for, the live rows it resolved, the hits it could not resolve, the rounds it took and the files it
 * read on each branch.
 */
private[spark] case class VectorSearchStats(
    initialCandidates: Long,
    lastRequest: Long,
    liveCandidates: Int,
    deletedOrInvisibleHits: Int,
    hitsOutsideSnapshot: Int,
    rounds: Int,
    rowsAtFiles: Int,
    residualFiles: Int,
    indexUnderfilled: Boolean)
