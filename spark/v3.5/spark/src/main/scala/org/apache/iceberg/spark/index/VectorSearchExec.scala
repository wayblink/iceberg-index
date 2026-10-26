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

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.iceberg.index.{IndexQuery, IndexSearchResult}
import org.apache.iceberg.index.IndexQuery.Consistency
import org.apache.iceberg.index.spark.{DistributedIndexSearch, IndexEntries}
import org.apache.iceberg.index.vector.VectorIndexAdapter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.types.{StructField, StructType}
import org.slf4j.Logger
import org.slf4j.LoggerFactory

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

  def rows(spark: SparkSession, source: VectorSearchSource): RDD[InternalRow] = {
    val context = source.context
    val names = source.output.map(_.name)
    val readSchema = schemaOf(source.output)
    val found = candidateRows(spark, context, names, readSchema)
    val indexBranch = rowsToRdd(spark, found.rows, readSchema)
    val residual = residualRows(spark, context, names, readSchema)
    logger.info(
      "vector search contract={} k={} overSample={}[{}] candidates={} live={} rounds={} "
        + "reason={} residualFiles={} driverMaterialization=true",
      Array[Object](
        context.request.input.contract.name,
        Integer.valueOf(context.request.input.topK),
        JLong.valueOf(context.parameters.overSample),
        context.parameters.overSampleOrigin,
        Integer.valueOf(found.requested),
        Integer.valueOf(found.rows.size),
        Integer.valueOf(found.rounds),
        found.reason,
        Integer.valueOf(context.inputs.uncoveredDataFiles.size)): _*)
    if (residual == null) {
      indexBranch
    } else {
      spark.sparkContext.union(Seq(indexBranch, residual))
    }
  }

  private case class Candidates(rows: Seq[Row], requested: Int, rounds: Int, reason: String)

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
      return Candidates(Nil, 0, 0, "empty_request")
    }

    val adapter = new VectorIndexAdapter(inputs.definition)
    val found = mutable.LinkedHashMap.empty[(String, Long), Row]
    var budget =
      VectorCandidateBudget.derive(k, context.parameters.overSample, inputs.entryUpperBound)
    val initial = budget.initial
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
        // hits of data files the frozen snapshot no longer has cannot be resolved, so they are
        // dropped here instead of failing the query on a row that is not part of the answer
        val hits = outcome.hits().asScala.filter(hit => inputs.dataFiles.contains(hit.dataFile()))
        val positions = positionsOf(hits)
        if (!positions.isEmpty) {
          val rowsByFile = IndexEntries.rowsAt(
            spark,
            inputs.table,
            positions,
            names.toList.asJava,
            readSchema,
            inputs.scanTasks)
          collectLive(found, hits, rowsByFile)
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
    Candidates(found.values.toSeq, initial, round, reason)
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

  private def collectLive(
      found: mutable.LinkedHashMap[(String, Long), Row],
      hits: Seq[IndexSearchResult],
      rowsByFile: java.util.Map[String, java.util.Map[JLong, Row]]): Unit = {
    hits.foreach { hit =>
      val key = (hit.dataFile(), hit.rowPosition())
      if (!found.contains(key)) {
        Option(rowsByFile.get(hit.dataFile())).foreach { byPosition =>
          Option(byPosition.get(JLong.valueOf(hit.rowPosition())))
            .foreach(row => found.put(key, row))
        }
      }
    }
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
