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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.plans.physical.UnknownPartitioning
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.metric.SQLMetrics

/**
 * Plans a lowered vector source as a physical operator.
 *
 * <p>The strategy handles only its own node; ordering, projection and the limit above it stay
 * Spark's own operators. Building the operator does not search: the search runs when the operator
 * executes, so explaining or planning a query reads no data file and starts no job.
 */
case class VectorSearchStrategy(spark: SparkSession) extends Strategy {

  // only the root a strategy is offered may be replaced: Spark treats whatever a strategy returns
  // as the whole physical plan, so matching a nested node here would answer the query with that
  // node and drop the operators above it, including the limit
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case source: VectorSearchSource => VectorSearchSourceExec(spark, source, source.output) :: Nil
    case hits: VectorSearchHitsRequest => VectorSearchHitsRequestExec(spark, hits) :: Nil
    case _ => Nil
  }
}

/** Runs an index-hit request, keeping the index's own ranked scores. */
private[spark] case class VectorSearchHitsRequestExec(
    spark: SparkSession,
    request: VectorSearchHitsRequest)
    extends LeafExecNode {

  override def output: Seq[Attribute] = request.output

  override protected def doExecute(): RDD[InternalRow] = {
    val rows = VectorSearchCalls.searchRequested(spark, request)
    val schema =
      if (request.batch) {
        org.apache.iceberg.index.spark.IndexSearch.RESULT_SCHEMA
      } else {
        org.apache.iceberg.index.spark.IndexSearch.SINGLE_RESULT_SCHEMA
      }
    spark.createDataFrame(rows, schema).queryExecution.toRdd.map(row => row.copy())
  }

  override def outputPartitioning: Partitioning = UnknownPartitioning(0)

  override def outputOrdering: Seq[org.apache.spark.sql.catalyst.expressions.SortOrder] = Nil

  override def simpleString(maxFields: Int): String = request.simpleString(maxFields)
}

private[spark] case class VectorSearchSourceExec(
    spark: SparkSession,
    // an operator above captures its child plan in a closure and that closure travels to the
    // executors, so this node is serialised even though the search runs on the driver. The context
    // holds the frozen index snapshot, its metadata and the index files it lists, none of which can
    // be sent: it is driver-only state, and the output attributes are kept beside it so that a
    // deserialised copy still answers what the plan asks of a node (its schema, its metrics).
    @transient private val source: VectorSearchSource,
    override val output: Seq[Attribute])
    extends LeafExecNode {

  /**
   * What the driver barrier moved, so a query can be explained and measured without its rows.
   *
   * <p>A hit without a live row is a candidate the index still ranks but the frozen table snapshot
   * no longer returns: the row was deleted or is otherwise not visible. Underfill here is the index
   * branch's own shortfall after its last allowed round, not the whole answer's: a residual scan can
   * still complete a partly covered query. The reason is logged, because a Spark metric carries a
   * number and not a sentence.
   */
  override lazy val metrics: Map[String, SQLMetric] =
    Map(
      "numCandidates" -> SQLMetrics.createMetric(
        spark.sparkContext,
        "initial candidates the query asked for"),
      "numCandidateRequests" -> SQLMetrics.createMetric(
        spark.sparkContext,
        "candidates asked for in the last round"),
      "numCandidateRounds" -> SQLMetrics.createMetric(
        spark.sparkContext,
        "rounds the delete-aware search took"),
      "numLiveCandidates" -> SQLMetrics.createMetric(
        spark.sparkContext,
        "candidate hits that resolved to a live row"),
      "numDeletedOrInvisibleHits" -> SQLMetrics.createMetric(
        spark.sparkContext,
        "candidate hits with no live row at the frozen snapshot"),
      "numHitsOutsideSnapshot" -> SQLMetrics.createMetric(
        spark.sparkContext,
        "candidate hits whose data file the frozen snapshot no longer has"),
      "numRowsAtFiles" -> SQLMetrics.createMetric(
        spark.sparkContext,
        "distinct data files read to resolve candidates"),
      "numResidualFiles" -> SQLMetrics.createMetric(
        spark.sparkContext,
        "uncovered files the residual branch was planned over"),
      "numIndexUnderfilled" -> SQLMetrics.createMetric(
        spark.sparkContext,
        "index branches that ended with fewer than k live candidates"),
      "numOutputRows" -> SQLMetrics.createMetric(spark.sparkContext, "rows produced"))

  override protected def doExecute(): RDD[InternalRow] = {
    val (rows, stats) = VectorSearchExec.rows(spark, source)
    val updates = Map(
      "numCandidates" -> stats.initialCandidates,
      "numCandidateRequests" -> stats.lastRequest,
      "numCandidateRounds" -> stats.rounds.toLong,
      "numLiveCandidates" -> stats.liveCandidates.toLong,
      "numDeletedOrInvisibleHits" -> stats.deletedOrInvisibleHits.toLong,
      "numHitsOutsideSnapshot" -> stats.hitsOutsideSnapshot.toLong,
      "numRowsAtFiles" -> stats.rowsAtFiles.toLong,
      "numResidualFiles" -> stats.residualFiles.toLong,
      "numIndexUnderfilled" -> (if (stats.indexUnderfilled) 1L else 0L))
    updates.foreach { case (name, value) => metrics(name).set(value) }
    val executionId = spark.sparkContext.getLocalProperty("spark.sql.execution.id")
    if (executionId != null) {
      SQLMetrics.postDriverMetricUpdates(
        spark.sparkContext,
        executionId,
        updates.keys.toSeq.map(metrics))
    }
    val outputRows = metrics("numOutputRows")
    rows.map { row =>
      outputRows.add(1L)
      row
    }
  }

  override def outputPartitioning: Partitioning = UnknownPartitioning(0)

  override def outputOrdering: Seq[org.apache.spark.sql.catalyst.expressions.SortOrder] = Nil

  override def simpleString(maxFields: Int): String =
    if (source == null) {
      // a node that travelled to an executor carries no context: saying so is better than a null
      "VectorSearchSourceExec[context available on the driver only]"
    } else {
      s"VectorSearchSourceExec${source.simpleString(maxFields)}"
    }
}
