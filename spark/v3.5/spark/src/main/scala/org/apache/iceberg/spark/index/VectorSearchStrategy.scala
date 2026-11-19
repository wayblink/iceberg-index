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

import org.apache.iceberg.index.spark.DistributedIndexSearch
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
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Plans index searches as physical operators.
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
    case text: TextSearchSource => TextSearchExec(spark, text, text.output) :: Nil
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
    val plan = VectorSearchCalls.planRequested(spark, request)
    val schema =
      if (request.batch) {
        org.apache.iceberg.index.spark.IndexSearch.RESULT_SCHEMA
      } else {
        org.apache.iceberg.index.spark.IndexSearch.SINGLE_RESULT_SCHEMA
      }
    // the search answers a block at a time, and each block is converted as it arrives so the block's
    // rows and their JSON are released once they are UnsafeRows. The driver does not stream the
    // batch: the union below holds every block's UnsafeRows until it executes, so the peak is the
    // batch and not one block. What the block-at-a-time conversion saves is each block's row
    // wrappers and JSON strings, which are dropped as soon as the block has been converted.
    val sc = spark.sparkContext
    val partitions = mutable.ArrayBuffer.empty[RDD[InternalRow]]
    VectorSearchBridge.hitBlocks(spark, plan.context).foreach { rows =>
      if (rows.nonEmpty) {
        // a partition has to be made of the rows Spark can serialise, which the local frame gives
        // UnsafeRow copies for; coalescing keeps one block one partition, which the local scan would
        // otherwise slice into one per default parallelism
        partitions += spark
          .createDataFrame(rows.asJava, schema)
          .queryExecution
          .toRdd
          .map(row => row.copy())
          .coalesce(1)
      }
    }

    // one union over the blocks keeps the tree flat however many blocks a batch took
    if (partitions.isEmpty) sc.emptyRDD[InternalRow] else sc.union(partitions)
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
   * What the search crossed, so a query can be explained and measured without its rows.
   *
   * <p>A hit without a live row is a candidate the index still ranks but the frozen table snapshot
   * no longer returns: the row was deleted or is otherwise not visible. Underfill here is an index
   * file's own shortfall after its last allowed round, not the whole answer's: a residual scan can
   * still complete a partly covered query.
   *
   * <p>Everything but the files the scan branch was planned over is counted by the tasks that read
   * the rows, because the rows never reach the driver for it to count them.
   */
  override lazy val metrics: Map[String, SQLMetric] = {
    val counters =
      Map(
        "numCandidates" -> SQLMetrics.createMetric(
          spark.sparkContext,
          "candidates asked for, summed over the index files"),
        "numCandidateRequests" -> SQLMetrics.createMetric(
          spark.sparkContext,
          "candidates asked for in the last round, summed over the index files"),
        "numCandidateRounds" -> SQLMetrics.createMetric(
          spark.sparkContext,
          "rounds the delete-aware search took, summed over the index files"),
        "numLiveCandidates" -> SQLMetrics.createMetric(
          spark.sparkContext,
          "candidate hits that resolved to a live row"),
        "numFetchedRows" -> SQLMetrics.createMetric(
          spark.sparkContext,
          "rows read at their data files to resolve candidates, summed over the rounds"),
        "numDeletedOrInvisibleHits" -> SQLMetrics.createMetric(
          spark.sparkContext,
          "candidate hits with no live row at the frozen snapshot"),
        "numHitsOutsideSnapshot" -> SQLMetrics.createMetric(
          spark.sparkContext,
          "candidate hits whose data file the frozen snapshot no longer has"),
        "numRowsAtFiles" -> SQLMetrics.createMetric(
          spark.sparkContext,
          "data files read to resolve candidates, summed over the index files"),
        "numResidualFiles" -> SQLMetrics.createMetric(
          spark.sparkContext,
          "uncovered files the residual branch was planned over"),
        "numIndexUnderfilled" -> SQLMetrics.createMetric(
          spark.sparkContext,
          "index files that ended with fewer than k live rows"),
        "numScanTasksShipped" -> SQLMetrics.createMetric(
          spark.sparkContext,
          "scan tasks handed to the index tasks, which are the files each covers"),
        "numOutputRows" -> SQLMetrics.createMetric(spark.sparkContext, "rows produced"))
    // the stage metrics name themselves in one place, so a log that reads them and the search that
    // reports them cannot drift apart
    val stages =
      DistributedIndexSearch.SearchStages.descriptions().asScala.map { case (name, description) =>
        name -> SQLMetrics.createMetric(spark.sparkContext, description)
      }
    counters ++ stages
  }

  override protected def doExecute(): RDD[InternalRow] = {
    val (rows, stats) = VectorSearchExec.rows(spark, source, metrics)
    // what the driver knows before the tasks run is set here; everything else they add themselves
    val updates = Map("numResidualFiles" -> stats.residualFiles.toLong)
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
