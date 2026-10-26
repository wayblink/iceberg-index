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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.LeafExecNode

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
    case source: VectorSearchSource => VectorSearchSourceExec(spark, source) :: Nil
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

private[spark] case class VectorSearchSourceExec(spark: SparkSession, source: VectorSearchSource)
    extends LeafExecNode {

  override def output: Seq[Attribute] = source.output

  override protected def doExecute(): RDD[InternalRow] = VectorSearchExec.rows(spark, source)

  override def outputPartitioning: Partitioning = UnknownPartitioning(0)

  override def outputOrdering: Seq[org.apache.spark.sql.catalyst.expressions.SortOrder] = Nil

  override def simpleString(maxFields: Int): String =
    s"VectorSearchSourceExec${source.simpleString(maxFields)}"
}
