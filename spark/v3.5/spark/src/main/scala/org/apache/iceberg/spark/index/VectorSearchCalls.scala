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

import java.util.{List => JList}

import scala.collection.JavaConverters._

import org.apache.iceberg.Table
import org.apache.iceberg.index.spark.VectorIndexes
import org.apache.iceberg.spark.Spark3Util
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * The Java-facing entry point for the index-hit search, so {@code CALL vector_search} runs the same
 * planning and parameter decisions as the vector table functions and the ordinary SQL rewrite.
 *
 * <p>Planning and execution stay separate calls: a procedure plans and then searches, while a table
 * function returns the request it was given and never searches while its plan is built.
 */
private[spark] object VectorSearchCalls {

  /** Plans a request, failing with the caller-visible error for a missing or ambiguous index. */
  def plan(
      spark: SparkSession,
      table: Table,
      column: String,
      indexArgument: String,
      queries: JList[Array[Float]],
      k: Int,
      batch: Boolean,
      coverage: String): VectorSearchHitsPlan = {
    VectorSearchBridge.plan(
      spark,
      table,
      column,
      indexArgument,
      queries,
      k,
      null,
      batch,
      coverage) match {
      case BoundVectorSearch(context) => VectorSearchHitsPlan(context, batch)
      case NoApplicableVectorIndex =>
        throw new IllegalArgumentException(
          s"No vector index exists for column $column of table ${table.name()}")
      case AmbiguousVectorIndex =>
        // the names are what the caller has to choose between, so they are part of the message
        val available = VectorIndexes
          .indexesFor(table, column, "vector")
          .asScala
          .map(metadata => if (metadata.name() == null) "<unnamed>" else metadata.name())
          .mkString(", ")
        throw new IllegalArgumentException(
          s"No vector index can be chosen for column $column of table ${table.name()}: " +
            s"several indexes exist ($available), pass index => '...' to choose one")
    }
  }

  /** The request a table function returns, before anything is bound or read. */
  def planOf(
      tableName: String,
      column: String,
      indexArgument: String,
      queries: JList[Array[Float]],
      k: Int,
      batch: Boolean): LogicalPlan = {
    val copied = queries.asScala.map(query => query.toVector).toSeq
    // the output attributes are created once, here, so a copy the optimizer makes keeps the same
    // expression ids and the callers of the function still refer to them
    val schema =
      if (batch) {
        org.apache.iceberg.index.spark.IndexSearch.RESULT_SCHEMA
      } else {
        org.apache.iceberg.index.spark.IndexSearch.SINGLE_RESULT_SCHEMA
      }
    val attributes = schema.fields.map { field =>
      org.apache.spark.sql.catalyst.expressions
        .AttributeReference(field.name, field.dataType, field.nullable, field.metadata)()
    }.toSeq
    VectorSearchHitsRequest(tableName, column, indexArgument, copied, k, batch)(attributes)
  }

  /** Searches a request the caller already planned. */
  def search(plan: VectorSearchHitsPlan, spark: SparkSession): JList[Row] =
    VectorSearchBridge.execute(spark, plan.context)

  /** Plans and searches a request, which is what a procedure does when it is called. */
  def search(
      spark: SparkSession,
      table: Table,
      column: String,
      indexArgument: String,
      queries: JList[Array[Float]],
      k: Int,
      batch: Boolean,
      coverage: String): JList[Row] =
    search(plan(spark, table, column, indexArgument, queries, k, batch, coverage), spark)

  /**
   * Loads the table of a request and searches it; the table function's execution path.
   *
   * <p>A table function has no coverage parameter, so it answers for the whole table. Only a caller
   * that names the mode through a procedure can ask for the index-only answer.
   */
  def searchRequested(spark: SparkSession, request: VectorSearchHitsRequest): JList[Row] = {
    val queries = new java.util.ArrayList[Array[Float]](request.queries.size)
    request.queries.foreach(query => queries.add(query.toArray))
    search(
      spark,
      Spark3Util.loadIcebergTable(spark, request.tableName),
      request.column,
      request.indexArgument,
      queries,
      request.topK,
      request.batch,
      "table")
  }
}
