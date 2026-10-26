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

import scala.collection.mutable

import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.Ascending
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.expressions.IsNotNull
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.logical.{Filter, GlobalLimit, LocalLimit, LogicalPlan, Project, Sort, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Replaces the source of {@code ORDER BY vec_distance(column, query) LIMIT k} with a vector search
 * source, so the query reads its candidates from an index instead of scanning the whole table.
 *
 * <p>The rule decides only whether a query is eligible and what its frozen inputs are. It reads
 * table and index metadata, never a data file or an index payload, and starts no job; the search
 * itself happens when the physical operator runs. Everything above the source, the projection, the
 * sort and the limit, stays exactly as it was, so attributes and the exact re-scoring are untouched.
 *
 * <p>Eligibility is a white list rather than a search for a scan anywhere in the plan: a limit over a
 * global sort of a single ascending distance, over one Iceberg scan, with only projections, aliases
 * and an optional {@code IsNotNull} guard on the vector column in between. A join, an aggregate, a
 * window, an offset or any other filter leaves the query alone, because a candidate set is not the
 * same as the input those operators expect.
 */
case class RewriteVectorSearch(spark: SparkSession) extends Rule[LogicalPlan] {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformDown {
      case limit @ GlobalLimit(rows, _) if !alreadyLowered(limit) =>
        extract(limit, rows).getOrElse(limit)
      case limit: GlobalLimit => limit
    }
  }

  /** A lowered source is not extracted again, so repeated optimization keeps the same plan. */
  private def alreadyLowered(plan: LogicalPlan): Boolean = {
    plan.exists {
      case _: VectorSearchSource => true
      case _ => false
    }
  }

  private def extract(limit: GlobalLimit, rows: Expression): Option[LogicalPlan] = {
    val sort = globalSort(limit).getOrElse(return None)
    val order = sort.order.head
    if (order.direction != Ascending || !order.child.isInstanceOf[VectorDistanceExpression]) {
      return None
    }
    val distance = order.child.asInstanceOf[VectorDistanceExpression]
    val column = distance.vector match {
      case attribute: AttributeReference => attribute.name
      case _ => return None
    }
    val k = IndexRewriteSupport.foldableInt(rows).getOrElse(return None)
    if (k <= 0) {
      return None
    }

    val (scan, guard) = eligibleChain(limit, column).getOrElse(return None)
    val sparkTable = IndexRewriteSupport.icebergTable(scan).getOrElse(return None)
    // a data file does not carry the values of its partition columns, but the ones an identity
    // partition puts in the path are recovered from it, so only the other transforms are a reason
    // to stand aside
    if (!IndexRewriteSupport.hasOnlyIdentityPartitions(sparkTable.table())) {
      return None
    }
    IndexRewriteSupport.scanColumns(scan).getOrElse(return None)

    val query = IndexRewriteSupport.queryOf(distance).getOrElse(return None)
    val snapshotId = IndexRewriteSupport
      .scanSnapshotId(sparkTable)
      .map(java.lang.Long.valueOf)
      .orNull
    val contract = VectorResultContract.TableLiveTopK.name

    VectorSearchRequest
      .forTable(
        sparkTable.table(),
        column,
        java.util.Collections.singletonList(query),
        k,
        snapshotId,
        contract,
        null,
        batch = false) match {
      case request =>
        BindVectorSearch.bind(spark, request) match {
          case BoundVectorSearch(context) =>
            val resolved = bindDecisions(context, k)
            if (resolved == null) {
              None
            } else {
              val source = VectorSearchSource(resolved, scan.output)
              // the distance in the sort is bound to the metric of the index that answers it: the
              // re-scoring has to measure what the query asked for, and the function alone only
              // knows the default metric
              val metric = resolved.inputs.definition.metric()
              val lowered = limit.transformDown {
                case relation: DataSourceV2Relation if relation eq scan => source
                case node: Sort if node.order.exists(_.child eq distance) =>
                  node.copy(order =
                    node.order.map(order => order.copy(child = distance.copy(metric = metric))))
              }
              logger.info("rewrote a vector query into {}", source.simpleString(100))
              Some(lowered)
            }
          case NoApplicableVectorIndex =>
            None
          case AmbiguousVectorIndex =>
            // answering from one of several indexes would be a guess, and a guess that returns
            // another index's rows looks exactly like an answer
            None
        }
    }
  }

  /** Applies the shared parameter decision and derives the candidate budget, or null if invalid. */
  private def bindDecisions(context: VectorSearchContext, k: Int): VectorSearchContext = {
    val parameters = VectorQueryParameters.decide(context.rawParameters)
    val budget =
      VectorCandidateBudget.derive(k, parameters.overSample, context.inputs.entryUpperBound)
    context.copy(parameters = parameters, budget = budget)
  }

  /** The scan of an eligible plan, and the optional {@code IsNotNull} guard above it. */
  private def eligibleChain(
      limit: GlobalLimit,
      column: String): Option[(DataSourceV2Relation, Option[Filter])] = {
    val allowed = mutable.ListBuffer.empty[LogicalPlan]
    var node: LogicalPlan = limit.child
    var guard: Option[Filter] = None
    var scans: Option[DataSourceV2Relation] = None
    while (scans.isEmpty) {
      node match {
        case relation: DataSourceV2Relation =>
          scans = Some(relation)
        case filter @ Filter(condition, child)
            if isVectorGuard(condition, column) && guard.isEmpty =>
          guard = Some(filter)
          allowed += filter
          node = child
        case project @ Project(_, child) =>
          allowed += project
          node = child
        case sort @ Sort(_, _, child) =>
          allowed += sort
          node = child
        case local: LocalLimit =>
          allowed += local
          node = local.child
        case alias @ SubqueryAlias(_, child) =>
          allowed += alias
          node = child
        case _ =>
          // a join, an aggregate, a window, an offset or an unexplained node
          return None
      }
    }
    val sortCount = allowed.count(_.isInstanceOf[Sort])
    if (sortCount != 1) {
      return None
    }
    scans.map(scan => (scan, guard))
  }

  /** Whether a filter is a non-null guard on the vector column, which both branches can apply. */
  private def isVectorGuard(condition: Expression, column: String): Boolean = {
    condition match {
      case IsNotNull(attribute: AttributeReference) => attribute.name == column
      case _ => false
    }
  }

  /** The global sort of a limit node, if its ordering is a single ascending vector distance. */
  private def globalSort(limit: GlobalLimit): Option[Sort] = {
    limit.child match {
      case sort: Sort if sort.order.size == 1 && sort.global => Some(sort)
      case _ =>
        // Spark puts a local limit between the global limit and the global sort
        limit.child.collectFirst { case sort: Sort if sort.global => sort }
    }
  }

  private val EmptyRow: InternalRow = new GenericInternalRow(new Array[Any](0))
}
