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

import scala.collection.mutable

import org.apache.iceberg.index.vector.VectorIndexDefinition.Metric
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.Ascending
import org.apache.spark.sql.catalyst.expressions.ExprId
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.expressions.IsNotNull
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
 *
 * <p>The distance the sort orders by is either computed in the sort or named by a projection below
 * it, so {@code ORDER BY vec_distance(...)} and {@code ORDER BY d} over {@code ... AS d} are the same
 * query. Where it is computed is also where its metric is bound, because the vector column is not
 * an output of the projection that hides it.
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
    if (order.direction != Ascending) {
      return None
    }

    val chain = eligibleChain(limit).getOrElse(return None)
    // the distance the sort orders by, whether the sort computes it or names a projection that does
    val distance = chain.distanceOf(order.child).getOrElse(return None)
    val column = distance.vector match {
      case attribute: AttributeReference => attribute.name
      case _ => return None
    }
    // the filters are judged once the column is known: a guard is one on the vector column, and a
    // filter on anything else is a reason to leave the query to Spark
    if (!chain.guards(column)) {
      return None
    }
    val k = IndexRewriteSupport.foldableInt(rows).getOrElse(return None)
    if (k <= 0) {
      return None
    }

    val scan = chain.scan
    val sparkTable = IndexRewriteSupport.icebergTable(scan).getOrElse(return None)
    if (sparkTable.table().schema().findField(column) == null) {
      return None
    }
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

    val bindStart = System.nanoTime()
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
            val bindMillis = (System.nanoTime() - bindStart) / 1000000L
            val resolved = bindDecisions(context, k)
            if (resolved == null) {
              None
            } else {
              val source = VectorSearchSource(resolved)(scan.output)
              // the distance in the sort is bound to the metric of the index that answers it: the
              // re-scoring has to measure what the query asked for, and the function alone only
              // knows the default metric
              val metric = resolved.inputs.definition.metric()
              val lowered = bindMetric(limit, distance, metric).transformDown {
                case relation: DataSourceV2Relation if relation eq scan => source
                case node: Sort if node.order.exists(_.child eq distance) =>
                  node.copy(order =
                    node.order.map(order => order.copy(child = distance.copy(metric = metric))))
              }
              logger.info(
                "vector search plan: bind={}ms overSample={}[{}] uncoveredFiles={}",
                Array[Object](
                  JLong.valueOf(bindMillis),
                  JLong.valueOf(resolved.parameters.overSample),
                  resolved.parameters.overSampleOrigin,
                  Integer.valueOf(resolved.inputs.uncoveredDataFiles.size)): _*)
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

  /**
   * Binds the metric where the distance is computed, instead of moving it into the sort.
   *
   * <p>A distance the sort computes can carry the metric itself. One the sort only names cannot: the
   * projection below outputs the name and not the vector column, so a sort above it would refer to a
   * column nothing above it has. Binding every projection that computes this distance keeps the
   * attribute the sort orders by pointing at the metric the index answers with. It goes through the
   * distance rather than the ordering key on purpose: the distance it binds is the very expression
   * the resolution returned, so a query can never be rewritten with a distance whose metric is not
   * then bound.
   */
  private def bindMetric(
      plan: LogicalPlan,
      distance: VectorDistanceExpression,
      metric: Metric): LogicalPlan = {
    plan.transformDown {
      case project: Project =>
        val rewritten = project.projectList.map {
          case alias: Alias if alias.child eq distance =>
            // the projection keeps its name and its expression id, so the attribute the sort orders
            // by still refers to the distance, now measured the way the index measures it
            Alias(distance.copy(metric = metric), alias.name)(
              exprId = alias.exprId,
              qualifier = alias.qualifier,
              explicitMetadata = alias.explicitMetadata,
              nonInheritableMetadataKeys = alias.nonInheritableMetadataKeys)
          case other => other
        }
        if (rewritten == project.projectList) {
          project
        } else {
          project.copy(projectList = rewritten)
        }
      case other => other
    }
  }

  /**
   * The scan of an eligible plan, with the distances its projections named and the filters it kept.
   *
   * <p>The distances are collected while the chain is walked because the column the query is about
   * is only known once one of them is found: a filter is a guard on the vector column, and the
   * vector column is what the distance names.
   */
  private case class EligibleChain(
      scan: DataSourceV2Relation,
      computed: Map[ExprId, VectorDistanceExpression],
      filters: Seq[Filter]) {

    /**
     * The distance an ordering key stands for, when the key names a projection that computes one.
     *
     * <p>The key is matched by expression id and not by name. A name is not what an attribute refers
     * to: {@code ORDER BY t.rank} names a column of the table, and a projection above it may alias a
     * distance to the same name, so resolving on the name would answer a query for that column with
     * a distance — a different query, and one whose metric is then not the one the index answers with.
     * An attribute can only be the distance when it *is* that projection's output.
     */
    def distanceOf(key: Expression): Option[VectorDistanceExpression] = {
      key match {
        case expression: VectorDistanceExpression => Some(expression)
        case attribute: AttributeReference => computed.get(attribute.exprId)
        case _ => None
      }
    }

    /** Whether every filter between the limit and the scan is a non-null guard on the column. */
    def guards(column: String): Boolean = {
      filters.forall(filter => isVectorGuard(filter.condition, column))
    }
  }

  /**
   * The scan of an eligible plan, if everything between the limit and it keeps rows as they are.
   *
   * <p>A {@code IsNotNull} guard on the vector column is one of them: it stays above the source and
   * filters both branches, so it needs no special handling.
   *
   * <p>A row whose vector is null is not part of the answer, and that holds whether or not a guard
   * is present: the build skips such a row, so the index carries no entry for it, and the scan
   * branch filters it out as well so both branches answer the same question. A distance needs a
   * vector, so a row without one has no place in a distance ordering. Note that unmodified Spark
   * would sort that row first under {@code ASC} (nulls sort first by default), so a rewritten
   * search returns exactly the rows whose vector exists.
   */
  private def eligibleChain(limit: GlobalLimit): Option[EligibleChain] = {
    val allowed = mutable.ListBuffer.empty[LogicalPlan]
    val computed = mutable.Map.empty[ExprId, VectorDistanceExpression]
    val filters = mutable.ListBuffer.empty[Filter]
    var node: LogicalPlan = limit.child
    var scans: Option[DataSourceV2Relation] = None
    while (scans.isEmpty) {
      node match {
        case relation: DataSourceV2Relation =>
          scans = Some(relation)
        case filter @ Filter(condition, child) =>
          allowed += filter
          filters += filter
          node = child
        case project @ Project(projectList, child) =>
          allowed += project
          projectList.foreach {
            case alias: Alias =>
              alias.child match {
                case expression: VectorDistanceExpression =>
                  computed.put(alias.exprId, expression)
                case _ =>
              }
            case _ =>
          }
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
    Some(EligibleChain(scans.get, computed.toMap, filters.toList))
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
    // a sort over more than the distance is not a nearest-neighbour query: the second key decides
    // ties, and a candidate set ordered by the index cannot reproduce it
    def single(sort: Sort): Option[Sort] = if (sort.order.size == 1) Some(sort) else None
    limit.child match {
      case sort: Sort if sort.global => single(sort)
      case _ =>
        // Spark puts a local limit between the global limit and the global sort
        limit.child.collectFirst { case sort: Sort if sort.global => sort }.flatMap(single)
    }
  }

  private val EmptyRow: InternalRow = new GenericInternalRow(new Array[Any](0))
}
