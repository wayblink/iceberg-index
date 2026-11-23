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

import org.apache.iceberg.index.IndexMetadata
import org.apache.iceberg.index.IndexQuery
import org.apache.iceberg.index.IndexQuery.Consistency
import org.apache.iceberg.index.IndexReader
import org.apache.iceberg.index.IndexSearchResult
import org.apache.iceberg.index.spark.IndexEntries
import org.apache.iceberg.index.spark.VectorIndexes
import org.apache.iceberg.index.text.TextIndexAdapter
import org.apache.iceberg.index.text.TextIndexAdapter.CorpusStatistics
import org.apache.iceberg.index.text.TextIndexDefinition
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.IndexErrors
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.Descending
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.plans.logical.GlobalLimit
import org.apache.spark.sql.catalyst.plans.logical.LocalLimit
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.plans.logical.Sort
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.DoubleType
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._

/**
 * Rewrites a text search written the ordinary way into a text index lookup, in two forms:
 *
 * <p>{@code ORDER BY text_score(column, query) DESC LIMIT k}, the ranking form, and {@code WHERE
 * match(column, query)}, the filter form.
 *
 * <p>The ranking form is lowered to {@link TextSearchSource}, the source the {@code search} table
 * function already runs on: the rule decides only whether the query is eligible and what it asks,
 * the search happens when the operator executes, on the executors, through the same distributed
 * search the procedure and the table function use. Reading and scoring every index file on the
 * driver while planning made the plain SQL form of a search an order of magnitude slower than the
 * CALL that asks the same index the same question.
 *
 * <p>The filter form is still answered while planning: it reads the index and materialises the
 * matched rows in place of the scan. The source answers a top-k, and a filter asks for every match,
 * so lowering it needs the source to answer "all matches" rather than the best k.
 *
 * <p>Unlike a vector search, a text search is only answered from an index snapshot bound to exactly
 * the snapshot being read. A BM25 score depends on corpus statistics, the document frequency of the
 * query terms and the average document length, so an older snapshot would give scores a full scan
 * cannot reproduce, and the data files it does not cover would not be scored at all. An index that
 * is behind therefore means leaving the plan as it is.
 */
case class RewriteTextSearch(spark: SparkSession) extends Rule[LogicalPlan] {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  /** Above this many matching rows the filter form is left alone: materialising them would cost. */
  private val MaxMatchRows = 100000

  /** The name the lowered source gives the score of a hit. */
  private val ScoreColumn = "_score"

  override def apply(plan: LogicalPlan): LogicalPlan = {
    // each search answered while planning, with the node that now stands in its place: binding its
    // score expressions needs the plan around that node, which the pass below cannot see from
    // inside
    val boundSearches =
      collection.mutable.ListBuffer.empty[(LogicalPlan, BoundIndex, CorpusStatistics)]

    val rewritten = plan.transformDown {
      // the ranking form: a limit over a global sort, with the projection in between
      case node @ GlobalLimit(k, _) if !alreadyLowered(node) =>
        globalSort(node) match {
          case Some((_, score)) => rewrite(node, k, score).getOrElse(node)
          case None => node
        }

      // the filter form: a predicate over the scan
      case filter @ Filter(matchExpression: TextMatchExpression, _) if !alreadyLowered(filter) =>
        rewriteFilter(filter, matchExpression) match {
          case Some((replacement, bound, statistics)) =>
            boundSearches.append((replacement, bound, statistics))
            replacement
          case None => filter
        }
    }

    boundSearches.foldLeft(rewritten) { case (current, (target, bound, statistics)) =>
      bindPath(current, target, bound, statistics)._1
    }
  }

  /** A lowered source is not lowered again, so repeated optimization keeps the same plan. */
  private def alreadyLowered(plan: LogicalPlan): Boolean = {
    plan.exists {
      case _: TextSearchSource => true
      case _ => false
    }
  }

  /**
   * The global sort of a limit node and the score it orders by.
   *
   * <p>A score can be ordered by where it is written or by the name it was given: `ORDER BY
   * text_score(...) DESC` and `SELECT ... text_score(...) AS _score ... ORDER BY _score DESC` are
   * the same question, and the second is the form a caller reaches for once the column has a name.
   * The name is resolved through the projection's own attribute identity, never through the text of
   * the name, so a table column that happens to be called the same does not stand in for a score.
   */
  private def globalSort(limit: LogicalPlan): Option[(Sort, TextScoreExpression)] = {
    limit.collectFirst {
      case sort @ Sort(Seq(order), true, _) if order.direction == Descending =>
        scoreOf(order.child, limit).map(score => (sort, score))
    }.flatten
  }

  /** The score an ordering names, whether it holds the expression or an alias of it. */
  private def scoreOf(child: Expression, plan: LogicalPlan): Option[TextScoreExpression] = {
    child match {
      case score: TextScoreExpression =>
        Some(score)
      case attribute: AttributeReference =>
        val aliases = plan.collect { case node =>
          node.expressions.flatMap(_.collect { case alias: Alias => alias })
        }
        aliases.flatten.collectFirst {
          case alias
              if alias.child.isInstanceOf[TextScoreExpression] &&
                alias.toAttribute.exprId == attribute.exprId =>
            alias.child.asInstanceOf[TextScoreExpression]
        }
      case _ =>
        None
    }
  }

  /**
   * Lowers a limit-over-sort node to the index source, or says it cannot.
   *
   * <p>Nothing is searched here: the rule decides what the query asks, and the answer is produced
   * when the operator runs. What is checked is everything a search cannot decide later, the snapshot
   * the index is bound to, whether it covers every data file of that snapshot, and whether the rows
   * can be read back at all.
   */
  private def rewrite(
      node: LogicalPlan,
      k: Expression,
      score: TextScoreExpression): Option[LogicalPlan] = {
    val limit = IndexRewriteSupport.foldableInt(k).getOrElse(return None)
    if (limit <= 0) {
      return None
    }

    val scan = rankedScan(node).getOrElse(return None)
    val sparkTable = IndexRewriteSupport.icebergTable(scan).getOrElse(return None)
    val table = sparkTable.table()

    // a data file does not carry the values of its partition columns, but the ones an identity
    // partition puts in the path are recovered from it, so only the other transforms are a reason
    // to stand aside
    if (!IndexRewriteSupport.hasOnlyIdentityPartitions(table)) {
      return None
    }

    val names = IndexRewriteSupport.scanColumns(scan).getOrElse(return None)
    val column = textColumn(score).getOrElse(return None)
    if (!names.contains(column)) {
      return None
    }

    val query = queryText(score).getOrElse(return None)

    val bound = bind(table, sparkTable, scan, score, column, query).getOrElse(return None)
    val snapshotId = bound.snapshotId

    // a text search is only answered from the snapshot it is bound to, so anything the index does
    // not cover has to be absent
    val dataFiles = IndexEntries.dataFiles(table, snapshotId).asScala.toSet
    val uncovered = bound.reader.uncoveredDataFiles(snapshotId, dataFiles.asJava).asScala.toSet
    if (uncovered.nonEmpty) {
      logger.warn(
        "Text index of column {} of table {} does not cover {} of {} data files of snapshot {}, scanning",
        Array(
          column,
          table.name(),
          uncovered.size: java.lang.Integer,
          dataFiles.size: java.lang.Integer,
          snapshotId: java.lang.Long): _*)
      return None
    }

    val scoreAttribute = AttributeReference(ScoreColumn, DoubleType, nullable = false)()
    val source = TextSearchSource(
      table,
      bound.metadata,
      snapshotId,
      query,
      limit,
      None,
      scan.output :+ scoreAttribute)
    val lowered = node.transformDown {
      case relation: DataSourceV2Relation if relation eq scan => source
      case other => other
    }

    // the score the plan above computes is the score the source carries, wherever that attribute is
    // visible: a node that sees the source's own output can read the number instead of computing it
    // again from statistics, which is one index read and one scoring pass this rule no longer does.
    // A score held by a node that sees a projection instead of the source is bound to the
    // statistics of the index instead, so a query is never rewritten into an unresolved attribute.
    val pointed = lowered.transformDown {
      case current
          if current.children.size == 1 &&
            current.children.head.output.exists(_.exprId == scoreAttribute.exprId) =>
        current.mapExpressions { expression =>
          expression.transformDown {
            case score: TextScoreExpression if answers(score, bound) => scoreAttribute
          }
        }
      case other => other
    }

    val ordered = orderByScoreColumn(pointed, bound, scoreAttribute)
    val remaining = scoresOf(ordered, bound)
    if (remaining.isEmpty) {
      Some(ordered)
    } else {
      statisticsOf(bound, column) match {
        case Some(statistics) =>
          logger.info(
            "Text search of column {} of table {} re-scores {} score expressions that the index"
              + " source is not visible to; binding them to the statistics of snapshot {}",
            Array(
              column,
              table.name(),
              remaining.size: java.lang.Integer,
              snapshotId: java.lang.Long): _*)
          Some(bindScores(ordered, bound, statistics))
        case None =>
          logger.warn(
            "Text search of column {} of table {} leaves scores outside the reach of the index"
              + " source and cannot bind them, scanning",
            Array[Object](column, table.name()): _*)
          None
      }
    }
  }

  /**
   * The scan of an eligible ranking query: a limit over a global sort of the score, with only
   * projections, local limits and aliases in between.
   *
   * <p>A join, an aggregate, a window, an offset or a filter decides which rows there are, and the
   * source answers with its own top-k of the whole table: lifting the scan through one of them
   * would answer a different question than the one that was asked. The vector rewrite recognises
   * its own queries by the same shape.
   */
  private def rankedScan(limit: LogicalPlan): Option[DataSourceV2Relation] = {
    @scala.annotation.tailrec
    def downThrough(node: LogicalPlan): Option[DataSourceV2Relation] = node match {
      case relation: DataSourceV2Relation => Some(relation)
      case sort: Sort => downThrough(sort.child)
      case project: Project => downThrough(project.child)
      case local: LocalLimit => downThrough(local.child)
      case alias: SubqueryAlias => downThrough(alias.child)
      case _ => None
    }

    downThrough(limit.children.head)
  }

  /**
   * Orders a sort of the score by the column that already holds it.
   *
   * <p>A query that projects the score sorts by the expression while the projection below it carries
   * the number: `Sort [text_score(body, q)]` over `Project [_score AS score, ...]`. Ordering by the
   * score of the index instead is the same order, and it is the only scoring this query does, rather
   * than one scoring of the sort and another of the projection.
   */
  private def orderByScoreColumn(
      plan: LogicalPlan,
      bound: BoundIndex,
      scoreAttribute: AttributeReference): LogicalPlan = {
    plan.transformDown {
      case sort: Sort =>
        val keys = sort.order.map { ordering =>
          ordering.child match {
            case score: TextScoreExpression if answers(score, bound) =>
              scoreColumnOf(sort.children.head, scoreAttribute) match {
                case Some(column) => ordering.copy(child = column)
                case None => ordering
              }
            case _ => ordering
          }
        }
        if (keys == sort.order) sort else sort.copy(order = keys)
      case other => other
    }
  }

  /** The column of a projection that already holds the given score, under whatever name. */
  private def scoreColumnOf(
      child: LogicalPlan,
      scoreAttribute: AttributeReference): Option[Attribute] = {
    child match {
      case project: Project =>
        project.projectList.collectFirst {
          case alias: Alias if alias.child.semanticEquals(scoreAttribute) => alias.toAttribute
        }
      case _ => None
    }
  }

  /** The scores of one index that a plan still computes, which the source's attribute cannot answer. */
  private def scoresOf(plan: LogicalPlan, bound: BoundIndex): Seq[TextScoreExpression] = {
    plan.flatMap { node =>
      node.expressions.flatMap(_.collect {
        case score: TextScoreExpression if answers(score, bound) => score
      })
    }
  }

  /**
   * Rewrites a filter node, or says it cannot, with the index and the statistics it was bound to.
   *
   * <p>This form is answered while planning, by reading the index and materialising the rows in
   * place of the scan. The index source answers a top-k and this form asks for every match, so
   * lowering it needs the source to answer an unbounded search; until it does, this is the shape the
   * rewrite keeps.
   */
  private def rewriteFilter(
      filter: Filter,
      matchExpression: TextMatchExpression): Option[(LogicalPlan, BoundIndex, CorpusStatistics)] = {
    val scan = IndexRewriteSupport.findIcebergScan(filter).getOrElse(return None)
    val sparkTable = IndexRewriteSupport.icebergTable(scan).getOrElse(return None)
    val table = sparkTable.table()

    if (!IndexRewriteSupport.hasOnlyIdentityPartitions(table)) {
      return None
    }

    val names = IndexRewriteSupport.scanColumns(scan).getOrElse(return None)
    val column = textColumn(matchExpression.score).getOrElse(return None)
    if (!names.contains(column)) {
      return None
    }

    val query = queryText(matchExpression.score).getOrElse(return None)

    val bound =
      bind(table, sparkTable, scan, matchExpression.score, column, query)
        .getOrElse(return None)
    val snapshotId = bound.snapshotId
    // a score of a row this form returns is computed here, so the statistics have to be readable
    // before anything is materialised
    val statistics = statisticsOf(bound, column).getOrElse(return None)

    val dataFiles = IndexEntries.dataFiles(table, snapshotId).asScala.toSet
    val uncovered = bound.reader.uncoveredDataFiles(snapshotId, dataFiles.asJava).asScala.toSet
    if (uncovered.nonEmpty) {
      logger.warn(
        "Text index of column {} of table {} does not cover {} of {} data files of snapshot {}, scanning",
        Array(
          column,
          table.name(),
          uncovered.size: java.lang.Integer,
          dataFiles.size: java.lang.Integer,
          snapshotId: java.lang.Long): _*)
      return None
    }

    // a filter asks for every matching row, and a score of zero is not a match
    val hits =
      search(bound, snapshotId, query, Int.MaxValue).filter(hit => hit.score() > 0.0)
    if (hits.isEmpty) {
      return None
    }

    if (hits.size > MaxMatchRows) {
      logger.warn(
        "Text search of column {} of table {} matches {} rows, which is more than the {} a rewrite"
          + " reads back, scanning",
        Array(
          column,
          table.name(),
          hits.size: java.lang.Integer,
          MaxMatchRows: java.lang.Integer): _*)
      return None
    }

    val tableSchema = IndexRewriteSupport.tableSchema(sparkTable)
    val rows = hitsRows(sparkTable, snapshotId, hits, names, tableSchema).getOrElse(return None)
    if (rows.isEmpty) {
      return None
    }

    Some((rowsInPlaceOfScan(filter, scan, rows), bound, statistics))
  }

  /**
   * Replaces the scan with the rows the index returned, keeping what the plan put above it.
   *
   * <p>The rows are turned into the internal rows a local relation holds by the same conversion
   * Spark applies to a local collection, not by reading them back through a dataset: going through
   * one cost a job to convert a handful of rows, and the rows only ever fill a relation that the
   * plan above reads. They are given the scan's own attributes, so every column the plan above
   * named still resolves, and the score it recomputes is read from the columns it asked for.
   */
  private def rowsInPlaceOfScan(
      node: LogicalPlan,
      scan: DataSourceV2Relation,
      rows: Seq[Row]): LogicalPlan = {
    val relation = LocalRelation.fromExternalRows(scan.output, rows)
    // the scan goes by identity, so the node replaced is the one this search read and no other
    node.transformDown { case current if current eq scan => relation }
  }

  /**
   * Binds the score expressions of one rewritten search to the statistics of its index.
   *
   * <p>A score is bound only when it reads the same attribute and query as the search. Attributes
   * with the same name from different table relations do not share corpus statistics.
   *
   * <p>The binding reaches the nodes on the way down to the rewritten search as well as the ones
   * inside it: a projection that asks for the score of a matched row sits above the filter that
   * names the search, and an unbound score expression refuses to score at all rather than returning
   * a wrong number.
   *
   * @return the rebuilt plan, and whether the rewritten search was in it
   */
  private def bindPath(
      plan: LogicalPlan,
      target: LogicalPlan,
      bound: BoundIndex,
      statistics: CorpusStatistics): (LogicalPlan, Boolean) = {
    if (plan eq target) {
      (bindScores(plan, bound, statistics), true)
    } else {
      var found = false
      val children = plan.children.map { child =>
        val (rewritten, inChild) = bindPath(child, target, bound, statistics)
        found = found || inChild
        rewritten
      }

      if (found) {
        (bindNode(plan.withNewChildren(children), bound, statistics), true)
      } else {
        (plan, false)
      }
    }
  }

  /** Binds the score expressions of a subtree and of every node in it. */
  private def bindScores(
      node: LogicalPlan,
      bound: BoundIndex,
      statistics: CorpusStatistics): LogicalPlan = {
    node.transformUp { case plan => bindNode(plan, bound, statistics) }
  }

  /**
   * Binds the score expressions of one node.
   *
   * <p>The plan's own expression traversal reaches the expressions a node holds, not the ones inside
   * them, and a score usually sits inside an alias of a projection. Descending through each
   * expression is what reaches it.
   */
  private def bindNode(
      node: LogicalPlan,
      bound: BoundIndex,
      statistics: CorpusStatistics): LogicalPlan = {
    node.mapExpressions { expression =>
      expression.transformDown {
        case score: TextScoreExpression if answers(score, bound) =>
          score.copy(definition = bound.definition, statistics = statistics)
      }
    }
  }

  /** Whether a score expression is the one this index answers. */
  private def answers(score: TextScoreExpression, bound: BoundIndex): Boolean = {
    score.text match {
      case attribute: AttributeReference =>
        attribute.exprId == bound.attribute.exprId && queryText(score).contains(bound.query)
      case _ => false
    }
  }

  /** The index of the column a text expression reads, bound to the snapshot being read. */
  private def bind(
      table: org.apache.iceberg.Table,
      sparkTable: org.apache.iceberg.spark.source.SparkTable,
      scan: DataSourceV2Relation,
      score: TextScoreExpression,
      column: String,
      query: String): Option[BoundIndex] = {
    val attribute = score.text match {
      case reference: AttributeReference if scan.output.exists(_.exprId == reference.exprId) =>
        reference
      case _ => return None
    }

    val candidates = VectorIndexes.indexesFor(table, column, "text")
    if (candidates.isEmpty) {
      return None
    }
    if (candidates.size() > 1) {
      // this form names no index, and a score needs the statistics of one: taking the first would
      // answer with another index's statistics, which reads as a scoring bug rather than as a
      // missing choice, so the caller is told how to choose instead
      val names = candidates.asScala.map {
        case metadata if metadata.name() != null => metadata.name()
        case metadata => metadata.indexUuid()
      }
      throw IndexErrors.analysis(
        s"Column $column of table ${table.name()} has ${candidates.size()} text indexes " +
          s"${names.mkString("[", ", ", "]")}; the text_score form cannot choose one. " +
          "Use CALL text_search(..., index => '...') or the search table function, or keep one index")
    }

    val index = candidates.get(0)
    if (!IndexRewriteSupport.isIndexTypeRegistered(index)) {
      logger.warn(
        "Index type {} of column {} of table {} is not registered in this session, scanning",
        Array[Object](index.indexType(), column, table.name()): _*)
      return None
    }

    val snapshotId = IndexRewriteSupport.scanSnapshotId(sparkTable).getOrElse(return None)
    // only an index snapshot bound to exactly this snapshot has statistics that match a full scan
    val indexSnapshot = index.snapshotForSourceTableSnapshot(snapshotId)
    if (indexSnapshot == null) {
      logger.warn(
        "Text index of column {} of table {} is not bound to snapshot {}, scanning",
        Array(column, table.name(), snapshotId: java.lang.Long): _*)
      return None
    }

    val definition = index.definition().asInstanceOf[TextIndexDefinition]
    val reader = new IndexReader(index, table)
    Some(BoundIndex(snapshotId, index, definition, reader, attribute, query))
  }

  /**
   * The corpus statistics a score of this index is computed with, or nothing when they cannot be
   * read.
   *
   * <p>Only a search this rule answers itself needs them: the index source computes its own scores
   * when it runs, and the statistics a lowered query reads are the ones the source reads.
   */
  private def statisticsOf(bound: BoundIndex, column: String): Option[CorpusStatistics] = {
    try {
      val payload = new TextIndexAdapter(bound.definition).encodeQuery(bound.query)
      Some(
        bound.reader
          .searchStatistics(IndexQuery.of(bound.snapshotId, payload, 0))
          .asInstanceOf[CorpusStatistics])
    } catch {
      case scala.util.control.NonFatal(e) =>
        logger.warn(
          "Cannot read the statistics of the text index of column {} because of {}",
          Array[Object](column, String.valueOf(e.getMessage)): _*)
        None
    }
  }

  /** Searches the index for the query of a score expression, which has to be a constant. */
  private def search(
      bound: BoundIndex,
      snapshotId: Long,
      query: String,
      limit: Int): List[IndexSearchResult] = {
    val payload = new TextIndexAdapter(bound.definition).encodeQuery(query)
    bound.reader
      .search(IndexQuery.of(snapshotId, payload, limit))
      .asScala
      .toList
  }

  /** The live rows of the given hits, or nothing when they cannot be read back. */
  private def hitsRows(
      sparkTable: org.apache.iceberg.spark.source.SparkTable,
      snapshotId: Long,
      hits: List[IndexSearchResult],
      names: Seq[String],
      tableSchema: org.apache.spark.sql.types.StructType): Option[List[Row]] = {
    val rowsByFile =
      IndexRewriteSupport.rowsAt(spark, sparkTable, snapshotId, hits, names, tableSchema)
    val rows = hits.flatMap(hit =>
      Option(rowsByFile.get(hit.dataFile())).flatMap(byPosition =>
        Option(byPosition.get(hit.rowPosition()))))
    if (rows.isEmpty) {
      None
    } else {
      Some(rows)
    }
  }

  private def textColumn(score: TextScoreExpression): Option[String] = {
    score.text match {
      case attribute: AttributeReference =>
        Some(attribute.name)
      case _ =>
        None
    }
  }

  /** The query text of a score expression, which has to be a constant for the index to see it. */
  private def queryText(score: TextScoreExpression): Option[String] = {
    if (!score.query.foldable) {
      return None
    }

    score.query.eval(EmptyRow) match {
      case value: org.apache.spark.unsafe.types.UTF8String => Some(value.toString)
      case value: String => Some(value)
      case _ => None
    }
  }

  /** The index a search is answered from, and what its score expressions are matched by. */
  private case class BoundIndex(
      snapshotId: Long,
      metadata: IndexMetadata,
      definition: TextIndexDefinition,
      reader: IndexReader,
      attribute: AttributeReference,
      query: String)

  private val EmptyRow: InternalRow = new GenericInternalRow(new Array[Any](0))
}
