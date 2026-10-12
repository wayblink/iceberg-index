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

import scala.collection.JavaConverters._

import org.apache.iceberg.index.IndexQuery
import org.apache.iceberg.index.IndexQuery.Consistency
import org.apache.iceberg.index.IndexReader
import org.apache.iceberg.index.IndexSearchResult
import org.apache.iceberg.index.spark.{IndexEntries, VectorIndexes}
import org.apache.iceberg.index.text.TextIndexAdapter
import org.apache.iceberg.index.text.TextIndexAdapter.CorpusStatistics
import org.apache.iceberg.index.text.TextIndexDefinition
import org.apache.spark.sql.catalyst.expressions.Descending
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.plans.logical.GlobalLimit
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Sort
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Rewrites a text search written the ordinary way into a text index lookup, in two forms:
 *
 * <p>{@code ORDER BY text_score(column, query) DESC LIMIT k}, the ranking form, and {@code WHERE
 * match(column, query)}, the filter form. Both are answered from the index and the matched rows are
 * read back, so the plan above them keeps working on the same attributes and re-computes the score.
 *
 * <p>Unlike a vector search, a text search is only answered from an index snapshot bound to exactly
 * the snapshot being read. A BM25 score depends on corpus statistics, the document frequency of the
 * query terms and the average document length, so an older snapshot would give scores a full scan
 * cannot reproduce, and the data files it does not cover would not be scored at all. An index that
 * is behind therefore means scanning, which is what this rule falls back to.
 */
case class RewriteTextSearch(spark: SparkSession) extends Rule[LogicalPlan] {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  /** Above this many matching rows the filter form is left alone: materialising them would cost. */
  private val MaxMatchRows = 100000

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformDown {
      // the ranking form: a limit over a global sort, with the projection in between
      case node @ GlobalLimit(k, _) =>
        globalSort(node) match {
          case Some(sort) =>
            rewrite(node, k, sort.order.head.child.asInstanceOf[TextScoreExpression])
              .getOrElse(node)
          case None => node
        }

      // the filter form: a predicate over the scan
      case filter @ Filter(matchExpression: TextMatchExpression, _) =>
        rewriteFilter(filter, matchExpression).getOrElse(filter)
    }
  }

  /** The global sort of a limit node, if its ordering is a descending text score. */
  private def globalSort(limit: LogicalPlan): Option[Sort] = {
    limit.collectFirst {
      case sort @ Sort(Seq(order), true, _) if isDescendingScore(order) => sort
    }
  }

  private def isDescendingScore(order: SortOrder): Boolean = {
    order.direction == Descending && order.child.isInstanceOf[TextScoreExpression]
  }

  /** Rewrites a limit-over-sort node, or says it cannot. */
  private def rewrite(
      node: LogicalPlan,
      k: Expression,
      score: TextScoreExpression): Option[LogicalPlan] = {
    val limit = IndexRewriteSupport.foldableInt(k).getOrElse(return None)
    if (limit <= 0) {
      return None
    }

    val scan = IndexRewriteSupport.findIcebergScan(node).getOrElse(return None)
    val sparkTable = IndexRewriteSupport.icebergTable(scan).getOrElse(return None)
    val table = sparkTable.table()

    // data files never carry the partition columns, so a partitioned table cannot be read this way
    if (!table.spec().isUnpartitioned()) {
      return None
    }

    val names = IndexRewriteSupport.scanColumns(scan).getOrElse(return None)
    val column = textColumn(score).getOrElse(return None)
    if (!names.contains(column)) {
      return None
    }

    val bound = bind(table, sparkTable, scan, score, column).getOrElse(return None)
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

    val query = queryText(score).getOrElse(return None)
    val hits = search(bound, snapshotId, query, limit)
    if (hits.isEmpty) {
      return None
    }

    val tableSchema = IndexRewriteSupport.tableSchema(sparkTable)
    val rows = hitsRows(sparkTable, snapshotId, hits, names, tableSchema).getOrElse(return None)
    if (rows.isEmpty) {
      return None
    }

    val specialized = score.copy(definition = bound.definition, statistics = bound.statistics)
    val newSort =
      Sort(
        Seq(SortOrder(specialized, Descending)),
        true,
        LocalRelation(scan.output, IndexRewriteSupport.internalRows(spark, rows)))
    Some(node.withNewChildren(Seq(newSort)))
  }

  /** Rewrites a filter node, or says it cannot. */
  private def rewriteFilter(
      filter: Filter,
      matchExpression: TextMatchExpression): Option[LogicalPlan] = {
    val scan = IndexRewriteSupport.findIcebergScan(filter).getOrElse(return None)
    val sparkTable = IndexRewriteSupport.icebergTable(scan).getOrElse(return None)
    val table = sparkTable.table()

    if (!table.spec().isUnpartitioned()) {
      return None
    }

    val names = IndexRewriteSupport.scanColumns(scan).getOrElse(return None)
    val column = textColumn(matchExpression.score).getOrElse(return None)
    if (!names.contains(column)) {
      return None
    }

    val bound = bind(table, sparkTable, scan, matchExpression.score, column).getOrElse(return None)
    val snapshotId = bound.snapshotId

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
    val query = queryText(matchExpression.score).getOrElse(return None)
    val hits =
      search(bound, snapshotId, query, Int.MaxValue).filter(hit => hit.score() > 0.0)
    if (hits.isEmpty) {
      return None
    }

    if (hits.size > MaxMatchRows) {
      logger.warn(
        "Text search of column {} of table {} matches {} rows, which is more than the {} a rewrite reads back, scanning",
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

    val specialized =
      matchExpression.score.copy(definition = bound.definition, statistics = bound.statistics)
    Some(
      Filter(
        matchExpression.copy(score = specialized),
        LocalRelation(scan.output, IndexRewriteSupport.internalRows(spark, rows))))
  }

  /** The index of the column a text expression reads, bound to the snapshot being read. */
  private def bind(
      table: org.apache.iceberg.Table,
      sparkTable: org.apache.iceberg.spark.source.SparkTable,
      scan: org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation,
      score: TextScoreExpression,
      column: String): Option[BoundIndex] = {
    val index = VectorIndexes.findIndexOrNull(table, column, "text")
    if (index == null) {
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
    val statistics =
      try {
        reader
          .searchStatistics(IndexQuery.of(snapshotId, java.nio.ByteBuffer.allocate(0), 0))
          .asInstanceOf[CorpusStatistics]
      } catch {
        case scala.util.control.NonFatal(e) =>
          logger.warn(
            "Cannot read the statistics of the text index of column {} because of {}",
            Array[Object](column, String.valueOf(e.getMessage)): _*)
          return None
      }

    Some(BoundIndex(snapshotId, definition, statistics, reader))
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
      case attribute: org.apache.spark.sql.catalyst.expressions.AttributeReference =>
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

  private case class BoundIndex(
      snapshotId: Long,
      definition: TextIndexDefinition,
      statistics: CorpusStatistics,
      reader: IndexReader)

  private val EmptyRow: InternalRow = new GenericInternalRow(new Array[Any](0))
}
