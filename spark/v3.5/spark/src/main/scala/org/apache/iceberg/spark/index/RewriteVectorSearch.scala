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

import org.apache.iceberg.util.SnapshotUtil
import org.apache.iceberg.index.IndexQuery
import org.apache.iceberg.index.IndexReader
import org.apache.iceberg.index.IndexSearchResult
import org.apache.iceberg.index.vector.VectorIndexAdapter
import org.apache.iceberg.index.vector.VectorIndexDefinition
import org.apache.iceberg.index.vector.VectorIndexDefinition.IndexMode
import org.apache.iceberg.index.spark.{IndexEntries, VectorIndexes}
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.Ascending
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.logical.GlobalLimit
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Sort
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.SparkSession
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Rewrites {@code ORDER BY vec_distance(column, query) LIMIT k} into a vector index lookup.
 *
 * <p>The rule only fires when the index can be trusted to answer the query on its own, which means
 * all of the following hold: the column has a vector index, the index snapshot is bound to exactly
 * the table snapshot the scan reads, the index is in exact mode, and the index covers every data
 * file of that snapshot. Otherwise the plan is left exactly as it was, which is asserted in the
 * tests.
 *
 * <p>The rewrite replaces the scan with a local relation holding the rows the index points at, so
 * everything above it, the projection, the sort and the limit, keeps working on the same attributes
 * and re-computes the distance. That is what keeps the output schema and the row order of the
 * original query intact.
 *
 * <p>What this deliberately does not do: an approximate (HNSW) index is left alone because its
 * candidates have to be re-scored first, and a stale index is left alone because the data files it
 * does not cover would have to be scanned as well. Both are later stages.
 */
case class RewriteVectorSearch(spark: SparkSession) extends Rule[LogicalPlan] {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformDown {
      // a plain ORDER BY is not a top-k query: only a limit over a global sort is, and Spark puts
      // a local limit and the projection between the two
      case node @ GlobalLimit(k, _) =>
        globalSort(node) match {
          case Some(sort) =>
            rewrite(node, k, sort.order.head.child.asInstanceOf[VectorDistanceExpression])
              .getOrElse(node)
          case None => node
        }
    }
  }

  /** The global sort of a limit node, if its ordering is an ascending vector distance. */
  private def globalSort(limit: LogicalPlan): Option[Sort] = {
    limit.collectFirst {
      case sort @ Sort(Seq(order), true, _) if isAscendingDistance(order) => sort
    }
  }

  /** Rewrites a matching limit-over-sort node, or says it cannot. */
  private def rewrite(
      node: LogicalPlan,
      k: Expression,
      distance: VectorDistanceExpression): Option[LogicalPlan] = {
    val limit = foldableInt(k).getOrElse(return None)
    if (limit <= 0) {
      return None
    }

    val scan = findIcebergScan(node).getOrElse(return None)
    val sparkTable = icebergTable(scan).getOrElse(return None)
    val table = sparkTable.table()

    val column = vectorColumn(distance).getOrElse(return None)
    val index = VectorIndexes.findIndexOrNull(table, column, "vector")
    if (index == null) {
      return None
    }
    val definition = index.definition().asInstanceOf[VectorIndexDefinition]
    if (definition.mode() != IndexMode.EXACT) {
      return None
    }

    val snapshotId = scanSnapshotId(sparkTable).getOrElse {
      return None
    }
    if (index.snapshotForSourceTableSnapshot(snapshotId) == null) {
      // the index is not bound to the snapshot this scan reads
      return None
    }

    val dataFiles = IndexEntries.dataFiles(table, snapshotId).asScala.toSet
    val uncovered =
      new IndexReader(index, table.io()).uncoveredDataFiles(snapshotId, dataFiles.asJava)
    if (!uncovered.isEmpty) {
      // some data files are not in the index yet, so the answer would be incomplete
      return None
    }

    val payload = new VectorIndexAdapter(definition).encodeQuery(queryFloats(distance))
    val hits =
      new IndexReader(index, table.io())
        .search(IndexQuery.of(snapshotId, payload, limit))
    if (hits.isEmpty) {
      return None
    }

    val rows = readRows(sparkTable, scan.output, hits.asScala.toList).getOrElse(return None)

    val specialized = distance.copy(metric = definition.metric())
    val newSort =
      Sort(Seq(SortOrder(specialized, Ascending)), true, LocalRelation(scan.output, rows))
    Some(node.withNewChildren(Seq(newSort)))
  }

  /** Returns the first Iceberg scan of the plan, which is the one the distance column belongs to. */
  private def findIcebergScan(plan: LogicalPlan): Option[DataSourceV2Relation] = {
    plan.collectFirst {
      case relation: DataSourceV2Relation if icebergTable(relation).isDefined => relation
    }
  }

  private def icebergTable(relation: DataSourceV2Relation): Option[SparkTable] = {
    relation.table match {
      case sparkTable: SparkTable => Some(sparkTable)
      case _ => None
    }
  }

  /** The table snapshot the scan reads: a pinned one, a branch head, or the current one. */
  private def scanSnapshotId(sparkTable: SparkTable): Option[Long] = {
    val pinned = sparkTable.snapshotId()
    if (pinned != null) {
      return Some(pinned)
    }

    val branch = sparkTable.branch()
    if (branch != null) {
      val branchSnapshot = SnapshotUtil.latestSnapshot(sparkTable.table(), branch)
      if (branchSnapshot != null) {
        return Some(branchSnapshot.snapshotId())
      }
    }

    val current = sparkTable.table().currentSnapshot()
    if (current != null) {
      Some(current.snapshotId())
    } else {
      None
    }
  }

  private def vectorColumn(distance: VectorDistanceExpression): Option[String] = {
    distance.vector match {
      case attribute: AttributeReference => Some(attribute.name)
      case _ => None
    }
  }

  private def queryFloats(distance: VectorDistanceExpression): Array[Float] = {
    distance.query.eval(EmptyRow) match {
      case values: org.apache.spark.sql.catalyst.util.ArrayData =>
        val doubles = values.toDoubleArray
        Array.tabulate(doubles.length)(index => doubles(index).toFloat)
      case other =>
        throw new IllegalArgumentException("Query vector must be an array of doubles: " + other)
    }
  }

  private def foldableInt(expression: Expression): Option[Int] = {
    if (expression.foldable) {
      expression.eval(EmptyRow) match {
        case value: Int => Some(value)
        case value: Long => Some(value.toInt)
        case _ => None
      }
    } else {
      None
    }
  }

  private def isAscendingDistance(order: SortOrder): Boolean = {
    order.direction == Ascending && order.child.isInstanceOf[VectorDistanceExpression]
  }

  /**
   * Reads the rows the index points at, with the columns the scan was asked for and in its order.
   * Returns nothing when the rows cannot be read that way, which is the case for a partitioned
   * table, whose data files do not carry the partition columns, and for a scan that asks for
   * metadata columns.
   */
  private def readRows(
      sparkTable: SparkTable,
      output: Seq[AttributeReference],
      hits: List[IndexSearchResult]): Option[Seq[InternalRow]] = {
    val names = output.map(attribute => attribute.name)
    val fields = sparkTable.table().schema().columns().asScala.map(field => field.name()).toSet
    if (!names.forall(name => fields.contains(name))) {
      return None
    }

    val positionsByFile = hits.groupBy(hit => hit.dataFile())
    val rows = scala.collection.mutable.ArrayBuffer[InternalRow]()
    try {
      positionsByFile.foreach { case (dataFile, hitsForFile) =>
        val positions = hitsForFile.map(hit => hit.rowPosition()).toSet
        // the table schema is passed in so the rows come back with the types the scan promised,
        // which parquet type inference does not always get right, a uuid for instance
        val expected = SparkSchemaUtil.convert(sparkTable.table().schema().select(names: _*))
        val data = spark.read.schema(expected).parquet(dataFile)
        // the scan reuses one row object, so copy before numbering: otherwise every position
        // would end up pointing at the last row of the file
        data.queryExecution.toRdd
          .map(row => row.copy())
          .zipWithIndex()
          .filter { case (_, position) => positions.contains(position) }
          .map { case (row, _) => row }
          .collect()
          .foreach(row => rows += row)
      }
    } catch {
      case scala.util.control.NonFatal(e) =>
        logger.warn("Cannot read the rows a vector index points at, keeping the original plan", e)

        return None
    }

    Some(rows)
  }

  private val EmptyRow: InternalRow = new GenericInternalRow(new Array[Any](0))

}
