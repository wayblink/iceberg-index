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

import java.nio.ByteBuffer

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.iceberg.util.SnapshotUtil
import org.apache.iceberg.index.IndexQuery
import org.apache.iceberg.index.IndexQuery.Consistency
import org.apache.iceberg.index.IndexReader
import org.apache.iceberg.index.IndexSearchResult
import org.apache.iceberg.index.vector.VectorIndexAdapter
import org.apache.iceberg.index.vector.VectorIndexDefinition
import org.apache.iceberg.index.vector.VectorDistance
import org.apache.iceberg.index.vector.VectorIndexDefinition.VectorStructure
import org.apache.iceberg.index.vector.VectorIndexDefinition.Metric
import org.apache.iceberg.index.spark.{DistributedIndexSearch, IndexEntries, VectorIndexes}
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.Ascending
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.Row
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.plans.logical.GlobalLimit
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Sort
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.StructType
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
 * <p>An index that is behind the snapshot being read is still used, together with a scan of the
 * data files it does not cover, so a query does not have to wait for a refresh. Rows the index
 * points at are read through the same delete-aware reader the index is built with, so rows removed
 * after the index snapshot are not returned. What is deliberately left alone is an approximate
 * (HNSW) index, whose candidates have to be re-scored first, which is later work.
 */
case class RewriteVectorSearch(spark: SparkSession) extends Rule[LogicalPlan] {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  /** How often more hits are asked for when delete files removed some of them. */
  private val MaxHitRetries = 8

  /**
   * How many candidates to ask the index for, per hit the query actually wants.
   *
   * <p>A kernel that scores approximately cannot be trusted to put the true nearest neighbours
   * first, so the search asks for more than it needs and the exact re-scoring below decides. How
   * much more is a property of the kernel and of how tightly the distances of a dataset cluster:
   * measured on clustered 128 dimensional data, three bits per dimension needs ten times the hits
   * for the true top ten to be inside the set, and one bit needs fifty. It is a configuration
   * rather than a constant so it can be measured rather than assumed.
   *
   * <p>One means ask for exactly what the query wants, which is what an exactly scoring kernel
   * needs: merging each file's exact top-k gives the exact global top-k, so asking for more would
   * change nothing.
   */
  private val OverSampleConf = "spark.iceberg.index.vector.over-sample"

  /**
   * Which of the column's indexes a query is answered from, by name or by UUID.
   *
   * <p>A column may hold several indexes that differ in their parameters, which is how two of them
   * are compared on one table. A query written as {@code ORDER BY vec_distance(...)} has nowhere to
   * write the name, so it arrives as a setting; without it, a column that holds several indexes
   * leaves the plan alone, because answering from one of them would be a guess.
   */
  private val IndexNameConf = "spark.iceberg.index.vector.name"

  /**
   * A setting, from the spark context if it was given at submit time and from the session otherwise.
   *
   * <p>A rule cannot assume it was configured the same way as the process that started it: the
   * context carries what spark-submit was given and the session carries what a SET statement set.
   */
  private def conf(name: String): Option[String] = {
    spark.sparkContext.getConf
      .getOption(name)
      .orElse(spark.conf.getOption(name))
      .map(_.trim)
      .filter(_.nonEmpty)
  }

  private def overSample: Int = {
    val configured = conf(OverSampleConf).map(_.toInt).getOrElse(1)
    if (configured < 1) 1 else configured
  }

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

  /** The index a query on this column is answered from, or none when the plan should stand aside. */
  private def indexFor(
      table: org.apache.iceberg.Table,
      column: String): Option[org.apache.iceberg.index.IndexMetadata] = {
    val named = conf(IndexNameConf)
    if (named.isDefined) {
      return Some(VectorIndexes.findIndex(table, column, "vector", named.get))
    }

    val indexes = VectorIndexes.indexesFor(table, column, "vector")
    if (indexes.size() > 1) {
      // answering from one of them would be a guess, and a guess that returns another index's rows
      // looks exactly like an answer
      logger.warn(
        "Column {} of table {} has {} vector indexes and the query does not name one; set {} to "
          + "choose, or leave the query to scan",
        Array[Object](column, table.name(), Integer.valueOf(indexes.size()), IndexNameConf): _*)
      return None
    }

    if (indexes.isEmpty) None else Some(indexes.get(0))
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
    val limit = IndexRewriteSupport.foldableInt(k).getOrElse(return None)
    if (limit <= 0) {
      return None
    }

    val scan = IndexRewriteSupport.findIcebergScan(node).getOrElse(return None)
    val sparkTable = IndexRewriteSupport.icebergTable(scan).getOrElse(return None)
    val table = sparkTable.table()

    // a data file does not carry the values of its partition columns, but the ones an identity
    // partition puts in the path are recovered from it, so only the other transforms are a reason
    // to stand aside
    if (!IndexRewriteSupport.hasOnlyIdentityPartitions(table)) {
      return None
    }

    val names = IndexRewriteSupport.scanColumns(scan).getOrElse(return None)

    val column = vectorColumn(distance).getOrElse(return None)
    val columnIndex = names.indexOf(column)
    if (columnIndex < 0) {
      return None
    }

    val index = indexFor(table, column).getOrElse(return None)
    if (!IndexRewriteSupport.isIndexTypeRegistered(index)) {
      logger.warn(
        "Index type {} of column {} of table {} is not registered in this session, scanning",
        Array[Object](index.indexType(), column, table.name()): _*)
      return None
    }
    val definition = index.definition().asInstanceOf[VectorIndexDefinition]

    // an exact index is the vector column repacked with a row reference, and searching it walks
    // every entry, which is the same work as scanning the table, over a payload that is larger than
    // the column it repacks. Rewriting would make the query slower, so the scan is left alone.
    if (definition.structure() == VectorStructure.FLAT) {
      logger.warn(
        "Index of column {} of table {} is exact, which searches every entry like a scan does, "
          + "so scanning",
        Array[Object](column, table.name()): _*)
      return None
    }

    val snapshotId = IndexRewriteSupport.scanSnapshotId(sparkTable).getOrElse {
      return None
    }
    // the reader resolves the index snapshot from the parent chain of the table snapshots
    val reader = new IndexReader(index, table)
    val query = queryFloats(distance)
    val payload = new VectorIndexAdapter(definition).encodeQuery(query)
    val metric = definition.metric()
    val t0 = System.currentTimeMillis()
    // one pass over the snapshot's manifests serves the whole rewrite: the data file list the index
    // coverage is checked against, and the rows the hits point at
    val scanTasks = IndexEntries.scanTasks(table, snapshotId)
    val dataFiles =
      scanTasks.asScala.map(task => task.file().location().toString()).toSet
    logger.info(
      "TIMING plan data files {} ms",
      java.lang.Long.valueOf(System.currentTimeMillis() - t0))

    // an index with nothing to say about this snapshot is not an error, it just means scanning
    val coverage = IndexQuery.of(snapshotId, payload, 0).withConsistency(Consistency.AT_OR_BEFORE)
    val uncovered =
      try {
        reader.uncoveredDataFiles(coverage, dataFiles.asJava).asScala.toSet
      } catch {
        case NonFatal(e) =>
          logger.warn(
            "No index snapshot to answer a query about snapshot {}, scanning instead",
            snapshotId)
          return None
      }
    logger.info("TIMING uncovered {} ms", java.lang.Long.valueOf(System.currentTimeMillis() - t0))
    if (uncovered.size == dataFiles.size) {
      // the index is so far behind that its uncovered part is the whole table
      return None
    }

    val tableSchema = IndexRewriteSupport.tableSchema(sparkTable)
    val tSearch = System.currentTimeMillis()
    val hitRows =
      liveHitRows(
        reader,
        index,
        table,
        snapshotId,
        payload,
        dataFiles,
        limit,
        names,
        tableSchema,
        scanTasks)
        .getOrElse(return None)
    logger.info(
      "TIMING search+resolve {} ms",
      java.lang.Long.valueOf(System.currentTimeMillis() - tSearch))

    val residual =
      residualRows(
        table,
        uncovered,
        names,
        tableSchema,
        limit,
        columnIndex,
        query,
        metric,
        scanTasks)
    val tMerge = System.currentTimeMillis()
    val rows =
      (hitRows ++ residual).sortBy(row => distanceOf(row, columnIndex, query, metric)).take(limit)
    logger.info("TIMING merge {} ms", java.lang.Long.valueOf(System.currentTimeMillis() - tMerge))

    if (rows.isEmpty) {
      return None
    }

    val specialized = distance.copy(metric = metric)
    val newSort =
      Sort(
        Seq(SortOrder(specialized, Ascending)),
        true,
        LocalRelation(scan.output, IndexRewriteSupport.internalRows(spark, rows)))
    Some(node.withNewChildren(Seq(newSort)))
  }

  /**
   * The rows of the index hits that still exist in the table snapshot being read, best first.
   *
   * <p>The index is asked for more hits than the query wants, because its scores are estimates: what
   * it returns is a candidate set that the caller re-scores exactly, so that the nearest rows are
   * chosen by the real distance rather than by the estimate. The hits arrive in the adapter's own
   * order, which is the order a kernel is searched in, so the best of them is the first one whatever
   * the metric measures.
   *
   * <p>The index is asked for still more hits when delete files removed some of them, because the
   * limit has to be met with live rows: a removed row leaves a hole the next hit fills.
   */
  private def liveHitRows(
      reader: IndexReader,
      index: org.apache.iceberg.index.IndexMetadata,
      table: org.apache.iceberg.Table,
      snapshotId: Long,
      payload: ByteBuffer,
      dataFiles: Set[String],
      limit: Int,
      names: Seq[String],
      tableSchema: StructType,
      scanTasks: java.util.List[org.apache.iceberg.FileScanTask]): Option[List[Row]] = {
    // the index is asked for more candidates than the query wants when the kernel scores
    // approximately; the exact re-scoring below then has the true neighbours to choose from
    var searchLimit = Math.multiplyExact(limit, overSample)
    logger.info(
      "index search asks for {} hits for a limit of {} (over-sample {})",
      java.lang.Integer.valueOf(searchLimit),
      java.lang.Integer.valueOf(limit),
      java.lang.Integer.valueOf(overSample))
    var rows: List[Row] = Nil
    var done = false
    var exhausted = false
    var retries = 0
    while (!done && !exhausted && retries < MaxHitRetries) {
      retries += 1
      val outcome =
        DistributedIndexSearch
          .search(
            spark,
            index,
            table,
            IndexQuery
              .of(snapshotId, payload, searchLimit)
              .withConsistency(Consistency.AT_OR_BEFORE))
      val searched = outcome.hits().asScala.toList
      exhausted = !outcome.moreHits()

      // hits of data files the table no longer has cannot be resolved, so they are dropped here,
      // and the order of the adapter is kept: the direction of that order is a function of the
      // metric, a distance putting the smallest score first and a similarity the largest, so
      // re-sorting by "best score" would keep the farthest candidates of a distance
      val hits =
        searched.filter(hit => dataFiles.contains(hit.dataFile))
      val positionsByFile = hits
        .groupBy(hit => hit.dataFile())
        .map { case (dataFile, fileHits) =>
          dataFile -> fileHits.map(hit => java.lang.Long.valueOf(hit.rowPosition())).toSet.asJava
        }
      val rowsByFile =
        IndexEntries.rowsAt(
          spark,
          table,
          positionsByFile.asJava,
          names.toList.asJava,
          tableSchema,
          scanTasks)

      // the live hits, best first: a removed row leaves a hole the next hit fills
      rows = hits.flatMap(hit => liveRow(rowsByFile, hit))
      done = rows.size >= limit || exhausted
      if (!done && !exhausted) {
        searchLimit = searched.size + (limit - rows.size)
      }
    }

    // every live candidate is handed back rather than the first `limit` of them: the caller re-scores
    // what it is given exactly and keeps the nearest, and cutting the list here would leave the
    // kernel's own order to decide which candidates reach that re-scoring, which is the whole of what
    // the over-sampling was asked for
    Some(rows)
  }

  /** The row of a hit, or nothing when the row was removed after the index snapshot was built. */
  private def liveRow(
      rowsByFile: java.util.Map[String, java.util.Map[java.lang.Long, Row]],
      hit: IndexSearchResult): Option[Row] = {
    Option(rowsByFile.get(hit.dataFile)).flatMap(byPosition =>
      Option(byPosition.get(hit.rowPosition())))
  }

  /** All live rows of the data files the index does not cover, closest first. */
  private def residualRows(
      table: org.apache.iceberg.Table,
      uncovered: Set[String],
      names: Seq[String],
      tableSchema: StructType,
      limit: Int,
      columnIndex: Int,
      query: Array[Float],
      metric: Metric,
      scanTasks: java.util.List[org.apache.iceberg.FileScanTask]): List[Row] = {
    if (uncovered.isEmpty) {
      return Nil
    }

    IndexEntries
      .liveRows(spark, table, uncovered.asJava, names.toList.asJava, tableSchema, scanTasks)
      .rdd
      .takeOrdered(limit)(Ordering.by(row => distanceOf(row, columnIndex, query, metric)))
      .toList
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
        IndexRewriteSupport.toFloats(
          values,
          distance.query.dataType.asInstanceOf[org.apache.spark.sql.types.ArrayType].elementType)
      case other =>
        throw new IllegalArgumentException("Query vector must be an array of numbers: " + other)
    }
  }

  /** Whether a sort order is an ascending distance, which is what a nearest-neighbour query is. */
  private def isAscendingDistance(order: SortOrder): Boolean = {
    order.direction == Ascending && order.child.isInstanceOf[VectorDistanceExpression]
  }

  /** The distance of a row to the query vector; a row without a usable vector sorts last. */
  private def distanceOf(
      row: Row,
      columnIndex: Int,
      query: Array[Float],
      metric: Metric): Double = {
    IndexRewriteSupport.arrayValue(row, columnIndex) match {
      case Some(vector) if vector.length == query.length =>
        VectorDistance.distance(metric, vector, query)
      case _ =>
        Double.MaxValue
    }
  }

  private val EmptyRow: InternalRow = new GenericInternalRow(new Array[Any](0))
}
