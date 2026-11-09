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
import java.util.{List => JList}
import org.apache.iceberg.index.IndexQuery
import org.apache.iceberg.index.IndexQuery.Consistency
import org.apache.iceberg.index.IndexSearchResult
import org.apache.iceberg.index.spark.DistributedIndexSearch
import org.apache.iceberg.index.spark.IndexEntries
import org.apache.iceberg.index.spark.IndexSearch
import org.apache.iceberg.index.vector.VectorDistance
import org.apache.iceberg.index.vector.VectorIndexAdapter
import org.apache.spark.sql.Row
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * The shared entry point for a vector search whose result keeps the index's own hits: {@code CALL
 * vector_search} and the vector table functions.
 *
 * <p>Planning a request does what the ordinary SQL path does: it binds the table and the index,
 * freezes both snapshots, and decides only the parameters the caller left unset. Execution then
 * searches the frozen index snapshot and resolves the rows from the frozen table snapshot, so a
 * refresh or an append after planning cannot make one query read two snapshots.
 *
 * <p>This contract is not the complete-table one. The rows are the index's own ranked hits with
 * their raw scores, a hit whose row was removed still appears with a null row, and the files the
 * index does not cover are not scanned. Sharing the parameter decision does not change what a
 * caller gets back.
 */
private[spark] object VectorSearchBridge {

  /** The least a row can cost, for a column whose type declares no size of its own. */
  private[spark] val BytesPerRowFloor = 8L

  /**
   * Nearest first, and by where the row is when two distances are equal.
   *
   * <p>A distance alone leaves two hits in whatever order they arrived in, which is the order the
   * tasks happened to produce: a cached query and an uncached one then answer the same question
   * differently, and so do two runs of the same query. The data file and the row position make the
   * order a function of the question rather than of the scheduling.
   */
  private val NearestFirst: Ordering[(String, Long, Double, Row)] =
    Ordering.by[(String, Long, Double, Row), (Double, String, Long)](candidate =>
      (candidate._3, candidate._1, candidate._2))

  /** The same order for a candidate on its way back from the cluster, before it is a hit. */
  private val NearestScannedFirst: Ordering[(Row, String, java.lang.Long, Double)] =
    Ordering.by[(Row, String, java.lang.Long, Double), (Double, String, Long)](scored =>
      (scored._4, scored._2, scored._3.longValue()))

  /** A request whose contract is the index's own hits, planned but not searched. */
  def plan(
      spark: SparkSession,
      table: org.apache.iceberg.Table,
      column: String,
      indexArgument: String,
      queries: JList[Array[Float]],
      k: Int,
      batch: Boolean,
      coverage: String): VectorBindingResult = {
    val contract = VectorResultContract.forCoverage(coverage)
    val request = VectorSearchRequest.forTable(
      table,
      column,
      queries,
      k,
      // the request is planned against the table's current snapshot; a caller that has already
      // frozen one hands it to the search, not to the plan
      null,
      contract.name,
      indexArgument,
      batch)
    BindVectorSearch.bind(spark, request) match {
      case BoundVectorSearch(context) =>
        // a request that asks for nothing is answered with nothing: its parameters are not judged
        // against a search that will not happen
        if (k <= 0 || queries.isEmpty) {
          return BoundVectorSearch(
            context.copy(
              parameters = null,
              budget = VectorCandidateBudget(0L, 0, context.inputs.entryUpperBound)))
        }
        val parameters = VectorQueryParameters.decide(
          context.rawParameters,
          context.inputs.definition)
        BoundVectorSearch(
          context.copy(
            parameters = parameters,
            budget = VectorCandidateBudget
              .derive(k, parameters.overSample, context.inputs.entryUpperBound)))
      case other => other
    }
  }

  /**
   * Searches a planned request, and returns what its contract promises.
   *
   * <p>The hit view returns the index's own hits; the two coverage modes return the live rows of the
   * requested snapshot, scored with the metric the query asks for, which is the number
   * {@code ORDER BY vec_distance} would produce for the same row.
   */
  def execute(spark: SparkSession, context: VectorSearchContext): JList[Row] = {
    if (context.request.input.contract != VectorResultContract.IndexHits) {
      return nearestRows(spark, context)
    }

    val request = context.request.input
    val batch = request.batch
    val schema = if (batch) IndexSearch.RESULT_SCHEMA else IndexSearch.SINGLE_RESULT_SCHEMA
    if (request.topK <= 0 || request.queries.isEmpty) {
      return spark.createDataFrame(new java.util.ArrayList[Row](), schema).collectAsList()
    }

    // every query is searched first, then all of their hits are resolved in one pass, so a data
    // file that several queries hit is read once for the whole batch
    val hitsPerQuery = request.queries.map(query => searchHits(spark, context, query.toArray))
    val allHits = hitsPerQuery.flatten
    val json = resolveRows(spark, context, allHits)

    val rows = mutable.ListBuffer.empty[Row]
    hitsPerQuery.zipWithIndex.foreach { case (hits, queryId) =>
      hits.foreach { hit =>
        val row = Option(json.get(hit.dataFile()))
          .flatMap(byPosition => Option(byPosition.get(JLong.valueOf(hit.rowPosition()))))
          .orNull
        val values: Array[Object] =
          if (batch) {
            Array[Object](
              Integer.valueOf(queryId),
              hit.dataFile(),
              JLong.valueOf(hit.rowPosition()),
              java.lang.Double.valueOf(hit.score()),
              row)
          } else {
            Array[Object](
              hit.dataFile(),
              JLong.valueOf(hit.rowPosition()),
              java.lang.Double.valueOf(hit.score()),
              row)
          }
        rows += RowFactory.create(values: _*)
      }
    }
    spark.createDataFrame(rows.asJava, schema).collectAsList()
  }

  /**
   * The hits of one query, asking for a wider candidate set when more were requested.
   *
   * <p>A hit whose row was removed is kept: this contract reports what the index found and lets the
   * caller see the missing row, which is what makes the score comparable with the one the index
   * produced. Only the candidate budget grows, and the search stops as soon as the kernel says it
   * has nothing more to give.
   */
  private def searchHits(
      spark: SparkSession,
      context: VectorSearchContext,
      query: Array[Float]): Seq[IndexSearchResult] = {
    val inputs = context.inputs
    val k = context.request.input.topK
    val adapter = new VectorIndexAdapter(inputs.definition)
    val payload = adapter.encodeQuery(query)
    var budget = context.budget
    var round = 0
    val hits = mutable.LinkedHashMap.empty[(String, Long), IndexSearchResult]
    var done = false
    while (!done && round < 10) {
      round += 1
      val outcome = DistributedIndexSearch.search(
        spark,
        inputs.indexMetadata,
        inputs.table,
        IndexQuery
          .of(inputs.indexSnapshot.sourceTableSnapshotId(), payload, budget.initial)
          .withConsistency(Consistency.AT_OR_BEFORE))
      // a hit whose file this snapshot no longer has is kept: the caller sees the hit with a null
      // row, which is what this contract reports, instead of a shorter answer
      outcome
        .hits()
        .asScala
        .foreach(hit => hits.getOrElseUpdate((hit.dataFile(), hit.rowPosition()), hit))
      if (!outcome.moreHits() || hits.size >= k || budget.initial == 0) {
        done = true
      } else {
        val next =
          VectorCandidateBudget.nextOrNull(budget.initial, hits.size, k, budget.entryUpperBound)
        if (next == null) {
          done = true
        } else {
          budget = next
        }
      }
    }
    // the search asked for k times the multiplier, but this contract still answers one query with
    // at most k hits, in the index's own order
    hits.values.take(k).toSeq
  }

  /**
   * The nearest live rows of the requested snapshot, under the contract's coverage mode.
   *
   * <p>Every candidate is resolved against the frozen table snapshot before it can be answered with:
   * a hit whose data file the snapshot no longer has, or whose position no longer holds a live row,
   * is dropped. That is the same rule the rewritten query applies, and it is what makes a deleted
   * row impossible to return from either mode.
   *
   * <p>The table mode adds the rows of the files the index does not cover, scored the same way, and
   * takes the k nearest of both together, so its answer is the answer a scan would give. The indexed
   * mode stops at the index, so its answer can be shorter than k.
   *
   * <p>A batch is answered one query at a time: the scan of the uncovered files is a per-query
   * scoring pass, and sharing one pass across queries is an optimization this mode does not need yet.
   */
  private def nearestRows(spark: SparkSession, context: VectorSearchContext): JList[Row] = {
    val request = context.request.input
    val schema = structOf(context.request.output)
    if (request.topK <= 0 || request.queries.isEmpty) {
      return spark.createDataFrame(new java.util.ArrayList[Row](), schema).collectAsList()
    }

    val answered = mutable.ListBuffer.empty[Row]
    request.queries.zipWithIndex.foreach { case (queryVector, queryId) =>
      val query = queryVector.toArray
      val indexed = indexCandidates(spark, context, query)
      val scanned =
        if (request.contract == VectorResultContract.IndexCovered) {
          Seq.empty
        } else {
          uncoveredCandidates(spark, context, query)
        }

      val best = (indexed ++ scanned).sorted(NearestFirst).take(request.topK)
      // the selected rows are read once more, as JSON: it is at most k rows per query, and it
      // keeps the row's own schema deciding its shape
      val json = rowsJsonOf(spark, context, best.map(candidate => (candidate._1, candidate._2)))
      best.foreach { case (dataFile, position, distance, _) =>
        val rowJson =
          Option(json.get(dataFile))
            .flatMap(byPosition => Option(byPosition.get(JLong.valueOf(position))))
            .orNull
        answered += (if (request.batch) {
                       RowFactory.create(
                         Integer.valueOf(queryId),
                         dataFile,
                         JLong.valueOf(position),
                         java.lang.Double.valueOf(distance),
                         rowJson)
                     } else {
                       RowFactory.create(
                         dataFile,
                         JLong.valueOf(position),
                         java.lang.Double.valueOf(distance),
                         rowJson)
                     })
      }
    }

    spark.createDataFrame(answered.asJava, schema).collectAsList()
  }

  /**
   * The live rows the index covers that are nearest to one query, with their exact distance.
   *
   * <p>The search widens while it has fewer than k live rows: a deleted row keeps its place in the
   * index, and answering with k minus the deleted ones would report a short answer for a table that
   * has nothing wrong with it.
   */
  private def indexCandidates(
      spark: SparkSession,
      context: VectorSearchContext,
      query: Array[Float]): Seq[(String, Long, Double, Row)] = {
    val inputs = context.inputs
    val request = context.request.input
    val metric = inputs.definition.metric()
    val names = columnNames(context)
    val vectorIndex = names.indexOf(request.column)
    val adapter = new VectorIndexAdapter(inputs.definition)
    val payload = adapter.encodeQuery(query)

    val found = mutable.LinkedHashMap.empty[(String, Long), Row]
    var budget = context.budget
    var round = 0
    var done = false
    while (!done && round < 10) {
      round += 1
      val outcome = DistributedIndexSearch.search(
        spark,
        inputs.indexMetadata,
        inputs.table,
        IndexQuery
          .of(inputs.indexSnapshot.sourceTableSnapshotId(), payload, budget.initial)
          .withConsistency(Consistency.AT_OR_BEFORE))

      // a hit of a file the snapshot no longer has is not a candidate at all: the row it names is
      // gone, and scoring it would let a rewritten-away file occupy a place in the answer. The
      // fetch-back below drops these too — a scan task is only planned for a file the snapshot has —
      // so this is what keeps them out before the work of resolving them, not the only guard
      val fresh = outcome
        .hits()
        .asScala
        .filter(hit => inputs.dataFiles.contains(hit.dataFile()))
        .filterNot(hit => found.contains((hit.dataFile(), hit.rowPosition())))

      if (fresh.nonEmpty) {
        val byFile = fresh
          .groupBy(hit => hit.dataFile())
          .map { case (dataFile, hits) =>
            dataFile -> hits.map(hit => JLong.valueOf(hit.rowPosition())).toSet.asJava
          }
          .asJava
        val rowsByFile = IndexEntries.rowsAt(
          spark,
          inputs.table,
          byFile,
          names.toList.asJava,
          inputs.readSchema,
          inputs.scanTasks)
        fresh.foreach { hit =>
          val row =
            Option(rowsByFile.get(hit.dataFile()))
              .flatMap(byPosition => Option(byPosition.get(JLong.valueOf(hit.rowPosition()))))
              .orNull
          if (row != null && (vectorIndex < 0 || !row.isNullAt(vectorIndex))) {
            found.put((hit.dataFile(), hit.rowPosition()), row)
          }
        }
      }

      if (found.size >= request.topK || !outcome.moreHits() || budget.initial == 0) {
        done = true
      } else {
        val next =
          VectorCandidateBudget.nextOrNull(
            budget.initial,
            found.size,
            request.topK,
            budget.entryUpperBound)
        if (next == null) {
          done = true
        } else {
          budget = next
        }
      }
    }

    found.map { case ((dataFile, position), row) =>
      (dataFile, position, distanceOf(metric, row, vectorIndex, query), row)
    }.toSeq
  }

  /**
   * The k nearest live rows of the files the index does not cover, scored on the cluster.
   *
   * <p>This is the scan the table mode adds, and the reason it can answer with the whole table: a
   * file an index built earlier never saw is read here instead of being left out. Only k rows per
   * query come back to the driver.
   */
  private def uncoveredCandidates(
      spark: SparkSession,
      context: VectorSearchContext,
      query: Array[Float]): Seq[(String, Long, Double, Row)] = {
    val inputs = context.inputs
    val request = context.request.input
    val uncovered = inputs.uncoveredDataFiles
    if (uncovered.isEmpty) {
      return Seq.empty
    }

    val names = columnNames(context)
    val vectorIndex = names.indexOf(request.column)
    val rows = IndexEntries.liveRowsWithPositions(
      spark,
      inputs.table,
      uncovered.asJava,
      names.toList.asJava,
      inputs.readSchema,
      inputs.scanTasks)
    if (rows == null) {
      return Seq.empty
    }

    val metric = inputs.definition.metric()
    val dimension = query.length
    val broadcast = spark.sparkContext.broadcast(query)
    rows.rdd
      .filter { entry =>
        val row = entry._2._2
        vectorIndex >= 0 && !row.isNullAt(vectorIndex) &&
        row.getList[AnyRef](vectorIndex).size() == dimension
      }
      .map { entry =>
        val row = entry._2._2
        (row, entry._1, entry._2._1, distanceOf(metric, row, vectorIndex, broadcast.value))
      }
      // a row without a vector is not a candidate, and neither is one whose vector does not have
      // the dimension the query has: it cannot be compared with the query, so scoring it would fail
      // the whole search instead of leaving it out. The index branch cannot return one either,
      // because every entry it holds was built for one dimension, so both branches answer the same
      // question
      .takeOrdered(request.topK)(NearestScannedFirst)
      .map { scored =>
        (scored._2, scored._3.longValue(), scored._4, scored._1)
      }
      .toSeq
  }

  /** The schema a request answers with, which is what its output attributes declare. */
  private def structOf(
      output: Seq[org.apache.spark.sql.catalyst.expressions.Attribute]): StructType = {
    new StructType(
      output
        .map(attribute => StructField(attribute.name, attribute.dataType, attribute.nullable))
        .toArray)
  }

  private def columnNames(context: VectorSearchContext): Seq[String] = {
    context.inputs.table.schema().columns().asScala.map(column => column.name()).toSeq
  }

  /** The distance of one row's vector from the query, which is what the rewrite sorts by. */
  private def distanceOf(
      metric: org.apache.iceberg.index.vector.VectorIndexDefinition.Metric,
      row: Row,
      vectorIndex: Int,
      query: Array[Float]): Double = {
    if (vectorIndex < 0 || row.isNullAt(vectorIndex)) {
      return Double.PositiveInfinity
    }

    val values = row.getList[AnyRef](vectorIndex)
    val vector = new Array[Float](values.size())
    var i = 0
    while (i < vector.length) {
      vector(i) = values.get(i).asInstanceOf[java.lang.Number].floatValue()
      i += 1
    }

    VectorDistance.distance(metric, vector, query)
  }

  /** The JSON of the selected rows, keyed by data file and then by position. */
  private def rowsJsonOf(
      spark: SparkSession,
      context: VectorSearchContext,
      selected: Seq[(String, Long)]): java.util.Map[String, java.util.Map[JLong, String]] = {
    val inputs = context.inputs
    if (selected.isEmpty) {
      return java.util.Collections.emptyMap()
    }

    val positions = selected
      .groupBy(selected => selected._1)
      .map { case (dataFile, rows) =>
        dataFile -> rows.map(row => JLong.valueOf(row._2)).toSet.asJava
      }
      .asJava

    IndexEntries.rowsAtJson(
      spark,
      inputs.table,
      positions,
      columnNames(context).toList.asJava,
      inputs.readSchema,
      inputs.scanTasks)
  }

  /**
   * The JSON of each hit's live row, keyed by data file and then by position.
   *
   * <p>The JSON is serialised where the row is read: a lookup resolves a whole record per hit, and
   * collecting the records only to parallelise them again to serialise them would move them across
   * the driver barrier twice.
   */
  private def resolveRows(
      spark: SparkSession,
      context: VectorSearchContext,
      hits: Seq[IndexSearchResult]): java.util.Map[String, java.util.Map[JLong, String]] = {
    val inputs = context.inputs
    if (hits.isEmpty) {
      return java.util.Collections.emptyMap()
    }

    val names = inputs.table.schema().columns().asScala.map(_.name()).toSeq
    val positions = hits
      .groupBy(hit => hit.dataFile())
      .map { case (dataFile, fileHits) =>
        dataFile -> fileHits.map(hit => JLong.valueOf(hit.rowPosition())).toSet.asJava
      }
      .asJava

    IndexEntries.rowsAtJson(
      spark,
      inputs.table,
      positions,
      names.toList.asJava,
      inputs.readSchema,
      inputs.scanTasks)
  }
}

/**
 * The request a table function returns: the names, the vectors and the contract form, and nothing
 * that has been bound or read yet.
 *
 * <p>It is a serializable description rather than a frozen plan, because a physical node travels to
 * the driver that runs it and a snapshot, a manifest list or a scan task is not what should travel
 * with it. Binding still happens once, at the start of execution, and fixes both snapshots before
 * anything is read.
 */
private[spark] case class VectorSearchHitsRequest(
    tableName: String,
    column: String,
    indexArgument: String,
    queries: Seq[Seq[Float]],
    topK: Int,
    batch: Boolean)(override val output: Seq[org.apache.spark.sql.catalyst.expressions.Attribute])
    extends LeafNode {

  override protected def otherCopyArgs: Seq[AnyRef] = output :: Nil

  /**
   * The hits this request can answer with, which is all it ever returns.
   *
   * <p>A leaf of a plan has to estimate itself: an operator that sizes its input — a join does, a
   * filter or a projection does not — asks for one while planning, and the default estimate for a
   * leaf is not implemented. The contract bounds the answer tightly: one hit per query for each of
   * the {@code topK} results asked for, and no residual scan to add uncovered rows, unlike the
   * rewritten source.
   */
  override def computeStats: Statistics = {
    val perQuery = math.max(0, topK).toLong
    val rows = if (batch) perQuery * queries.size else perQuery
    val bytesPerRow = output.foldLeft(VectorSearchBridge.BytesPerRowFloor) { (bytes, attribute) =>
      bytes + attribute.dataType.defaultSize
    }
    Statistics(sizeInBytes = bytesPerRow * rows, rowCount = Some(rows))
  }

  def queryVectors: JList[Array[Float]] = {
    val vectors = new java.util.ArrayList[Array[Float]](queries.size)
    queries.foreach(query => vectors.add(query.toArray))
    vectors
  }

  override def simpleString(maxFields: Int): String =
    s"VectorSearchHits[contract=INDEX_HITS (table functions answer for the whole table), " +
      s"batch=$batch, table=$tableName, column=$column, k=$topK, queries=${queries.size}, " +
      s"driverMaterialization=true]"
}

/** A planned index-hit search whose request has already been bound to its snapshots. */
private[spark] case class VectorSearchHitsPlan(context: VectorSearchContext, batch: Boolean)
    extends LeafNode {

  override val output: Seq[org.apache.spark.sql.catalyst.expressions.Attribute] =
    context.request.output

  override def simpleString(maxFields: Int): String =
    s"VectorSearchHits[contract=${context.request.input.contract.name}, " +
      s"batch=$batch, k=${context.request.input.topK}, " +
      s"index=${context.inputs.indexMetadata.indexUuid()}, " +
      s"indexSnapshot=${context.inputs.indexSnapshot.snapshotId}, " +
      s"tableSnapshot=${context.inputs.tableSnapshot.snapshotId()}, " +
      s"coveredFiles=${context.inputs.dataFiles.size - context.inputs.uncoveredDataFiles.size}, " +
      s"uncoveredFiles=${context.inputs.uncoveredDataFiles.size}, driverMaterialization=true]"
}
