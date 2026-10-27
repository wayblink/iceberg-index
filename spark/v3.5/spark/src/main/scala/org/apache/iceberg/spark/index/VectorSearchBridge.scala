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

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.iceberg.index.{IndexQuery, IndexSearchResult}
import org.apache.iceberg.index.IndexQuery.Consistency
import org.apache.iceberg.index.spark.{DistributedIndexSearch, IndexEntries, IndexSearch}
import org.apache.iceberg.index.vector.VectorIndexAdapter
import org.apache.spark.sql.{Row, RowFactory, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.LeafNode

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

  /** A request whose contract is the index's own hits, planned but not searched. */
  def plan(
      spark: SparkSession,
      table: org.apache.iceberg.Table,
      column: String,
      indexArgument: String,
      queries: JList[Array[Float]],
      k: Int,
      tableSnapshotId: JLong,
      batch: Boolean): VectorBindingResult = {
    val request = VectorSearchRequest.forTable(
      table,
      column,
      queries,
      k,
      tableSnapshotId,
      VectorResultContract.IndexHits.name,
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
        val parameters = VectorQueryParameters.decide(context.rawParameters)
        BoundVectorSearch(
          context.copy(
            parameters = parameters,
            budget = VectorCandidateBudget
              .derive(k, parameters.overSample, context.inputs.entryUpperBound)))
      case other => other
    }
  }

  /** Searches a planned request and returns its hits, one row per hit. */
  def execute(spark: SparkSession, context: VectorSearchContext): JList[Row] = {
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

  def queryVectors: JList[Array[Float]] = {
    val vectors = new java.util.ArrayList[Array[Float]](queries.size)
    queries.foreach(query => vectors.add(query.toArray))
    vectors
  }

  override def simpleString(maxFields: Int): String =
    s"VectorSearchHits[contract=INDEX_HITS, batch=$batch, table=$tableName, column=$column, " +
      s"k=$topK, queries=${queries.size}, driverMaterialization=true]"
}

/** A planned index-hit search whose request has already been bound to its snapshots. */
private[spark] case class VectorSearchHitsPlan(context: VectorSearchContext, batch: Boolean)
    extends LeafNode {

  override val output: Seq[org.apache.spark.sql.catalyst.expressions.Attribute] =
    context.request.output

  override def simpleString(maxFields: Int): String =
    s"VectorSearchHits[contract=INDEX_HITS, batch=$batch, k=${context.request.input.topK}, " +
      s"index=${context.inputs.indexMetadata.indexUuid()}, " +
      s"indexSnapshot=${context.inputs.indexSnapshot.snapshotId}, " +
      s"tableSnapshot=${context.inputs.tableSnapshot.snapshotId()}, driverMaterialization=true]"
}
