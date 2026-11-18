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

import org.apache.iceberg.index.IndexQuery
import org.apache.iceberg.index.IndexQuery.Consistency
import org.apache.iceberg.index.spark.IndexEntries
import org.apache.iceberg.index.spark.DistributedIndexSearch
import org.apache.iceberg.index.spark.VectorIndexSearch
import org.apache.iceberg.index.spark.IndexRowFilter
import org.apache.iceberg.index.vector.VectorIndexAdapter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._

/**
 * How a lowered vector source produces rows.
 *
 * <p>Two branches share the decisions the optimizer already made: an index branch returns the live
 * rows its candidates point at, and, for a complete-table query, a scan branch returns the live rows
 * of the files the index does not cover. The operator above the source re-scores and sorts exactly,
 * so this class never decides which candidate is nearest.
 *
 * <p>An approximate kernel does not promise that a wider search keeps its smaller answer as a
 * prefix, so a task that has to ask its index file for more keeps the live rows it already found and
 * asks for a cumulative number of candidates. Asking only for the difference would let a re-ranked
 * candidate set drop rows that were already resolved.
 *
 * <p>Neither branch crosses the driver barrier with rows: the index branch reads the rows in the
 * tasks that found them and the scan branch reads its own, so nothing a task found is collected and
 * sent out again.
 */
private[spark] object VectorSearchExec {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  /** The rounds a delete-aware search may take before it returns what it has. */
  /**
   * The rows this query may answer with, read by the tasks that found them.
   *
   * <p>One job, and nothing crosses the driver barrier: each task searches its own index file, reads
   * the rows its hits name through the format model, applies the snapshot's deletes and the query's
   * filter, and keeps its own top-k. A row among the k nearest of the whole answer is also among the
   * k nearest of the file that found it, so the top-k of the tasks is the answer's top-k, and the
   * sort above the source decides the order.
   *
   * <p>The filter is a decision about this task's own work, not about the query's shape: with the
   * filter before the search, a task reads the columns it needs from the files its own index file
   * covers, so the rows it may score are the ones the filter admits and the kernel never scores the
   * rest; with the filter after the search, it scores the whole of its file, reads the rows it found
   * and judges them there, and the ones it turns down do not fill the window. Either way the columns
   * the filter needs are read by the task that judges it, and a task that comes up short asks its
   * own index file for more instead of sending the question back to the driver.
   */
  private def indexRows(
      spark: SparkSession,
      context: VectorSearchContext,
      names: Seq[String],
      readSchema: StructType,
      metrics: Map[String, SQLMetric]): RDD[InternalRow] = {
    val inputs = context.inputs
    val k = context.request.input.topK
    val queries = context.request.input.queries
    if (k <= 0 || queries.isEmpty || context.parameters == null) {
      return spark.sparkContext.emptyRDD[InternalRow]
    }

    // where the filter runs is the query's call, and it decides what this side of the search reads:
    // before the search, a task reads the filter's columns for the files it covers and the search
    // scores only those rows; after it, a task reads the columns with each row it found and judges
    // them there
    val prefilter =
      context.hasFilter && context.parameters.filterMode == VectorQueryParameters.FilterModePre
    val postfilter = context.hasFilter && !prefilter
    val missing =
      if (postfilter) context.filterColumns.filterNot(names.contains) else Seq.empty[String]
    val readColumns = names ++ missing
    val fetchSchema = widenedSchema(readSchema, missing, inputs.table.schema)
    val rowFilter =
      if (postfilter) {
        new IndexRowFilter(inputs.table.schema.select(readColumns.asJava), context.filter)
      } else {
        null
      }
    val candidateFilter =
      if (prefilter) {
        new IndexRowFilter(inputs.table.schema.select(context.filterColumns.asJava), context.filter)
      } else {
        null
      }
    val adapter = new VectorIndexAdapter(inputs.definition)
    val payloads =
      queries.map(query => VectorIndexSearch.encodeQuery(spark, adapter, query)).asJava
    val opening =
      if (context.budget == null) {
        VectorCandidateBudget.derive(k, context.parameters.overSample, inputs.entryUpperBound)
      } else {
        context.budget
      }
    // the counts describe rows that never reach the driver, so the tasks report them as they run
    val counters = new DistributedIndexSearch.RowCounters(
      metrics("numCandidates"),
      metrics("numCandidateRequests"),
      metrics("numLiveCandidates"),
      metrics("numFetchedRows"),
      metrics("numDeletedOrInvisibleHits"),
      metrics("numHitsOutsideSnapshot"),
      metrics("numRowsAtFiles"),
      metrics("numCandidateRounds"),
      metrics("numIndexUnderfilled"),
      metrics("numScanTasksShipped"))
    // the stages describe the same tasks' wall clock, and the metrics they write are the ones the
    // source declared, so a log that reads one reads the other
    val stages = new DistributedIndexSearch.SearchStages(metrics.asJava)

    val rows =
      DistributedIndexSearch.searchWithRows(
        spark,
        inputs.indexMetadata,
        inputs.table,
        IndexQuery
          .of(inputs.indexSnapshot.sourceTableSnapshotId(), payloads.get(0), opening.initial)
          .withConsistency(Consistency.AT_OR_BEFORE),
        payloads,
        inputs.table.schema.select(readColumns.asJava),
        fetchSchema,
        names.size,
        k,
        opening.initial,
        inputs.scanTasks,
        rowFilter,
        candidateFilter,
        if (prefilter) context.filterColumns.toList.asJava else java.util.Collections.emptyList(),
        counters,
        stages)

    // the format model returns rows, and the source's output is internal rows: this conversion is a
    // map over what the tasks produced, not a job that collects them and sends them out again
    spark.createDataFrame(rows, readSchema).queryExecution.toRdd
  }

  def rows(
      spark: SparkSession,
      source: VectorSearchSource,
      metrics: Map[String, SQLMetric]): (RDD[InternalRow], VectorSearchStats) = {
    val context = source.context
    val names = source.output.map(_.name)
    val readSchema = schemaOf(source.output)
    val indexBranch = indexRows(spark, context, names, readSchema, metrics)
    // a filtered query whose candidates could not be narrowed is answered by the scan branch: the
    // index has nothing to add once every row of the table may be in the answer
    val residual = residualRows(spark, context, names, readSchema)
    val residualFiles = if (residual == null) 0 else context.inputs.uncoveredDataFiles.size
    // the parameters are decided before a source is ever built, but the candidate loop treats them
    // as possibly absent, so the log reads them the same way instead of failing where that guard
    // would have returned an empty request
    val overSample =
      if (context.parameters == null) {
        "unset[unset]"
      } else {
        s"${context.parameters.overSample}[${context.parameters.overSampleOrigin}]"
      }
    logger.info(
      "vector search contract={} k={} overSample={} filterMode={} residualFiles={} "
        + "driverMaterialization=false",
      Array[Object](
        context.request.input.contract.name,
        Integer.valueOf(context.request.input.topK),
        overSample,
        if (context.parameters == null) "unset" else context.parameters.filterMode,
        Integer.valueOf(residualFiles)): _*)
    val rows =
      if (residual == null) indexBranch else spark.sparkContext.union(Seq(indexBranch, residual))
    val stats = VectorSearchStats(residualFiles)
    (rows, stats)
  }

  /** The live rows of the files the index does not cover, as a distributed branch. */
  private def residualRows(
      spark: SparkSession,
      context: VectorSearchContext,
      names: Seq[String],
      readSchema: StructType): RDD[InternalRow] = {
    val uncovered = context.inputs.uncoveredDataFiles
    if (uncovered.isEmpty || context.request.input.contract == VectorResultContract.IndexHits) {
      return null
    }

    val vectorIndex = names.indexOf(context.request.input.column)
    IndexEntries
      .liveRows(
        spark,
        context.inputs.table,
        uncovered.asJava,
        names.toList.asJava,
        readSchema,
        context.inputs.scanTasks)
      .queryExecution
      .toRdd
      // a row without a vector is not a candidate, and the index branch never returns one: the two
      // branches have to answer the same question, or the same row would appear or not depending on
      // whether its file happens to be covered
      .filter(row => vectorIndex < 0 || !row.isNullAt(vectorIndex))
      .map(row => row.copy())
  }

  def schemaOf(output: Seq[Attribute]): StructType =
    StructType(output.map { attribute =>
      StructField(attribute.name, attribute.dataType, attribute.nullable, attribute.metadata)
    }.toArray)

  /** The schema the fetched rows are read with: the projection, then what judging a row needs. */
  private def widenedSchema(
      base: StructType,
      extra: Seq[String],
      tableSchema: org.apache.iceberg.Schema): StructType = {
    if (extra.isEmpty) {
      return base
    }

    val widened =
      org.apache.iceberg.spark.SparkSchemaUtil.convert(tableSchema.select(extra.asJava))
    var widenedSchema = base
    widened.fields
      .filterNot(field => base.fieldNames.contains(field.name))
      .foreach(field => widenedSchema = widenedSchema.add(field))
    widenedSchema
  }
}

/**
 * What the driver knows about an execution before the tasks run: the files the scan branch was
 * planned over. Everything else about an execution is reported by the tasks themselves, because the
 * rows they read never reach the driver for it to count them.
 */
private[spark] case class VectorSearchStats(residualFiles: Int)
