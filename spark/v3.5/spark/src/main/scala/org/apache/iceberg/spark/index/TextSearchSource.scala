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

import java.util.Collections
import java.util.function.Predicate
import org.apache.iceberg.Schema
import org.apache.iceberg.Table
import org.apache.iceberg.exceptions.ValidationException
import org.apache.iceberg.expressions.Binder
import org.apache.iceberg.expressions.{Expression => IcebergExpression}
import org.apache.iceberg.expressions.Expressions
import org.apache.iceberg.index.IndexMetadata
import org.apache.iceberg.index.IndexQuery
import org.apache.iceberg.index.IndexReader
import org.apache.iceberg.index.spark.DistributedIndexSearch
import org.apache.iceberg.index.spark.IndexEntries
import org.apache.iceberg.index.text.TextIndexAdapter
import org.apache.iceberg.index.text.TextIndexDefinition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.IndexErrors
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.BindReferences
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.InterpretedPredicate
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.expressions.Unevaluable
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.catalyst.optimizer.ConstantFolding
import org.apache.spark.sql.catalyst.optimizer.NullPropagation
import org.apache.spark.sql.catalyst.optimizer.ReplaceExpressions
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.datasources.SparkExpressionConverter
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

/** A text search over a bound table snapshot, planned without searching its index files. */
private[spark] case class TextSearchSource(
    table: Table,
    metadata: IndexMetadata,
    snapshotId: Long,
    query: String,
    limit: Int,
    filter: Option[TextSearchFilter],
    override val output: Seq[Attribute])
    extends LeafNode {

  override protected def stringArgs: Iterator[Any] =
    Iterator(
      s"index=${metadata.indexUuid()}",
      s"snapshot=$snapshotId",
      s"k=$limit",
      s"prefilter=${filter.isDefined}")

  override def computeStats(): Statistics =
    Statistics(BigInt(limit) * math.max(1, output.map(_.dataType.defaultSize).sum))
}

/** Runs distributed scoring and reads only the rows that won the global merge. */
private[spark] case class TextSearchExec(
    spark: SparkSession,
    @transient private val source: TextSearchSource,
    override val output: Seq[Attribute])
    extends LeafExecNode {

  override protected def doExecute(): RDD[InternalRow] = {
    val table = source.table
    val metadata = source.metadata
    val indexUuid = metadata.indexUuid()
    val snapshotId = source.snapshotId
    val adapter = new TextIndexAdapter(metadata.definition().asInstanceOf[TextIndexDefinition])
    val query = IndexQuery.of(snapshotId, adapter.encodeQuery(source.query), source.limit)
    val tasks = source.filter match {
      case Some(filter) => IndexEntries.scanTasks(table, snapshotId, filter.statsColumns.asJava)
      case None => IndexEntries.scanTasks(table, snapshotId)
    }
    val files = tasks.asScala.map(_.file().location().toString).asJava
    val uncovered = new IndexReader(metadata, table).uncoveredDataFiles(query, files)
    require(
      uncovered.isEmpty,
      s"Text index $indexUuid leaves ${uncovered.size()} files uncovered " +
        s"at snapshot $snapshotId; run refresh_text_index")

    val hits = source.filter match {
      case Some(filter) =>
        DistributedIndexSearch
          .searchFilteredText(
            spark,
            metadata,
            table,
            query,
            tasks,
            filter.projection,
            filter.pruning,
            filter)
          .asScala
      case None =>
        DistributedIndexSearch
          .searchBatch(spark, metadata, table, java.util.Collections.singletonList(query))
          .get(0)
          .asScala
    }
    if (hits.isEmpty) {
      return spark.sparkContext.emptyRDD[InternalRow]
    }

    val columns = output.dropRight(1).map(_.name)
    val readSchema = IndexEntries.sparkSchema(table, columns.asJava)
    val resultSchema = StructType(output.map(a => StructField(a.name, a.dataType, a.nullable)))

    // what a task needs to answer with a row: the score of the hit that named it, where that hit
    // sat in the merged order, and which positions its files owe an answer
    val searched = hits.zipWithIndex.map { case (hit, index) => (hit, index) }.toVector
    val scored = searched
      .groupBy { case (hit, _) => (hit.dataFile(), hit.rowPosition()) }
      .map { case (at, atPosition) =>
        at -> atPosition.map { case (hit, index) => (index, hit.score()) }.sortBy(_._1).toArray
      }
    val expected = searched
      .groupBy { case (hit, _) => hit.dataFile() }
      .map { case (file, ofFile) =>
        file -> ofFile.map { case (hit, _) => hit.rowPosition() }.toSet
      }

    // a hit names a data file of the snapshot the index was bound to, so a hit of a file the
    // snapshot no longer plans is a stale index: failing before the rows are read keeps the answer
    // from being quietly short
    val planned = tasks.asScala.map(_.file().location().toString).toSet
    searched.find { case (hit, _) => !planned.contains(hit.dataFile()) }.foreach { case (hit, _) =>
      throw TextSearchHits.absentRow(indexUuid, snapshotId, hit.dataFile(), hit.rowPosition())
    }

    val down = spark.sparkContext.broadcast(TextSearchHits(scored, expected))
    val wanted = hits
      .groupBy(_.dataFile())
      .map { case (file, matches) =>
        file -> matches.map(hit => java.lang.Long.valueOf(hit.rowPosition())).asJavaCollection
      }
      .asJava
    val fetched =
      IndexEntries.rowsAtRdd(spark, table, wanted, columns.asJava, readSchema, tasks)
    if (fetched == null) {
      return spark.sparkContext.emptyRDD[InternalRow]
    }

    // One partition per hit data file, and the conversion happens in the task that read the row:
    // the rows of the answer never pass through the driver, and the reads of the hit files run
    // side by side rather than one after another
    fetched.rdd.mapPartitions { entries =>
      val plan = down.value
      val convert = CatalystTypeConverters.createToCatalystConverter(resultSchema)
      val unsafe = UnsafeProjection.create(resultSchema)
      val read = entries.toVector

      // a fetch-back reads the positions it was asked for, so a position the answer names and the
      // read did not answer with is a row the snapshot no longer has, which is a stale index; a
      // file whose every hit is gone answers no rows at all, which this cannot tell from a file
      // that was never hit
      for ((file, ofFile) <- read.groupBy(_._1)) {
        val found = ofFile.map(entry => entry._2._1.longValue()).toSet
        val missing = plan.expected.getOrElse(file, Set.empty[Long]) -- found
        if (missing.nonEmpty) {
          throw TextSearchHits.absentRow(indexUuid, snapshotId, file, missing.head)
        }
      }

      // the rows of a partition arrive in data file and position order, and the answer's order is
      // the order the hits merged in: sorting one partition's rows is a sort of what this
      // partition read, not a pass over the whole answer, and for a search whose hits came from
      // one file it restores exactly the order the answer had when it was built on the driver
      read
        .flatMap { case (file, (position, row)) =>
          plan.scored
            .getOrElse((file, position.longValue()), Array.empty[(Int, Double)])
            .map { case (index, score) => (index, Row.fromSeq(row.toSeq :+ score)) }
        }
        .sortBy(_._1)
        .iterator
        .map { case (_, row) => unsafe(convert(row).asInstanceOf[InternalRow]).copy() }
    }
  }
}

/**
 * The hits of one search, as the tasks that answer with their rows see them: the score of the hit
 * that named a row, where that hit sat in the merged order, and the positions each hit file owes.
 *
 * <p>The driver merges the hits, so it knows all of this; handing it down once is what lets the rows
 * be read and converted where they are read instead of being collected and sent out again.
 */
private[spark] case class TextSearchHits(
    scored: Map[(String, Long), Array[(Int, Double)]],
    expected: Map[String, Set[Long]])

private[spark] object TextSearchHits {

  /** The failure of a hit whose row the snapshot does not have: the index refers to a lost row. */
  def absentRow(
      indexUuid: String,
      snapshotId: Long,
      dataFile: String,
      position: Long): IllegalStateException =
    new IllegalStateException(
      s"Text index $indexUuid referred to a row absent from snapshot $snapshotId " +
        s"at $dataFile:$position; run refresh_text_index")
}

/** Uses Spark SQL's truth rules for row admission and Iceberg's inclusive filter for pruning. */
private[spark] case class TextSearchFilter(
    projection: Schema,
    pruning: IcebergExpression,
    statsColumns: Seq[String],
    condition: Expression,
    schema: StructType)
    extends Predicate[Row]
    with Serializable {

  @transient private lazy val evaluate = InterpretedPredicate(condition)
  @transient private lazy val convert = CatalystTypeConverters.createToCatalystConverter(schema)

  override def test(row: Row): Boolean = evaluate.eval(convert(row).asInstanceOf[InternalRow])
}

private[spark] object TextSearchFilter {
  def bind(
      spark: SparkSession,
      schema: Schema,
      attributes: Seq[Attribute],
      sql: String): TextSearchFilter = {
    val parsed = spark.sessionState.sqlParser.parseExpression(sql)
    if (parsed.exists(_.isInstanceOf[SubqueryExpression])) {
      throw IndexErrors.analysis("search filter does not accept subqueries")
    }

    val relation = LocalRelation(attributes)
    val resolved =
      ReplaceExpressions(spark.sessionState.executePlan(Filter(parsed, relation)).analyzed)
    val condition = resolved match {
      case Filter(expression, _: LocalRelation) if expression.deterministic => expression
      case _ => throw IndexErrors.analysis("search requires a deterministic scalar filter")
    }
    val columns = attributes.filter(condition.references.contains)
    if (columns.exists(attr => !schema.findField(attr.name).`type`().isPrimitiveType)) {
      throw IndexErrors.analysis("search filter requires top-level scalar columns")
    }

    val bound = BindReferences.bindReference(condition, columns)
    if (bound.exists(_.isInstanceOf[Unevaluable])) {
      throw IndexErrors.analysis("search requires a deterministic scalar filter")
    }

    val (pruning, statsColumns) =
      try {
        val folded = NullPropagation(ConstantFolding(Filter(condition, relation))) match {
          case Filter(expression, _) => expression
          case _ => condition
        }
        val pruning = pruningExpression(folded)
        val fieldIds =
          Binder.boundReferences(schema.asStruct(), Collections.singletonList(pruning), true)
        val names =
          columns.map(_.name).filter(name => fieldIds.contains(schema.findField(name).fieldId()))
        (pruning, names)
      } catch {
        case _: IllegalArgumentException | _: ValidationException =>
          (Expressions.alwaysTrue(), Seq.empty)
      }

    TextSearchFilter(
      schema.select(columns.map(_.name).asJava),
      pruning,
      statsColumns,
      bound,
      StructType(columns.map(a => StructField(a.name, a.dataType, a.nullable))))
  }

  private def pruningExpression(condition: Expression): IcebergExpression =
    condition match {
      case Literal(true, _) => Expressions.alwaysTrue()
      case Literal(false, _) | Literal(null, _) => Expressions.alwaysFalse()
      // Spark treats both signs of zero as equal, while Iceberg orders negative zero first.
      case other
          if other.references.exists(a => a.dataType == FloatType || a.dataType == DoubleType) =>
        Expressions.alwaysTrue()
      case other => SparkExpressionConverter.convertToIcebergExpression(other)
    }
}
