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
      s"Text index ${metadata.indexUuid()} leaves ${uncovered.size()} files uncovered " +
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
    val positions = hits
      .groupBy(_.dataFile())
      .map { case (file, matches) =>
        file -> matches.map(hit => java.lang.Long.valueOf(hit.rowPosition())).asJavaCollection
      }
      .asJava
    val byFile = IndexEntries.rowsAt(spark, table, positions, columns.asJava, readSchema, tasks)
    val rows = hits.map { hit =>
      val row = Option(byFile.get(hit.dataFile()))
        .flatMap(rows => Option(rows.get(hit.rowPosition())))
        .getOrElse(throw new IllegalStateException(
          s"Text index ${metadata.indexUuid()} referred to a row absent from snapshot $snapshotId " +
            s"at ${hit.dataFile()}:${hit.rowPosition()}; run refresh_text_index"))
      Row.fromSeq(row.toSeq :+ hit.score())
    }.toVector

    val resultSchema = StructType(output.map(a => StructField(a.name, a.dataType, a.nullable)))
    spark.sparkContext.parallelize(rows, 1).mapPartitions { values =>
      val convert = CatalystTypeConverters.createToCatalystConverter(resultSchema)
      val unsafe = UnsafeProjection.create(resultSchema)
      values.map(row => unsafe(convert(row).asInstanceOf[InternalRow]).copy())
    }
  }
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
