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

import org.apache.iceberg.index.IndexSearchResult
import org.apache.iceberg.index.spark.IndexEntries
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.Decimal
import org.apache.spark.sql.types.StructType
import scala.collection.JavaConverters._

/**
 * What the index rewrite rules have in common: finding the scan a query reads, resolving the rows a
 * set of hits points at, and turning rows into the internal rows a local relation holds.
 *
 * <p>Row resolution goes through the same delete-aware reader the index is built with, so a row
 * removed after the index snapshot is not returned, and its position is absent from the result. The
 * caller learns which hits survived from what is missing.
 */
object IndexRewriteSupport {

  /**
   * The floats of a vector, read element by element.
   *
   * <p>Spark's own conversion from an array to doubles only works when the array is backed by
   * doubles, and a vector column is not necessarily one: {@code array(1.0, 2.0)} in a query is an
   * array of decimals, and a {@code float} column is backed by floats. Both fail a cast to double,
   * so every element is converted on its own, through the element type the expression knows.
   */
  def toFloats(values: ArrayData, elementType: DataType): Array[Float] = {
    val count = values.numElements()
    Array.tabulate(count)(index => toFloat(values.get(index, elementType)))
  }

  private def toFloat(value: Any): Float = {
    value match {
      case null => throw new IllegalArgumentException("Vector element is null")
      // Spark's Decimal wraps a BigDecimal rather than extending Number, so it is taken apart here
      case decimal: Decimal => decimal.toDouble.floatValue
      case number: Number => number.floatValue
      case other => throw new IllegalArgumentException("Vector element is not a number: " + other)
    }
  }

  /**
   * The query vector of a distance expression, as floats.
   *
   * <p>The expression's array is read element by element, because the element type is whatever the
   * query wrote and not necessarily a double.
   */
  def queryOf(distance: VectorDistanceExpression): Option[Array[Float]] = {
    if (!distance.query.foldable) {
      return None
    }
    distance.query.eval(EmptyRow) match {
      case values: ArrayData =>
        try {
          Some(toFloats(
            values,
            distance.query.dataType.asInstanceOf[org.apache.spark.sql.types.ArrayType].elementType))
        } catch {
          case _: IllegalArgumentException => None
        }
      case _ => None
    }
  }

  /** The first Iceberg scan of a plan, which is the one the indexed column belongs to. */
  def findIcebergScan(plan: LogicalPlan): Option[DataSourceV2Relation] = {
    plan.collectFirst {
      case relation: DataSourceV2Relation if icebergTable(relation).isDefined => relation
    }
  }

  def icebergTable(relation: DataSourceV2Relation): Option[SparkTable] = {
    relation.table match {
      case sparkTable: SparkTable => Some(sparkTable)
      case _ => None
    }
  }

  /** The table snapshot the scan reads: a pinned one, a branch head, or the current one. */
  def scanSnapshotId(sparkTable: SparkTable): Option[Long] = {
    val pinned = sparkTable.snapshotId()
    if (pinned != null) {
      return Some(pinned)
    }

    val branch = sparkTable.branch()
    if (branch != null) {
      val branchSnapshot =
        org.apache.iceberg.util.SnapshotUtil.latestSnapshot(sparkTable.table(), branch)
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

  /**
   * Whether every partition column of the table is identity-partitioned, which is what makes its
   * value recoverable from the data file path. A bucket, truncation or time transform does not put
   * the column value itself in the path, so the rows read back would have a null in its place.
   */
  def hasOnlyIdentityPartitions(table: org.apache.iceberg.Table): Boolean = {
    table
      .spec()
      .fields()
      .asScala
      .forall(field => field.transform().isIdentity())
  }

  /**
   * Whether the type of an index is registered in this session, which is what the reader needs to
   * create its adapter. A session that never touched an index type has not registered it, and a
   * query must not fail for that.
   */
  def isIndexTypeRegistered(index: org.apache.iceberg.index.IndexMetadata): Boolean = {
    org.apache.iceberg.index.IndexTypes.registered().containsKey(index.indexType())
  }

  /** The Spark schema of a table, which is what its rows are read with. */
  def tableSchema(sparkTable: SparkTable): StructType = {
    SparkSchemaUtil.convert(sparkTable.table().schema())
  }

  /** The names a scan was asked for, and the table columns they have to be a subset of. */
  def scanColumns(scan: DataSourceV2Relation): Option[Seq[String]] = {
    val fields = scan.table
      .asInstanceOf[SparkTable]
      .table()
      .schema()
      .columns()
      .asScala
      .map(field => field.name())
      .toSet
    val names = scan.output.map(attribute => attribute.name)
    if (names.forall(name => fields.contains(name))) {
      Some(names)
    } else {
      None
    }
  }

  /** A constant expression as an int, or nothing when it is not a constant. */
  def foldableInt(expression: Expression): Option[Int] = {
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

  /**
   * The live rows of the given hits, keyed by data file and then by row position. Hits whose row was
   * removed, or whose data file the table no longer has, are absent.
   */
  def rowsAt(
      spark: SparkSession,
      sparkTable: SparkTable,
      snapshotId: Long,
      hits: Seq[IndexSearchResult],
      names: Seq[String],
      tableSchema: StructType): java.util.Map[String, java.util.Map[java.lang.Long, Row]] = {
    val positionsByFile = hits
      .groupBy(hit => hit.dataFile())
      .map { case (dataFile, fileHits) =>
        dataFile -> fileHits.map(hit => java.lang.Long.valueOf(hit.rowPosition())).toSet.asJava
      }
      .toMap
      .asJava

    IndexEntries.rowsAt(
      spark,
      sparkTable.table(),
      snapshotId,
      positionsByFile,
      names.toList.asJava,
      tableSchema)
  }

  /** Turns rows into the internal rows a local relation holds, in the order they are given. */
  def internalRows(spark: SparkSession, rows: Seq[Row]): Seq[InternalRow] = {
    if (rows.isEmpty) {
      Nil
    } else {
      spark
        .createDataFrame(rows.asJava, rows.head.schema)
        .queryExecution
        .toRdd
        .map(row => row.copy())
        .collect()
        .toSeq
    }
  }

  private val EmptyRow: InternalRow = new GenericInternalRow(new Array[Any](0))
}
