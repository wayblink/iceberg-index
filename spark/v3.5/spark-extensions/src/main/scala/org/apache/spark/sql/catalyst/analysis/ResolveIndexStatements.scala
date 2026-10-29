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

package org.apache.spark.sql.catalyst.analysis

import org.apache.iceberg.Table
import org.apache.iceberg.index.IndexMetadata
import org.apache.iceberg.index.spark.VectorIndexes
import org.apache.iceberg.spark.Spark3Util
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.CallStatement
import org.apache.spark.sql.catalyst.plans.logical.DropIndexStatement
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.NamedArgument
import org.apache.spark.sql.catalyst.plans.logical.RefreshIndexStatement
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.LookupCatalog
import scala.jdk.CollectionConverters._

/**
 * Resolves `ALTER TABLE ... REFRESH INDEX name` and `DROP INDEX name` into the procedure calls that
 * already do the work.
 *
 * <p>The statements name an index while the procedures address a column, so the index is looked up
 * in the table here. Keeping the lookup in analysis means an unknown or ambiguous name fails while
 * the query is analyzed rather than part way through a job.
 */
case class ResolveIndexStatements(spark: SparkSession) extends Rule[LogicalPlan] with LookupCatalog {

  protected lazy val catalogManager: CatalogManager = spark.sessionState.catalogManager

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case RefreshIndexStatement(CatalogAndIdentifier(catalog, ident), name) =>
      val (icebergTable, metadata) = resolve(catalog.name(), ident, name)
      metadata.indexType match {
        case "vector" =>
          call(
            catalog.name(),
            "refresh_vector_index",
            tableName(catalog.name(), ident),
            icebergTable,
            metadata,
            name)
        case other =>
          throw new AnalysisException(s"Index $name cannot be refreshed: unsupported type $other")
      }

    case DropIndexStatement(CatalogAndIdentifier(catalog, ident), name) =>
      val (icebergTable, metadata) = resolve(catalog.name(), ident, name)
      metadata.indexType match {
        case "vector" =>
          call(
            catalog.name(),
            "drop_vector_index",
            tableName(catalog.name(), ident),
            icebergTable,
            metadata,
            name)
        case other =>
          throw new AnalysisException(
            s"Index $name cannot be dropped: unsupported type $other")
      }
  }

  /** The table and the one index it holds under that name or UUID, failing on none or several. */
  private def resolve(catalogName: String, ident: Identifier, name: String): (Table, IndexMetadata) = {
    val icebergTable = Spark3Util.loadIcebergTable(spark, tableName(catalogName, ident))
    val matches = VectorIndexes
      .indexesOf(icebergTable)
      .asScala
      .filter(metadata => name == metadata.name() || name == metadata.indexUuid())
      .toSeq

    if (matches.isEmpty) {
      val unnamed = VectorIndexes
        .indexesOf(icebergTable)
        .asScala
        .filter(metadata => metadata.name() == null)
        .map(metadata => columnName(icebergTable, metadata))
        .toSeq

      val hint =
        if (unnamed.isEmpty) {
          ""
        } else {
          // a text index has no name yet, so a name is not what addresses it
          s"; the table has an unnamed index on ${unnamed.mkString(", ")}: " +
            "address it by column with the matching procedure"
        }

      throw new AnalysisException(
        s"Index $name does not exist in table ${tableName(catalogName, ident)}$hint")
    }

    if (matches.size > 1) {
      throw new AnalysisException(
        s"Index name $name is ambiguous in table ${tableName(catalogName, ident)}: " +
          s"${matches.size} indexes carry it")
    }

    (icebergTable, matches.head)
  }

  private def columnName(icebergTable: Table, metadata: IndexMetadata): String = {
    val name = icebergTable.schema().findColumnName(metadata.definition().fieldId())
    if (name == null) {
      throw new AnalysisException(
        s"Index ${metadata.indexUuid()} refers to field ${metadata.definition().fieldId()}, " +
          "which the table no longer has")
    }

    name
  }

  private def tableName(catalogName: String, ident: Identifier): String = {
    (Seq(catalogName) ++ ident.namespace().toSeq :+ ident.name()).mkString(".")
  }

  private def call(
      catalogName: String,
      procedure: String,
      table: String,
      icebergTable: Table,
      metadata: IndexMetadata,
      index: String): CallStatement = {
    CallStatement(
      Seq(catalogName, "system", procedure),
      Seq(
        NamedArgument("table", Literal(table)),
        NamedArgument("column", Literal(columnName(icebergTable, metadata))),
        NamedArgument("index", Literal(index))))
  }
}
