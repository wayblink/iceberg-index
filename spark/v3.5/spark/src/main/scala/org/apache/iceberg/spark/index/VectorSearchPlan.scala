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
import org.apache.iceberg.FileScanTask
import org.apache.iceberg.Snapshot
import org.apache.iceberg.Table
import org.apache.iceberg.index.IndexFile
import org.apache.iceberg.index.IndexMetadata
import org.apache.iceberg.index.IndexSnapshot
import org.apache.iceberg.index.spark.IndexSearch
import org.apache.iceberg.index.vector.VectorIndexDefinition
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.types.StructType
import scala.collection.JavaConverters._

/**
 * What a vector request promises about the rows it returns.
 *
 * <p>This is the caller's choice, not a decision the code makes for them: an index can be behind the
 * table, and the two honest answers to that are "search only what the index covers" and "search the
 * whole table, scanning what the index does not cover". Neither is a fallback for the other, and
 * nothing here degrades into a scan on its own.
 */
private[spark] sealed trait VectorResultContract {
  def name: String
}

private[spark] object VectorResultContract {
  case object TableLiveTopK extends VectorResultContract {
    override val name: String = "TABLE_LIVE_TOPK"
  }

  /**
   * The nearest live rows of the whole table, answered as hits.
   *
   * <p>The same answer {@link TableLiveTopK} gives, in the shape the procedures return: the row
   * reference, the exact distance and the row. It is a separate contract because the shape is part of
   * what a contract promises, and a caller of a procedure asks for hits while the rewrite asks for
   * rows.
   */
  case object TableCovered extends VectorResultContract {
    override val name: String = "TABLE_COVERED"
  }

  /**
   * The live rows of the files the index covers, and nothing else.
   *
   * <p>A hit is dropped when its data file is not in the queried snapshot or its position has no live
   * row, and the search widens until it has k live rows or the kernel runs out. The files the index
   * does not cover are not read, so the answer can be shorter than k: it is exact about the rows it
   * could have found.
   */
  case object IndexCovered extends VectorResultContract {
    override val name: String = "INDEX_COVERED"
  }

  /**
   * The index's own hits, as the index produced them.
   *
   * <p>A hit whose row was removed is kept with a null row, the score is the index's estimate, and
   * the files the index does not cover are not read. This is a diagnostic view of the index, not a
   * contract a query result can carry.
   */
  case object IndexHits extends VectorResultContract {
    override val name: String = "INDEX_HITS"
  }

  /** The coverage modes a caller may name, in the vocabulary the procedures accept. */
  val CoverageModes: Set[String] = Set("table", "indexed", "index_hits")

  def fromName(name: String): VectorResultContract = name match {
    case "TABLE_LIVE_TOPK" => TableLiveTopK
    case "TABLE_COVERED" => TableCovered
    case "INDEX_COVERED" => IndexCovered
    case "INDEX_HITS" => IndexHits
    case _ => throw new IllegalArgumentException(s"Unknown vector result contract: $name")
  }

  /**
   * The contract a {@code coverage} argument names.
   *
   * <p>{@code table} is the default because a query that silently answers from a subset of the table
   * is the failure mode this whole distinction exists to prevent.
   */
  def forCoverage(coverage: String): VectorResultContract = coverage match {
    case "table" => TableCovered
    case "indexed" => IndexCovered
    case "index_hits" => IndexHits
    case other =>
      throw new IllegalArgumentException(
        s"Invalid coverage: '$other'; expected one of table, indexed, index_hits")
  }
}

private[spark] case class VectorRequestInput(
    tableName: String,
    table: Table,
    tableSnapshotId: JLong,
    column: String,
    queries: Vector[Vector[Float]],
    topK: Int,
    indexArgument: String,
    contract: VectorResultContract,
    batch: Boolean)

private[spark] case class VectorSearchRequest(
    input: VectorRequestInput,
    override val output: Seq[Attribute])
    extends LeafNode {
  override protected def stringArgs: Iterator[Any] =
    Iterator(input.tableName, input.column, input.contract.name, s"k=${input.topK}")
}

private[spark] object VectorSearchRequest {
  def forTable(
      table: Table,
      column: String,
      queries: JList[Array[Float]],
      k: Int,
      tableSnapshotId: JLong,
      contractName: String,
      indexArgument: String,
      batch: Boolean): VectorSearchRequest = {
    require(table != null, "Vector source table cannot be null")
    val contract = VectorResultContract.fromName(contractName)
    // the rewritten source answers with table rows; the coverage modes and the hit view answer with
    // hits, which carry the file, the position and the score of what they found
    val schema = if (contract == VectorResultContract.TableLiveTopK) {
      SparkSchemaUtil.convert(table.schema())
    } else if (batch) {
      IndexSearch.RESULT_SCHEMA
    } else {
      IndexSearch.SINGLE_RESULT_SCHEMA
    }
    val input = VectorRequestInput(
      table.name(),
      table,
      tableSnapshotId,
      column,
      queries.asScala.map(_.toVector).toVector,
      k,
      indexArgument,
      contract,
      batch)
    val output = schema.fields.map { field =>
      AttributeReference(field.name, field.dataType, field.nullable, field.metadata)()
    }.toSeq
    VectorSearchRequest(input, output)
  }
}

private[spark] case class ResolvedExecutionInputs(
    table: Table,
    metadataLocation: String,
    tableSnapshot: Snapshot,
    indexSnapshot: IndexSnapshot,
    indexMetadata: IndexMetadata,
    manifestListLocation: String,
    indexFiles: Vector[IndexFile],
    scanTasks: JList[FileScanTask],
    dataFiles: Set[String],
    readSchema: StructType,
    uncoveredDataFiles: Set[String],
    entryUpperBound: JLong,
    definition: VectorIndexDefinition)

private[spark] case class VectorSearchContext(
    request: VectorSearchRequest,
    inputs: ResolvedExecutionInputs,
    rawParameters: RawVectorParameters,
    parameters: ResolvedVectorParameters = null,
    budget: VectorCandidateBudget = null) {
  def queryVectors: JList[Array[Float]] = request.input.queries.map(_.toArray).asJava
}

private[spark] sealed trait VectorBindingResult {
  def status: String
  def context: VectorSearchContext
}

private[spark] case class BoundVectorSearch(context: VectorSearchContext)
    extends VectorBindingResult {
  override val status: String = "BOUND"
}

private[spark] case object NoApplicableVectorIndex extends VectorBindingResult {
  override val status: String = "NO_APPLICABLE_INDEX"
  override val context: VectorSearchContext = null
}

private[spark] case object AmbiguousVectorIndex extends VectorBindingResult {
  override val status: String = "AMBIGUOUS_INDEX"
  override val context: VectorSearchContext = null
}
