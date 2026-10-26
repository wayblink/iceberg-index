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

import org.apache.iceberg.{FileScanTask, Snapshot, Table}
import org.apache.iceberg.index.{IndexFile, IndexMetadata, IndexSnapshot}
import org.apache.iceberg.index.spark.IndexSearch
import org.apache.iceberg.index.vector.VectorIndexDefinition
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.types.StructType

private[spark] sealed trait VectorResultContract {
  def name: String
}

private[spark] object VectorResultContract {
  case object TableLiveTopK extends VectorResultContract {
    override val name: String = "TABLE_LIVE_TOPK"
  }
  case object IndexHits extends VectorResultContract {
    override val name: String = "INDEX_HITS"
  }

  def fromName(name: String): VectorResultContract = name match {
    case "TABLE_LIVE_TOPK" => TableLiveTopK
    case "INDEX_HITS" => IndexHits
    case _ => throw new IllegalArgumentException(s"Unknown vector result contract: $name")
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
