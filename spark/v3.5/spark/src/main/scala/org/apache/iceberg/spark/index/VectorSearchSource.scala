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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LeafNode

/**
 * The lowered vector source: the live rows of a frozen table snapshot that a vector search can
 * return, produced by an index candidate branch plus, for a complete-table query, a scan of the
 * files the index does not cover.
 *
 * <p>The node carries everything the execution needs and nothing it has to look up again: the
 * frozen table and index snapshots, the parameters with their origins, the candidate budget, and
 * the output attributes of the scan it replaces. Planning it reads no data file and starts no job.
 */
private[spark] case class VectorSearchSource(context: VectorSearchContext, key: Long = 0L)(
    override val output: Seq[Attribute])
    extends LeafNode {

  override protected def otherCopyArgs: Seq[AnyRef] = output :: Nil

  override protected def stringArgs: Iterator[Any] = {
    val resolved = if (context.budget == null) "" else s", budget=${context.budget.initial}"
    Iterator(
      s"contract=${context.request.input.contract.name}",
      s"column=${context.request.input.column}",
      s"k=${context.request.input.topK}",
      s"index=${context.inputs.indexMetadata.indexUuid()}",
      s"tableSnapshot=${context.inputs.tableSnapshot.snapshotId()}",
      s"indexSnapshot=${context.inputs.indexSnapshot.snapshotId}",
      s"uncoveredFiles=${context.inputs.uncoveredDataFiles.size}",
      s"driverMaterialization=true$resolved")
  }

  override def simpleString(maxFields: Int): String =
    s"VectorSearchSource[${stringArgs.mkString(", ")}]"

  def withContext(newContext: VectorSearchContext): VectorSearchSource =
    VectorSearchSource(newContext, key)(output)
}

private[spark] object VectorSearchSource {
  def apply(context: VectorSearchContext, output: Seq[Attribute]): VectorSearchSource =
    new VectorSearchSource(context)(output)

  def internalRow(value: Any): InternalRow = value.asInstanceOf[InternalRow]
}
