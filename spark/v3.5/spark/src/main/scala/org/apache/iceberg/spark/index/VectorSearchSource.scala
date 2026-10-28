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

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.catalyst.plans.logical.Statistics

/**
 * The lowered vector source: the live rows of a frozen table snapshot that a vector search can
 * return, produced by an index candidate branch plus, for a complete-table query, a scan of the
 * files the index does not cover.
 *
 * <p>The node carries everything the execution needs and nothing it has to look up again: the
 * frozen table and index snapshots, the parameters with their origins, the candidate budget, and
 * the output attributes of the scan it replaces. Planning it reads no data file and starts no job.
 */
private[spark] case class VectorSearchSource(context: VectorSearchContext)(
    override val output: Seq[Attribute])
    extends LeafNode {

  override protected def otherCopyArgs: Seq[AnyRef] = output :: Nil

  override protected def stringArgs: Iterator[Any] = {
    val parameters = context.parameters
    val overSample =
      if (parameters == null) {
        "overSample=unset"
      } else {
        val details = new StringBuilder(parameters.overSampleOrigin)
        if (parameters.overSampleLocked) {
          details.append(", locked")
        }
        if (parameters.overSampleReason != null) {
          details.append(", ").append(parameters.overSampleReason)
        }
        if (parameters.overSampleShadowedOrigin != null) {
          details.append(", overrides ").append(parameters.overSampleShadowedOrigin)
        }
        s"overSample=${parameters.overSample} [$details]"
      }
    val budget =
      if (context.budget == null) {
        "requestedCandidates=unset, initialCandidates=unset"
      } else {
        s"requestedCandidates=${context.budget.requested}, " +
          s"initialCandidates=${context.budget.initial}"
      }
    Iterator(
      s"contract=${context.request.input.contract.name}",
      s"column=${context.request.input.column}",
      s"k=${context.request.input.topK}",
      s"metric=${context.inputs.definition.metric()}",
      overSample,
      budget,
      s"index=${context.inputs.indexMetadata.indexUuid()}",
      s"tableSnapshot=${context.inputs.tableSnapshot.snapshotId()}",
      s"indexSnapshot=${context.inputs.indexSnapshot.snapshotId}",
      s"uncoveredFiles=${context.inputs.uncoveredDataFiles.size}",
      "driverMaterialization=true")
  }

  override def simpleString(maxFields: Int): String =
    s"VectorSearchSource[${stringArgs.mkString(", ")}]"

  /**
   * Estimates the source from its frozen data files, before the sort and limit above it.
   *
   * <p>The source returns candidates and uncovered rows together, so its row count is not bounded
   * by the query's k. File record counts give a conservative bound without reading rows or applying
   * deletes; multiple scan tasks for the same file count that file once.
   */
  override def computeStats: Statistics = {
    val seen = mutable.HashSet.empty[String]
    val rows = context.inputs.scanTasks.asScala.foldLeft(BigInt(0)) { (total, task) =>
      val file = task.file()
      if (seen.add(file.location().toString)) total + file.recordCount() else total
    }
    val bytesPerRow = output.foldLeft(BigInt(8)) { (bytes, attribute) =>
      bytes + attribute.dataType.defaultSize
    }
    Statistics(sizeInBytes = bytesPerRow * rows, rowCount = Some(rows))
  }
}
