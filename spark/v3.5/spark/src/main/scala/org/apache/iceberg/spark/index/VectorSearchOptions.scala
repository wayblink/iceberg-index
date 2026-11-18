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

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.UnaryNode

/**
 * The settings a table function call named, carried into the plan it built.
 *
 * <p>A table function's arguments are gone by the time the optimizer runs, and the vector rewrite is
 * an optimizer rule: it cannot see what the call said unless the call leaves it in the plan. This
 * node is that message. The rewrite consumes it while lowering the query, so it never survives into
 * the optimized plan; a plan the index cannot answer drops it and keeps its scan, because a setting
 * describes how to search and there is nothing to search.
 *
 * <p>Each field is the argument as the call wrote it, or null when the call named none. They are
 * parsed where every other layer of the same setting is parsed, so a value the call wrote and a
 * value the session set go through one decision.
 */
private[spark] case class VectorSearchOptions(arguments: VectorSearchArguments, child: LogicalPlan)
    extends UnaryNode {

  override def output: Seq[Attribute] = child.output

  override protected def withNewChildInternal(newChild: LogicalPlan): VectorSearchOptions =
    copy(child = newChild)
}

/** The query settings a vector search call may name, each null when the call named none. */
private[spark] case class VectorSearchArguments(
    nprobeArgument: String,
    overSampleArgument: String,
    filterModeArgument: String,
    indexArgument: String)

private[spark] object VectorSearchArguments {

  /** A query that names none of the settings, which is a query the rewrite planned. */
  val none: VectorSearchArguments = VectorSearchArguments(null, null, null, null)
}
