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

import org.apache.iceberg.index.vector.VectorDistance
import org.apache.iceberg.index.vector.VectorIndexDefinition.Metric
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.DoubleType

/**
 * The distance between a vector column and a query vector, for which smaller always means closer.
 *
 * <p>The metric is not part of the function signature: the expression defaults to cosine, and the
 * rewrite rule specializes it to the metric of the index it finds, so a query follows the index it
 * uses. The math itself lives in {@link VectorDistance}, which is also what the index adapter scores
 * with, so the two can never drift apart.
 */
case class VectorDistanceExpression(metric: Metric, vector: Expression, query: Expression)
    extends Expression
    with CodegenFallback {

  override def nullable: Boolean = true

  override def dataType: org.apache.spark.sql.types.DataType = DoubleType

  override def children: Seq[Expression] = Seq(vector, query)

  override def eval(input: InternalRow): Any = {
    val stored = vector.eval(input)
    val asked = query.eval(input)
    if (stored == null || asked == null) {
      null
    } else {
      VectorDistance.distance(
        metric,
        toFloats(stored.asInstanceOf[ArrayData]),
        toFloats(asked.asInstanceOf[ArrayData]))
    }
  }

  override def toString: String = s"vec_distance($metric, $vector, $query)"

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression = {
    copy(metric, newChildren(0), newChildren(1))
  }

  private def toFloats(values: ArrayData): Array[Float] = {
    val doubles = values.toDoubleArray
    Array.tabulate(doubles.length)(index => doubles(index).toFloat)
  }
}
