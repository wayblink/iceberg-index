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
package org.apache.iceberg.spark.extensions

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo
import org.apache.spark.sql.catalyst.analysis.CheckViews
import org.apache.spark.sql.catalyst.analysis.ProcedureArgumentCoercion
import org.apache.spark.sql.catalyst.analysis.ResolveProcedures
import org.apache.spark.sql.catalyst.analysis.ResolveViews
import org.apache.spark.sql.catalyst.analysis.RewriteMergeIntoTableForRowLineage
import org.apache.spark.sql.catalyst.analysis.RewriteUpdateTableForRowLineage
import org.apache.spark.sql.catalyst.optimizer.RemoveRowLineageOutputFromOriginalTable
import org.apache.spark.sql.catalyst.optimizer.ReplaceStaticInvoke
import org.apache.spark.sql.catalyst.parser.extensions.IcebergSparkSqlExtensionsParser
import org.apache.spark.sql.execution.datasources.v2.ExtendedDataSourceV2Strategy

class IcebergSparkSessionExtensions extends (SparkSessionExtensions => Unit) {

  private val VEC_DISTANCE = "vec_distance"

  override def apply(extensions: SparkSessionExtensions): Unit = {
    // parser extensions
    extensions.injectParser { case (_, parser) => new IcebergSparkSqlExtensionsParser(parser) }

    // index table-valued functions
    org.apache.iceberg.spark.index.IndexTableFunctions.register(extensions)

    // vector distance expression and the rule that rewrites a nearest-neighbour query into an
    // index lookup
    extensions.injectFunction {
      (
        new FunctionIdentifier(VEC_DISTANCE),
        new ExpressionInfo(VEC_DISTANCE, "vec_distance(vector, query): distance to a query vector"),
        (args: Seq[Expression]) =>
          org.apache.iceberg.spark.index.VectorDistanceExpression(
            org.apache.iceberg.index.vector.VectorIndexDefinition.Metric.COSINE,
            args(0),
            args(1)))
    }
    extensions.injectOptimizerRule { spark =>
      org.apache.iceberg.spark.index.RewriteVectorSearch(spark)
    }

    // analyzer extensions
    extensions.injectResolutionRule { spark => ResolveProcedures(spark) }
    extensions.injectResolutionRule { spark => ResolveViews(spark) }
    extensions.injectResolutionRule { _ => ProcedureArgumentCoercion }
    extensions.injectCheckRule(_ => CheckViews)
    extensions.injectResolutionRule { _ => RewriteUpdateTableForRowLineage }
    extensions.injectResolutionRule { _ => RewriteMergeIntoTableForRowLineage }

    // optimizer extensions
    extensions.injectOptimizerRule { _ => ReplaceStaticInvoke }
    extensions.injectOptimizerRule { _ => RemoveRowLineageOutputFromOriginalTable }

    // planner extensions
    extensions.injectPlannerStrategy { spark => ExtendedDataSourceV2Strategy(spark) }
  }
}
