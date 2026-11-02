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
import org.apache.iceberg.index.vector.VectorIndexDefinition.Metric
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo
import org.apache.spark.sql.catalyst.analysis.CheckViews
import org.apache.spark.sql.catalyst.analysis.ProcedureArgumentCoercion
import org.apache.spark.sql.catalyst.analysis.ResolveIndexStatements
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
  private val TEXT_SCORE = "text_score"
  private val TEXT_MATCH = "match"

  /**
   * The metric the query named, or cosine when it named none.
   *
   * <p>A named metric is taken literally: it is never replaced by whatever an index happens to use,
   * because that would answer a question the caller did not ask.
   */
  private def metricOf(args: Seq[Expression]): Metric = {
    if (args.size < 2) {
      throw new AnalysisException(
        "vec_distance needs a vector and a query vector",
        Map.empty[String, String])
    }

    if (args.size > 3) {
      throw new AnalysisException(
        s"vec_distance takes a vector, a query vector and an optional metric, got ${args.size} " +
          "arguments",
        Map.empty[String, String])
    }

    if (args.size == 2) {
      Metric.COSINE
    } else {
      val name = args(2) match {
        case org.apache.spark.sql.catalyst.expressions
              .Literal(value: org.apache.spark.unsafe.types.UTF8String, _) =>
          value.toString
        case _ =>
          throw new AnalysisException(
            "The metric of vec_distance must be a constant: cosine, dot_product or euclidean",
            Map.empty[String, String])
      }

      name.trim.toLowerCase(java.util.Locale.ROOT) match {
        case "cosine" | "cos" => Metric.COSINE
        case "dot" | "dot_product" | "inner_product" | "ip" => Metric.DOT_PRODUCT
        case "euclidean" | "l2" => Metric.EUCLIDEAN
        case other =>
          throw new AnalysisException(
            s"Unknown metric for vec_distance: $other, expected cosine, dot_product or euclidean",
            Map.empty[String, String])
      }
    }
  }

  /**
   * The column and the query of a text function, refusing a call with anything else.
   *
   * <p>A third argument would otherwise be dropped in silence, and a caller who wrote
   * `match(body, 'error', 'operator=AND')` would believe the options were applied. Until the text
   * index can answer options, an extra argument is an error rather than a no-op.
   */
  private def textArguments(name: String, args: Seq[Expression]): (Expression, Expression) = {
    if (args.size != 2) {
      throw new AnalysisException(
        s"$name takes a column and a query, got ${args.size} arguments: options are not supported " +
          "by the text index yet",
        Map.empty[String, String])
    }

    (args(0), args(1))
  }

  override def apply(extensions: SparkSessionExtensions): Unit = {
    // the index types the engine ships with. IndexTypes is a static registry that fails fast on an
    // unknown type, so a session that has not registered them can neither build nor read an index,
    // and the rewrite rules stand aside rather than failing the query. Registering here is what
    // makes a deployment work without the process having to know about the index layer.
    org.apache.iceberg.index.vector.VectorIndexTypes.register()
    org.apache.iceberg.index.text.TextIndexTypes.register()

    // parser extensions
    extensions.injectParser { case (_, parser) => new IcebergSparkSqlExtensionsParser(parser) }

    // index table-valued functions
    org.apache.iceberg.spark.index.IndexTableFunctions.register(extensions)

    // vector distance expression and the rule that rewrites a nearest-neighbour query into an
    // index lookup
    extensions.injectFunction {
      (
        new FunctionIdentifier(VEC_DISTANCE),
        new ExpressionInfo(
          VEC_DISTANCE,
          "vec_distance(vector, query[, metric]): distance to a query vector"),
        (args: Seq[Expression]) =>
          org.apache.iceberg.spark.index
            .VectorDistanceExpression(metricOf(args), args.head, args(1), args.size > 2))
    }
    extensions.injectOptimizerRule { spark =>
      org.apache.iceberg.spark.index.RewriteVectorSearch(spark)
    }
    // the physical side of that rewrite: the source it lowers to is planned here, and the search
    // happens when that operator runs
    extensions.injectPlannerStrategy { spark =>
      org.apache.iceberg.spark.index.VectorSearchStrategy(spark)
    }

    // text score and match, which a rule binds to the statistics of the index it finds, for the
    // same reason a vector distance is bound to the metric of its index
    extensions.injectFunction {
      (
        new FunctionIdentifier(TEXT_SCORE),
        new ExpressionInfo(TEXT_SCORE, "text_score(text, query): BM25 score against a query"),
        (args: Seq[Expression]) => {
          // the rewrite rule binds the definition and the statistics to the index it finds
          val (text, query) = textArguments(TEXT_SCORE, args)
          org.apache.iceberg.spark.index.TextScoreExpression(null, null, text, query)
        })
    }
    extensions.injectFunction {
      (
        new FunctionIdentifier(TEXT_MATCH),
        new ExpressionInfo(TEXT_MATCH, "match(text, query): whether a text matches a query"),
        (args: Seq[Expression]) => {
          val (text, query) = textArguments(TEXT_MATCH, args)
          org.apache.iceberg.spark.index.TextMatchExpression(
            null,
            null,
            org.apache.iceberg.spark.index.TextScoreExpression(null, null, text, query))
        })
    }
    extensions.injectOptimizerRule { spark =>
      org.apache.iceberg.spark.index.RewriteTextSearch(spark)
    }

    // analyzer extensions
    extensions.injectResolutionRule { spark => ResolveIndexStatements(spark) }
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
