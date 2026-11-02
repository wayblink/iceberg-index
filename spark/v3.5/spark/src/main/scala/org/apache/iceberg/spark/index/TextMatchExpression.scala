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

import org.apache.iceberg.index.text.TextIndexAdapter.CorpusStatistics
import org.apache.iceberg.index.text.TextIndexDefinition
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Predicate
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback

/**
 * Whether a text column matches a query, which for BM25 means it shares at least one term with it,
 * so a positive score.
 *
 * <p>This is the filter form of {@link TextScoreExpression}: a query that wants the matching rows
 * rather than the best ones writes {@code WHERE match(column, 'query')}, which can be combined with
 * other predicates, while {@code ORDER BY text_score(...) LIMIT k} cannot. It needs the same index
 * statistics, for the same reason.
 */
case class TextMatchExpression(
    definition: TextIndexDefinition,
    statistics: CorpusStatistics,
    score: TextScoreExpression)
    extends Predicate
    with CodegenFallback {

  override def nullable: Boolean = true

  override def children: Seq[org.apache.spark.sql.catalyst.expressions.Expression] = Seq(score)

  override def eval(input: InternalRow): Any = {
    score.eval(input) match {
      // a null document or query scores nothing, and SQL does not answer a comparison against an
      // unknown with false: the row is filtered out either way, but the value it projects is unknown
      case null => null
      case value: Double => value > 0.0
      case _ => null
    }
  }

  override def toString: String = s"match(${score.text}, ${score.query})"

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[org.apache.spark.sql.catalyst.expressions.Expression])
      : TextMatchExpression = {
    copy(definition, statistics, newChildren(0).asInstanceOf[TextScoreExpression])
  }
}
