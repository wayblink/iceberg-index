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

import org.apache.iceberg.index.text.TextIndexAdapter
import org.apache.iceberg.index.text.TextIndexAdapter.CorpusStatistics
import org.apache.iceberg.index.text.TextIndexDefinition
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DoubleType

/**
 * The BM25 score of a text column against a query, for which higher always means more relevant.
 *
 * <p>A BM25 score is not a function of the document and the query alone: it also needs the document
 * frequency of the query terms and the average document length of the corpus, which live in the
 * index. So unlike a vector distance, this expression cannot score anything until the rewrite rule
 * binds it to the statistics of the index snapshot it reads. That is also why a text query is only
 * answered from an index snapshot bound to exactly the snapshot being read: statistics from an older
 * snapshot would give scores a full scan cannot reproduce.
 *
 * <p>The math itself lives in {@link TextIndexAdapter}, the same implementation the index scores
 * with, so a score computed here is the score the index would have given.
 */
case class TextScoreExpression(
    definition: TextIndexDefinition,
    statistics: CorpusStatistics,
    text: Expression,
    query: Expression)
    extends Expression
    with CodegenFallback {

  override def nullable: Boolean = true

  override def dataType: DataType = DoubleType

  override def children: Seq[Expression] = Seq(text, query)

  override def eval(input: InternalRow): Any = {
    val document = text.eval(input)
    val asked = query.eval(input)
    if (document == null || asked == null) {
      null
    } else if (definition == null || statistics == null) {
      // the rewrite rule binds the expression to the statistics of the index it finds, so reaching
      // this point means there is no index on the column to score against
      throw new IllegalArgumentException(
        "text_score needs a text index on the column; none was found, build one with build_text_index")
    } else {
      TextIndexAdapter.score(definition, statistics, document.toString, asked.toString)
    }
  }

  override def toString: String = s"text_score($text, $query)"

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression = {
    copy(definition, statistics, newChildren(0), newChildren(1))
  }
}
