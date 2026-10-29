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

import org.apache.iceberg.index.text.TextIndexTypes
import org.apache.iceberg.index.vector.VectorIndexTypes
import org.apache.iceberg.spark.Spark3Util
import org.apache.iceberg.index.spark.IndexSearch
import org.apache.iceberg.index.spark.VectorIndexSearch
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.DoubleType
import scala.collection.JavaConverters._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * Registers the index table-valued functions: functions that take a table, a column and a query and
 * return the matched rows as a table, so they can be used in a FROM clause and composed with the
 * rest of a query.
 *
 * <p>Each function is a thin wrapper over the same search the corresponding CALL procedure runs, so
 * the two always agree. The rows are computed on the driver when the query is planned, which is why
 * the result is a local relation: index files are small and there is nothing to distribute.
 */
object IndexTableFunctions {

  private val VECTOR_INDEX_SEARCH = "vector_index_search"
  private val VECTOR_INDEX_SEARCH_BATCH = "vector_index_search_batch"
  private val TEXT_INDEX_SEARCH = "text_index_search"

  def register(extensions: SparkSessionExtensions): Unit = {
    extensions.injectTableFunction(
      (
        new FunctionIdentifier(VECTOR_INDEX_SEARCH),
        new ExpressionInfo(
          VECTOR_INDEX_SEARCH,
          "vector_index_search(table, column, query, k): searches a vector index"),
        vectorIndexSearch))
    extensions.injectTableFunction(
      (
        new FunctionIdentifier(VECTOR_INDEX_SEARCH_BATCH),
        new ExpressionInfo(
          VECTOR_INDEX_SEARCH_BATCH,
          "vector_index_search_batch(table, column, queries, k): searches a vector index for many "
            + "queries at once"),
        vectorIndexSearchBatch))
    extensions.injectTableFunction(
      (
        new FunctionIdentifier(TEXT_INDEX_SEARCH),
        new ExpressionInfo(
          TEXT_INDEX_SEARCH,
          "text_index_search(table, column, query, k): searches a BM25 text index"),
        textIndexSearch))
  }

  private def vectorIndexSearch(args: Seq[Expression]): LogicalPlan = {
    checkArity(args, 4, VECTOR_INDEX_SEARCH, "(table, column, query, k)")
    val spark = SparkSession.active
    VectorIndexTypes.register()
    val tableName = stringArg(args, 0)
    val column = stringArg(args, 1)
    val query = doubleArrayArg(args, 2)
    val topK = intArg(args, 3)

    // the function builds the request and stops there: a search runs when the plan is executed,
    // not while the plan is being built, and not while it is optimized
    val queries = new java.util.ArrayList[Array[Float]]()
    queries.add(query.map(_.toFloat))
    VectorSearchCalls.planOf(tableName, column, null, queries, topK, batch = false)
  }

  private def vectorIndexSearchBatch(args: Seq[Expression]): LogicalPlan = {
    checkArity(args, 4, VECTOR_INDEX_SEARCH_BATCH, "(table, column, queries, k)")
    val spark = SparkSession.active
    VectorIndexTypes.register()
    val tableName = stringArg(args, 0)
    val column = stringArg(args, 1)
    val queries = stringArrayArg(args, 2)
    val topK = intArg(args, 3)

    val vectors = new java.util.ArrayList[Array[Float]]()
    queries.foreach(query => vectors.add(IndexTableFunctions.parseVector(query)))
    // as with the single-query form, the function returns a request and never searches
    VectorSearchCalls.planOf(tableName, column, null, vectors, topK, batch = true)
  }

  private def textIndexSearch(args: Seq[Expression]): LogicalPlan = {
    checkArity(args, 4, TEXT_INDEX_SEARCH, "(table, column, query, k)")
    val spark = SparkSession.active
    TextIndexTypes.register()
    val tableName = stringArg(args, 0)
    val column = stringArg(args, 1)
    val query = stringArg(args, 2)
    val topK = intArg(args, 3)

    val payload =
      new org.apache.iceberg.index.text.TextIndexAdapter(indexDefinition(spark, tableName, column))
        .encodeQuery(query)
    IndexSearch
      .search(spark, Spark3Util.loadIcebergTable(spark, tableName), column, "text", payload, topK)
      .queryExecution
      .analyzed
  }

  private def indexDefinition(
      spark: SparkSession,
      tableName: String,
      column: String): org.apache.iceberg.index.text.TextIndexDefinition = {
    val table = Spark3Util.loadIcebergTable(spark, tableName)
    org.apache.iceberg.index.spark.VectorIndexes
      .findIndex(table, column, "text")
      .definition()
      .asInstanceOf[org.apache.iceberg.index.text.TextIndexDefinition]
  }

  /** Parses a comma-separated list of numbers, the form the batch functions take. */
  def parseVector(text: String): Array[Float] = {
    text.split(",").filter(_.nonEmpty).map(_.trim.toFloat)
  }

  private def checkArity(
      args: Seq[Expression],
      expected: Int,
      name: String,
      signature: String): Unit = {
    require(
      args.length == expected,
      s"$name expects $expected arguments $signature, got ${args.length}")
  }

  private def stringArg(args: Seq[Expression], ordinal: Int): String = {
    fold(args, ordinal).toString
  }

  private def stringArrayArg(args: Seq[Expression], ordinal: Int): Seq[String] = {
    val array = fold(args, ordinal).asInstanceOf[ArrayData]
    (0 until array.numElements()).map(index => array.getUTF8String(index).toString)
  }

  private def doubleArrayArg(args: Seq[Expression], ordinal: Int): Array[Double] = {
    fold(args, ordinal).asInstanceOf[ArrayData].toDoubleArray
  }

  private def intArg(args: Seq[Expression], ordinal: Int): Int = {
    fold(args, ordinal).toString.toInt
  }

  /**
   * Folds an argument to a constant. Table-valued functions are resolved before the query has rows
   * to work with, so their arguments must be constants; anything else cannot be planned.
   */
  private def fold(args: Seq[Expression], ordinal: Int): Any = {
    val argument = args(ordinal)
    if (!argument.foldable) {
      throw new IllegalArgumentException(
        s"Argument ${ordinal + 1} of an index table function must be a constant expression")
    }
    argument.eval()
  }

}
