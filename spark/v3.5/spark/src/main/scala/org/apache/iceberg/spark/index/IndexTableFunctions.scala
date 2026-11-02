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
import org.apache.iceberg.index.spark.VectorIndexes
import org.apache.iceberg.index.vector.VectorIndexDefinition.Metric
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.Ascending
import org.apache.spark.sql.catalyst.expressions.Descending
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.NamedArgumentExpression
import org.apache.spark.sql.catalyst.expressions.NullsFirst
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.logical.GlobalLimit
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.plans.logical.Sort
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.iceberg.relocated.com.google.common.base.Preconditions
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import java.util.Locale

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

  private val VECTOR_SEARCH = "vector_search"
  private val TEXT_SEARCH = "search"
  private val VECTOR_COLUMN = "vector_column"
  private val DISTANCE_COLUMN = "_distance"
  private val SCORE_COLUMN = "_score"
  private val VECTOR_INDEX_SEARCH = "vector_index_search"
  private val VECTOR_INDEX_SEARCH_BATCH = "vector_index_search_batch"
  private val TEXT_INDEX_SEARCH = "text_index_search"

  def register(extensions: SparkSessionExtensions): Unit = {
    extensions.injectTableFunction(
      (
        new FunctionIdentifier(TEXT_SEARCH),
        new ExpressionInfo(
          TEXT_SEARCH,
          "search(table, query, search_columns, limit): rows ranked by a text index"),
        textSearch))
    extensions.injectTableFunction(
      (
        new FunctionIdentifier(VECTOR_SEARCH),
        new ExpressionInfo(
          VECTOR_SEARCH,
          "vector_search(table, query_vector, k, vector_column, distance_type, columns): the "
            + "nearest rows of a table"),
        vectorSearch))
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

  /**
   * The nearest rows of a table, as the table's own columns plus the distance that chose them.
   *
   * <p>The relation is built as an ordinary projection over a sort over a limit, so the search is
   * answered by the same rewrite, snapshot binding and live-row handling that an `ORDER BY` query
   * uses. What the caller gets is a business row, not an index address: a hit that no longer
   * resolves is not a row, and a table whose files the index does not cover is still scanned for
   * them.
   *
   * <p>The metric is always named to the distance, so a query never changes meaning because of the
   * index that happens to answer it; when no index matches the metric, the scan answers instead.
   */
  private def analysisError(message: String): Nothing =
    throw new IllegalArgumentException(message)

  /**
   * The rows a text index ranks for a query, as the table's own columns plus the score.
   *
   * <p>Like the vector function, this is an ordinary projection over a sort over a limit, so the
   * search is answered by the rewrite that binds the score to the index statistics. Naming one
   * column is required: a text index belongs to one column, and asking for several would need a
   * fusion rule that does not exist yet.
   */
  private def textSearch(args: Seq[Expression]): LogicalPlan = {
    val spark = SparkSession.active
    TextIndexTypes.register()

    val arguments =
      new TableFunctionArguments(TEXT_SEARCH, args, Seq("table", "query", "search_columns", "k"))
    arguments.requireOnly(
      required = Seq("table", "search_columns"),
      optional = Seq("query", "query_string", "limit", "k", "num_results", "columns"))

    val tableName = arguments.string("table")
    val query = arguments
      .stringOption("query")
      .orElse(arguments.stringOption("query_string"))
      .getOrElse(analysisError("search needs query"))
    val searched = arguments.strings("search_columns")
    val k = arguments
      .int("limit")
      .orElse(arguments.int("k"))
      .orElse(arguments.int("num_results"))
      .getOrElse(10)
    val columns = arguments.strings("columns")

    Preconditions.checkArgument(
      searched.size == 1,
      "search needs exactly one column in search_columns, got %s",
      searched.size)
    Preconditions.checkArgument(k > 0, "search needs a positive number of rows, got %s", k)

    val catalogAndIdent = Spark3Util.catalogAndIdentifier(spark, tableName)
    val catalog = catalogAndIdent.catalog()
    val ident = catalogAndIdent.identifier()
    val table = catalog.asInstanceOf[TableCatalog].loadTable(ident)
    val relation = DataSourceV2Relation.create(
      table,
      Some(catalog),
      Some(ident),
      CaseInsensitiveStringMap.empty())

    def columnOf(name: String): Attribute = {
      relation.output
        .find(attribute => attribute.name.equalsIgnoreCase(name))
        .getOrElse(analysisError(s"Column $name does not exist in $tableName; the table has " +
          relation.output.map(_.name).mkString(", ")))
    }

    val vector = columnOf(searched.head)
    val selected = if (columns.isEmpty) relation.output else columns.map(columnOf)
    if (relation.output.exists(attribute => attribute.name.equalsIgnoreCase(SCORE_COLUMN))) {
      analysisError(s"$tableName already has a column named $SCORE_COLUMN")
    }

    // the score is the whole search here: the rule finds the query and the searched column in the
    // expression, binds its statistics from the index of that column, and the projection above then
    // reads the score it produced
    val score = TextScoreExpression(null, null, vector, Literal(query))
    val ordered =
      Sort(Seq(SortOrder(score, Descending, NullsFirst, Seq.empty)), global = true, relation)
    val limited = GlobalLimit(Literal(k), ordered)
    Project(selected :+ Alias(score, SCORE_COLUMN)(), limited)
  }

  private def vectorSearch(args: Seq[Expression]): LogicalPlan = {
    val spark = SparkSession.active
    VectorIndexTypes.register()

    val arguments = new TableFunctionArguments(
      "vector_search",
      args,
      Seq("table", "query_vector", "k", VECTOR_COLUMN))
    arguments.requireOnly(
      required = Seq("table", "query_vector"),
      optional = Seq("k", "num_results", "limit", VECTOR_COLUMN, "distance_type", "columns"))

    val tableName = arguments.string("table")
    val query = arguments.expression("query_vector")
    val k = arguments
      .int("k")
      .orElse(arguments.int("num_results"))
      .orElse(arguments.int("limit"))
      .getOrElse(10)
    val column = arguments.string(VECTOR_COLUMN)
    // the function states its metric, and cosine when the caller names none. Leaving it unnamed to
    // follow the index would need the projected distance to be re-bound to the index metric, which
    // the rewrite only does for the ordering, not for a projection above the limit; until that is
    // fixed, a stated metric is the only way to keep the ordering and the returned distance equal.
    val named = arguments.metric("distance_type")
    val columns = arguments.strings("columns")

    Preconditions.checkArgument(
      k > 0,
      "vector_search needs a positive number of results, got %s",
      k)

    val catalogAndIdent = Spark3Util.catalogAndIdentifier(spark, tableName)
    val catalog = catalogAndIdent.catalog()
    val ident = catalogAndIdent.identifier()
    val table = catalog.asInstanceOf[TableCatalog].loadTable(ident)
    val relation = DataSourceV2Relation.create(
      table,
      Some(catalog),
      Some(ident),
      CaseInsensitiveStringMap.empty())

    def columnOf(name: String): Attribute = {
      relation.output
        .find(attribute => attribute.name.equalsIgnoreCase(name))
        .getOrElse(analysisError(s"Column $name does not exist in $tableName; the table has " +
          relation.output.map(_.name).mkString(", ")))
    }

    val vector = columnOf(column)
    val selected = if (columns.isEmpty) relation.output else columns.map(columnOf)
    // the caller's list is not the only thing that ends up in the output, so the table itself decides
    // whether the distance can be added without a duplicate name
    if (relation.output.exists(attribute => attribute.name.equalsIgnoreCase(DISTANCE_COLUMN))) {
      analysisError(
        s"$tableName already has a column named $DISTANCE_COLUMN, so the distance cannot be added")
    }

    val distance =
      VectorDistanceExpression(named.getOrElse(Metric.COSINE), vector, query, explicitMetric = true)
    val ordered =
      Sort(Seq(SortOrder(distance, Ascending, NullsFirst, Seq.empty)), global = true, relation)
    val limited = GlobalLimit(Literal(k), ordered)
    Project(selected :+ Alias(distance, DISTANCE_COLUMN)(), limited)
  }

  /** The named and positional arguments of a table function, refusing what it does not know. */
  private class TableFunctionArguments(
      name: String,
      args: Seq[Expression],
      positionalNames: Seq[String]) {
    private val named = scala.collection.mutable.LinkedHashMap.empty[String, Expression]
    private val positional = scala.collection.mutable.ArrayBuffer.empty[Expression]

    args.foreach { argument =>
      argument match {
        case NamedArgumentExpression(key, value) =>
          if (named.contains(key.toLowerCase(Locale.ROOT))) {
            analysisError(s"$name was given $key more than once")
          }

          named.put(key.toLowerCase(Locale.ROOT), value)
        case other =>
          positional += other
      }
    }

    positional.zipWithIndex.foreach { case (value, index) =>
      if (index >= positionalNames.length) {
        analysisError(s"$name takes at most ${positionalNames.length} arguments")
      }

      if (named.contains(positionalNames(index))) {
        analysisError(s"$name was given ${positionalNames(index)} both by name and by position")
      }

      named.put(positionalNames(index), value)
    }

    def requireOnly(required: Seq[String], optional: Seq[String]): Unit = {
      val known = required ++ optional
      named.keys.find(key => !known.contains(key)).foreach { unknown =>
        analysisError(s"$name does not have an argument $unknown; it takes ${known.mkString(", ")}")
      }

      required.find(key => !named.contains(key)).foreach { missing =>
        analysisError(s"$name needs $missing")
      }
    }

    def expression(key: String): Expression = named(key)

    def string(key: String): String = {
      named.get(key) match {
        case Some(value) if value.foldable => value.eval().toString
        case Some(_) =>
          analysisError(s"$key of $name must be a constant")
        case None =>
          analysisError(s"$name needs $key")
      }
    }

    def stringOption(key: String): Option[String] = {
      named.get(key).map { value =>
        if (!value.foldable) {
          analysisError(s"$key of $name must be a constant")
        }

        value.eval().toString
      }
    }

    def strings(key: String): Seq[String] = {
      named.get(key) match {
        case None => Seq.empty
        case Some(value) if !value.foldable =>
          analysisError(s"$key of $name must be a constant")
        case Some(value) =>
          value.eval() match {
            case array: ArrayData =>
              (0 until array.numElements()).map(index => array.getUTF8String(index).toString)
            case other =>
              analysisError(s"$key of $name must be an array of names, got $other")
          }
      }
    }

    def int(key: String): Option[Int] = {
      named.get(key).map { value =>
        if (!value.foldable) {
          analysisError(s"$key of $name must be a constant")
        }

        value.eval() match {
          case number: Number => number.intValue()
          case other => analysisError(s"$key of $name must be a number, got $other")
        }
      }
    }

    def metric(key: String): Option[Metric] = {
      named.get(key) match {
        case None => None
        case Some(value) =>
          if (!value.foldable) {
            analysisError(s"$key of $name must be a constant")
          }

          Some(value.eval().toString.toLowerCase(Locale.ROOT) match {
            case "cosine" | "cos" => Metric.COSINE
            case "dot" | "dot_product" | "inner_product" | "ip" => Metric.DOT_PRODUCT
            case "euclidean" | "l2" => Metric.EUCLIDEAN
            case other =>
              analysisError(
                s"Unknown distance_type $other: expected cosine, dot_product or euclidean")
          })
      }
    }
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
    // a table function has no parameter for the coverage mode, so it answers for the whole table:
    // the mode that can be short is the one a caller has to ask for by name
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
