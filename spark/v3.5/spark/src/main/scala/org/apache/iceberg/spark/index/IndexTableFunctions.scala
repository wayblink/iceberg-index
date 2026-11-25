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

import java.util.Locale
import org.apache.iceberg.SerializableTable
import org.apache.iceberg.index.spark.IndexDriverMetadata
import org.apache.iceberg.index.spark.IndexSearch
import org.apache.iceberg.index.spark.IndexSelection
import org.apache.iceberg.index.text.TextIndexTypes
import org.apache.iceberg.index.vector.VectorIndexDefinition.Metric
import org.apache.iceberg.index.vector.VectorIndexTypes
import org.apache.iceberg.relocated.com.google.common.base.Preconditions
import org.apache.iceberg.spark.Spark3Util
import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.catalyst.analysis.IndexErrors
import org.apache.spark.sql.catalyst.expressions.IsNotNull
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.Ascending
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.NamedArgumentExpression
import org.apache.spark.sql.catalyst.expressions.NullsFirst
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.logical.GlobalLimit
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.plans.logical.Sort
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Registers the index table-valued functions: functions that take a table, a column and a query and
 * return the matched rows as a table, so they can be used in a FROM clause and composed with the
 * rest of a query.
 *
 * <p>Business row functions bind a table snapshot and use a physical search operator. Index hit
 * functions expose the file, position and score of each hit.
 */
object IndexTableFunctions {

  private val VECTOR_SEARCH = "vector_search"
  private val TEXT_SEARCH = "search"
  private val VECTOR_COLUMN = "vector_column"
  private val DISTANCE_COLUMN = "_distance"
  // the column a search answers the score it ranked by in: one name for every entry point that
  // publishes one, and the name the source keeps when it answers fewer columns
  private val SCORE_COLUMN = TextSearchSource.ScoreColumn
  private val VECTOR_INDEX_SEARCH = "vector_index_search"
  private val VECTOR_INDEX_SEARCH_BATCH = "vector_index_search_batch"
  private val TEXT_INDEX_SEARCH = "text_index_search"

  /** The rows a search returns when the caller names no limit. */
  private val DefaultLimit = 10

  /** The arguments every index table function takes: a table, a column, what to look for, and k. */
  private val SearchArgumentCount = 4

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
          "vector_search(table, query_vector, k, vector_column, distance_type, columns, nprobe, "
            + "refine_factor, index): the nearest rows of a table"),
        vectorSearch))
    extensions.injectTableFunction((
      new FunctionIdentifier(VECTOR_INDEX_SEARCH),
      new ExpressionInfo(
        VECTOR_INDEX_SEARCH,
        "vector_index_search(table, column, query, k[, nprobe, refine_factor, index]): the index's "
          + "own hits"),
      vectorIndexSearch))
    extensions.injectTableFunction(
      (
        new FunctionIdentifier(VECTOR_INDEX_SEARCH_BATCH),
        new ExpressionInfo(
          VECTOR_INDEX_SEARCH_BATCH,
          "vector_index_search_batch(table, column, queries, k[, nprobe, refine_factor, index]): the "
            + "index's own hits for many queries at once"),
        vectorIndexSearchBatch))
    extensions.injectTableFunction((
      new FunctionIdentifier(TEXT_INDEX_SEARCH),
      new ExpressionInfo(
        TEXT_INDEX_SEARCH,
        "text_index_search(table, column, query, k[, index]): the index's own hits for one query"),
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
  private def analysisError(message: String): Nothing = throw IndexErrors.analysis(message)

  /** Returns rows of one table snapshot ranked by its text index. */
  private def textSearch(args: Seq[Expression]): LogicalPlan = {
    val spark = SparkSession.active
    TextIndexTypes.register()

    val arguments =
      new TableFunctionArguments(TEXT_SEARCH, args, Seq("table", "query", "search_columns", "k"))
    arguments.requireOnly(
      required = Seq("table", "search_columns"),
      optional =
        Seq("query", "query_string", "limit", "k", "num_results", "columns", "filter", "index"))

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
      .getOrElse(DefaultLimit)
    val columns = arguments.strings("columns")
    val indexName = arguments.stringOption("index")

    Preconditions.checkArgument(
      searched.size == 1,
      "search needs exactly one column in search_columns, got %s",
      searched.size)
    Preconditions.checkArgument(k > 0, "search needs a positive number of rows, got %s", k)

    val catalogAndIdent = Spark3Util.catalogAndIdentifier(spark, tableName)
    val catalog = catalogAndIdent.catalog()
    val ident = catalogAndIdent.identifier()
    val sparkTable = catalog.asInstanceOf[TableCatalog].loadTable(ident) match {
      case table: SparkTable => table
      case _ => analysisError(s"search needs an Iceberg table: $tableName")
    }
    val icebergTable = sparkTable.table()
    icebergTable.refresh()
    val relation = DataSourceV2Relation.create(
      sparkTable,
      Some(catalog),
      Some(ident),
      CaseInsensitiveStringMap.empty())

    def columnOf(name: String): Attribute = {
      relation.output
        .find(attribute => attribute.name.equalsIgnoreCase(name))
        .getOrElse(analysisError(s"Column $name does not exist in $tableName; the table has " +
          relation.output.map(_.name).mkString(", ")))
    }

    val column = columnOf(searched.head).name
    val selected =
      if (columns.isEmpty) {
        relation.output
      } else {
        val chosen = columns.map(columnOf)
        val seen = scala.collection.mutable.Set.empty[String]
        chosen.foreach { attribute =>
          if (!seen.add(attribute.name.toLowerCase(java.util.Locale.ROOT))) {
            analysisError(s"columns names column ${attribute.name} more than once")
          }
        }
        chosen
      }
    if (relation.output.exists(attribute => attribute.name.equalsIgnoreCase(SCORE_COLUMN))) {
      analysisError(s"$tableName already has a column named $SCORE_COLUMN")
    }

    val snapshotId = IndexRewriteSupport
      .scanSnapshotId(sparkTable)
      .getOrElse(
        analysisError(s"Table $tableName has no snapshot; build a text index after writing data"))
    val frozen =
      if (IndexDriverMetadata.enabled(spark)) {
        SerializableTable.copyOfLoaded(icebergTable)
      } else {
        SerializableTable.copyOf(icebergTable)
      }
    val index =
      try {
        IndexSelection.resolve(frozen, column, "text", indexName.orNull)
      } catch {
        case e: IllegalArgumentException => analysisError(e.getMessage)
      }
    if (index.snapshotForSourceTableSnapshot(snapshotId) == null) {
      analysisError(
        s"Text index of $tableName.$column does not cover snapshot $snapshotId; " +
          "run refresh_text_index")
    }

    val filter = arguments.stringOption("filter").map { sql =>
      TextSearchFilter.bind(spark, frozen.schema(), relation.output, sql)
    }
    val score = AttributeReference(SCORE_COLUMN, DoubleType, nullable = false)()
    TextSearchSource(frozen, index, snapshotId, query, k, filter, selected :+ score)
  }

  private def vectorSearch(args: Seq[Expression]): LogicalPlan = {
    val spark = SparkSession.active
    VectorIndexTypes.register()

    val arguments = new TableFunctionArguments(
      "vector_search",
      args,
      Seq("table", "query_vector", "k", VECTOR_COLUMN))
    arguments.refuseRenamed(Map("over_sample" -> "refine_factor"))
    arguments.requireOnly(
      required = Seq("table", "query_vector"),
      optional = Seq(
        "k",
        "num_results",
        "limit",
        VECTOR_COLUMN,
        "distance_type",
        "columns",
        // the query settings: a call may name them, and then they win over the session's
        "nprobe",
        "refine_factor",
        "index"))

    val tableName = arguments.string("table")
    val query = arguments.expression("query_vector")
    val k = arguments
      .int("k")
      .orElse(arguments.int("num_results"))
      .orElse(arguments.int("limit"))
      .getOrElse(DefaultLimit)
    val column = arguments.string(VECTOR_COLUMN)
    // a metric the caller names is the question, and one it does not name is whatever the index was
    // built with: the distance is projected below the limit, where the rewrite binds its metric, so
    // the ordering and the returned distance stay the same measurement either way
    val named = arguments.metric("distance_type")
    val columns = arguments.strings("columns")
    val nprobe = arguments.int("nprobe")
    val refineFactor = arguments.int("refine_factor")
    val index = arguments.stringOption("index")

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
    val selected =
      if (columns.isEmpty) {
        relation.output
      } else {
        val chosen = columns.map(columnOf)
        val seen = scala.collection.mutable.Set.empty[String]
        chosen.foreach { attribute =>
          if (!seen.add(attribute.name.toLowerCase(java.util.Locale.ROOT))) {
            analysisError(s"columns names column ${attribute.name} more than once")
          }
        }
        chosen
      }
    // the caller's list is not the only thing that ends up in the output, so the table itself decides
    // whether the distance can be added without a duplicate name
    if (relation.output.exists(attribute => attribute.name.equalsIgnoreCase(DISTANCE_COLUMN))) {
      analysisError(
        s"$tableName already has a column named $DISTANCE_COLUMN, so the distance cannot be added")
    }

    val distance = VectorDistanceExpression(
      named.getOrElse(Metric.DEFAULT),
      vector,
      query,
      explicitMetric = named.isDefined)
    // a row without a vector has no distance, so it has no place in a distance ordering: plain
    // Spark would sort it first under ASC and spend one of the k rows on it, which is neither what
    // the index branch answers nor what the guide promises
    val candidates = Filter(IsNotNull(vector), relation)
    // the distance is projected below the limit, not above it, so the projection is inside the part
    // of the plan the rewrite binds the index's metric in: a sort over the name the projection gives
    // means the same thing as a sort that computes it, and both follow the index
    val computed = Project(selected :+ Alias(distance, DISTANCE_COLUMN)(), candidates)
    val ordered =
      Sort(
        Seq(SortOrder(computed.output.last, Ascending, NullsFirst, Seq.empty)),
        global = true,
        computed)
    VectorSearchOptions(
      VectorSearchArguments(
        nprobe.map(value => Integer.toString(value)).orNull,
        refineFactor.map(value => Integer.toString(value)).orNull,
        index.orNull),
      GlobalLimit(Literal(k), ordered))
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

    /**
     * Refuses an argument under the name it used before being renamed.
     *
     * <p>The old spelling is not an alias: saying so is better than the unknown-argument message,
     * which reads like a typo and leaves the caller to guess which name replaced it.
     */
    def refuseRenamed(renamed: Map[String, String]): Unit = {
      named.keys.find(key => renamed.contains(key)).foreach { old =>
        analysisError(s"$name's $old argument was renamed to ${renamed(old)}")
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

          val metricName = value.eval().toString
          try {
            Some(Metric.fromName(metricName))
          } catch {
            case _: IllegalArgumentException =>
              analysisError(s"Unknown distance_type $metricName: expected l2, cosine or dot")
          }
      }
    }
  }

  private def vectorIndexSearch(args: Seq[Expression]): LogicalPlan = {
    checkAtLeast(
      args,
      SearchArgumentCount,
      VECTOR_INDEX_SEARCH,
      "(table, column, query, k[, option => value])")
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
    val options = queryOptions(VECTOR_INDEX_SEARCH, args)
    // a table function has no coverage argument, so it answers with the index's own hits: a hit
    // whose row was removed is kept with a null row, and the files the index does not cover are not
    // scanned. A caller who wants the whole table asks a procedure for the table coverage mode.
    VectorSearchCalls.planOf(
      tableName,
      column,
      options.index.orNull,
      queries,
      topK,
      batch = false,
      options.nprobe.map(value => Integer.toString(value)).orNull,
      options.refineFactor.map(value => Integer.toString(value)).orNull)
  }

  private def vectorIndexSearchBatch(args: Seq[Expression]): LogicalPlan = {
    checkAtLeast(
      args,
      SearchArgumentCount,
      VECTOR_INDEX_SEARCH_BATCH,
      "(table, column, queries, k[, option => value])")
    val spark = SparkSession.active
    VectorIndexTypes.register()
    val tableName = stringArg(args, 0)
    val column = stringArg(args, 1)
    val queries = stringArrayArg(args, 2)
    val topK = intArg(args, 3)

    val vectors = new java.util.ArrayList[Array[Float]]()
    queries.foreach(query => vectors.add(IndexTableFunctions.parseVector(query)))
    val options = queryOptions(VECTOR_INDEX_SEARCH_BATCH, args)
    // as with the single-query form, the function returns a request and never searches
    VectorSearchCalls.planOf(
      tableName,
      column,
      options.index.orNull,
      vectors,
      topK,
      batch = true,
      options.nprobe.map(value => Integer.toString(value)).orNull,
      options.refineFactor.map(value => Integer.toString(value)).orNull)
  }

  private def textIndexSearch(args: Seq[Expression]): LogicalPlan = {
    checkAtLeast(
      args,
      SearchArgumentCount,
      TEXT_INDEX_SEARCH,
      "(table, column, query, k[, index => '...'])")
    val spark = SparkSession.active
    TextIndexTypes.register()
    val tableName = stringArg(args, 0)
    val column = stringArg(args, 1)
    val query = stringArg(args, 2)
    val topK = intArg(args, 3)

    val table = Spark3Util.loadIcebergTable(spark, tableName)
    val index =
      try {
        IndexSelection.resolve(table, column, "text", textIndexOption(args).orNull)
      } catch {
        case e: IllegalArgumentException => analysisError(e.getMessage)
      }
    val payload =
      new org.apache.iceberg.index.text.TextIndexAdapter(
        index.definition().asInstanceOf[org.apache.iceberg.index.text.TextIndexDefinition])
        .encodeQuery(query)
    IndexSearch
      .search(spark, table, column, index, payload, topK)
      .queryExecution
      .analyzed
  }

  /** Reads the index a text index function names after its positional arguments. */
  private def textIndexOption(args: Seq[Expression]): Option[String] = {
    val extras = args.drop(SearchArgumentCount)
    extras.foreach {
      case _: NamedArgumentExpression => ()
      case other =>
        analysisError(
          s"$TEXT_INDEX_SEARCH takes only named options after its arguments, got $other")
    }

    val arguments = new TableFunctionArguments(TEXT_INDEX_SEARCH, extras, Seq.empty)
    arguments.requireOnly(required = Seq.empty, optional = Seq("index"))
    arguments.stringOption("index")
  }

  /** Parses a comma-separated list of numbers, the form the batch functions take. */
  def parseVector(text: String): Array[Float] = {
    text.split(",").filter(_.nonEmpty).map(_.trim.toFloat)
  }

  /** The query settings a call may name after its positional arguments. */
  private case class QueryOptions(
      nprobe: Option[Int],
      refineFactor: Option[Int],
      index: Option[String])

  /**
   * Reads the named options a table function call wrote after its positional arguments.
   *
   * <p>Only named arguments are accepted there: a fifth positional value has no meaning the reader
   * could agree on, so it is refused rather than guessed. An option the function does not know is
   * refused for the same reason.
   */
  private def queryOptions(name: String, args: Seq[Expression]): QueryOptions = {
    val extras = args.drop(SearchArgumentCount)
    extras.foreach {
      case _: NamedArgumentExpression => ()
      case other =>
        analysisError(s"$name takes only named options after its arguments, got $other")
    }

    val arguments = new TableFunctionArguments(name, extras, Seq.empty)
    arguments.refuseRenamed(Map("over_sample" -> "refine_factor"))
    arguments.requireOnly(required = Seq.empty, optional = Seq("nprobe", "refine_factor", "index"))
    QueryOptions(
      arguments.int("nprobe"),
      arguments.int("refine_factor"),
      arguments.stringOption("index"))
  }

  private def checkAtLeast(
      args: Seq[Expression],
      expected: Int,
      name: String,
      signature: String): Unit = {
    require(
      args.length >= expected,
      s"$name expects at least $expected arguments $signature, got ${args.length}")
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
