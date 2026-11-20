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

import java.lang.{Long => JLong}
import java.util.{Map => JMap}
import org.apache.iceberg.index.vector.VectorIndexDefinition

/**
 * One setting as the layer that supplied it reported it.
 *
 * <p>{@code locked} says whether the value is passed on as the query's own instruction instead of
 * staying a default: a call's argument is, and so is a configured multiple or index name, while a
 * configured probe count is not, because the encoder reads that setting itself.
 */
private[spark] case class VectorParameter(
    value: String,
    origin: String,
    locked: Boolean,
    shadowedOrigin: String)

private[spark] case class RawVectorParameters(
    refineFactor: VectorParameter,
    indexName: VectorParameter,
    filterMode: VectorParameter,
    nprobe: VectorParameter)

private[spark] case class ResolvedVectorParameters(
    refineFactor: Long,
    refineFactorOrigin: String,
    refineFactorLocked: Boolean,
    refineFactorShadowedOrigin: String,
    refineFactorReason: String,
    indexName: VectorParameter,
    filterMode: String = VectorQueryParameters.DefaultFilterMode,
    filterModeOrigin: String = "DEFAULT",
    nprobe: Long = 0L,
    nprobeOrigin: String = "DEFAULT",
    nprobeLocked: Boolean = false,
    nprobeShadowedOrigin: String = null)

private[spark] object VectorQueryParameters {
  val RefineFactorConf = "spark.iceberg.index.vector.refine-factor"

  /**
   * The name this setting carried before the rename; kept only so it can be refused by name.
   *
   * <p>Spark ignores a configuration key it does not know, so a session still carrying the old name
   * would answer with the default multiple — a narrower candidate window, and fewer of the rows the
   * caller asked for — rather than fail.
   */
  val RenamedOverSampleConf = "spark.iceberg.index.vector.over-sample"

  val IndexNameConf = "spark.iceberg.index.vector.name"

  /**
   * How a filter that a rewrite carries into the search is applied: before the search, or after it.
   *
   * <p>The link an operator is not told about is the one that cannot surprise it, so this is a
   * session setting rather than a property of the table, and it is a choice between two links rather
   * than a rule that picks one: a query that was not asked how to filter is answered the way it
   * always was, not the way a threshold guessed. It is not an argument a call can name, because it
   * describes the rewrite that carries the filter, not the search a call asks for.
   */
  val FilterModeConf = "spark.iceberg.index.vector.filter-mode"

  /**
   * The probe count a query uses when it names none.
   *
   * <p>It is read from the same setting the encoder reads, so a count the session sets and a count
   * the call passes cannot disagree about which one is in force. Only a count the call names is
   * handed to the encoder explicitly; a configured one is read by the encoder itself, which is what
   * keeps an index that does not probe lists ignoring it.
   */
  val NprobeConf: String = org.apache.iceberg.index.spark.VectorIndexSearch.IVF_NPROBE

  /** Read the filter's rows first and let the search skip the rest. */
  val FilterModePre = "pre"

  /** Search first and keep the rows the filter admits, asking for more while the window is short. */
  val FilterModePost = "post"

  /**
   * The link a query gets when it names none.
   *
   * <p>This one loses no rows: it narrows where the search looks rather than trimming what the
   * search found, so a filtered answer never comes back shorter than the rows that pass. The other
   * link is for a filter whose columns are expensive to read, and the query has to ask for it.
   */
  val DefaultFilterMode = FilterModePre

  def capture(
      session: JMap[String, String],
      submit: JMap[String, String],
      indexArgument: String): RawVectorParameters =
    capture(session, submit, indexArgument, null, null)

  def capture(
      session: JMap[String, String],
      submit: JMap[String, String],
      indexArgument: String,
      nprobeArgument: String,
      refineFactorArgument: String = null): RawVectorParameters = {
    require(session != null, "Session configuration cannot be null")
    require(submit != null, "Submit configuration cannot be null")
    refuseRenamed(session, submit)
    val refineFactor = argued(refineFactorArgument, configured(session, submit, RefineFactorConf))
    val index = argued(indexArgument, configured(session, submit, IndexNameConf))
    val filterMode = configured(session, submit, FilterModeConf)
    val nprobe = namedProbe(nprobeArgument, configured(session, submit, NprobeConf))
    RawVectorParameters(refineFactor, index, filterMode, nprobe)
  }

  /**
   * The value the call named, or the configured one it did not.
   *
   * <p>A call's argument is the query's own question, so it wins over the session and names what it
   * shadowed; the configured value is still reported so a log can say which layer answered.
   */
  private def argued(argument: String, configured: VectorParameter): VectorParameter = {
    if (argument == null) {
      configured
    } else {
      VectorParameter(argument, "CALL_ARGUMENT", true, shadowedBy(configured))
    }
  }

  private def shadowedBy(configured: VectorParameter): String = {
    if (configured == null) {
      null
    } else if (configured.shadowedOrigin == null) {
      configured.origin
    } else {
      s"${configured.origin},${configured.shadowedOrigin}"
    }
  }

  /**
   * The probe count the encoder is told to use, or null when the encoder reads the session's itself.
   *
   * <p>Only a count the call named travels with the query. A configured one is the setting the
   * encoder reads at the same point it always did, so it stays a default rather than an instruction:
   * a value that is not passed cannot be mistaken for one the call asked for, and an index that does
   * not probe lists keeps ignoring the session. The origin of the configured value is kept, so a log
   * still says which layer supplied it.
   */
  private def namedProbe(argument: String, configuredValue: VectorParameter): VectorParameter = {
    if (argument != null) {
      VectorParameter(argument, "CALL_ARGUMENT", true, shadowedBy(configuredValue))
    } else if (configuredValue == null) {
      null
    } else {
      configuredValue.copy(locked = false)
    }
  }

  def decide(
      raw: RawVectorParameters,
      definition: VectorIndexDefinition): ResolvedVectorParameters = {
    require(raw != null, "Vector parameters cannot be null")
    require(definition != null, "Vector index definition cannot be null")
    val parameter = raw.refineFactor
    val index = if (raw.indexName == null) {
      null
    } else {
      require(
        raw.indexName.value != null && raw.indexName.value.trim.nonEmpty,
        s"Invalid $IndexNameConf: '${raw.indexName.value}'")
      raw.indexName.copy(value = raw.indexName.value.trim)
    }
    val mode = filterModeOf(raw.filterMode)
    val probes = nprobeOf(raw.nprobe)
    if (parameter == null) {
      // The multiple a query gets when it names none: one, the multiple that means "do not widen".
      // It is stated rather than left as zero so the width a plan reports is the width it uses; an
      // explicit zero stays legal and resolves to the same base budget, k times max(1, multiple).
      ResolvedVectorParameters(
        1L,
        "DEFAULT",
        false,
        null,
        null,
        index,
        mode._1,
        mode._2,
        probes._1,
        probes._2,
        probes._3,
        probes._4)
    } else {
      val message =
        s"Invalid $RefineFactorConf: '${parameter.value}'; expected a non-negative integer"
      require(parameter.value != null, message)
      val value =
        try {
          JLong.parseLong(parameter.value.trim)
        } catch {
          case cause: NumberFormatException => throw new IllegalArgumentException(message, cause)
        }
      require(value >= 0L, message)
      ResolvedVectorParameters(
        value,
        parameter.origin,
        parameter.locked,
        parameter.shadowedOrigin,
        null,
        index,
        mode._1,
        mode._2,
        probes._1,
        probes._2,
        probes._3,
        probes._4)
    }
  }

  /**
   * The link a query asked for, or the rule that picks one.
   *
   * <p>An unrecognised value fails the query: answering it some other way would be answering a
   * question the caller did not ask, and a filter that is applied on the wrong side of the search
   * returns the wrong rows rather than fewer of the right ones.
   */
  private def filterModeOf(parameter: VectorParameter): (String, String) = {
    if (parameter == null) {
      return (DefaultFilterMode, "DEFAULT")
    }

    val value =
      if (parameter.value == null) null else parameter.value.trim.toLowerCase(java.util.Locale.ROOT)
    val valid = Set(FilterModePre, FilterModePost)
    require(
      valid.contains(value),
      s"Invalid $FilterModeConf: '${parameter.value}'; expected one of pre, post")
    (value, parameter.origin)
  }

  /**
   * The probe count the query asks for, or zero when it asks for the encoder's default.
   *
   * <p>A count that is not a positive integer that fits an int fails the query: probing a different
   * number of lists than the caller asked for would answer a different question, and a count the
   * encoder cannot represent cannot be trimmed to fit without asking one.
   */
  private def nprobeOf(parameter: VectorParameter): (Long, String, Boolean, String) = {
    if (parameter == null) {
      return (0L, "DEFAULT", false, null)
    }

    val message = s"Invalid $NprobeConf: '${parameter.value}'; expected a positive integer"
    require(parameter.value != null, message)
    val value =
      try {
        JLong.parseLong(parameter.value.trim)
      } catch {
        case cause: NumberFormatException => throw new IllegalArgumentException(message, cause)
      }
    require(value > 0L, message)
    require(value <= Integer.MAX_VALUE.toLong, message)
    (value, parameter.origin, parameter.locked, parameter.shadowedOrigin)
  }

  /**
   * Refuses the setting's former name instead of letting Spark ignore it.
   *
   * <p>The two names never mean different things: the old one is only here to be reported, so a
   * configuration written before the rename fails with the name to use now. A key neither layer
   * sets is not a problem and passes.
   */
  private def refuseRenamed(session: JMap[String, String], submit: JMap[String, String]): Unit = {
    require(
      !session.containsKey(RenamedOverSampleConf) && !submit.containsKey(RenamedOverSampleConf),
      s"$RenamedOverSampleConf was renamed to $RefineFactorConf; update the setting")
  }

  private def configured(
      session: JMap[String, String],
      submit: JMap[String, String],
      key: String): VectorParameter = {
    if (session.containsKey(key)) {
      val sameSubmit = submit.containsKey(key) && session.get(key) == submit.get(key)
      val origin = if (sameSubmit) "USER_CONF(session-or-submit)" else "SESSION_CONF"
      val shadowed = if (submit.containsKey(key) && !sameSubmit) "SUBMIT_CONF" else null
      VectorParameter(session.get(key), origin, true, shadowed)
    } else if (submit.containsKey(key)) {
      VectorParameter(submit.get(key), "SUBMIT_CONF", true, null)
    } else {
      null
    }
  }
}
