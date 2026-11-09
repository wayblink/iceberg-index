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
import org.apache.iceberg.index.vector.RaBitQQuantizer
import org.apache.iceberg.index.vector.VectorIndexDefinition
import org.apache.iceberg.index.vector.RaBitQQuantizer
import org.apache.iceberg.index.vector.VectorIndexDefinition.VectorQuantizer
import org.apache.iceberg.index.vector.RaBitQQuantizer
import org.apache.iceberg.index.vector.VectorIndexDefinition.VectorStructure
private[spark] case class VectorParameter(
    value: String,
    origin: String,
    locked: Boolean,
    shadowedOrigin: String)

private[spark] case class RawVectorParameters(
    overSample: VectorParameter,
    indexName: VectorParameter)

private[spark] case class ResolvedVectorParameters(
    overSample: Long,
    overSampleOrigin: String,
    overSampleLocked: Boolean,
    overSampleShadowedOrigin: String,
    overSampleReason: String,
    indexName: VectorParameter)

private[spark] object VectorQueryParameters {
  val OverSampleConf = "spark.iceberg.index.vector.over-sample"
  val IndexNameConf = "spark.iceberg.index.vector.name"

  def capture(
      session: JMap[String, String],
      submit: JMap[String, String],
      indexArgument: String): RawVectorParameters = {
    require(session != null, "Session configuration cannot be null")
    require(submit != null, "Submit configuration cannot be null")
    val overSample = configured(session, submit, OverSampleConf)
    val configuredIndex = configured(session, submit, IndexNameConf)
    val index = if (indexArgument == null) {
      configuredIndex
    } else {
      val shadowed = if (configuredIndex == null) {
        null
      } else if (configuredIndex.shadowedOrigin == null) {
        configuredIndex.origin
      } else {
        s"${configuredIndex.origin},${configuredIndex.shadowedOrigin}"
      }
      VectorParameter(indexArgument, "CALL_ARGUMENT", true, shadowed)
    }
    RawVectorParameters(overSample, index)
  }

  def decide(
      raw: RawVectorParameters,
      definition: VectorIndexDefinition): ResolvedVectorParameters = {
    require(raw != null, "Vector parameters cannot be null")
    require(definition != null, "Vector index definition cannot be null")
    val parameter = raw.overSample
    val index = if (raw.indexName == null) {
      null
    } else {
      require(
        raw.indexName.value != null && raw.indexName.value.trim.nonEmpty,
        s"Invalid $IndexNameConf: '${raw.indexName.value}'")
      raw.indexName.copy(value = raw.indexName.value.trim)
    }
    if (parameter == null) {
      ResolvedVectorParameters(
        defaultOverSample(definition),
        "STRUCTURE_DEFAULT",
        false,
        null,
        "documented-experience-values",
        index)
    } else {
      val message = s"Invalid $OverSampleConf: '${parameter.value}'; expected a positive integer"
      require(parameter.value != null, message)
      val value =
        try {
          JLong.parseLong(parameter.value.trim)
        } catch {
          case cause: NumberFormatException => throw new IllegalArgumentException(message, cause)
        }
      require(value > 0L, message)
      ResolvedVectorParameters(
        value,
        parameter.origin,
        parameter.locked,
        parameter.shadowedOrigin,
        null,
        index)
    }
  }

  /**
   * The multiple applied when the caller configured none, chosen from how the index stores vectors.
   *
   * <p>A query is not answered by a scan because a knob was left alone: the index is used, and this
   * is the width it uses. Picking the width is unavoidable, so it is picked from what the structure
   * does rather than from a compatibility constant:
   *
   * <ul>
   *   <li>a flat index walks every entry, so any width is the whole table;
   *   <li>an IVF index over unquantized vectors scores exactly, so the k nearest of the partitions
   *       nprobe visits are the k nearest of them at any width;
   *   <li>an IVF index over a quantized code orders candidates by an estimate, and the digits of
   *       separation between candidates shrink with the bits stored: one bit per dimension needs a
   *       much wider net than three or four before the true neighbours are inside it;
   *   <li>a graph's breadth is its own ef, and a wider request can widen the search itself.
   * </ul>
   *
   * <p>The numbers are the ranges this project has published for these tables (one bit needs fifty
   * to a hundred, three bits about ten, a graph one to three), interpolated rather than measured per
   * table. They are a default, not a guarantee: a caller who needs a different width sets
   * {@link #OverSampleConf} and the plan then says so.
   */
  def defaultOverSample(definition: VectorIndexDefinition): Long =
    definition.structure() match {
      case VectorStructure.FLAT => 1L
      case VectorStructure.IVF =>
        definition.quantizer() match {
          case VectorQuantizer.NONE => 1L
          case _ =>
            // the quantizer reads its own option, including the default it applies when the build
            // did not write one - asking it rather than parsing the map keeps the two in step
            RaBitQQuantizer.bitsFrom(definition.properties()) match {
              case b if b <= 1 => 50L
              case 2 => 25L
              case 3 => 10L
              case _ => 5L
            }
        }
      case VectorStructure.HNSW => 3L
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
