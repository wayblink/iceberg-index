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

private[spark] case class VectorCandidateBudget(
    requested: Long,
    initial: Int,
    entryUpperBound: JLong)

private[spark] object VectorCandidateBudget {
  def derive(k: Int, overSample: Long, entryUpperBound: JLong): VectorCandidateBudget = {
    require(k > 0, s"Invalid vector top-k: $k")
    require(overSample > 0, s"Invalid vector over-sample: $overSample")
    val requested =
      try {
        Math.multiplyExact(k.toLong, overSample)
      } catch {
        case cause: ArithmeticException =>
          throw new IllegalArgumentException(s"Candidate budget overflow: $k * $overSample", cause)
      }
    VectorCandidateBudget(requested, bounded(requested, entryUpperBound), entryUpperBound)
  }

  def next(current: Int, live: Int, k: Int, entryUpperBound: JLong): Int = {
    require(current >= 0, s"Invalid current candidate budget: $current")
    require(live >= 0, s"Invalid live candidate count: $live")
    require(k > 0, s"Invalid vector top-k: $k")
    val requested = current.toLong + math.max(1L, k.toLong - live.toLong)
    val nextBudget = bounded(requested, entryUpperBound)
    require(
      nextBudget > current,
      s"Candidate budget cannot make progress: current=$current, next=$nextBudget")
    nextBudget
  }

  /**
   * The next cumulative budget, or null when it cannot be expressed.
   *
   * <p>A retry loop treats a budget it cannot represent as the end of its retries rather than a
   * reason to fail a query that already has candidates: it returns what it found and says why it
   * stopped.
   */
  def nextOrNull(current: Int, live: Int, k: Int, entryUpperBound: JLong): VectorCandidateBudget = {
    if (current < 0 || live < 0 || k <= 0) {
      return null
    }
    val requested = current.toLong + math.max(1L, k.toLong - live.toLong)
    val effective = if (entryUpperBound == null) {
      requested
    } else {
      math.min(requested, entryUpperBound.longValue())
    }
    if (effective <= current || effective > Int.MaxValue) {
      null
    } else {
      VectorCandidateBudget(requested, effective.toInt, entryUpperBound)
    }
  }

  private def bounded(requested: Long, entryUpperBound: JLong): Int = {
    val effective = if (entryUpperBound == null) {
      requested
    } else {
      require(entryUpperBound >= 0L, s"Invalid index entry upper bound: $entryUpperBound")
      math.min(requested, entryUpperBound.longValue())
    }
    require(
      effective <= Int.MaxValue,
      s"Effective candidate budget $effective cannot be represented as an int")
    effective.toInt
  }
}
