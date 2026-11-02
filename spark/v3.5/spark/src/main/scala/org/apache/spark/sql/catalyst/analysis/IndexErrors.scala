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
package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.AnalysisException

/**
 * An analysis error with a message of ours rather than one of Spark's error classes.
 *
 * <p>Spark's other constructor takes an error class and its parameters: a message passed there is
 * looked up as a class name, and one it does not find is reported as an internal error reading
 * `Cannot find main error class '<the message>'`. A caller who mistyped an argument would see that
 * instead of what they did wrong.
 *
 * <p>The constructor that takes the message alone is private to Spark's own packages, which is why
 * this sits in one of them: Iceberg's rules already live here for the same reason.
 */
object IndexErrors {

  def analysis(message: String): AnalysisException = new AnalysisException(message)
}
