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
package org.apache.iceberg.index.vector;

import java.util.Map;

/**
 * A kernel whose prepared size follows from the structural counts it records.
 *
 * <p>The counts are the ones a build writes into the memory record of a file, so a caller can price
 * a payload it has never opened with the formula this build uses rather than with a number that
 * another model or another layout measured. A kernel whose prepared form those counts do not
 * describe does not implement this.
 */
interface RetainedBytes {

  /**
   * The bytes a searcher prepared from this payload retains, or zero when the counts do not say.
   *
   * <p>The payload itself must not be read: the caller prices files the driver has not opened.
   */
  long retainedBytes(long payloadBytes, Map<String, Long> structure);
}
