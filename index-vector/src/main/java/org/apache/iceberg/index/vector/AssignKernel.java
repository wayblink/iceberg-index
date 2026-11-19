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

/**
 * The nearest-centroid assignment a build can run on a vectorized kernel.
 *
 * <p>The implementation is optional: it is written against the vector API, which ships as an
 * incubator module and is therefore not compiled with the rest of this structure. A build that
 * cannot load it keeps the exact kernel, so this interface only describes the work an
 * implementation does, not a requirement.
 */
interface AssignKernel {

  /**
   * Puts the rows in {@code range} under their nearest centroid.
   *
   * <p>The rows of a range are the only slots the implementation may write, so each range can run
   * on its own thread and produce the assignment a single thread would have produced.
   *
   * @param vectors the vectors, one row each
   * @param centroids the centroids, one row each
   * @param dimension the length of a vector
   * @param range the rows to assign: {@code [from, to]}
   * @param assignment the slots to write, one list per row of {@code range}
   */
  void assignRange(
      float[][] vectors, float[][] centroids, int dimension, int[] range, int[] assignment);
}
