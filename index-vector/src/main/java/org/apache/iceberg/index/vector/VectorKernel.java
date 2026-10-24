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

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.iceberg.index.IndexAdapter.PreparedSearch;
import org.apache.iceberg.index.IndexBuildResult;
import org.apache.iceberg.index.IndexEntry;
import org.apache.iceberg.index.IndexSearchResult;

/**
 * The implementation that builds a vector index payload and searches it.
 *
 * <p>This is the seam a third-party index library is plugged into. Everything above it — the
 * definition, the payload's role and version, where the files live, how a query is planned into a
 * distributed search, how results from several files are merged — is the same whichever kernel ran,
 * and everything a kernel does is confined to two things: turning the entries of one index file
 * into a payload, and searching a payload.
 *
 * <h2>What a kernel has to answer before it can be shipped</h2>
 *
 * <ul>
 *   <li><strong>Serialization</strong>: {@link #payloadFormatVersion()} is bumped whenever {@link
 *       #build} would produce bytes an older reader would misread. A reader refuses a version it
 *       does not know rather than guessing, and says which kernel and which version it met.
 *   <li><strong>Native libraries</strong>: a kernel backed by one loads it once per JVM,
 *       idempotently, and fails with a message that names what is missing. A JNI library is not on
 *       the classpath of a jar, so it has to be on the image and the failure has to say so.
 *   <li><strong>Memory</strong>: off-heap memory a kernel allocates counts against the container's
 *       limit but not against the Java heap, so it has to be accounted for. A build that quietly
 *       allocates a gigabyte off-heap has its executor killed by the container, and what the
 *       scheduler reports is "an executor exited caused by one of the running tasks".
 *   <li><strong>Threads</strong>: {@link #build} is handed the number of threads it may use, which
 *       is the number of cores the task reserved. A kernel that parallelizes internally must not
 *       use more, and a kernel that cannot parallelize must say so through its capabilities rather
 *       than silently ignoring the hint.
 *   <li><strong>Lifecycle</strong>: {@link #prepare} is called once per payload and the returned
 *       search handles many queries, possibly from one thread at a time. It must not hold the
 *       payload bytes beyond what it needs.
 * </ul>
 *
 * <h2>What every kernel has to get right</h2>
 *
 * <p>A score obeys {@link VectorScoring#score}: a distance for {@code EUCLIDEAN} and a similarity
 * for the others, because that is what the index layer orders results by when it merges the hits of
 * several index files. A kernel whose graph traversal measures something else still has to report
 * the score this way, which usually means re-scoring its candidates against the stored vectors.
 *
 * <p>Serializable because an adapter carries its kernel to the executors that build and search; a
 * kernel that keeps something that cannot be shipped turns every task into a serialization failure.
 */
public interface VectorKernel extends java.io.Serializable {

  /** The name recorded in a payload header, and the name an error message uses. */
  String name();

  /** What this kernel can do, so a caller can refuse a request instead of guessing. */
  KernelCapabilities capabilities();

  /**
   * The version of the payload format {@link #build} writes.
   *
   * <p>Part of the payload rather than of the blob type: the blob type says which role a payload
   * plays, and the payload says who wrote it and in which of its formats.
   */
  int payloadFormatVersion();

  /**
   * Builds the payload of one index file.
   *
   * @param entries the entries the payload holds, in the order the caller wants them inserted
   * @param parallelism how many threads this call may use, taken from the cores the task reserved
   */
  ByteBuffer build(List<IndexEntry> entries, int parallelism);

  /** Builds a payload with a memory estimate, or an unknown estimate for legacy kernels. */
  default IndexBuildResult buildWithMemory(List<IndexEntry> entries, int parallelism) {
    return IndexBuildResult.of(build(entries, parallelism), null);
  }

  /**
   * Decodes the entries of an entry-bearing payload, for an index type that stores them as they are
   * rather than in a structure. A kernel that only writes auxiliary payloads never has this called.
   */
  List<IndexEntry> entries(ByteBuffer payload);

  /** Loads a payload once, returning a handle that searches it for as many queries as asked. */
  PreparedSearch prepare(ByteBuffer payload);

  /** Searches a payload once, for a caller that has a single query to run. */
  default List<IndexSearchResult> search(ByteBuffer payload, ByteBuffer query, int topK) {
    return prepare(payload).search(query, topK);
  }
}
