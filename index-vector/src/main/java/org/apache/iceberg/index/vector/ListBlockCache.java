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
 * The list blocks of an IVF file, kept where a later search can reach them.
 *
 * <p>A range search reads the lists it probes and scores them; the bytes it read are the same bytes
 * the next search of the same file will ask for, because the blocking layout of a file does not
 * change between queries. Keeping those blocks lets a repeat query skip the object-store read that
 * a whole-file cache would otherwise carry, without having to hold the whole file: the block of a
 * list is what was read, so it is what is remembered.
 *
 * <p>A cache may drop a block at any time; a search that misses reads it again. An implementation
 * is shared by the searches of one executor, so it must be thread safe, and it may be asked for a
 * block while another search is loading the same one: returning null then is allowed, but two
 * searches should not both read a block the cache could have given one of them.
 */
public interface ListBlockCache {

  /**
   * What one search's view of a cache did since the view was created.
   *
   * <p>A view belongs to one search, so the counters describe that search rather than the
   * executor-wide cache. An implementation that shares one cache between searches must still keep
   * the counters of each view apart: a caller that sums counters per task would otherwise count
   * another task's activity, and the executor's lifetime counters more than once.
   */
  record Stats(long hits, long misses, long loads, long evictions, long bytes) {
    public static final Stats EMPTY = new Stats(0L, 0L, 0L, 0L, 0L);
  }

  /**
   * The bytes of the block of one list, or null when the cache does not hold it.
   *
   * @param location the index file the block belongs to
   * @param offset the byte offset of the list's block in the file payload
   * @param size the length of the block
   */
  byte[] get(String location, int offset, int size);

  /**
   * Keeps the block of one list, or as much of it as the cache's budget allows.
   *
   * <p>The cache keeps the array it is given rather than copying it: the bytes of a list block are
   * immutable for the life of the index file, and a copy would double the memory a block costs. The
   * cache must not modify the array, and a caller that hands one over must not modify it either.
   *
   * @param location the index file the block belongs to
   * @param offset the byte offset of the list's block in the file payload
   * @param bytes the block, which neither side may modify after the call
   */
  void put(String location, int offset, byte[] bytes);

  /**
   * Whether the cache holds the block of one list, without recording a lookup.
   *
   * <p>A planner asks this before it decides whether to read a range at all, so the question must
   * not change the hit and miss counters a search reports.
   *
   * @param location the index file the block belongs to
   * @param offset the byte offset of the list's block in the file payload
   * @param size the length of the block
   */
  default boolean contains(String location, int offset, int size) {
    return get(location, offset, size) != null;
  }

  /** What this search's view of the cache did, for the search that reads it. */
  default Stats stats() {
    return Stats.EMPTY;
  }
}
