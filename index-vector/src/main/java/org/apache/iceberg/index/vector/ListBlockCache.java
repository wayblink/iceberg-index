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

  /** What a cache has done since it was created. */
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
   * Keeps a copy of the block of one list.
   *
   * @param location the index file the block belongs to
   * @param offset the byte offset of the list's block in the file payload
   * @param bytes the block, which the cache must not modify
   */
  void put(String location, int offset, byte[] bytes);

  /** What the cache has done, for the search that reads it. */
  default Stats stats() {
    return Stats.EMPTY;
  }
}
