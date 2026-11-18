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

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import org.apache.iceberg.index.vector.VectorIndexAdapter.RangeReadMetrics;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/** Selects and retains byte ranges for the lifetime of one index-file search task. */
final class IvfListRanges implements AutoCloseable {
  private static final int MAX_CACHED_BYTES = 8 * 1024 * 1024;
  private static final int MAX_CACHED_RANGES = 256;
  private static final int MIN_SCATTERED_RANGES = 4;

  /**
   * The largest gap between two selected runs that is read as part of one range.
   *
   * <p>Zero merges only runs that touch, which is what adjacent lists are. A positive value trades
   * bytes for requests: the gap is read but not scored, so it is worth it when one more read costs
   * more than the bandwidth the gap takes. That ratio is a property of the storage, not of the
   * index, so it is a constant to be set from the measurement in the design document rather than a
   * setting to be carried with a table.
   */
  static final int MAX_MERGE_GAP_BYTES = 0;

  private final IvfRangeInput input;
  private final int[] starts;
  private final int mergeGapBytes;
  private final ListBlockCache blocks;
  private final RangeReadMetrics metadataReads;
  private final TreeMap<Integer, Integer> planned = new TreeMap<>();
  private final TreeMap<Integer, byte[]> cachedByOffset = new TreeMap<>();
  private final LinkedHashMap<Integer, byte[]> lru = new LinkedHashMap<>(16, 0.75f, true);
  private int cachedBytes;
  private int wholeListAreaReads;
  private long blockHits;
  private long blockMisses;
  private long blockLoads;
  private byte[] wholeLists;

  IvfListRanges(IvfRangeInput input, int[] starts) {
    this(input, starts, MAX_MERGE_GAP_BYTES, null);
  }

  IvfListRanges(IvfRangeInput input, int[] starts, int mergeGapBytes) {
    this(input, starts, mergeGapBytes, null);
  }

  IvfListRanges(IvfRangeInput input, int[] starts, int mergeGapBytes, ListBlockCache blocks) {
    Preconditions.checkArgument(mergeGapBytes >= 0, "Invalid merge gap: %s", mergeGapBytes);
    this.input = input;
    this.starts = starts;
    this.mergeGapBytes = mergeGapBytes;
    this.blocks = blocks;
    this.metadataReads = input.metrics();
  }

  /** Plans the reads and orders the selected lists so each coalesced run is consumed together. */
  void planInReadOrder(int[] lists, boolean[] scored) {
    Arrays.sort(lists);
    if (wholeLists != null || lists.length == 0) {
      return;
    }

    planned.clear();
    long selectedBytes = 0L;
    long missingBytes = 0L;
    int runStart = -1;
    int runEnd = -1;
    for (int list : lists) {
      int start = starts[list];
      int end = endOf(list);
      selectedBytes += end - start;
      if ((scored != null && scored[list]) || find(start, end) != null) {
        continue;
      }

      missingBytes += end - start;
      if (runEnd >= 0
          && (long) runEnd + mergeGapBytes >= start
          && end - runStart <= MAX_CACHED_BYTES) {
        runEnd = end;
      } else {
        if (runStart >= 0) {
          planned.put(runStart, runEnd);
        }

        runStart = start;
        runEnd = end;
      }
    }

    if (runStart >= 0) {
      planned.put(runStart, runEnd);
    }

    // Reuse the loaded metadata when a dense probe can read the list area in one request.
    // During expansion, require substantial remaining I/O before re-reading scored ranges.
    int listAreaBytes = input.length() - starts[0];
    boolean mostlyUnread = 4 * missingBytes >= 3L * listAreaBytes;
    boolean scatteredExpansion =
        4 * selectedBytes >= 3L * listAreaBytes
            && 2 * missingBytes >= listAreaBytes
            && planned.size() >= MIN_SCATTERED_RANGES;
    if (mostlyUnread || scatteredExpansion) {
      planned.clear();
      lru.clear();
      cachedByOffset.clear();
      cachedBytes = 0;
      wholeLists = input.read(starts[0], listAreaBytes);
      wholeListAreaReads += 1;
    }
  }

  VectorPayloadReader read(int list) {
    int start = starts[list];
    int end = endOf(list);
    if (wholeLists != null) {
      return window(wholeLists, starts[0], start, end);
    }

    Map.Entry<Integer, byte[]> cached = find(start, end);
    if (cached != null) {
      byte[] bytes = lru.get(cached.getKey());
      return window(bytes, cached.getKey(), start, end);
    }

    // a later search of the same file wants exactly this list, so the shared cache is asked
    // before the plan widens the read to cover neighbours
    if (blocks != null) {
      byte[] shared = blocks.get(input.location(), start, end - start);
      if (shared != null) {
        blockHits++;
        return window(shared, start, start, end);
      }

      blockMisses++;
    }

    Map.Entry<Integer, Integer> plan = planned.floorEntry(start);
    int readStart = start;
    int readEnd = end;
    if (plan != null && end <= plan.getValue()) {
      readStart = plan.getKey();
      readEnd = plan.getValue();
    }

    int size = readEnd - readStart;
    if (size > MAX_CACHED_BYTES) {
      return window(input.read(readStart, size), readStart, start, end);
    }

    while (cachedBytes + size > MAX_CACHED_BYTES || lru.size() >= MAX_CACHED_RANGES) {
      Integer oldest = lru.keySet().iterator().next();
      cachedBytes -= lru.remove(oldest).length;
      cachedByOffset.remove(oldest);
    }

    byte[] bytes = input.read(readStart, size);
    lru.put(readStart, bytes);
    cachedByOffset.put(readStart, bytes);
    cachedBytes += size;
    if (blocks != null) {
      keepCoveredLists(readStart, readEnd, bytes);
    }

    return window(bytes, readStart, start, end);
  }

  /**
   * Offers every list the read covers to the shared cache, not only the one that asked for it.
   *
   * <p>A range read can carry several adjacent lists; caching only the requested one would make the
   * next search read the same bytes again for its neighbour. The block of a list is a view of the
   * bytes just read, so a cache that keeps it holds a window of one array rather than a copy of the
   * whole read.
   */
  private void keepCoveredLists(int readStart, int readEnd, byte[] bytes) {
    int list = listAtOrAfter(readStart);
    while (list < starts.length && starts[list] < readEnd) {
      int start = starts[list];
      int end = endOf(list);
      if (start >= readStart && end <= readEnd) {
        int from = start - readStart;
        int length = end - start;
        byte[] block =
            from == 0 && length == bytes.length
                ? bytes
                : java.util.Arrays.copyOfRange(bytes, from, from + length);
        blocks.put(input.location(), start, block);
        blockLoads++;
      }

      list++;
    }
  }

  private int listAtOrAfter(int offset) {
    int found = java.util.Arrays.binarySearch(starts, offset);
    return found >= 0 ? found : -(found + 1);
  }

  private Map.Entry<Integer, byte[]> find(int start, int end) {
    Map.Entry<Integer, byte[]> candidate = cachedByOffset.floorEntry(start);
    return candidate != null && end - candidate.getKey() <= candidate.getValue().length
        ? candidate
        : null;
  }

  private int endOf(int list) {
    return list + 1 == starts.length ? input.length() : starts[list + 1];
  }

  private static VectorPayloadReader window(byte[] bytes, int base, int start, int end) {
    return new VectorPayloadReader(bytes, start - base, end - base);
  }

  RangeReadMetrics metrics() {
    RangeReadMetrics total = input.metrics();
    return new RangeReadMetrics(
        total.nanos() - metadataReads.nanos(),
        total.bytes() - metadataReads.bytes(),
        total.requests() - metadataReads.requests());
  }

  /** How many times the selected ranges were replaced by one read of the whole list area. */
  int wholeListAreaReads() {
    return wholeListAreaReads;
  }

  /**
   * What the shared cache did for this searcher, and what it holds, or empty when there is none.
   */
  ListBlockCache.Stats blockStats() {
    if (blocks == null) {
      return ListBlockCache.Stats.EMPTY;
    }

    ListBlockCache.Stats cache = blocks.stats();
    return new ListBlockCache.Stats(
        blockHits, blockMisses, blockLoads, cache.evictions(), cache.bytes());
  }

  @Override
  public void close() {
    input.close();
  }
}
