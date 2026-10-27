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
package org.apache.iceberg.index.spark;

import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * The size one index file's payload may have, on both sides of the build.
 *
 * <p>A query reads a payload into a single array, so a file larger than that can never be searched.
 * Checking it while the payload is built fails the build instead of publishing an index whose files
 * every query would reject, and the message names the property that decides how much a file holds.
 */
final class IndexPayloadLimits {
  /** The most bytes a payload can be read into: one array's length. */
  static final long MAX_PAYLOAD_BYTES = Integer.MAX_VALUE;

  /**
   * Leaves room for a table where estimates and reality differ a little: a budget that just fits is
   * one entry away from not fitting.
   */
  private static final double SUGGESTION_MARGIN = 0.9;

  static void checkFits(long payloadBytes, long entries, String location) {
    Preconditions.checkArgument(
        payloadBytes <= MAX_PAYLOAD_BYTES,
        "Index file %s would hold %s bytes, more than the %s a payload can be read into. "
            + "Build it with a smaller %s%s",
        location,
        payloadBytes,
        MAX_PAYLOAD_BYTES,
        org.apache.iceberg.index.IndexMetadata.MAX_ENTRIES_PER_INDEX_FILE_PROPERTY,
        suggestion(entries, payloadBytes));
  }

  /** The same message for the read side, where the payload is already written. */
  static String tooLargeToRead(long payloadBytes, String location) {
    return String.format(
        "Index file %s is %s bytes, more than the %s a payload can be read into; rebuild the index "
            + "with a smaller %s, or check whether this file was built by a version without that "
            + "check",
        location,
        payloadBytes,
        MAX_PAYLOAD_BYTES,
        org.apache.iceberg.index.IndexMetadata.MAX_ENTRIES_PER_INDEX_FILE_PROPERTY);
  }

  private static String suggestion(long entries, long payloadBytes) {
    if (entries <= 0 || payloadBytes <= 0) {
      return "";
    }

    long bytesPerEntry = Math.max(1L, payloadBytes / entries);
    long entriesThatFit = (long) (MAX_PAYLOAD_BYTES * SUGGESTION_MARGIN) / bytesPerEntry;
    return String.format(
        " -- this file holds %s entries at about %s bytes each, so %s entries per index file or "
            + "fewer keeps every file under the limit",
        entries, bytesPerEntry, entriesThatFit);
  }

  private IndexPayloadLimits() {}
}
