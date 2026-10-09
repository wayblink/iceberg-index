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
package org.apache.iceberg.index;

import java.util.Arrays;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * An immutable index entry: a reference to a source table row plus a type-specific payload.
 *
 * <p>The row reference ({@code dataFile} + {@code rowPosition}) uses the same {@code _file + _pos}
 * anchor as Iceberg manifest entries, so any engine can resolve a search result back to a table
 * row. The payload bytes are opaque to the generic layer and are encoded by the {@link
 * IndexAdapter} of the index type.
 */
public final class IndexEntry {

  private final String dataFile;
  private final long rowPosition;
  private final byte[] payload;

  private IndexEntry(String dataFile, long rowPosition, byte[] payload) {
    Preconditions.checkArgument(
        dataFile != null && !dataFile.isEmpty(), "Data file location is empty");
    Preconditions.checkArgument(rowPosition >= 0, "Invalid row position: %s", rowPosition);
    Preconditions.checkNotNull(payload, "Entry payload is null");
    this.dataFile = dataFile;
    this.rowPosition = rowPosition;
    this.payload = payload.clone();
  }

  public static IndexEntry of(String dataFile, long rowPosition, byte[] payload) {
    return new IndexEntry(dataFile, rowPosition, payload);
  }

  public String dataFile() {
    return dataFile;
  }

  public long rowPosition() {
    return rowPosition;
  }

  /** Returns a copy so callers cannot mutate the indexed payload. */
  public byte[] payload() {
    return payload.clone();
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof IndexEntry)) {
      return false;
    }
    IndexEntry that = (IndexEntry) other;
    return rowPosition == that.rowPosition
        && dataFile.equals(that.dataFile)
        && Arrays.equals(payload, that.payload);
  }

  @Override
  public int hashCode() {
    int result = dataFile.hashCode();
    result = 31 * result + Long.hashCode(rowPosition);
    result = 31 * result + Arrays.hashCode(payload);
    return result;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("dataFile", dataFile)
        .add("rowPosition", rowPosition)
        .add("payloadSize", payload.length)
        .toString();
  }
}
