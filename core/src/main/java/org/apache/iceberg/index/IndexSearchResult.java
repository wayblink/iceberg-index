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

import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * A search hit: a reference to a source table row plus a type-specific score.
 *
 * <p>The row reference uses the same {@code _file + _pos} anchor as Iceberg manifest entries, so
 * any engine can resolve the hit back to a table row for filtering, delete semantics, and final
 * projection. The interpretation of the score (similarity or distance) belongs to the {@link
 * IndexAdapter} of the index type.
 */
public final class IndexSearchResult {

  private final String dataFile;
  private final long rowPosition;
  private final double score;

  private IndexSearchResult(String dataFile, long rowPosition, double score) {
    Preconditions.checkArgument(
        dataFile != null && !dataFile.isEmpty(), "Data file location is empty");
    Preconditions.checkArgument(rowPosition >= 0, "Invalid row position: %s", rowPosition);
    Preconditions.checkArgument(Double.isFinite(score), "Score must be finite: %s", score);
    this.dataFile = dataFile;
    this.rowPosition = rowPosition;
    this.score = score;
  }

  public static IndexSearchResult of(String dataFile, long rowPosition, double score) {
    return new IndexSearchResult(dataFile, rowPosition, score);
  }

  public String dataFile() {
    return dataFile;
  }

  public long rowPosition() {
    return rowPosition;
  }

  public double score() {
    return score;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof IndexSearchResult)) {
      return false;
    }
    IndexSearchResult that = (IndexSearchResult) other;
    return rowPosition == that.rowPosition
        && Double.compare(score, that.score) == 0
        && dataFile.equals(that.dataFile);
  }

  @Override
  public int hashCode() {
    int result = dataFile.hashCode();
    result = 31 * result + Long.hashCode(rowPosition);
    result = 31 * result + Double.hashCode(score);
    return result;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("dataFile", dataFile)
        .add("rowPosition", rowPosition)
        .add("score", score)
        .toString();
  }
}
