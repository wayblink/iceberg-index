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

import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * One IVF index shard, held in memory: the vectors one index file carries and the rows they point
 * at.
 *
 * <p>A production shard stores quantized codes and is read by range; this prototype keeps the
 * vectors themselves so that a routed search can be compared with the exact scan. An entry is
 * identified by its shard and its index in the shard; that pair stands in for the {@code (data
 * file, row position)} pair the index records, which is what fetch-back resolves.
 *
 * <p>The vectors are referenced rather than copied, because a test builds a shard from the rows it
 * already holds. A caller must not mutate them after the router has been trained on them.
 */
final class IvfShard {

  private final String location;
  private final float[][] vectors;

  private IvfShard(String location, float[][] vectors) {
    this.location = location;
    this.vectors = vectors;
  }

  /** A shard at {@code location} holding {@code vectors}, one entry per vector. */
  static IvfShard of(String location, float[][] vectors) {
    Preconditions.checkArgument(location != null && !location.isEmpty(), "Shard location is empty");
    Preconditions.checkNotNull(vectors, "Shard vectors are null");
    for (float[] vector : vectors) {
      Preconditions.checkNotNull(vector, "Shard entry vector is null");
    }

    return new IvfShard(location, vectors);
  }

  String location() {
    return location;
  }

  int entryCount() {
    return vectors.length;
  }

  float[] vector(int entry) {
    return vectors[entry];
  }

  float[][] vectors() {
    return vectors;
  }

  /** The row position the entry points at, which in the prototype is its index in the shard. */
  long rowPosition(int entry) {
    Preconditions.checkArgument(
        entry >= 0 && entry < vectors.length,
        "Entry %s is outside %s entries",
        entry,
        entryCount());
    return entry;
  }
}
