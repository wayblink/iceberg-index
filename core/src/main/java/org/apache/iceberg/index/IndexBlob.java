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
 * A blob stored inside an index file container: an opaque, type-tagged payload.
 *
 * <p>Blob types are open strings owned by index types, for example {@code
 * iceberg-vector-entries-v1}. Readers dispatch on the blob type, and unknown types can be skipped
 * so that an engine can still use the index files it understands.
 */
public final class IndexBlob {

  private final String type;
  private final byte[] payload;

  private IndexBlob(String type, byte[] payload) {
    Preconditions.checkArgument(type != null && !type.isEmpty(), "Blob type is empty");
    Preconditions.checkNotNull(payload, "Blob payload is null");
    this.type = type;
    this.payload = payload.clone();
  }

  public static IndexBlob of(String type, byte[] payload) {
    return new IndexBlob(type, payload);
  }

  public String type() {
    return type;
  }

  /** Returns a copy so callers cannot mutate the blob payload. */
  public byte[] payload() {
    return payload.clone();
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof IndexBlob)) {
      return false;
    }
    IndexBlob that = (IndexBlob) other;
    return type.equals(that.type) && Arrays.equals(payload, that.payload);
  }

  @Override
  public int hashCode() {
    int result = type.hashCode();
    result = 31 * result + Arrays.hashCode(payload);
    return result;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("type", type)
        .add("payloadSize", payload.length)
        .toString();
  }
}
