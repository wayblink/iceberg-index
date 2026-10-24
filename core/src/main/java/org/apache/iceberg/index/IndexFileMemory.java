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

import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * A retained-memory estimate and the reader, runtime, and structure it describes.
 *
 * <p>Estimates exclude transient load buffers and query workspace. They are usable only with a
 * matching reader, memory model, and runtime profile.
 */
public final class IndexFileMemory {
  private final Long fileBytes;
  private final long payloadBytes;
  private final long preparedBytes;
  private final int modelVersion;
  private final String readerId;
  private final String runtimeProfile;
  private final Map<String, Long> structure;

  private IndexFileMemory(Builder builder) {
    Preconditions.checkArgument(
        builder.fileBytes == null || builder.fileBytes >= 0,
        "Invalid index file bytes: %s",
        builder.fileBytes);
    Preconditions.checkArgument(builder.payloadBytes != null, "Index payload bytes are not set");
    Preconditions.checkArgument(
        builder.payloadBytes >= 0, "Invalid index payload bytes: %s", builder.payloadBytes);
    Preconditions.checkArgument(builder.preparedBytes != null, "Index prepared bytes are not set");
    Preconditions.checkArgument(
        builder.preparedBytes >= 0, "Invalid index prepared bytes: %s", builder.preparedBytes);
    Preconditions.checkArgument(
        builder.payloadBytes <= Long.MAX_VALUE - builder.preparedBytes,
        "Index retained bytes overflow: payload=%s, prepared=%s",
        builder.payloadBytes,
        builder.preparedBytes);
    Preconditions.checkArgument(
        builder.modelVersion != null && builder.modelVersion > 0,
        "Invalid index memory model version: %s",
        builder.modelVersion);
    Preconditions.checkArgument(
        builder.readerId != null && !builder.readerId.isBlank(), "Index memory reader ID is empty");
    Preconditions.checkArgument(
        builder.runtimeProfile != null && !builder.runtimeProfile.isBlank(),
        "Index memory runtime profile is empty");
    Preconditions.checkArgument(builder.structure != null, "Index memory structure is null");
    builder.structure.forEach(
        (key, value) -> {
          Preconditions.checkArgument(
              key != null && !key.isBlank(), "Index memory structure key is empty");
          Preconditions.checkArgument(
              value != null && value >= 0,
              "Invalid index memory structure value for %s: %s",
              key,
              value);
        });
    this.fileBytes = builder.fileBytes;
    this.payloadBytes = builder.payloadBytes;
    this.preparedBytes = builder.preparedBytes;
    this.modelVersion = builder.modelVersion;
    this.readerId = builder.readerId;
    this.runtimeProfile = builder.runtimeProfile;
    this.structure = Map.copyOf(builder.structure);
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Returns the serialized container size, or null before the file has been written. */
  @Nullable
  public Long fileBytes() {
    return fileBytes;
  }

  /** Returns the retained payload-buffer estimate in bytes. */
  public long payloadBytes() {
    return payloadBytes;
  }

  /** Returns the prepared search structure estimate in bytes, excluding the payload buffer. */
  public long preparedBytes() {
    return preparedBytes;
  }

  /** Returns the positive model version, which may be unknown to a reader. */
  public int modelVersion() {
    return modelVersion;
  }

  /** Returns the payload reader identifier, including its encoding version. */
  public String readerId() {
    return readerId;
  }

  /** Returns the runtime profile assumed by the estimate. */
  public String runtimeProfile() {
    return runtimeProfile;
  }

  /** Returns structural counts used by the memory model. */
  public Map<String, Long> structure() {
    return structure;
  }

  public Builder toBuilder() {
    return builder()
        .fileBytes(fileBytes)
        .payloadBytes(payloadBytes)
        .preparedBytes(preparedBytes)
        .modelVersion(modelVersion)
        .readerId(readerId)
        .runtimeProfile(runtimeProfile)
        .structure(structure);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }

    if (!(other instanceof IndexFileMemory that)) {
      return false;
    }

    return Objects.equals(fileBytes, that.fileBytes)
        && payloadBytes == that.payloadBytes
        && preparedBytes == that.preparedBytes
        && modelVersion == that.modelVersion
        && readerId.equals(that.readerId)
        && runtimeProfile.equals(that.runtimeProfile)
        && structure.equals(that.structure);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        fileBytes, payloadBytes, preparedBytes, modelVersion, readerId, runtimeProfile, structure);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("fileBytes", fileBytes)
        .add("payloadBytes", payloadBytes)
        .add("preparedBytes", preparedBytes)
        .add("modelVersion", modelVersion)
        .add("readerId", readerId)
        .add("runtimeProfile", runtimeProfile)
        .add("structure", structure)
        .toString();
  }

  public static final class Builder {
    private Long fileBytes;
    private Long payloadBytes;
    private Long preparedBytes;
    private Integer modelVersion;
    private String readerId;
    private String runtimeProfile;
    private Map<String, Long> structure = Map.of();

    private Builder() {}

    public Builder fileBytes(@Nullable Long newFileBytes) {
      this.fileBytes = newFileBytes;
      return this;
    }

    public Builder payloadBytes(long newPayloadBytes) {
      this.payloadBytes = newPayloadBytes;
      return this;
    }

    public Builder preparedBytes(long newPreparedBytes) {
      this.preparedBytes = newPreparedBytes;
      return this;
    }

    public Builder modelVersion(int newModelVersion) {
      this.modelVersion = newModelVersion;
      return this;
    }

    public Builder readerId(String newReaderId) {
      this.readerId = newReaderId;
      return this;
    }

    public Builder runtimeProfile(String newRuntimeProfile) {
      this.runtimeProfile = newRuntimeProfile;
      return this;
    }

    public Builder structure(Map<String, Long> newStructure) {
      this.structure = newStructure;
      return this;
    }

    public IndexFileMemory build() {
      return new IndexFileMemory(this);
    }
  }
}
