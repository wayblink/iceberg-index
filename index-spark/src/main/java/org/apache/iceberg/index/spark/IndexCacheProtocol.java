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

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.index.IndexDefinition;
import org.apache.iceberg.index.IndexFileMemory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/** Immutable command/report messages exchanged between the driver and executors. */
final class IndexCacheProtocol {
  private IndexCacheProtocol() {}

  enum CommandType {
    LOAD,
    RELEASE
  }

  enum AckStatus {
    LOADED,
    RELEASED,
    REJECTED,
    FAILED
  }

  enum Status {
    QUEUED,
    PENDING,
    COMPLETE,
    PARTIAL,
    FAILED,
    RELEASING,
    OFFLOADED,
    CANCELLED
  }

  static final class File implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String location;
    private final String cacheKey;
    private final String kind;
    private final String blobType;
    // ByteBuffer is not serializable; the payload travels as a byte array and is wrapped on read.
    private final byte[] payload;
    private final Long estimatedBytes;
    private final Memory memory;

    private File(Builder builder) {
      Preconditions.checkArgument(
          builder.location != null && !builder.location.isEmpty(),
          "Index file location is missing");
      Preconditions.checkArgument(
          builder.cacheKey != null && !builder.cacheKey.isEmpty(), "Index cache key is missing");
      Preconditions.checkArgument(
          builder.estimatedBytes == null || builder.estimatedBytes > 0,
          "Invalid estimated bytes: %s",
          builder.estimatedBytes);
      if (builder.memory != null) {
        Preconditions.checkArgument(
            builder.estimatedBytes == null
                || builder.estimatedBytes.equals(
                    IndexCacheMemory.residentBytes(builder.memory.toIndexFileMemory())),
            "Estimated bytes do not match the declared index memory");
      }

      this.location = builder.location;
      this.cacheKey = builder.cacheKey;
      this.kind = builder.kind;
      this.blobType = builder.blobType;
      this.payload = builder.payload == null ? null : copyOfRemaining(builder.payload);
      this.estimatedBytes = builder.estimatedBytes;
      this.memory = builder.memory;
    }

    static Builder builder() {
      return new Builder();
    }

    String location() {
      return location;
    }

    String cacheKey() {
      return cacheKey;
    }

    String kind() {
      return kind;
    }

    String blobType() {
      return blobType;
    }

    ByteBuffer payload() {
      return payload == null ? null : ByteBuffer.wrap(payload).asReadOnlyBuffer();
    }

    Long estimatedBytes() {
      return estimatedBytes;
    }

    /** Rebuilds the engine-agnostic estimate carried by this message. */
    IndexFileMemory memory() {
      return memory == null ? null : memory.toIndexFileMemory();
    }

    static final class Builder {
      private String location;
      private String cacheKey;
      private String kind;
      private String blobType;
      private ByteBuffer payload;
      private Long estimatedBytes;
      private Memory memory;

      Builder location(String value) {
        this.location = value;
        return this;
      }

      Builder cacheKey(String value) {
        this.cacheKey = value;
        return this;
      }

      Builder kind(String value) {
        this.kind = value;
        return this;
      }

      Builder blobType(String value) {
        this.blobType = value;
        return this;
      }

      Builder payload(ByteBuffer value) {
        this.payload = value;
        return this;
      }

      Builder estimatedBytes(Long value) {
        this.estimatedBytes = value;
        return this;
      }

      Builder memory(IndexFileMemory value) {
        this.memory = value == null ? null : Memory.from(value);
        return this;
      }

      File build() {
        return new File(this);
      }
    }
  }

  /**
   * Serialized capacity estimate. {@link IndexFileMemory} is a core value object that is not itself
   * serializable, and executors need the estimate without a driver round trip.
   */
  static final class Memory implements Serializable {
    private static final long serialVersionUID = 1L;
    private final Long fileBytes;
    private final long payloadBytes;
    private final long preparedBytes;
    private final int modelVersion;
    private final String readerId;
    private final String runtimeProfile;
    private final Map<String, Long> structure;

    private Memory(IndexFileMemory memory) {
      this.fileBytes = memory.fileBytes();
      this.payloadBytes = memory.payloadBytes();
      this.preparedBytes = memory.preparedBytes();
      this.modelVersion = memory.modelVersion();
      this.readerId = memory.readerId();
      this.runtimeProfile = memory.runtimeProfile();
      this.structure = immutableMap(memory.structure());
    }

    static Memory from(IndexFileMemory memory) {
      return new Memory(memory);
    }

    IndexFileMemory toIndexFileMemory() {
      return IndexFileMemory.builder()
          .fileBytes(fileBytes)
          .payloadBytes(payloadBytes)
          .preparedBytes(preparedBytes)
          .modelVersion(modelVersion)
          .readerId(readerId)
          .runtimeProfile(runtimeProfile)
          .structure(structure)
          .build();
    }
  }

  static final class Target implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String indexUuid;
    private final long snapshotId;
    private final List<File> files;
    private final IndexDefinition definition;
    private final FileIO io;

    private Target(Builder builder) {
      Preconditions.checkArgument(
          builder.indexUuid != null && !builder.indexUuid.isEmpty(), "Index identity is missing");
      Preconditions.checkArgument(builder.files != null, "Index files are missing");
      Set<String> locations = new HashSet<>();
      Set<String> cacheKeys = new HashSet<>();
      for (File file : builder.files) {
        Preconditions.checkArgument(file != null, "Index file is null");
        Preconditions.checkArgument(
            locations.add(file.location()), "Duplicate index file: %s", file.location());
        Preconditions.checkArgument(
            cacheKeys.add(file.cacheKey()), "Duplicate index cache key: %s", file.cacheKey());
      }

      this.indexUuid = builder.indexUuid;
      this.snapshotId = builder.snapshotId;
      this.files = List.copyOf(builder.files);
      this.definition = builder.definition;
      this.io = builder.io;
    }

    static Builder builder() {
      return new Builder();
    }

    String indexUuid() {
      return indexUuid;
    }

    long snapshotId() {
      return snapshotId;
    }

    /**
     * Files this message considered. In a planner result these are the candidates, including the
     * rejected ones; in a reply they are the files a command should act on. Neither is a claim that
     * a replica is resident.
     */
    List<File> files() {
      return files;
    }

    /** Read context; a target without it can be planned but never executed. */
    boolean hasReadContext() {
      return definition != null && io != null;
    }

    IndexDefinition definition() {
      return definition;
    }

    FileIO io() {
      return io;
    }

    static final class Builder {
      private String indexUuid;
      private long snapshotId;
      private List<File> files = List.of();
      private IndexDefinition definition;
      private FileIO io;

      Builder indexUuid(String value) {
        this.indexUuid = value;
        return this;
      }

      Builder snapshotId(long value) {
        this.snapshotId = value;
        return this;
      }

      Builder files(List<File> value) {
        this.files = value;
        return this;
      }

      Builder definition(IndexDefinition value) {
        this.definition = value;
        return this;
      }

      Builder io(FileIO value) {
        this.io = value;
        return this;
      }

      Target build() {
        return new Target(this);
      }
    }
  }

  static final class NodeReport implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String session;
    private final String executorId;
    private final String incarnation;
    private final long sequence;
    private final long budgetBytes;
    private final long usedBytes;
    private final long reservedBytes;
    private final long releasingBytes;
    private final Map<String, Long> holders;
    private final Map<String, Long> reservedByPlan;
    private final Map<String, Long> reservedByCommand;
    private final String profile;
    private final Map<String, Long> accessAgeNanos;

    private NodeReport(Builder builder) {
      Preconditions.checkArgument(
          builder.session != null && !builder.session.isEmpty(), "Application session is missing");
      Preconditions.checkArgument(
          builder.executorId != null && !builder.executorId.isEmpty(),
          "Executor identity is missing");
      Preconditions.checkArgument(
          builder.incarnation != null && !builder.incarnation.isEmpty(),
          "Executor incarnation is missing");
      Preconditions.checkArgument(
          builder.sequence >= 0, "Invalid report sequence: %s", builder.sequence);
      Preconditions.checkArgument(
          builder.budgetBytes >= 0, "Invalid executor budget: %s", builder.budgetBytes);
      Preconditions.checkArgument(
          builder.usedBytes >= 0, "Invalid used bytes: %s", builder.usedBytes);
      Preconditions.checkArgument(
          builder.reservedBytes >= 0, "Invalid reserved bytes: %s", builder.reservedBytes);
      Preconditions.checkArgument(
          builder.releasingBytes >= 0 && builder.releasingBytes <= builder.usedBytes,
          "Releasing bytes are not a subset of used bytes: %s / %s",
          builder.releasingBytes,
          builder.usedBytes);
      Preconditions.checkArgument(
          builder.reservedBytes <= Long.MAX_VALUE - builder.usedBytes,
          "Executor occupancy overflows: %s + %s",
          builder.usedBytes,
          builder.reservedBytes);
      this.session = builder.session;
      this.executorId = builder.executorId;
      this.incarnation = builder.incarnation;
      this.sequence = builder.sequence;
      this.budgetBytes = builder.budgetBytes;
      this.usedBytes = builder.usedBytes;
      this.reservedBytes = builder.reservedBytes;
      this.releasingBytes = builder.releasingBytes;
      this.holders = immutableMap(builder.holders);
      this.reservedByPlan = immutableMap(builder.reservedByPlan);
      this.reservedByCommand = immutableMap(builder.reservedByCommand);
      this.profile = builder.profile;
      this.accessAgeNanos = immutableMap(builder.accessAgeNanos);
    }

    static Builder builder() {
      return new Builder();
    }

    String session() {
      return session;
    }

    String executorId() {
      return executorId;
    }

    String incarnation() {
      return incarnation;
    }

    long sequence() {
      return sequence;
    }

    long budgetBytes() {
      return budgetBytes;
    }

    long usedBytes() {
      return usedBytes;
    }

    long reservedBytes() {
      return reservedBytes;
    }

    // Used bytes include retained files awaiting release; releasing is a diagnostic subset.
    long releasingBytes() {
      return releasingBytes;
    }

    // A report that was serialized before these fields existed deserializes without running the
    // constructor, so each map answers empty rather than null: the fields are additive, and a
    // reader
    // of an older message has nothing to attribute.
    Map<String, Long> holders() {
      return holders == null ? Map.of() : holders;
    }

    Map<String, Long> reservedByPlan() {
      return reservedByPlan == null ? Map.of() : reservedByPlan;
    }

    Map<String, Long> reservedByCommand() {
      return reservedByCommand == null ? Map.of() : reservedByCommand;
    }

    String profile() {
      return profile;
    }

    /** Relative age of the last recorded search access per index, or absent when never accessed. */
    Map<String, Long> accessAgeNanos() {
      return accessAgeNanos;
    }

    /** Orders reports of the same executor so older reports cannot overwrite newer state. */
    long accessSequence() {
      return sequence;
    }

    long reservedBytesFor(String planId) {
      return reservedByPlan.getOrDefault(planId, 0L);
    }

    Builder copy() {
      return builder()
          .session(session)
          .executorId(executorId)
          .incarnation(incarnation)
          .sequence(sequence)
          .budgetBytes(budgetBytes)
          .usedBytes(usedBytes)
          .reservedBytes(reservedBytes)
          .releasingBytes(releasingBytes)
          .holders(holders)
          .reservedByPlan(reservedByPlan)
          .reservedByCommand(reservedByCommand())
          .profile(profile)
          .accessAgeNanos(accessAgeNanos);
    }

    static final class Builder {
      private String session;
      private String executorId;
      private String incarnation;
      private long sequence;
      private long budgetBytes;
      private long usedBytes;
      private long reservedBytes;
      private long releasingBytes;
      private Map<String, Long> holders = Map.of();
      private Map<String, Long> reservedByPlan = Map.of();
      private Map<String, Long> reservedByCommand = Map.of();
      private String profile;
      private Map<String, Long> accessAgeNanos = Map.of();

      Builder session(String value) {
        this.session = value;
        return this;
      }

      Builder executorId(String value) {
        this.executorId = value;
        return this;
      }

      Builder incarnation(String value) {
        this.incarnation = value;
        return this;
      }

      Builder sequence(long value) {
        this.sequence = value;
        return this;
      }

      Builder budgetBytes(long value) {
        this.budgetBytes = value;
        return this;
      }

      Builder usedBytes(long value) {
        this.usedBytes = value;
        return this;
      }

      Builder reservedBytes(long value) {
        this.reservedBytes = value;
        return this;
      }

      Builder releasingBytes(long value) {
        this.releasingBytes = value;
        return this;
      }

      Builder holders(Map<String, Long> value) {
        this.holders = value;
        return this;
      }

      Builder reservedByPlan(Map<String, Long> value) {
        this.reservedByPlan = value;
        return this;
      }

      Builder reservedByCommand(Map<String, Long> value) {
        this.reservedByCommand = value;
        return this;
      }

      Builder profile(String value) {
        this.profile = value;
        return this;
      }

      Builder accessAgeNanos(Map<String, Long> value) {
        this.accessAgeNanos = value;
        return this;
      }

      NodeReport build() {
        return new NodeReport(this);
      }
    }
  }

  static final class Command implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String session;
    private final String executorId;
    private final String incarnation;
    private final String indexUuid;
    private final long epoch;
    private final String planId;
    private final String commandId;
    private final CommandType type;
    private final String mode;
    private final File file;
    private final IndexDefinition definition;
    private final FileIO io;
    private final long sequence;

    private Command(Builder builder) {
      Preconditions.checkArgument(builder.session != null, "Command session is missing");
      Preconditions.checkArgument(builder.executorId != null, "Command executor is missing");
      Preconditions.checkArgument(builder.incarnation != null, "Command incarnation is missing");
      Preconditions.checkArgument(builder.indexUuid != null, "Command index is missing");
      Preconditions.checkArgument(
          builder.epoch >= 0, "Command epoch is invalid: %s", builder.epoch);
      Preconditions.checkArgument(builder.planId != null, "Command plan is missing");
      Preconditions.checkArgument(builder.commandId != null, "Command identity is missing");
      Preconditions.checkArgument(builder.type != null, "Command type is missing");
      Preconditions.checkArgument(builder.file != null, "Command file is missing");
      if (builder.type == CommandType.LOAD) {
        Preconditions.checkArgument(
            builder.definition != null,
            "Command definition is missing: an executor cannot decode the file without it");
        Preconditions.checkArgument(
            builder.io != null,
            "Command FileIO is missing: an executor cannot read the file without a read context");
      }
      this.session = builder.session;
      this.executorId = builder.executorId;
      this.incarnation = builder.incarnation;
      this.indexUuid = builder.indexUuid;
      this.epoch = builder.epoch;
      this.planId = builder.planId;
      this.commandId = builder.commandId;
      this.type = builder.type;
      this.mode = builder.mode;
      this.file = builder.file;
      this.definition = builder.definition;
      this.io = builder.io;
      this.sequence = builder.sequence;
    }

    static Builder builder() {
      return new Builder();
    }

    String session() {
      return session;
    }

    String executorId() {
      return executorId;
    }

    String incarnation() {
      return incarnation;
    }

    String indexUuid() {
      return indexUuid;
    }

    long epoch() {
      return epoch;
    }

    String planId() {
      return planId;
    }

    String commandId() {
      return commandId;
    }

    CommandType type() {
      return type;
    }

    /**
     * Routing scope for the executor, for example {@code LOAD}, {@code EVICT}, or {@code ORPHAN}
     * for a tombstone releasing a copy no plan owns. It is never a protection or ownership signal.
     */
    String mode() {
      return mode;
    }

    File file() {
      return file;
    }

    long sequence() {
      return sequence;
    }

    /** Read context for a LOAD; never logged or copied into a status report. */
    IndexDefinition definition() {
      return definition;
    }

    /** Read context for a LOAD; the executor reads storage directly and never returns payloads. */
    FileIO io() {
      return io;
    }

    static final class Builder {
      private String session;
      private String executorId;
      private String incarnation;
      private String indexUuid;
      private long epoch;
      private String planId;
      private String commandId;
      private CommandType type;
      private String mode;
      private File file;
      private IndexDefinition definition;
      private FileIO io;
      private long sequence;

      Builder session(String value) {
        this.session = value;
        return this;
      }

      Builder executorId(String value) {
        this.executorId = value;
        return this;
      }

      Builder incarnation(String value) {
        this.incarnation = value;
        return this;
      }

      Builder indexUuid(String value) {
        this.indexUuid = value;
        return this;
      }

      Builder epoch(long value) {
        this.epoch = value;
        return this;
      }

      Builder planId(String value) {
        this.planId = value;
        return this;
      }

      Builder commandId(String value) {
        this.commandId = value;
        return this;
      }

      Builder type(CommandType value) {
        this.type = value;
        return this;
      }

      Builder mode(String value) {
        this.mode = value;
        return this;
      }

      Builder file(File value) {
        this.file = value;
        return this;
      }

      Builder definition(IndexDefinition value) {
        this.definition = value;
        return this;
      }

      Builder io(FileIO value) {
        this.io = value;
        return this;
      }

      Builder sequence(long value) {
        this.sequence = value;
        return this;
      }

      Command build() {
        return new Command(this);
      }
    }
  }

  static final class Ack implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String session;
    private final String planId;
    private final String commandId;
    private final String executorId;
    private final String incarnation;
    private final long epoch;
    private final long sequence;
    private final AckStatus status;
    private final long bytes;
    private final String reason;

    private Ack(Builder builder) {
      Preconditions.checkArgument(builder.session != null, "ACK session is missing");
      Preconditions.checkArgument(builder.planId != null, "ACK plan is missing");
      Preconditions.checkArgument(builder.commandId != null, "ACK command is missing");
      Preconditions.checkArgument(builder.executorId != null, "ACK executor is missing");
      Preconditions.checkArgument(builder.incarnation != null, "ACK incarnation is missing");
      Preconditions.checkArgument(builder.epoch >= 0, "ACK epoch is invalid: %s", builder.epoch);
      Preconditions.checkArgument(
          builder.sequence >= 0, "ACK sequence is invalid: %s", builder.sequence);
      Preconditions.checkArgument(builder.status != null, "ACK status is missing");
      Preconditions.checkArgument(builder.bytes >= 0, "ACK bytes are negative: %s", builder.bytes);
      this.session = builder.session;
      this.planId = builder.planId;
      this.commandId = builder.commandId;
      this.executorId = builder.executorId;
      this.incarnation = builder.incarnation;
      this.epoch = builder.epoch;
      this.sequence = builder.sequence;
      this.status = builder.status;
      this.bytes = builder.bytes;
      this.reason = builder.reason;
    }

    static Builder builder() {
      return new Builder();
    }

    String session() {
      return session;
    }

    String planId() {
      return planId;
    }

    String commandId() {
      return commandId;
    }

    String executorId() {
      return executorId;
    }

    String incarnation() {
      return incarnation;
    }

    long epoch() {
      return epoch;
    }

    long sequence() {
      return sequence;
    }

    AckStatus status() {
      return status;
    }

    long bytes() {
      return bytes;
    }

    String reason() {
      return reason;
    }

    static final class Builder {
      private String session;
      private String planId;
      private String commandId;
      private String executorId;
      private String incarnation;
      private long epoch;
      private long sequence;
      private AckStatus status;
      private long bytes;
      private String reason;

      Builder session(String value) {
        this.session = value;
        return this;
      }

      Builder planId(String value) {
        this.planId = value;
        return this;
      }

      Builder commandId(String value) {
        this.commandId = value;
        return this;
      }

      Builder executorId(String value) {
        this.executorId = value;
        return this;
      }

      Builder incarnation(String value) {
        this.incarnation = value;
        return this;
      }

      Builder epoch(long value) {
        this.epoch = value;
        return this;
      }

      Builder sequence(long value) {
        this.sequence = value;
        return this;
      }

      Builder status(AckStatus value) {
        this.status = value;
        return this;
      }

      Builder bytes(long value) {
        this.bytes = value;
        return this;
      }

      Builder reason(String value) {
        this.reason = value;
        return this;
      }

      Ack build() {
        return new Ack(this);
      }
    }
  }

  static final class Reply implements Serializable {
    private static final long serialVersionUID = 1L;
    private final List<Command> commands;
    private final long nextSequence;

    private Reply(Builder builder) {
      this.commands = List.copyOf(builder.commands);
      this.nextSequence = builder.nextSequence;
    }

    static Builder builder() {
      return new Builder();
    }

    List<Command> commands() {
      return commands;
    }

    long nextSequence() {
      return nextSequence;
    }

    static final class Builder {
      private List<Command> commands = List.of();
      private long nextSequence;

      Builder commands(List<Command> value) {
        this.commands = value;
        return this;
      }

      Builder nextSequence(long value) {
        this.nextSequence = value;
        return this;
      }

      Reply build() {
        return new Reply(this);
      }
    }
  }

  static final class Plan implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String planId;
    private final String indexUuid;
    private final long snapshotId;
    private final long epoch;
    private final Map<String, String> placements;
    private final Map<String, String> rejected;
    private final List<File> files;

    private Plan(Builder builder) {
      Preconditions.checkArgument(
          builder.indexUuid != null && !builder.indexUuid.isEmpty(),
          "Plan index identity is missing");
      this.planId = builder.planId;
      this.indexUuid = builder.indexUuid;
      this.snapshotId = builder.snapshotId;
      this.epoch = builder.epoch;
      this.placements = immutableMap(builder.placements);
      this.rejected = immutableMap(builder.rejected);
      this.files = List.copyOf(builder.files);
    }

    static Builder builder() {
      return new Builder();
    }

    String planId() {
      return planId;
    }

    String indexUuid() {
      return indexUuid;
    }

    long snapshotId() {
      return snapshotId;
    }

    long epoch() {
      return epoch;
    }

    Map<String, String> placements() {
      return placements;
    }

    Map<String, String> rejected() {
      return rejected;
    }

    List<File> files() {
      return files;
    }

    static final class Builder {
      private String planId;
      private String indexUuid;
      private long snapshotId;
      private long epoch;
      private Map<String, String> placements = Map.of();
      private Map<String, String> rejected = Map.of();
      private List<File> files = List.of();

      Builder planId(String value) {
        this.planId = value;
        return this;
      }

      Builder indexUuid(String value) {
        this.indexUuid = value;
        return this;
      }

      Builder snapshotId(long value) {
        this.snapshotId = value;
        return this;
      }

      Builder epoch(long value) {
        this.epoch = value;
        return this;
      }

      Builder placements(Map<String, String> value) {
        this.placements = value;
        return this;
      }

      Builder rejected(Map<String, String> value) {
        this.rejected = value;
        return this;
      }

      Builder files(List<File> value) {
        this.files = value;
        return this;
      }

      Plan build() {
        return new Plan(this);
      }
    }
  }

  /** Protection summary of one index; permanent protection never expires. */
  static final class Protection implements Serializable {
    private static final long serialVersionUID = 1L;
    private final boolean permanent;
    private final String text;
    private final long deadlineNanos;

    private Protection(Builder builder) {
      Preconditions.checkArgument(builder.text != null, "Protection text is missing");
      Preconditions.checkArgument(
          builder.deadlineNanos >= 0, "Invalid protection deadline: %s", builder.deadlineNanos);
      this.permanent = builder.permanent;
      this.text = builder.text;
      this.deadlineNanos = builder.deadlineNanos;
    }

    static Builder builder() {
      return new Builder();
    }

    boolean permanent() {
      return permanent;
    }

    String text() {
      return text;
    }

    /** Finite deadline; zero when permanent protection never expires. */
    long deadlineNanos() {
      return deadlineNanos;
    }

    static final class Builder {
      private boolean permanent;
      private String text;
      private long deadlineNanos;

      Builder permanent(boolean value) {
        this.permanent = value;
        return this;
      }

      Builder text(String value) {
        this.text = value;
        return this;
      }

      Builder deadlineNanos(long value) {
        this.deadlineNanos = value;
        return this;
      }

      Protection build() {
        return new Protection(this);
      }
    }
  }

  static final class Result implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String planId;
    private final String indexUuid;
    private final long snapshotId;
    private final long epoch;
    private final Status status;
    private final Protection protection;
    private final List<String> targetFiles;
    private final Map<String, String> uncovered;
    private final long residentBytes;
    private final List<String> evictedIndexes;
    private final List<String> reusedFiles;
    private final Map<String, String> migrations;
    private final Map<String, String> releasePending;
    private final Map<String, String> migrationNotices;

    private Result(Builder builder) {
      Preconditions.checkArgument(builder.planId != null, "Result plan is missing");
      Preconditions.checkArgument(builder.status != null, "Result status is missing");
      this.planId = builder.planId;
      this.indexUuid = builder.indexUuid;
      this.snapshotId = builder.snapshotId;
      this.epoch = builder.epoch;
      this.status = builder.status;
      this.protection = builder.protection;
      this.targetFiles = List.copyOf(builder.targetFiles);
      this.uncovered = immutableMap(builder.uncovered);
      this.residentBytes = builder.residentBytes;
      this.evictedIndexes = List.copyOf(builder.evictedIndexes);
      this.reusedFiles = List.copyOf(builder.reusedFiles);
      this.migrations = immutableMap(builder.migrations);
      this.releasePending = immutableMap(builder.releasePending);
      this.migrationNotices = immutableMap(builder.migrationNotices);
    }

    static Builder builder() {
      return new Builder();
    }

    String planId() {
      return planId;
    }

    String indexUuid() {
      return indexUuid;
    }

    long snapshotId() {
      return snapshotId;
    }

    long epoch() {
      return epoch;
    }

    Status status() {
      return status;
    }

    Protection protection() {
      return protection;
    }

    List<String> targetFiles() {
      return targetFiles;
    }

    Map<String, String> uncovered() {
      return uncovered;
    }

    long residentBytes() {
      return residentBytes;
    }

    List<String> evictedIndexes() {
      return evictedIndexes;
    }

    /** Files this plan reused from an already confirmed copy instead of loading again. */
    List<String> reusedFiles() {
      return reusedFiles;
    }

    /** Planned migrations: file location to the executor that receives the second copy first. */
    Map<String, String> migrations() {
      return migrations;
    }

    /** Copies this offload has asked an executor to release and has not seen acknowledged. */
    Map<String, String> releasePending() {
      return releasePending;
    }

    /** Files that could not be rebalanced, with the reason, for example a missing second copy. */
    Map<String, String> migrationNotices() {
      return migrationNotices;
    }

    static final class Builder {
      private String planId;
      private String indexUuid;
      private long snapshotId;
      private long epoch;
      private Status status;
      private Protection protection;
      private List<String> targetFiles = List.of();
      private Map<String, String> uncovered = Map.of();
      private long residentBytes;
      private List<String> evictedIndexes = List.of();
      private List<String> reusedFiles = List.of();
      private Map<String, String> migrations = Map.of();
      private Map<String, String> releasePending = Map.of();
      private Map<String, String> migrationNotices = Map.of();

      Builder planId(String value) {
        this.planId = value;
        return this;
      }

      Builder indexUuid(String value) {
        this.indexUuid = value;
        return this;
      }

      Builder snapshotId(long value) {
        this.snapshotId = value;
        return this;
      }

      Builder epoch(long value) {
        this.epoch = value;
        return this;
      }

      Builder status(Status value) {
        this.status = value;
        return this;
      }

      Builder protection(Protection value) {
        this.protection = value;
        return this;
      }

      Builder targetFiles(List<String> value) {
        this.targetFiles = value;
        return this;
      }

      Builder uncovered(Map<String, String> value) {
        this.uncovered = value;
        return this;
      }

      Builder residentBytes(long value) {
        this.residentBytes = value;
        return this;
      }

      Builder evictedIndexes(List<String> value) {
        this.evictedIndexes = value;
        return this;
      }

      Builder reusedFiles(List<String> value) {
        this.reusedFiles = value;
        return this;
      }

      Builder migrations(Map<String, String> value) {
        this.migrations = value;
        return this;
      }

      Builder releasePending(Map<String, String> value) {
        this.releasePending = value;
        return this;
      }

      Builder migrationNotices(Map<String, String> value) {
        this.migrationNotices = value;
        return this;
      }

      Result build() {
        return new Result(this);
      }
    }
  }

  private static byte[] copyOfRemaining(ByteBuffer buffer) {
    ByteBuffer window = buffer.duplicate();
    byte[] bytes = new byte[window.remaining()];
    window.get(bytes);
    return bytes;
  }

  private static <K, V> Map<K, V> immutableMap(Map<K, V> values) {
    Preconditions.checkArgument(values != null, "Protocol map is null");
    return Collections.unmodifiableMap(new LinkedHashMap<>(values));
  }
}
