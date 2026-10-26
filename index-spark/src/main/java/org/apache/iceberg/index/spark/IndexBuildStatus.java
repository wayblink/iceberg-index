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

import com.fasterxml.jackson.databind.JsonNode;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.index.SidecarIndexCatalog;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.apache.iceberg.util.JsonUtil;

/**
 * How far a build of one index has got, as the driver last reported it.
 *
 * <p>A build of a large table runs for tens of minutes and only its driver knows how much of it is
 * done: the thread that runs the Spark job is blocked in it, so the report has to be readable from
 * another session while it happens. That is why it is a file beside the index rather than a table
 * property: publishing progress to a table property costs a metadata commit per update, and the
 * property would also be copied into every later metadata version of the table.
 *
 * <p>The file lives at {@code <index location>/status/build-status-{0,1}.json}. A report is written
 * to the slot the next generation does not occupy and names its generation, so a reader picks the
 * higher of the two and a write that is cut off leaves the previous report intact. Nothing is ever
 * overwritten in place, which is what keeps an unparsable file from erasing the last good one.
 *
 * <p>{@link State#ABANDONED} is never written: a build whose driver died leaves {@code RUNNING}
 * behind, and the reader derives the state from {@link #updatedAtMillis()} so that a report that
 * stopped updating is not mistaken for one that is still going.
 */
public final class IndexBuildStatus {

  /** Where the report of a build stands. Only the first three are ever written. */
  public enum State {
    RUNNING,
    SUCCEEDED,
    FAILED,

    /** A build that stopped reporting, derived at read time from the age of the report. */
    ABANDONED
  }

  /** How much of a failure message is kept; the rest of a stack trace is of no use in a table. */
  public static final int MAX_ERROR_LENGTH = 500;

  static final String STATUS_DIR = "status";
  static final String SLOT_FILE_PREFIX = "build-status-";

  /** The directory that holds the reports of one index, inside the index's own tree. */
  public static String statusDirFor(String indexLocation) {
    Preconditions.checkArgument(
        indexLocation != null && !indexLocation.isEmpty(), "Invalid index location: null or empty");
    return indexLocation + "/" + STATUS_DIR;
  }

  /**
   * The directory that holds the report of the last attempt to index one column, outside any index
   * tree.
   *
   * <p>A build that fails is rolled back, registration and tree together, so a report kept inside
   * the index would be deleted by the very failure it describes. This one is keyed by the column
   * the attempt was for and survives that, which is what makes "why did it fail" answerable
   * afterwards. It is only written when an attempt fails and cleared when one succeeds.
   */
  public static String rolledBackStatusDir(String tableLocation, int fieldId) {
    Preconditions.checkArgument(
        tableLocation != null && !tableLocation.isEmpty(), "Invalid table location: null or empty");
    return tableLocation + "/" + SidecarIndexCatalog.INDEXES_DIR + "/build-status/" + fieldId;
  }

  private static final String GENERATION = "generation";
  private static final String STATE = "state";
  private static final String INDEX_UUID = "index-uuid";
  private static final String COLUMN = "column";
  private static final String SOURCE_SNAPSHOT_ID = "source-snapshot-id";
  private static final String STARTED_AT = "started-at";
  private static final String UPDATED_AT = "updated-at";
  private static final String FILES_BUILT = "files-built";
  private static final String FILES_TOTAL = "files-total";
  private static final String FILES_FAILED = "files-failed";
  private static final String ROWS = "rows";
  private static final String ERROR = "error";

  private final State state;
  private final String indexUuid;
  private final String column;
  private final long sourceSnapshotId;
  private final long startedAtMillis;
  private final long updatedAtMillis;
  private final long filesBuilt;
  private final long filesTotal;
  private final long filesFailed;
  private final long rows;
  private final String error;

  private IndexBuildStatus(
      State state,
      String indexUuid,
      String column,
      long sourceSnapshotId,
      long startedAtMillis,
      long updatedAtMillis,
      long filesBuilt,
      long filesTotal,
      long filesFailed,
      long rows,
      String error) {
    this.state = state;
    this.indexUuid = indexUuid;
    this.column = column;
    this.sourceSnapshotId = sourceSnapshotId;
    this.startedAtMillis = startedAtMillis;
    this.updatedAtMillis = updatedAtMillis;
    this.filesBuilt = filesBuilt;
    this.filesTotal = filesTotal;
    this.filesFailed = filesFailed;
    this.rows = rows;
    this.error = error;
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * The state this report describes as of {@code nowMillis}, given how old a live report may be.
   */
  public State stateAt(long nowMillis, long staleAfterMillis) {
    if (state == State.RUNNING && nowMillis - updatedAtMillis > staleAfterMillis) {
      return State.ABANDONED;
    }

    return state;
  }

  public State state() {
    return state;
  }

  /** The index this attempt was for, which is what names a report that outlives its index. */
  public String indexUuid() {
    return indexUuid;
  }

  public String column() {
    return column;
  }

  public long sourceSnapshotId() {
    return sourceSnapshotId;
  }

  public long startedAtMillis() {
    return startedAtMillis;
  }

  public long updatedAtMillis() {
    return updatedAtMillis;
  }

  public long filesBuilt() {
    return filesBuilt;
  }

  public long filesTotal() {
    return filesTotal;
  }

  public long filesFailed() {
    return filesFailed;
  }

  public long rows() {
    return rows;
  }

  /** Why the build failed, or null when it did not. */
  public String error() {
    return error;
  }

  /**
   * The reason something failed, in the form a report keeps it: the chain of messages, no trace.
   *
   * <p>A stack trace across the layers a Spark job reports through says where it was thrown, which
   * is of no use to someone asking why their build failed; the messages say what went wrong.
   */
  public static String describeFailure(Throwable failure) {
    if (failure == null) {
      return null;
    }

    StringBuilder described = new StringBuilder();
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      if (described.length() > 0) {
        described.append(": ");
      }

      described.append(cause.getClass().getName());
      if (cause.getMessage() != null) {
        described.append(": ").append(cause.getMessage());
      }

      if (described.length() >= MAX_ERROR_LENGTH) {
        break;
      }
    }

    return described.toString();
  }

  /** This report, closed as a success at {@code atMillis}. */
  public IndexBuildStatus succeeded(long builtFiles, long writtenRows, long atMillis) {
    return new IndexBuildStatus(
        State.SUCCEEDED,
        indexUuid,
        column,
        sourceSnapshotId,
        startedAtMillis,
        atMillis,
        builtFiles,
        filesTotal,
        filesFailed,
        writtenRows,
        null);
  }

  /**
   * This report, closed as a failure at {@code atMillis}, keeping a readable part of the reason.
   */
  public IndexBuildStatus failed(String reason, long atMillis) {
    String message = reason == null ? "" : reason;
    if (message.length() > MAX_ERROR_LENGTH) {
      message = message.substring(0, MAX_ERROR_LENGTH);
    }

    return new IndexBuildStatus(
        State.FAILED,
        indexUuid,
        column,
        sourceSnapshotId,
        startedAtMillis,
        atMillis,
        filesBuilt,
        filesTotal,
        filesFailed,
        rows,
        message.isEmpty() ? null : message);
  }

  /** The same report with the counters and time of the moment it is published. */
  IndexBuildStatus progress(long built, long failed, long writtenRows, long atMillis) {
    return new IndexBuildStatus(
        state,
        indexUuid,
        column,
        sourceSnapshotId,
        startedAtMillis,
        atMillis,
        built,
        filesTotal,
        failed,
        writtenRows,
        error);
  }

  /** Reads the newest report in {@code statusDir}, or null if there is none. */
  public static IndexBuildStatus read(FileIO io, String statusDir) {
    Preconditions.checkArgument(io != null, "Invalid file IO: null");
    Preconditions.checkArgument(
        statusDir != null && !statusDir.isEmpty(), "Invalid status directory: null or empty");

    Slot newest = null;
    for (int slot = 0; slot < 2; slot += 1) {
      Slot read = readSlot(io, slotPath(statusDir, slot));
      if (read != null && (newest == null || read.generation > newest.generation)) {
        newest = read;
      }
    }

    return newest == null ? null : newest.status;
  }

  /**
   * Writes one report, replacing the previous one from the point of view of {@link #read}.
   *
   * <p>The generation is what makes that replacement total: the file written is the one the current
   * generation does not name, so a reader that runs while the write is in flight still finds the
   * previous report in the other slot.
   */
  public static void write(FileIO io, String statusDir, IndexBuildStatus status) {
    Preconditions.checkArgument(io != null, "Invalid file IO: null");
    Preconditions.checkArgument(
        statusDir != null && !statusDir.isEmpty(), "Invalid status directory: null or empty");
    Preconditions.checkArgument(status != null, "Invalid build status: null");
    Preconditions.checkArgument(
        status.state != State.ABANDONED, "Cannot write an abandoned build: %s", status.state);

    long currentGeneration = 0L;
    for (int slot = 0; slot < 2; slot += 1) {
      Slot read = readSlot(io, slotPath(statusDir, slot));
      if (read != null && read.generation > currentGeneration) {
        currentGeneration = read.generation;
      }
    }

    long generation = currentGeneration + 1L;
    writeString(io, slotPath(statusDir, (int) (generation % 2)), toJson(generation, status));
  }

  static String toJson(long generation, IndexBuildStatus status) {
    return JsonUtil.generate(
        generator -> {
          generator.writeStartObject();
          generator.writeNumberField(GENERATION, generation);
          generator.writeStringField(STATE, status.state.name());
          generator.writeStringField(INDEX_UUID, status.indexUuid);
          if (status.column != null) {
            generator.writeStringField(COLUMN, status.column);
          }

          generator.writeNumberField(SOURCE_SNAPSHOT_ID, status.sourceSnapshotId);
          generator.writeNumberField(STARTED_AT, status.startedAtMillis);
          generator.writeNumberField(UPDATED_AT, status.updatedAtMillis);
          generator.writeNumberField(FILES_BUILT, status.filesBuilt);
          generator.writeNumberField(FILES_TOTAL, status.filesTotal);
          generator.writeNumberField(FILES_FAILED, status.filesFailed);
          generator.writeNumberField(ROWS, status.rows);
          if (status.error != null) {
            generator.writeStringField(ERROR, status.error);
          }

          generator.writeEndObject();
        },
        false);
  }

  static IndexBuildStatus fromJson(JsonNode node) {
    String state = JsonUtil.getString(STATE, node);
    return new IndexBuildStatus(
        State.valueOf(state),
        JsonUtil.getStringOrNull(INDEX_UUID, node),
        JsonUtil.getStringOrNull(COLUMN, node),
        JsonUtil.getLong(SOURCE_SNAPSHOT_ID, node),
        JsonUtil.getLong(STARTED_AT, node),
        JsonUtil.getLong(UPDATED_AT, node),
        JsonUtil.getLong(FILES_BUILT, node),
        JsonUtil.getLong(FILES_TOTAL, node),
        JsonUtil.getLong(FILES_FAILED, node),
        JsonUtil.getLong(ROWS, node),
        JsonUtil.getStringOrNull(ERROR, node));
  }

  /** Removes every report in {@code statusDir}, which is what a later success does to a failure. */
  public static void delete(FileIO io, String statusDir) {
    Preconditions.checkArgument(io != null, "Invalid file IO: null");
    Preconditions.checkArgument(
        statusDir != null && !statusDir.isEmpty(), "Invalid status directory: null or empty");

    for (int slot = 0; slot < 2; slot += 1) {
      String path = slotPath(statusDir, slot);
      if (io.newInputFile(path).exists()) {
        io.deleteFile(path);
      }
    }
  }

  static String slotPath(String statusDir, int slot) {
    return statusDir + "/" + SLOT_FILE_PREFIX + slot + ".json";
  }

  private static Slot readSlot(FileIO io, String path) {
    String json = readIfExists(io, path);
    if (json == null) {
      return null;
    }

    try {
      JsonNode node = JsonUtil.parse(json, unused -> unused);
      return new Slot(JsonUtil.getLong(GENERATION, node), fromJson(node));
    } catch (RuntimeException e) {
      // a report is only ever appended to a slot the other generation does not name, so this is a
      // write that was cut off, and the other slot still holds the last complete report
      return null;
    }
  }

  private static String readIfExists(FileIO io, String path) {
    InputFile inputFile = io.newInputFile(path);
    if (!inputFile.exists()) {
      return null;
    }

    try (SeekableInputStream in = inputFile.newStream()) {
      return new String(ByteStreams.toByteArray(in), StandardCharsets.UTF_8);
    } catch (FileNotFoundException e) {
      return null;
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read " + path, e);
    } catch (NotFoundException e) {
      return null;
    }
  }

  private static void writeString(FileIO io, String path, String content) {
    try (PositionOutputStream out = io.newOutputFile(path).createOrOverwrite()) {
      out.write(content.getBytes(StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to write " + path, e);
    }
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }

    if (other == null || getClass() != other.getClass()) {
      return false;
    }

    IndexBuildStatus that = (IndexBuildStatus) other;
    return state == that.state
        && Objects.equals(indexUuid, that.indexUuid)
        && sourceSnapshotId == that.sourceSnapshotId
        && startedAtMillis == that.startedAtMillis
        && updatedAtMillis == that.updatedAtMillis
        && filesBuilt == that.filesBuilt
        && filesTotal == that.filesTotal
        && filesFailed == that.filesFailed
        && rows == that.rows
        && Objects.equals(column, that.column)
        && Objects.equals(error, that.error);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        state,
        indexUuid,
        column,
        sourceSnapshotId,
        startedAtMillis,
        updatedAtMillis,
        filesBuilt,
        filesTotal,
        filesFailed,
        rows,
        error);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("state", state)
        .add("indexUuid", indexUuid)
        .add("column", column)
        .add("sourceSnapshotId", sourceSnapshotId)
        .add("startedAt", startedAtMillis)
        .add("updatedAt", updatedAtMillis)
        .add("filesBuilt", filesBuilt)
        .add("filesTotal", filesTotal)
        .add("filesFailed", filesFailed)
        .add("rows", rows)
        .add("error", error)
        .toString();
  }

  /** A report and the generation it was written with, which is what orders the two slots. */
  private static final class Slot {
    private final long generation;
    private final IndexBuildStatus status;

    private Slot(long generation, IndexBuildStatus status) {
      this.generation = generation;
      this.status = status;
    }
  }

  public static final class Builder {
    private State state = State.RUNNING;
    private String indexUuid;
    private String column;
    private long sourceSnapshotId = -1L;
    private long startedAtMillis;
    private long updatedAtMillis;
    private long filesBuilt;
    private long filesTotal;
    private long filesFailed;
    private long rows;
    private String error;

    private Builder() {}

    public Builder state(State newState) {
      Preconditions.checkArgument(newState != null, "Invalid state: null");
      Preconditions.checkArgument(
          newState != State.ABANDONED, "Cannot build an abandoned report: %s", newState);
      this.state = newState;
      return this;
    }

    public Builder indexUuid(String newIndexUuid) {
      this.indexUuid = newIndexUuid;
      return this;
    }

    public Builder column(String newColumn) {
      this.column = newColumn;
      return this;
    }

    public Builder sourceSnapshotId(long newSourceSnapshotId) {
      this.sourceSnapshotId = newSourceSnapshotId;
      return this;
    }

    public Builder startedAtMillis(long newStartedAtMillis) {
      this.startedAtMillis = newStartedAtMillis;
      return this;
    }

    public Builder updatedAtMillis(long newUpdatedAtMillis) {
      this.updatedAtMillis = newUpdatedAtMillis;
      return this;
    }

    public Builder filesBuilt(long newFilesBuilt) {
      this.filesBuilt = newFilesBuilt;
      return this;
    }

    public Builder filesTotal(long newFilesTotal) {
      this.filesTotal = newFilesTotal;
      return this;
    }

    public Builder filesFailed(long newFilesFailed) {
      this.filesFailed = newFilesFailed;
      return this;
    }

    public Builder rows(long newRows) {
      this.rows = newRows;
      return this;
    }

    public Builder error(String newError) {
      this.error = newError;
      return this;
    }

    public IndexBuildStatus build() {
      if (updatedAtMillis == 0L) {
        updatedAtMillis = startedAtMillis;
      }

      Preconditions.checkArgument(
          indexUuid != null && !indexUuid.isEmpty(), "Invalid index uuid: null or empty");
      return new IndexBuildStatus(
          state,
          indexUuid,
          column,
          sourceSnapshotId,
          startedAtMillis,
          updatedAtMillis,
          filesBuilt,
          filesTotal,
          filesFailed,
          rows,
          error);
    }
  }
}
