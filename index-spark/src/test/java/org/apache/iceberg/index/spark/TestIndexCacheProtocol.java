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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.index.IndexDefinition;
import org.apache.iceberg.index.IndexFileMemory;
import org.junit.jupiter.api.Test;

/** The driver and executors exchange these messages only through Java serialization. */
class TestIndexCacheProtocol {
  private static final IndexDefinition DEFINITION = new VectorDefinition(3);
  private static final IndexFileMemory MEMORY =
      IndexFileMemory.builder()
          .fileBytes(2_048L)
          .payloadBytes(1_024L)
          .preparedBytes(512L)
          .modelVersion(1)
          .readerId("vector/hnswlib/1")
          .runtimeProfile("reference-bytes=4;alignment=8")
          .structure(Map.of("entries", 100L))
          .build();

  @Test
  void fileSurvivesSerializationWithItsReadContextAndEstimate() {
    IndexCacheProtocol.File file = file("f1", 2_560L);

    IndexCacheProtocol.File copy = roundTrip(file);

    assertThat(copy.location()).isEqualTo("f1");
    assertThat(copy.cacheKey()).isEqualTo("adapter|table#A|f1");
    assertThat(copy.kind()).isEqualTo("index");
    assertThat(copy.blobType()).isEqualTo("vector-hnsw");
    assertThat(copy.estimatedBytes()).isEqualTo(2_560L);
    assertThat(copy.memory()).isNotNull();
    assertThat(copy.memory().fileBytes()).isEqualTo(2_048L);
    assertThat(copy.memory().preparedBytes()).isEqualTo(512L);
    assertThat(copy.memory().modelVersion()).isEqualTo(1);
    assertThat(copy.memory().readerId()).isEqualTo("vector/hnswlib/1");
    assertThat(copy.memory().structure()).containsEntry("entries", 100L);
    assertThat(readPayload(copy.payload())).isEqualTo("payload");
  }

  @Test
  void targetCarriesTheReadContextWithoutACatalog() {
    IndexCacheProtocol.Target target =
        IndexCacheProtocol.Target.builder()
            .indexUuid("table#A")
            .snapshotId(7)
            .files(List.of(file("f1", 2_560L)))
            .definition(DEFINITION)
            .io(new HadoopFileIO(new Configuration()))
            .build();

    IndexCacheProtocol.Target copy = roundTrip(target);

    assertThat(copy.hasReadContext()).isTrue();
    assertThat(copy.indexUuid()).isEqualTo("table#A");
    assertThat(copy.snapshotId()).isEqualTo(7);
    assertThat(copy.files()).hasSize(1);
    assertThat(copy.definition().type()).isEqualTo("vector");
    assertThat(copy.definition().fieldId()).isEqualTo(3);
    assertThat(copy.io()).isInstanceOf(HadoopFileIO.class);
  }

  @Test
  void commandAndAckKeepTheIdentityThatMakesThemIdempotent() {
    IndexCacheProtocol.Command command =
        IndexCacheProtocol.Command.builder()
            .session("application-1")
            .executorId("executor-1")
            .incarnation("executor-1#3")
            .indexUuid("table#A")
            .epoch(4)
            .planId("plan-9")
            .commandId("plan-9#2-LOAD")
            .type(IndexCacheProtocol.CommandType.LOAD)
            .mode("adapter")
            .file(file("f1", 2_560L))
            .definition(DEFINITION)
            .io(new HadoopFileIO(new Configuration()))
            .sequence(11)
            .build();

    IndexCacheProtocol.Command commandCopy = roundTrip(command);
    assertThat(commandCopy.commandId()).isEqualTo("plan-9#2-LOAD");
    assertThat(commandCopy.incarnation()).isEqualTo("executor-1#3");
    assertThat(commandCopy.epoch()).isEqualTo(4);
    assertThat(commandCopy.mode()).isEqualTo("adapter");
    assertThat(commandCopy.file().location()).isEqualTo("f1");

    IndexCacheProtocol.Ack ack =
        IndexCacheProtocol.Ack.builder()
            .session("application-1")
            .planId("plan-9")
            .commandId("plan-9#2-LOAD")
            .executorId("executor-1")
            .incarnation("executor-1#3")
            .epoch(4)
            .sequence(12)
            .status(IndexCacheProtocol.AckStatus.LOADED)
            .bytes(2_560L)
            .reason("ok")
            .build();

    IndexCacheProtocol.Ack ackCopy = roundTrip(ack);
    assertThat(ackCopy.commandId()).isEqualTo("plan-9#2-LOAD");
    assertThat(ackCopy.status()).isEqualTo(IndexCacheProtocol.AckStatus.LOADED);
    assertThat(ackCopy.bytes()).isEqualTo(2_560L);
    assertThat(ackCopy.reason()).isEqualTo("ok");
  }

  @Test
  void reportPlanReplyAndResultSurviveSerialization() {
    IndexCacheProtocol.NodeReport report =
        IndexCacheProtocol.NodeReport.builder()
            .session("application-1")
            .executorId("executor-1")
            .incarnation("executor-1#3")
            .sequence(5)
            .budgetBytes(1_000)
            .usedBytes(100)
            .reservedBytes(20)
            .releasingBytes(10)
            .holders(Map.of("adapter|table#A|f1", 40L))
            .reservedByPlan(Map.of("plan-9", 20L))
            .profile("reference-bytes=4")
            .accessAgeNanos(Map.of("table#A", 3_000L))
            .build();

    IndexCacheProtocol.NodeReport reportCopy = roundTrip(report);
    assertThat(reportCopy.holders()).containsEntry("adapter|table#A|f1", 40L);
    assertThat(reportCopy.reservedBytesFor("plan-9")).isEqualTo(20L);
    assertThat(reportCopy.accessAgeNanos()).containsEntry("table#A", 3_000L);
    assertThat(reportCopy.profile()).isEqualTo("reference-bytes=4");

    IndexCacheProtocol.Plan plan =
        IndexCacheProtocol.Plan.builder()
            .planId("plan-9")
            .indexUuid("table#A")
            .snapshotId(7)
            .epoch(4)
            .placements(Map.of("f1", "executor-1"))
            .rejected(Map.of("f2", "FILE_TOO_LARGE"))
            .files(List.of(file("f1", 2_560L)))
            .build();
    assertThat(roundTrip(plan).rejected()).containsEntry("f2", "FILE_TOO_LARGE");

    IndexCacheProtocol.Reply reply =
        IndexCacheProtocol.Reply.builder().commands(List.of()).nextSequence(5).build();
    assertThat(roundTrip(reply).nextSequence()).isEqualTo(5);

    IndexCacheProtocol.Result result =
        IndexCacheProtocol.Result.builder()
            .planId("plan-9")
            .indexUuid("table#A")
            .snapshotId(7)
            .epoch(4)
            .status(IndexCacheProtocol.Status.PARTIAL)
            .protection(
                IndexCacheProtocol.Protection.builder()
                    .text("30m")
                    .deadlineNanos(1_800_000_000_000L)
                    .build())
            .targetFiles(List.of("f1", "f2"))
            .uncovered(Map.of("f2", "PENDING"))
            .residentBytes(2_560L)
            .evictedIndexes(List.of("table#B"))
            .reusedFiles(List.of("f1"))
            .migrations(Map.of("f1", "executor-2"))
            .migrationNotices(Map.of("f3", "NEEDS_DOUBLE_SPACE"))
            .build();

    IndexCacheProtocol.Result resultCopy = roundTrip(result);
    assertThat(resultCopy.status()).isEqualTo(IndexCacheProtocol.Status.PARTIAL);
    assertThat(resultCopy.protection().text()).isEqualTo("30m");
    assertThat(resultCopy.uncovered()).containsEntry("f2", "PENDING");
    assertThat(resultCopy.residentBytes()).isEqualTo(2_560L);
    assertThat(resultCopy.migrations()).containsEntry("f1", "executor-2");
    assertThat(resultCopy.migrationNotices()).containsEntry("f3", "NEEDS_DOUBLE_SPACE");
  }

  @Test
  void deserializedCollectionsCannotMutateTheMessage() {
    Map<String, Long> holders = new HashMap<>(Map.of("a", 1L));
    IndexCacheProtocol.NodeReport report =
        IndexCacheProtocol.NodeReport.builder()
            .session("s")
            .executorId("e")
            .incarnation("e#1")
            .sequence(1)
            .budgetBytes(10)
            .usedBytes(1)
            .holders(holders)
            .build();
    holders.put("b", 2L);

    IndexCacheProtocol.NodeReport copy = roundTrip(report);
    assertThat(copy.holders()).containsOnlyKeys("a");
    assertThatThrownBy(() -> copy.holders().put("c", 3L))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void incompleteReadContextIsRejectedBeforeItReachesAnExecutor() {
    assertThatThrownBy(
            () ->
                IndexCacheProtocol.Command.builder()
                    .session("s")
                    .executorId("e")
                    .incarnation("e#1")
                    .indexUuid("table#A")
                    .epoch(1)
                    .planId("plan-1")
                    .commandId("plan-1#1-LOAD")
                    .type(IndexCacheProtocol.CommandType.LOAD)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("file");
  }

  private static IndexCacheProtocol.File file(String location, long bytes) {
    return IndexCacheProtocol.File.builder()
        .location(location)
        .cacheKey("adapter|table#A|" + location)
        .kind("index")
        .blobType("vector-hnsw")
        .payload(ByteBuffer.wrap("payload".getBytes(StandardCharsets.UTF_8)))
        .estimatedBytes(bytes)
        .memory(MEMORY)
        .build();
  }

  private static String readPayload(ByteBuffer buffer) {
    byte[] bytes = new byte[buffer.remaining()];
    buffer.duplicate().get(bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  private static <T> T roundTrip(T value) {
    try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(value);
      out.flush();
      try (ObjectInputStream in =
          new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
        @SuppressWarnings("unchecked")
        T copy = (T) in.readObject();
        return copy;
      }
    } catch (Exception e) {
      throw new IllegalStateException("Cannot serialize " + value.getClass().getName(), e);
    }
  }

  private static final class VectorDefinition implements IndexDefinition {
    private static final long serialVersionUID = 1L;
    private final int fieldId;

    private VectorDefinition(int fieldId) {
      this.fieldId = fieldId;
    }

    @Override
    public String type() {
      return "vector";
    }

    @Override
    public int fieldId() {
      return fieldId;
    }
  }
}
