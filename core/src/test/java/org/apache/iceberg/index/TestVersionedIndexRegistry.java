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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.TestTables.LocalFileIO;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.JsonUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * A registration used to read the registry, add to it and overwrite it, so two writers registering
 * different indexes at the same time lost one of them. The registry is now immutable and the table
 * property that names it is compared and swapped on commit, so a writer that loses the race has to
 * merge the winner's registry instead of overwriting it.
 */
class TestVersionedIndexRegistry {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.required(
              2, "vec", Types.ListType.ofRequired(3, Types.FloatType.get())));

  private static final String TABLE_NAME = "versioned_registry_table";

  @TempDir private Path tempDir;
  private File tableDir;

  @BeforeAll
  static void registerTestingIndexType() {
    IndexTypes.register(new TestingIndexType());
  }

  @BeforeEach
  void before() throws IOException {
    TestTables.clearTables();
    this.tableDir = Files.createDirectories(tempDir.resolve("table")).toFile();
  }

  @Test
  void registersIndexesInVersionedFilesPointedToFromTheTable() {
    TestTables.TestTableOperations ops = operations();
    SidecarIndexCatalog catalog = versionedCatalog(ops);

    catalog.createIndex(request("index-a"));
    String first = registryPointer(ops);
    assertThat(first).matches("indexmeta\\.v1-[0-9a-f]{8}\\.json");
    assertThat(indexUuids(first)).containsExactly("index-a");

    catalog.createIndex(request("index-b"));
    String second = registryPointer(ops);
    assertThat(second).matches("indexmeta\\.v2-[0-9a-f]{8}\\.json");
    assertThat(indexUuids(second)).containsExactly("index-a", "index-b");
    // the earlier version is kept, so the history of the registry survives
    assertThat(indexUuids(first)).containsExactly("index-a");
    assertThat(previousFile(second)).isEqualTo(first);
    assertThat(new File(indexesRoot(), "index-registry.json")).doesNotExist();

    assertThat(catalog.listIndexes(tableDir.toString(), "table-uuid"))
        .extracting(IndexMetadata::indexUuid)
        .containsExactly("index-a", "index-b");
  }

  @Test
  void readsTheLegacyRegistryWhenTheTablePointsAtNothing() {
    writeIndex("index-legacy", "table-uuid");
    writeLegacyRegistry("index-legacy");
    TestTables.TestTableOperations ops = operations();
    SidecarIndexCatalog catalog = versionedCatalog(ops);

    assertThat(catalog.listIndexes(tableDir.toString()))
        .extracting(IndexMetadata::indexUuid)
        .containsExactly("index-legacy");
    assertThat(registryPointer(ops)).isNull();
  }

  @Test
  void firstRegistrationAfterTheLegacyRegistryKeepsTheLegacyList() {
    writeIndex("index-legacy", "table-uuid");
    writeLegacyRegistry("index-legacy");
    TestTables.TestTableOperations ops = operations();
    SidecarIndexCatalog catalog = versionedCatalog(ops);

    catalog.createIndex(request("index-new"));

    assertThat(indexUuids(registryPointer(ops))).containsExactly("index-legacy", "index-new");
    // the legacy file is left in place for readers that predate the pointer
    assertThat(new File(indexesRoot(), "index-registry.json")).exists();
  }

  @Test
  void refusesToFollowAPathInsteadOfARegistryFileName() {
    TestTables.TestTableOperations ops = operations();
    ops.commit(
        ops.current(),
        ops.current()
            .replaceProperties(Map.of(SidecarIndexCatalog.REGISTRY_PROPERTY, "../x.json")));
    SidecarIndexCatalog catalog = versionedCatalog(ops);

    assertThatThrownBy(() -> catalog.listIndexes(tableDir.toString()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid index registry file");
  }

  @Test
  void aWriterThatLosesTheRaceMergesTheWinnersRegistry() {
    TestTables.TestTableOperations ops = operations();
    // a second writer over the same table: its operations read the metadata the first one created
    TestTables.TestTableOperations otherOps =
        new TestTables.TestTableOperations(TABLE_NAME, tableDir);
    // the second writer's first commit fails, as it would when another writer moved the pointer
    SidecarIndexCatalog loser =
        new SidecarIndexCatalog(
            new LocalFileIO(),
            new TableIndexRegistryPointer(
                new BaseTable(new FailFirstCommit(otherOps), TABLE_NAME)));
    SidecarIndexCatalog winner = versionedCatalog(ops);

    winner.createIndex(request("index-winner"));
    String winnerPointer = registryPointer(ops);

    loser.createIndex(request("index-loser"));

    String pointer = registryPointer(ops);
    assertThat(pointer).matches("indexmeta\\.v2-[0-9a-f]{8}\\.json");
    assertThat(indexUuids(pointer)).containsExactly("index-winner", "index-loser");
    assertThat(previousFile(pointer)).isEqualTo(winnerPointer);
    // a reader sees both, which is the point: no registration is ever dropped
    assertThat(versionedCatalog(ops).listIndexes(tableDir.toString()))
        .extracting(IndexMetadata::indexUuid)
        .containsExactly("index-winner", "index-loser");
  }

  @Test
  void snapshotPublicationRetriesAfterAnotherPublisherCommits() {
    TestTables.TestTableOperations ops = operations();
    SidecarIndexCatalog winner = versionedCatalog(ops);
    winner.createIndex(request("index-a"));
    FailFirstCommit racing =
        new FailFirstCommit(
            new TestTables.TestTableOperations(TABLE_NAME, tableDir),
            () ->
                winner.updateIndex(
                    tableDir.toString(), "index-a", current -> current.withSnapshot(snapshot(1))));
    SidecarIndexCatalog loser =
        new SidecarIndexCatalog(new LocalFileIO(), new BaseTable(racing, TABLE_NAME));

    loser.updateIndex(tableDir.toString(), "index-a", current -> current.withSnapshot(snapshot(2)));

    assertThat(winner.loadIndex(tableDir.toString(), "index-a").snapshots())
        .extracting(IndexSnapshot::snapshotId)
        .containsExactly(1L, 2L);
  }

  @Test
  void aRegistrationLostToAWriterThatDidNotCompareIsRestored() {
    TestTables.TestTableOperations ops = operations();
    SidecarIndexCatalog catalog = versionedCatalog(ops);
    catalog.createIndex(request("index-b"));
    catalog.createIndex(request("index-a"));

    // a writer that read the table before index-a was registered: it moves the pointer from its own
    // view without comparing it, so the registry it published leaves index-a out and names no
    // previous file, because the view it was built from carried none
    String staleRegistry = publishRegistryFromStaleView(ops, "index-b");

    catalog.updateIndex(
        tableDir.toString(), "index-a", current -> current.withSnapshot(snapshot(1)));

    String pointer = registryPointer(ops);
    // the change is applied to the tree this catalog registered earlier, which is still on disk, so
    // the publication puts the entry back instead of failing over a registration it never lost. The
    // restored registry is the version the publication landed on, and it descends from the stale
    // one
    assertThat(indexUuids(pointer)).containsExactly("index-b", "index-a");
    assertThat(previousFile(previousFile(pointer))).isEqualTo(staleRegistry);
    assertThat(catalog.loadIndex(tableDir.toString(), "index-a").snapshots())
        .extracting(IndexSnapshot::snapshotId)
        .containsExactly(1L);
    // the index the stale writer kept is still readable, so restoring nothing dropped anything
    assertThat(catalog.loadIndex(tableDir.toString(), "index-b")).isNotNull();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void anIndexDroppedWhileItWasBeingUpdatedIsNotRegisteredAgain(boolean purge) {
    TestTables.TestTableOperations ops = operations();
    SidecarIndexCatalog catalog = versionedCatalog(ops);
    catalog.createIndex(request("index-a"));

    // another session drops the index while this one is still building it. Without the purge the
    // tree is still on disk, so the registry's history is what says the entry was removed on
    // purpose rather than lost by a writer that never saw it
    assertThat(versionedCatalog(ops).dropIndex(tableDir.toString(), "index-a", purge)).isTrue();

    assertThatThrownBy(
            () ->
                catalog.updateIndex(
                    tableDir.toString(), "index-a", current -> current.withSnapshot(snapshot(1))))
        .isInstanceOf(NotFoundException.class)
        .hasMessageContaining("was dropped while it was being updated");
    // a dropped index stays dropped: publishing into it must not put the entry back
    assertThat(indexUuids(registryPointer(ops))).doesNotContain("index-a");
  }

  @Test
  void aSecondIndexCannotReuseANameOnTheSameColumn() {
    TestTables.TestTableOperations ops = operations();
    SidecarIndexCatalog catalog = versionedCatalog(ops);
    catalog.createIndex(namedRequest("index-a", "shared"));

    assertThatThrownBy(() -> catalog.createIndex(namedRequest("index-b", "shared")))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining("shared")
        .hasMessageContaining("index-a");
    // the loser names no entry, and the tree it wrote before losing is removed again
    assertThat(indexUuids(registryPointer(ops))).containsExactly("index-a");
    File loserMetadata = new File(indexesRoot(), "index-b/metadata");
    assertThat(loserMetadata.exists() ? loserMetadata.listFiles() : new File[0]).isEmpty();
  }

  @Test
  void aRegistrationThatLosesTheRaceRefusesTheNameTheWinnerTook() {
    TestTables.TestTableOperations ops = operations();
    SidecarIndexCatalog winner = versionedCatalog(ops);
    // the first commit fails after the winner has registered the same name: the loser has to redo
    // its check against the registry the winner published, not the one it prechecked against
    SidecarIndexCatalog loser =
        new SidecarIndexCatalog(
            new LocalFileIO(),
            new FailsFirstCommitPointer(
                new TableIndexRegistryPointer(
                    new BaseTable(
                        new TestTables.TestTableOperations(TABLE_NAME, tableDir), TABLE_NAME)),
                () -> winner.createIndex(namedRequest("winner", "shared"))));

    assertThatThrownBy(() -> loser.createIndex(namedRequest("loser", "shared")))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining("shared")
        .hasMessageContaining("winner");
    assertThat(indexUuids(registryPointer(ops))).containsExactly("winner");
  }

  @Test
  void twoWritersRegisteringTheSameNameLeaveOneIndex() throws Exception {
    TestTables.TestTableOperations ops = operations();
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      CountDownLatch start = new CountDownLatch(1);
      List<Future<Boolean>> attempts = new ArrayList<>();
      for (int i = 0; i < 2; i += 1) {
        String indexUuid = "index-" + i;
        attempts.add(
            executor.submit(
                () -> {
                  start.await();
                  SidecarIndexCatalog catalog =
                      versionedCatalog(new TestTables.TestTableOperations(TABLE_NAME, tableDir));
                  try {
                    catalog.createIndex(namedRequest(indexUuid, "shared"));
                    return true;
                  } catch (AlreadyExistsException e) {
                    return false;
                  }
                }));
      }

      start.countDown();
      assertThat(
              List.of(
                  attempts.get(0).get(30, TimeUnit.SECONDS),
                  attempts.get(1).get(30, TimeUnit.SECONDS)))
          .containsExactlyInAnyOrder(true, false);
    } finally {
      executor.shutdownNow();
    }

    assertThat(versionedCatalog(ops).listIndexes(tableDir.toString(), "table-uuid"))
        .extracting(IndexMetadata::name)
        .containsExactly("shared");
  }

  @ParameterizedTest
  @ValueSource(ints = {8, 9, 10})
  void aDroppedIndexStaysDroppedAfterTheHistoryIsPruned(int registrations) {
    TestTables.TestTableOperations ops = operations();
    SidecarIndexCatalog catalog = versionedCatalog(ops);
    catalog.createIndex(request("index-a"));

    // another session drops the index without purging it: the tree this catalog wrote is still on
    // disk, so nothing but the registry's own record could say the entry was removed on purpose
    assertThat(versionedCatalog(ops).dropIndex(tableDir.toString(), "index-a", false)).isTrue();
    JsonNode dropped = registryJson(registryPointer(ops)).get("dropped");
    assertThat(dropped).isNotNull();
    assertThat(dropped.get(0).asText()).isEqualTo("index-a");

    // enough later registrations to prune the version that still named the dropped index, which is
    // the only evidence a registry without a drop record leaves behind
    for (int i = 0; i < registrations; i += 1) {
      versionedCatalog(ops).createIndex(request("index-" + i));
    }

    assertThatThrownBy(
            () ->
                catalog.updateIndex(
                    tableDir.toString(), "index-a", current -> current.withSnapshot(snapshot(1))))
        .isInstanceOf(NotFoundException.class)
        .hasMessageContaining("was dropped while it was being updated");
    assertThat(indexUuids(registryPointer(ops))).doesNotContain("index-a");
  }

  /**
   * Publishes a registry the way a writer that does not compare the pointer would: from a view of
   * the table it read before the latest registration existed. That view carried no pointer, so the
   * registry names no previous file, and its absence from the chain of versions is what tells a
   * reader that the entries it leaves out were not removed on purpose.
   */
  private String publishRegistryFromStaleView(
      TestTables.TestTableOperations ops, String... indexUuids) {
    String fileName =
        SidecarIndexCatalog.REGISTRY_FILE_PREFIX
            + "1-"
            + UUID.randomUUID().toString().replace("-", "").substring(0, 8)
            + ".json";
    JsonNode named = registryJson(registryPointer(ops));
    Map<String, String> files = new LinkedHashMap<>();
    Map<String, String> tableUuids = new LinkedHashMap<>();
    JsonNode recordedUuids = named.get("table-uuids");
    for (String indexUuid : indexUuids) {
      files.put(indexUuid, named.get("metadata-files").get(indexUuid).asText());
      if (recordedUuids != null && recordedUuids.get(indexUuid) != null) {
        tableUuids.put(indexUuid, recordedUuids.get(indexUuid).asText());
      }
    }

    String json =
        JsonUtil.generate(
            generator -> {
              generator.writeStartObject();
              generator.writeNumberField("format-version", 3);
              generator.writeNumberField("updated-ms", System.currentTimeMillis());
              generator.writeArrayFieldStart("index-uuids");
              for (String indexUuid : files.keySet()) {
                generator.writeString(indexUuid);
              }
              generator.writeEndArray();
              JsonUtil.writeStringMap("metadata-files", files, generator);
              if (!tableUuids.isEmpty()) {
                JsonUtil.writeStringMap("table-uuids", tableUuids, generator);
              }
              // a registry the current build writes always carries the record, even when empty: a
              // registration is restored only when the document could have recorded a drop
              generator.writeArrayFieldStart("dropped");
              generator.writeEndArray();
              generator.writeEndObject();
            },
            false);
    try {
      Files.write(indexesRoot().toPath().resolve(fileName), json.getBytes(UTF_8));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    TableMetadata base = ops.refresh();
    Map<String, String> properties = new HashMap<>(base.properties());
    properties.put(SidecarIndexCatalog.REGISTRY_PROPERTY, fileName);
    ops.commit(base, base.replaceProperties(properties));
    return fileName;
  }

  @Test
  void firstSnapshotPublicationMigratesLegacyMetadata() {
    writeIndex("index-legacy", "table-uuid");
    writeLegacyRegistry("index-legacy");
    TestTables.TestTableOperations ops = operations();
    SidecarIndexCatalog catalog = versionedCatalog(ops);

    catalog.updateIndex(
        tableDir.toString(), "index-legacy", current -> current.withSnapshot(snapshot(1)));

    assertThat(registryPointer(ops)).isNotNull();
    assertThat(catalog.loadIndex(tableDir.toString(), "index-legacy").snapshots())
        .containsExactly(snapshot(1));
    assertThat(new File(indexesRoot(), "index-legacy/metadata/index-metadata-v1.json")).exists();
  }

  private static IndexSnapshot snapshot(long id) {
    return IndexSnapshot.builder()
        .snapshotId(id)
        .sourceTableSnapshotId(id)
        .timestampMillis(id)
        .indexManifestList("manifest-list-" + id)
        .build();
  }

  @Test
  void refusesToStartFromEmptyWhenTheRegistryFileIsGone() {
    TestTables.TestTableOperations ops = operations();
    SidecarIndexCatalog catalog = versionedCatalog(ops);
    catalog.createIndex(request("index-a"));

    // the file the table points at was removed: what the table holds is not known, so it must not
    // be treated as a table with no indexes, which the next registration would overwrite
    new File(indexesRoot(), registryPointer(ops)).delete();

    assertThatThrownBy(() -> catalog.createIndex(request("index-b")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("does not exist");
  }

  @Test
  void aRegistrationThatNeverCompletesLeavesNoTreeBehind() {
    TestTables.TestTableOperations ops = operations();
    SidecarIndexCatalog catalog =
        new SidecarIndexCatalog(
            new LocalFileIO(),
            new TableIndexRegistryPointer(new BaseTable(new AlwaysFailCommit(ops), TABLE_NAME)));

    assertThatThrownBy(() -> catalog.createIndex(request("index-a")))
        .isInstanceOf(CommitFailedException.class)
        .hasMessageContaining("index-a");
    // the tree written before the registration is removed again, so a failed build leaves nothing
    File metadata = new File(indexesRoot(), "index-a/metadata");
    assertThat(metadata.exists() ? metadata.listFiles() : new File[0]).isEmpty();
    assertThat(ops.refresh().properties()).doesNotContainKey(SidecarIndexCatalog.REGISTRY_PROPERTY);
  }

  @Test
  void droppingAnIndexAdvancesTheRegistryAndPurgesOnRequest() {
    TestTables.TestTableOperations ops = operations();
    SidecarIndexCatalog catalog = versionedCatalog(ops);

    catalog.createIndex(request("index-a"));
    IndexMetadata dropped = catalog.createIndex(request("index-b"));
    assertThat(new File(dropped.location(), "metadata").listFiles()).hasSize(1);

    assertThat(catalog.dropIndex(tableDir.toString(), "index-b", true)).isTrue();

    assertThat(indexUuids(registryPointer(ops))).containsExactly("index-a");
    // every metadata version and both pointer slots are gone, leaving the directory empty
    assertThat(new File(dropped.location(), "metadata").listFiles()).isEmpty();
    assertThat(catalog.dropIndex(tableDir.toString(), "index-b", true)).isFalse();
  }

  @Test
  void keepsTheMostRecentRegistryFilesAndDeletesTheRest() {
    TestTables.TestTableOperations ops = operations();
    SidecarIndexCatalog catalog = versionedCatalog(ops);

    int registrations = SidecarIndexCatalog.VERSIONS_TO_KEEP + 3;
    for (int i = 0; i < registrations; i += 1) {
      catalog.createIndex(request("index-" + i));
    }

    List<String> versions = registryFiles();
    assertThat(versions).hasSize(SidecarIndexCatalog.VERSIONS_TO_KEEP);
    assertThat(versions.get(versions.size() - 1)).isEqualTo(registryPointer(ops));
  }

  @Test
  void leavesOutTheIndexesOfATableInstanceThatWasDroppedAndCreatedAgain() {
    writeIndex("index-of-old-table", "old-table-uuid");
    writeLegacyRegistry("index-of-old-table");
    TestTables.TestTableOperations ops = operations();
    SidecarIndexCatalog catalog = versionedCatalog(ops);

    catalog.createIndex(request("index-new"));

    // the new table instance owns its own registry: the indexes left behind at the location are
    // not its, and keeping them would carry a table uuid the new table never had
    assertThat(indexUuids(registryPointer(ops))).containsExactly("index-new");
  }

  @Test
  void recordsTheTableInstanceEachIndexWasBuiltFor() {
    TestTables.TestTableOperations ops = operations();
    SidecarIndexCatalog catalog = versionedCatalog(ops);

    catalog.createIndex(request("index-a"));
    JsonNode tableUuids = registryJson(registryPointer(ops)).get("table-uuids");
    assertThat(tableUuids).isNotNull();
    assertThat(tableUuids.get("index-a").asText()).isEqualTo("table-uuid");

    // the entry leaves with the index, so a registry with no indexes names no table instances
    catalog.dropIndex(tableDir.toString(), "index-a", false);
    assertThat(registryJson(registryPointer(ops)).get("table-uuids")).isNull();
  }

  @Test
  void registrationDoesNotReadTheIndexesTheRegistryNames() {
    TestTables.TestTableOperations ops = operations();
    SidecarIndexCatalog catalog = versionedCatalog(ops);
    catalog.createIndex(request("index-a"));

    // the registry names the table instance of index-a, so registering index-b knows that index is
    // not foreign without opening its metadata: the metadata is deleted, and a registration that
    // opened it anyway would fail here instead
    JsonNode registry = registryJson(registryPointer(ops));
    deleteMetadataRoot("index-a", registry.get("metadata-files").get("index-a").asText());

    catalog.createIndex(request("index-b"));

    assertThat(indexUuids(registryPointer(ops))).containsExactly("index-a", "index-b");
  }

  @Test
  void registrationReadsIndexesTheRegistryNamesNoTableInstanceFor() {
    TestTables.TestTableOperations ops = operations();
    SidecarIndexCatalog catalog = versionedCatalog(ops);
    catalog.createIndex(request("index-a"));

    // a registry written before the table uuids existed names no table instance, so the
    // registration reads the metadata to decide whether the index is this table's
    String registryFile = registryPointer(ops);
    dropTableUuids(registryFile);
    deleteMetadataRoot("index-a", "index-metadata-" + rootOf(registryFile) + ".json");

    // the metadata that would say whose index it is is gone, so no table can claim the entry: the
    // registration removes it from the registry instead of failing on it, which is what makes the
    // unreadable entry recoverable
    catalog.createIndex(request("index-b"));

    assertThat(indexUuids(registryPointer(ops))).containsExactly("index-b");
  }

  @Test
  void listingSkipsAnIndexWhoseMetadataIsGone() {
    TestTables.TestTableOperations ops = operations();
    SidecarIndexCatalog catalog = versionedCatalog(ops);
    catalog.createIndex(request("index-a"));
    catalog.createIndex(request("index-b"));
    deleteMetadataRoot("index-a", metadataRootOf(registryPointer(ops), "index-a"));

    // one unreadable entry does not fail the listing of the others
    assertThat(catalog.listIndexes(tableDir.toString()))
        .extracting(IndexMetadata::indexUuid)
        .containsExactly("index-b");
    assertThat(catalog.damagedIndexUuids(tableDir.toString())).containsExactly("index-a");
    // asking for the damaged index by UUID is still an error: the caller named it
    assertThatThrownBy(() -> catalog.loadIndex(tableDir.toString(), "index-a"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Index metadata file does not exist");
  }

  @Test
  void treatsAMetadataDocumentThatIsNotJsonAsDamaged() {
    TestTables.TestTableOperations ops = operations();
    SidecarIndexCatalog catalog = versionedCatalog(ops);
    catalog.createIndex(request("index-a"));
    catalog.createIndex(request("index-b"));
    String root = metadataRootOf(registryPointer(ops), "index-a");
    corruptMetadataRoot("index-a", root);

    // a document nothing can parse is as unusable as one that is gone: the entries that are
    // readable still list, and the damaged one is reported and removable
    assertThat(catalog.listIndexes(tableDir.toString()))
        .extracting(IndexMetadata::indexUuid)
        .containsExactly("index-b");
    assertThat(catalog.damagedIndexUuids(tableDir.toString())).containsExactly("index-a");
    assertThat(catalog.dropIndex(tableDir.toString(), "index-a", true)).isTrue();
    assertThat(catalog.damagedIndexUuids(tableDir.toString())).isEmpty();
  }

  @Test
  void refusesToRestoreARegistrationWhenTheRegistryRecordsNoDrops() {
    TestTables.TestTableOperations ops = operations();
    SidecarIndexCatalog catalog = versionedCatalog(ops);
    catalog.createIndex(request("index-b"));
    catalog.createIndex(request("index-a"));

    // a writer of an older build moved the pointer past the registration of index-a and wrote a
    // registry without the drop record, so nothing on disk can tell a lost registration from one
    // that was dropped on purpose
    String staleRegistry = publishRegistryFromStaleView(ops, "index-b");
    dropDroppedRecord(staleRegistry);

    assertThatThrownBy(
            () ->
                catalog.updateIndex(
                    tableDir.toString(), "index-a", current -> current.withSnapshot(snapshot(1))))
        .isInstanceOf(NotFoundException.class)
        .hasMessageContaining("records no dropped indexes");
    // the entry is left out because restoring it could bring back an index someone removed
    assertThat(indexUuids(registryPointer(ops))).doesNotContain("index-a");
  }

  @Test
  void droppingADamagedEntryRemovesItFromTheRegistry() {
    TestTables.TestTableOperations ops = operations();
    SidecarIndexCatalog catalog = versionedCatalog(ops);
    catalog.createIndex(request("index-a"));
    catalog.createIndex(request("index-b"));
    deleteMetadataRoot("index-a", metadataRootOf(registryPointer(ops), "index-a"));

    // a purge of an entry whose metadata is already gone still removes the registration and what is
    // left of its tree, which is the supported repair for a damaged entry
    assertThat(catalog.dropIndex(tableDir.toString(), "index-a", true)).isTrue();

    assertThat(indexUuids(registryPointer(ops))).containsExactly("index-b");
    assertThat(catalog.damagedIndexUuids(tableDir.toString())).isEmpty();
    assertThat(catalog.listIndexes(tableDir.toString()))
        .extracting(IndexMetadata::indexUuid)
        .containsExactly("index-b");
  }

  @Test
  void droppingADamagedLegacyEntryRemovesIt() {
    writeIndex("index-legacy", "table-uuid");
    writeLegacyRegistry("index-legacy");
    new File(indexesRoot(), "index-legacy/metadata/index-metadata-v1.json").delete();
    TestTables.TestTableOperations ops = operations();
    SidecarIndexCatalog catalog = versionedCatalog(ops);

    assertThat(catalog.dropIndex(tableDir.toString(), "index-legacy", true)).isTrue();

    // the drop publishes a versioned registry; the legacy file stays for readers that predate the
    // pointer, which is the same rule a first registration follows
    assertThat(indexUuids(registryPointer(ops))).isEmpty();
    assertThat(new File(indexesRoot(), "index-registry.json")).exists();
  }

  private String rootOf(String registryFile) {
    String root = metadataRootOf(registryFile, "index-a");
    return root.substring("index-metadata-".length(), root.length() - ".json".length());
  }

  private String metadataRootOf(String registryFile, String indexUuid) {
    return registryJson(registryFile).get("metadata-files").get(indexUuid).asText();
  }

  private void dropTableUuids(String registryFile) {
    Path path = indexesRoot().toPath().resolve(registryFile);
    try {
      JsonNode node = registryJson(registryFile);
      ((com.fasterxml.jackson.databind.node.ObjectNode) node).remove("table-uuids");
      Files.write(path, node.toString().getBytes(UTF_8));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private void corruptMetadataRoot(String indexUuid, String root) {
    try {
      Files.write(
          Path.of(indexesRoot().toString(), indexUuid, "metadata", root),
          ("{\"metadata\": ").getBytes(UTF_8));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private void dropDroppedRecord(String registryFile) {
    Path path = indexesRoot().toPath().resolve(registryFile);
    try {
      JsonNode node = registryJson(registryFile);
      ((com.fasterxml.jackson.databind.node.ObjectNode) node).remove("dropped");
      Files.write(path, node.toString().getBytes(UTF_8));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private void deleteMetadataRoot(String indexUuid, String root) {
    try {
      Files.delete(Path.of(indexesRoot().toString(), indexUuid, "metadata", root));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private TestTables.TestTableOperations operations() {
    TestTables.TestTableOperations ops = new TestTables.TestTableOperations(TABLE_NAME, tableDir);
    TestTables.create(
        tableDir, TABLE_NAME, SCHEMA, PartitionSpec.unpartitioned(), SortOrder.unsorted(), 2, ops);
    return ops;
  }

  private SidecarIndexCatalog versionedCatalog(TestTables.TestTableOperations ops) {
    return new SidecarIndexCatalog(new LocalFileIO(), new BaseTable(ops, TABLE_NAME));
  }

  private CreateIndexRequest request(String indexUuid) {
    return CreateIndexRequest.builder()
        .tableLocation(tableDir.toString())
        .tableUuid("table-uuid")
        .indexUuid(indexUuid)
        .definition(TestingIndexDefinition.of(4))
        .build();
  }

  private CreateIndexRequest namedRequest(String indexUuid, String name) {
    return CreateIndexRequest.builder()
        .tableLocation(tableDir.toString())
        .tableUuid("table-uuid")
        .indexUuid(indexUuid)
        .name(name)
        .definition(TestingIndexDefinition.of(4))
        .build();
  }

  private String registryPointer(TableOperations ops) {
    return ops.refresh().properties().get(SidecarIndexCatalog.REGISTRY_PROPERTY);
  }

  private List<String> indexUuids(String registryFile) {
    List<String> uuids = new ArrayList<>();
    registryJson(registryFile).get("index-uuids").forEach(node -> uuids.add(node.asText()));
    return uuids;
  }

  private String previousFile(String registryFile) {
    JsonNode previous = registryJson(registryFile).get("previous-file");
    return previous == null || previous.isNull() ? null : previous.asText();
  }

  private JsonNode registryJson(String registryFile) {
    try {
      Path path = indexesRoot().toPath().resolve(registryFile);
      return JsonUtil.parse(new String(Files.readAllBytes(path), UTF_8), node -> node);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read " + registryFile, e);
    }
  }

  /** Registry files in version order, oldest first. */
  private List<String> registryFiles() {
    String[] names = indexesRoot().list();
    if (names == null) {
      return List.of();
    }
    return Arrays.stream(names)
        .filter(name -> name.startsWith(SidecarIndexCatalog.REGISTRY_FILE_PREFIX))
        .sorted(Comparator.comparingLong(TestVersionedIndexRegistry::versionOf))
        .collect(Collectors.toList());
  }

  private static long versionOf(String fileName) {
    String rest = fileName.substring(SidecarIndexCatalog.REGISTRY_FILE_PREFIX.length());
    return Long.parseLong(rest.substring(0, rest.indexOf('-')));
  }

  private File indexesRoot() {
    return new File(tableDir, SidecarIndexCatalog.INDEXES_DIR);
  }

  private void writeIndex(String indexUuid, String tableUuid) {
    Path metadataFile =
        Path.of(indexesRoot().toString(), indexUuid, "metadata", "index-metadata-v1.json");
    try {
      Files.createDirectories(metadataFile.getParent());
      Files.write(
          metadataFile,
          IndexMetadataParser.toJson(
                  IndexMetadata.builder()
                      .indexUuid(indexUuid)
                      .tableUuid(tableUuid)
                      .tableLocation(tableDir.toString())
                      .location(Path.of(indexesRoot().toString(), indexUuid).toString())
                      .definition(TestingIndexDefinition.of(4))
                      .build())
              .getBytes(UTF_8));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private void writeLegacyRegistry(String indexUuid) {
    try {
      Files.createDirectories(indexesRoot().toPath());
      Files.write(
          indexesRoot().toPath().resolve("index-registry.json"),
          ("{\"format-version\":1,\"index-uuids\":[\"" + indexUuid + "\"]}").getBytes(UTF_8));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Fails the first commit, standing in for a writer that lost the race. */
  private static final class FailFirstCommit implements TableOperations {
    private final TableOperations delegate;
    private final Runnable onConflict;
    private boolean failed = false;

    FailFirstCommit(TableOperations delegate) {
      this(delegate, () -> {});
    }

    FailFirstCommit(TableOperations delegate, Runnable onConflict) {
      this.delegate = delegate;
      this.onConflict = onConflict;
    }

    @Override
    public TableMetadata current() {
      return delegate.current();
    }

    @Override
    public TableMetadata refresh() {
      return delegate.refresh();
    }

    @Override
    public void commit(TableMetadata base, TableMetadata updatedMetadata) {
      if (!failed) {
        this.failed = true;
        onConflict.run();
        throw new CommitFailedException("Injected conflict");
      }
      delegate.commit(base, updatedMetadata);
    }

    @Override
    public String metadataFileLocation(String fileName) {
      return delegate.metadataFileLocation(fileName);
    }

    @Override
    public FileIO io() {
      return delegate.io();
    }

    @Override
    public LocationProvider locationProvider() {
      return delegate.locationProvider();
    }

    @Override
    public long newSnapshotId() {
      return delegate.newSnapshotId();
    }
  }

  /** Fails the first pointer move, after letting another writer commit, then behaves normally. */
  private static final class FailsFirstCommitPointer implements IndexRegistryPointer {
    private final IndexRegistryPointer delegate;
    private final Runnable beforeConflict;
    private boolean failed = false;

    FailsFirstCommitPointer(IndexRegistryPointer delegate, Runnable beforeConflict) {
      this.delegate = delegate;
      this.beforeConflict = beforeConflict;
    }

    @Override
    public String current() {
      return delegate.current();
    }

    @Override
    public boolean compareAndSet(String expected, String next) {
      if (!failed) {
        this.failed = true;
        beforeConflict.run();
        return false;
      }

      return delegate.compareAndSet(expected, next);
    }
  }

  /** Fails every commit, standing in for a pointer that keeps moving under the writer. */
  private static final class AlwaysFailCommit implements TableOperations {
    private final TableOperations delegate;

    AlwaysFailCommit(TableOperations delegate) {
      this.delegate = delegate;
    }

    @Override
    public TableMetadata current() {
      return delegate.current();
    }

    @Override
    public TableMetadata refresh() {
      return delegate.refresh();
    }

    @Override
    public void commit(TableMetadata base, TableMetadata updatedMetadata) {
      throw new CommitFailedException("Injected conflict");
    }

    @Override
    public String metadataFileLocation(String fileName) {
      return delegate.metadataFileLocation(fileName);
    }

    @Override
    public FileIO io() {
      return delegate.io();
    }

    @Override
    public LocationProvider locationProvider() {
      return delegate.locationProvider();
    }

    @Override
    public long newSnapshotId() {
      return delegate.newSnapshotId();
    }
  }
}
