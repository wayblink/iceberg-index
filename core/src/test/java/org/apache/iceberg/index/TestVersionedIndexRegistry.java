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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.TestTables.LocalFileIO;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.JsonUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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
    // registration
    // reads the metadata to decide whether the index is this table's
    String registryFile = registryPointer(ops);
    dropTableUuids(registryFile);
    deleteMetadataRoot("index-a", "index-metadata-" + rootOf(registryFile) + ".json");

    // the index's metadata is gone, and the registration has to read it to learn its table uuid
    assertThatThrownBy(() -> catalog.createIndex(request("index-b")))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Index metadata file does not exist");
  }

  private String rootOf(String registryFile) {
    String root = registryJson(registryFile).get("metadata-files").get("index-a").asText();
    return root.substring("index-metadata-".length(), root.length() - ".json".length());
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
