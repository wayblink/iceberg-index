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

import com.fasterxml.jackson.databind.JsonNode;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.UnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.apache.iceberg.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link IndexCatalog} that keeps index metadata in a self-contained sidecar tree under the
 * table location:
 *
 * <p>Layout under the table location:
 *
 * <ul>
 *   <li>{@code indexes/indexmeta.v{N}-{suffix}.json}: an immutable registry, named by the table
 *       property {@link #REGISTRY_PROPERTY}, that lists index UUIDs and their metadata files
 *   <li>{@code indexes/{index-uuid}/metadata/}: immutable metadata documents
 *   <li>{@code indexes/{index-uuid}/snapshots/}: manifest lists and manifests
 *   <li>{@code indexes/{index-uuid}/data/}: index files
 *   <li>{@code indexes/{index-uuid}/staging/}: per-attempt write-ahead ledgers
 * </ul>
 *
 * <p>A table commit publishes a new registry and all the metadata it names. Concurrent publishers
 * write to independent paths, and a publisher that loses the commit applies its update again to the
 * winning registry. The registry and metadata pointers are relative names.
 *
 * <p>Legacy registries name only index UUIDs. For those entries, readers follow the generation
 * pointers under the index's metadata directory until an update publishes an immutable document
 * through the registry. A standalone catalog without table operations uses this legacy layout and
 * requires a single writer.
 */
public final class SidecarIndexCatalog implements IndexCatalog {

  /** The directory, relative to the table location, that holds all index trees. */
  public static final String INDEXES_DIR = "indexes";

  /**
   * The directory, relative to {@link #INDEXES_DIR}, that holds the failed-build reports. It is
   * shared by every column of the table and named after the column, so it is not an index UUID.
   */
  public static final String BUILD_STATUS_DIR = "build-status";

  /**
   * The table property that names the current registry file, relative to the {@code indexes}
   * directory. A table without it keeps its registry in a single file written in place.
   */
  public static final String REGISTRY_PROPERTY = "index.registry";

  /** The prefix every immutable registry file name starts with, followed by its version. */
  public static final String REGISTRY_FILE_PREFIX = "indexmeta.v";

  /** How many recently written registry files are kept, the current one included. */
  public static final int VERSIONS_TO_KEEP = 10;

  private static final String REGISTRY_FILE = "index-registry.json";
  private static final Pattern REGISTRY_FILE_NAME =
      Pattern.compile("indexmeta\\.v(\\d{1,9})-[0-9a-f]{8}\\.json");
  private static final int MAX_REGISTRY_ATTEMPTS = 5;
  private static final long RETRY_MIN_WAIT_MS = 20L;
  private static final String POINTER_FILE_PREFIX = "index-metadata-pointer-";
  private static final int POINTER_SLOTS = 2;
  private static final String POINTER_GENERATION = "generation";
  private static final String POINTER_METADATA_FILE = "metadata-file";

  /** The directory, relative to an index location, that holds index metadata roots. */
  public static final String METADATA_DIR = "metadata";

  /** The directory, relative to an index location, that holds manifest lists and manifests. */
  public static final String SNAPSHOTS_DIR = "snapshots";

  /** The directory, relative to an index location, that holds index files. */
  public static final String DATA_DIR = "data";

  /** The directory, relative to an index location, that holds per-attempt write-ahead ledgers. */
  public static final String STAGING_DIR = "staging";

  private static final String REGISTRY_FORMAT_VERSION = "format-version";
  private static final String REGISTRY_INDEX_UUIDS = "index-uuids";
  private static final String REGISTRY_UPDATED_MS = "updated-ms";
  private static final String REGISTRY_PREVIOUS_FILE = "previous-file";
  private static final String REGISTRY_METADATA_FILES = "metadata-files";
  private static final String REGISTRY_TABLE_UUIDS = "table-uuids";
  private static final String REGISTRY_DROPPED = "dropped";
  private static final String METADATA_DOCUMENT = "metadata";
  private static final int REGISTRY_VERSION = 3;
  private static final int UUID_ONLY_REGISTRY_VERSION = 2;
  private static final int LEGACY_REGISTRY_VERSION = 1;

  private static final Logger LOG = LoggerFactory.getLogger(SidecarIndexCatalog.class);

  private final FileIO fileIO;
  private final IndexRegistryPointer pointer;

  /**
   * The metadata roots this catalog registered, by index uuid.
   *
   * <p>A registry entry can go missing for two reasons that need opposite answers: a writer that
   * moved the pointer from a view it read before the registration existed leaves the entry out
   * without removing it, and a writer that drops the index removes it on purpose. Only the tree the
   * catalog wrote distinguishes them, and once the registry stops naming it, this is the only place
   * that still knows it.
   */
  private final Map<String, String> registeredRoots = new ConcurrentHashMap<>();

  /**
   * Creates a catalog for a table whose registry is a single file written in place, which is what a
   * table without a pointer property has.
   */
  public SidecarIndexCatalog(FileIO fileIO) {
    this(fileIO, (IndexRegistryPointer) null);
  }

  /** Creates a catalog that registers indexes by pointing the table at a new registry file. */
  public SidecarIndexCatalog(FileIO fileIO, Table table) {
    this(fileIO, new TableIndexRegistryPointer(table));
  }

  SidecarIndexCatalog(FileIO fileIO, IndexRegistryPointer pointer) {
    this.fileIO = Preconditions.checkNotNull(fileIO, "File IO is null");
    this.pointer = pointer;
  }

  @Override
  public IndexMetadata createIndex(CreateIndexRequest request) {
    Preconditions.checkNotNull(request, "Create index request is null");
    // fail fast on an index type this build does not know
    IndexTypes.fromType(request.definition().type());

    String tableLocation = request.tableLocation();
    String indexLocation = indexLocation(tableLocation, request.indexUuid());
    Preconditions.checkArgument(
        !readIndexUuids(tableLocation).contains(request.indexUuid())
            && readMetadataFile(indexLocation) == null,
        "Index already exists for table %s: %s",
        tableLocation,
        request.indexUuid());

    IndexMetadata metadata =
        IndexMetadata.builder()
            .indexUuid(request.indexUuid())
            .name(request.name())
            .tableUuid(request.tableUuid())
            .tableLocation(tableLocation)
            .location(indexLocation)
            .definition(request.definition())
            .properties(request.properties())
            .build();
    if (pointer != null) {
      register(tableLocation, request.tableUuid(), request.indexUuid(), metadata);
    } else {
      publish(metadata, indexLocation, 0);
      try {
        register(tableLocation, request.tableUuid(), request.indexUuid(), metadata);
      } catch (RuntimeException e) {
        purgeMetadata(indexLocation);
        throw e;
      }
    }

    return metadata;
  }

  /**
   * Lists the indexes of a table location that belong to the given table instance, leaving out the
   * ones a dropped table left behind.
   *
   * <p>The table's location outlives the table, so a table created again at the same location finds
   * the indexes of the one before it. They are not its, and counting them as existing would refuse
   * to build the index it needs.
   */
  @Override
  public List<IndexMetadata> listIndexes(String tableLocation, String tableUuid) {
    Preconditions.checkArgument(
        tableLocation != null && !tableLocation.isEmpty(), "Table location is empty");
    List<IndexMetadata> indexes = listIndexes(tableLocation);
    if (tableUuid == null) {
      return indexes;
    }

    List<IndexMetadata> mine = Lists.newArrayListWithCapacity(indexes.size());
    for (IndexMetadata metadata : indexes) {
      // an index written before the table uuid was recorded is given the benefit of the doubt
      if (metadata.tableUuid() == null || tableUuid.equals(metadata.tableUuid())) {
        mine.add(metadata);
      } else {
        LOG.warn(
            "Ignoring index {} of table {}: it was built for table instance {}, not {}",
            metadata.indexUuid(),
            tableLocation,
            metadata.tableUuid(),
            tableUuid);
      }
    }

    return mine;
  }

  @Override
  public IndexMetadata loadIndex(String tableLocation, String indexUuid) {
    Preconditions.checkArgument(
        tableLocation != null && !tableLocation.isEmpty(), "Table location is empty");
    Preconditions.checkArgument(indexUuid != null && !indexUuid.isEmpty(), "Index UUID is empty");
    Map<String, String> registry = registryEntries(tableLocation);
    IndexMetadata metadata = loadRegistered(tableLocation, indexUuid, registry);
    Preconditions.checkArgument(
        metadata != null, "Index not found for table %s: %s", tableLocation, indexUuid);
    return metadata;
  }

  @Override
  public List<IndexMetadata> listIndexes(String tableLocation) {
    List<IndexMetadata> indexes = Lists.newArrayList();
    Map<String, String> registry = registryEntries(tableLocation);
    for (Map.Entry<String, String> entry : registry.entrySet()) {
      String indexUuid = entry.getKey();
      IndexMetadata metadata = readRegisteredEntry(tableLocation, indexUuid, entry.getValue());
      if (metadata == null) {
        // one entry whose metadata is gone must not fail the listing: the other indexes are still
        // usable, and damagedIndexUuids reports this one so a drop can remove it
        LOG.warn(
            "Skipping index {} of table {}: its metadata at {} is gone; drop it to repair the "
                + "registry",
            indexUuid,
            tableLocation,
            entry.getValue() == null
                ? indexLocation(tableLocation, indexUuid)
                : metadataRootPath(indexLocation(tableLocation, indexUuid), entry.getValue()));
      } else {
        indexes.add(metadata);
      }
    }
    return ImmutableList.copyOf(indexes);
  }

  @Override
  public Set<String> damagedIndexUuids(String tableLocation) {
    Preconditions.checkArgument(
        tableLocation != null && !tableLocation.isEmpty(), "Table location is empty");
    Set<String> damaged = new LinkedHashSet<>();
    Map<String, String> registry = registryEntries(tableLocation);
    for (Map.Entry<String, String> entry : registry.entrySet()) {
      if (readRegisteredEntry(tableLocation, entry.getKey(), entry.getValue()) == null) {
        damaged.add(entry.getKey());
      }
    }
    return damaged;
  }

  @Override
  public IndexMetadata updateIndex(IndexMetadata metadata) {
    Preconditions.checkNotNull(metadata, "Index metadata is null");
    Preconditions.checkArgument(
        metadata.location() != null && !metadata.location().isEmpty(), "Index location is empty");
    if (pointer != null) {
      return updateRegistered(metadata.tableLocation(), metadata.indexUuid(), current -> metadata);
    }

    String indexLocation = metadata.location();
    Pointer current = currentPointer(indexLocation);
    publish(metadata, indexLocation, current == null ? 0 : current.generation);
    return metadata;
  }

  /**
   * Applies a change to current index metadata. A table-backed catalog commits the change through
   * the registry. A standalone catalog requires callers to serialize mutations.
   */
  @Override
  public IndexMetadata updateIndex(
      String tableLocation, String indexUuid, UnaryOperator<IndexMetadata> change) {
    Preconditions.checkArgument(
        tableLocation != null && !tableLocation.isEmpty(), "Table location is empty");
    Preconditions.checkArgument(indexUuid != null && !indexUuid.isEmpty(), "Index UUID is empty");
    Preconditions.checkNotNull(change, "Index metadata change is null");

    if (pointer != null) {
      return updateRegistered(tableLocation, indexUuid, change);
    }

    String indexLocation = indexLocation(tableLocation, indexUuid);
    for (int attempt = 1; attempt <= MAX_REGISTRY_ATTEMPTS; attempt += 1) {
      MetadataDocument current = currentDocument(indexLocation);
      if (current == null) {
        throw new IllegalArgumentException(
            "Index not found for table " + tableLocation + ": " + indexUuid);
      }

      IndexMetadata next = change.apply(IndexMetadataParser.fromJson(current.json));
      Preconditions.checkArgument(
          next != null, "Index metadata change returned null for index %s", indexUuid);
      Preconditions.checkArgument(
          indexUuid.equals(next.indexUuid()),
          "Index metadata change returned index %s, expected %s",
          next.indexUuid(),
          indexUuid);
      Preconditions.checkArgument(
          indexLocation.equals(next.location()),
          "Index metadata change moved index %s from %s to %s",
          indexUuid,
          indexLocation,
          next.location());

      // the publication exists to claim exactly the generation after the document the change was
      // applied to, so a writer that loses that generation re-applies its change to whatever the
      // winner published instead of publishing above a state it never saw
      long generation = current.generation + 1;
      if (publishDocument(indexLocation, generation, IndexMetadataParser.toJson(next))) {
        return next;
      }

      if (attempt == MAX_REGISTRY_ATTEMPTS) {
        throw new CommitFailedException(
            "Failed to update index metadata at %s after %s attempts: another publisher kept"
                + " writing a newer generation",
            indexLocation, MAX_REGISTRY_ATTEMPTS);
      }

      backoff(attempt);
    }

    throw new CommitFailedException("Failed to update index metadata for index %s", indexUuid);
  }

  private IndexMetadata updateRegistered(
      String tableLocation, String indexUuid, UnaryOperator<IndexMetadata> change) {
    for (int attempt = 1; attempt <= MAX_REGISTRY_ATTEMPTS; attempt += 1) {
      String previous = pointer.current();
      Registry registry = readRegistry(tableLocation, previous);
      Map<String, String> entries = registry.roots();
      IndexMetadata current = loadRegistered(tableLocation, indexUuid, entries);
      if (current == null) {
        // the registry names no tree for this index: either an entry a writer that does not compare
        // the pointer left out, or an index that was dropped while this one was building it
        if (attempt == MAX_REGISTRY_ATTEMPTS) {
          throw new CommitFailedException(
              "Failed to restore the registration of index %s for table %s after %s attempts",
              indexUuid, tableLocation, MAX_REGISTRY_ATTEMPTS);
        }

        if (restoreLostRegistration(tableLocation, previous, registry, indexUuid)) {
          continue;
        }

        backoff(attempt);
        continue;
      }

      IndexMetadata next = change.apply(current);
      Preconditions.checkArgument(
          next != null
              && indexUuid.equals(next.indexUuid())
              && current.location().equals(next.location()),
          "Invalid metadata update for index %s at %s",
          indexUuid,
          current.location());
      if (current.equals(next)) {
        return current;
      }

      String root = writeMetadataRoot(current.location(), next, entries.get(indexUuid));
      entries.put(indexUuid, root);
      if (next.tableUuid() != null) {
        registry.tableUuids().put(indexUuid, next.tableUuid());
      } else {
        registry.tableUuids().remove(indexUuid);
      }

      if (commitRegistry(tableLocation, previous, registry)) {
        // the latest tree of an index this catalog registered: a registration lost later is
        // restored from it, so what it restores is everything published up to that point
        registeredRoots.put(indexUuid, root);
        return next;
      }

      deleteIfExists(metadataRootPath(current.location(), root));
      if (attempt < MAX_REGISTRY_ATTEMPTS) {
        backoff(attempt);
      }
    }

    throw new CommitFailedException("Failed to update index metadata for index %s", indexUuid);
  }

  /**
   * Puts back a registration the registry no longer names, or refuses when the index was dropped.
   *
   * <p>The tree this catalog wrote is the only evidence that the index was registered at all: an
   * entry that a writer moved the pointer past is restored from it, while an entry a drop removed
   * is not, because publishing into a dropped index would make it current again, which is what the
   * drop asked not to happen.
   *
   * @return true when the registry now names the tree again, false when another writer moved the
   *     pointer first
   */
  private boolean restoreLostRegistration(
      String tableLocation, String previous, Registry registry, String indexUuid) {
    String root = registeredRoots.get(indexUuid);
    if (root == null) {
      throw new NotFoundException(
          "Index %s of table %s was dropped while it was being updated: the registry no longer "
              + "names it, and this catalog did not register it",
          indexUuid, tableLocation);
    }

    if (registry.dropped().contains(indexUuid)) {
      throw new NotFoundException(
          "Index %s of table %s was dropped while it was being updated: the registry records that "
              + "it was dropped",
          indexUuid, tableLocation);
    }

    if (namedInHistory(tableLocation, previous, indexUuid)) {
      throw new NotFoundException(
          "Index %s of table %s was dropped while it was being updated: a later registry version "
              + "removed the entry",
          indexUuid, tableLocation);
    }

    if (!registry.hasDroppedRecord()) {
      // the registry was written before the drop record existed: a writer that never saw a drop
      // looks exactly like a writer that was moved past the entry, and restoring the second would
      // bring back an index someone removed. The registration is left missing and named here rather
      // than restored on a guess.
      throw new NotFoundException(
          "Index %s of table %s is not named by the registry, which records no dropped indexes: a "
              + "writer of an older build could have removed the entry without recording it, so it "
              + "is not restored. Rebuild the index if it is needed, or drop the registration to "
              + "repair the table",
          indexUuid, tableLocation);
    }

    String path = metadataRootPath(indexLocation(tableLocation, indexUuid), root);
    String json = readIfExists(path);
    if (json == null) {
      throw new NotFoundException(
          "Index %s of table %s was dropped while it was being updated: its metadata at %s is gone",
          indexUuid, tableLocation, path);
    }

    IndexMetadata metadata =
        JsonUtil.parse(
            json, node -> IndexMetadataParser.fromJson(JsonUtil.get(METADATA_DOCUMENT, node)));
    registry.roots().put(indexUuid, root);
    if (metadata.tableUuid() != null) {
      registry.tableUuids().put(indexUuid, metadata.tableUuid());
    } else {
      registry.tableUuids().remove(indexUuid);
    }

    return commitRegistry(tableLocation, previous, registry);
  }

  /**
   * Whether a registry version the current one descends from named the index.
   *
   * <p>A registration only adds an entry and a drop only removes one, so a version in the chain
   * that names an index the current registry does not is a removal. A registration that a writer
   * overwrote without comparing the pointer leaves no trace in the chain: the registry it wrote
   * descends from the state it read, which never named the index.
   *
   * <p>The walk is bounded by the versions that are kept, because the chain is the only index of
   * them: the first version whose file was pruned ends it.
   */
  private boolean namedInHistory(String tableLocation, String registryFile, String indexUuid) {
    String file = registryFile;
    for (int depth = 0; file != null && depth < VERSIONS_TO_KEEP; depth += 1) {
      String json = readIfExists(registryPath(tableLocation, file));
      if (json == null) {
        return false;
      }

      JsonNode node = JsonUtil.parse(json, candidate -> candidate);
      for (JsonNode named : JsonUtil.get(REGISTRY_INDEX_UUIDS, node)) {
        if (indexUuid.equals(named.asText())) {
          return true;
        }
      }

      file = previousFile(node);
    }

    return false;
  }

  @Override
  public boolean dropIndex(String tableLocation, String indexUuid, boolean purge) {
    Preconditions.checkArgument(
        tableLocation != null && !tableLocation.isEmpty(), "Table location is empty");
    Preconditions.checkArgument(indexUuid != null && !indexUuid.isEmpty(), "Index UUID is empty");
    if (pointer != null) {
      return dropRegistered(tableLocation, indexUuid, purge);
    }

    if (!readIndexUuids(tableLocation).contains(indexUuid)) {
      return false;
    }

    // load the metadata before unregistering, because purging reads it from disk afterwards; a
    // registration whose metadata is already gone is still removed, which is what repairs the
    // registry
    IndexMetadata metadata = null;
    if (purge) {
      try {
        metadata = loadIndex(tableLocation, indexUuid);
      } catch (RuntimeException e) {
        LOG.warn(
            "Dropping index {} of table {} without its metadata: it is already unreadable",
            indexUuid,
            tableLocation,
            e);
      }
    }

    // unregister first: a registration whose files are already gone cannot be loaded, while files
    // left behind by a failed purge are only garbage
    unregister(tableLocation, indexUuid);

    if (metadata != null) {
      purgeIndexFiles(metadata);
    }

    // the documents this registration named are part of the index: a purge removes them even when
    // the metadata could not be read, which is what makes a drop of a damaged entry a repair
    if (purge) {
      purgeMetadataRoots(indexLocation(tableLocation, indexUuid), null);
    }

    return true;
  }

  private void register(
      String tableLocation, String tableUuid, String indexUuid, IndexMetadata metadata) {
    if (pointer == null) {
      List<String> indexUuids = Lists.newArrayList();
      for (IndexMetadata existing : listIndexes(tableLocation, tableUuid)) {
        indexUuids.add(existing.indexUuid());
      }
      indexUuids.add(indexUuid);
      writeLegacyRegistry(tableLocation, indexUuids);
      return;
    }

    String root = writeMetadataRoot(metadata.location(), metadata, null);
    for (int attempt = 1; attempt <= MAX_REGISTRY_ATTEMPTS; attempt += 1) {
      String previous = pointer.current();
      Registry registry = readRegistry(tableLocation, previous);
      Map<String, String> entries = registry.roots();
      if (entries.containsKey(indexUuid)) {
        deleteIfExists(metadataRootPath(metadata.location(), root));
        throw new AlreadyExistsException("Index already exists: %s", indexUuid);
      }

      if (tableUuid != null) {
        dropForeignIndexes(tableLocation, registry, tableUuid);
      }

      IndexMetadata sameName = conflictingName(tableLocation, registry, metadata);
      if (sameName != null) {
        deleteIfExists(metadataRootPath(metadata.location(), root));
        throw new AlreadyExistsException(
            "An index named %s already exists for column field %s of table %s: %s",
            metadata.name(), metadata.definition().fieldId(), tableLocation, sameName.indexUuid());
      }

      entries.put(indexUuid, root);
      // a registration of this uuid supersedes an earlier drop record: the tree is current again by
      // the caller's own decision, so a later lost entry is restorable rather than refused
      registry.dropped().remove(indexUuid);
      if (tableUuid != null) {
        registry.tableUuids().put(indexUuid, tableUuid);
      }

      if (commitRegistry(tableLocation, previous, registry)) {
        registeredRoots.put(indexUuid, root);
        return;
      }

      if (attempt < MAX_REGISTRY_ATTEMPTS) {
        backoff(attempt);
      }
    }

    deleteIfExists(metadataRootPath(metadata.location(), root));
    throw new CommitFailedException(
        "Failed to register index %s for table %s", indexUuid, tableLocation);
  }

  /**
   * Drops the registry entries that belong to another table instance.
   *
   * <p>The registry names the table instance each index was built for, so the indexes of a table
   * dropped and created again at the same location are dropped from the registry without reading
   * their metadata: an index's metadata is only read when the registry predates the table uuids, or
   * when it does not name one for an entry.
   */
  private void dropForeignIndexes(String tableLocation, Registry registry, String tableUuid) {
    Map<String, String> entries = registry.roots();
    Map<String, String> tableUuids = registry.tableUuids();
    Iterator<Map.Entry<String, String>> iterator = entries.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<String, String> entry = iterator.next();
      String recorded = tableUuids.get(entry.getKey());
      if (recorded == null) {
        // a registry written before the table uuids existed: the metadata decides whether the entry
        // is this table's. One whose metadata is gone cannot be claimed by any table, so it is
        // dropped from the registry instead of failing the registration that would repair it.
        IndexMetadata existing =
            readRegisteredEntry(tableLocation, entry.getKey(), entry.getValue());
        if (existing == null) {
          registry.dropped().add(entry.getKey());
          iterator.remove();
          continue;
        }

        recorded = existing.tableUuid();
        if (recorded == null) {
          // an index written before the registry named table instances: it cannot refuse the table
          // it is offered to, so it stays, and the next registration reads it again
          continue;
        }

        tableUuids.put(entry.getKey(), recorded);
      }

      if (!tableUuid.equals(recorded)) {
        registry.dropped().add(entry.getKey());
        iterator.remove();
        tableUuids.remove(entry.getKey());
      }
    }
  }

  private void unregister(String tableLocation, String indexUuid) {
    List<String> indexUuids = Lists.newArrayList(readIndexUuids(tableLocation));
    if (indexUuids.remove(indexUuid)) {
      writeLegacyRegistry(tableLocation, indexUuids);
    }
  }

  /**
   * Finds the index that already carries this index's name on the same column, or null.
   *
   * <p>The check runs against the registry version the registration commits on, inside the retry
   * loop: two writers that both passed a pre-check against an older version cannot both register
   * the name, because the writer whose commit loses the race re-reads the winner's registry and
   * finds the entry there. The name is read from the index metadata rather than kept in the
   * registry, which costs one read per registered index on a named creation but leaves registries
   * written before the rule in place: an entry is named where it always was.
   *
   * <p>A name is unique per column, not per table: the same name may be reused over another column,
   * which is what makes the column part of the comparison.
   */
  private IndexMetadata conflictingName(
      String tableLocation, Registry registry, IndexMetadata metadata) {
    if (metadata.name() == null) {
      return null;
    }

    int fieldId = metadata.definition().fieldId();
    for (Map.Entry<String, String> entry : registry.roots().entrySet()) {
      if (entry.getKey().equals(metadata.indexUuid())) {
        continue;
      }

      IndexMetadata existing = readRegisteredEntry(tableLocation, entry.getKey(), entry.getValue());
      if (existing != null
          && metadata.name().equals(existing.name())
          && existing.definition().fieldId() == fieldId) {
        return existing;
      }
    }

    return null;
  }

  /**
   * Reads the metadata a registry entry names, or null when it is gone.
   *
   * <p>An entry carried over from a uuid-only registry names no root, and its metadata is still
   * where the legacy readers look for it. The registry and the metadata are read separately, so a
   * caller that only consults an entry treats a missing file as the entry being gone rather than as
   * a broken registry.
   */
  private IndexMetadata readRegisteredEntry(String tableLocation, String indexUuid, String root) {
    String location = indexLocation(tableLocation, indexUuid);
    if (root == null) {
      // the metadata a uuid-only registry points at is written as the document itself, without the
      // registry wrapper the versioned entries carry
      String json = readMetadataFile(location);
      return json == null
          ? null
          : parseRegisteredEntry(tableLocation, indexUuid, location, json, false);
    }

    String path = metadataRootPath(location, root);
    String json = readIfExists(path);
    if (json == null) {
      return null;
    }

    return parseRegisteredEntry(tableLocation, indexUuid, path, json, true);
  }

  /**
   * Parses the metadata a registry entry names.
   *
   * <p>A document that is not valid JSON is indistinguishable from one whose file is gone: nothing
   * but a rewrite can read it, so the entry is treated as damaged and a drop can remove it. A valid
   * document this build cannot read is different - the file is intact and only the reader is old -
   * and is reported as that error instead of becoming a deletable entry.
   */
  private IndexMetadata parseRegisteredEntry(
      String tableLocation, String indexUuid, String path, String json, boolean document) {
    try {
      return document
          ? JsonUtil.parse(
              json, node -> IndexMetadataParser.fromJson(JsonUtil.get(METADATA_DOCUMENT, node)))
          : IndexMetadataParser.fromJson(json);
    } catch (UncheckedIOException e) {
      LOG.warn(
          "Treating index {} of table {} as damaged: its metadata at {} is not valid JSON",
          indexUuid,
          tableLocation,
          path,
          e);
      return null;
    }
  }

  private boolean dropRegistered(String tableLocation, String indexUuid, boolean purge) {
    for (int attempt = 1; attempt <= MAX_REGISTRY_ATTEMPTS; attempt += 1) {
      String previous = pointer.current();
      Registry registry = readRegistry(tableLocation, previous);
      Map<String, String> entries = registry.roots();
      if (!entries.containsKey(indexUuid)) {
        return false;
      }

      // a purge removes whatever the entry names; the metadata is read only when it can be read, so
      // an entry whose metadata is already gone is still removable, which is the repair path
      IndexMetadata metadata =
          purge ? readRegisteredEntry(tableLocation, indexUuid, entries.get(indexUuid)) : null;
      String root = entries.remove(indexUuid);
      registry.tableUuids().remove(indexUuid);
      // the record outlives the versions that name the entry: an old writer that never saw the drop
      // must still be refused once the evidence has been pruned
      registry.dropped().add(indexUuid);
      if (commitRegistry(tableLocation, previous, registry)) {
        registeredRoots.remove(indexUuid);
        if (purge) {
          if (metadata != null) {
            purgeIndexFiles(metadata);
          }

          purgeMetadataRoots(indexLocation(tableLocation, indexUuid), root);
          // a tree written before the pointer named a single document, and a registration that
          // outlived the switch can still have one under the same directory
          deleteIfExists(legacyMetadataPath(indexLocation(tableLocation, indexUuid)));
        }

        return true;
      }

      if (attempt < MAX_REGISTRY_ATTEMPTS) {
        backoff(attempt);
      }
    }

    throw new CommitFailedException(
        "Failed to drop index %s from table %s", indexUuid, tableLocation);
  }

  private boolean commitRegistry(String tableLocation, String previous, Registry registry) {
    String next = writeRegistryVersion(tableLocation, registry, previous);
    if (pointer.compareAndSet(previous, next)) {
      pruneOldVersions(tableLocation, next);
      return true;
    }

    deleteIfExists(registryPath(tableLocation, next));
    return false;
  }

  private IndexMetadata loadRegistered(
      String tableLocation, String indexUuid, Map<String, String> registry) {
    String location = indexLocation(tableLocation, indexUuid);
    String root = registry.get(indexUuid);
    if (root != null) {
      IndexMetadata metadata = readRegisteredEntry(tableLocation, indexUuid, root);
      Preconditions.checkState(
          metadata != null,
          "Index metadata file does not exist: %s",
          metadataRootPath(location, root));
      return metadata;
    }

    if (pointer != null && !registry.containsKey(indexUuid)) {
      return null;
    }

    return readRegisteredEntry(tableLocation, indexUuid, null);
  }

  private String writeMetadataRoot(String indexLocation, IndexMetadata metadata, String previous) {
    String name = "index-metadata-" + UUID.randomUUID() + ".json";
    String json =
        JsonUtil.generate(
            generator -> {
              generator.writeStartObject();
              generator.writeFieldName(METADATA_DOCUMENT);
              IndexMetadataParser.toJson(metadata, generator);
              if (previous != null) {
                generator.writeStringField(REGISTRY_PREVIOUS_FILE, previous);
              }
              generator.writeEndObject();
            },
            false);
    writeString(metadataRootPath(indexLocation, name), json, true);
    return name;
  }

  private void purgeMetadataRoots(String indexLocation, String root) {
    String current = root;
    while (current != null) {
      String path = metadataRootPath(indexLocation, current);
      String json = readIfExists(path);
      if (json == null) {
        break;
      }

      // the file goes even when it cannot be parsed: a purge of a damaged entry is exactly the case
      // where the document is unreadable, and leaving it behind would keep what the drop removed
      deleteIfExists(path);
      try {
        current = JsonUtil.parse(json, SidecarIndexCatalog::previousFile);
      } catch (UncheckedIOException e) {
        // a document that cannot be parsed cannot name the version before it: the chain ends here
        LOG.warn(
            "Stopped walking the metadata roots of {}: {} cannot be parsed",
            indexLocation,
            path,
            e);
        break;
      }
    }

    // the slots name the document each generation published, and a purge removes them with the
    // documents they name: nothing else ever reads a slot of an index that is gone
    for (int slot = 0; slot < POINTER_SLOTS; slot += 1) {
      Pointer pointer = readPointer(indexLocation, slot);
      if (pointer != null) {
        deleteIfExists(pointer.file);
      }

      deleteIfExists(pointerPath(indexLocation, slot));
    }

    deleteIfExists(legacyMetadataPath(indexLocation));
  }

  private static String metadataRootPath(String indexLocation, String name) {
    return indexLocation + "/" + METADATA_DIR + "/" + name;
  }

  private static void backoff(int attempt) {
    long waitMs = RETRY_MIN_WAIT_MS << Math.min(attempt - 1, 3);
    try {
      Thread.sleep(waitMs + (long) (Math.random() * waitMs));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new CommitFailedException(
          e, "Interrupted while waiting to retry an index registration");
    }
  }

  private void purgeIndexFiles(IndexMetadata metadata) {
    Set<String> manifestLists = new HashSet<>();
    Set<String> manifests = new HashSet<>();
    Set<String> data = new HashSet<>();
    for (IndexSnapshot snapshot : metadata.snapshots()) {
      if (snapshot.statisticsLocation() != null) {
        data.add(snapshot.statisticsLocation());
      }

      if (manifestLists.add(snapshot.indexManifestList())) {
        IndexManifestList manifestList = readManifestList(snapshot.indexManifestList());
        for (IndexManifestList.ManifestRef ref : manifestList.manifests()) {
          if (manifests.add(ref.location())) {
            for (IndexFile indexFile : readManifest(ref.location()).indexFiles()) {
              data.add(indexFile.location());
            }
          }
        }
      }
    }

    data.forEach(this::deleteIfExists);
    manifests.forEach(this::deleteIfExists);
    manifestLists.forEach(this::deleteIfExists);
    purgeMetadata(metadata.location());
  }

  /** Deletes every metadata version, both pointer slots and the pre-pointer layout. */
  private void purgeMetadata(String indexLocation) {
    Pointer current = currentPointer(indexLocation);
    long lastGeneration = current == null ? 1 : current.generation;
    for (long generation = 1; generation <= lastGeneration; generation++) {
      deleteIfExists(metadataPath(indexLocation, generation));
    }

    for (int slot = 0; slot < POINTER_SLOTS; slot++) {
      // a publication that a writer withdrew leaves a file under a name derived from the attempt,
      // and a slot can still name one of those if the writer never settled it
      Pointer pointer = readPointer(indexLocation, slot);
      if (pointer != null) {
        deleteIfExists(pointer.file);
      }

      deleteIfExists(pointerPath(indexLocation, slot));
    }
    // indexes created before the pointer switch kept a single metadata file
    deleteIfExists(legacyMetadataPath(indexLocation));
  }

  /**
   * Publishes a metadata document above the generations the caller has seen.
   *
   * @param baseGeneration the newest generation the caller has seen
   */
  private void publish(IndexMetadata metadata, String indexLocation, long baseGeneration) {
    String json = IndexMetadataParser.toJson(metadata);
    for (int attempt = 1; attempt <= MAX_REGISTRY_ATTEMPTS; attempt += 1) {
      long generation = baseGeneration + attempt;
      if (publishDocument(indexLocation, generation, json)) {
        return;
      }

      if (attempt == MAX_REGISTRY_ATTEMPTS) {
        throw new CommitFailedException(
            "Failed to publish index metadata at %s: the pointer kept naming another publisher's"
                + " document for %s generations",
            indexLocation, MAX_REGISTRY_ATTEMPTS);
      }

      backoff(attempt);
    }
  }

  /**
   * Publishes a document as one generation's content, and says whether this writer owns it.
   *
   * <p>A publication is two facts: a complete document, and a pointer naming it. The document is
   * written under a name unique to this attempt, because {@link FileIO} offers no exclusive create
   * to arbitrate with — the local one probes and then opens, so two writers can both pass the
   * probe, and an object store overwrites without asking. The pointer is what decides: a writer
   * whose claim the slot names owns the generation, and the other one withdraws its claim and
   * publishes above the winner's document instead.
   *
   * <p>The owner then writes the name derived from the generation and points the slot at it, which
   * is what lets the tree be swept by generation. Only the owner writes that name, and it does so
   * only while the slot still names its own claim, so two writers cannot both settle the same
   * generation: whichever finds its claim replaced withdraws and retries one generation later.
   */
  private boolean publishDocument(String indexLocation, long generation, String json) {
    String candidate = candidateMetadataPath(indexLocation, generation);
    writeString(candidate, json, true);

    if (!claimSlot(indexLocation, generation, candidate)) {
      deleteIfExists(candidate);
      return false;
    }

    String file = metadataPath(indexLocation, generation);
    if (!settleSlot(indexLocation, generation, candidate, file)) {
      // another writer claimed this generation in the same slot and its claim is the one the slot
      // names, so this document never became the current one
      deleteIfExists(candidate);
      return false;
    }

    // only the writer that settled the generation writes its published name; a writer that lost
    // never touches it, which is what keeps a loss from destroying the winner's document
    writeString(file, json, false);

    Pointer newest = currentPointer(indexLocation);
    if (newest == null || newest.generation != generation) {
      // a publisher that got further while this one was writing has the document readers follow, so
      // this publication is withdrawn instead of being reported as the published one
      deleteIfExists(file);
      deleteIfExists(candidate);
      return false;
    }

    deleteIfExists(candidate);
    return true;
  }

  /**
   * Claims a generation by pointing its slot at this writer's document, unless the slot already
   * names that generation or a later one.
   *
   * <p>The claim is tentative: two writers that read the slot before either writes can both claim
   * the same generation, and {@link #settleSlot} is what tells them apart afterwards.
   */
  private boolean claimSlot(String indexLocation, long generation, String candidate) {
    int slot = slotFor(generation);
    Pointer present = readPointer(indexLocation, slot);
    if (present != null && present.generation >= generation) {
      return false;
    }

    writePointer(indexLocation, slot, generation, candidate);
    Pointer written = readPointer(indexLocation, slot);
    return written != null && written.generation == generation && candidate.equals(written.file);
  }

  /**
   * Turns this writer's claim into the generation's published name, when the slot still names it.
   *
   * <p>This is where a generation gets exactly one owner. Two writers can both claim it, but the
   * slot names one document at a time, so only the writer whose claim is named here settles it; the
   * other one finds the slot naming something else and withdraws.
   */
  private boolean settleSlot(String indexLocation, long generation, String candidate, String file) {
    int slot = slotFor(generation);
    Pointer present = readPointer(indexLocation, slot);
    if (present == null || present.generation != generation || !candidate.equals(present.file)) {
      return false;
    }

    writePointer(indexLocation, slot, generation, file);
    Pointer written = readPointer(indexLocation, slot);
    return written != null && written.generation == generation && file.equals(written.file);
  }

  /**
   * Points one slot at a generation's document.
   *
   * <p>A slot is overwritten in place, and the local file system implements an overwrite as a
   * delete followed by a create, so a writer that arrives in between makes this one's create fail
   * even though nothing is wrong: what counts is that a pointer lands, so the write is retried
   * rather than reported as a publication failure.
   */
  private void writePointer(String indexLocation, int slot, long generation, String metadataFile) {
    String path = pointerPath(indexLocation, slot);
    String json =
        JsonUtil.generate(
            generator -> {
              generator.writeStartObject();
              generator.writeNumberField(POINTER_GENERATION, generation);
              generator.writeStringField(POINTER_METADATA_FILE, metadataFile);
              generator.writeEndObject();
            },
            false);

    RuntimeException failure = null;
    for (int attempt = 1; attempt <= MAX_REGISTRY_ATTEMPTS; attempt += 1) {
      try {
        writeString(path, json, false);
        return;
      } catch (AlreadyExistsException | RuntimeIOException e) {
        failure = e;
      }
    }

    throw failure;
  }

  /** The slot a generation lives in; the two alternate, so a reader can fall back to the older. */
  private static int slotFor(long generation) {
    return (int) ((generation - 1) % POINTER_SLOTS);
  }

  /** Reads the current metadata JSON, following the pointer, with a legacy layout fallback. */
  private String readMetadataFile(String indexLocation) {
    MetadataDocument document = currentDocument(indexLocation);
    return document == null ? null : document.json;
  }

  /**
   * The current metadata document together with the generation the pointer names for it.
   *
   * <p>The two are one value because they have to be read as one: the generation a publication
   * claims has to be the generation of the document it derived its change from. An index that still
   * uses the pre-pointer layout has a single document and no generation, which reads as zero so
   * that the first publication lands in generation one.
   */
  private MetadataDocument currentDocument(String indexLocation) {
    MetadataDocument newest = null;
    for (int slot = 0; slot < POINTER_SLOTS; slot += 1) {
      Pointer pointer = readPointer(indexLocation, slot);
      if (pointer == null || (newest != null && pointer.generation <= newest.generation)) {
        continue;
      }

      String json = readIfExists(pointer.file);
      if (json == null) {
        // a slot can name a document that was withdrawn, or one a writer is between claiming and
        // publishing; the other slot still holds a complete document, which is stale but whole
        continue;
      }

      newest = new MetadataDocument(pointer.generation, json);
    }

    if (newest != null) {
      return newest;
    }

    String legacy = readIfExists(legacyMetadataPath(indexLocation));
    return legacy == null ? null : new MetadataDocument(0L, legacy);
  }

  /** Returns the pointer with the higher generation, or null if neither slot holds one. */
  private Pointer currentPointer(String indexLocation) {
    Pointer newest = null;
    for (int slot = 0; slot < POINTER_SLOTS; slot++) {
      Pointer pointer = readPointer(indexLocation, slot);
      if (pointer != null && (newest == null || pointer.generation > newest.generation)) {
        newest = pointer;
      }
    }
    return newest;
  }

  private Pointer readPointer(String indexLocation, int slot) {
    String json = readIfExists(pointerPath(indexLocation, slot));
    if (json == null) {
      return null;
    }
    try {
      JsonNode node = JsonUtil.parse(json, unused -> unused);
      return new Pointer(
          JsonUtil.getLong(POINTER_GENERATION, node),
          JsonUtil.getString(POINTER_METADATA_FILE, node));
    } catch (RuntimeException e) {
      // a pointer is a two-number document, so a parse failure means the file was truncated by a
      // writer that did not finish; the other slot is tried next
      return null;
    }
  }

  private static final class MetadataDocument {
    private final long generation;
    private final String json;

    private MetadataDocument(long generation, String json) {
      this.generation = generation;
      this.json = json;
    }
  }

  private static final class Pointer {
    private final long generation;
    private final String file;

    private Pointer(long generation, String file) {
      this.generation = generation;
      this.file = file;
    }
  }

  private IndexManifestList readManifestList(String location) {
    try {
      return IndexManifestList.read(fileIO.newInputFile(location));
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read index manifest list: " + location, e);
    }
  }

  private IndexManifest readManifest(String location) {
    try {
      return IndexManifest.read(fileIO.newInputFile(location));
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read index manifest: " + location, e);
    }
  }

  private List<String> readIndexUuids(String tableLocation) {
    return ImmutableList.copyOf(registryEntries(tableLocation).keySet());
  }

  private Map<String, String> registryEntries(String tableLocation) {
    return readRegistryEntries(tableLocation, pointer == null ? null : pointer.current());
  }

  /** One version of a table's index registry. */
  private static final class Registry {
    private final Map<String, String> roots;
    private final Map<String, String> tableUuids;
    private final Set<String> dropped;
    private final boolean hasDroppedRecord;

    private Registry(
        Map<String, String> roots,
        Map<String, String> tableUuids,
        Set<String> dropped,
        boolean hasDroppedRecord) {
      this.roots = roots;
      this.tableUuids = tableUuids;
      this.dropped = dropped;
      this.hasDroppedRecord = hasDroppedRecord;
    }

    /** The tree each index is published under, by index uuid. */
    private Map<String, String> roots() {
      return roots;
    }

    /** The table instance each index was built for, by index uuid, when the registry names one. */
    private Map<String, String> tableUuids() {
      return tableUuids;
    }

    /**
     * The indexes this registry records as dropped.
     *
     * <p>A removal cannot be told from a registration a writer never saw by reading the entries
     * alone, and the versions that still named the index are pruned: the record is what keeps a
     * dropped index dropped once its history is gone. Registries written before the record existed
     * read as one that records no drops.
     */
    private Set<String> dropped() {
      return dropped;
    }

    /**
     * Whether the document this registry was read from carries the record field at all.
     *
     * <p>An empty set and a document written before the field existed read the same through {@link
     * #dropped}, but they mean opposite things when a registration is missing: one says nothing was
     * dropped, the other says a writer of an older build could have dropped it without recording
     * that. Only the first is safe to restore.
     */
    private boolean hasDroppedRecord() {
      return hasDroppedRecord;
    }
  }

  /** Reads the uuids of the registry file the table points at, or of the legacy file when none. */
  private Map<String, String> readRegistryEntries(String tableLocation, String registryFile) {
    return readRegistry(tableLocation, registryFile).roots();
  }

  /**
   * Reads the registry the table points at, or the legacy file when the table names none.
   *
   * <p>The table uuids are an addition to the version 3 document: a registry written before them
   * parses as one that names none, and registering then reads each index's metadata, which is what
   * this map is meant to save.
   */
  private Registry readRegistry(String tableLocation, String registryFile) {
    String json = readIfExists(registryPath(tableLocation, registryFile));
    if (json == null) {
      Preconditions.checkArgument(
          registryFile == null,
          "The table points at registry file %s, which does not exist. The table property %s is "
              + "set to a file that was deleted or never written, so what the table holds is not "
              + "known; unset it to fall back to the file written in place",
          registryFile,
          REGISTRY_PROPERTY);
      // there is no registry document, so there is nothing whose record of drops could have been
      // lost: a writer never removed an entry of a document that does not exist
      return new Registry(
          new LinkedHashMap<>(), new LinkedHashMap<>(), new LinkedHashSet<>(), true);
    }

    JsonNode registryNode = JsonUtil.parse(json, node -> node);
    int version = JsonUtil.getInt(REGISTRY_FORMAT_VERSION, registryNode);
    Preconditions.checkArgument(
        version == LEGACY_REGISTRY_VERSION
            || version == UUID_ONLY_REGISTRY_VERSION
            || version == REGISTRY_VERSION,
        "Unsupported index registry format version: %s in %s",
        version,
        registryFile);

    Map<String, String> files =
        version == REGISTRY_VERSION
            ? JsonUtil.getStringMap(REGISTRY_METADATA_FILES, registryNode)
            : Map.of();
    Map<String, String> recordedUuids =
        version == REGISTRY_VERSION
            ? JsonUtil.getStringMapOrNull(REGISTRY_TABLE_UUIDS, registryNode)
            : null;
    List<String> recordedDrops =
        version == REGISTRY_VERSION
            ? JsonUtil.getStringListOrNull(REGISTRY_DROPPED, registryNode)
            : null;
    Map<String, String> entries = new LinkedHashMap<>();
    Map<String, String> tableUuids = new LinkedHashMap<>();
    for (JsonNode indexUuid : JsonUtil.get(REGISTRY_INDEX_UUIDS, registryNode)) {
      String uuid = indexUuid.asText();
      entries.put(uuid, files.get(uuid));
      if (recordedUuids != null && recordedUuids.get(uuid) != null) {
        tableUuids.put(uuid, recordedUuids.get(uuid));
      }
    }

    Set<String> dropped = new LinkedHashSet<>();
    if (recordedDrops != null) {
      dropped.addAll(recordedDrops);
    }

    // the field, not the set, tells a document written before the record existed from one that
    // records no drops: an older writer could have removed an entry without recording it
    boolean hasDroppedRecord = version == REGISTRY_VERSION && registryNode.has(REGISTRY_DROPPED);
    return new Registry(entries, tableUuids, dropped, hasDroppedRecord);
  }

  /** Writes a new registry version and returns its file name, which is never reused. */
  private String writeRegistryVersion(
      String tableLocation, Registry registry, String previousFile) {
    String fileName =
        REGISTRY_FILE_PREFIX
            + nextVersion(previousFile)
            + "-"
            + UUID.randomUUID().toString().replace("-", "").substring(0, 8)
            + ".json";
    writeString(registryPath(tableLocation, fileName), registryJson(registry, previousFile), true);
    return fileName;
  }

  /** The registry a table without a pointer property keeps, written in place. */
  private void writeLegacyRegistry(String tableLocation, List<String> indexUuids) {
    String json =
        JsonUtil.generate(
            generator -> {
              generator.writeStartObject();
              generator.writeNumberField(REGISTRY_FORMAT_VERSION, LEGACY_REGISTRY_VERSION);
              generator.writeArrayFieldStart(REGISTRY_INDEX_UUIDS);
              for (String indexUuid : indexUuids) {
                generator.writeString(indexUuid);
              }
              generator.writeEndArray();
              generator.writeEndObject();
            },
            false);
    writeString(registryPath(tableLocation, null), json, false);
  }

  private static String registryJson(Registry registry, String previousFile) {
    Map<String, String> entries = registry.roots();
    Map<String, String> roots = new LinkedHashMap<>();
    entries.forEach(
        (uuid, root) -> {
          if (root != null) {
            roots.put(uuid, root);
          }
        });
    Map<String, String> tableUuids = new LinkedHashMap<>();
    entries
        .keySet()
        .forEach(
            uuid -> {
              String tableUuid = registry.tableUuids().get(uuid);
              if (tableUuid != null) {
                tableUuids.put(uuid, tableUuid);
              }
            });
    return JsonUtil.generate(
        generator -> {
          generator.writeStartObject();
          generator.writeNumberField(REGISTRY_FORMAT_VERSION, REGISTRY_VERSION);
          generator.writeNumberField(REGISTRY_UPDATED_MS, System.currentTimeMillis());
          generator.writeArrayFieldStart(REGISTRY_INDEX_UUIDS);
          for (String indexUuid : entries.keySet()) {
            generator.writeString(indexUuid);
          }
          generator.writeEndArray();
          JsonUtil.writeStringMap(REGISTRY_METADATA_FILES, roots, generator);
          if (!tableUuids.isEmpty()) {
            JsonUtil.writeStringMap(REGISTRY_TABLE_UUIDS, tableUuids, generator);
          }

          // the field, not the set, tells a document of this build from one written before the drop
          // record existed: a reader restores a lost registration only from a document that could
          // have recorded the drop, so an empty set is still written
          generator.writeArrayFieldStart(REGISTRY_DROPPED);
          for (String droppedUuid : registry.dropped()) {
            generator.writeString(droppedUuid);
          }
          generator.writeEndArray();

          if (previousFile != null) {
            generator.writeStringField(REGISTRY_PREVIOUS_FILE, previousFile);
          }
          generator.writeEndObject();
        },
        false);
  }

  private static long nextVersion(String previousFile) {
    if (previousFile == null) {
      return 1L;
    }

    Matcher matcher = REGISTRY_FILE_NAME.matcher(previousFile);
    return matcher.matches() ? Long.parseLong(matcher.group(1)) + 1 : 1L;
  }

  /**
   * Deletes the registry versions beyond the ones that are kept, walking back from the current one.
   *
   * <p>The chain of versions is the only index of them: {@link FileIO} cannot list a directory, and
   * the file names carry a random suffix so that two writers never write the same version file.
   * Pruning is best effort, because an unreferenced registry file is only a small leftover.
   */
  private void pruneOldVersions(String tableLocation, String currentFile) {
    try {
      String file = currentFile;
      int kept = 0;
      while (file != null) {
        String json = readIfExists(registryPath(tableLocation, file));
        if (json == null) {
          return;
        }

        String previous = previousFile(JsonUtil.parse(json, node -> node));
        if (kept >= VERSIONS_TO_KEEP) {
          deleteIfExists(registryPath(tableLocation, file));
        }

        kept += 1;
        file = previous;
      }
    } catch (RuntimeException e) {
      LOG.warn("Failed to prune older index registry files of table {}", tableLocation, e);
    }
  }

  private static String previousFile(JsonNode registryNode) {
    JsonNode previous = registryNode.get(REGISTRY_PREVIOUS_FILE);
    return previous == null || previous.isNull() ? null : previous.asText();
  }

  private String readIfExists(String path) {
    InputFile inputFile = fileIO.newInputFile(path);
    try (SeekableInputStream in = inputFile.newStream()) {
      return new String(ByteStreams.toByteArray(in), StandardCharsets.UTF_8);
    } catch (FileNotFoundException | NoSuchFileException e) {
      return null;
    } catch (IOException e) {
      if (isMissing(e)) {
        return null;
      }
      throw new UncheckedIOException("Failed to read " + path, e);
    } catch (RuntimeException e) {
      if (isMissing(e)) {
        return null;
      }
      throw e;
    }
  }

  private void writeString(String path, String content, boolean createNew) {
    try (PositionOutputStream out =
        createNew
            ? fileIO.newOutputFile(path).create()
            : fileIO.newOutputFile(path).createOrOverwrite()) {
      out.write(content.getBytes(StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to write " + path, e);
    }
  }

  private void deleteIfExists(String path) {
    // probing first keeps this portable: FileIO implementations differ in how they report a
    // missing file, and some throw without a cause
    if (fileIO.newInputFile(path).exists()) {
      fileIO.deleteFile(path);
    }
  }

  private static boolean isMissing(Throwable throwable) {
    for (Throwable cause = throwable; cause != null; cause = cause.getCause()) {
      if (cause instanceof FileNotFoundException
          || cause instanceof NoSuchFileException
          || cause instanceof NotFoundException) {
        return true;
      }
    }
    return false;
  }

  private static String indexesRoot(String tableLocation) {
    String location = tableLocation;
    while (location.endsWith("/")) {
      location = location.substring(0, location.length() - 1);
    }
    return location + "/" + INDEXES_DIR;
  }

  private static String registryPath(String tableLocation, String registryFile) {
    if (registryFile == null) {
      return indexesRoot(tableLocation) + "/" + REGISTRY_FILE;
    }

    // the pointer is a file name in this directory, not a path: following anything else would let
    // a corrupted or hostile property read a file anywhere
    Preconditions.checkArgument(
        REGISTRY_FILE_NAME.matcher(registryFile).matches(),
        "Invalid index registry file: %s (expected %s{N}-{8 hex}.json)",
        registryFile,
        REGISTRY_FILE_PREFIX);
    return indexesRoot(tableLocation) + "/" + registryFile;
  }

  /**
   * The directory of one index on the table's sidecar path.
   *
   * <p>A caller repairing a registry entry it can no longer read needs it: the entry names no
   * readable tree, so the location is the only thing that identifies what is left to delete.
   */
  public static String locationOf(String tableLocation, String indexUuid) {
    Preconditions.checkArgument(
        tableLocation != null && !tableLocation.isEmpty(), "Table location is empty");
    Preconditions.checkArgument(indexUuid != null && !indexUuid.isEmpty(), "Index UUID is empty");
    return indexLocation(tableLocation, indexUuid);
  }

  private static String indexLocation(String tableLocation, String indexUuid) {
    return indexesRoot(tableLocation) + "/" + indexUuid;
  }

  private static String legacyMetadataPath(String indexLocation) {
    return indexLocation
        + "/"
        + METADATA_DIR
        + "/index-metadata-v"
        + IndexMetadata.FORMAT_VERSION
        + ".json";
  }

  /**
   * The name a publication writes before it owns the generation.
   *
   * <p>Unique per attempt, because two writers that pick the same generation must not overwrite
   * each other: the pointer decides which of the two the index follows, and the loser withdraws its
   * file.
   */
  private static String candidateMetadataPath(String indexLocation, long generation) {
    return indexLocation
        + "/"
        + METADATA_DIR
        + "/index-metadata-v"
        + IndexMetadata.FORMAT_VERSION
        + "-g"
        + generation
        + "-"
        + UUID.randomUUID()
        + ".json";
  }

  private static String metadataPath(String indexLocation, long generation) {
    return indexLocation
        + "/"
        + METADATA_DIR
        + "/index-metadata-v"
        + IndexMetadata.FORMAT_VERSION
        + "-g"
        + generation
        + ".json";
  }

  private static String pointerPath(String indexLocation, int slot) {
    return indexLocation + "/" + METADATA_DIR + "/" + POINTER_FILE_PREFIX + slot + ".json";
  }
}
