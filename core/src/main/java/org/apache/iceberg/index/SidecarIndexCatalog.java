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
import java.util.List;
import java.util.UUID;
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
 *   <li>{@code indexes/indexmeta.v{N}-{suffix}.json}: an immutable version of the registry of index
 *       UUIDs, which the table property {@link #REGISTRY_PROPERTY} names by its file name
 *   <li>{@code indexes/index-registry.json}: the registry of index UUIDs, written in place by
 *       tables that do not carry the pointer property
 *   <li>{@code indexes/{index-uuid}/metadata/index-metadata-v{N}-g{G}.json}: a version of the index
 *       metadata root, one file per publication
 *   <li>{@code indexes/{index-uuid}/metadata/index-metadata-pointer-{0,1}.json}: the pointer to the
 *       current metadata version, alternating between two slots
 *   <li>{@code indexes/{index-uuid}/snapshots/}: manifest lists and manifests
 *   <li>{@code indexes/{index-uuid}/data/}: index files
 *   <li>{@code indexes/{index-uuid}/staging/}: per-attempt write-ahead ledgers
 * </ul>
 *
 * <p>Discovery uses the registry file because {@link FileIO} exposes no listing operation, which
 * also keeps the layout portable across file systems.
 *
 * <p>The registry used to be a single file written in place, so two writers registering different
 * indexes at the same time overwrote each other and one index was left behind with no way to find
 * it. A registry file is now immutable and the table property names the current one, which makes a
 * registration a compare-and-swap through the table commit: a writer that loses the race retries
 * against the winner's registry instead of replacing it. A catalog without a pointer property, such
 * as a table built by {@code HadoopTables}, falls back to the single file and is single-writer.
 *
 * <p>The pointer is a file name relative to the {@code indexes} directory, not an absolute path, so
 * it keeps working when the table is registered or moved under a different location.
 *
 * <p>Publishing an index snapshot is ordered: a new metadata version is created as a new file and
 * only then does the pointer switch to it, so a reader always sees either the previous or the new
 * metadata root, never a partially written one. A crash before the pointer switch leaves the new
 * version as an unreferenced file, which is harmless.
 *
 * <p>The metadata file is created rather than overwritten, and its name carries the generation, so
 * the writer that creates it first owns that generation: a second publisher that read the same
 * pointer moves on to a later one instead of replacing a document a reader may be following. The
 * pointer alternates between two slots, which is a small file a crash could truncate; a reader then
 * falls back to the other slot, and a writer that finds its pointer replaced fails rather than
 * reporting a publication nobody will follow.
 */
public final class SidecarIndexCatalog implements IndexCatalog {

  /** The directory, relative to the table location, that holds all index trees. */
  public static final String INDEXES_DIR = "indexes";

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
  private static final int REGISTRY_VERSION = 2;
  private static final int LEGACY_REGISTRY_VERSION = 1;

  private static final Logger LOG = LoggerFactory.getLogger(SidecarIndexCatalog.class);

  private final FileIO fileIO;
  private final IndexRegistryPointer pointer;

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
    // the index tree is written first: registering an index whose metadata does not exist yet
    // would publish an index that cannot be loaded
    publish(metadata, indexLocation, 0);
    try {
      register(tableLocation, request.tableUuid(), request.indexUuid());
    } catch (RuntimeException e) {
      // a registration that never happened must not leave a tree nothing points at
      purgeMetadata(indexLocation);
      throw e;
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
    String json = readMetadataFile(indexLocation(tableLocation, indexUuid));
    if (json == null) {
      throw new IllegalArgumentException(
          "Index not found for table " + tableLocation + ": " + indexUuid);
    }
    return IndexMetadataParser.fromJson(json);
  }

  @Override
  public List<IndexMetadata> listIndexes(String tableLocation) {
    List<IndexMetadata> indexes = Lists.newArrayList();
    for (String indexUuid : readIndexUuids(tableLocation)) {
      String json = readMetadataFile(indexLocation(tableLocation, indexUuid));
      if (json != null) {
        indexes.add(IndexMetadataParser.fromJson(json));
      }
    }
    return ImmutableList.copyOf(indexes);
  }

  @Override
  public IndexMetadata updateIndex(IndexMetadata metadata) {
    Preconditions.checkNotNull(metadata, "Index metadata is null");
    Preconditions.checkArgument(
        metadata.location() != null && !metadata.location().isEmpty(), "Index location is empty");
    String indexLocation = metadata.location();
    Pointer current = currentPointer(indexLocation);
    publish(metadata, indexLocation, current == null ? 0 : current.generation);
    return metadata;
  }

  /**
   * Applies a change to the metadata as it stands, and publishes it above whatever generation is
   * current when it finally lands.
   *
   * <p>A change is applied to a document, and the generation it is published into is claimed by
   * creating that generation's file. When the claim fails, another publisher wrote a document after
   * this one read the metadata, so the change is applied again to the newer document rather than
   * published above a state it never saw: that is what keeps a publisher that appends a snapshot
   * from dropping one another publisher appended in the same window.
   *
   * <p>The document and the generation it belongs to are read together, as one document {@link
   * MetadataDocument}: the pointer names a generation and the file that holds it, so reading the
   * pointer once and then that file is what makes the pair consistent. Reading the metadata and
   * then the pointer separately lets a publisher apply its change to generation N and publish it
   * into N+2, which is the loss this method exists to prevent and which neither the claim nor the
   * pointer read-back can detect.
   *
   * <p>The change must be idempotent, because a retry hands it a document that may already carry
   * its result. {@link #updateIndex(IndexMetadata)} cannot re-apply anything, because it replaces
   * the document with the one the caller assembled from a copy it read earlier.
   */
  @Override
  public IndexMetadata updateIndex(
      String tableLocation, String indexUuid, UnaryOperator<IndexMetadata> change) {
    Preconditions.checkArgument(
        tableLocation != null && !tableLocation.isEmpty(), "Table location is empty");
    Preconditions.checkArgument(indexUuid != null && !indexUuid.isEmpty(), "Index UUID is empty");
    Preconditions.checkNotNull(change, "Index metadata change is null");

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

  @Override
  public boolean dropIndex(String tableLocation, String indexUuid, boolean purge) {
    Preconditions.checkArgument(
        tableLocation != null && !tableLocation.isEmpty(), "Table location is empty");
    Preconditions.checkArgument(indexUuid != null && !indexUuid.isEmpty(), "Index UUID is empty");
    if (!readIndexUuids(tableLocation).contains(indexUuid)) {
      return false;
    }

    // load the metadata before unregistering, because purging reads it from disk afterwards
    IndexMetadata metadata = purge ? loadIndex(tableLocation, indexUuid) : null;

    // unregister first: a registration whose files are already gone cannot be loaded, while files
    // left behind by a failed purge are only garbage
    unregister(tableLocation, indexUuid);

    if (metadata != null) {
      purgeIndexFiles(metadata);
    }

    return true;
  }

  /**
   * Adds an index to the registry, writing a new registry file and then pointing the table at it.
   *
   * <p>Whatever the registry holds that belongs to another table instance is dropped rather than
   * kept: the table was dropped and created again at this location, so those indexes are not the
   * new table's.
   */
  private void register(String tableLocation, String tableUuid, String indexUuid) {
    if (pointer == null) {
      List<String> indexUuids = Lists.newArrayList();
      for (IndexMetadata existing : listIndexes(tableLocation, tableUuid)) {
        indexUuids.add(existing.indexUuid());
      }
      indexUuids.add(indexUuid);
      writeLegacyRegistry(tableLocation, indexUuids);
      return;
    }

    String previous = pointer.current();
    for (int attempt = 1; ; attempt += 1) {
      List<String> indexUuids =
          Lists.newArrayList(uuidsOfTable(tableLocation, previous, tableUuid));
      if (!indexUuids.contains(indexUuid)) {
        indexUuids.add(indexUuid);
      }

      String next = writeRegistryVersion(tableLocation, indexUuids, previous);
      if (pointer.compareAndSet(previous, next)) {
        pruneOldVersions(tableLocation, next);
        return;
      }

      if (attempt >= MAX_REGISTRY_ATTEMPTS) {
        throw new CommitFailedException(
            "Failed to register index %s for table %s after %s attempts: the registry of the "
                + "table kept changing",
            indexUuid, tableLocation, MAX_REGISTRY_ATTEMPTS);
      }

      // another writer moved the pointer: rebuild the registry from the one it published
      previous = pointer.current();
      backoff(attempt);
    }
  }

  /**
   * The uuids of a registry that belong to the given table instance, which is what a registration
   * carries over into the new registry version.
   */
  private List<String> uuidsOfTable(String tableLocation, String registryFile, String tableUuid) {
    List<String> indexUuids = readRegistryUuids(tableLocation, registryFile);
    if (tableUuid == null) {
      return indexUuids;
    }

    List<String> mine = Lists.newArrayListWithCapacity(indexUuids.size());
    for (String indexUuid : indexUuids) {
      String json = readMetadataFile(indexLocation(tableLocation, indexUuid));
      if (json == null) {
        continue;
      }

      // an index written before the table uuid was recorded is given the benefit of the doubt
      IndexMetadata metadata = IndexMetadataParser.fromJson(json);
      if (metadata.tableUuid() == null || tableUuid.equals(metadata.tableUuid())) {
        mine.add(indexUuid);
      }
    }

    return mine;
  }

  /** Removes an index from the registry, or does nothing if it is already gone. */
  private void unregister(String tableLocation, String indexUuid) {
    if (pointer == null) {
      List<String> indexUuids = Lists.newArrayList(readIndexUuids(tableLocation));
      if (indexUuids.remove(indexUuid)) {
        writeLegacyRegistry(tableLocation, indexUuids);
      }
      return;
    }

    String previous = pointer.current();
    for (int attempt = 1; ; attempt += 1) {
      List<String> indexUuids = Lists.newArrayList(readRegistryUuids(tableLocation, previous));
      if (!indexUuids.remove(indexUuid)) {
        return;
      }

      String next = writeRegistryVersion(tableLocation, indexUuids, previous);
      if (pointer.compareAndSet(previous, next)) {
        pruneOldVersions(tableLocation, next);
        return;
      }

      if (attempt >= MAX_REGISTRY_ATTEMPTS) {
        throw new CommitFailedException(
            "Failed to drop index %s from table %s after %s attempts: the registry of the table "
                + "kept changing",
            indexUuid, tableLocation, MAX_REGISTRY_ATTEMPTS);
      }

      previous = pointer.current();
      backoff(attempt);
    }
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
    for (IndexSnapshot snapshot : metadata.snapshots()) {
      IndexManifestList manifestList = readManifestList(snapshot.indexManifestList());
      for (IndexManifestList.ManifestRef ref : manifestList.manifests()) {
        IndexManifest manifest = readManifest(ref.location());
        for (IndexFile indexFile : manifest.indexFiles()) {
          deleteIfExists(indexFile.location());
        }
        deleteIfExists(ref.location());
      }
      deleteIfExists(snapshot.indexManifestList());
    }
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
    String current = pointer == null ? null : pointer.current();
    return readRegistryUuids(tableLocation, current);
  }

  /**
   * Reads the uuids of the registry file the table points at, or of the legacy file when the table
   * names none.
   */
  private List<String> readRegistryUuids(String tableLocation, String registryFile) {
    String json = readIfExists(registryPath(tableLocation, registryFile));
    if (json == null) {
      Preconditions.checkArgument(
          registryFile == null,
          "The table points at registry file %s, which does not exist. The table property %s is "
              + "set to a file that was deleted or never written, so what the table holds is not "
              + "known; unset it to fall back to the file written in place",
          registryFile,
          REGISTRY_PROPERTY);
      return ImmutableList.of();
    }

    JsonNode registryNode = JsonUtil.parse(json, node -> node);
    int version = JsonUtil.getInt(REGISTRY_FORMAT_VERSION, registryNode);
    Preconditions.checkArgument(
        version == LEGACY_REGISTRY_VERSION || version == REGISTRY_VERSION,
        "Unsupported index registry format version: %s in %s",
        version,
        registryFile);

    List<String> indexUuids = Lists.newArrayList();
    for (JsonNode indexUuid : JsonUtil.get(REGISTRY_INDEX_UUIDS, registryNode)) {
      indexUuids.add(indexUuid.asText());
    }
    return indexUuids;
  }

  /** Writes a new registry version and returns its file name, which is never reused. */
  private String writeRegistryVersion(
      String tableLocation, List<String> indexUuids, String previousFile) {
    String fileName =
        REGISTRY_FILE_PREFIX
            + nextVersion(previousFile)
            + "-"
            + UUID.randomUUID().toString().replace("-", "").substring(0, 8)
            + ".json";
    writeString(
        registryPath(tableLocation, fileName), registryJson(indexUuids, previousFile), true);
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

  private static String registryJson(List<String> indexUuids, String previousFile) {
    return JsonUtil.generate(
        generator -> {
          generator.writeStartObject();
          generator.writeNumberField(REGISTRY_FORMAT_VERSION, REGISTRY_VERSION);
          generator.writeNumberField(REGISTRY_UPDATED_MS, System.currentTimeMillis());
          generator.writeArrayFieldStart(REGISTRY_INDEX_UUIDS);
          for (String indexUuid : indexUuids) {
            generator.writeString(indexUuid);
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
