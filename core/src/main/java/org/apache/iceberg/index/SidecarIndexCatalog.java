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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NotFoundException;
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
 * <p>Publishing an index snapshot is atomic: a new metadata version is written as a new file and
 * only then does the pointer switch to it, so a reader always sees either the previous or the new
 * metadata root, never a partially written one. A crash before the pointer switch leaves the new
 * version as an unreferenced file, which is harmless. The pointer alternates between two slots and
 * is never overwritten in place, so a crash cannot truncate it either; a reader picks the slot with
 * the higher generation.
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
    writeMetadata(metadata, 1, 0);
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
    Pointer current = currentPointer(metadata.location());
    long nextGeneration = current == null ? 1 : current.generation + 1;
    int nextSlot = current == null ? 0 : (current.slot + 1) % POINTER_SLOTS;
    writeMetadata(metadata, nextGeneration, nextSlot);
    return metadata;
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
      deleteIfExists(pointerPath(indexLocation, slot));
    }
    // indexes created before the pointer switch kept a single metadata file
    deleteIfExists(legacyMetadataPath(indexLocation));
  }

  /** Writes a metadata version as a new file and then switches the pointer to it. */
  private void writeMetadata(IndexMetadata metadata, long generation, int slot) {
    String indexLocation = metadata.location();
    writeString(
        metadataPath(indexLocation, generation), IndexMetadataParser.toJson(metadata), false);
    writeString(
        pointerPath(indexLocation, slot),
        JsonUtil.generate(
            generator -> {
              generator.writeStartObject();
              generator.writeNumberField(POINTER_GENERATION, generation);
              generator.writeStringField(
                  POINTER_METADATA_FILE, metadataPath(indexLocation, generation));
              generator.writeEndObject();
            },
            false),
        false);
  }

  /** Reads the current metadata JSON, following the pointer, with a legacy layout fallback. */
  private String readMetadataFile(String indexLocation) {
    Pointer pointer = currentPointer(indexLocation);
    if (pointer != null) {
      return readIfExists(pointer.file);
    }
    return readIfExists(legacyMetadataPath(indexLocation));
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
          JsonUtil.getInt(POINTER_GENERATION, node),
          JsonUtil.getString(POINTER_METADATA_FILE, node),
          slot);
    } catch (RuntimeException e) {
      // a pointer is written once and never overwritten, so this only happens on corruption
      return null;
    }
  }

  private static final class Pointer {
    private final long generation;
    private final String file;
    private final int slot;

    private Pointer(long generation, String file, int slot) {
      this.generation = generation;
      this.file = file;
      this.slot = slot;
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
