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

/**
 * An {@link IndexCatalog} that keeps index metadata in a self-contained sidecar tree under the
 * table location:
 *
 * <p>Layout under the table location:
 *
 * <ul>
 *   <li>{@code indexes/index-registry.json}: the registry of index UUIDs
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
 * also keeps the layout portable across file systems. All catalog mutations are single-writer.
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

  private static final String REGISTRY_FILE = "index-registry.json";
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

  private static final String REGISTRY_FORMAT_VERSION = "format-version";
  private static final String REGISTRY_INDEX_UUIDS = "index-uuids";
  private static final int REGISTRY_VERSION = 1;

  private final FileIO fileIO;

  public SidecarIndexCatalog(FileIO fileIO) {
    this.fileIO = Preconditions.checkNotNull(fileIO, "File IO is null");
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
            .tableUuid(request.tableUuid())
            .tableLocation(tableLocation)
            .location(indexLocation)
            .definition(request.definition())
            .properties(request.properties())
            .build();
    writeMetadata(metadata, 1, 0);

    List<String> indexUuids = Lists.newArrayList(readIndexUuids(tableLocation));
    indexUuids.add(request.indexUuid());
    writeRegistry(tableLocation, indexUuids);
    return metadata;
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
    List<String> indexUuids = readIndexUuids(tableLocation);
    if (!indexUuids.contains(indexUuid)) {
      return false;
    }

    if (purge) {
      purgeIndexFiles(loadIndex(tableLocation, indexUuid));
    }

    indexUuids = Lists.newArrayList(indexUuids);
    indexUuids.remove(indexUuid);
    writeRegistry(tableLocation, indexUuids);
    return true;
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
    writeString(metadataPath(indexLocation, generation), IndexMetadataParser.toJson(metadata));
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
            false));
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
    String json = readIfExists(registryPath(tableLocation));
    if (json == null) {
      return ImmutableList.of();
    }
    JsonNode registryNode = JsonUtil.parse(json, node -> node);
    int version = JsonUtil.getInt(REGISTRY_FORMAT_VERSION, registryNode);
    Preconditions.checkArgument(
        version == REGISTRY_VERSION, "Unsupported index registry format version: %s", version);
    List<String> indexUuids = Lists.newArrayList();
    for (JsonNode indexUuid : JsonUtil.get(REGISTRY_INDEX_UUIDS, registryNode)) {
      indexUuids.add(indexUuid.asText());
    }
    return indexUuids;
  }

  private void writeRegistry(String tableLocation, List<String> indexUuids) {
    String json =
        JsonUtil.generate(
            generator -> {
              generator.writeStartObject();
              generator.writeNumberField(REGISTRY_FORMAT_VERSION, REGISTRY_VERSION);
              generator.writeArrayFieldStart(REGISTRY_INDEX_UUIDS);
              for (String indexUuid : indexUuids) {
                generator.writeString(indexUuid);
              }
              generator.writeEndArray();
              generator.writeEndObject();
            },
            false);
    writeString(registryPath(tableLocation), json);
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

  private void writeString(String path, String content) {
    try (PositionOutputStream out = fileIO.newOutputFile(path).createOrOverwrite()) {
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

  private static String registryPath(String tableLocation) {
    return indexesRoot(tableLocation) + "/" + REGISTRY_FILE;
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
