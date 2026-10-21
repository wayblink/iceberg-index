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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.JsonUtil;

/**
 * JSON serialization for {@link IndexMetadata}.
 *
 * <p>The {@code definition} object is serialized and parsed by the {@link IndexType} registered for
 * the metadata's {@code index-type}, so new index types only extend their own parser. Unknown index
 * types fail fast through {@link IndexTypes#fromType(String)}.
 */
public final class IndexMetadataParser {

  private static final String FORMAT_VERSION = "format-version";
  private static final String INDEX_UUID = "index-uuid";
  private static final String INDEX_NAME = "index-name";
  private static final String TABLE_UUID = "table-uuid";
  private static final String TABLE_LOCATION = "table-location";
  private static final String LOCATION = "location";
  private static final String INDEX_TYPE = "index-type";
  private static final String DEFINITION = "definition";
  private static final String LAST_UPDATED_MILLIS = "last-updated-ms";
  private static final String PROPERTIES = "properties";
  private static final String SNAPSHOTS = "snapshots";
  private static final String SNAPSHOT_ID = "snapshot-id";
  private static final String SOURCE_TABLE_SNAPSHOT_ID = "source-table-snapshot-id";
  private static final String PARENT_INDEX_SNAPSHOT_ID = "parent-index-snapshot-id";
  private static final String TIMESTAMP_MILLIS = "timestamp-ms";
  private static final String STATUS = "status";
  private static final String INDEX_MANIFEST_LIST = "index-manifest-list";

  private IndexMetadataParser() {}

  /** Serializes an index definition to JSON, for display and diagnostics. */
  public static String toJson(IndexDefinition definition) {
    Preconditions.checkNotNull(definition, "Index definition is null");
    return JsonUtil.generate(
        gen -> {
          IndexType indexType = IndexTypes.fromType(definition.type());
          gen.writeStartObject();
          indexType.writeDefinition(definition, gen);
          gen.writeEndObject();
        },
        false);
  }

  public static String toJson(IndexMetadata metadata) {
    return toJson(metadata, false);
  }

  public static String toJson(IndexMetadata metadata, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(metadata, gen), pretty);
  }

  public static void toJson(IndexMetadata metadata, JsonGenerator generator) throws IOException {
    Preconditions.checkNotNull(metadata, "Index metadata is null");
    IndexType indexType = IndexTypes.fromType(metadata.indexType());

    generator.writeStartObject();
    generator.writeNumberField(FORMAT_VERSION, IndexMetadata.FORMAT_VERSION);
    generator.writeStringField(INDEX_UUID, metadata.indexUuid());
    if (metadata.name() != null) {
      generator.writeStringField(INDEX_NAME, metadata.name());
    }
    generator.writeStringField(TABLE_UUID, metadata.tableUuid());
    if (metadata.tableLocation() != null) {
      generator.writeStringField(TABLE_LOCATION, metadata.tableLocation());
    }
    generator.writeStringField(LOCATION, metadata.location());
    generator.writeStringField(INDEX_TYPE, metadata.indexType());
    generator.writeObjectFieldStart(DEFINITION);
    indexType.writeDefinition(metadata.definition(), generator);
    generator.writeEndObject();
    if (metadata.lastUpdatedMillis() != null) {
      generator.writeNumberField(LAST_UPDATED_MILLIS, metadata.lastUpdatedMillis());
    }
    if (!metadata.properties().isEmpty()) {
      JsonUtil.writeStringMap(PROPERTIES, metadata.properties(), generator);
    }
    generator.writeArrayFieldStart(SNAPSHOTS);
    for (IndexSnapshot snapshot : metadata.snapshots()) {
      writeSnapshot(snapshot, generator);
    }
    generator.writeEndArray();
    generator.writeEndObject();
  }

  public static IndexMetadata fromJson(String json) {
    return JsonUtil.parse(json, IndexMetadataParser::fromJson);
  }

  public static IndexMetadata fromJson(JsonNode node) {
    int formatVersion = JsonUtil.getInt(FORMAT_VERSION, node);
    Preconditions.checkArgument(
        formatVersion == IndexMetadata.FORMAT_VERSION,
        "Unsupported index metadata format version: %s",
        formatVersion);
    String indexType = JsonUtil.getString(INDEX_TYPE, node);
    IndexType type = IndexTypes.fromType(indexType);
    IndexDefinition definition = type.parseDefinition(JsonUtil.get(DEFINITION, node));

    IndexMetadata.Builder builder =
        IndexMetadata.builder()
            .indexUuid(JsonUtil.getString(INDEX_UUID, node))
            .tableUuid(JsonUtil.getString(TABLE_UUID, node))
            .location(JsonUtil.getString(LOCATION, node))
            .indexType(indexType)
            .definition(definition);
    String name = JsonUtil.getStringOrNull(INDEX_NAME, node);
    if (name != null) {
      builder.name(name);
    }
    String tableLocation = JsonUtil.getStringOrNull(TABLE_LOCATION, node);
    if (tableLocation != null) {
      builder.tableLocation(tableLocation);
    }
    Long lastUpdatedMillis = JsonUtil.getLongOrNull(LAST_UPDATED_MILLIS, node);
    if (lastUpdatedMillis != null) {
      builder.lastUpdatedMillis(lastUpdatedMillis);
    }
    Map<String, String> properties =
        node.has(PROPERTIES) ? JsonUtil.getStringMap(PROPERTIES, node) : ImmutableMap.of();
    builder.properties(properties);

    JsonNode snapshots = JsonUtil.get(SNAPSHOTS, node);
    Preconditions.checkArgument(
        snapshots.isArray(), "Cannot parse index snapshots from non-array: %s", snapshots);
    for (JsonNode snapshot : snapshots) {
      builder.addSnapshot(readSnapshot(snapshot));
    }
    return builder.build();
  }

  private static void writeSnapshot(IndexSnapshot snapshot, JsonGenerator generator)
      throws IOException {
    generator.writeStartObject();
    generator.writeNumberField(SNAPSHOT_ID, snapshot.snapshotId());
    generator.writeNumberField(SOURCE_TABLE_SNAPSHOT_ID, snapshot.sourceTableSnapshotId());
    Long parent = snapshot.parentIndexSnapshotId();
    if (parent != null) {
      generator.writeNumberField(PARENT_INDEX_SNAPSHOT_ID, parent);
    }
    generator.writeNumberField(TIMESTAMP_MILLIS, snapshot.timestampMillis());
    generator.writeStringField(STATUS, snapshot.status().name());
    generator.writeStringField(INDEX_MANIFEST_LIST, snapshot.indexManifestList());
    if (!snapshot.properties().isEmpty()) {
      JsonUtil.writeStringMap(PROPERTIES, snapshot.properties(), generator);
    }
    generator.writeEndObject();
  }

  private static IndexSnapshot readSnapshot(JsonNode node) {
    IndexSnapshot.Builder builder =
        IndexSnapshot.builder()
            .snapshotId(JsonUtil.getLong(SNAPSHOT_ID, node))
            .sourceTableSnapshotId(JsonUtil.getLong(SOURCE_TABLE_SNAPSHOT_ID, node))
            .timestampMillis(JsonUtil.getLong(TIMESTAMP_MILLIS, node))
            .indexManifestList(JsonUtil.getString(INDEX_MANIFEST_LIST, node));
    Long parent = JsonUtil.getLongOrNull(PARENT_INDEX_SNAPSHOT_ID, node);
    if (parent != null) {
      builder.parentIndexSnapshotId(parent);
    }
    String status = JsonUtil.getStringOrNull(STATUS, node);
    if (status != null) {
      builder.status(parseStatus(status));
    }
    if (node.has(PROPERTIES)) {
      builder.properties(JsonUtil.getStringMap(PROPERTIES, node));
    }
    return builder.build();
  }

  private static IndexSnapshot.Status parseStatus(String value) {
    try {
      return IndexSnapshot.Status.valueOf(value.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Unsupported index snapshot status: " + value, e);
    }
  }
}
