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

import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

/**
 * A request to register a new index with an {@link IndexCatalog}.
 *
 * <p>The catalog assigns the index location from the table location and the index UUID; the
 * definition selects the index type and its parameters.
 */
public final class CreateIndexRequest {

  private final String tableLocation;
  private final String tableUuid;
  private final String indexUuid;
  private final IndexDefinition definition;
  private final Map<String, String> properties;

  private CreateIndexRequest(
      String tableLocation,
      String tableUuid,
      String indexUuid,
      IndexDefinition definition,
      Map<String, String> properties) {
    Preconditions.checkArgument(
        tableLocation != null && !tableLocation.isEmpty(), "Table location is empty");
    Preconditions.checkArgument(tableUuid != null && !tableUuid.isEmpty(), "Table UUID is empty");
    Preconditions.checkArgument(indexUuid != null && !indexUuid.isEmpty(), "Index UUID is empty");
    Preconditions.checkNotNull(definition, "Index definition is null");
    this.tableLocation = tableLocation;
    this.tableUuid = tableUuid;
    this.indexUuid = indexUuid;
    this.definition = definition;
    this.properties = ImmutableMap.copyOf(properties);
  }

  public static Builder builder() {
    return new Builder();
  }

  public String tableLocation() {
    return tableLocation;
  }

  public String tableUuid() {
    return tableUuid;
  }

  public String indexUuid() {
    return indexUuid;
  }

  public IndexDefinition definition() {
    return definition;
  }

  public Map<String, String> properties() {
    return properties;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("tableLocation", tableLocation)
        .add("tableUuid", tableUuid)
        .add("indexUuid", indexUuid)
        .add("definition", definition)
        .add("properties", properties)
        .toString();
  }

  public static final class Builder {
    private String tableLocation;
    private String tableUuid;
    private String indexUuid;
    private IndexDefinition definition;
    private final ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();

    private Builder() {}

    public Builder tableLocation(String newTableLocation) {
      this.tableLocation = newTableLocation;
      return this;
    }

    public Builder tableUuid(String newTableUuid) {
      this.tableUuid = newTableUuid;
      return this;
    }

    public Builder indexUuid(String newIndexUuid) {
      this.indexUuid = newIndexUuid;
      return this;
    }

    public Builder definition(IndexDefinition newDefinition) {
      this.definition = newDefinition;
      return this;
    }

    public Builder property(String key, String value) {
      properties.put(key, value);
      return this;
    }

    public Builder properties(Map<String, String> newProperties) {
      properties.putAll(newProperties);
      return this;
    }

    public CreateIndexRequest build() {
      Preconditions.checkNotNull(tableLocation, "Table location is not set");
      Preconditions.checkNotNull(tableUuid, "Table UUID is not set");
      Preconditions.checkNotNull(indexUuid, "Index UUID is not set");
      Preconditions.checkNotNull(definition, "Index definition is not set");
      return new CreateIndexRequest(
          tableLocation, tableUuid, indexUuid, definition, properties.build());
    }
  }
}
