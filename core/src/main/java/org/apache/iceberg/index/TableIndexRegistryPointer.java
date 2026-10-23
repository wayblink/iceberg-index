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
import java.util.Objects;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * Holds the index registry pointer in a table property, so that moving it is a table commit.
 *
 * <p>The pointer is a file name relative to the {@code indexes} directory, not an absolute path:
 * the registry has to survive the table being registered under a different location or moved along
 * with its files, and an absolute path would keep pointing at the old place.
 *
 * <p>The commit is the compare-and-swap. Catalog commit implementations reject a commit based on
 * metadata that has moved on, which is exactly the race to catch here: a writer that read the
 * pointer, built a registry from it and comes back to find it changed has to redo the work against
 * the winner's registry instead of overwriting it. The property update is done directly on {@link
 * TableOperations}, not through {@code UpdateProperties}, because that helper retries on its own
 * with the value it already computed, which would write the loser's registry and drop the winner's
 * index.
 */
class TableIndexRegistryPointer implements IndexRegistryPointer {

  private final TableOperations ops;

  TableIndexRegistryPointer(Table table) {
    Preconditions.checkNotNull(table, "Table is null");
    Preconditions.checkArgument(
        table instanceof HasTableOperations,
        "Cannot read index registry pointer from table that does not expose its operations: %s",
        table.getClass().getName());
    this.ops = ((HasTableOperations) table).operations();
  }

  @Override
  public String current() {
    return ops.refresh().properties().get(SidecarIndexCatalog.REGISTRY_PROPERTY);
  }

  @Override
  public boolean compareAndSet(String expected, String next) {
    Preconditions.checkNotNull(next, "Next registry file is null");
    TableMetadata base = ops.refresh();
    if (!Objects.equals(expected, base.properties().get(SidecarIndexCatalog.REGISTRY_PROPERTY))) {
      // the pointer moved before this attempt got to commit, so the registry built from it is stale
      return false;
    }

    Map<String, String> properties = Maps.newHashMap(base.properties());
    properties.put(SidecarIndexCatalog.REGISTRY_PROPERTY, next);
    try {
      ops.commit(base, base.replaceProperties(properties));
      return true;
    } catch (CommitFailedException e) {
      return false;
    }
  }
}
