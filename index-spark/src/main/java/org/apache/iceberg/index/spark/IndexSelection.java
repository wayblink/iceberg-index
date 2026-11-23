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

import java.util.List;
import org.apache.iceberg.Table;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * Resolves the index a statement means, and refuses a build that would make that resolution
 * ambiguous.
 *
 * <p>The rules are the same for every index type: a name identifies an index in SQL, a UUID is the
 * identity in the metadata, and a column with several indexes must be answered by naming one. A
 * statement that builds or searches an index passes its type here instead of carrying a copy of
 * these rules, so a new index type is wired up rather than copied.
 */
public final class IndexSelection {

  private IndexSelection() {}

  /**
   * Resolves the index of one type that a caller named, or the only one the column has.
   *
   * <p>When the column carries several indexes of the type and the caller named none, the message
   * lists them rather than picking one: a query that silently used the other index would look like
   * an accuracy problem instead of a missing argument.
   */
  public static IndexMetadata resolve(
      Table table, String column, String indexType, String nameOrUuid) {
    List<IndexMetadata> candidates = VectorIndexes.indexesFor(table, column, indexType);
    if (nameOrUuid != null) {
      IndexMetadata found =
          findByNameOrUuid(candidates, nameOrUuid, table.name(), column, indexType);
      if (found != null) {
        return found;
      }

      throw new IllegalArgumentException(
          "No "
              + indexType
              + " index named "
              + nameOrUuid
              + " exists for column "
              + column
              + " of table "
              + table.name()
              + "; the column has "
              + names(candidates));
    }

    if (candidates.isEmpty()) {
      throw new IllegalArgumentException(
          "No " + indexType + " index exists for column " + column + " of table " + table.name());
    }

    if (candidates.size() > 1) {
      throw new IllegalArgumentException(
          "Column "
              + column
              + " of table "
              + table.name()
              + " has "
              + candidates.size()
              + " "
              + indexType
              + " indexes "
              + names(candidates)
              + "; pass index => '...' to say which one to search");
    }

    return candidates.get(0);
  }

  /**
   * Resolves an index of any type on a column, which is what removing one index needs: a column
   * carries one kind of index in practice, so the type does not have to be named to remove it.
   */
  public static IndexMetadata resolveAny(Table table, String column, String nameOrUuid) {
    List<IndexMetadata> candidates = VectorIndexes.indexesFor(table, column);
    if (nameOrUuid != null) {
      IndexMetadata found = findByNameOrUuid(candidates, nameOrUuid, table.name(), column, null);
      if (found != null) {
        return found;
      }

      throw new IllegalArgumentException(
          "No index named "
              + nameOrUuid
              + " exists for column "
              + column
              + " of table "
              + table.name()
              + (candidates.isEmpty() ? "" : "; existing indexes: " + describe(candidates)));
    }

    if (candidates.isEmpty()) {
      throw new IllegalArgumentException(
          "No index exists for column " + column + " of table " + table.name());
    }

    if (candidates.size() > 1) {
      throw new IllegalArgumentException(
          "Column "
              + column
              + " of table "
              + table.name()
              + " has "
              + candidates.size()
              + " indexes; pass index => <name or uuid> to pick one: "
              + describe(candidates));
    }

    return candidates.get(0);
  }

  /**
   * Refuses a build that would make the column ambiguous, unless the caller named the index.
   *
   * <p>One index per column is what search and refresh assume when no name is given, so a second
   * unnamed index is refused whatever its type; a named one is a new index over the same column,
   * which is how the parameters of two arms are compared side by side.
   */
  public static void checkCanBuild(
      Table table, String column, String indexType, String tableName, String name) {
    List<IndexMetadata> existing = VectorIndexes.indexesFor(table, column);
    if (name == null) {
      if (!existing.isEmpty()) {
        throw new IllegalArgumentException(
            "A "
                + indexType
                + " index already exists for column "
                + column
                + " of table "
                + tableName
                + "; pass name => '...' to build another index over the same column");
      }

      return;
    }

    for (IndexMetadata metadata : existing) {
      if (name.equals(metadata.name())) {
        throw new IllegalArgumentException(
            "An index named "
                + name
                + " already exists for column "
                + column
                + " of table "
                + tableName);
      }

      if (name.equals(metadata.indexUuid())) {
        // a name that is another index's UUID makes the selector mean both: a search by it would
        // hit one index, a drop could hit the other, so the build is refused instead of being
        // recorded in a state resolution has to report as ambiguous afterwards
        throw new IllegalArgumentException(
            "The index name "
                + name
                + " is the UUID of the "
                + indexType
                + " index on column "
                + column
                + " of table "
                + tableName
                + "; pass a name that is not an existing index UUID, or the selector would mean"
                + " both indexes");
      }
    }
  }

  /**
   * Returns the index the caller named by name or UUID, or null if none of them is it.
   *
   * <p>A selector has to identify one index: a table written before names were checked against the
   * UUIDs can carry a name that is another index's UUID, and registrations from before that check
   * could leave two indexes with one name. Resolving such a selector to whichever candidate came
   * first would silently search or drop the wrong index, so it is refused and the clashing indexes
   * are named in the message.
   */
  private static IndexMetadata findByNameOrUuid(
      List<IndexMetadata> candidates,
      String nameOrUuid,
      String tableName,
      String column,
      String indexType) {
    IndexMetadata found = null;
    for (IndexMetadata metadata : candidates) {
      if (!nameOrUuid.equals(metadata.indexUuid()) && !nameOrUuid.equals(metadata.name())) {
        continue;
      }

      if (found != null && !found.indexUuid().equals(metadata.indexUuid())) {
        throw new IllegalArgumentException(
            "The index selector "
                + nameOrUuid
                + " is ambiguous for column "
                + column
                + " of table "
                + tableName
                + ": it matches "
                + describeOne(metadata)
                + " and "
                + describeOne(found)
                + "; pass a name or UUID that identifies only one "
                + (indexType == null ? "index" : indexType + " index"));
      }

      found = metadata;
    }

    return found;
  }

  /** Lists an index as {@code index-uuid (named name)} so an error can name a clashing pair. */
  private static String describeOne(IndexMetadata metadata) {
    return metadata.name() == null
        ? "index " + metadata.indexUuid()
        : "index " + metadata.indexUuid() + " (named " + metadata.name() + ")";
  }

  /** Lists the resolvable indexes by name, falling back to the UUID of an unnamed one. */
  private static List<String> names(List<IndexMetadata> indexes) {
    List<String> names = Lists.newArrayListWithCapacity(indexes.size());
    for (IndexMetadata metadata : indexes) {
      names.add(metadata.name() != null ? metadata.name() : metadata.indexUuid());
    }

    return names;
  }

  /** Lists the resolvable indexes as {@code type:name(uuid)} so an error can name them. */
  private static String describe(List<IndexMetadata> indexes) {
    List<String> described = Lists.newArrayListWithCapacity(indexes.size());
    for (IndexMetadata metadata : indexes) {
      String name = metadata.name() != null ? metadata.name() : metadata.indexUuid();
      described.add(metadata.indexType() + ":" + name + "(" + metadata.indexUuid() + ")");
    }

    return String.join(", ", described);
  }
}
