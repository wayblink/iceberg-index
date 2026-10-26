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
package org.apache.iceberg.spark.procedures;

import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.Table;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.spark.VectorIndexes;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

/** Shared row shape and index resolution for the explicit load and offload procedures. */
abstract class IndexResidencyProcedure extends BaseProcedure {
  static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField("status", DataTypes.StringType, false, Metadata.empty()),
            new StructField("plan_id", DataTypes.StringType, false, Metadata.empty()),
            new StructField("index_uuid", DataTypes.StringType, false, Metadata.empty()),
            new StructField("snapshot_id", DataTypes.LongType, false, Metadata.empty()),
            new StructField("protection", DataTypes.StringType, true, Metadata.empty()),
            new StructField("deadline_nanos", DataTypes.LongType, false, Metadata.empty()),
            new StructField("permanent", DataTypes.BooleanType, false, Metadata.empty()),
            new StructField("file_location", DataTypes.StringType, true, Metadata.empty()),
            new StructField("executor_id", DataTypes.StringType, true, Metadata.empty()),
            new StructField("bytes", DataTypes.LongType, true, Metadata.empty()),
            new StructField("reason", DataTypes.StringType, true, Metadata.empty())
          });

  IndexResidencyProcedure(TableCatalog catalog) {
    super(catalog);
  }

  @Override
  public StructType outputType() {
    return OUTPUT_TYPE;
  }

  /**
   * Resolves the index a caller named. A name is what SQL uses and a UUID is what the metadata
   * uses; a name that maps to two indexes of this table is refused rather than guessed.
   */
  static IndexMetadata resolveIndex(Table table, String nameOrUuid) {
    Preconditions.checkArgument(
        nameOrUuid != null && !nameOrUuid.isEmpty(), "Index name or UUID is missing");

    List<IndexMetadata> matches = new ArrayList<>();
    List<String> available = new ArrayList<>();
    for (IndexMetadata metadata : VectorIndexes.indexesOf(table)) {
      boolean byName = nameOrUuid.equals(metadata.name());
      boolean byUuid = nameOrUuid.equals(metadata.indexUuid());
      if (byName || byUuid) {
        matches.add(metadata);
      }

      available.add(metadata.name() != null ? metadata.name() : metadata.indexUuid());
    }

    Preconditions.checkArgument(
        !matches.isEmpty(),
        "No index named %s exists for table %s; this table has %s",
        nameOrUuid,
        table.name(),
        available);
    Preconditions.checkArgument(
        matches.size() == 1,
        "Index %s is ambiguous for table %s: it matches %s",
        nameOrUuid,
        table.name(),
        matches.stream().map(IndexMetadata::indexUuid).toList());
    return matches.get(0);
  }

  /** One summary row plus one row per planned file, so gaps stay visible. */
  InternalRow[] rows(org.apache.iceberg.index.spark.IndexCacheControl.Result result) {
    List<InternalRow> rows = Lists.newArrayListWithCapacity(result.files().size() + 1);
    rows.add(
        newInternalRow(
            UTF8String.fromString(result.status()),
            UTF8String.fromString(result.planId()),
            UTF8String.fromString(result.indexUuid()),
            result.snapshotId(),
            result.protection() == null ? null : UTF8String.fromString(result.protection()),
            result.deadlineNanos(),
            result.permanent(),
            null,
            null,
            null,
            null));

    for (org.apache.iceberg.index.spark.IndexCacheControl.FileResult file : result.files()) {
      rows.add(
          newInternalRow(
              UTF8String.fromString(result.status()),
              UTF8String.fromString(result.planId()),
              UTF8String.fromString(result.indexUuid()),
              result.snapshotId(),
              result.protection() == null ? null : UTF8String.fromString(result.protection()),
              result.deadlineNanos(),
              result.permanent(),
              UTF8String.fromString(file.fileLocation()),
              file.executorId() == null ? null : UTF8String.fromString(file.executorId()),
              file.bytes(),
              file.reason() == null ? null : UTF8String.fromString(file.reason())));
    }

    return rows.toArray(new InternalRow[0]);
  }
}
