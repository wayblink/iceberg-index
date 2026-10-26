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

import java.util.List;
import org.apache.iceberg.Table;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.spark.IndexBuildStatus;
import org.apache.iceberg.index.spark.VectorIndexes;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.procedures.SparkProcedures.ProcedureBuilder;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * How far the builds of a table's indexes have got, one row per index.
 *
 * <p>A build of a large table runs for tens of minutes, and until this existed the only thing it
 * said was a line in the driver log, which is gone when the build ends and says nothing at all
 * about a build that failed. The table carries the report instead, written beside each index, so
 * that this can be asked at any time — including while a build is running, from another session.
 *
 * <pre>{@code
 * CALL spark_catalog.system.index_build_status(table => 'db.documents');
 * }</pre>
 */
class IndexBuildStatusProcedure extends BaseProcedure {

  private static final ProcedureParameter TABLE_PARAM =
      ProcedureParameter.required("table", DataTypes.StringType);

  private static final ProcedureParameter[] PARAMETERS = new ProcedureParameter[] {TABLE_PARAM};

  /**
   * How long a running build may go without saying anything before it is taken to have stopped.
   *
   * <p>The reporter writes every thirty seconds, so this is several intervals: a build that is
   * merely slow keeps reporting, and one whose driver died stops. Nothing is written on the way
   * out, so this is the only way such a build is ever distinguished from a live one.
   */
  private static final long STALE_AFTER_MILLIS = 5L * 60L * 1000L;

  /** What an index with no report at all is called: built before this existed, or rebuilt since. */
  private static final String UNKNOWN = "UNKNOWN";

  private static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField("table", DataTypes.StringType, false, Metadata.empty()),
            new StructField("column", DataTypes.StringType, true, Metadata.empty()),
            new StructField("index_uuid", DataTypes.StringType, false, Metadata.empty()),
            new StructField("index_name", DataTypes.StringType, true, Metadata.empty()),
            new StructField("index_type", DataTypes.StringType, true, Metadata.empty()),
            new StructField("state", DataTypes.StringType, false, Metadata.empty()),
            new StructField("started_at", DataTypes.TimestampType, true, Metadata.empty()),
            new StructField("updated_at", DataTypes.TimestampType, true, Metadata.empty()),
            new StructField("age_ms", DataTypes.LongType, true, Metadata.empty()),
            new StructField("stale", DataTypes.BooleanType, false, Metadata.empty()),
            new StructField("files_built", DataTypes.LongType, true, Metadata.empty()),
            new StructField("files_total", DataTypes.LongType, true, Metadata.empty()),
            new StructField("files_failed", DataTypes.LongType, true, Metadata.empty()),
            new StructField("rows", DataTypes.LongType, true, Metadata.empty()),
            new StructField("error", DataTypes.StringType, true, Metadata.empty())
          });

  public static ProcedureBuilder builder() {
    return new BaseProcedure.Builder<IndexBuildStatusProcedure>() {
      @Override
      public IndexBuildStatusProcedure doBuild() {
        return new IndexBuildStatusProcedure(tableCatalog());
      }
    };
  }

  private IndexBuildStatusProcedure(TableCatalog catalog) {
    super(catalog);
  }

  @Override
  public ProcedureParameter[] parameters() {
    return PARAMETERS;
  }

  @Override
  public StructType outputType() {
    return OUTPUT_TYPE;
  }

  @Override
  public InternalRow[] call(InternalRow args) {
    VectorIndexProcedures.registerVectorIndexType();
    VectorIndexes.registerTextIndexType();
    ProcedureInput input = new ProcedureInput(spark(), tableCatalog(), PARAMETERS, args);
    String tableName = input.asString(TABLE_PARAM);
    List<InternalRow> rows = Lists.newArrayList();

    Table table;
    try {
      table = org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark(), tableName);
    } catch (org.apache.spark.sql.catalyst.parser.ParseException e) {
      throw new IllegalArgumentException("Not a table identifier: " + tableName, e);
    } catch (org.apache.spark.sql.catalyst.analysis.NoSuchTableException e) {
      throw new IllegalArgumentException("No such table: " + tableName, e);
    }

    long now = System.currentTimeMillis();
    java.util.Set<String> reported = new java.util.LinkedHashSet<>();
    for (IndexMetadata metadata : VectorIndexes.indexesOf(table)) {
      rows.add(row(tableName, table, metadata, now));
      reported.add(metadata.indexUuid());
    }

    // a build that failed is rolled back, registration included, so the only trace of it is the
    // report its column kept. Those are added here, or the failure that made the user look would be
    // the one thing this could not show.
    for (IndexBuildStatus failed : rolledBack(table, tableName, reported, now)) {
      rows.add(row(tableName, failed, now));
    }

    return rows.toArray(new InternalRow[0]);
  }

  /** The failed attempts whose index no longer exists, one per column. */
  private List<IndexBuildStatus> rolledBack(
      Table table, String tableName, java.util.Set<String> reported, long now) {
    List<IndexBuildStatus> attempts = Lists.newArrayList();
    for (org.apache.iceberg.types.Types.NestedField field : table.schema().columns()) {
      IndexBuildStatus report =
          IndexBuildStatus.read(
              table.io(), IndexBuildStatus.rolledBackStatusDir(table.location(), field.fieldId()));
      if (report == null
          || report.state() != IndexBuildStatus.State.FAILED
          || reported.contains(report.indexUuid())) {
        continue;
      }

      attempts.add(report);
    }

    return attempts;
  }

  private InternalRow row(String tableName, Table table, IndexMetadata metadata, long now) {
    // both indexes that were built through a procedure and ones that were not are reported: an
    // index with no report is a fact about that index, not a reason to leave it out
    IndexBuildStatus status =
        IndexBuildStatus.read(table.io(), IndexBuildStatus.statusDirFor(metadata.location()));
    String column = columnNameOrNull(table, metadata);
    if (status == null) {
      return newInternalRow(
          UTF8String.fromString(tableName),
          column == null ? null : UTF8String.fromString(column),
          UTF8String.fromString(metadata.indexUuid()),
          metadata.name() == null ? null : UTF8String.fromString(metadata.name()),
          UTF8String.fromString(metadata.indexType()),
          UTF8String.fromString(UNKNOWN),
          null,
          null,
          null,
          false,
          null,
          null,
          null,
          null,
          null);
    }

    long ageMillis = now - status.updatedAtMillis();
    IndexBuildStatus.State state = status.stateAt(now, STALE_AFTER_MILLIS);
    return newInternalRow(
        UTF8String.fromString(tableName),
        status.column() != null
            ? UTF8String.fromString(status.column())
            : (column == null ? null : UTF8String.fromString(column)),
        UTF8String.fromString(metadata.indexUuid()),
        metadata.name() == null ? null : UTF8String.fromString(metadata.name()),
        UTF8String.fromString(metadata.indexType()),
        UTF8String.fromString(state.name()),
        status.startedAtMillis() * 1000L,
        status.updatedAtMillis() * 1000L,
        ageMillis,
        state == IndexBuildStatus.State.ABANDONED,
        status.filesBuilt(),
        status.filesTotal(),
        status.filesFailed(),
        status.rows(),
        status.error() == null ? null : UTF8String.fromString(status.error()));
  }

  /** A row for a failed attempt whose index was rolled back, so it has no registration to read. */
  private InternalRow row(String tableName, IndexBuildStatus status, long now) {
    long ageMillis = now - status.updatedAtMillis();
    return newInternalRow(
        UTF8String.fromString(tableName),
        status.column() == null ? null : UTF8String.fromString(status.column()),
        UTF8String.fromString(status.indexUuid()),
        null,
        null,
        UTF8String.fromString(status.stateAt(now, STALE_AFTER_MILLIS).name()),
        status.startedAtMillis() * 1000L,
        status.updatedAtMillis() * 1000L,
        ageMillis,
        false,
        status.filesBuilt(),
        status.filesTotal(),
        status.filesFailed(),
        status.rows(),
        status.error() == null ? null : UTF8String.fromString(status.error()));
  }

  /** The name of the indexed column, or null when the schema no longer has that field. */
  private static String columnNameOrNull(Table table, IndexMetadata metadata) {
    org.apache.iceberg.types.Types.NestedField field =
        table.schema().findField(metadata.definition().fieldId());
    return field == null ? null : field.name();
  }

  @Override
  public String description() {
    return "index_build_status";
  }
}
