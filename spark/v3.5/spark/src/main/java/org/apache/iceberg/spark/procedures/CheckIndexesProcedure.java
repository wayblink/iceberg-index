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

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.iceberg.Table;
import org.apache.iceberg.index.IndexBlob;
import org.apache.iceberg.index.IndexFile;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.IndexQuery;
import org.apache.iceberg.index.IndexReader;
import org.apache.iceberg.index.IndexSnapshot;
import org.apache.iceberg.index.PuffinIndexFileContainer;
import org.apache.iceberg.index.vector.VectorIndexAdapter;
import org.apache.iceberg.index.vector.VectorIndexDefinition;
import org.apache.iceberg.index.vector.VectorPayloadHeader;
import org.apache.iceberg.inmemory.InMemoryInputFile;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.InputFile;
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
 * Reports whether the payloads of a table's indexes can be read by this build.
 *
 * <p>A payload names the kernel and version that wrote it, so a build whose kernel has moved on
 * cannot read an index written before that, and the only fix is a rebuild. Until this existed the
 * mismatch surfaced as an error on the first query that touched the index, which is a bad time to
 * learn about it and a worse way to find the others.
 */
class CheckIndexesProcedure extends BaseProcedure {

  private static final ProcedureParameter TABLE_PARAM =
      ProcedureParameter.required("table", DataTypes.StringType);

  private static final ProcedureParameter[] PARAMETERS = new ProcedureParameter[] {TABLE_PARAM};

  private static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField("table", DataTypes.StringType, false, Metadata.empty()),
            new StructField("column", DataTypes.StringType, true, Metadata.empty()),
            new StructField("index_uuid", DataTypes.StringType, false, Metadata.empty()),
            new StructField("index_name", DataTypes.StringType, true, Metadata.empty()),
            new StructField("index_type", DataTypes.StringType, false, Metadata.empty()),
            new StructField("writer", DataTypes.StringType, true, Metadata.empty()),
            new StructField("writer_version", DataTypes.IntegerType, true, Metadata.empty()),
            new StructField("reader_version", DataTypes.IntegerType, true, Metadata.empty()),
            new StructField("readable", DataTypes.BooleanType, false, Metadata.empty()),
            new StructField("source_snapshot_id", DataTypes.LongType, true, Metadata.empty()),
            new StructField("stale", DataTypes.BooleanType, false, Metadata.empty()),
            new StructField("suggestion", DataTypes.StringType, true, Metadata.empty())
          });

  public static ProcedureBuilder builder() {
    return new BaseProcedure.Builder<CheckIndexesProcedure>() {
      @Override
      public CheckIndexesProcedure doBuild() {
        return new CheckIndexesProcedure(tableCatalog());
      }
    };
  }

  private CheckIndexesProcedure(TableCatalog catalog) {
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
    ProcedureInput input = new ProcedureInput(spark(), tableCatalog(), PARAMETERS, args);
    List<InternalRow> rows = Lists.newArrayList();

    String tableName = input.asString(TABLE_PARAM, null);
    Table table;
    try {
      table = org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark(), tableName);
      checkTable(table, rows);
    } catch (org.apache.spark.sql.catalyst.parser.ParseException e) {
      throw new IllegalArgumentException("Not a table identifier: " + tableName, e);
    } catch (org.apache.spark.sql.catalyst.analysis.NoSuchTableException e) {
      throw new IllegalArgumentException("No such table: " + tableName, e);
    } catch (java.io.IOException e) {
      throw new java.io.UncheckedIOException("Failed to read index payloads", e);
    }

    return rows.toArray(new InternalRow[0]);
  }

  private void checkTable(Table table, List<InternalRow> rows) throws java.io.IOException {
    for (IndexMetadata metadata : org.apache.iceberg.index.spark.VectorIndexes.indexesOf(table)) {
      IndexSnapshot snapshot = metadata.latestSnapshot();
      String column = VectorIndexProcedures.columnName(table, metadata.definition().fieldId());

      if (snapshot == null) {
        rows.add(
            row(
                table,
                column,
                metadata,
                null,
                -1,
                -1,
                false,
                null,
                false,
                "the index has no snapshot; build or refresh it"));
        continue;
      }

      VectorIndexAdapter adapter =
          new VectorIndexAdapter((VectorIndexDefinition) metadata.definition());
      VectorPayloadHeader header;
      try {
        header = readHeader(table, metadata, snapshot, adapter);
      } catch (RuntimeException e) {
        // a payload whose header cannot be read at all is exactly what this reports: the index has
        // to be rebuilt, and the error says why
        rows.add(
            row(
                table,
                column,
                metadata,
                null,
                -1,
                adapter.kernel().payloadFormatVersion(),
                false,
                snapshot.sourceTableSnapshotId(),
                isStale(table, snapshot),
                "payload header is unreadable (" + e.getMessage() + "); rebuild the index"));
        continue;
      }

      if (header == null) {
        rows.add(
            row(
                table,
                column,
                metadata,
                null,
                -1,
                -1,
                false,
                snapshot.sourceTableSnapshotId(),
                isStale(table, snapshot),
                "no payload found in the index files; refresh the index"));
        continue;
      }

      boolean readable = header.readableBy(adapter.kernel());
      int readerVersion = adapter.kernel().payloadFormatVersion();
      rows.add(
          row(
              table,
              column,
              metadata,
              header.kernel(),
              header.kernelVersion(),
              readerVersion,
              readable,
              snapshot.sourceTableSnapshotId(),
              isStale(table, snapshot),
              readable
                  ? null
                  : "payload was written by "
                      + header
                      + ", this build reads "
                      + adapter.kernel().name()
                      + " version "
                      + readerVersion
                      + "; rebuild the index"));
    }
  }

  /** Reads the header of the first payload, which is where a mismatch shows up first. */
  private VectorPayloadHeader readHeader(
      Table table, IndexMetadata metadata, IndexSnapshot snapshot, VectorIndexAdapter adapter)
      throws java.io.IOException {
    List<IndexFile> files =
        new IndexReader(metadata, table)
            .indexFiles(IndexQuery.of(snapshot.sourceTableSnapshotId(), ByteBuffer.allocate(4), 1));
    if (files.isEmpty()) {
      return null;
    }

    String blobType =
        adapter.writesAuxiliaryFiles() ? adapter.auxiliaryBlobType() : adapter.entryBlobType();
    IndexFile file = files.get(0);
    byte[] content;
    InputFile input = table.io().newInputFile(file.location());
    content = new byte[(int) input.getLength()];
    try (org.apache.iceberg.io.SeekableInputStream in = input.newStream()) {
      IOUtil.readFully(in, content, 0, content.length);
    }

    for (IndexBlob blob : new PuffinIndexFileContainer().read(new InMemoryInputFile(content))) {
      if (blobType.equals(blob.type())) {
        return VectorPayloadHeader.of(blob.payload());
      }
    }

    return null;
  }

  private static boolean isStale(Table table, IndexSnapshot snapshot) {
    return table.currentSnapshot() != null
        && table.currentSnapshot().snapshotId() != snapshot.sourceTableSnapshotId();
  }

  private InternalRow row(
      Table table,
      String column,
      IndexMetadata metadata,
      String writer,
      int writerVersion,
      int readerVersion,
      boolean readable,
      Long sourceSnapshotId,
      boolean stale,
      String suggestion) {
    return newInternalRow(
        UTF8String.fromString(table.name()),
        column == null ? null : UTF8String.fromString(column),
        UTF8String.fromString(metadata.indexUuid()),
        metadata.name() == null ? null : UTF8String.fromString(metadata.name()),
        UTF8String.fromString(metadata.indexType()),
        writer == null ? null : UTF8String.fromString(writer),
        writerVersion < 0 ? null : writerVersion,
        readerVersion < 0 ? null : readerVersion,
        readable,
        sourceSnapshotId,
        stale,
        suggestion == null ? null : UTF8String.fromString(suggestion));
  }

  @Override
  public String description() {
    return "check_indexes";
  }
}
