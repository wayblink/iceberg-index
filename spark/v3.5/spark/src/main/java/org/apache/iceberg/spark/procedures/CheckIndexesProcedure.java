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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.Table;
import org.apache.iceberg.index.IndexAdapter;
import org.apache.iceberg.index.IndexBlob;
import org.apache.iceberg.index.IndexFile;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.IndexQuery;
import org.apache.iceberg.index.IndexReader;
import org.apache.iceberg.index.IndexSnapshot;
import org.apache.iceberg.index.IndexTypes;
import org.apache.iceberg.index.PuffinIndexFileContainer;
import org.apache.iceberg.index.spark.IndexEntries;
import org.apache.iceberg.index.spark.VectorIndexes;
import org.apache.iceberg.index.vector.VectorIndexAdapter;
import org.apache.iceberg.index.vector.VectorPayloadHeader;
import org.apache.iceberg.inmemory.InMemoryInputFile;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
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
 * <p>A payload names the format and version that wrote it, so a build whose format has moved on
 * cannot read an index written before that, and the only fix is a rebuild. Until this existed the
 * mismatch surfaced as an error on the first query that touched the index, which is a bad time to
 * learn about it and a worse way to find the others.
 *
 * <p>A table holds indexes of more than one type, so every index is checked by the reader of its
 * own type: what says an index is unreadable is a property of the type, a kernel version in the
 * payload header for vectors and the blob type the manifest declares for text.
 */
class CheckIndexesProcedure extends BaseProcedure {

  // package private so a test can pin the statement a row suggests without staging a broken index
  static String refreshSuggestionFor(
      String catalog, String tableName, String column, String indexType) {
    return refreshSuggestion(catalog, tableName, column, indexType);
  }

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
            new StructField("uncovered_data_files", DataTypes.LongType, true, Metadata.empty()),
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

    // resolved through the catalog the call names, so that the table of `CALL local.system...` is
    // the one checked even when the session's current catalog is another one
    return withIcebergTable(
        input.ident(TABLE_PARAM),
        table -> {
          try {
            checkTable(tableCatalog().name(), table, tableName, rows);
          } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException("Failed to read index payloads", e);
          }

          return rows.toArray(new InternalRow[0]);
        });
  }

  private void checkTable(String catalog, Table table, String tableName, List<InternalRow> rows)
      throws java.io.IOException {
    for (IndexMetadata metadata : VectorIndexes.indexesOf(table)) {
      IndexSnapshot snapshot = metadata.latestSnapshot();
      String column = VectorIndexProcedures.columnName(table, metadata.definition().fieldId());
      // what this index does not cover: a build that lost a group, an append nobody refreshed, or a
      // rewrite that replaced a covered file. Queries still answer correctly - the uncovered files
      // are scanned - but every one of them is a slower path, so the count belongs in the report.
      Long uncovered = uncoveredDataFiles(table, snapshot);

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
                uncovered,
                "the index has no snapshot; build or refresh it"));
        continue;
      }

      // the reader belongs to the index type, so a table holding indexes of several types is
      // checked one type-specific way each rather than through the one type this knows best
      IndexAdapter adapter =
          IndexTypes.fromType(metadata.indexType()).createAdapter(metadata.definition());
      if (adapter instanceof VectorIndexAdapter) {
        checkVectorPayload(
            catalog,
            table,
            tableName,
            column,
            metadata,
            snapshot,
            (VectorIndexAdapter) adapter,
            uncovered,
            rows);
      } else {
        checkDeclaredPayloads(
            catalog, table, tableName, column, metadata, snapshot, adapter, uncovered, rows);
      }
    }
  }

  /**
   * Checks a vector index by the kernel version written inside the first payload, which is what a
   * reader of this type matches on.
   */
  private void checkVectorPayload(
      String catalog,
      Table table,
      String tableName,
      String column,
      IndexMetadata metadata,
      IndexSnapshot snapshot,
      VectorIndexAdapter adapter,
      Long uncovered,
      List<InternalRow> rows)
      throws java.io.IOException {
    String indexType = metadata.indexType();
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
              uncovered,
              "payload header is unreadable ("
                  + e.getMessage()
                  + "); "
                  + refreshSuggestion(catalog, tableName, column, indexType)));
      return;
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
              uncovered,
              "no payload found in the index files; "
                  + refreshSuggestion(catalog, tableName, column, indexType)));
      return;
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
            uncovered,
            readable
                ? incompleteCoverageSuggestion(catalog, tableName, column, indexType, uncovered)
                : "payload was written by "
                    + header
                    + ", this build reads "
                    + adapter.kernel().name()
                    + " version "
                    + readerVersion
                    + "; "
                    + refreshSuggestion(catalog, tableName, column, indexType)));
  }

  /**
   * Checks an index type whose payload version is part of the blob type instead of a header inside
   * the payload, such as the text index.
   *
   * <p>Such a type says in its own adapter which blob types it reads, and the manifest declares the
   * blob type of every index file, so the manifest alone answers whether the payloads of a snapshot
   * can be read: no file has to be opened. A payload that declares a version this build does not
   * read would be skipped by a query rather than fail it, which is why a partly readable snapshot
   * is reported as needing a rebuild instead of as readable.
   */
  private void checkDeclaredPayloads(
      String catalog,
      Table table,
      String tableName,
      String column,
      IndexMetadata metadata,
      IndexSnapshot snapshot,
      IndexAdapter adapter,
      Long uncovered,
      List<InternalRow> rows) {
    String indexType = metadata.indexType();
    Set<String> present = new LinkedHashSet<>();
    Set<String> unreadable = new LinkedHashSet<>();
    for (IndexFile file : VectorIndexes.indexFiles(table.io(), snapshot)) {
      present.add(file.blobType());
      if (!readablePayload(adapter, file.kind(), file.blobType())) {
        unreadable.add(file.blobType());
      }
    }

    if (present.isEmpty()) {
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
              uncovered,
              "no payload found in the index files; "
                  + refreshSuggestion(catalog, tableName, column, indexType)));
      return;
    }

    boolean readable = unreadable.isEmpty();
    rows.add(
        row(
            table,
            column,
            metadata,
            null,
            -1,
            -1,
            readable,
            snapshot.sourceTableSnapshotId(),
            isStale(table, snapshot),
            uncovered,
            readable
                ? null
                : "payloads were written as "
                    + String.join(", ", unreadable)
                    + ", this build reads "
                    + String.join(", ", readableBlobTypes(adapter))
                    + "; "
                    + refreshSuggestion(catalog, tableName, column, indexType)));
  }

  // package private so a test can pin the decision without staging an index of another version
  static boolean readablePayload(IndexAdapter adapter, IndexFile.Kind kind, String blobType) {
    Set<String> readableTypes =
        kind == IndexFile.Kind.ENTRY_BEARING
            ? adapter.readableEntryBlobTypes()
            : adapter.readableAuxiliaryBlobTypes();
    return readableTypes.contains(blobType);
  }

  /** Every blob type this build reads for the index, which is what a rebuild has to produce. */
  private static Set<String> readableBlobTypes(IndexAdapter adapter) {
    Set<String> blobTypes = new LinkedHashSet<>(adapter.readableEntryBlobTypes());
    blobTypes.addAll(adapter.readableAuxiliaryBlobTypes());
    return blobTypes;
  }

  /** The statement that rebuilds the index, which is the only thing a mismatch leaves to do. */
  private static String refreshSuggestion(
      String catalog, String tableName, String column, String indexType) {
    // a table named with or without its catalog both end up as catalog.namespace.table
    String bare =
        tableName.startsWith(catalog + ".") ? tableName.substring(catalog.length() + 1) : tableName;
    return String.format(
        "run CALL %s.system.refresh_%s_index(table => '%s', column => '%s')",
        catalog, indexType, catalog + "." + bare, column);
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

  /**
   * How many data files of the current snapshot the index does not cover.
   *
   * <p>Coverage is a file-level property the manifest states, so this reads the manifest rather
   * than the table data. It is null when there is no snapshot: nothing is known about coverage
   * then, and a zero would read as "fully covered".
   */
  /**
   * The line a fully readable index gets: nothing, or what to do about the files it does not cover.
   *
   * <p>A gap is not damage - a query scans those files and still answers - so the suggestion says
   * what closes it rather than that something is broken. A refresh only appends segments, which is
   * why it is offered first.
   */
  private static String incompleteCoverageSuggestion(
      String catalog, String tableName, String column, String indexType, Long uncovered) {
    if (uncovered == null || uncovered == 0L) {
      return null;
    }

    return uncovered
        + " data files of the current snapshot are not covered by this index, so a query scans them"
        + " each time; "
        + refreshSuggestion(catalog, tableName, column, indexType);
  }

  private static Long uncoveredDataFiles(Table table, IndexSnapshot snapshot) {
    if (snapshot == null || table.currentSnapshot() == null) {
      return null;
    }

    Set<String> covered = Sets.newHashSet();
    for (IndexFile file : VectorIndexes.indexFiles(table.io(), snapshot)) {
      covered.addAll(file.coveredDataFiles());
    }

    List<String> dataFiles = IndexEntries.dataFiles(table, table.currentSnapshot().snapshotId());
    int uncovered = 0;
    for (String dataFile : dataFiles) {
      if (!covered.contains(dataFile)) {
        uncovered += 1;
      }
    }

    return (long) uncovered;
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
      Long uncoveredDataFiles,
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
        uncoveredDataFiles,
        suggestion == null ? null : UTF8String.fromString(suggestion));
  }

  @Override
  public String description() {
    return "check_indexes";
  }
}
