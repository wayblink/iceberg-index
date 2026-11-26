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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.iceberg.Table;
import org.apache.iceberg.index.IndexAdapter;
import org.apache.iceberg.index.IndexFile;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.IndexQuery;
import org.apache.iceberg.index.IndexReader;
import org.apache.iceberg.index.IndexSnapshot;
import org.apache.iceberg.index.IndexTypes;
import org.apache.iceberg.index.PuffinIndexFileContainer;
import org.apache.iceberg.index.SidecarIndexCatalog;
import org.apache.iceberg.index.spark.IndexEntries;
import org.apache.iceberg.index.spark.VectorIndexes;
import org.apache.iceberg.index.text.TextIndexAdapter;
import org.apache.iceberg.index.vector.VectorIndexAdapter;
import org.apache.iceberg.index.vector.VectorIndexDefinition;
import org.apache.iceberg.index.vector.VectorIndexDefinition.VectorStructure;
import org.apache.iceberg.index.vector.VectorPayloadHeader;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.procedures.SparkProcedures.ProcedureBuilder;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
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
 * own type: what says an index is unreadable is a property of the type. Both types this build knows
 * write the format version inside the payload, so both are checked by reading the header of the
 * first payload; a type whose version is part of the blob type instead is checked by the manifest
 * alone, which is why that check is kept as the fallback.
 */
class CheckIndexesProcedure extends BaseProcedure {

  // package private so a test can pin the statement a row suggests without staging a broken index
  static String refreshSuggestionFor(
      String catalog, String tableName, String column, String indexType) {
    return refreshSuggestion(catalog, qualified(catalog, tableName), column, indexType);
  }

  // package private so a test can pin the statements that migrate an unreadable index
  static String rebuildSuggestionFor(
      String catalog, String tableName, String column, String indexType, String indexUuid) {
    return rebuildSuggestion(
        catalog, qualified(catalog, tableName), column, indexType, indexUuid, "");
  }

  /**
   * How many bytes of a payload the checks read.
   *
   * <p>A payload starts with the format version that wrote it, and a vector payload also names its
   * kernel and repeats a version after the name, so a kilobyte covers both headers with room for a
   * long kernel name. Reading no more than this is what keeps a check of an index of hundreds of
   * megabytes from depending on the driver heap.
   */
  private static final int PAYLOAD_PREFIX_BYTES = 1024;

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

    Identifier identifier = input.ident(TABLE_PARAM);
    String catalog = tableCatalog().name();
    String tableName = Spark3Util.quotedFullIdentifier(catalog, identifier);

    // resolved through the catalog the call names, so that the table of `CALL local.system...` is
    // the one checked even when the session's current catalog is another one
    return withIcebergTable(
        identifier,
        table -> {
          checkTable(quotedCatalog(catalog), table, tableName, rows);
          return rows.toArray(new InternalRow[0]);
        });
  }

  private void checkTable(String catalog, Table table, String tableName, List<InternalRow> rows) {
    for (IndexMetadata metadata : VectorIndexes.indexesOf(table)) {
      String column = null;
      try {
        IndexSnapshot snapshot = metadata.latestSnapshot();
        column = VectorIndexProcedures.columnName(table, metadata.definition().fieldId());
        // what this index does not cover: a build that lost a group, an append nobody refreshed, or
        // a
        // rewrite that replaced a covered file. Queries still answer correctly - the uncovered
        // files
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
                  "the index has no snapshot; it cannot be refreshed: "
                      + rebuildSuggestion(catalog, table, tableName, column, metadata)));
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
      } catch (RuntimeException e) {
        // one index whose manifests or payloads cannot be read is reported as unreadable instead of
        // failing the report of every other index of the table
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
                null,
                "the index could not be checked: "
                    + summary(e)
                    + "; report it as unreadable, or remove it by UUID with "
                    + "CALL "
                    + catalog
                    + ".system.drop_index(table => '"
                    + tableName
                    + "', column => '<any column>', index => '"
                    + metadata.indexUuid()
                    + "', purge => true)"));
      }
    }

    // entries the registry names whose metadata is gone are left out of the listing above, because
    // one unreadable entry must not fail the report of the others; they are still reported here so
    // the registry can be repaired by dropping them by UUID
    SidecarIndexCatalog sidecar = VectorIndexProcedures.indexCatalog(table);
    for (String indexUuid : sidecar.damagedIndexUuids(table.location())) {
      rows.add(
          newInternalRow(
              UTF8String.fromString(table.name()),
              null,
              UTF8String.fromString(indexUuid),
              null,
              UTF8String.fromString("unknown"),
              null,
              null,
              null,
              false,
              null,
              false,
              null,
              UTF8String.fromString(
                  "the registry names this index but its metadata file is gone; remove it with "
                      + "CALL "
                      + catalog
                      + ".system.drop_index(table => '"
                      + tableName
                      + "', column => '<any column>', index => '"
                      + indexUuid
                      + "', purge => true)")));
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
      List<InternalRow> rows) {
    String indexType = metadata.indexType();
    int readerVersion = adapter.kernel().payloadFormatVersion();
    VectorPayloadHeader header;
    try {
      header = readHeader(table, metadata, snapshot, adapter);
    } catch (IOException | RuntimeException e) {
      // a payload whose header cannot be read at all - a file that is gone or unreadable, or bytes
      // that do not parse - is exactly what this reports: the index has to be rebuilt, and the
      // error says why
      rows.add(
          row(
              table,
              column,
              metadata,
              null,
              -1,
              readerVersion,
              false,
              snapshot.sourceTableSnapshotId(),
              isStale(table, snapshot),
              uncovered,
              "payload header is unreadable ("
                  + summary(e)
                  + "); "
                  + rebuildSuggestion(catalog, table, tableName, column, metadata)));
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
                  + rebuildSuggestion(catalog, table, tableName, column, metadata)));
      return;
    }

    boolean readable = header.readableBy(adapter.kernel());
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
                    + rebuildSuggestion(catalog, table, tableName, column, metadata)));
  }

  /**
   * Checks an index type whose format version is part of the blob type instead of the payload
   * header, such as the text index, and additionally reads the payload version of a type that does
   * write one there.
   *
   * <p>A type that names its version in the blob type says in its adapter which blob types it
   * reads, and the manifest declares the blob type of every index file, so the manifest alone can
   * answer whether the payloads of a snapshot can be read: no file has to be opened. A type that
   * writes the version in the payload, as the text index now does, is checked the way the vector
   * index is: read the header of the first payload and compare its version with the one this build
   * reads.
   *
   * <p>A payload that declares a version this build does not read would be skipped by a query
   * rather than fail it, which is why a partly readable snapshot is reported as needing a rebuild
   * instead of as readable. A refresh carries the readable files of the previous snapshot over, so
   * it cannot migrate an index whose bytes this build does not decode: the report gives the drop
   * and the build that can.
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
    TextIndexAdapter text = adapter instanceof TextIndexAdapter ? (TextIndexAdapter) adapter : null;
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
              text == null ? null : "text",
              -1,
              text == null ? -1 : text.payloadFormatVersion(),
              false,
              snapshot.sourceTableSnapshotId(),
              isStale(table, snapshot),
              uncovered,
              "no payload found in the index files; "
                  + (text == null
                      ? refreshSuggestion(catalog, tableName, column, indexType)
                      : rebuildSuggestion(catalog, table, tableName, column, metadata))));
      return;
    }

    int readerVersion = -1;
    Integer writerVersion = null;
    boolean versionReadable = true;
    boolean missingPayload = false;
    String headerError = null;
    if (text != null) {
      readerVersion = text.payloadFormatVersion();
      if (unreadable.isEmpty()) {
        try {
          byte[] payload = firstPayload(table, metadata, snapshot, text.auxiliaryBlobType());
          if (payload == null) {
            missingPayload = true;
            versionReadable = false;
          } else {
            writerVersion = text.payloadVersion(ByteBuffer.wrap(payload));
            versionReadable = writerVersion == readerVersion;
          }
        } catch (IOException | RuntimeException e) {
          // a payload that cannot be read at all - a file that is gone or unreadable, or bytes that
          // do not parse - is exactly what this reports
          versionReadable = false;
          headerError = summary(e);
        }
      }
    }

    boolean readable = unreadable.isEmpty() && versionReadable;
    String suggestion = null;
    if (!readable) {
      String reason;
      if (!unreadable.isEmpty()) {
        reason =
            "payloads were written as "
                + String.join(", ", unreadable)
                + ", this build reads "
                + String.join(", ", readableBlobTypes(adapter));
      } else if (missingPayload) {
        reason = "no payload found in the index files";
      } else if (headerError != null) {
        reason = "payload header is unreadable (" + headerError + ")";
      } else {
        reason =
            "payload was written by text version "
                + writerVersion
                + ", this build reads "
                + readerVersion;
      }

      suggestion =
          reason
              + "; "
              + (text == null
                  ? refreshSuggestion(catalog, tableName, column, indexType)
                  : rebuildSuggestion(catalog, table, tableName, column, metadata));
    }

    rows.add(
        row(
            table,
            column,
            metadata,
            text == null ? null : "text",
            writerVersion == null ? -1 : writerVersion,
            readerVersion,
            readable,
            snapshot.sourceTableSnapshotId(),
            isStale(table, snapshot),
            uncovered,
            suggestion));
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

  /** The statement that asks the index of the type to refresh itself from the table. */
  private static String refreshSuggestion(
      String catalog, String tableName, String column, String indexType) {
    return String.format(
        "run CALL %s.system.refresh_%s_index(table => '%s', column => '%s')",
        catalog, indexType, tableName, column);
  }

  /**
   * The two statements that rebuild an index this build cannot read.
   *
   * <p>A refresh carries the readable files of the previous snapshot over, so a payload of another
   * format version survives it: the index has to be dropped and built again. Both statements are
   * named, in the order they have to run, because either one alone leaves the index as it is.
   */
  private static String rebuildSuggestion(
      String catalog, Table table, String tableName, String column, IndexMetadata metadata) {
    StringBuilder options = new StringBuilder();
    if (metadata.definition() instanceof VectorIndexDefinition vector) {
      options.append(", dimension => ").append(vector.dimension());
      options.append(", metric => ").append(sqlString(vector.metric().name()));
      options.append(", structure => ").append(sqlString(vector.structure().name()));
      options.append(", quantizer => ").append(sqlString(vector.quantizer().name()));

      var properties = new TreeMap<>(vector.properties());
      if (vector.structure() == VectorStructure.IVF) {
        // Older indexes persisted nprobe, which is now a query setting.
        properties.remove("nprobe");
      }

      if (!properties.isEmpty()) {
        String arguments =
            properties.entrySet().stream()
                .map(entry -> sqlString(entry.getKey()) + ", " + sqlString(entry.getValue()))
                .collect(Collectors.joining(", "));
        options.append(", options => map(").append(arguments).append(")");
      }

      options
          .append(", max_entries_per_index_file => ")
          .append(metadata.maxEntriesPerIndexFile())
          .append("L");
    }

    String name = metadata.name();
    if (name == null && column != null && VectorIndexes.indexesFor(table, column).size() > 1) {
      // A replacement alongside another index needs a name. The old UUID remains a valid selector.
      name = metadata.indexUuid();
    }

    if (name != null) {
      options.append(", name => ").append(sqlString(name));
    }

    return rebuildSuggestion(
        catalog, tableName, column, metadata.indexType(), metadata.indexUuid(), options.toString());
  }

  private static String rebuildSuggestion(
      String catalog,
      String tableName,
      String column,
      String indexType,
      String indexUuid,
      String options) {
    String table = sqlString(tableName);
    String field = sqlString(String.valueOf(column));
    return String.format(
        "run CALL %s.system.drop_index(table => %s, column => %s, index => %s, purge => true)"
            + " then CALL %s.system.build_%s_index(table => %s, column => %s%s)",
        catalog, table, field, sqlString(indexUuid), catalog, indexType, table, field, options);
  }

  private static String sqlString(String value) {
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'";
  }

  private static String quotedCatalog(String catalog) {
    return catalog.matches("[a-zA-Z_][a-zA-Z0-9_]*")
        ? catalog
        : "`" + catalog.replace("`", "``") + "`";
  }

  /** A table named with or without its catalog both end up as catalog.namespace.table. */
  private static String qualified(String catalog, String tableName) {
    String bare =
        tableName.startsWith(catalog + ".") ? tableName.substring(catalog.length() + 1) : tableName;
    return catalog + "." + bare;
  }

  /** What a failure says went wrong, which is what the report quotes of it. */
  private static String summary(Throwable failure) {
    String message = failure.getMessage();
    return message == null || message.isEmpty() ? failure.getClass().getSimpleName() : message;
  }

  /** Reads the header of the first payload, which is where a mismatch shows up first. */
  private VectorPayloadHeader readHeader(
      Table table, IndexMetadata metadata, IndexSnapshot snapshot, VectorIndexAdapter adapter)
      throws IOException {
    byte[] payload =
        firstPayload(
            table,
            metadata,
            snapshot,
            adapter.writesAuxiliaryFiles() ? adapter.auxiliaryBlobType() : adapter.entryBlobType());
    return payload == null ? null : VectorPayloadHeader.of(payload);
  }

  /**
   * The head of the payload of the first file of the index, which is where a format version shows
   * up first.
   *
   * <p>The version is in the first bytes of the payload, so only those are read: the file can be
   * hundreds of megabytes, and an operations command must not hold one whole index file in the
   * driver heap to report a version.
   *
   * @return the first {@link #PAYLOAD_PREFIX_BYTES} bytes of the payload, or null when the index
   *     has no file or its first file carries no blob of the given type
   */
  private byte[] firstPayload(
      Table table, IndexMetadata metadata, IndexSnapshot snapshot, String blobType)
      throws IOException {
    List<IndexFile> files =
        new IndexReader(metadata, table)
            .indexFiles(IndexQuery.of(snapshot.sourceTableSnapshotId(), ByteBuffer.allocate(4), 1));
    if (files.isEmpty()) {
      return null;
    }

    InputFile input = table.io().newInputFile(files.get(0).location());
    return PuffinIndexFileContainer.payloadPrefix(input, blobType, PAYLOAD_PREFIX_BYTES);
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
