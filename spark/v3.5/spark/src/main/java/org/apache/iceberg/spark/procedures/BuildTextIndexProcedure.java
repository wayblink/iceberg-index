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

import java.util.UUID;
import org.apache.iceberg.index.CreateIndexRequest;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.IndexSnapshot;
import org.apache.iceberg.index.SidecarIndexCatalog;
import org.apache.iceberg.index.spark.SparkIndexBuilder;
import org.apache.iceberg.index.text.TextIndexDefinition;
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
 * Builds a full BM25 text index for a table snapshot and registers it.
 *
 * <pre>{@code
 * CALL spark_catalog.system.build_text_index(
 *   table => 'db.documents', column => 'body');
 * }</pre>
 */
class BuildTextIndexProcedure extends BaseProcedure {
  private static final ProcedureParameter TABLE_PARAM =
      requiredInParameter("table", DataTypes.StringType);
  private static final ProcedureParameter COLUMN_PARAM =
      requiredInParameter("column", DataTypes.StringType);
  private static final ProcedureParameter K1_PARAM =
      optionalInParameter("k1", DataTypes.DoubleType);
  private static final ProcedureParameter LENGTH_NORM_PARAM =
      optionalInParameter("length_norm", DataTypes.DoubleType);
  private static final ProcedureParameter MIN_TOKEN_LENGTH_PARAM =
      optionalInParameter("min_token_length", DataTypes.IntegerType);
  private static final ProcedureParameter LOWERCASE_PARAM =
      optionalInParameter("lowercase", DataTypes.BooleanType);
  private static final ProcedureParameter MAX_ENTRIES_PER_INDEX_FILE_PARAM =
      optionalInParameter("max_entries_per_index_file", DataTypes.LongType);
  private static final ProcedureParameter STOPWORDS_PARAM =
      optionalInParameter("stopwords", STRING_ARRAY);
  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {
        TABLE_PARAM,
        COLUMN_PARAM,
        K1_PARAM,
        LENGTH_NORM_PARAM,
        MIN_TOKEN_LENGTH_PARAM,
        LOWERCASE_PARAM,
        STOPWORDS_PARAM,
        MAX_ENTRIES_PER_INDEX_FILE_PARAM
      };

  private static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField("index_uuid", DataTypes.StringType, false, Metadata.empty()),
            new StructField("source_snapshot_id", DataTypes.LongType, false, Metadata.empty()),
            new StructField("index_files", DataTypes.LongType, false, Metadata.empty()),
            new StructField("entries", DataTypes.LongType, false, Metadata.empty())
          });

  public static ProcedureBuilder builder() {
    return new BaseProcedure.Builder<BuildTextIndexProcedure>() {
      @Override
      public BuildTextIndexProcedure doBuild() {
        return new BuildTextIndexProcedure(tableCatalog());
      }
    };
  }

  private BuildTextIndexProcedure(TableCatalog tableCatalog) {
    super(tableCatalog);
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
    VectorIndexProcedures.registerTextIndexType();
    ProcedureInput input = new ProcedureInput(spark(), tableCatalog(), PARAMETERS, args);
    String tableName = input.asString(TABLE_PARAM);
    String column = input.asString(COLUMN_PARAM);

    return modifyIcebergTable(
        input.ident(TABLE_PARAM),
        table -> {
          if (table.currentSnapshot() == null) {
            throw new IllegalArgumentException(
                "Table " + tableName + " has no data: cannot build an index");
          }

          int fieldId = VectorIndexProcedures.fieldId(table, column);
          SidecarIndexCatalog catalog = VectorIndexProcedures.indexCatalog(table);
          if (VectorIndexProcedures.findAnyIndexOrNull(table, column) != null) {
            throw new IllegalArgumentException(
                "An index already exists for column " + column + " of table " + tableName);
          }

          TextIndexDefinition.Builder definition = TextIndexDefinition.builder().fieldId(fieldId);
          if (input.isProvided(K1_PARAM)) {
            definition.k1(args.getDouble(VectorIndexProcedures.ordinal(PARAMETERS, K1_PARAM)));
          }
          if (input.isProvided(LENGTH_NORM_PARAM)) {
            definition.lengthNorm(
                args.getDouble(VectorIndexProcedures.ordinal(PARAMETERS, LENGTH_NORM_PARAM)));
          }
          if (input.isProvided(MIN_TOKEN_LENGTH_PARAM)) {
            definition.minTokenLength(input.asInt(MIN_TOKEN_LENGTH_PARAM));
          }
          if (input.isProvided(LOWERCASE_PARAM)) {
            definition.lowercase(
                args.getBoolean(VectorIndexProcedures.ordinal(PARAMETERS, LOWERCASE_PARAM)));
          }
          for (String stopword : input.asStringArray(STOPWORDS_PARAM, new String[0])) {
            definition.stopword(stopword);
          }

          long maxEntriesPerIndexFile =
              VectorIndexProcedures.maxEntriesPerIndexFile(input, MAX_ENTRIES_PER_INDEX_FILE_PARAM);
          String indexUuid = UUID.randomUUID().toString();
          long sourceSnapshotId = table.currentSnapshot().snapshotId();
          IndexMetadata created =
              catalog.createIndex(
                  CreateIndexRequest.builder()
                      .tableLocation(table.location())
                      .tableUuid(String.valueOf(table.uuid()))
                      .indexUuid(indexUuid)
                      .definition(definition.build())
                      .property(
                          IndexMetadata.MAX_ENTRIES_PER_INDEX_FILE_PROPERTY,
                          Long.toString(maxEntriesPerIndexFile))
                      .build());

          IndexMetadata updated =
              SparkIndexBuilder.build(
                  spark(), table, created, sourceSnapshotId, column, System.currentTimeMillis());
          catalog.updateIndex(updated);

          IndexSnapshot snapshot = updated.latestSnapshot();
          long[] counts = VectorIndexProcedures.counts(table.io(), snapshot);
          InternalRow row =
              newInternalRow(
                  UTF8String.fromString(indexUuid), sourceSnapshotId, counts[0], counts[1]);
          return new InternalRow[] {row};
        });
  }

  @Override
  public String description() {
    return "BuildTextIndexProcedure";
  }
}
