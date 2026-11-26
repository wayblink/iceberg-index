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
import org.apache.iceberg.index.TextStatisticsPolicy;
import org.apache.iceberg.index.spark.IndexBuildStatus;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds a full BM25 text index for a table snapshot and registers it.
 *
 * <pre>{@code
 * CALL spark_catalog.system.build_text_index(
 *   table => 'db.documents', column => 'body');
 * }</pre>
 *
 * <p>A build also decides what it does with the corpus-wide statistics BM25 scores with: {@code
 * text_statistics => 'auto'|'always'|'never'}, default {@code auto}. The choice is recorded on the
 * index rather than read at query time; see {@link org.apache.iceberg.index.TextStatisticsPolicy}.
 */
class BuildTextIndexProcedure extends BaseProcedure {
  private static final Logger LOG = LoggerFactory.getLogger(BuildTextIndexProcedure.class);

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
  private static final ProcedureParameter TEXT_STATISTICS_PARAM =
      optionalInParameter("text_statistics", DataTypes.StringType);
  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {
        TABLE_PARAM,
        COLUMN_PARAM,
        K1_PARAM,
        LENGTH_NORM_PARAM,
        MIN_TOKEN_LENGTH_PARAM,
        LOWERCASE_PARAM,
        STOPWORDS_PARAM,
        MAX_ENTRIES_PER_INDEX_FILE_PARAM,
        IndexProcedures.NAME_PARAM,
        TEXT_STATISTICS_PARAM
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
          String name = IndexProcedures.name(input);
          IndexProcedures.checkCanBuild(table, column, "text", tableName, name);

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
          // the policy describes the artifact this build produces, so it is recorded with the index
          // rather than read at query time: a query reads a published statistics file when the
          // snapshot has one and sums the shards' counts when it does not
          TextStatisticsPolicy textStatistics =
              input.isProvided(TEXT_STATISTICS_PARAM)
                  ? TextStatisticsPolicy.parse(input.asString(TEXT_STATISTICS_PARAM))
                  : TextStatisticsPolicy.AUTO;
          String indexUuid = UUID.randomUUID().toString();
          long sourceSnapshotId = table.currentSnapshot().snapshotId();
          IndexMetadata created =
              catalog.createIndex(
                  CreateIndexRequest.builder()
                      .tableLocation(table.location())
                      .tableUuid(String.valueOf(table.uuid()))
                      .indexUuid(indexUuid)
                      .name(name)
                      .definition(definition.build())
                      .property(
                          IndexMetadata.MAX_ENTRIES_PER_INDEX_FILE_PROPERTY,
                          Long.toString(maxEntriesPerIndexFile))
                      .property(IndexMetadata.TEXT_STATISTICS_PROPERTY, textStatistics.value())
                      .build());

          IndexMetadata updated;
          try {
            updated =
                SparkIndexBuilder.build(
                    spark(), table, created, sourceSnapshotId, column, System.currentTimeMillis());
            // publishing is part of the attempt: a registration no publish ever pointed at is the
            // same "index that exists with no snapshot" the rollback removes
            VectorIndexProcedures.publishSnapshot(
                catalog, table, indexUuid, updated.latestSnapshot());
          } catch (Throwable failure) {
            // a build that fails must not leave a registration behind: an index with no snapshot
            // looks like an index that exists, and the column then refuses a retry that does not
            // name it, so the registration and whatever it wrote are removed before the failure is
            // reported. Throwable rather than Exception because a driver failure such as an OOM
            // ends the attempt just as surely, and the rollback is the only thing that cleans up.
            IndexBuildStatus report = null;
            try {
              report =
                  IndexBuildStatus.read(
                      table.io(), IndexBuildStatus.statusDirFor(created.location()));
            } catch (RuntimeException unreadable) {
              // the report is an observation: it must not replace the failure that ended the build
              failure.addSuppressed(unreadable);
            }

            boolean treePurged = false;
            try {
              // the whole tree, not only the files a published snapshot names: an attempt that
              // failed can have written files no metadata points at yet
              IndexDrop.purgeTree(spark(), table, indexUuid);
              treePurged = true;
            } catch (RuntimeException rollbackFailure) {
              failure.addSuppressed(rollbackFailure);
            }

            if (treePurged) {
              try {
                // the tree is already gone; removing the registration must not try to read it
                catalog.dropIndex(table.location(), indexUuid, false);
              } catch (RuntimeException rollbackFailure) {
                // the caller has to see the failure that ended the build; a rollback that failed is
                // attached to it rather than thrown in its place
                failure.addSuppressed(rollbackFailure);
              }
            } else {
              // the files this attempt wrote could not be removed, so the registration stays: it is
              // what a later drop by uuid needs to find and purge them. Dropping it here would
              // leave
              // files no entry names, which is the one state a product cannot clean up.
              LOG.warn(
                  "Keeping the registration of the failed index {} of {} so its files can be purged",
                  indexUuid,
                  table.name());
            }

            IndexProcedures.keepFailedAttempt(table, fieldId, column, indexUuid, report, failure);
            if (failure instanceof RuntimeException) {
              throw (RuntimeException) failure;
            } else if (failure instanceof Error) {
              throw (Error) failure;
            } else {
              throw new RuntimeException(failure);
            }
          }

          IndexProcedures.forgetFailedAttempt(table, fieldId);

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
