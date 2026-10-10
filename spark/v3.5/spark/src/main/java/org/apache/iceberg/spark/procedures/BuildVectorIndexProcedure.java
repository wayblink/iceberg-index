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
import org.apache.iceberg.index.vector.VectorIndexDefinition;
import org.apache.iceberg.index.vector.VectorIndexDefinition.IndexMode;
import org.apache.iceberg.index.vector.VectorIndexDefinition.Metric;
import org.apache.iceberg.spark.index.SparkIndexBuilder;
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
 * Builds a full vector index for a table snapshot and registers it.
 *
 * <pre>{@code
 * CALL spark_catalog.system.build_vector_index(
 *   table => 'db.documents', column => 'embedding', dimension => 768, metric => 'cosine');
 * }</pre>
 */
class BuildVectorIndexProcedure extends BaseProcedure {
  private static final ProcedureParameter TABLE_PARAM =
      requiredInParameter("table", DataTypes.StringType);
  private static final ProcedureParameter COLUMN_PARAM =
      requiredInParameter("column", DataTypes.StringType);
  private static final ProcedureParameter DIMENSION_PARAM =
      requiredInParameter("dimension", DataTypes.IntegerType);
  private static final ProcedureParameter METRIC_PARAM =
      optionalInParameter("metric", DataTypes.StringType);
  private static final ProcedureParameter MODE_PARAM =
      optionalInParameter("mode", DataTypes.StringType);
  private static final ProcedureParameter MAX_M_PARAM =
      optionalInParameter("max_m", DataTypes.IntegerType);
  private static final ProcedureParameter EF_CONSTRUCTION_PARAM =
      optionalInParameter("ef_construction", DataTypes.IntegerType);
  private static final ProcedureParameter ENTRIES_PER_INDEX_FILE_PARAM =
      optionalInParameter("entries_per_index_file", DataTypes.LongType);
  private static final ProcedureParameter EF_SEARCH_PARAM =
      optionalInParameter("ef_search", DataTypes.IntegerType);
  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {
        TABLE_PARAM,
        COLUMN_PARAM,
        DIMENSION_PARAM,
        METRIC_PARAM,
        MODE_PARAM,
        MAX_M_PARAM,
        EF_CONSTRUCTION_PARAM,
        EF_SEARCH_PARAM,
        ENTRIES_PER_INDEX_FILE_PARAM
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
    return new BaseProcedure.Builder<BuildVectorIndexProcedure>() {
      @Override
      public BuildVectorIndexProcedure doBuild() {
        return new BuildVectorIndexProcedure(tableCatalog());
      }
    };
  }

  private BuildVectorIndexProcedure(TableCatalog tableCatalog) {
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
    VectorIndexProcedures.registerVectorIndexType();
    ProcedureInput input = new ProcedureInput(spark(), tableCatalog(), PARAMETERS, args);
    String tableName = input.asString(TABLE_PARAM);
    String column = input.asString(COLUMN_PARAM);
    int dimension = input.asInt(DIMENSION_PARAM);
    Metric metric = parseMetric(input.asString(METRIC_PARAM, Metric.COSINE.name()));
    IndexMode mode = parseMode(input.asString(MODE_PARAM, IndexMode.EXACT.name()));
    int maxM = input.asInt(MAX_M_PARAM, 16);
    int efConstruction = input.asInt(EF_CONSTRUCTION_PARAM, 200);
    int efSearch = input.asInt(EF_SEARCH_PARAM, 50);
    long entriesPerIndexFile =
        VectorIndexProcedures.entriesPerIndexFile(input, ENTRIES_PER_INDEX_FILE_PARAM);

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
                "A vector index already exists for column " + column + " of table " + tableName);
          }

          String indexUuid = UUID.randomUUID().toString();
          long sourceSnapshotId = table.currentSnapshot().snapshotId();
          IndexMetadata created =
              catalog.createIndex(
                  CreateIndexRequest.builder()
                      .tableLocation(table.location())
                      .tableUuid(String.valueOf(table.uuid()))
                      .indexUuid(indexUuid)
                      .definition(
                          VectorIndexDefinition.builder()
                              .fieldId(fieldId)
                              .dimension(dimension)
                              .metric(metric)
                              .mode(mode)
                              .maxM(maxM)
                              .efConstruction(efConstruction)
                              .efSearch(efSearch)
                              .build())
                      .property(
                          IndexMetadata.ENTRIES_PER_INDEX_FILE_PROPERTY,
                          Long.toString(entriesPerIndexFile))
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

  private static Metric parseMetric(String metric) {
    try {
      return Metric.valueOf(metric.toUpperCase(java.util.Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Unknown metric: " + metric, e);
    }
  }

  private static IndexMode parseMode(String mode) {
    try {
      return IndexMode.valueOf(mode.toUpperCase(java.util.Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Unknown mode: " + mode, e);
    }
  }

  @Override
  public String description() {
    return "BuildVectorIndexProcedure";
  }
}
