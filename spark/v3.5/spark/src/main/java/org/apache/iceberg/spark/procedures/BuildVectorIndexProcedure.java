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

import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.index.CreateIndexRequest;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.IndexSnapshot;
import org.apache.iceberg.index.SidecarIndexCatalog;
import org.apache.iceberg.index.spark.IndexBuildStatus;
import org.apache.iceberg.index.spark.SparkIndexBuilder;
import org.apache.iceberg.index.vector.VectorIndexDefinition;
import org.apache.iceberg.index.vector.VectorIndexDefinition.Metric;
import org.apache.iceberg.index.vector.VectorIndexDefinition.VectorQuantizer;
import org.apache.iceberg.index.vector.VectorIndexDefinition.VectorStructure;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
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
 *   table => 'db.documents', column => 'embedding', dimension => 768, metric => 'l2');
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
  private static final ProcedureParameter STRUCTURE_PARAM =
      optionalInParameter("structure", DataTypes.StringType);
  private static final ProcedureParameter QUANTIZER_PARAM =
      optionalInParameter("quantizer", DataTypes.StringType);

  /** Names the structure before it was separated from the quantizer; still accepted. */
  private static final ProcedureParameter MODE_PARAM =
      optionalInParameter("mode", DataTypes.StringType);

  /**
   * Options of the structure being built, such as nlist for IVF.
   *
   * <p>A map rather than a parameter per option, because the options of one structure mean nothing
   * to another, and a procedure that named them all would have to name each new one as well.
   */
  private static final ProcedureParameter OPTIONS_PARAM =
      ProcedureParameter.optional("options", STRING_MAP);

  private static final ProcedureParameter MAX_M_PARAM =
      optionalInParameter("max_m", DataTypes.IntegerType);
  private static final ProcedureParameter EF_CONSTRUCTION_PARAM =
      optionalInParameter("ef_construction", DataTypes.IntegerType);
  private static final ProcedureParameter MAX_ENTRIES_PER_INDEX_FILE_PARAM =
      optionalInParameter("max_entries_per_index_file", DataTypes.LongType);
  private static final ProcedureParameter EF_SEARCH_PARAM =
      optionalInParameter("ef_search", DataTypes.IntegerType);
  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {
        TABLE_PARAM,
        COLUMN_PARAM,
        DIMENSION_PARAM,
        METRIC_PARAM,
        STRUCTURE_PARAM,
        QUANTIZER_PARAM,
        MODE_PARAM,
        OPTIONS_PARAM,
        MAX_M_PARAM,
        EF_CONSTRUCTION_PARAM,
        EF_SEARCH_PARAM,
        IndexProcedures.NAME_PARAM,
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
    Metric metric = parseMetric(input.asString(METRIC_PARAM, Metric.DEFAULT.name()));
    VectorStructure structure = parseStructure(input, MODE_PARAM);
    VectorQuantizer quantizer = parseQuantizer(input, QUANTIZER_PARAM);
    Map<String, String> options = input.asStringMap(OPTIONS_PARAM, java.util.Map.of());
    if (structure == VectorStructure.IVF && options.containsKey("nprobe")) {
      throw new IllegalArgumentException(
          "IVF nprobe is a query setting. Remove nprobe from build options and set "
              + "spark.iceberg.index.vector.ivf.nprobe before searching");
    }
    int maxM = input.asInt(MAX_M_PARAM, 16);
    int efConstruction = input.asInt(EF_CONSTRUCTION_PARAM, 200);
    int efSearch = input.asInt(EF_SEARCH_PARAM, 50);
    long maxEntriesPerIndexFile =
        VectorIndexProcedures.maxEntriesPerIndexFile(input, MAX_ENTRIES_PER_INDEX_FILE_PARAM);

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
          IndexProcedures.checkCanBuild(table, column, "vector", tableName, name);

          String indexUuid = UUID.randomUUID().toString();
          long sourceSnapshotId = table.currentSnapshot().snapshotId();
          IndexMetadata created =
              catalog.createIndex(
                  CreateIndexRequest.builder()
                      .tableLocation(table.location())
                      .tableUuid(String.valueOf(table.uuid()))
                      .indexUuid(indexUuid)
                      .name(name)
                      .definition(
                          vectorDefinition(
                              structure,
                              quantizer,
                              fieldId,
                              dimension,
                              metric,
                              maxM,
                              efConstruction,
                              efSearch,
                              options))
                      .property(
                          IndexMetadata.MAX_ENTRIES_PER_INDEX_FILE_PROPERTY,
                          Long.toString(maxEntriesPerIndexFile))
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

            try {
              // the whole tree, not only the files a published snapshot names: an attempt that
              // failed can have written files no metadata points at yet
              IndexDrop.purgeTree(spark(), table, indexUuid);
            } catch (RuntimeException rollbackFailure) {
              failure.addSuppressed(rollbackFailure);
            }

            try {
              // the tree is already gone; removing the registration must not try to read it again
              catalog.dropIndex(table.location(), indexUuid, false);
            } catch (RuntimeException rollbackFailure) {
              // the caller has to see the failure that ended the build; a rollback that failed is
              // attached to it rather than thrown in its place
              failure.addSuppressed(rollbackFailure);
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

  private static Metric parseMetric(String metric) {
    return Metric.fromName(metric);
  }

  /**
   * The definition of the index being built.
   *
   * <p>The structure comes from the {@code structure} parameter, or from the {@code mode} parameter
   * that named it before the two axes were separated: {@code EXACT} meant a flat scan and {@code
   * HNSW} meant a graph.
   *
   * <p>The graph's parameters are only written when the structure is a graph: a structure that
   * reads another one's options reports it, so writing them anyway would turn every IVF build into
   * a failure. The caller's option map is applied last, so it wins over the named parameters.
   */
  private static VectorIndexDefinition vectorDefinition(
      VectorStructure structure,
      VectorQuantizer quantizer,
      int fieldId,
      int dimension,
      org.apache.iceberg.index.vector.VectorIndexDefinition.Metric metric,
      int maxM,
      int efConstruction,
      int efSearch,
      Map<String, String> options) {
    VectorIndexDefinition.Builder builder =
        VectorIndexDefinition.builder()
            .fieldId(fieldId)
            .dimension(dimension)
            .metric(metric)
            .structure(structure)
            .quantizer(quantizer);
    if (structure == VectorStructure.HNSW) {
      builder.maxM(maxM).efConstruction(efConstruction).efSearch(efSearch);
    }
    return builder.properties(options).build();
  }

  private static VectorStructure parseStructure(ProcedureInput input, ProcedureParameter legacy) {
    String structure = input.asString(STRUCTURE_PARAM, null);
    String mode = input.asString(legacy, null);
    Preconditions.checkArgument(
        structure == null || mode == null,
        "Pass either %s or %s, not both",
        STRUCTURE_PARAM.name(),
        legacy.name());

    if (structure != null) {
      try {
        return VectorStructure.valueOf(structure.toUpperCase(java.util.Locale.ROOT));
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("Unsupported vector index structure: " + structure, e);
      }
    }

    if (mode != null) {
      switch (mode.toUpperCase(java.util.Locale.ROOT)) {
        case "EXACT":
          return VectorStructure.FLAT;
        case "HNSW":
          return VectorStructure.HNSW;
        default:
          throw new IllegalArgumentException("Unsupported vector index mode: " + mode);
      }
    }

    return VectorStructure.FLAT;
  }

  private static VectorQuantizer parseQuantizer(
      ProcedureInput input, ProcedureParameter parameter) {
    String quantizer = input.asString(parameter, null);
    if (quantizer == null) {
      return VectorQuantizer.NONE;
    }

    try {
      return VectorQuantizer.valueOf(quantizer.toUpperCase(java.util.Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Unsupported vector index quantizer: " + quantizer, e);
    }
  }

  @Override
  public String description() {
    return "BuildVectorIndexProcedure";
  }
}
