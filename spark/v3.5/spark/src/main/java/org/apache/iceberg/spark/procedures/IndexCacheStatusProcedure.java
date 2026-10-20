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
import org.apache.iceberg.index.spark.IndexCacheStatus;
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
 * What the index cache holds, one row per executor plus a driver row.
 *
 * <p>The counts live in the executor JVMs, so the driver knows them only as of each executor's last
 * task. That is what this returns by default, which costs nothing; {@code refresh => true} asks the
 * executors again, which costs one small job and is what a caller wants when nothing has run for a
 * while.
 */
class IndexCacheStatusProcedure extends BaseProcedure {

  private static final ProcedureParameter REFRESH_PARAM =
      optionalInParameter("refresh", DataTypes.BooleanType);

  private static final ProcedureParameter[] PARAMETERS = new ProcedureParameter[] {REFRESH_PARAM};

  private static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField("executor_id", DataTypes.StringType, false, Metadata.empty()),
            new StructField("host", DataTypes.StringType, false, Metadata.empty()),
            new StructField("epoch", DataTypes.LongType, false, Metadata.empty()),
            new StructField("mode", DataTypes.StringType, false, Metadata.empty()),
            new StructField("entries", DataTypes.LongType, false, Metadata.empty()),
            new StructField("bytes", DataTypes.LongType, false, Metadata.empty()),
            new StructField("hits", DataTypes.LongType, false, Metadata.empty()),
            new StructField("misses", DataTypes.LongType, false, Metadata.empty()),
            new StructField("loads", DataTypes.LongType, false, Metadata.empty()),
            new StructField("evictions", DataTypes.LongType, false, Metadata.empty()),
            new StructField("coverage", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("working_set_bytes", DataTypes.LongType, false, Metadata.empty()),
            new StructField("reported_at", DataTypes.TimestampType, false, Metadata.empty())
          });

  public static ProcedureBuilder builder() {
    return new BaseProcedure.Builder<IndexCacheStatusProcedure>() {
      @Override
      public IndexCacheStatusProcedure doBuild() {
        return new IndexCacheStatusProcedure(tableCatalog());
      }
    };
  }

  private IndexCacheStatusProcedure(TableCatalog catalog) {
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
    ProcedureInput input = new ProcedureInput(spark(), tableCatalog(), PARAMETERS, args);
    boolean refresh = input.asBoolean(REFRESH_PARAM, false);

    List<IndexCacheStatus.Row> rows =
        refresh ? IndexCacheStatus.refresh(spark()) : IndexCacheStatus.snapshot(spark());

    List<InternalRow> result = Lists.newArrayListWithCapacity(rows.size());
    for (IndexCacheStatus.Row row : rows) {
      result.add(
          newInternalRow(
              UTF8String.fromString(row.executorId()),
              UTF8String.fromString(row.host()),
              row.epoch(),
              UTF8String.fromString(row.mode()),
              row.entries(),
              row.bytes(),
              row.hits(),
              row.misses(),
              row.loads(),
              row.evictions(),
              row.coverage(),
              row.workingSetBytes(),
              row.reportedAtMillis() * 1000L));
    }

    return result.toArray(new InternalRow[0]);
  }

  @Override
  public String description() {
    return "index_cache_status";
  }
}
