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
package org.apache.iceberg.spark.index;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.Table;
import org.apache.iceberg.index.IndexBlob;
import org.apache.iceberg.index.IndexEntry;
import org.apache.iceberg.index.IndexFile;
import org.apache.iceberg.index.IndexFile.Kind;
import org.apache.iceberg.index.IndexFileContainer;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.IndexSnapshot;
import org.apache.iceberg.index.IndexSnapshotWriter;
import org.apache.iceberg.index.IndexTypes;
import org.apache.iceberg.index.PuffinIndexFileContainer;
import org.apache.iceberg.index.VectorIndexAdapter;
import org.apache.iceberg.index.VectorIndexDefinition;
import org.apache.iceberg.index.VectorIndexDefinition.Metric;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

/**
 * Builds a vector index snapshot for an Iceberg table snapshot with Spark.
 *
 * <p>Entries are read by {@link VectorIndexEntries}, shuffled by data file so that all rows of a
 * data file land in one partition, and written as one index file per data file per partition. That
 * keeps source-file coverage precise: a refresh only has to drop the index files of data files that
 * were removed or rewritten.
 *
 * <p>Like the core builder, this only writes files and returns updated metadata; publishing it
 * through the {@code IndexCatalog} is a separate step.
 */
public final class SparkIndexBuilder {

  private static final String DATA_DIR = "data";

  private SparkIndexBuilder() {}

  /**
   * Builds a full index snapshot for the given source table snapshot.
   *
   * @param spark the Spark session
   * @param table the Iceberg table to index
   * @param currentMetadata the index metadata to extend
   * @param sourceTableSnapshotId the source table snapshot to build from
   * @param vectorColumn the name of the numeric array column to index
   * @param timestampMillis the publication timestamp of the new snapshot
   * @return updated metadata with the new snapshot appended, not yet published
   */
  public static IndexMetadata build(
      SparkSession spark,
      Table table,
      IndexMetadata currentMetadata,
      long sourceTableSnapshotId,
      String vectorColumn,
      long timestampMillis) {
    VectorIndexDefinition definition = (VectorIndexDefinition) currentMetadata.definition();
    String indexLocation = currentMetadata.location();
    long snapshotId = nextSnapshotId(currentMetadata);
    int fieldId = definition.fieldId();
    int dimension = definition.dimension();
    Metric metric = definition.metric();
    FileIO fileIO = table.io();
    String entryBlobType =
        IndexTypes.fromType(definition.type()).createAdapter(definition).entryBlobType();

    Dataset<Row> entries =
        VectorIndexEntries.entries(spark, table, sourceTableSnapshotId, vectorColumn, definition);

    List<Row> indexFileRows =
        entries
            .repartition(functions.col(VectorIndexEntries.DATA_FILE_COLUMN))
            .toJavaRDD()
            .mapPartitions(
                (FlatMapFunction<Iterator<Row>, Row>)
                    iterator -> {
                      IndexFileContainer fileContainer = new PuffinIndexFileContainer();
                      VectorIndexAdapter adapter =
                          new VectorIndexAdapter(
                              VectorIndexDefinition.builder()
                                  .fieldId(fieldId)
                                  .dimension(dimension)
                                  .metric(metric)
                                  .build());
                      Map<String, List<IndexEntry>> entriesByFile = new LinkedHashMap<>();
                      while (iterator.hasNext()) {
                        Row row = iterator.next();
                        entriesByFile
                            .computeIfAbsent(row.getString(0), key -> Lists.newArrayList())
                            .add(IndexEntry.of(row.getString(0), row.getLong(1), row.getAs(2)));
                      }

                      List<Row> files = Lists.newArrayList();
                      int seq = 0;
                      for (Map.Entry<String, List<IndexEntry>> group : entriesByFile.entrySet()) {
                        String location =
                            indexLocation
                                + "/"
                                + DATA_DIR
                                + "/index-file-"
                                + snapshotId
                                + "-"
                                + seq
                                + "-"
                                + UUID.randomUUID();
                        fileContainer.write(
                            fileIO.newOutputFile(location),
                            List.of(
                                IndexBlob.of(
                                    entryBlobType,
                                    adapter.encodeEntries(group.getValue()).array())));
                        files.add(
                            RowFactory.create(
                                location, (long) group.getValue().size(), List.of(group.getKey())));
                        seq++;
                      }
                      return files.iterator();
                    })
            .collect();

    List<IndexFile> indexFiles = Lists.newArrayListWithCapacity(indexFileRows.size());
    for (Row row : indexFileRows) {
      indexFiles.add(
          IndexFile.builder()
              .location(row.getString(0))
              .kind(Kind.ENTRY_BEARING)
              .blobType(entryBlobType)
              .entryCount(row.getLong(1))
              .coveredDataFiles(coveredDataFiles(row))
              .build());
    }

    String manifestList =
        new IndexSnapshotWriter(fileIO).writeManifestList(indexLocation, snapshotId, indexFiles);

    IndexSnapshot snapshot =
        IndexSnapshot.builder()
            .snapshotId(snapshotId)
            .sourceTableSnapshotId(sourceTableSnapshotId)
            .parentIndexSnapshotId(null)
            .timestampMillis(timestampMillis)
            .indexManifestList(manifestList)
            .build();
    return currentMetadata.withSnapshot(snapshot);
  }

  @SuppressWarnings("unchecked")
  private static List<String> coveredDataFiles(Row row) {
    return (List<String>) row.getAs(2);
  }

  private static long nextSnapshotId(IndexMetadata metadata) {
    IndexSnapshot latest = metadata.latestSnapshot();
    return latest == null ? 1L : latest.snapshotId() + 1;
  }
}
