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
package org.apache.iceberg.index.vector;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.apache.iceberg.TestTables.LocalFileIO;
import org.apache.iceberg.index.IndexBuildRequest;
import org.apache.iceberg.index.IndexBuilder;
import org.apache.iceberg.index.IndexEntry;
import org.apache.iceberg.index.IndexFile;
import org.apache.iceberg.index.IndexFile.Kind;
import org.apache.iceberg.index.IndexManifest;
import org.apache.iceberg.index.IndexManifestList;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.IndexMetadataParser;
import org.apache.iceberg.index.IndexQuery;
import org.apache.iceberg.index.IndexReader;
import org.apache.iceberg.index.IndexSearchResult;
import org.apache.iceberg.index.IndexSnapshot;
import org.apache.iceberg.index.vector.VectorIndexDefinition.VectorStructure;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Integration of the vector adapter with the generic core: builds indexes through the core builder,
 * searches them through the core reader, and round-trips metadata through the core parser, for both
 * exact and HNSW modes.
 */
class TestVectorIndexIntegration {

  @BeforeAll
  static void registerVectorIndexType() {
    VectorIndexTypes.register();
  }

  @Test
  public void buildsAndSearchesBothModes(@TempDir Path tempDir) throws IOException {
    LocalFileIO fileIO = new LocalFileIO();
    float[] query = {0.95f, 0.05f};

    IndexMetadata exact =
        build(fileIO, tempDir, "exact", VectorIndexDefinition.VectorStructure.FLAT);
    IndexReader exactReader = new IndexReader(exact, fileIO);
    VectorIndexAdapter exactAdapter = adapter(VectorIndexDefinition.VectorStructure.FLAT);
    List<IndexSearchResult> exactResults =
        exactReader.search(IndexQuery.of(10, exactAdapter.encodeQuery(query), 2));
    assertThat(exactResults)
        .extracting(IndexSearchResult::dataFile)
        .containsExactly("data-1.parquet", "data-3.parquet");
    assertThat(exactResults.get(0).score()).isGreaterThan(exactResults.get(1).score());
    assertThat(exactReader.uncoveredDataFiles(10, List.of("data-1.parquet", "data-9.parquet")))
        .containsExactly("data-9.parquet");

    IndexMetadata hnsw = build(fileIO, tempDir, "hnsw", VectorIndexDefinition.VectorStructure.HNSW);
    IndexReader hnswReader = new IndexReader(hnsw, fileIO);
    VectorIndexAdapter hnswAdapter = adapter(VectorIndexDefinition.VectorStructure.HNSW);
    List<IndexSearchResult> hnswResults =
        hnswReader.search(IndexQuery.of(10, hnswAdapter.encodeQuery(query), 2));
    assertThat(hnswResults)
        .extracting(IndexSearchResult::dataFile)
        .containsExactly("data-1.parquet", "data-3.parquet");
    assertThat(hnswResults.get(0).score()).isGreaterThan(hnswResults.get(1).score());

    // metadata round-trips through the core parser, including HNSW parameters
    IndexMetadata parsed = IndexMetadataParser.fromJson(IndexMetadataParser.toJson(hnsw));
    assertThat(parsed).isEqualTo(hnsw);
    VectorIndexDefinition definition = (VectorIndexDefinition) parsed.definition();
    assertThat(definition.structure()).isEqualTo(VectorIndexDefinition.VectorStructure.HNSW);
    assertThat(definition.maxM()).isEqualTo(8);

    // exact mode writes entry-bearing files; HNSW mode writes auxiliary files
    assertThat(indexFileOf(fileIO, exact).kind()).isEqualTo(Kind.ENTRY_BEARING);
    assertThat(indexFileOf(fileIO, hnsw).kind()).isEqualTo(Kind.AUXILIARY);
    assertThat(indexFileOf(fileIO, hnsw).blobType())
        .isEqualTo(VectorIndexAdapter.AUXILIARY_BLOB_TYPE);
  }

  private static IndexMetadata build(
      LocalFileIO fileIO, Path tempDir, String indexUuid, VectorStructure structure) {
    VectorIndexAdapter adapter = adapter(structure);
    IndexMetadata metadata =
        IndexMetadata.builder()
            .indexUuid(indexUuid)
            .tableUuid("table-uuid")
            .tableLocation(tempDir.resolve("table").toString())
            .location(tempDir.resolve("indexes/" + indexUuid).toString())
            .definition(
                VectorIndexDefinition.builder()
                    .fieldId(1)
                    .dimension(2)
                    .metric(VectorIndexDefinition.Metric.COSINE)
                    .structure(structure)
                    .maxM(8)
                    .efConstruction(100)
                    .efSearch(64)
                    .build())
            .build();

    return new IndexBuilder(fileIO)
        .build(
            IndexBuildRequest.builder()
                .currentMetadata(metadata)
                .sourceTableSnapshotId(10)
                .entries(
                    List.of(
                        IndexEntry.of(
                            "data-1.parquet", 0, adapter.encodeVector(new float[] {1.0f, 0.0f})),
                        IndexEntry.of(
                            "data-2.parquet", 1, adapter.encodeVector(new float[] {0.0f, 1.0f})),
                        IndexEntry.of(
                            "data-3.parquet", 2, adapter.encodeVector(new float[] {0.7f, 0.7f}))))
                .timestampMillis(30)
                .build());
  }

  private static VectorIndexAdapter adapter(VectorStructure structure) {
    return new VectorIndexAdapter(
        VectorIndexDefinition.builder()
            .fieldId(1)
            .dimension(2)
            .metric(VectorIndexDefinition.Metric.COSINE)
            .structure(structure)
            .maxM(8)
            .efConstruction(100)
            .efSearch(64)
            .build());
  }

  private static IndexFile indexFileOf(LocalFileIO fileIO, IndexMetadata metadata)
      throws IOException {
    IndexSnapshot snapshot = metadata.snapshots().get(0);
    IndexManifestList manifestList =
        IndexManifestList.read(fileIO.newInputFile(snapshot.indexManifestList()));
    IndexManifest manifest =
        IndexManifest.read(fileIO.newInputFile(manifestList.manifests().get(0).location()));
    return manifest.indexFiles().get(0);
  }
}
