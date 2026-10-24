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
package org.apache.iceberg.spark.extensions;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Table;
import org.apache.iceberg.index.IndexBlob;
import org.apache.iceberg.index.IndexFile;
import org.apache.iceberg.index.IndexFileMemory;
import org.apache.iceberg.index.IndexManifest;
import org.apache.iceberg.index.IndexManifestList;
import org.apache.iceberg.index.IndexManifestList.ManifestRef;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.PuffinIndexFileContainer;
import org.apache.iceberg.index.SidecarIndexCatalog;
import org.apache.iceberg.index.vector.VectorIndexAdapter;
import org.apache.iceberg.index.vector.VectorIndexDefinition;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.spark.Spark3Util;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestIndexBuildMemory extends ExtensionsTestBase {
  @TestTemplate
  void flatBuildPublishesMemory() throws Exception {
    assertPublishedMemory("FLAT", "NONE", "vector/bruteforce/1", IndexFile.Kind.ENTRY_BEARING);
  }

  @TestTemplate
  void hnswBuildPublishesMemory() throws Exception {
    assertPublishedMemory("HNSW", "NONE", "vector/hnswlib/1", IndexFile.Kind.AUXILIARY);
  }

  @TestTemplate
  void ivfBuildPublishesMemory() throws Exception {
    assertPublishedMemory("IVF", "NONE", "vector/ivf/1", IndexFile.Kind.AUXILIARY);
  }

  @TestTemplate
  void quantizedIvfBuildPublishesMemory() throws Exception {
    assertPublishedMemory("IVF", "RABITQ", "vector/ivf/2", IndexFile.Kind.AUXILIARY);
  }

  @TestTemplate
  void legacyTextBuildKeepsUnknownEstimate() throws Exception {
    String name = tableName("build_memory_text_" + catalogName);
    String indexUuid = null;
    try {
      sql("CREATE TABLE %s (id bigint, body string) USING iceberg", name);
      sql("INSERT INTO %s VALUES (1, 'iceberg'), (2, 'index')", name);
      List<Object[]> built =
          sql(
              "CALL %s.system.build_text_index(table => '%s', column => 'body')",
              catalogName, name);
      indexUuid = built.get(0)[0].toString();
      Table table = Spark3Util.loadIcebergTable(spark, name);
      IndexMetadata metadata =
          new SidecarIndexCatalog(table.io(), table).loadIndex(table.location(), indexUuid);
      List<IndexFile> files = indexFiles(table, metadata);
      assertThat(files).isNotEmpty();
      assertThat(files).allSatisfy(file -> assertThat(file.memory()).isNull());
      assertThat(files.stream().mapToLong(IndexFile::entryCount).sum()).isEqualTo(2);
    } finally {
      try {
        if (indexUuid != null) {
          Table table = Spark3Util.loadIcebergTable(spark, name);
          new SidecarIndexCatalog(table.io(), table).dropIndex(table.location(), indexUuid, true);
        }
      } finally {
        sql("DROP TABLE IF EXISTS %s PURGE", name);
      }
    }
  }

  private void assertPublishedMemory(
      String structure, String quantizer, String readerId, IndexFile.Kind kind) throws Exception {
    String name = tableName("build_memory_" + structure + "_" + quantizer + "_" + catalogName);
    String indexUuid = null;
    try {
      sql("CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg", name);
      sql(
          "INSERT INTO %s SELECT id, array(CAST(id AS double), CAST(20 - id AS double)) "
              + "FROM range(0, 20)",
          name);
      List<Object[]> built =
          sql(
              "CALL %s.system.build_vector_index(table => '%s', column => 'embedding', "
                  + "dimension => 2, metric => 'euclidean', structure => '%s', quantizer => '%s')",
              catalogName, name, structure, quantizer);
      assertThat(built).hasSize(1);
      indexUuid = built.get(0)[0].toString();
      Table table = Spark3Util.loadIcebergTable(spark, name);
      IndexMetadata metadata =
          new SidecarIndexCatalog(table.io(), table).loadIndex(table.location(), indexUuid);
      VectorIndexAdapter adapter =
          new VectorIndexAdapter((VectorIndexDefinition) metadata.definition());
      List<IndexFile> files = indexFiles(table, metadata);

      Set<String> sourceFiles = new HashSet<>();
      try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
        for (FileScanTask task : tasks) {
          sourceFiles.add(task.file().location().toString());
        }
      }

      Set<String> covered = new HashSet<>();
      long entries = 0;
      assertThat(files).isNotEmpty();
      assertThat((Number) built.get(0)[2]).isEqualTo((long) files.size());
      for (IndexFile file : files) {
        assertThat(file.location()).isNotBlank();
        assertThat(file.kind()).isEqualTo(kind);
        assertThat(file.blobType())
            .isEqualTo(
                kind == IndexFile.Kind.AUXILIARY
                    ? VectorIndexAdapter.AUXILIARY_BLOB_TYPE
                    : VectorIndexAdapter.ENTRY_BLOB_TYPE);
        assertThat(file.coveredDataFiles()).isNotEmpty();
        covered.addAll(file.coveredDataFiles());
        entries += file.entryCount();
        IndexFileMemory memory = file.memory();
        assertThat(memory).isNotNull();
        assertThat(memory.fileBytes())
            .isEqualTo(table.io().newInputFile(file.location()).getLength());
        assertThat(memory.modelVersion()).isEqualTo(1);
        assertThat(memory.readerId()).isEqualTo(readerId);
        assertThat(memory.runtimeProfile())
            .contains("reference-bytes=4", "alignment=8", "compact-strings=true");
        assertThat(
                memory.structure().get(structure.equals("HNSW") ? "mapping-count" : "entry-count"))
            .isEqualTo(file.entryCount());
        IndexBlob blob =
            new PuffinIndexFileContainer().read(table.io().newInputFile(file.location())).get(0);
        assertThat(memory.payloadBytes()).isEqualTo(blob.payload().length);
        assertThat(memory.preparedBytes())
            .isEqualTo(adapter.kernel().prepare(ByteBuffer.wrap(blob.payload())).estimatedBytes());
      }

      assertThat(entries).isEqualTo(20);
      assertThat(covered).containsExactlyInAnyOrderElementsOf(sourceFiles);
    } finally {
      try {
        if (indexUuid != null) {
          sql(
              "CALL %s.system.drop_vector_index(table => '%s', column => 'embedding', purge => true)",
              catalogName, name);
        }
      } finally {
        sql("DROP TABLE IF EXISTS %s PURGE", name);
      }
    }
  }

  private static List<IndexFile> indexFiles(Table table, IndexMetadata metadata)
      throws IOException {
    IndexManifestList list =
        IndexManifestList.read(
            table.io().newInputFile(metadata.latestSnapshot().indexManifestList()));
    List<IndexFile> files = new ArrayList<>();
    for (ManifestRef manifest : list.manifests()) {
      files.addAll(IndexManifest.read(table.io().newInputFile(manifest.location())).indexFiles());
    }

    return files;
  }
}
