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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

import java.util.List;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.index.vector.VectorIndexTypes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestVectorIndexProcedures extends ExtensionsTestBase {

  @BeforeAll
  static void registerVectorIndexType() {
    VectorIndexTypes.register();
  }

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName("vectors_build_list_" + catalogName));
    sql("DROP TABLE IF EXISTS %s", tableName("vectors_hnsw_" + catalogName));
    sql("DROP TABLE IF EXISTS %s", tableName("vectors_refresh_" + catalogName));
    sql("DROP TABLE IF EXISTS %s", tableName("vectors_exists_" + catalogName));
    sql("DROP TABLE IF EXISTS %s", tableName("vectors_missing_" + catalogName));
    sql("DROP TABLE IF EXISTS %s", tableName("vectors_dimension_" + catalogName));
    sql("DROP TABLE IF EXISTS %s", tableName("vectors_structure_" + catalogName));
  }

  @TestTemplate
  public void testBuildListAndSearchVectorIndex() {
    String tableName = tableName("vectors_build_list_" + catalogName);
    sql(
        "CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg PARTITIONED BY (bucket(4, id))",
        tableName);
    sql(
        "INSERT INTO %s VALUES (1, array(1.0, 0.0)), (2, array(0.9, 0.1)), (3, array(0.0, 1.0)), "
            + "(4, array(0.1, 0.95)), (5, array(-1.0, 0.0)), (6, array(0.0, -1.0))",
        tableName);

    List<Object[]> buildOutput =
        sql(
            "CALL %s.system.build_vector_index(table => '%s', column => 'embedding', dimension => 2, metric => 'cosine')",
            catalogName, tableName);
    assertThat(buildOutput).hasSize(1);
    String indexUuid = buildOutput.get(0)[0].toString();
    long sourceSnapshotId = ((Number) buildOutput.get(0)[1]).longValue();
    long indexFiles = ((Number) buildOutput.get(0)[2]).longValue();
    long entries = ((Number) buildOutput.get(0)[3]).longValue();
    assertThat(indexUuid).isNotBlank();
    assertThat(sourceSnapshotId).isPositive();
    assertThat(indexFiles).isPositive();
    assertThat(entries).isEqualTo(6L);

    List<Object[]> listOutput =
        sql("CALL %s.system.list_indexes(table => '%s')", catalogName, tableName);
    assertThat(listOutput).hasSize(1);
    assertThat(listOutput.get(0)[0]).isEqualTo(indexUuid);
    assertThat(listOutput.get(0)[1]).isEqualTo("embedding");
    assertThat(listOutput.get(0)[2]).isEqualTo("vector");
    assertThat(listOutput.get(0)[3]).isEqualTo(sourceSnapshotId);
    assertThat(listOutput.get(0)[4]).isEqualTo("PUBLISHED");
    assertThat(((Number) listOutput.get(0)[5]).longValue()).isPositive();
    assertThat(((Number) listOutput.get(0)[6]).longValue()).isEqualTo(6L);
    // type-specific parameters live in the definition JSON
    assertThat(listOutput.get(0)[7].toString())
        .contains("\"structure\":\"FLAT\"")
        .contains("\"quantizer\":\"NONE\"");
    assertThat(listOutput.get(0)[7].toString()).contains("\"metric\":\"COSINE\"");
    assertThat(listOutput.get(0)[7].toString()).contains("\"dimension\":2");

    // a query close to (1.0, 0.0) must return id=1 first, with the resolved row attached
    List<Object[]> searchOutput =
        sql(
            "CALL %s.system.vector_search(table => '%s', column => 'embedding', query => array(0.95d, 0.05d), k => 2)",
            catalogName, tableName);
    assertThat(searchOutput).hasSize(2);
    assertThat(searchOutput.get(0)[0]).isInstanceOf(String.class);
    assertThat(searchOutput.get(0)[1]).isInstanceOf(Number.class);
    double firstDistance = ((Number) searchOutput.get(0)[2]).doubleValue();
    double secondDistance = ((Number) searchOutput.get(1)[2]).doubleValue();
    // the score of the default coverage mode is the distance the query asks for, so the nearer row
    // has the smaller one; this used to be the index's own (similarity) score
    assertThat(firstDistance).isLessThan(secondDistance);
    assertThat(searchOutput.get(0)[3].toString()).contains("\"id\":1");

    // and it is the very number the query itself computes: the same two rows, ordered by
    // vec_distance, carry the distances the procedure reported
    List<Object[]> byDistance =
        sql(
            "SELECT vec_distance(embedding, array(0.95d, 0.05d)) AS distance FROM %s "
                + "ORDER BY distance ASC LIMIT 2",
            tableName);
    assertThat(byDistance).hasSize(2);
    assertThat(((Number) byDistance.get(0)[0]).doubleValue())
        .isCloseTo(firstDistance, within(1e-12));
    assertThat(((Number) byDistance.get(1)[0]).doubleValue())
        .isCloseTo(secondDistance, within(1e-12));

    // k = 0 returns no rows
    assertThat(
            sql(
                "CALL %s.system.vector_search(table => '%s', column => 'embedding', query => array(0.95d, 0.05d), k => 0)",
                catalogName, tableName))
        .isEmpty();
  }

  @TestTemplate
  public void testStructureAndQuantizerParameters() {
    String tableName = tableName("vectors_structure_" + catalogName);
    sql("CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, array(1.0, 0.0)), (2, array(0.0, 1.0))", tableName);

    // structure says the same thing the mode parameter used to, and quantizer defaults to none
    List<Object[]> built =
        sql(
            "CALL %s.system.build_vector_index(table => '%s', column => 'embedding', dimension => 2, structure => 'HNSW', ef_search => 64)",
            catalogName, tableName);
    assertThat(built).hasSize(1);

    List<Object[]> listed =
        sql("CALL %s.system.list_indexes(table => '%s')", catalogName, tableName);
    assertThat(listed.get(0)[7].toString()).contains("\"structure\":\"HNSW\"");
    assertThat(listed.get(0)[7].toString()).contains("\"quantizer\":\"NONE\"");
  }

  @TestTemplate
  public void testBuildsAnIvfIndex() {
    String tableName = tableName("vectors_structure_" + catalogName);
    sql("CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg", tableName);
    sql(
        "INSERT INTO %s VALUES (1, array(1.0, 0.0)), (2, array(0.9, 0.1)), (3, array(0.0, 1.0)), "
            + "(4, array(0.1, 0.95)), (5, array(-1.0, 0.0)), (6, array(0.0, -1.0))",
        tableName);

    // IVF partitions the vectors of each index file around centroids of its own, so no training
    // over the table is involved and nothing is redistributed
    List<Object[]> built =
        sql(
            "CALL %s.system.build_vector_index(table => '%s', column => 'embedding', dimension => 2, structure => 'IVF', options => map('nprobe', '4'))",
            catalogName, tableName);
    assertThat(built).hasSize(1);

    List<Object[]> listed =
        sql("CALL %s.system.list_indexes(table => '%s')", catalogName, tableName);
    assertThat(listed.get(0)[7].toString()).contains("\"structure\":\"IVF\"");

    // the search path itself is covered by the kernel's own tests, which can compare against an
    // exact scan; this one is about the procedure accepting a structure the kernel registry offers
    assertThat(listed).hasSize(1);
  }

  @TestTemplate
  public void testRefusesAQuantizerNothingAppliesYet() {
    String tableName = tableName("vectors_structure_" + catalogName);
    sql("CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, array(1.0, 0.0)), (2, array(0.0, 1.0))", tableName);

    // RaBitQ is a quantizer a definition may name, but no kernel applies it, so the build says so
    // rather than leaving behind an index nothing can search
    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.build_vector_index(table => '%s', column => 'embedding', dimension => 2, quantizer => 'RABITQ')",
                    catalogName, tableName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("No vector kernel builds a FLAT index with the RABITQ quantizer")
        .hasMessageContaining("bruteforce");
  }

  @TestTemplate
  public void testAFailedBuildLeavesNoIndexBehind() {
    String tableName = tableName("vectors_failed_build_" + catalogName);
    sql("CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg", tableName);
    // a zero vector has no direction, so the build refuses it part way through, after the index was
    // registered
    sql("INSERT INTO %s VALUES (1, array(1.0d, 0.0d)), (2, array(0.0d, 0.0d))", tableName);

    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.build_vector_index(table => '%s', column => 'embedding', "
                        + "dimension => 2, metric => 'cosine', structure => 'IVF')",
                    catalogName, tableName))
        .hasStackTraceContaining("zero vector");

    // nothing that looks like an index is left behind, so the column is not stuck on a build that
    // never produced anything
    assertThat(sql("CALL %s.system.list_indexes(table => '%s')", catalogName, tableName)).isEmpty();

    // and the same call, with parameters that can be built, still works
    sql(
        "CALL %s.system.build_vector_index(table => '%s', column => 'embedding', "
            + "dimension => 2, metric => 'euclidean', structure => 'IVF')",
        catalogName, tableName);
    assertThat(sql("CALL %s.system.list_indexes(table => '%s')", catalogName, tableName))
        .hasSize(1);
  }

  @TestTemplate
  public void testRefusesBothModeAndStructure() {
    String tableName = tableName("vectors_structure_" + catalogName);
    sql("CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, array(1.0, 0.0)), (2, array(0.0, 1.0))", tableName);

    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.build_vector_index(table => '%s', column => 'embedding', dimension => 2, mode => 'EXACT', structure => 'HNSW')",
                    catalogName, tableName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("not both");
  }

  @TestTemplate
  public void testBuildVectorIndexInHnswMode() {
    String tableName = tableName("vectors_hnsw_" + catalogName);
    sql("CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, array(1.0, 0.0)), (2, array(0.0, 1.0))", tableName);

    sql(
        "CALL %s.system.build_vector_index(table => '%s', column => 'embedding', dimension => 2, metric => 'cosine', mode => 'HNSW', max_m => 8, ef_construction => 100, ef_search => 64)",
        catalogName, tableName);

    List<Object[]> listOutput =
        sql("CALL %s.system.list_indexes(table => '%s')", catalogName, tableName);
    assertThat(listOutput).hasSize(1);
    assertThat(listOutput.get(0)[7].toString()).contains("\"structure\":\"HNSW\"");

    List<Object[]> searchOutput =
        sql(
            "CALL %s.system.vector_search(table => '%s', column => 'embedding', query => array(0.95d, 0.05d), k => 1)",
            catalogName, tableName);
    assertThat(searchOutput).hasSize(1);
    assertThat(searchOutput.get(0)[3].toString()).contains("\"id\":1");
  }

  @TestTemplate
  public void testRefreshVectorIndexPicksUpNewData() {
    String tableName = tableName("vectors_refresh_" + catalogName);
    sql("CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, array(1.0, 0.0))", tableName);

    sql(
        "CALL %s.system.build_vector_index(table => '%s', column => 'embedding', dimension => 2)",
        catalogName, tableName);

    // refreshing without changes reports zero files and entries
    List<Object[]> noopOutput =
        sql(
            "CALL %s.system.refresh_vector_index(table => '%s', column => 'embedding')",
            catalogName, tableName);
    assertThat(noopOutput).hasSize(1);
    assertThat(((Number) noopOutput.get(0)[2]).longValue()).isZero();
    assertThat(((Number) noopOutput.get(0)[3]).longValue()).isZero();

    sql("INSERT INTO %s VALUES (2, array(0.0, 1.0))", tableName);

    List<Object[]> refreshOutput =
        sql(
            "CALL %s.system.refresh_vector_index(table => '%s', column => 'embedding')",
            catalogName, tableName);
    assertThat(refreshOutput).hasSize(1);
    assertThat(((Number) refreshOutput.get(0)[2]).longValue()).isEqualTo(2L);
    assertThat(((Number) refreshOutput.get(0)[3]).longValue()).isEqualTo(2L);

    // the refreshed index finds the new row
    List<Object[]> searchOutput =
        sql(
            "CALL %s.system.vector_search(table => '%s', column => 'embedding', query => array(0.0d, 0.95d), k => 1)",
            catalogName, tableName);
    assertThat(searchOutput).hasSize(1);
    assertThat(searchOutput.get(0)[3].toString()).contains("\"id\":2");
  }

  @TestTemplate
  public void testBuildVectorIndexFailsWhenIndexExists() {
    String tableName = tableName("vectors_exists_" + catalogName);
    sql("CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, array(1.0, 0.0))", tableName);

    sql(
        "CALL %s.system.build_vector_index(table => '%s', column => 'embedding', dimension => 2)",
        catalogName, tableName);

    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.build_vector_index(table => '%s', column => 'embedding', dimension => 2)",
                    catalogName, tableName))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("already exists");
  }

  @TestTemplate
  public void testVectorSearchWithoutIndexFails() {
    String tableName = tableName("vectors_missing_" + catalogName);
    sql("CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, array(1.0, 0.0))", tableName);

    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.vector_search(table => '%s', column => 'embedding', query => array(0.95d, 0.05d), k => 1)",
                    catalogName, tableName))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("No vector index exists");
  }

  @TestTemplate
  public void testVectorSearchWithWrongDimensionFails() {
    String tableName = tableName("vectors_dimension_" + catalogName);
    sql("CREATE TABLE %s (id bigint, embedding array<double>) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, array(1.0, 0.0))", tableName);

    sql(
        "CALL %s.system.build_vector_index(table => '%s', column => 'embedding', dimension => 2)",
        catalogName, tableName);

    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.vector_search(table => '%s', column => 'embedding', query => array(0.95d, 0.05d, 0.5d), k => 1)",
                    catalogName, tableName))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("does not match index dimension");
  }
}
