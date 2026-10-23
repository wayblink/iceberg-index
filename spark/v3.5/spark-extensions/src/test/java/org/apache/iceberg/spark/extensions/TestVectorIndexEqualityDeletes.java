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

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.io.OutputFile;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.TestTemplate;

/**
 * Equality deletes and the index.
 *
 * <p>An equality delete removes a row by value, so the index entry that points at its position is
 * still there when the delete is committed; the row has to be filtered out while it is read back.
 * That filter compares the key columns of the data row with the keys of the delete file, and both
 * sides have to encode a value the same way for the comparison to work. The index path had no test
 * for this, and Spark's own DELETE writes position deletes, so an equality delete never reached it.
 */
public class TestVectorIndexEqualityDeletes extends ExtensionsTestBase {

  @TestTemplate
  public void aStringKeyDeletedByValueDoesNotComeBackFromTheIndex() throws Exception {
    String table = tableName("eq_delete_name_" + catalogName);
    createAndIndex(table);
    assertDeletedRowIsGone(table, "name", "b");
  }

  @TestTemplate
  public void aBinaryKeyDeletedByValueDoesNotComeBackFromTheIndex() throws Exception {
    // the data side and the delete side hand a binary value back as different objects (a ByteBuffer
    // and a byte array), so the key has to be built from the bytes, not from the value itself
    String table = tableName("eq_delete_bin_" + catalogName);
    sql(
        "CREATE TABLE %s (id bigint, payload binary, embedding array<double>) "
            + "USING iceberg TBLPROPERTIES ('format-version'='2')",
        table);
    sql(
        "INSERT INTO %s VALUES "
            + "(1, X'01', array(1.0d, 0.0d)), "
            + "(2, X'02', array(0.0d, 1.0d)), "
            + "(3, X'03', array(1.0d, 1.0d))",
        table);
    sql(
        "CALL %s.system.build_vector_index(table => '%s', column => 'embedding', dimension => 2, "
            + "metric => 'euclidean')",
        catalogName, table);

    assertDeletedRowIsGone(table, "payload", java.nio.ByteBuffer.wrap(new byte[] {2}));
  }

  @TestTemplate
  public void aTimestampKeyDeletedByValueDoesNotComeBackFromTheIndex() throws Exception {
    // the key is the value the row actually holds: a timestamp literal is read in the session's
    // time zone, so restating it here would delete an instant eight hours away from the row
    String table = tableName("eq_delete_ts_" + catalogName);
    createAndIndex(table);
    java.sql.Timestamp stored =
        (java.sql.Timestamp) sql("SELECT ts FROM %s WHERE id = 2", table).get(0)[0];

    assertDeletedRowIsGone(table, "ts", stored.toInstant().atOffset(java.time.ZoneOffset.UTC));
  }

  private void createAndIndex(String tableName) {
    sql(
        "CREATE TABLE %s (id bigint, name string, ts timestamp, embedding array<double>) "
            + "USING iceberg TBLPROPERTIES ('format-version'='2')",
        tableName);
    sql(
        "INSERT INTO %s VALUES "
            + "(1, 'a', TIMESTAMP '2024-01-01 00:00:01', array(1.0d, 0.0d)), "
            + "(2, 'b', TIMESTAMP '2024-01-01 00:00:02', array(0.0d, 1.0d)), "
            + "(3, 'c', TIMESTAMP '2024-01-01 00:00:03', array(1.0d, 1.0d)), "
            + "(4, 'd', TIMESTAMP '2024-01-01 00:00:04', array(2.0d, 2.0d))",
        tableName);
    sql(
        "CALL %s.system.build_vector_index(table => '%s', column => 'embedding', dimension => 2, "
            + "metric => 'euclidean')",
        catalogName, tableName);
  }

  @TestTemplate
  public void deletesOnDifferentKeysInOneSnapshotAreAllApplied() throws Exception {
    String table = tableName("eq_delete_two_key_sets_" + catalogName);
    createAndIndex(table);

    Table icebergTable = org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark, table);
    java.sql.Timestamp stored =
        (java.sql.Timestamp) sql("SELECT ts FROM %s WHERE id = 3", table).get(0)[0];
    DeleteFile byName = deleteFile(icebergTable, "name", "b");
    DeleteFile byTs =
        deleteFile(icebergTable, "ts", stored.toInstant().atOffset(java.time.ZoneOffset.UTC));

    // both key sets in one commit: a snapshot can hold equality deletes that key on different
    // columns, and each removes its own rows
    icebergTable.newRowDelta().addDeletes(byName).addDeletes(byTs).commit();
    sql("REFRESH TABLE %s", table);

    List<Long> indexed = indexedIds(table);
    List<Long> scanned = scannedIds(table);
    assertThat(indexed).doesNotContain(2L, 3L);
    assertThat(indexed.stream().sorted().collect(Collectors.toList()))
        .isEqualTo(scanned.stream().sorted().collect(Collectors.toList()));
  }

  @TestTemplate
  public void aDecimalKeyRemovesItsRow() throws Exception {
    String table = tableName("eq_delete_decimal_" + catalogName);
    sql(
        "CREATE TABLE %s (id bigint, price decimal(9, 2), embedding array<double>) USING iceberg "
            + "TBLPROPERTIES ('format-version'='2')",
        table);
    sql(
        "INSERT INTO %s VALUES "
            + "(1, CAST(1.00 AS decimal(9,2)), array(1.0d, 0.0d)), "
            + "(2, CAST(2.50 AS decimal(9,2)), array(0.0d, 1.0d)), "
            + "(3, CAST(3.75 AS decimal(9,2)), array(1.0d, 1.0d))",
        table);
    sql(
        "CALL %s.system.build_vector_index(table => '%s', column => 'embedding', dimension => 2, "
            + "metric => 'euclidean')",
        catalogName, table);

    // a decimal key is carried as the canonical decimal rather than its text, so a delete does not
    // depend on both sides reading the same scale (TestEqualityKeyEncoding pins 2.50 == 2.5)
    Table icebergTable = org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark, table);
    DeleteFile deleted = deleteFile(icebergTable, "price", new java.math.BigDecimal("2.50"));
    icebergTable.newRowDelta().addDeletes(deleted).commit();
    sql("REFRESH TABLE %s", table);

    List<Long> indexed = indexedIds(table);
    List<Long> scanned = scannedIds(table);
    assertThat(indexed).doesNotContain(2L);
    assertThat(indexed.stream().sorted().collect(Collectors.toList()))
        .isEqualTo(scanned.stream().sorted().collect(Collectors.toList()));
  }

  @TestTemplate
  public void aDeleteWithMetricsIsStillApplied() throws Exception {
    String table = tableName("eq_delete_metrics_" + catalogName);
    createAndIndex(table);

    Table icebergTable = org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark, table);
    // the delete file keeps the metrics its writer produced: a delete that carries bounds is
    // eligible for pruning against the data files, and that must not stop it from matching
    DeleteFile withMetrics = deleteFile(icebergTable, "name", "b");
    icebergTable.newRowDelta().addDeletes(withMetrics).commit();
    sql("REFRESH TABLE %s", table);

    assertThat(indexedIds(table)).doesNotContain(2L);
  }

  private DeleteFile deleteFile(Table table, String keyColumn, Object keyValue) throws Exception {
    Schema deleteSchema = table.schema().select(keyColumn);
    GenericRecord template = GenericRecord.create(deleteSchema);
    List<org.apache.iceberg.data.Record> records =
        java.util.Collections.singletonList(template.copy(keyColumn, keyValue));
    OutputFile out =
        Files.localOutput(File.createTempFile("equality-delete", ".parquet", temp.toFile()));
    return FileHelpers.writeDeleteFile(table, out, records, deleteSchema);
  }

  private void assertDeletedRowIsGone(String tableName, String keyColumn, Object keyValue)
      throws Exception {
    Table table = org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark, tableName);
    addEqualityDelete(table, keyColumn, keyValue);

    List<Long> indexed = indexedIds(tableName);
    List<Long> scanned = scannedIds(tableName);

    assertThat(indexed)
        .as("the row deleted by value is gone from the index path")
        .doesNotContain(2L);
    assertThat(scanned).doesNotContain(2L);
    // the order of equal candidates is not what this test is about: it is about which rows are
    // still there
    assertThat(indexed.stream().sorted().collect(Collectors.toList()))
        .as("the index answers what a full scan answers after an equality delete")
        .isEqualTo(scanned.stream().sorted().collect(Collectors.toList()));
  }

  /** Writes an equality delete file on one key column and commits it through a row delta. */
  private void addEqualityDelete(Table table, String keyColumn, Object keyValue)
      throws java.io.IOException {
    // the key value is given as the Java type the table's schema expects (a string, a
    // LocalDateTime), not as a Spark value: the delete file is written by the Iceberg writer
    Schema deleteSchema = table.schema().select(keyColumn);
    GenericRecord template = GenericRecord.create(deleteSchema);
    List<org.apache.iceberg.data.Record> records =
        java.util.Collections.singletonList(template.copy(keyColumn, keyValue));

    OutputFile out =
        Files.localOutput(File.createTempFile("equality-delete", ".parquet", temp.toFile()));
    DeleteFile deleteFile;
    try {
      deleteFile = FileHelpers.writeDeleteFile(table, out, records, deleteSchema);
    } catch (java.io.IOException e) {
      throw new java.io.UncheckedIOException(e);
    }

    // the delete file is committed without metrics: a delete that carries bounds is pruned
    // against the data files' bounds, and this test is about the value comparison rather than
    // about which files the delete is attached to
    int[] equalityFields =
        deleteSchema.columns().stream().mapToInt(field -> field.fieldId()).toArray();
    DeleteFile withoutMetrics =
        org.apache.iceberg.FileMetadata.deleteFileBuilder(table.spec())
            .ofEqualityDeletes(equalityFields)
            .withPath(deleteFile.location())
            .withFileSizeInBytes(deleteFile.fileSizeInBytes())
            .withRecordCount(deleteFile.recordCount())
            .build();

    table.newRowDelta().addDeletes(withoutMetrics).commit();
    sql("REFRESH TABLE %s", tableName(table));
  }

  private String tableName(Table table) {
    return table.name();
  }

  private List<Long> indexedIds(String tableName) {
    return sql(
            "CALL %s.system.vector_search(table => '%s', column => 'embedding', "
                + "query => array(0.0d, 1.0d), k => 10)",
            catalogName, tableName)
        .stream()
        // a hit whose row was removed comes back without a row, which is what a deleted position
        // looks like on the index path
        .filter(row -> row[3] != null)
        .map(row -> Long.parseLong(((String) row[3]).replaceAll(".*\"id\":(\\d+).*", "$1")))
        .collect(Collectors.toList());
  }

  private List<Long> scannedIds(String tableName) {
    List<Row> rows =
        spark
            .sql(
                String.format(
                    "SELECT id FROM %s ORDER BY vec_distance(embedding, array(0.0d, 1.0d)) + 0.0d "
                        + "LIMIT 10",
                    tableName))
            .collectAsList();
    return rows.stream().map(row -> row.getLong(0)).collect(Collectors.toList());
  }
}
