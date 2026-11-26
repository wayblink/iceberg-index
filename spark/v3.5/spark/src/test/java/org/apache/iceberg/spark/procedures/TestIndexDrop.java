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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.iceberg.Table;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.junit.jupiter.api.Test;

class TestIndexDrop {
  @Test
  void purgesTheDirectoryThroughTheTablesFileIO() {
    SupportsPrefixOperations io = mock(SupportsPrefixOperations.class);
    Table table = mock(Table.class);
    when(table.location()).thenReturn("test-store://bucket/table");
    when(table.io()).thenReturn(io);

    IndexDrop.purgeTree(null, table, "index-uuid");

    verify(io).deletePrefix("test-store://bucket/table/indexes/index-uuid/");
  }
}
