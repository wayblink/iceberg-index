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
package org.apache.iceberg.index.spark;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.index.IndexType;
import org.apache.iceberg.index.IndexTypes;
import org.junit.jupiter.api.Test;

/**
 * An executor deserializes the index definition a command carries, so it needs the same types the
 * driver registered. Without them every load fails with "Unknown index type: vector", which is what
 * the first cluster run reported.
 */
public class TestIndexTypeRegistration {
  @Test
  void everyIndexTypeACacheCommandCanCarryIsRegistered() {
    IndexCacheTypeRegistration.registerAll();

    assertThat(IndexTypes.fromType("vector")).isNotNull();
    assertThat(IndexTypes.fromType("text")).isInstanceOf(IndexType.class);
  }

  @Test
  void registeringTwiceKeepsOneEntryPerType() {
    IndexCacheTypeRegistration.registerAll();
    int afterFirst = IndexTypes.registered().size();
    IndexCacheTypeRegistration.registerAll();

    assertThat(IndexTypes.registered()).hasSize(afterFirst);
    assertThat(IndexTypes.fromType("vector")).isNotNull();
  }
}
