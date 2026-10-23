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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** Pins the statement check_indexes suggests, which is the only thing a mismatch leaves to do. */
class TestCheckIndexesSuggestion {

  @Test
  public void theStatementNamesTheCatalogTheCallerNamed() {
    // the suggestion is advice to paste back into the same session, so it names the catalog the
    // caller called rather than reading one out of the table name
    assertThat(
            CheckIndexesProcedure.refreshSuggestionFor(
                "spark_catalog", "spark_catalog.default.t", "embedding"))
        .isEqualTo(
            "run CALL spark_catalog.system.refresh_vector_index(table => "
                + "'spark_catalog.default.t', column => 'embedding')");
  }

  @Test
  public void aTableNamedWithoutItsCatalogGetsItBack() {
    assertThat(CheckIndexesProcedure.refreshSuggestionFor("prod", "db.t", "embedding"))
        .isEqualTo(
            "run CALL prod.system.refresh_vector_index(table => 'prod.db.t', column => "
                + "'embedding')");
  }
}
