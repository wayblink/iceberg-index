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

import org.apache.spark.sql.SparkSession;

/**
 * The switch over the driver-side metadata reuse and caches of a query.
 *
 * <p>One query touches the driver's table metadata, the parsed index manifests, and the planned
 * files of a snapshot. All three are read-only, so the driver can keep them: the metadata is handed
 * to the serializable copy rather than read again, and the manifests and the plan are cached. That
 * is the default, and it is what turns a repeated query into cache hits.
 *
 * <p>The switch turns all three off at once. It exists for diagnosis, not for tuning: a query with
 * it off does the reads it did before any of the three existed, so a suspected stale cache can be
 * ruled out without reverting code. It is read from the session first and from the submit
 * configuration second, the same way the other index settings are, and it applies from the next
 * query: an index that is already cached stays cached until something evicts it.
 */
public final class IndexDriverMetadata {

  /** Whether a query may reuse loaded metadata and cache parsed manifests and planned files. */
  public static final String ENABLED = "spark.iceberg.index.driver-metadata.enabled";

  private static final String DEFAULT = "true";

  private IndexDriverMetadata() {}

  /** Whether the driver may reuse and cache what the query path reads. */
  public static boolean enabled(SparkSession spark) {
    String configured = spark.conf().get(ENABLED, null);
    if (configured == null) {
      configured = spark.sparkContext().getConf().get(ENABLED, DEFAULT);
    }

    String value = configured.trim();
    if ("true".equalsIgnoreCase(value)) {
      return true;
    }

    if ("false".equalsIgnoreCase(value)) {
      return false;
    }

    throw new IllegalArgumentException(
        "Invalid " + ENABLED + ": '" + configured + "', expected true or false");
  }
}
