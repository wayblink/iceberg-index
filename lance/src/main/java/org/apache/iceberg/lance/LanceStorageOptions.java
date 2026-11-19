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
package org.apache.iceberg.lance;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Translates Iceberg and Hadoop storage properties into the bare keys the Lance file reader and
 * writer take.
 *
 * <p>Lance reads object-store settings by the names the Rust {@code object_store} crate uses:
 * {@code endpoint}, {@code access_key_id}, {@code secret_access_key}, {@code session_token}, {@code
 * region}, {@code allow_http}, {@code virtual_hosted_style_request}. Passing a property through
 * untouched puts it in a map Lance never looks in, and the read silently falls back to the process
 * environment.
 */
final class LanceStorageOptions {

  private static final Logger LOG = LoggerFactory.getLogger(LanceStorageOptions.class);

  private static final String LANCE_PREFIX = "lance.storage.";

  /** Iceberg's {@code s3.*} catalog properties, in the names Lance understands. */
  private static final Map<String, String> S3_KEYS =
      ImmutableMap.<String, String>builder()
          .put("s3.endpoint", "endpoint")
          .put("s3.access-key-id", "access_key_id")
          .put("s3.secret-access-key", "secret_access_key")
          .put("s3.session-token", "session_token")
          .put("s3.region", "region")
          .build();

  /** The Hadoop keys an engine passes along when S3A is the configured filesystem. */
  private static final Map<String, String> S3A_KEYS =
      ImmutableMap.<String, String>builder()
          .put("fs.s3a.endpoint", "endpoint")
          .put("fs.s3a.access.key", "access_key_id")
          .put("fs.s3a.secret.key", "secret_access_key")
          .put("fs.s3a.session.token", "session_token")
          .put("fs.s3a.endpoint.region", "region")
          .build();

  /** Keys whose value Lance reads as "use virtual-hosted style addressing". */
  private static final List<String> PATH_STYLE_KEYS =
      Lists.newArrayList("s3.path-style-access", "fs.s3a.path.style.access");

  /** Keys that say whether the connection is encrypted; Lance asks the opposite question. */
  private static final List<String> SSL_KEYS = Lists.newArrayList("fs.s3a.connection.ssl.enabled");

  private LanceStorageOptions() {}

  /**
   * Returns the storage options Lance should be given for these properties.
   *
   * <p>Values are shaped for {@code LanceFileReader}/{@code LanceFileWriter}; properties that are
   * not storage settings, and storage settings Lance has no equivalent for, are left out rather
   * than failing the read.
   */
  static Map<String, String> from(Map<String, String> properties) {
    Map<String, String> options = Maps.newHashMap();
    List<String> unmapped = Lists.newArrayList();

    for (Map.Entry<String, String> entry : properties.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      if (value == null) {
        continue;
      }

      String bareKey = S3_KEYS.get(key);
      if (bareKey == null) {
        bareKey = S3A_KEYS.get(key);
      }

      if (bareKey != null) {
        options.put(bareKey, value);
      } else if (PATH_STYLE_KEYS.contains(key)) {
        // Iceberg says "path style = yes"; Lance asks for the opposite flag
        Boolean pathStyle = parseBoolean(key, value);
        if (pathStyle != null) {
          options.put("virtual_hosted_style_request", Boolean.toString(!pathStyle));
        }
      } else if (SSL_KEYS.contains(key)) {
        // Hadoop says "ssl enabled"; Lance asks whether plain http is allowed
        Boolean sslEnabled = parseBoolean(key, value);
        if (sslEnabled != null) {
          options.put("allow_http", Boolean.toString(!sslEnabled));
        }
      } else if (key.startsWith(LANCE_PREFIX)) {
        // explicit Lance escape hatch, applied last so it wins over a translated equivalent
        String lanceKey = key.substring(LANCE_PREFIX.length());
        if (!lanceKey.isEmpty()) {
          options.put(lanceKey, value);
        }
      } else if (looksLikeStorageConfig(key)) {
        unmapped.add(key);
      }
    }

    if (!unmapped.isEmpty()) {
      // the Hadoop configuration brings dozens of fs.s3a.* tuning keys of its own, all expected
      // here; a warning per file open would drown the log
      LOG.debug("Ignoring storage properties Lance has no equivalent for: {}", unmapped);
    }

    return options;
  }

  private static boolean looksLikeStorageConfig(String key) {
    return key.startsWith("s3.")
        || key.startsWith("fs.s3a.")
        || key.startsWith("gcs.")
        || key.startsWith("fs.gs.")
        || key.startsWith("azure.")
        || key.startsWith("fs.azure.")
        || key.startsWith("lance.storage");
  }

  private static Boolean parseBoolean(String key, String value) {
    String lower = value.trim().toLowerCase(Locale.ROOT);
    if ("true".equals(lower)) {
      return true;
    } else if ("false".equals(lower)) {
      return false;
    }

    LOG.warn("Ignoring storage property {}={}: expected true or false", key, value);
    return null;
  }
}
