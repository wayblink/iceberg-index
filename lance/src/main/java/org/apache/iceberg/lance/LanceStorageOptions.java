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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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

  /** The region the AWS clients sign for when nothing else names one. */
  private static final String DEFAULT_REGION = "us-east-1";

  /**
   * The region an endpoint names when the endpoint is one of AWS's own: a host such as {@code
   * s3.eu-west-1.amazonaws.com}, {@code s3-eu-west-1.amazonaws.com}, or the VPC endpoint form, all
   * of which carry the region.
   */
  private static final Pattern REGION_IN_ENDPOINT =
      Pattern.compile("(?:^|[./])s3[.-]([a-z]{2}(?:-[a-z]+)*-[0-9]+)\\.[a-z0-9.-]*amazonaws");

  private static final AtomicBoolean DEFAULT_REGION_WARNED = new AtomicBoolean(false);

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
   * <p>The properties are expected in the order {@code StorageProperties.resolve} returns, from the
   * least to the most specific source. The translation walks them backwards so that the more
   * specific spelling of one setting wins over the less specific one: a table that sets {@code
   * fs.s3a.endpoint} has to beat a catalog that sets {@code s3.endpoint}, and a map without an
   * order cannot say which of the two was the table's.
   *
   * <p>Values are shaped for {@code LanceFileReader}/{@code LanceFileWriter}; properties that are
   * not storage settings, and storage settings Lance has no equivalent for, are left out rather
   * than failing the read.
   */
  static Map<String, String> from(Map<String, String> properties) {
    Map<String, String> options = Maps.newHashMap();
    List<String> unmapped = Lists.newArrayList();
    boolean s3 = false;

    // Two passes, not one: the explicit escape hatch is applied after every translation, so a
    // setting spelled both ways resolves to the explicit one whatever order the map iterates in.
    List<Map.Entry<String, String>> entries = Lists.newArrayList(properties.entrySet());
    for (int i = entries.size() - 1; i >= 0; i -= 1) {
      Map.Entry<String, String> entry = entries.get(i);
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
        s3 = true;
        // the first entry in reverse order comes from the most specific source that spells it
        options.putIfAbsent(bareKey, value);
      } else if (PATH_STYLE_KEYS.contains(key)) {
        // Iceberg says "path style = yes"; Lance asks for the opposite flag
        Boolean pathStyle = parseBoolean(key, value);
        if (pathStyle != null) {
          options.putIfAbsent("virtual_hosted_style_request", Boolean.toString(!pathStyle));
        }
      } else if (SSL_KEYS.contains(key)) {
        // Hadoop says "ssl enabled"; Lance asks whether plain http is allowed
        Boolean sslEnabled = parseBoolean(key, value);
        if (sslEnabled != null) {
          options.putIfAbsent("allow_http", Boolean.toString(!sslEnabled));
        }
      } else if (!key.startsWith(LANCE_PREFIX) && looksLikeStorageConfig(key)) {
        unmapped.add(key);
      }
    }

    for (Map.Entry<String, String> entry : properties.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      if (value == null || !key.startsWith(LANCE_PREFIX)) {
        continue;
      }

      String lanceKey = key.substring(LANCE_PREFIX.length());
      if (!lanceKey.isEmpty()) {
        options.put(lanceKey, value);
      }
    }

    if (s3) {
      addRegion(options);
    }

    if (!unmapped.isEmpty()) {
      // the Hadoop configuration brings dozens of fs.s3a.* tuning keys of its own, all expected
      // here; a warning per file open would drown the log
      LOG.debug("Ignoring storage properties Lance has no equivalent for: {}", unmapped);
    }

    return options;
  }

  /**
   * Fills in the region the S3 reader has to be told, when the properties do not name one.
   *
   * <p>Lance asks the AWS instance metadata service for the region it was not given. That service
   * is not there outside EC2, and asking costs the client's one-second connect timeout on every
   * open: a fetch-back that resolves hundreds of data files pays it once per file, which is most of
   * the time such a lookup takes. The region the endpoint names is used where it names one, the
   * region the process names where it names one, and the one the AWS clients fall back to
   * otherwise.
   *
   * <p>Only a reader that was handed credentials and an endpoint is filled in: such a reader needs
   * no metadata service, while one that was handed neither may be on an instance that keeps the
   * region and the credentials there, and overriding that would sign for the wrong region.
   */
  private static void addRegion(Map<String, String> options) {
    if (options.containsKey("region")
        || !options.containsKey("endpoint")
        || !options.containsKey("access_key_id")
        || !options.containsKey("secret_access_key")) {
      return;
    }

    String region = regionOfEndpoint(options.get("endpoint"));
    if (region == null) {
      region = environment("AWS_REGION");
    }
    if (region == null) {
      region = environment("AWS_DEFAULT_REGION");
    }
    if (region == null) {
      region = DEFAULT_REGION;
      // once per lifetime rather than once per lookup: the settings are the same for every read
      if (DEFAULT_REGION_WARNED.compareAndSet(false, true)) {
        LOG.warn(
            "No region was configured for the S3 endpoint {}, so reads sign for {}. Lance otherwise "
                + "asks the instance metadata service for a region, and that lookup costs each data "
                + "file open its one-second connect timeout. Set s3.region (or "
                + "fs.s3a.endpoint.region) if the store expects another region.",
            options.get("endpoint"),
            DEFAULT_REGION);
      }
    }

    options.put("region", region);
  }

  /** The AWS region an endpoint host names, or null when the host names none. */
  private static String regionOfEndpoint(String endpoint) {
    Matcher matcher = REGION_IN_ENDPOINT.matcher(endpoint);
    return matcher.find() ? matcher.group(1) : null;
  }

  private static String environment(String name) {
    String value = System.getenv(name);
    return value == null || value.trim().isEmpty() ? null : value.trim();
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
