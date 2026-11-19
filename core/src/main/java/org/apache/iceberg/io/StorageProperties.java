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
package org.apache.iceberg.io;

import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * Resolves the file-system settings a format that opens files itself needs.
 *
 * <p>A format that reaches the object store through its own client (Lance, for one) cannot read the
 * settings from an {@link InputFile}: it is handed a path and a map of options. Those settings can
 * live in four places, and engines keep them in different ones:
 *
 * <ol>
 *   <li>the table's properties, the most specific;
 *   <li>the catalog's {@link FileIO} properties;
 *   <li>the engine's Hadoop configuration ({@code fs.s3a.*} and its siblings), which is where a
 *       Spark submission puts {@code spark.hadoop.fs.s3a.*};
 *   <li>the process environment, which the object-store client itself reads when nothing else sets
 *       a value.
 * </ol>
 *
 * <p>This class resolves the first three, in that order: a later source never overwrites a value an
 * earlier one set. The environment is deliberately left out of the map — it remains the client's
 * own last resort, so copying it here would only obscure where a value came from. A caller that
 * wants to report what was missing can look at the resolved map and still be right, because a value
 * the environment supplies was not resolved from any of the three sources.
 *
 * <p>Hadoop configurations are filtered to the storage keys: they carry hundreds of unrelated
 * entries, some of them secrets that have no business being handed to a file format.
 */
public final class StorageProperties {

  private static final ImmutableList<String> PREFIXES =
      ImmutableList.of(
          "s3.", "fs.s3.", "fs.s3a.", "gcs.", "fs.gs.", "azure.", "fs.azure.", "lance.storage.");

  private StorageProperties() {}

  /** Whether this key configures an object store client rather than the engine or the table. */
  public static boolean isStorageProperty(String key) {
    for (String prefix : PREFIXES) {
      if (key.startsWith(prefix)) {
        return true;
      }
    }

    return false;
  }

  /** The storage keys of a Hadoop configuration, in the form the engine carries them. */
  public static Map<String, String> fromConf(Iterable<Map.Entry<String, String>> conf) {
    Map<String, String> properties = Maps.newLinkedHashMap();
    for (Map.Entry<String, String> entry : conf) {
      if (entry.getKey() != null && entry.getValue() != null && isStorageProperty(entry.getKey())) {
        properties.put(entry.getKey(), entry.getValue());
      }
    }

    return properties;
  }

  /** The properties a {@link FileIO} exposes to the formats it opens files for. */
  public static Map<String, String> fromFileIO(FileIO io) {
    try {
      return io.properties();
    } catch (UnsupportedOperationException e) {
      // this FileIO does not expose its configuration; the other sources still apply
      return Maps.newLinkedHashMap();
    }
  }

  /**
   * The settings a format should be given, with the table's properties winning over the catalog's
   * and the catalog's over the Hadoop configuration's.
   *
   * @param io the table's FileIO, or null when the path has no FileIO behind it
   * @param tableProperties the table's properties, most specific
   * @param conf the engine's Hadoop configuration entries, least specific
   */
  public static Map<String, String> resolve(
      FileIO io, Map<String, String> tableProperties, Iterable<Map.Entry<String, String>> conf) {
    Map<String, String> properties = Maps.newLinkedHashMap();
    properties.putAll(fromConf(conf));
    if (io != null) {
      properties.putAll(fromFileIO(io));
    }

    if (tableProperties != null) {
      properties.putAll(tableProperties);
    }

    return properties;
  }
}
