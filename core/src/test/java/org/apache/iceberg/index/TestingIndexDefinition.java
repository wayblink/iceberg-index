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
package org.apache.iceberg.index;

import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

/** A minimal {@link IndexDefinition} used by core tests so core needs no real adapter. */
public final class TestingIndexDefinition implements IndexDefinition {

  private final int width;
  private final Map<String, String> properties;

  private TestingIndexDefinition(int width, Map<String, String> properties) {
    Preconditions.checkArgument(width > 0, "Invalid width: %s", width);
    this.width = width;
    this.properties = ImmutableMap.copyOf(properties);
  }

  public static TestingIndexDefinition of(int width) {
    return new TestingIndexDefinition(width, ImmutableMap.of());
  }

  @Override
  public String type() {
    return TestingIndexType.TYPE;
  }

  public int width() {
    return width;
  }

  public Map<String, String> properties() {
    return properties;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof TestingIndexDefinition)) {
      return false;
    }
    TestingIndexDefinition that = (TestingIndexDefinition) other;
    return width == that.width && properties.equals(that.properties);
  }

  @Override
  public int hashCode() {
    return 31 * Integer.hashCode(width) + properties.hashCode();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("width", width)
        .add("properties", properties)
        .toString();
  }
}
