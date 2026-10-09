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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;

/**
 * The contract for one index type, registered in {@link IndexTypes}.
 *
 * <p>An index type owns three things: the stable type identifier, the JSON serialization of its
 * {@link IndexDefinition}, and the creation of the {@link IndexAdapter} that encodes, decodes and
 * searches payloads for that type. The generic index layer never inspects type-specific payloads.
 */
public interface IndexType {

  /** Returns the stable type identifier, for example {@code "vector"}. */
  String type();

  /** Parses a definition of this type from its JSON representation. */
  IndexDefinition parseDefinition(JsonNode node);

  /** Writes a definition of this type to JSON. */
  void writeDefinition(IndexDefinition definition, JsonGenerator generator) throws IOException;

  /** Creates the adapter for the given definition, which must be of this type. */
  IndexAdapter createAdapter(IndexDefinition definition);
}
