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
package org.apache.iceberg.index.text;

import java.io.IOException;
import java.util.Set;
import org.apache.iceberg.index.text.TextIndexAdapter.CorpusCounts;
import org.apache.iceberg.index.text.TextIndexAdapter.FileIndex;

/**
 * The decodable content of one BM25 payload, independent of the payload layout.
 *
 * <p>A payload written before a format change stays readable: the adapter opens the layout the
 * payload's signature names, and scores through this interface so the search path does not fork.
 */
interface TextIndexShard {
  /** The payload's document count, token count and the document frequency of the given terms. */
  CorpusCounts counts(Set<String> terms) throws IOException;

  /**
   * The payload's postings, limited to the given terms, or only its statistics.
   *
   * @param terms the terms to read, or null for every term
   * @param statisticsOnly whether to read the statistics header only, without postings or documents
   */
  FileIndex shard(Set<String> terms, boolean statisticsOnly) throws IOException;
}
