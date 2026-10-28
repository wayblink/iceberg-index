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

import org.apache.iceberg.index.IndexAdapter;
import org.apache.iceberg.index.IndexTypes;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Reads and prepares one index file on the executor, using the read context the command carries.
 * The adapter is created from the index definition; the reader never runs on the driver.
 */
final class IndexCacheReadContext implements ExecutorIndexCacheWorker.Loader {
  @Override
  public ExecutorIndexCache.CachedFile load(IndexCacheProtocol.Command command) {
    IndexAdapter adapter =
        IndexTypes.fromType(command.definition().type()).createAdapter(command.definition());
    Preconditions.checkArgument(
        IndexCacheMemory.compatible(command.file().memory(), adapter),
        "Index memory model, reader or runtime profile is incompatible: %s",
        command.file().location());
    DistributedIndexSearch.LoadedFile loaded =
        DistributedIndexSearch.loadFileState(command.io(), command.file().location(), adapter);
    Preconditions.checkArgument(
        loaded.preparedBytes() > 0,
        "Index prepared memory is unknown: %s",
        command.file().location());
    return loaded.file();
  }
}
