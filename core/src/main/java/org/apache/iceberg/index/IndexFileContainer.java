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

import java.io.IOException;
import java.util.List;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

/**
 * Container for the physical payload files of an index snapshot.
 *
 * <p>An index file container stores type-tagged {@link IndexBlob blobs} in a single file. The
 * default implementation is Puffin ({@link PuffinIndexFileContainer}), Iceberg's standard format
 * for index and statistics artifacts, so index files can be read by any engine without depending on
 * this library. Alternative containers can be plugged in by implementing this interface; the
 * metadata model does not change.
 */
public interface IndexFileContainer {

  /** Writes blobs to a new index file. */
  void write(OutputFile file, List<IndexBlob> blobs) throws IOException;

  /** Reads all blobs of an index file, in offset order. */
  List<IndexBlob> read(InputFile file) throws IOException;
}
