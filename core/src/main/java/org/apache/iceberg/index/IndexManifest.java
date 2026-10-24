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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

/**
 * An immutable listing of the {@link IndexFile index files} of a group within an {@link
 * IndexSnapshot}, the counterpart of an Iceberg manifest.
 *
 * <p>Each entry describes one index file (location, kind, blob type, entry count) and declares the
 * source data files it covers. The union of covered data files across manifests is the coverage set
 * of the index snapshot: source data files that are not covered still need a residual scan.
 *
 * <p>The binary encoding is a self-describing, versioned MVP format. It can be replaced by a
 * standard encoding without changing the metadata model.
 */
public final class IndexManifest {

  /** Format version of this encoding. */
  public static final int FORMAT_VERSION = 2;

  private static final int LEGACY_FORMAT_VERSION = 1;

  private static final byte[] MAGIC = new byte[] {'I', 'C', 'E', 'I', 'M', 'N', '1', 0};

  private final List<IndexFile> indexFiles;

  private IndexManifest(List<IndexFile> indexFiles) {
    this.indexFiles = ImmutableList.copyOf(indexFiles);
  }

  public static void write(OutputFile outputFile, List<IndexFile> indexFiles) throws IOException {
    Preconditions.checkNotNull(outputFile, "Output file is null");
    Preconditions.checkNotNull(indexFiles, "Index files are null");
    for (IndexFile indexFile : indexFiles) {
      Preconditions.checkArgument(
          indexFile.memory() == null || indexFile.memory().fileBytes() != null,
          "Index file bytes are not set: %s",
          indexFile.location());
    }

    try (PositionOutputStream raw = outputFile.create();
        DataOutputStream out = new DataOutputStream(raw)) {
      out.write(MAGIC);
      out.writeInt(FORMAT_VERSION);
      out.writeInt(indexFiles.size());
      for (IndexFile indexFile : indexFiles) {
        out.writeUTF(indexFile.location());
        out.writeUTF(indexFile.kind().name());
        out.writeUTF(indexFile.blobType());
        out.writeLong(indexFile.entryCount());
        List<String> coveredDataFiles = indexFile.coveredDataFiles();
        out.writeInt(coveredDataFiles.size());
        for (String dataFile : coveredDataFiles) {
          out.writeUTF(dataFile);
        }

        writeMemory(out, indexFile.memory());
      }
    }
  }

  public static IndexManifest read(InputFile inputFile) throws IOException {
    Preconditions.checkNotNull(inputFile, "Input file is null");
    try (SeekableInputStream raw = inputFile.newStream();
        DataInputStream in = new DataInputStream(raw)) {
      byte[] magic = new byte[MAGIC.length];
      in.readFully(magic);
      Preconditions.checkArgument(Arrays.equals(MAGIC, magic), "Invalid index manifest magic");
      int version = in.readInt();
      Preconditions.checkArgument(
          version == LEGACY_FORMAT_VERSION || version == FORMAT_VERSION,
          "Unsupported index manifest format version: %s",
          version);
      int indexFileCount = in.readInt();
      Preconditions.checkArgument(
          indexFileCount >= 0, "Invalid index file count: %s", indexFileCount);
      List<IndexFile> indexFiles = Lists.newArrayList();
      for (int i = 0; i < indexFileCount; i++) {
        String location = in.readUTF();
        IndexFile.Kind kind = IndexFile.Kind.valueOf(in.readUTF());
        String blobType = in.readUTF();
        long entryCount = in.readLong();
        int coveredCount = in.readInt();
        Preconditions.checkArgument(
            coveredCount >= 0, "Invalid covered data file count: %s", coveredCount);
        List<String> coveredDataFiles = Lists.newArrayList();
        for (int j = 0; j < coveredCount; j++) {
          coveredDataFiles.add(in.readUTF());
        }
        indexFiles.add(
            IndexFile.builder()
                .location(location)
                .kind(kind)
                .blobType(blobType)
                .entryCount(entryCount)
                .coveredDataFiles(coveredDataFiles)
                .memory(version == LEGACY_FORMAT_VERSION ? null : readMemory(in))
                .build());
      }
      return new IndexManifest(indexFiles);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read index manifest", e);
    }
  }

  private static void writeMemory(DataOutputStream out, IndexFileMemory memory) throws IOException {
    out.writeBoolean(memory != null);
    if (memory == null) {
      return;
    }

    out.writeLong(memory.fileBytes());
    out.writeLong(memory.payloadBytes());
    out.writeLong(memory.preparedBytes());
    out.writeInt(memory.modelVersion());
    out.writeUTF(memory.readerId());
    out.writeUTF(memory.runtimeProfile());
    out.writeInt(memory.structure().size());
    for (String key : new TreeSet<>(memory.structure().keySet())) {
      out.writeUTF(key);
      out.writeLong(memory.structure().get(key));
    }
  }

  private static IndexFileMemory readMemory(DataInputStream in) throws IOException {
    if (!in.readBoolean()) {
      return null;
    }

    IndexFileMemory.Builder builder =
        IndexFileMemory.builder()
            .fileBytes(in.readLong())
            .payloadBytes(in.readLong())
            .preparedBytes(in.readLong())
            .modelVersion(in.readInt())
            .readerId(in.readUTF())
            .runtimeProfile(in.readUTF());
    int structureCount = in.readInt();
    Preconditions.checkArgument(
        structureCount >= 0, "Invalid index memory structure count: %s", structureCount);
    Map<String, Long> structure = new HashMap<>();
    for (int i = 0; i < structureCount; i++) {
      String key = in.readUTF();
      Preconditions.checkArgument(
          !structure.containsKey(key), "Duplicate index memory structure key: %s", key);
      structure.put(key, in.readLong());
    }

    return builder.structure(structure).build();
  }

  /** Returns the index files listed by this manifest. */
  public List<IndexFile> indexFiles() {
    return indexFiles;
  }

  /** Returns source data files covered by this manifest, preserving first-seen order. */
  public Set<String> indexedDataFiles() {
    Set<String> dataFiles = Sets.newLinkedHashSet();
    for (IndexFile indexFile : indexFiles) {
      dataFiles.addAll(indexFile.coveredDataFiles());
    }
    return ImmutableSet.copyOf(dataFiles);
  }
}
