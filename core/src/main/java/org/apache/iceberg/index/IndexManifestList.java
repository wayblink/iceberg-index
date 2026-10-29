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
import java.util.List;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * The immutable file listing of an {@link IndexSnapshot}, the counterpart of an Iceberg manifest
 * list.
 *
 * <p>The manifest list is the root of an index snapshot's files: it references the index manifests
 * and carries per-manifest summaries (index file count and entry count) so a reader can prune
 * without opening every manifest. Coverage of source data files is declared by the manifests
 * themselves.
 *
 * <p>The binary encoding is a self-describing, versioned MVP format. It can be replaced by a
 * standard encoding without changing the metadata model.
 */
public final class IndexManifestList {

  /** Format version of this encoding. */
  public static final int FORMAT_VERSION = 1;

  private static final byte[] MAGIC = new byte[] {'I', 'C', 'E', 'I', 'M', 'L', '1', 0};

  private final List<ManifestRef> manifests;

  private IndexManifestList(List<ManifestRef> manifests) {
    this.manifests = ImmutableList.copyOf(manifests);
  }

  public static void write(OutputFile outputFile, List<ManifestRef> manifests) throws IOException {
    Preconditions.checkNotNull(outputFile, "Output file is null");
    Preconditions.checkNotNull(manifests, "Manifest references are null");
    try (PositionOutputStream raw = outputFile.create();
        DataOutputStream out = new DataOutputStream(raw)) {
      out.write(MAGIC);
      out.writeInt(FORMAT_VERSION);
      out.writeInt(manifests.size());
      for (ManifestRef manifest : manifests) {
        out.writeUTF(manifest.location());
        out.writeInt(manifest.indexFileCount());
        out.writeLong(manifest.entryCount());
      }
    }
  }

  public static IndexManifestList read(InputFile inputFile) throws IOException {
    Preconditions.checkNotNull(inputFile, "Input file is null");
    try (SeekableInputStream raw = inputFile.newStream();
        DataInputStream in = new DataInputStream(raw)) {
      byte[] magic = new byte[MAGIC.length];
      in.readFully(magic);
      Preconditions.checkArgument(Arrays.equals(MAGIC, magic), "Invalid index manifest list magic");
      int version = in.readInt();
      Preconditions.checkArgument(
          version == FORMAT_VERSION, "Unsupported index manifest list format version: %s", version);
      int manifestCount = in.readInt();
      Preconditions.checkArgument(
          manifestCount >= 0, "Invalid index manifest count: %s", manifestCount);
      List<ManifestRef> manifests = Lists.newArrayList();
      for (int i = 0; i < manifestCount; i++) {
        String location = in.readUTF();
        int indexFileCount = in.readInt();
        long entryCount = in.readLong();
        manifests.add(ManifestRef.of(location, indexFileCount, entryCount));
      }
      return new IndexManifestList(manifests);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read index manifest list", e);
    }
  }

  /** Returns the manifests referenced by this list. */
  public List<ManifestRef> manifests() {
    return manifests;
  }

  /**
   * A reference to one index manifest, with the size of what it holds.
   *
   * <p>The counts are written so a reader can size the snapshot without opening every manifest, and
   * they are part of the list format rather than a hint: nothing skips a manifest on them today.
   */
  public static final class ManifestRef {

    private final String location;
    private final int indexFileCount;
    private final long entryCount;

    private ManifestRef(String location, int indexFileCount, long entryCount) {
      Preconditions.checkArgument(
          location != null && !location.isEmpty(), "Index manifest location is empty");
      Preconditions.checkArgument(
          indexFileCount >= 0, "Invalid index file count: %s", indexFileCount);
      Preconditions.checkArgument(entryCount >= 0, "Invalid entry count: %s", entryCount);
      this.location = location;
      this.indexFileCount = indexFileCount;
      this.entryCount = entryCount;
    }

    public static ManifestRef of(String location, int indexFileCount, long entryCount) {
      return new ManifestRef(location, indexFileCount, entryCount);
    }

    public String location() {
      return location;
    }

    public int indexFileCount() {
      return indexFileCount;
    }

    public long entryCount() {
      return entryCount;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof ManifestRef)) {
        return false;
      }
      ManifestRef that = (ManifestRef) other;
      return indexFileCount == that.indexFileCount
          && entryCount == that.entryCount
          && location.equals(that.location);
    }

    @Override
    public int hashCode() {
      int result = location.hashCode();
      result = 31 * result + Integer.hashCode(indexFileCount);
      result = 31 * result + Long.hashCode(entryCount);
      return result;
    }

    @Override
    public String toString() {
      return "ManifestRef{location='"
          + location
          + "', indexFileCount="
          + indexFileCount
          + ", entryCount="
          + entryCount
          + "}";
    }
  }
}
