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
package org.apache.iceberg.index.vector;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iceberg.index.IndexEntry;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The bridge from a vector index definition to the native IVF build kernel.
 *
 * <p>The native kernel is compared against {@link IvfKernel} byte for byte, so every input the Java
 * kernel reads from a definition travels here in one frame: the parameters in a single buffer, the
 * entries as the native side asks for them, and the payload back. The frames are big-endian and
 * versioned by {@link #ABI_VERSION}; none of their fields is a user-facing option.
 *
 * <p>A definition that asks for the native kernel reaches it through {@link #ensureLoaded} and
 * {@link #buildFromVectors}: the vectors the Java kernel decoded are what the native one is handed,
 * and the payload it returns is the payload the Java kernel would have written. The library is
 * optional, so a build falls back to the Java kernel when it cannot be loaded; nothing here is a
 * requirement for a build to succeed.
 */
final class NativeIvfBuild {

  private static final Logger LOG = LoggerFactory.getLogger(NativeIvfBuild.class);

  /** The library name, without the prefix and suffix the platform gives it. */
  static final String LIBRARY_NAME = "iceberg_ivf_native";

  /** Where a jar keeps the per-platform library, relative to this class. */
  private static final String LIBRARY_RESOURCE_PREFIX = "/org/apache/iceberg/index/vector/native/";

  /** The magic of the parameters frame, "NIVP". */
  private static final int PARAMS_MAGIC = 0x4E495650;

  /** The magic of the result frame, "NIVR". */
  private static final int RESULT_MAGIC = 0x4E495652;

  /** The version of the frames this bridge writes and reads. */
  static final int ABI_VERSION = 1;

  private static final int METRIC_L2 = 0;

  private static final int METRIC_COSINE = 1;

  private static final int METRIC_DOT = 2;

  private static final AtomicBoolean LOADED = new AtomicBoolean(false);

  /** Keeps a JVM that cannot load the library from warning once per build. */
  private static final AtomicBoolean LOAD_FAILURE_REPORTED = new AtomicBoolean(false);

  private NativeIvfBuild() {}

  /** The entries a native build reads, one block at a time. */
  interface VectorChunkSource {

    /** The number of entries the build covers. */
    int count();

    /**
     * Writes {@code count} records starting at {@code start} from the buffer's index zero.
     *
     * <p>Each record is the data file index, the row position and the vector, all big-endian. The
     * native side reads the buffer from zero to its capacity, so the position is left alone.
     */
    void fetch(int start, int count, ByteBuffer out);
  }

  /** The payload a native build wrote, and what the build spent. */
  static final class Built {
    private final byte[] payload;
    private final int listCount;
    private final int sampleCount;
    private final int iterations;
    private final long entryCount;
    private final long nanosRead;
    private final long nanosTrain;
    private final long nanosAssign;
    private final long nanosEncode;

    private Built(
        byte[] payload,
        int listCount,
        int sampleCount,
        int iterations,
        long entryCount,
        long nanosRead,
        long nanosTrain,
        long nanosAssign,
        long nanosEncode) {
      this.payload = payload;
      this.listCount = listCount;
      this.sampleCount = sampleCount;
      this.iterations = iterations;
      this.entryCount = entryCount;
      this.nanosRead = nanosRead;
      this.nanosTrain = nanosTrain;
      this.nanosAssign = nanosAssign;
      this.nanosEncode = nanosEncode;
    }

    byte[] payload() {
      return payload.clone();
    }

    int listCount() {
      return listCount;
    }

    int sampleCount() {
      return sampleCount;
    }

    int iterations() {
      return iterations;
    }

    long entryCount() {
      return entryCount;
    }

    long nanosRead() {
      return nanosRead;
    }

    long nanosTrain() {
      return nanosTrain;
    }

    long nanosAssign() {
      return nanosAssign;
    }

    long nanosEncode() {
      return nanosEncode;
    }
  }

  private static native int nativeVersion();

  private static native long nativeOpen(byte[] params, VectorChunkSource source);

  private static native byte[] nativeBuild(long handle);

  private static native void nativeClose(long handle);

  /** The ABI version the loaded library reports. */
  static int version() {
    return nativeVersion();
  }

  /** Whether {@link #load} has loaded the library in this JVM. */
  static boolean available() {
    return LOADED.get();
  }

  /** Loads the library from an explicit path, which is what a test does before building. */
  static void load(Path library) {
    Preconditions.checkNotNull(library, "Native IVF library path is null");
    System.load(library.toAbsolutePath().toString());
    LOADED.set(true);
  }

  /**
   * Loads the library a build uses, and reports whether a native build can run in this JVM.
   *
   * <p>The library is taken from the JVM's library path first, then from the resource this build
   * ships for the platform, so an installation that only has the jar still finds it. A JVM with
   * neither keeps building with the Java kernel: the native kernel is optional, and a build that
   * asked for it must not fail because it is missing.
   */
  static boolean ensureLoaded() {
    if (LOADED.get()) {
      return true;
    }

    synchronized (LOADED) {
      if (LOADED.get()) {
        return true;
      }

      String libraryPathFailure = loadFromLibraryPath();
      if (libraryPathFailure == null) {
        LOADED.set(true);
        return true;
      }

      String resourceFailure = loadFromResource();
      if (resourceFailure == null) {
        LOADED.set(true);
        return true;
      }

      if (LOAD_FAILURE_REPORTED.compareAndSet(false, true)) {
        LOG.warn(
            "The native IVF build kernel is not available ({}; {}); builds that ask for it fall"
                + " back to the Java kernel. It needs a loadable {} on java.library.path, or one"
                + " packaged under {}<platform>/",
            libraryPathFailure,
            resourceFailure,
            System.mapLibraryName(LIBRARY_NAME),
            LIBRARY_RESOURCE_PREFIX);
      }

      return false;
    }
  }

  /** Loads the library from the library path, answering null on success. */
  private static String loadFromLibraryPath() {
    try {
      System.loadLibrary(LIBRARY_NAME);
      return null;
    } catch (UnsatisfiedLinkError | SecurityException e) {
      return "no "
          + System.mapLibraryName(LIBRARY_NAME)
          + " on the library path: "
          + e.getMessage();
    }
  }

  /** Loads the library from the resource this build ships, answering null on success. */
  private static String loadFromResource() {
    String platform = platformName();
    if (platform == null) {
      return "no packaged library for "
          + System.getProperty("os.name")
          + "/"
          + System.getProperty("os.arch");
    }

    String resource =
        LIBRARY_RESOURCE_PREFIX + platform + "/" + System.mapLibraryName(LIBRARY_NAME);
    try (InputStream in = NativeIvfBuild.class.getResourceAsStream(resource)) {
      if (in == null) {
        return "the resource " + resource + " is not in this build";
      }

      Path extracted = Files.createTempFile("iceberg-ivf-native-", librarySuffix());
      extracted.toFile().deleteOnExit();
      Files.copy(in, extracted, StandardCopyOption.REPLACE_EXISTING);
      System.load(extracted.toAbsolutePath().toString());
      return null;
    } catch (IOException e) {
      return "cannot extract " + resource + ": " + e.getMessage();
    } catch (UnsatisfiedLinkError | SecurityException e) {
      return "cannot load " + resource + ": " + e.getMessage();
    }
  }

  /** The resource directory of this platform, or null when this build ships no library for it. */
  private static String platformName() {
    String os = operatingSystem();
    String arch = architecture();
    return os == null || arch == null ? null : os + "-" + arch;
  }

  private static String operatingSystem() {
    String name = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
    if (name.contains("linux")) {
      return "linux";
    } else if (name.contains("mac") || name.contains("darwin")) {
      return "macos";
    } else if (name.contains("windows")) {
      return "windows";
    }

    return null;
  }

  private static String architecture() {
    String arch = System.getProperty("os.arch", "").toLowerCase(Locale.ROOT);
    return switch (arch) {
      case "amd64", "x86_64" -> "x86_64";
      case "aarch64", "arm64" -> "aarch64";
      case "x86", "i386", "i686" -> "x86";
      default -> null;
    };
  }

  private static String librarySuffix() {
    String name = System.mapLibraryName(LIBRARY_NAME);
    int dot = name.lastIndexOf('.');
    return dot < 0 ? "" : name.substring(dot);
  }

  static Built build(VectorIndexAdapter adapter, List<IndexEntry> entries, int parallelism) {
    return build(adapter, entries, parallelism, "");
  }

  static Built build(
      VectorIndexAdapter adapter, List<IndexEntry> entries, int parallelism, String unitId) {
    Preconditions.checkNotNull(adapter, "Vector index adapter is null");
    Preconditions.checkNotNull(entries, "Index entries are null");
    Preconditions.checkArgument(!entries.isEmpty(), "A native IVF build needs at least one entry");

    VectorKernel kernel = adapter.kernel();
    Preconditions.checkArgument(
        kernel instanceof IvfKernel,
        "The native IVF build kernel builds IVF payloads only: %s",
        kernel.name());
    IvfKernel ivf = (IvfKernel) kernel;
    Preconditions.checkArgument(
        ivf.scoring().metric() == VectorIndexDefinition.Metric.L2,
        "The native IVF build kernel supports the L2 metric only: %s",
        ivf.scoring().metric());

    int dimension = ivf.dimension();
    int entryCount = entries.size();
    int threads = threadsFor(parallelism, entryCount);
    IvfOptions options = ivf.options();
    int listCount = options.nlistFor(entryCount);
    int sampleCount = options.samplesFor(entryCount, listCount);
    List<String> dataFiles = VectorPayloads.dataFilesOf(entries);
    int[] fileIndexes = fileIndexes(entries, dataFiles);
    byte[] params =
        encodeParams(
            unitId,
            dimension,
            entryCount,
            dataFiles,
            listCount,
            sampleCount,
            options,
            ivf.quantizer(),
            ivf.scoring().metric(),
            threads);

    return openAndBuild(params, new EntrySource(entries, fileIndexes, dimension));
  }

  /**
   * Builds the payload of vectors that are already decoded.
   *
   * <p>This is the path a production build takes: the Java kernel decodes the entries once, and the
   * native kernel reads those vectors instead of decoding the entries a second time. The data file
   * indexes are the positions of the entries' files in {@code dataFiles}, so the dictionary the
   * payload writes is the one the Java kernel writes.
   */
  static Built buildFromVectors(
      IvfKernel kernel,
      List<String> dataFiles,
      int[] fileIndexes,
      long[] rowPositions,
      float[][] vectors,
      int parallelism) {
    Preconditions.checkNotNull(kernel, "IVF kernel is null");
    Preconditions.checkNotNull(dataFiles, "Data files are null");
    Preconditions.checkNotNull(fileIndexes, "Data file indexes are null");
    Preconditions.checkNotNull(rowPositions, "Row positions are null");
    Preconditions.checkNotNull(vectors, "Decoded vectors are null");
    Preconditions.checkArgument(vectors.length > 0, "A native IVF build needs at least one entry");
    Preconditions.checkArgument(
        fileIndexes.length == vectors.length,
        "A native IVF build needs one data file index per vector: %s indexes for %s vectors",
        fileIndexes.length,
        vectors.length);
    Preconditions.checkArgument(
        rowPositions.length == vectors.length,
        "A native IVF build needs one row position per vector: %s positions for %s vectors",
        rowPositions.length,
        vectors.length);
    Preconditions.checkArgument(
        kernel.scoring().metric() == VectorIndexDefinition.Metric.L2,
        "The native IVF build kernel supports the L2 metric only: %s",
        kernel.scoring().metric());

    int dimension = kernel.dimension();
    for (int i = 0; i < vectors.length; i++) {
      Preconditions.checkArgument(
          vectors[i] != null && vectors[i].length == dimension,
          "Native IVF vector %s has %s dimensions, the index has %s",
          i,
          vectors[i] == null ? "no" : Integer.toString(vectors[i].length),
          dimension);
      Preconditions.checkArgument(
          fileIndexes[i] >= 0 && fileIndexes[i] < dataFiles.size(),
          "Native IVF vector %s refers to data file index %s, the unit has %s",
          i,
          fileIndexes[i],
          dataFiles.size());
    }

    int entryCount = vectors.length;
    int threads = threadsFor(parallelism, entryCount);
    IvfOptions options = kernel.options();
    int listCount = options.nlistFor(entryCount);
    int sampleCount = options.samplesFor(entryCount, listCount);
    byte[] params =
        encodeParams(
            "",
            dimension,
            entryCount,
            dataFiles,
            listCount,
            sampleCount,
            options,
            kernel.quantizer(),
            kernel.scoring().metric(),
            threads);

    return openAndBuild(params, new VectorSource(vectors, fileIndexes, rowPositions, dimension));
  }

  /** How many threads a build of {@code entryCount} entries may use. */
  private static int threadsFor(int parallelism, int entryCount) {
    return Math.max(1, Math.min(parallelism, Math.max(1, entryCount)));
  }

  /** Opens one build, reads its rows through {@code source}, and decodes the payload it returns. */
  private static Built openAndBuild(byte[] params, VectorChunkSource source) {
    long handle = 0L;
    byte[] frame;
    try {
      handle = nativeOpen(params, source);
      Preconditions.checkState(handle != 0L, "The native IVF build kernel opened no handle");
      frame = nativeBuild(handle);
    } finally {
      if (handle != 0L) {
        nativeClose(handle);
      }
    }

    return decode(frame);
  }

  private static int[] fileIndexes(List<IndexEntry> entries, List<String> dataFiles) {
    Map<String, Integer> positions = Maps.newHashMap();
    for (int i = 0; i < dataFiles.size(); i++) {
      positions.put(dataFiles.get(i), i);
    }

    int[] indexes = new int[entries.size()];
    for (int i = 0; i < entries.size(); i++) {
      indexes[i] = positions.get(entries.get(i).dataFile());
    }

    return indexes;
  }

  private static byte[] encodeParams(
      String unitId,
      int dimension,
      int entryCount,
      List<String> dataFiles,
      int listCount,
      int sampleCount,
      IvfOptions options,
      RaBitQQuantizer quantizer,
      VectorIndexDefinition.Metric metric,
      int threads) {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(bytes)) {
      out.writeInt(PARAMS_MAGIC);
      out.writeInt(ABI_VERSION);
      out.writeUTF(unitId == null ? "" : unitId);
      out.writeInt(dimension);
      out.writeInt(entryCount);
      out.writeInt(dataFiles.size());
      for (String dataFile : dataFiles) {
        out.writeUTF(dataFile);
      }

      out.writeInt(listCount);
      out.writeInt(sampleCount);
      out.writeInt(options.trainMaxIterations());
      out.writeLong(Double.doubleToRawLongBits(options.trainConvergenceTolerance()));
      out.writeInt(quantizer == null ? 0 : quantizer.bits());
      out.writeLong(quantizer == null ? 0L : quantizer.seed());
      out.writeInt(metricCode(metric));
      out.writeBoolean(options.listBlocks());
      out.writeInt(threads);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to encode the native IVF build parameters", e);
    }

    return bytes.toByteArray();
  }

  private static int metricCode(VectorIndexDefinition.Metric metric) {
    return switch (metric) {
      case L2 -> METRIC_L2;
      case COSINE -> METRIC_COSINE;
      case DOT -> METRIC_DOT;
    };
  }

  private static Built decode(byte[] frame) {
    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(frame))) {
      int magic = in.readInt();
      Preconditions.checkState(
          magic == RESULT_MAGIC,
          "The native IVF build returned an unexpected result magic: 0x%s",
          Integer.toHexString(magic));

      int version = in.readInt();
      Preconditions.checkState(
          version == ABI_VERSION,
          "The native IVF build returned result frame version %s; this bridge reads %s",
          version,
          ABI_VERSION);

      int status = in.readInt();
      String message = in.readUTF();
      Preconditions.checkState(
          status == 0,
          message == null || message.isEmpty()
              ? "The native IVF build failed with status " + status
              : message);

      int listCount = in.readInt();
      int sampleCount = in.readInt();
      int iterations = in.readInt();
      long entryCount = in.readLong();
      long nanosRead = in.readLong();
      long nanosTrain = in.readLong();
      long nanosAssign = in.readLong();
      long nanosEncode = in.readLong();
      int payloadBytes = in.readInt();
      Preconditions.checkState(
          payloadBytes >= 0, "Invalid native IVF payload bytes: %s", payloadBytes);
      byte[] payload = new byte[payloadBytes];
      in.readFully(payload);
      Preconditions.checkState(
          in.available() == 0,
          "The native IVF build returned %s bytes after its payload",
          in.available());

      return new Built(
          payload,
          listCount,
          sampleCount,
          iterations,
          entryCount,
          nanosRead,
          nanosTrain,
          nanosAssign,
          nanosEncode);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read the native IVF build result", e);
    }
  }

  /** The entries, served to the native kernel a block at a time. */
  private static final class EntrySource implements VectorChunkSource {
    private final List<IndexEntry> entries;
    private final int[] fileIndexes;
    private final int dimension;

    private EntrySource(List<IndexEntry> entries, int[] fileIndexes, int dimension) {
      this.entries = entries;
      this.fileIndexes = fileIndexes;
      this.dimension = dimension;
    }

    @Override
    public int count() {
      return entries.size();
    }

    @Override
    public void fetch(int start, int count, ByteBuffer out) {
      checkFetchBuffer(out, count, dimension);
      int recordBytes = recordBytes(dimension);
      for (int i = 0; i < count; i++) {
        int row = start + i;
        int offset = i * recordBytes;
        float[] vector =
            VectorPayloads.decodeVector(entries.get(row).payload(), dimension, "Entry");
        out.putInt(offset, fileIndexes[row]);
        out.putLong(offset + Integer.BYTES, entries.get(row).rowPosition());
        for (int d = 0; d < vector.length; d++) {
          out.putFloat(offset + Integer.BYTES + Long.BYTES + d * Float.BYTES, vector[d]);
        }
      }
    }
  }

  /** Decoded vectors, served to the native kernel a block at a time. */
  private static final class VectorSource implements VectorChunkSource {
    private final float[][] vectors;
    private final int[] fileIndexes;
    private final long[] rowPositions;
    private final int dimension;

    private VectorSource(float[][] vectors, int[] fileIndexes, long[] rowPositions, int dimension) {
      this.vectors = vectors;
      this.fileIndexes = fileIndexes;
      this.rowPositions = rowPositions;
      this.dimension = dimension;
    }

    @Override
    public int count() {
      return vectors.length;
    }

    @Override
    public void fetch(int start, int count, ByteBuffer out) {
      checkFetchBuffer(out, count, dimension);
      int recordBytes = recordBytes(dimension);
      for (int i = 0; i < count; i++) {
        int row = start + i;
        int offset = i * recordBytes;
        out.putInt(offset, fileIndexes[row]);
        out.putLong(offset + Integer.BYTES, rowPositions[row]);
        float[] vector = vectors[row];
        for (int d = 0; d < dimension; d++) {
          out.putFloat(offset + Integer.BYTES + Long.BYTES + d * Float.BYTES, vector[d]);
        }
      }
    }
  }

  /** Refuses a buffer the native side could not fill with {@code count} records. */
  private static void checkFetchBuffer(ByteBuffer out, int count, int dimension) {
    Preconditions.checkNotNull(out, "Native IVF vector buffer is null");
    Preconditions.checkArgument(out.isDirect(), "Native IVF vector buffer is not direct");
    long needed = (long) count * recordBytes(dimension);
    Preconditions.checkArgument(
        out.capacity() >= needed,
        "Native IVF vector buffer holds %s bytes, %s records need %s",
        out.capacity(),
        count,
        needed);
  }

  private static int recordBytes(int dimension) {
    return Integer.BYTES + Long.BYTES + dimension * Float.BYTES;
  }
}
