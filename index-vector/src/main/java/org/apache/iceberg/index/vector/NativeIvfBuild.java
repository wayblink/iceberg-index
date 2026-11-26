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
 * {@link #buildFromEntries}: each entry's payload travels to the native kernel as it was written,
 * so the Java side never holds the decoded vectors. {@link #buildFromVectors} takes vectors the
 * caller already decoded, which is what the bridge's own tests use. The library is optional, so a
 * build falls back to the Java kernel when it cannot be loaded or does not speak this bridge's ABI
 * version; nothing here is a requirement for a build to succeed.
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
  static final int ABI_VERSION = 2;

  /** The bytes an entry payload spends on its value format version and its dimension. */
  private static final int ENTRY_PAYLOAD_HEADER_BYTES = 2 * Integer.BYTES;

  private static final int METRIC_L2 = 0;

  private static final int METRIC_COSINE = 1;

  private static final int METRIC_DOT = 2;

  private static final LoadState STATE = new LoadState();

  private NativeIvfBuild() {}

  /**
   * The outcome of one load: whether the library is usable, whether a load has already failed, and
   * whether that failure has been reported. A failure is kept so that a build does not extract and
   * load the library again for every call.
   */
  static final class LoadState {
    private final AtomicBoolean loaded = new AtomicBoolean(false);
    private final AtomicBoolean failed = new AtomicBoolean(false);
    private final AtomicBoolean reported = new AtomicBoolean(false);

    boolean loaded() {
      return loaded.get();
    }

    boolean failed() {
      return failed.get();
    }
  }

  /** One attempt to load the library; answers null on success, or why it failed. */
  @FunctionalInterface
  interface LibraryLoader {
    String load();
  }

  /** The entries a native build reads, one block at a time. */
  interface VectorChunkSource {

    /** The number of entries the build covers. */
    int count();

    /**
     * Writes {@code count} records starting at {@code start} from the buffer's index zero.
     *
     * <p>Each record is the data file index, the row position and the vector, all big-endian. The
     * vector is the entry's payload as written, version and dimension included, or its decoded
     * coordinates, as the parameters frame says. The native side reads the buffer from zero to its
     * capacity, so the position is left alone.
     */
    void fetch(int start, int count, ByteBuffer out);
  }

  /** The payload a native build wrote, and what the build spent. */
  static final class Built {
    private final ByteBuffer payload;
    private final int listCount;
    private final int sampleCount;
    private final int iterations;
    private final long entryCount;
    private final long nanosRead;
    private final long nanosTrain;
    private final long nanosAssign;
    private final long nanosEncode;

    private Built(
        ByteBuffer payload,
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

    ByteBuffer payload() {
      return payload.duplicate();
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
    return STATE.loaded();
  }

  /** Loads the library from an explicit path, which is what a test does before building. */
  static void load(Path library) {
    Preconditions.checkNotNull(library, "Native IVF library path is null");
    System.load(library.toAbsolutePath().toString());
    String failure = versionFailure("The native IVF library at " + library.toAbsolutePath());
    Preconditions.checkState(failure == null, "%s", failure);
    STATE.loaded.set(true);
  }

  /**
   * Loads the library a build uses, and reports whether a native build can run in this JVM.
   *
   * <p>The library is taken from the JVM's library path first, then from the resource this build
   * ships for the platform, so an installation that only has the jar still finds it. A library that
   * speaks another ABI version is treated as missing: a bridge and a kernel that disagree about a
   * frame must not run together. A JVM with neither keeps building with the Java kernel: the native
   * kernel is optional, and a build that asked for it must not fail because it is missing.
   */
  static boolean ensureLoaded() {
    return ensureLoaded(
        STATE, NativeIvfBuild::loadFromLibraryPath, NativeIvfBuild::loadFromResource);
  }

  /**
   * The load of one {@link LoadState}, with the two places a library can come from supplied by the
   * caller so a test can exercise a failure without a real library.
   */
  static boolean ensureLoaded(LoadState state, LibraryLoader libraryPath, LibraryLoader resource) {
    if (state.loaded.get()) {
      return true;
    }

    if (state.failed.get()) {
      return false;
    }

    synchronized (state) {
      if (state.loaded.get()) {
        return true;
      }

      if (state.failed.get()) {
        return false;
      }

      String libraryPathFailure = libraryPath.load();
      if (libraryPathFailure == null) {
        state.loaded.set(true);
        return true;
      }

      String resourceFailure = resource.load();
      if (resourceFailure == null) {
        state.loaded.set(true);
        return true;
      }

      // a JVM that cannot load the library cannot load it later either, and every build would
      // extract a copy and ask the loader again: remember the failure, and report it once
      state.failed.set(true);
      if (state.reported.compareAndSet(false, true)) {
        LOG.warn(
            "The native IVF build kernel is not available ({}; {}); builds that ask for it fall"
                + " back to the Java kernel. It needs a loadable {} on java.library.path, or one"
                + " packaged under {}<platform>/, both speaking ABI version {}",
            libraryPathFailure,
            resourceFailure,
            System.mapLibraryName(LIBRARY_NAME),
            LIBRARY_RESOURCE_PREFIX,
            ABI_VERSION);
      }

      return false;
    }
  }

  /** Loads the library from the library path, answering null on success. */
  private static String loadFromLibraryPath() {
    try {
      System.loadLibrary(LIBRARY_NAME);
    } catch (UnsatisfiedLinkError | SecurityException e) {
      return "no "
          + System.mapLibraryName(LIBRARY_NAME)
          + " on the library path: "
          + e.getMessage();
    }

    return versionFailure("The " + System.mapLibraryName(LIBRARY_NAME) + " on the library path");
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

      return extractAndLoad(in, resource, null);
    } catch (IOException e) {
      return "cannot extract " + resource + ": " + e.getMessage();
    }
  }

  /**
   * Extracts the library bytes and loads them, answering null on success or why the load failed.
   *
   * <p>The extracted file is a copy this call owns: on any failure it is deleted rather than left
   * for the JVM's exit, so a JVM that cannot load the library does not collect one copy per
   * attempt. A successful load keeps the file, which the platform keeps mapped.
   *
   * @param in the library bytes
   * @param description what the bytes are, for the failure message
   * @param directory where to write the copy, or null for the system temporary directory
   */
  static String extractAndLoad(InputStream in, String description, Path directory) {
    Path extracted = null;
    try {
      extracted =
          directory == null
              ? Files.createTempFile("iceberg-ivf-native-", librarySuffix())
              : Files.createTempFile(directory, "iceberg-ivf-native-", librarySuffix());
      extracted.toFile().deleteOnExit();
      Files.copy(in, extracted, StandardCopyOption.REPLACE_EXISTING);
      System.load(extracted.toAbsolutePath().toString());
      String failure = versionFailure("The packaged " + description);
      if (failure != null) {
        deleteExtracted(extracted);
      }

      return failure;
    } catch (IOException e) {
      deleteExtracted(extracted);
      return "cannot extract " + description + ": " + e.getMessage();
    } catch (UnsatisfiedLinkError | SecurityException e) {
      deleteExtracted(extracted);
      return "cannot load " + description + ": " + e.getMessage();
    }
  }

  /** Deletes the copy this call extracted, which no load kept mapped. */
  private static void deleteExtracted(Path extracted) {
    if (extracted == null) {
      return;
    }

    try {
      Files.deleteIfExists(extracted);
    } catch (IOException e) {
      LOG.warn("Cannot delete the extracted native IVF library {}", extracted, e);
    }
  }

  /**
   * The reason a loaded library cannot serve this bridge, or null when its ABI version matches.
   *
   * <p>An old library answers its own version, which is not the one this bridge writes and reads:
   * building with it would fail on the first frame rather than fall back, so it is reported here.
   */
  private static String versionFailure(String library) {
    int reported;
    try {
      reported = nativeVersion();
    } catch (UnsatisfiedLinkError e) {
      return library + " cannot report its native IVF ABI version: " + e.getMessage();
    }

    return versionMismatch(reported, library);
  }

  /** The reason a library reporting {@code reported} cannot serve this bridge, or null. */
  static String versionMismatch(int reported, String library) {
    if (reported == ABI_VERSION) {
      return null;
    }

    return library
        + " speaks native IVF ABI version "
        + reported
        + "; this bridge reads "
        + ABI_VERSION;
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
            false,
            threads);

    return openAndBuild(params, new EntrySource(entries, fileIndexes, dimension));
  }

  /**
   * Builds the payload of vectors that the caller has already decoded.
   *
   * <p>The Java kernel hands over the vectors it decoded, and the native kernel reads those instead
   * of decoding the payloads a second time. The data file indexes are the positions of the entries'
   * files in {@code dataFiles}, so the dictionary the payload writes is the one the Java kernel
   * writes. A production build takes {@link #buildFromEntries} instead, which never decodes.
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
            false,
            threads);

    return openAndBuild(params, new VectorSource(vectors, fileIndexes, rowPositions, dimension));
  }

  /**
   * Builds the payload of entries whose vectors are still encoded, which is what a build takes.
   *
   * <p>Each entry's payload travels to the native kernel as it was written, so the vectors are
   * never decoded on the Java side and its heap does not hold them. The native side checks the
   * version and dimension of every payload it reads. The data file indexes are the positions of the
   * entries' files in the unit's dictionary, so the dictionary the payload writes is the one the
   * Java kernel writes.
   */
  static Built buildFromEntries(IvfKernel kernel, List<IndexEntry> entries, int parallelism) {
    Preconditions.checkNotNull(kernel, "IVF kernel is null");
    Preconditions.checkNotNull(entries, "Index entries are null");
    Preconditions.checkArgument(!entries.isEmpty(), "A native IVF build needs at least one entry");
    Preconditions.checkArgument(
        kernel.scoring().metric() == VectorIndexDefinition.Metric.L2,
        "The native IVF build kernel supports the L2 metric only: %s",
        kernel.scoring().metric());

    int dimension = kernel.dimension();
    int entryCount = entries.size();
    List<String> dataFiles = VectorPayloads.dataFilesOf(entries);
    int[] fileIndexes = fileIndexes(entries, dataFiles);

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
            true,
            threads);

    return openAndBuild(params, new EntryPayloadSource(entries, fileIndexes, dimension));
  }

  /** The bytes one entry payload takes: its version and dimension, then the coordinates. */
  private static int entryPayloadBytes(int dimension) {
    return ENTRY_PAYLOAD_HEADER_BYTES + dimension * Float.BYTES;
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
      boolean rawEntryPayloads,
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
      out.writeBoolean(rawEntryPayloads);
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
      String mismatch = versionMismatch(version, "The native IVF build result frame");
      Preconditions.checkState(mismatch == null, "%s", mismatch);

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
      Preconditions.checkState(
          in.available() == payloadBytes,
          "The native IVF build returned %s bytes after its counters, the frame says %s",
          in.available(),
          payloadBytes);
      // the payload is the last part of the frame, and it is used from there rather than copied
      // off it: a payload of hundreds of megabytes is not something to hold twice while it is
      // written to the index file
      int payloadStart = frame.length - payloadBytes;

      return new Built(
          ByteBuffer.wrap(frame, payloadStart, payloadBytes).slice().asReadOnlyBuffer(),
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
      checkFetchBuffer(out, count, dimension, false);
      int recordBytes = recordBytes(dimension, false);
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

  /** Encoded entry payloads, served to the native kernel a block at a time. */
  private static final class EntryPayloadSource implements VectorChunkSource {
    private final List<IndexEntry> entries;
    private final int[] fileIndexes;
    private final int dimension;

    private EntryPayloadSource(List<IndexEntry> entries, int[] fileIndexes, int dimension) {
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
      checkFetchBuffer(out, count, dimension, true);
      int recordBytes = recordBytes(dimension, true);
      int payloadBytes = entryPayloadBytes(dimension);
      for (int i = 0; i < count; i++) {
        int row = start + i;
        int offset = i * recordBytes;
        // the payload carries its own version and dimension, which the native side checks; only
        // its length is checked here, where a record is copied as a whole
        ByteBuffer payload = entries.get(row).payloadView();
        Preconditions.checkArgument(
            payload.remaining() == payloadBytes,
            "Native IVF entry %s carries a %s-byte payload; %s dimensions need %s bytes",
            row,
            payload.remaining(),
            dimension,
            payloadBytes);
        out.putInt(offset, fileIndexes[row]);
        out.putLong(offset + Integer.BYTES, entries.get(row).rowPosition());
        out.position(offset + Integer.BYTES + Long.BYTES);
        out.put(payload);
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
      checkFetchBuffer(out, count, dimension, false);
      int recordBytes = recordBytes(dimension, false);
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
  private static void checkFetchBuffer(
      ByteBuffer out, int count, int dimension, boolean rawEntryPayloads) {
    Preconditions.checkNotNull(out, "Native IVF vector buffer is null");
    Preconditions.checkArgument(out.isDirect(), "Native IVF vector buffer is not direct");
    long needed = (long) count * recordBytes(dimension, rawEntryPayloads);
    Preconditions.checkArgument(
        out.capacity() >= needed,
        "Native IVF vector buffer holds %s bytes, %s records need %s",
        out.capacity(),
        count,
        needed);
  }

  /**
   * The bytes one record takes: the data file index, the row position and the vector, which is the
   * entry payload when the parameters frame asks for raw values.
   */
  private static int recordBytes(int dimension, boolean rawEntryPayloads) {
    return Integer.BYTES
        + Long.BYTES
        + (rawEntryPayloads ? ENTRY_PAYLOAD_HEADER_BYTES : 0)
        + dimension * Float.BYTES;
  }
}
