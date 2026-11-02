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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iceberg.index.IndexAdapter;
import org.apache.iceberg.index.IndexBuildResult;
import org.apache.iceberg.index.IndexFile;
import org.apache.iceberg.index.IndexFileMemory;
import org.apache.iceberg.index.spark.IndexCacheMetadata.Component;
import org.apache.iceberg.index.spark.IndexCacheMetadata.Components;
import org.apache.iceberg.index.spark.IndexCacheMetadata.MetadataState;
import org.apache.iceberg.index.spark.IndexCacheMetadata.Mutation;
import org.apache.iceberg.index.spark.IndexCacheMetadata.Texts;
import org.apache.iceberg.index.vector.JvmLayout;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Converts prepared-state estimates to a cache-entry charge under the current memory model. */
final class IndexCacheMemory {
  // Fixed wrapper allowance: CachedFile (64), ReentrantLock (24), synchronizer (32),
  // and an owned SHA-256 context receipt byte[32] (48). Waiters are query scratch.
  // This wrapper bound alone does not include the cache maps and ownership associations.
  private static final long ENTRY_BYTES = 168;
  private static final int MAX_ESTIMATES = 100_000;
  private static final Logger LOG = LoggerFactory.getLogger(IndexCacheMemory.class);
  private static final Map<ContextKey, IndexFileMemory> DESCRIPTORS = new ConcurrentHashMap<>();
  private static final Map<EstimateKey, IndexFileMemory> ESTIMATES = new ConcurrentHashMap<>();

  private static final class ContextKey {
    private final Class<?> adapterType;
    private final String definition;
    private final String vm;
    private final int processors;

    private ContextKey(Class<?> adapterType, String definition, String vm, int processors) {
      this.adapterType = adapterType;
      this.definition = definition;
      this.vm = vm;
      this.processors = processors;
    }

    Class<?> adapterType() {
      return adapterType;
    }

    String definition() {
      return definition;
    }

    String vm() {
      return vm;
    }

    int processors() {
      return processors;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof ContextKey)) {
        return false;
      }
      ContextKey that = (ContextKey) other;
      return processors == that.processors
          && Objects.equals(adapterType, that.adapterType)
          && Objects.equals(definition, that.definition)
          && Objects.equals(vm, that.vm);
    }

    @Override
    public int hashCode() {
      return Objects.hash(adapterType, definition, vm, processors);
    }
  }

  private static final class EstimateKey {
    private final ContextKey context;
    private final String location;
    private final IndexFile.Kind kind;
    private final String blobType;
    private final long entries;
    private final IndexFileMemory declared;
    private final int model;
    private final String reader;
    private final String profile;

    private EstimateKey(
        ContextKey context,
        String location,
        IndexFile.Kind kind,
        String blobType,
        long entries,
        IndexFileMemory declared,
        int model,
        String reader,
        String profile) {
      this.context = context;
      this.location = location;
      this.kind = kind;
      this.blobType = blobType;
      this.entries = entries;
      this.declared = declared;
      this.model = model;
      this.reader = reader;
      this.profile = profile;
    }

    ContextKey context() {
      return context;
    }

    String location() {
      return location;
    }

    IndexFile.Kind kind() {
      return kind;
    }

    String blobType() {
      return blobType;
    }

    long entries() {
      return entries;
    }

    IndexFileMemory declared() {
      return declared;
    }

    int model() {
      return model;
    }

    String reader() {
      return reader;
    }

    String profile() {
      return profile;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof EstimateKey)) {
        return false;
      }
      EstimateKey that = (EstimateKey) other;
      return entries == that.entries
          && model == that.model
          && Objects.equals(context, that.context)
          && Objects.equals(location, that.location)
          && Objects.equals(kind, that.kind)
          && Objects.equals(blobType, that.blobType)
          && Objects.equals(declared, that.declared)
          && Objects.equals(reader, that.reader)
          && Objects.equals(profile, that.profile);
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          context, location, kind, blobType, entries, declared, model, reader, profile);
    }
  }

  static final class ChargeProjection {
    private final MetadataState afterState;
    private final long usedBytes;
    private final long residentBytes;
    private final long controlBytes;
    private final long reservedBytes;
    private final long incrementBytes;

    ChargeProjection(
        MetadataState afterState,
        long usedBytes,
        long residentBytes,
        long controlBytes,
        long reservedBytes,
        long incrementBytes) {
      this.afterState = afterState;
      this.usedBytes = usedBytes;
      this.residentBytes = residentBytes;
      this.controlBytes = controlBytes;
      this.reservedBytes = reservedBytes;
      this.incrementBytes = incrementBytes;
    }

    MetadataState afterState() {
      return afterState;
    }

    long usedBytes() {
      return usedBytes;
    }

    long residentBytes() {
      return residentBytes;
    }

    long controlBytes() {
      return controlBytes;
    }

    long reservedBytes() {
      return reservedBytes;
    }

    long incrementBytes() {
      return incrementBytes;
    }

    long effectiveBytes() {
      return Math.addExact(usedBytes, reservedBytes);
    }
  }

  /**
   * Projects a mutation without publishing nodes, growing the old pool, or retaining dead texts.
   */
  static ChargeProjection project(MetadataState before, Mutation mutation) {
    Components previous = components(before);
    MetadataState mutated = mutation.apply(before);
    Texts ownedTexts = mutated.texts().copy();
    MetadataState after = mutated.withTexts(ownedTexts);
    Components live = new Components();
    for (Component root : after.residentRoots()) {
      live.root(root);
    }
    for (Component root : after.controlRoots()) {
      live.root(root);
    }
    ownedTexts.retain(live);
    Components components = new Components();
    for (Component root : after.residentRoots()) {
      components.root(root);
    }
    ownedTexts.chargeResident(components);
    long resident = components.bytes();
    for (Component root : after.controlRoots()) {
      components.root(root);
    }
    ownedTexts.charge(components);
    long used = components.bytes();
    return new ChargeProjection(
        after,
        used,
        resident,
        used - resident,
        after.reservedBytes(),
        Math.max(0, used - previous.bytes()));
  }

  private static Components components(MetadataState state) {
    Components components = new Components();
    for (Component root : state.residentRoots()) {
      components.root(root);
    }
    for (Component root : state.controlRoots()) {
      components.root(root);
    }
    state.texts().charge(components);
    return components;
  }

  private IndexCacheMemory() {}

  /** Container and encoded payload lengths are diagnostic, not additional retained buffers. */
  static long residentBytes(IndexFileMemory memory) {
    Preconditions.checkArgument(supportedLayout(), "Index cache wrapper layout is unsupported");
    Preconditions.checkArgument(memory != null, "Index file memory is unknown");
    Preconditions.checkArgument(
        memory.modelVersion() == 2, "Unsupported index memory model: %s", memory.modelVersion());
    Preconditions.checkArgument(memory.preparedBytes() > 0, "Index prepared memory is unknown");
    return Math.addExact(memory.preparedBytes(), ENTRY_BYTES);
  }

  /** Resolves a file under this reader and runtime, or leaves its capacity explicitly unknown. */
  static Long resolve(FileIO io, IndexFile file, IndexAdapter adapter) {
    IndexFileMemory memory = resolveMemory(io, file, adapter);
    return memory == null ? null : residentBytes(memory);
  }

  /**
   * Resolves the retained-memory estimate of one file under this reader and runtime.
   *
   * <p>A manifest record whose model, reader and runtime already match this runtime is trusted
   * without reading the file. An old, missing or mismatched record is re-estimated by reading and
   * preparing the complete file, and a failure leaves the estimate explicitly unknown rather than
   * silently free.
   */
  static IndexFileMemory resolveMemory(FileIO io, IndexFile file, IndexAdapter adapter) {
    if (!supportedLayout()) {
      return null;
    }
    try {
      ContextKey context = contextKey(adapter);
      IndexFileMemory descriptor = descriptor(adapter, context);
      if (descriptor == null || descriptor.modelVersion() != 2 || descriptor.preparedBytes() <= 0) {
        return null;
      }

      // A matching manifest record already describes this exact representation, so it is trusted
      // without a read. Old, missing or mismatched records fall through to the re-estimate below.
      IndexFileMemory declared = file.memory();
      if (declared != null && declared.preparedBytes() > 0 && compatible(declared, descriptor)) {
        return declared;
      }

      EstimateKey key =
          new EstimateKey(
              context,
              file.location(),
              file.kind(),
              file.blobType(),
              file.entryCount(),
              file.memory(),
              descriptor.modelVersion(),
              descriptor.readerId(),
              descriptor.runtimeProfile());
      IndexFileMemory known = ESTIMATES.get(key);
      if (known != null) {
        return known;
      }

      // A record that does not match this reader or runtime is re-estimated by reading and
      // preparing the complete file; the full validation stays intact here and on the executor.
      DistributedIndexSearch.LoadedFile loaded =
          DistributedIndexSearch.loadFileState(io, file.location(), adapter);
      if (loaded.preparedBytes() <= 0) {
        return null;
      }

      IndexFileMemory memory =
          IndexFileMemory.builder()
              .fileBytes(loaded.fileBytes())
              .payloadBytes(loaded.payloadBytes())
              .preparedBytes(loaded.preparedBytes())
              .modelVersion(descriptor.modelVersion())
              .readerId(descriptor.readerId())
              .runtimeProfile(descriptor.runtimeProfile())
              .build();
      residentBytes(memory);
      if (ESTIMATES.size() < MAX_ESTIMATES) {
        ESTIMATES.putIfAbsent(key, memory);
      }
      return memory;
    } catch (RuntimeException e) {
      LOG.debug("Index capacity is unknown for {} under the current reader", file.location(), e);
      return null;
    }
  }

  /** Uses the existing codec SPI rather than adding a public memory-model descriptor interface. */
  private static IndexFileMemory descriptor(IndexAdapter adapter, ContextKey context) {
    IndexFileMemory known = DESCRIPTORS.get(context);
    if (known != null) {
      return known;
    }

    IndexBuildResult built =
        adapter.writesAuxiliaryFiles()
            ? adapter.encodeAuxiliaryPayloadWithMemory(List.of(), 1)
            : adapter.encodeEntriesWithMemory(List.of());
    IndexFileMemory memory = built.memory();
    if (memory != null && memory.modelVersion() == 2 && DESCRIPTORS.size() < MAX_ESTIMATES) {
      DESCRIPTORS.putIfAbsent(context, memory);
    }
    return memory;
  }

  static IndexFileMemory currentDescriptor(IndexAdapter adapter) {
    if (!supportedLayout()) {
      return null;
    }
    try {
      IndexFileMemory memory = descriptor(adapter, contextKey(adapter));
      return memory != null && memory.modelVersion() == 2 && memory.preparedBytes() > 0
          ? memory
          : null;
    } catch (RuntimeException e) {
      return null;
    }
  }

  static byte[] contextFingerprint(IndexAdapter adapter) {
    if (currentDescriptor(adapter) == null) {
      return null;
    }
    try {
      return HexFormat.of().parseHex(readerKey(adapter));
    } catch (RuntimeException e) {
      return null;
    }
  }

  /**
   * Whether this JVM's object layout is the one the cache's charge model describes.
   *
   * <p>The probe lives with the structural estimates it shares those facts with, so the two modules
   * cannot disagree about which JVMs are supported, and it is decided once per JVM rather than read
   * from an MXBean on every estimate.
   */
  static boolean supportedLayout() {
    return JvmLayout.supportedOrWarn("Index cache capacity accounting");
  }

  static boolean compatible(IndexFileMemory memory, IndexAdapter adapter) {
    if (!supportedLayout() || memory == null || memory.modelVersion() != 2) {
      return false;
    }
    try {
      return compatible(memory, descriptor(adapter, contextKey(adapter)));
    } catch (RuntimeException e) {
      return false;
    }
  }

  /**
   * Whether a record describes the same model and reader as this runtime, ignoring which JVM wrote
   * the estimate. A record from another runtime is still loadable: the resolver re-estimates it
   * here, so refusing it outright would make an index built by one JVM unreadable by another.
   */
  static boolean compatibleModel(IndexFileMemory memory, IndexAdapter adapter) {
    if (memory == null || !supportedLayout() || memory.modelVersion() != 2) {
      return false;
    }

    try {
      IndexFileMemory descriptor = descriptor(adapter, contextKey(adapter));
      return descriptor != null
          && descriptor.modelVersion() == memory.modelVersion()
          && descriptor.readerId().equals(memory.readerId());
    } catch (RuntimeException e) {
      return false;
    }
  }

  /**
   * Whether one memory record describes the same model, reader and runtime as another. A copy that
   * is already resident is reused only under the profile it was prepared with.
   */
  private static boolean compatible(IndexFileMemory memory, IndexFileMemory descriptor) {
    return memory != null
        && descriptor != null
        && memory.modelVersion() == descriptor.modelVersion()
        && memory.readerId().equals(descriptor.readerId())
        && memory.runtimeProfile().equals(descriptor.runtimeProfile());
  }

  static String runtimeIdentity() {
    return "vm="
        + System.getProperty("java.vm.name")
        + ";version="
        + System.getProperty("java.vm.version")
        + ";processors="
        + Runtime.getRuntime().availableProcessors();
  }

  /**
   * Whether a shape stated under {@code runtime} can be projected here.
   *
   * <p>Only the JVM family decides that. The processor count is not part of it: it decides which
   * prepared state may be shared between callers (see the adapter context key), not how an index
   * file is laid out, and a driver plans for executors whose parallelism it does not share.
   */
  static boolean compatibleRuntime(String runtime) {
    return compatibleRuntime(runtime, runtimeIdentity());
  }

  private static boolean compatibleRuntime(String left, String right) {
    return left != null && right != null && layoutIdentity(left).equals(layoutIdentity(right));
  }

  /** The part of a runtime identity that decides memory layout. */
  private static String layoutIdentity(String runtime) {
    int processors = runtime.indexOf(";processors=");
    return processors < 0 ? runtime : runtime.substring(0, processors);
  }

  static String readerKey(IndexAdapter adapter) {
    try {
      ContextKey context = contextKey(adapter);
      return readerIdentity(context.definition(), context.vm(), descriptor(adapter, context));
    } catch (RuntimeException | NoSuchAlgorithmException e) {
      // Unidentifiable legacy readers cannot share a prepared state across adapter instances.
      return "unknown-" + Integer.toHexString(System.identityHashCode(adapter));
    }
  }

  /**
   * What a prepared reader depends on: the definition, the VM, and how the file was measured.
   *
   * <p>The measuring host's processor count is left out for the same reason it does not decide
   * compatibility. The driver builds this key when it plans a load and every executor builds it
   * again for its own cache, so a key that followed the count would name one reader two ways and
   * the copy the driver planned would not be the copy a query looks up.
   */
  static String readerIdentity(String definition, String vm, IndexFileMemory descriptor)
      throws NoSuchAlgorithmException {
    String identity = definition + "/" + vm;
    if (descriptor != null) {
      identity +=
          "/"
              + descriptor.modelVersion()
              + "/"
              + descriptor.readerId()
              + "/"
              + layoutIdentity(descriptor.runtimeProfile());
    }

    return HexFormat.of()
        .formatHex(
            MessageDigest.getInstance("SHA-256").digest(identity.getBytes(StandardCharsets.UTF_8)));
  }

  private static ContextKey contextKey(IndexAdapter adapter) {
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
        out.writeObject(adapter.definition());
      }
      String definition =
          HexFormat.of()
              .formatHex(MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray()));
      return new ContextKey(
          adapter.getClass(),
          definition,
          System.getProperty("java.vm.name") + "/" + System.getProperty("java.vm.version"),
          Runtime.getRuntime().availableProcessors());
    } catch (IOException e) {
      throw new UncheckedIOException("Cannot identify the index definition for capacity", e);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is unavailable", e);
    }
  }

  /** A missing prepared estimate retains the explicitly inexact legacy sizing fallback. */
  static long residentBytes(long inputBytes, long preparedBytes) {
    return preparedBytes > 0 && supportedLayout()
        ? Math.addExact(preparedBytes, ENTRY_BYTES)
        : Math.multiplyExact(inputBytes, 2);
  }
}
