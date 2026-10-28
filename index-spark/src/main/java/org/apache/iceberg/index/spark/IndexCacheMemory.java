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

import com.sun.management.HotSpotDiagnosticMXBean;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
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

  private record ContextKey(Class<?> adapterType, String definition, String vm, int processors) {}

  private record EstimateKey(
      ContextKey context,
      String location,
      IndexFile.Kind kind,
      String blobType,
      long entries,
      IndexFileMemory declared,
      int model,
      String reader,
      String profile) {}

  record ChargeProjection(
      MetadataState afterState,
      long usedBytes,
      long residentBytes,
      long controlBytes,
      long reservedBytes,
      long incrementBytes) {
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

  /** Re-estimation never pairs a current charge with the old manifest's model or profile. */
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

      // Initial migration deliberately reads and prepares the complete file. Matching metadata
      // is not yet a substitute for validating the representation this runtime actually holds.
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

  static boolean supportedLayout() {
    try {
      HotSpotDiagnosticMXBean bean =
          ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class);
      return bean != null
          && "17".equals(System.getProperty("java.specification.version"))
          && "64".equals(System.getProperty("sun.arch.data.model"))
          && "true".equals(bean.getVMOption("UseCompressedOops").getValue())
          && "true".equals(bean.getVMOption("UseCompressedClassPointers").getValue())
          && "8".equals(bean.getVMOption("ObjectAlignmentInBytes").getValue())
          && "true".equals(bean.getVMOption("CompactStrings").getValue());
    } catch (RuntimeException | LinkageError e) {
      return false;
    }
  }

  static boolean compatible(IndexFileMemory memory, IndexAdapter adapter) {
    if (!supportedLayout() || memory == null || memory.modelVersion() != 2) {
      return false;
    }
    try {
      IndexFileMemory descriptor = descriptor(adapter, contextKey(adapter));
      return descriptor != null
          && memory.modelVersion() == descriptor.modelVersion()
          && memory.readerId().equals(descriptor.readerId())
          && memory.runtimeProfile().equals(descriptor.runtimeProfile());
    } catch (RuntimeException e) {
      return false;
    }
  }

  static String runtimeIdentity() {
    return "vm="
        + System.getProperty("java.vm.name")
        + ";version="
        + System.getProperty("java.vm.version")
        + ";processors="
        + Runtime.getRuntime().availableProcessors();
  }

  static String readerKey(IndexAdapter adapter) {
    try {
      ContextKey context = contextKey(adapter);
      IndexFileMemory descriptor = descriptor(adapter, context);
      String identity = context.definition() + "/" + context.vm() + "/" + context.processors();
      if (descriptor != null) {
        identity +=
            "/"
                + descriptor.modelVersion()
                + "/"
                + descriptor.readerId()
                + "/"
                + descriptor.runtimeProfile();
      }
      return HexFormat.of()
          .formatHex(
              MessageDigest.getInstance("SHA-256")
                  .digest(identity.getBytes(StandardCharsets.UTF_8)));
    } catch (RuntimeException | NoSuchAlgorithmException e) {
      // Unidentifiable legacy readers cannot share a prepared state across adapter instances.
      return "unknown-" + Integer.toHexString(System.identityHashCode(adapter));
    }
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
