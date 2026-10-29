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

import com.github.jelmerk.knn.Item;
import com.github.jelmerk.knn.hnsw.HnswIndex;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.ReentrantLock;
import org.eclipse.collections.impl.list.mutable.primitive.IntArrayList;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectIntHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectLongHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bounded structural receipt for hnswlib-core 1.1.0 and its Eclipse primitive containers. Only
 * library fields are inspected; the few JDK containers have explicit HotSpot 17 models. An
 * unrecognized representation is unknown, not the old average-level approximation.
 */
final class HnswStructureSize {
  private static final Logger LOG = LoggerFactory.getLogger(HnswStructureSize.class);
  private static final Set<String> LIBRARY_TYPES =
      Set.of(
          "com.github.jelmerk.knn.hnsw.HnswIndex",
          "com.github.jelmerk.knn.hnsw.HnswIndex$Node",
          "com.github.jelmerk.knn.hnsw.HnswIndex$ExactView",
          "com.github.jelmerk.knn.hnsw.HnswIndex$MaxValueComparator",
          "com.github.jelmerk.knn.JavaObjectSerializer",
          "com.github.jelmerk.knn.DistanceFunctions$FloatEuclideanDistance",
          "com.github.jelmerk.knn.DistanceFunctions$FloatCosineDistance",
          "com.github.jelmerk.knn.DistanceFunctions$FloatInnerProduct",
          "com.github.jelmerk.knn.util.GenericObjectPool",
          "com.github.jelmerk.knn.util.ArrayBitSet",
          "org.eclipse.collections.impl.map.mutable.primitive.ObjectIntHashMap",
          "org.eclipse.collections.impl.map.mutable.primitive.ObjectLongHashMap",
          "org.eclipse.collections.impl.list.mutable.primitive.IntArrayList");

  private static final ClassValue<List<Field>> FIELDS =
      new ClassValue<>() {
        @Override
        protected List<Field> computeValue(Class<?> type) {
          if (!LIBRARY_TYPES.contains(type.getName())) {
            throw new IllegalArgumentException("Unsupported HNSW representation: " + type);
          }
          List<Field> fields = new ArrayList<>();
          for (Class<?> current = type;
              current != Object.class;
              current = current.getSuperclass()) {
            for (Field field : current.getDeclaredFields()) {
              if (!Modifier.isStatic(field.getModifiers())) {
                if (!field.trySetAccessible()) {
                  throw new IllegalArgumentException("Cannot inspect HNSW field: " + field);
                }
                fields.add(field);
              }
            }
          }
          return List.copyOf(fields);
        }
      };

  private static final String LIBRARY_PROFILE = libraryProfile();

  private static String libraryProfile() {
    try {
      String hnsw =
          version(
              HnswIndex.class, "/META-INF/maven/com.github.jelmerk/hnswlib-core/pom.properties");
      String eclipse =
          version(
              ObjectIntHashMap.class,
              "/META-INF/maven/org.eclipse.collections/eclipse-collections/pom.properties");
      if (!"1.1.0".equals(hnsw) || !("9.2.0".equals(eclipse) || "11.1.0".equals(eclipse))) {
        return null;
      }
      return "hnswlib=" + hnsw + ";eclipse-collections=" + eclipse;
    } catch (IOException | RuntimeException e) {
      return null;
    }
  }

  private static String version(Class<?> type, String resource) throws IOException {
    try (InputStream stream = type.getResourceAsStream(resource)) {
      if (stream == null) {
        return null;
      }
      Properties properties = new Properties();
      properties.load(stream);
      return properties.getProperty("version");
    }
  }

  static String profile() {
    return LIBRARY_PROFILE;
  }

  private final PreparedStructureSize.Receipt receipt = new PreparedStructureSize.Receipt();
  private final boolean building;
  private final int m;

  private HnswStructureSize(boolean building, int m) {
    this.building = building;
    this.m = m;
  }

  static long bytes(HnswIndex<?, ?, ?, ?> index, boolean building) {
    if (!VectorMemoryEstimates.supportedLayout() || LIBRARY_PROFILE == null) {
      return 0;
    }
    try {
      return new HnswStructureSize(building, index.getM()).library(index);
    } catch (ReflectiveOperationException | RuntimeException | LinkageError e) {
      LOG.warn("HNSW prepared memory is unknown: unsupported library representation", e);
      return 0;
    }
  }

  private long library(Object value) throws ReflectiveOperationException {
    List<Field> fields = FIELDS.get(value.getClass());
    long ownFields = 0;
    for (Field field : fields) {
      ownFields += width(field.getType());
    }
    long bytes = receipt.object(value, ownFields);
    if (bytes == 0) {
      return 0;
    }
    for (Field field : fields) {
      if (!field.getType().isPrimitive()) {
        Object child = field.get(value);
        if (value instanceof HnswIndex<?, ?, ?, ?>) {
          // save/readObject rebuilds these two maps with initial capacity size(), and discards
          // insertion locks. Model that loaded representation without loading a second graph.
          if (building && field.getName().equals("lookup")) {
            ObjectIntHashMap<?> source = (ObjectIntHashMap<?>) child;
            ObjectIntHashMap<Object> loaded = new ObjectIntHashMap<>(source.size());
            source.forEachKeyValue(loaded::put);
            child = loaded;
          } else if (building && field.getName().equals("deletedItemVersions")) {
            ObjectLongHashMap<?> source = (ObjectLongHashMap<?>) child;
            ObjectLongHashMap<Object> loaded = new ObjectLongHashMap<>(source.size());
            source.forEachKeyValue(loaded::put);
            child = loaded;
          } else if (building && field.getName().equals("locks")) {
            child = new HashMap<>();
          }
        }
        if (building && field.getName().equals("connections")) {
          // The writer serializes size and contents, not capacity. The reader allocates 2M at
          // layer zero and M at each real upper layer, then adds those serialized contents.
          Object[] source = (Object[]) child;
          IntArrayList[] loaded = new IntArrayList[source.length];
          for (int layer = 0; layer < source.length; layer++) {
            IntArrayList contents = (IntArrayList) source[layer];
            loaded[layer] = new IntArrayList(layer == 0 ? Math.multiplyExact(2, m) : m);
            contents.forEach(loaded[layer]::add);
          }
          child = loaded;
        }
        bytes += retained(child);
      }
    }
    return bytes;
  }

  private long retained(Object value) throws ReflectiveOperationException {
    if (value == null || value instanceof Enum<?>) {
      return 0;
    }
    if (value.getClass().isArray()) {
      long bytes = receipt.array(value);
      if (bytes != 0 && value instanceof Object[] array) {
        for (Object child : array) {
          bytes += retained(child);
        }
      }
      return bytes;
    }
    if (value instanceof Long) {
      return receipt.object(value, Long.BYTES);
    }
    if (value instanceof Item<?, ?>) {
      // This kernel's VectorItem has a primitive long ID and a single owned vector reference.
      if (!value.getClass().getName().equals(HnswlibKernel.class.getName() + "$VectorItem")) {
        throw new IllegalArgumentException("Unsupported HNSW item: " + value.getClass());
      }
      long bytes = receipt.object(value, Long.BYTES + PreparedStructureSize.REFERENCE_BYTES);
      return bytes == 0 ? 0 : bytes + retained(((Item<?, ?>) value).vector());
    }
    if (value instanceof AtomicReferenceArray<?>) {
      AtomicReferenceArray<?> nodes = (AtomicReferenceArray<?>) value;
      long bytes = receipt.object(value, PreparedStructureSize.REFERENCE_BYTES);
      if (bytes != 0) {
        bytes += PreparedStructureSize.references(nodes.length());
        for (int i = 0; i < nodes.length(); i++) {
          bytes += retained(nodes.get(i));
        }
      }
      return bytes;
    }
    if (value instanceof ArrayBlockingQueue<?>) {
      ArrayBlockingQueue<?> queue = (ArrayBlockingQueue<?>) value;
      long bytes =
          receipt.object(value, 3L * Integer.BYTES + 5L * PreparedStructureSize.REFERENCE_BYTES);
      if (bytes != 0) {
        bytes += PreparedStructureSize.references(queue.size() + queue.remainingCapacity());
        bytes += lockBytes() + 2L * conditionBytes();
        // toArray does not create the queue's optional iterator-tracking state or borrow bitsets.
        for (Object item : queue.toArray()) {
          bytes += retained(item);
        }
      }
      return bytes;
    }
    if (value instanceof ReentrantLock) {
      return receipt.object(value, PreparedStructureSize.REFERENCE_BYTES) == 0 ? 0 : lockBytes();
    }
    if (value.getClass() == HashMap.class && ((Map<?, ?>) value).isEmpty()) {
      // A freshly loaded graph has a never-populated HashMap, hence no table allocation.
      return receipt.object(value, 4L * PreparedStructureSize.REFERENCE_BYTES + 4L * Integer.BYTES);
    }
    return library(value);
  }

  private static long lockBytes() {
    // ReentrantLock plus NonfairSync: AQS state, owner, head and tail; no waiting threads at
    // prepare.
    return PreparedStructureSize.object(PreparedStructureSize.REFERENCE_BYTES)
        + PreparedStructureSize.object(Integer.BYTES + 3L * PreparedStructureSize.REFERENCE_BYTES);
  }

  private static long conditionBytes() {
    return PreparedStructureSize.object(3L * PreparedStructureSize.REFERENCE_BYTES);
  }

  private static int width(Class<?> type) {
    return !type.isPrimitive()
        ? PreparedStructureSize.REFERENCE_BYTES
        : type == boolean.class || type == byte.class
            ? 1
            : type == short.class || type == char.class
                ? 2
                : type == long.class || type == double.class ? 8 : 4;
  }
}
