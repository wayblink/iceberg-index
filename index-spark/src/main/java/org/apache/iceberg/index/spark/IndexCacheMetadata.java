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

import java.io.Serializable;
import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;

/** Owned, capacity-visible metadata under the cache's supported HotSpot layout. */
final class IndexCacheMetadata {
  private IndexCacheMetadata() {}

  static final class MetadataState {
    private final List<Component> residentRoots;
    private final List<Component> controlRoots;
    private final Texts texts;
    private final long reservedBytes;

    MetadataState(
        List<Component> residentRoots,
        List<Component> controlRoots,
        Texts texts,
        long reservedBytes) {
      this.residentRoots = residentRoots;
      this.controlRoots = controlRoots;
      this.texts = texts;
      this.reservedBytes = reservedBytes;
    }

    List<Component> residentRoots() {
      return residentRoots;
    }

    List<Component> controlRoots() {
      return controlRoots;
    }

    Texts texts() {
      return texts;
    }

    long reservedBytes() {
      return reservedBytes;
    }

    MetadataState withTexts(Texts ownedTexts) {
      return new MetadataState(residentRoots, controlRoots, ownedTexts, reservedBytes);
    }
  }

  @FunctionalInterface
  interface Mutation {
    MetadataState apply(MetadataState before);
  }

  interface Component {
    void charge(Components components);
  }

  /** A value canonicalizer. Its keys and values refer to the same owned string and backing. */
  static final class Texts implements Serializable {
    private OwnedMap<String, String> values = new OwnedMap<>();

    Texts copy() {
      Texts copy = new Texts();
      copy.values = new OwnedMap<>(values);
      return copy;
    }

    void replace(Texts after) {
      this.values = after.values;
    }

    String own(String value) {
      String existing = values.get(value);
      if (existing != null) {
        return existing;
      }
      // Unlike String(String), this constructor creates a new compact backing array.
      String owned = new String(value.toCharArray());
      values.put(owned, owned);
      return owned;
    }

    void retain(Components components) {
      Iterator<String> keys = values.keySet().iterator();
      while (keys.hasNext()) {
        if (!components.contains(keys.next())) {
          keys.remove();
        }
      }
    }

    void chargeResident(Components components) {
      Set<String> resident = new HashSet<>();
      for (String key : values.keySet()) {
        if (components.contains(key)) {
          resident.add(key);
        }
      }
      values.chargeEntries(components, resident);
    }

    void charge(Components components) {
      if (!values.isEmpty()) {
        values.chargeAll(components);
      }
    }
  }

  /** Explicitly visits only this module's metadata shapes; never inspects arbitrary heap roots. */
  static final class Components {
    private final IdentityHashMap<Object, Boolean> seen = new IdentityHashMap<>();
    private long bytes;

    long bytes() {
      return bytes;
    }

    boolean contains(Object object) {
      return seen.containsKey(object);
    }

    /** Charges bytes that no separate object owns, such as a stated retained size. */
    void addBytes(long size) {
      bytes = Math.addExact(bytes, size);
    }

    boolean object(Object object, long size) {
      if (object == null || seen.put(object, Boolean.TRUE) != null) {
        return false;
      }
      bytes = Math.addExact(bytes, size);
      return true;
    }

    void root(Object object) {
      if (object instanceof Map<?, ?> map && map.isEmpty()
          || object instanceof Set<?> set && set.isEmpty()) {
        return;
      }
      add(object);
    }

    void add(Object object) {
      if (object == null || contains(object)) {
        return;
      }
      if (object instanceof String text) {
        long width = 1;
        for (int i = 0; i < text.length(); i++) {
          if (text.charAt(i) > 255) {
            width = 2;
            break;
          }
        }
        // All retained texts originate in Texts. Distinct strings own distinct backing arrays.
        object(text, Math.addExact(24, align(Math.addExact(16, width * text.length()))));
      } else if (object instanceof Component component) {
        component.charge(this);
      } else if (object instanceof Long) {
        object(object, 24);
      } else if (object instanceof Integer) {
        object(object, 16);
      } else if (object instanceof Boolean) {
        // The set sentinel is a JVM singleton, outside the cache's ownership.
      } else {
        throw new IllegalArgumentException("Unsupported owned metadata: " + object.getClass());
      }
    }
  }

  static long align(long value) {
    return Math.addExact(value, 7) & ~7L;
  }

  /** Hash buckets plus a linked access-order chain, with actual table capacity available. */
  static final class OwnedMap<K, V> extends AbstractMap<K, V> implements Component, Serializable {
    private static final Node<?, ?>[] EMPTY = new Node<?, ?>[0];
    private Node<?, ?>[] table = EMPTY;
    private Node<K, V> first;
    private Node<K, V> last;
    private int size;
    private final boolean accessOrder;

    OwnedMap() {
      this(false);
    }

    OwnedMap(boolean accessOrder) {
      this.accessOrder = accessOrder;
    }

    OwnedMap(OwnedMap<? extends K, ? extends V> source) {
      this(source.accessOrder);
      if (source.table.length > 0) {
        resize(source.table.length);
      }
      putAll(source);
    }

    OwnedMap(Map<? extends K, ? extends V> source) {
      this();
      putAll(source);
    }

    private static final class Node<K, V> implements Map.Entry<K, V>, Serializable {
      private final K key;
      private V value;
      private final int hash;
      private Node<K, V> bucket;
      private Node<K, V> previous;
      private Node<K, V> next;

      private Node(K key, V value, int hash) {
        this.key = key;
        this.value = value;
        this.hash = hash;
      }

      @Override
      public K getKey() {
        return key;
      }

      @Override
      public V getValue() {
        return value;
      }

      @Override
      public boolean equals(Object other) {
        return other instanceof Map.Entry<?, ?> entry
            && Objects.equals(key, entry.getKey())
            && Objects.equals(value, entry.getValue());
      }

      @Override
      public int hashCode() {
        return Objects.hashCode(key) ^ Objects.hashCode(value);
      }

      @Override
      public V setValue(V newValue) {
        V old = value;
        this.value = newValue;
        return old;
      }
    }

    @SuppressWarnings("unchecked")
    private Node<K, V> node(Object key) {
      if (table.length == 0) {
        return null;
      }
      int hash = Objects.hashCode(key);
      Node<K, V> node = (Node<K, V>) table[hash & (table.length - 1)];
      while (node != null) {
        if (node.hash == hash && Objects.equals(node.key, key)) {
          return node;
        }
        node = node.bucket;
      }
      return null;
    }

    @Override
    public V get(Object key) {
      Node<K, V> node = node(key);
      if (node == null) {
        return null;
      }
      if (accessOrder && node != last) {
        unlink(node);
        append(node);
      }
      return node.value;
    }

    V peek(Object key) {
      Node<K, V> node = node(key);
      return node == null ? null : node.value;
    }

    @Override
    public boolean containsKey(Object key) {
      return node(key) != null;
    }

    @Override
    public int size() {
      return size;
    }

    private void append(Node<K, V> node) {
      node.previous = last;
      node.next = null;
      if (last == null) {
        first = node;
      } else {
        last.next = node;
      }
      last = node;
    }

    private void unlink(Node<K, V> node) {
      if (node.previous == null) {
        first = node.next;
      } else {
        node.previous.next = node.next;
      }
      if (node.next == null) {
        last = node.previous;
      } else {
        node.next.previous = node.previous;
      }
    }

    @Override
    @SuppressWarnings("unchecked")
    public V put(K key, V value) {
      Node<K, V> existing = node(key);
      if (existing != null) {
        V old = existing.setValue(value);
        if (accessOrder && existing != last) {
          unlink(existing);
          append(existing);
        }
        return old;
      }
      if (table.length == 0 || size + 1 > table.length * 3L / 4) {
        resize(table.length == 0 ? 4 : Math.multiplyExact(table.length, 2));
      }
      int hash = Objects.hashCode(key);
      int slot = hash & (table.length - 1);
      Node<K, V> added = new Node<>(key, value, hash);
      added.bucket = (Node<K, V>) table[slot];
      table[slot] = added;
      append(added);
      size += 1;
      return null;
    }

    @SuppressWarnings("unchecked")
    private void resize(int capacity) {
      table = new Node<?, ?>[capacity];
      for (Node<K, V> node = first; node != null; node = node.next) {
        int slot = node.hash & (capacity - 1);
        node.bucket = (Node<K, V>) table[slot];
        table[slot] = node;
      }
    }

    @Override
    @SuppressWarnings("unchecked")
    public V remove(Object key) {
      Node<K, V> removed = node(key);
      if (removed == null) {
        return null;
      }
      int slot = removed.hash & (table.length - 1);
      Node<K, V> previous = null;
      Node<K, V> current = (Node<K, V>) table[slot];
      while (current != removed) {
        previous = current;
        current = current.bucket;
      }
      if (previous == null) {
        table[slot] = removed.bucket;
      } else {
        previous.bucket = removed.bucket;
      }
      unlink(removed);
      size -= 1;
      if (size == 0) {
        clear();
      } else if (table.length > 4 && size <= table.length / 4) {
        resize(table.length / 2);
      }
      return removed.value;
    }

    @Override
    public void clear() {
      table = EMPTY;
      first = null;
      last = null;
      size = 0;
    }

    @Override
    public Set<K> keySet() {
      return new AbstractSet<>() {
        @Override
        public int size() {
          return size;
        }

        @Override
        public Iterator<K> iterator() {
          Iterator<Map.Entry<K, V>> entries = entrySet().iterator();
          return new Iterator<>() {
            @Override
            public boolean hasNext() {
              return entries.hasNext();
            }

            @Override
            public K next() {
              return entries.next().getKey();
            }

            @Override
            public void remove() {
              entries.remove();
            }
          };
        }
      };
    }

    @Override
    public Collection<V> values() {
      return new AbstractCollection<>() {
        @Override
        public int size() {
          return size;
        }

        @Override
        public Iterator<V> iterator() {
          Iterator<Map.Entry<K, V>> entries = entrySet().iterator();
          return new Iterator<>() {
            @Override
            public boolean hasNext() {
              return entries.hasNext();
            }

            @Override
            public V next() {
              return entries.next().getValue();
            }

            @Override
            public void remove() {
              entries.remove();
            }
          };
        }
      };
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet() {
      return new AbstractSet<>() {
        @Override
        public int size() {
          return size;
        }

        @Override
        public Iterator<Map.Entry<K, V>> iterator() {
          return new Iterator<>() {
            private Node<K, V> next = first;
            private Node<K, V> current;

            @Override
            public boolean hasNext() {
              return next != null;
            }

            @Override
            public Map.Entry<K, V> next() {
              if (next == null) {
                throw new NoSuchElementException();
              }
              current = next;
              next = next.next;
              return current;
            }

            @Override
            public void remove() {
              if (current == null) {
                throw new IllegalStateException("No current metadata entry");
              }
              OwnedMap.this.remove(current.key);
              current = null;
            }
          };
        }
      };
    }

    void chargeEntries(Components components, Set<K> keys) {
      boolean nonempty = false;
      for (K key : keys) {
        if (containsKey(key)) {
          nonempty = true;
          break;
        }
      }
      if (!nonempty) {
        return;
      }
      components.object(this, 40);
      components.object(table, align(16L + 4L * table.length));
      for (K key : keys) {
        Node<K, V> node = node(key);
        if (node != null) {
          components.object(node, 40);
          components.add(node.key);
          components.add(node.value);
        }
      }
    }

    @Override
    public void charge(Components components) {
      // Empty root containers are the fixed baseline. clear actually releases their buckets.
      if (!components.object(this, 40)) {
        return;
      }
      chargeAll(components);
    }

    private void chargeAll(Components components) {
      components.object(this, 40);
      if (table.length > 0) {
        components.object(table, align(16L + 4L * table.length));
      }
      for (Node<K, V> node = first; node != null; node = node.next) {
        components.object(node, 40);
        components.add(node.key);
        components.add(node.value);
      }
    }
  }

  static final class OwnedSet<E> extends AbstractSet<E> implements Component, Serializable {
    private final OwnedMap<E, Boolean> values = new OwnedMap<>();

    OwnedSet() {}

    OwnedSet(Set<? extends E> source) {
      addAll(source);
    }

    @Override
    public boolean add(E value) {
      return values.put(value, Boolean.TRUE) == null;
    }

    @Override
    public boolean contains(Object value) {
      return values.containsKey(value);
    }

    @Override
    public boolean remove(Object value) {
      return values.remove(value) != null;
    }

    @Override
    public void clear() {
      values.clear();
    }

    @Override
    public int size() {
      return values.size();
    }

    @Override
    public Iterator<E> iterator() {
      return values.keySet().iterator();
    }

    void chargeEntries(Components components, Set<E> entries) {
      if (!entries.isEmpty()) {
        components.object(this, 16);
        values.chargeEntries(components, entries);
      }
    }

    @Override
    public void charge(Components components) {
      if (components.object(this, 16)) {
        components.add(values);
      }
    }
  }
}
