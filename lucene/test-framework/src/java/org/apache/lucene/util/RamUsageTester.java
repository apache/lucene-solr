package org.apache.lucene.util;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

/** Crawls object graph to collect RAM usage for testing */
public final class RamUsageTester {
  
  /** 
   * Estimates the RAM usage by the given object. It will
   * walk the object tree and sum up all referenced objects.
   * 
   * <p><b>Resource Usage:</b> This method internally uses a set of
   * every object seen during traversals so it does allocate memory
   * (it isn't side-effect free). After the method exits, this memory
   * should be GCed.</p>
   */
  public static long sizeOf(Object obj) {
    return measureObjectSize(obj);
  }
  
  /**
   * Return a human-readable size of a given object.
   * @see #sizeOf(Object)
   * @see RamUsageEstimator#humanReadableUnits(long)
   */
  public static String humanSizeOf(Object object) {
    return RamUsageEstimator.humanReadableUnits(sizeOf(object));
  }
  
  /*
   * Non-recursive version of object descend. This consumes more memory than recursive in-depth 
   * traversal but prevents stack overflows on long chains of objects
   * or complex graphs (a max. recursion depth on my machine was ~5000 objects linked in a chain
   * so not too much).  
   */
  private static long measureObjectSize(Object root) {
    // Objects seen so far.
    final IdentityHashSet<Object> seen = new IdentityHashSet<>();
    // Class cache with reference Field and precalculated shallow size. 
    final IdentityHashMap<Class<?>, ClassCache> classCache = new IdentityHashMap<>();
    // Stack of objects pending traversal. Recursion caused stack overflows. 
    final ArrayList<Object> stack = new ArrayList<>();
    stack.add(root);

    long totalSize = 0;
    while (!stack.isEmpty()) {
      final Object ob = stack.remove(stack.size() - 1);

      if (ob == null || seen.contains(ob)) {
        continue;
      }
      seen.add(ob);

      final Class<?> obClazz = ob.getClass();
      assert obClazz != null : "jvm bug detected (Object.getClass() == null). please report this to your vendor";
      if (obClazz.isArray()) {
        /*
         * Consider an array, possibly of primitive types. Push any of its references to
         * the processing stack and accumulate this array's shallow size. 
         */
        long size = RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
        final int len = Array.getLength(ob);
        if (len > 0) {
          Class<?> componentClazz = obClazz.getComponentType();
          if (componentClazz.isPrimitive()) {
            size += (long) len * RamUsageEstimator.shallowSizeOfInstance(componentClazz);
          } else {
            size += (long) RamUsageEstimator.NUM_BYTES_OBJECT_REF * len;

            // Push refs for traversal later.
            for (int i = len; --i >= 0 ;) {
              final Object o = Array.get(ob, i);
              if (o != null && !seen.contains(o)) {
                stack.add(o);
              }
            }            
          }
        }
        totalSize += RamUsageEstimator.alignObjectSize(size);
      } else {
        /*
         * Consider an object. Push any references it has to the processing stack
         * and accumulate this object's shallow size. 
         */
        try {
          ClassCache cachedInfo = classCache.get(obClazz);
          if (cachedInfo == null) {
            classCache.put(obClazz, cachedInfo = createCacheEntry(obClazz));
          }

          for (Field f : cachedInfo.referenceFields) {
            // Fast path to eliminate redundancies.
            final Object o = f.get(ob);
            if (o != null && !seen.contains(o)) {
              stack.add(o);
            }
          }

          totalSize += cachedInfo.alignedShallowInstanceSize;
        } catch (IllegalAccessException e) {
          // this should never happen as we enabled setAccessible().
          throw new RuntimeException("Reflective field access failed?", e);
        }
      }
    }

    // Help the GC (?).
    seen.clear();
    stack.clear();
    classCache.clear();

    return totalSize;
  }
  

  /**
   * Cached information about a given class.   
   */
  private static final class ClassCache {
    public final long alignedShallowInstanceSize;
    public final Field[] referenceFields;

    public ClassCache(long alignedShallowInstanceSize, Field[] referenceFields) {
      this.alignedShallowInstanceSize = alignedShallowInstanceSize;
      this.referenceFields = referenceFields;
    }    
  }
  
  /**
   * Create a cached information about shallow size and reference fields for 
   * a given class.
   */
  private static ClassCache createCacheEntry(final Class<?> clazz) {
    ClassCache cachedInfo;
    long shallowInstanceSize = RamUsageEstimator.NUM_BYTES_OBJECT_HEADER;
    final ArrayList<Field> referenceFields = new ArrayList<>(32);
    for (Class<?> c = clazz; c != null; c = c.getSuperclass()) {
      final Field[] fields = c.getDeclaredFields();
      for (final Field f : fields) {
        if (!Modifier.isStatic(f.getModifiers())) {
          shallowInstanceSize = RamUsageEstimator.adjustForField(shallowInstanceSize, f);

          if (!f.getType().isPrimitive()) {
            f.setAccessible(true);
            referenceFields.add(f);
          }
        }
      }
    }

    cachedInfo = new ClassCache(
        RamUsageEstimator.alignObjectSize(shallowInstanceSize), 
        referenceFields.toArray(new Field[referenceFields.size()]));
    return cachedInfo;
  }
  
  /**
   * An identity hash set implemented using open addressing. No null keys are allowed.
   * 
   * TODO: If this is useful outside this class, make it public - needs some work
   */
  static final class IdentityHashSet<KType> implements Iterable<KType> {
    /**
     * Default load factor.
     */
    public final static float DEFAULT_LOAD_FACTOR = 0.75f;

    /**
     * Minimum capacity for the set.
     */
    public final static int MIN_CAPACITY = 4;

    /**
     * All of set entries. Always of power of two length.
     */
    public Object[] keys;
    
    /**
     * Cached number of assigned slots.
     */
    public int assigned;
    
    /**
     * The load factor for this set (fraction of allocated or deleted slots before
     * the buffers must be rehashed or reallocated).
     */
    public final float loadFactor;
    
    /**
     * Cached capacity threshold at which we must resize the buffers.
     */
    private int resizeThreshold;
    
    /**
     * Creates a hash set with the default capacity of 16.
     * load factor of {@value #DEFAULT_LOAD_FACTOR}. `
     */
    public IdentityHashSet() {
      this(16, DEFAULT_LOAD_FACTOR);
    }
    
    /**
     * Creates a hash set with the given capacity, load factor of
     * {@value #DEFAULT_LOAD_FACTOR}.
     */
    public IdentityHashSet(int initialCapacity) {
      this(initialCapacity, DEFAULT_LOAD_FACTOR);
    }
    
    /**
     * Creates a hash set with the given capacity and load factor.
     */
    public IdentityHashSet(int initialCapacity, float loadFactor) {
      initialCapacity = Math.max(MIN_CAPACITY, initialCapacity);
      
      assert initialCapacity > 0 : "Initial capacity must be between (0, "
          + Integer.MAX_VALUE + "].";
      assert loadFactor > 0 && loadFactor < 1 : "Load factor must be between (0, 1).";
      this.loadFactor = loadFactor;
      allocateBuffers(roundCapacity(initialCapacity));
    }
    
    /**
     * Adds a reference to the set. Null keys are not allowed.
     */
    public boolean add(KType e) {
      assert e != null : "Null keys not allowed.";
      
      if (assigned >= resizeThreshold) expandAndRehash();
      
      final int mask = keys.length - 1;
      int slot = rehash(e) & mask;
      Object existing;
      while ((existing = keys[slot]) != null) {
        if (e == existing) {
          return false; // already found.
        }
        slot = (slot + 1) & mask;
      }
      assigned++;
      keys[slot] = e;
      return true;
    }

    /**
     * Checks if the set contains a given ref.
     */
    public boolean contains(KType e) {
      final int mask = keys.length - 1;
      int slot = rehash(e) & mask;
      Object existing;
      while ((existing = keys[slot]) != null) {
        if (e == existing) {
          return true;
        }
        slot = (slot + 1) & mask;
      }
      return false;
    }

    /** Rehash via MurmurHash.
     * 
     * <p>The implementation is based on the
     * finalization step from Austin Appleby's
     * <code>MurmurHash3</code>.
     * 
     * @see "http://sites.google.com/site/murmurhash/"
     */
    private static int rehash(Object o) {
      int k = System.identityHashCode(o);
      k ^= k >>> 16;
      k *= 0x85ebca6b;
      k ^= k >>> 13;
      k *= 0xc2b2ae35;
      k ^= k >>> 16;
      return k;
    }
    
    /**
     * Expand the internal storage buffers (capacity) or rehash current keys and
     * values if there are a lot of deleted slots.
     */
    private void expandAndRehash() {
      final Object[] oldKeys = this.keys;
      
      assert assigned >= resizeThreshold;
      allocateBuffers(nextCapacity(keys.length));
      
      /*
       * Rehash all assigned slots from the old hash table.
       */
      final int mask = keys.length - 1;
      for (int i = 0; i < oldKeys.length; i++) {
        final Object key = oldKeys[i];
        if (key != null) {
          int slot = rehash(key) & mask;
          while (keys[slot] != null) {
            slot = (slot + 1) & mask;
          }
          keys[slot] = key;
        }
      }
      Arrays.fill(oldKeys, null);
    }
    
    /**
     * Allocate internal buffers for a given capacity.
     * 
     * @param capacity
     *          New capacity (must be a power of two).
     */
    private void allocateBuffers(int capacity) {
      this.keys = new Object[capacity];
      this.resizeThreshold = (int) (capacity * DEFAULT_LOAD_FACTOR);
    }
    
    /**
     * Return the next possible capacity, counting from the current buffers' size.
     */
    protected int nextCapacity(int current) {
      assert current > 0 && Long.bitCount(current) == 1 : "Capacity must be a power of two.";
      assert ((current << 1) > 0) : "Maximum capacity exceeded ("
          + (0x80000000 >>> 1) + ").";
      
      if (current < MIN_CAPACITY / 2) current = MIN_CAPACITY / 2;
      return current << 1;
    }
    
    /**
     * Round the capacity to the next allowed value.
     */
    protected int roundCapacity(int requestedCapacity) {
      // Maximum positive integer that is a power of two.
      if (requestedCapacity > (0x80000000 >>> 1)) return (0x80000000 >>> 1);
      
      int capacity = MIN_CAPACITY;
      while (capacity < requestedCapacity) {
        capacity <<= 1;
      }

      return capacity;
    }
    
    public void clear() {
      assigned = 0;
      Arrays.fill(keys, null);
    }
    
    public int size() {
      return assigned;
    }
    
    public boolean isEmpty() {
      return size() == 0;
    }

    @Override
    public Iterator<KType> iterator() {
      return new Iterator<KType>() {
        int pos = -1;
        Object nextElement = fetchNext();

        @Override
        public boolean hasNext() {
          return nextElement != null;
        }

        @SuppressWarnings("unchecked")
        @Override
        public KType next() {
          Object r = this.nextElement;
          if (r == null) {
            throw new NoSuchElementException();
          }
          this.nextElement = fetchNext();
          return (KType) r;
        }

        private Object fetchNext() {
          pos++;
          while (pos < keys.length && keys[pos] == null) {
            pos++;
          }

          return (pos >= keys.length ? null : keys[pos]);
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    }
  }
}
