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
package org.apache.lucene.util;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Crawls object graph to collect RAM usage for testing */
public final class RamUsageTester {

  /** An accumulator of object references. This class allows for customizing RAM usage estimation. */
  public static class Accumulator {

    /** Accumulate transitive references for the provided fields of the given
     *  object into <code>queue</code> and return the shallow size of this object. */
    public long accumulateObject(Object o, long shallowSize, Map<Field, Object> fieldValues, Collection<Object> queue) {
      for (Object value : fieldValues.values()) {
        queue.add(value);
      }
      return shallowSize;
    }

    /** Accumulate transitive references for the provided values of the given
     *  array into <code>queue</code> and return the shallow size of this array. */
    public long accumulateArray(Object array, long shallowSize, List<Object> values, Collection<Object> queue) {
      queue.addAll(values);
      return shallowSize;
    }

  }

  /**
   * Estimates the RAM usage by the given object. It will
   * walk the object tree and sum up all referenced objects.
   *
   * <p><b>Resource Usage:</b> This method internally uses a set of
   * every object seen during traversals so it does allocate memory
   * (it isn't side-effect free). After the method exits, this memory
   * should be GCed.</p>
   */
  public static long sizeOf(Object obj, Accumulator accumulator) {
    return measureObjectSize(obj, accumulator);
  }

  /** Same as calling <code>sizeOf(obj, DEFAULT_FILTER)</code>. */
  public static long sizeOf(Object obj) {
    return sizeOf(obj, new Accumulator());
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
  private static long measureObjectSize(Object root, Accumulator accumulator) {
    // Objects seen so far.
    final Set<Object> seen = Collections.newSetFromMap(new IdentityHashMap<Object, Boolean>());
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
        final long shallowSize = RamUsageEstimator.shallowSizeOf(ob);
        final int len = Array.getLength(ob);
        final List<Object> values;
        Class<?> componentClazz = obClazz.getComponentType();
        if (componentClazz.isPrimitive()) {
          values = Collections.emptyList();
        } else {
          values = new AbstractList<Object>() {

            @Override
            public Object get(int index) {
              return Array.get(ob, index);
            }

            @Override
            public int size() {
              return len;
              }
              
            };         
          }
        totalSize += accumulator.accumulateArray(ob, shallowSize, values, stack);
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

          Map<Field, Object> fieldValues = new HashMap<>();
          for (Field f : cachedInfo.referenceFields) {
            fieldValues.put(f, f.get(ob));
          }

          totalSize += accumulator.accumulateObject(ob, cachedInfo.alignedShallowInstanceSize, fieldValues, stack);
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
    return AccessController.doPrivileged(new PrivilegedAction<ClassCache>() {
      @Override
      @SuppressForbidden(reason = "We need to access private fields of measured objects.")
      public ClassCache run() {
        ClassCache cachedInfo;
        long shallowInstanceSize = RamUsageEstimator.NUM_BYTES_OBJECT_HEADER;
        final ArrayList<Field> referenceFields = new ArrayList<>(32);
        for (Class<?> c = clazz; c != null; c = c.getSuperclass()) {
          if (c == Class.class) {
            // prevent inspection of Class' fields, throws SecurityException in Java 9!
            continue; 
          }
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
    });
  }

}
