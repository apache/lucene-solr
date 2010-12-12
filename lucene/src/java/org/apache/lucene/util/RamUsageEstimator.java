package org.apache.lucene.util;

/**
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

import java.lang.reflect.*;
import java.text.DecimalFormat;
import java.util.*;

/**
 * Estimates the size of a given Object using a given MemoryModel for primitive
 * size information.
 * 
 * Resource Usage: 
 * 
 * Internally uses a Map to temporally hold a reference to every
 * object seen. 
 * 
 * If checkInterned, all Strings checked will be interned, but those
 * that were not already interned will be released for GC when the
 * estimate is complete.
 * 
 * @lucene.internal
 */
public final class RamUsageEstimator {

  public final static int NUM_BYTES_SHORT = 2;
  public final static int NUM_BYTES_INT = 4;
  public final static int NUM_BYTES_LONG = 8;
  public final static int NUM_BYTES_FLOAT = 4;
  public final static int NUM_BYTES_DOUBLE = 8;
  public final static int NUM_BYTES_CHAR = 2;
  public final static int NUM_BYTES_OBJECT_HEADER = 8;
  public final static int NUM_BYTES_OBJECT_REF = Constants.JRE_IS_64BIT ? 8 : 4;
  public final static int NUM_BYTES_ARRAY_HEADER = NUM_BYTES_OBJECT_HEADER + NUM_BYTES_INT + NUM_BYTES_OBJECT_REF;

  private MemoryModel memoryModel;

  private final Map<Object,Object> seen;

  private int refSize;
  private int arraySize;
  private int classSize;

  private boolean checkInterned;

  /**
   * Constructs this object with an AverageGuessMemoryModel and
   * checkInterned = true.
   */
  public RamUsageEstimator() {
    this(new AverageGuessMemoryModel());
  }

  /**
   * @param checkInterned check if Strings are interned and don't add to size
   * if they are. Defaults to true but if you know the objects you are checking
   * won't likely contain many interned Strings, it will be faster to turn off
   * intern checking.
   */
  public RamUsageEstimator(boolean checkInterned) {
    this(new AverageGuessMemoryModel(), checkInterned);
  }

  /**
   * @param memoryModel MemoryModel to use for primitive object sizes.
   */
  public RamUsageEstimator(MemoryModel memoryModel) {
    this(memoryModel, true);
  }

  /**
   * @param memoryModel MemoryModel to use for primitive object sizes.
   * @param checkInterned check if Strings are interned and don't add to size
   * if they are. Defaults to true but if you know the objects you are checking
   * won't likely contain many interned Strings, it will be faster to turn off
   * intern checking.
   */
  public RamUsageEstimator(MemoryModel memoryModel, boolean checkInterned) {
    this.memoryModel = memoryModel;
    this.checkInterned = checkInterned;
    // Use Map rather than Set so that we can use an IdentityHashMap - not
    // seeing an IdentityHashSet
    seen = new IdentityHashMap<Object,Object>(64);
    this.refSize = memoryModel.getReferenceSize();
    this.arraySize = memoryModel.getArraySize();
    this.classSize = memoryModel.getClassSize();
  }

  public long estimateRamUsage(Object obj) {
    long size = size(obj);
    seen.clear();
    return size;
  }

  private long size(Object obj) {
    if (obj == null) {
      return 0;
    }
    // interned not part of this object
    if (checkInterned && obj instanceof String
        && obj == ((String) obj).intern()) { // interned string will be eligible
                                             // for GC on
                                             // estimateRamUsage(Object) return
      return 0;
    }

    // skip if we have seen before
    if (seen.containsKey(obj)) {
      return 0;
    }

    // add to seen
    seen.put(obj, null);

    Class<?> clazz = obj.getClass();
    if (clazz.isArray()) {
      return sizeOfArray(obj);
    }

    long size = 0;

    // walk type hierarchy
    while (clazz != null) {
      Field[] fields = clazz.getDeclaredFields();
      for (int i = 0; i < fields.length; i++) {
        if (Modifier.isStatic(fields[i].getModifiers())) {
          continue;
        }

        if (fields[i].getType().isPrimitive()) {
          size += memoryModel.getPrimitiveSize(fields[i].getType());
        } else {
          size += refSize;
          fields[i].setAccessible(true);
          try {
            Object value = fields[i].get(obj);
            if (value != null) {
              size += size(value);
            }
          } catch (IllegalAccessException ex) {
            // ignore for now?
          }
        }

      }
      clazz = clazz.getSuperclass();
    }
    size += classSize;
    return size;
  }

  private long sizeOfArray(Object obj) {
    int len = Array.getLength(obj);
    if (len == 0) {
      return 0;
    }
    long size = arraySize;
    Class<?> arrayElementClazz = obj.getClass().getComponentType();
    if (arrayElementClazz.isPrimitive()) {
      size += len * memoryModel.getPrimitiveSize(arrayElementClazz);
    } else {
      for (int i = 0; i < len; i++) {
        size += refSize + size(Array.get(obj, i));
      }
    }

    return size;
  }

  private static final long ONE_KB = 1024;
  private static final long ONE_MB = ONE_KB * ONE_KB;
  private static final long ONE_GB = ONE_KB * ONE_MB;

  /**
   * Return good default units based on byte size.
   */
  public static String humanReadableUnits(long bytes, DecimalFormat df) {
    String newSizeAndUnits;

    if (bytes / ONE_GB > 0) {
      newSizeAndUnits = String.valueOf(df.format((float) bytes / ONE_GB))
          + " GB";
    } else if (bytes / ONE_MB > 0) {
      newSizeAndUnits = String.valueOf(df.format((float) bytes / ONE_MB))
          + " MB";
    } else if (bytes / ONE_KB > 0) {
      newSizeAndUnits = String.valueOf(df.format((float) bytes / ONE_KB))
          + " KB";
    } else {
      newSizeAndUnits = String.valueOf(bytes) + " bytes";
    }

    return newSizeAndUnits;
  }
}
