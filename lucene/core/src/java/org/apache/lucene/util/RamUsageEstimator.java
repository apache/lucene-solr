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
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.IdentityHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Estimates the size (memory representation) of Java objects.
 * <p>
 * This class uses assumptions that were discovered for the Hotspot
 * virtual machine. If you use a non-OpenJDK/Oracle-based JVM,
 * the measurements may be slightly wrong.
 * 
 * @see #shallowSizeOf(Object)
 * @see #shallowSizeOfInstance(Class)
 * 
 * @lucene.internal
 */
public final class RamUsageEstimator {

  /** One kilobyte bytes. */
  public static final long ONE_KB = 1024;
  
  /** One megabyte bytes. */
  public static final long ONE_MB = ONE_KB * ONE_KB;
  
  /** One gigabyte bytes.*/
  public static final long ONE_GB = ONE_KB * ONE_MB;

  /** No instantiation. */
  private RamUsageEstimator() {}

  /** 
   * Number of bytes used to represent a {@code boolean} in binary form
   * @deprecated use {@code 1} instead.
   */
  @Deprecated
  public final static int NUM_BYTES_BOOLEAN = 1;
  /** 
   * Number of bytes used to represent a {@code byte} in binary form
   * @deprecated use {@code 1} instead.
   */
  @Deprecated
  public final static int NUM_BYTES_BYTE = 1;
  /** 
   * Number of bytes used to represent a {@code char} in binary form
   * @deprecated use {@link Character#BYTES} instead.
   */
  @Deprecated
  public final static int NUM_BYTES_CHAR = Character.BYTES;
  /** 
   * Number of bytes used to represent a {@code short} in binary form
   * @deprecated use {@link Short#BYTES} instead.
   */
  @Deprecated
  public final static int NUM_BYTES_SHORT = Short.BYTES;
  /** 
   * Number of bytes used to represent an {@code int} in binary form
   * @deprecated use {@link Integer#BYTES} instead.
   */
  @Deprecated
  public final static int NUM_BYTES_INT = Integer.BYTES;
  /** 
   * Number of bytes used to represent a {@code float} in binary form
   * @deprecated use {@link Float#BYTES} instead.
   */
  @Deprecated
  public final static int NUM_BYTES_FLOAT = Float.BYTES;
  /** 
   * Number of bytes used to represent a {@code long} in binary form
   * @deprecated use {@link Long#BYTES} instead.
   */
  @Deprecated
  public final static int NUM_BYTES_LONG = Long.BYTES;
  /** 
   * Number of bytes used to represent a {@code double} in binary form
   * @deprecated use {@link Double#BYTES} instead.
   */
  @Deprecated
  public final static int NUM_BYTES_DOUBLE = Double.BYTES;

  /** 
   * True, iff compressed references (oops) are enabled by this JVM 
   */
  public final static boolean COMPRESSED_REFS_ENABLED;

  /** 
   * Number of bytes this JVM uses to represent an object reference. 
   */
  public final static int NUM_BYTES_OBJECT_REF;

  /**
   * Number of bytes to represent an object header (no fields, no alignments).
   */
  public final static int NUM_BYTES_OBJECT_HEADER;

  /**
   * Number of bytes to represent an array header (no content, but with alignments).
   */
  public final static int NUM_BYTES_ARRAY_HEADER;
  
  /**
   * A constant specifying the object alignment boundary inside the JVM. Objects will
   * always take a full multiple of this constant, possibly wasting some space. 
   */
  public final static int NUM_BYTES_OBJECT_ALIGNMENT;

  /**
   * Sizes of primitive classes.
   */
  private static final Map<Class<?>,Integer> primitiveSizes = new IdentityHashMap<>();
  static {
    primitiveSizes.put(boolean.class, 1);
    primitiveSizes.put(byte.class, 1);
    primitiveSizes.put(char.class, Integer.valueOf(Character.BYTES));
    primitiveSizes.put(short.class, Integer.valueOf(Short.BYTES));
    primitiveSizes.put(int.class, Integer.valueOf(Integer.BYTES));
    primitiveSizes.put(float.class, Integer.valueOf(Float.BYTES));
    primitiveSizes.put(double.class, Integer.valueOf(Double.BYTES));
    primitiveSizes.put(long.class, Integer.valueOf(Long.BYTES));
  }

  /**
   * JVMs typically cache small longs. This tries to find out what the range is.
   */
  static final long LONG_CACHE_MIN_VALUE, LONG_CACHE_MAX_VALUE;
  static final int LONG_SIZE;
  
  /** For testing only */
  static final boolean JVM_IS_HOTSPOT_64BIT;
  
  static final String MANAGEMENT_FACTORY_CLASS = "java.lang.management.ManagementFactory";
  static final String HOTSPOT_BEAN_CLASS = "com.sun.management.HotSpotDiagnosticMXBean";

  /**
   * Initialize constants and try to collect information about the JVM internals. 
   */
  static {    
    if (Constants.JRE_IS_64BIT) {
      // Try to get compressed oops and object alignment (the default seems to be 8 on Hotspot);
      // (this only works on 64 bit, on 32 bits the alignment and reference size is fixed):
      boolean compressedOops = false;
      int objectAlignment = 8;
      boolean isHotspot = false;
      try {
        final Class<?> beanClazz = Class.forName(HOTSPOT_BEAN_CLASS);
        // we use reflection for this, because the management factory is not part
        // of Java 8's compact profile:
        final Object hotSpotBean = Class.forName(MANAGEMENT_FACTORY_CLASS)
          .getMethod("getPlatformMXBean", Class.class)
          .invoke(null, beanClazz);
        if (hotSpotBean != null) {
          isHotspot = true;
          final Method getVMOptionMethod = beanClazz.getMethod("getVMOption", String.class);
          try {
            final Object vmOption = getVMOptionMethod.invoke(hotSpotBean, "UseCompressedOops");
            compressedOops = Boolean.parseBoolean(
                vmOption.getClass().getMethod("getValue").invoke(vmOption).toString()
            );
          } catch (ReflectiveOperationException | RuntimeException e) {
            isHotspot = false;
          }
          try {
            final Object vmOption = getVMOptionMethod.invoke(hotSpotBean, "ObjectAlignmentInBytes");
            objectAlignment = Integer.parseInt(
                vmOption.getClass().getMethod("getValue").invoke(vmOption).toString()
            );
          } catch (ReflectiveOperationException | RuntimeException e) {
            isHotspot = false;
          }
        }
      } catch (ReflectiveOperationException | RuntimeException e) {
        isHotspot = false;
      }
      JVM_IS_HOTSPOT_64BIT = isHotspot;
      COMPRESSED_REFS_ENABLED = compressedOops;
      NUM_BYTES_OBJECT_ALIGNMENT = objectAlignment;
      // reference size is 4, if we have compressed oops:
      NUM_BYTES_OBJECT_REF = COMPRESSED_REFS_ENABLED ? 4 : 8;
      // "best guess" based on reference size:
      NUM_BYTES_OBJECT_HEADER = 8 + NUM_BYTES_OBJECT_REF;
      // array header is NUM_BYTES_OBJECT_HEADER + NUM_BYTES_INT, but aligned (object alignment):
      NUM_BYTES_ARRAY_HEADER = (int) alignObjectSize(NUM_BYTES_OBJECT_HEADER + Integer.BYTES);
    } else {
      JVM_IS_HOTSPOT_64BIT = false;
      COMPRESSED_REFS_ENABLED = false;
      NUM_BYTES_OBJECT_ALIGNMENT = 8;
      NUM_BYTES_OBJECT_REF = 4;
      NUM_BYTES_OBJECT_HEADER = 8;
      // For 32 bit JVMs, no extra alignment of array header:
      NUM_BYTES_ARRAY_HEADER = NUM_BYTES_OBJECT_HEADER + Integer.BYTES;
    }

    // get min/max value of cached Long class instances:
    long longCacheMinValue = 0;
    while (longCacheMinValue > Long.MIN_VALUE
        && Long.valueOf(longCacheMinValue - 1) == Long.valueOf(longCacheMinValue - 1)) {
      longCacheMinValue -= 1;
    }
    long longCacheMaxValue = -1;
    while (longCacheMaxValue < Long.MAX_VALUE
        && Long.valueOf(longCacheMaxValue + 1) == Long.valueOf(longCacheMaxValue + 1)) {
      longCacheMaxValue += 1;
    }
    LONG_CACHE_MIN_VALUE = longCacheMinValue;
    LONG_CACHE_MAX_VALUE = longCacheMaxValue;
    LONG_SIZE = (int) shallowSizeOfInstance(Long.class);
  }
  
  /** 
   * Aligns an object size to be the next multiple of {@link #NUM_BYTES_OBJECT_ALIGNMENT}. 
   */
  public static long alignObjectSize(long size) {
    size += (long) NUM_BYTES_OBJECT_ALIGNMENT - 1L;
    return size - (size % NUM_BYTES_OBJECT_ALIGNMENT);
  }

  /**
   * Return the size of the provided {@link Long} object, returning 0 if it is
   * cached by the JVM and its shallow size otherwise.
   */
  public static long sizeOf(Long value) {
    if (value >= LONG_CACHE_MIN_VALUE && value <= LONG_CACHE_MAX_VALUE) {
      return 0;
    }
    return LONG_SIZE;
  }

  /** Returns the size in bytes of the byte[] object. */
  public static long sizeOf(byte[] arr) {
    return alignObjectSize((long) NUM_BYTES_ARRAY_HEADER + arr.length);
  }
  
  /** Returns the size in bytes of the boolean[] object. */
  public static long sizeOf(boolean[] arr) {
    return alignObjectSize((long) NUM_BYTES_ARRAY_HEADER + arr.length);
  }
  
  /** Returns the size in bytes of the char[] object. */
  public static long sizeOf(char[] arr) {
    return alignObjectSize((long) NUM_BYTES_ARRAY_HEADER + (long) Character.BYTES * arr.length);
  }

  /** Returns the size in bytes of the short[] object. */
  public static long sizeOf(short[] arr) {
    return alignObjectSize((long) NUM_BYTES_ARRAY_HEADER + (long) Short.BYTES * arr.length);
  }
  
  /** Returns the size in bytes of the int[] object. */
  public static long sizeOf(int[] arr) {
    return alignObjectSize((long) NUM_BYTES_ARRAY_HEADER + (long) Integer.BYTES * arr.length);
  }
  
  /** Returns the size in bytes of the float[] object. */
  public static long sizeOf(float[] arr) {
    return alignObjectSize((long) NUM_BYTES_ARRAY_HEADER + (long) Float.BYTES * arr.length);
  }
  
  /** Returns the size in bytes of the long[] object. */
  public static long sizeOf(long[] arr) {
    return alignObjectSize((long) NUM_BYTES_ARRAY_HEADER + (long) Long.BYTES * arr.length);
  }
  
  /** Returns the size in bytes of the double[] object. */
  public static long sizeOf(double[] arr) {
    return alignObjectSize((long) NUM_BYTES_ARRAY_HEADER + (long) Double.BYTES * arr.length);
  }

  /** Returns the shallow size in bytes of the Object[] object. */
  // Use this method instead of #shallowSizeOf(Object) to avoid costly reflection
  public static long shallowSizeOf(Object[] arr) {
    return alignObjectSize((long) NUM_BYTES_ARRAY_HEADER + (long) NUM_BYTES_OBJECT_REF * arr.length);
  }

  /** 
   * Estimates a "shallow" memory usage of the given object. For arrays, this will be the
   * memory taken by array storage (no subreferences will be followed). For objects, this
   * will be the memory taken by the fields.
   * 
   * JVM object alignments are also applied.
   */
  public static long shallowSizeOf(Object obj) {
    if (obj == null) return 0;
    final Class<?> clz = obj.getClass();
    if (clz.isArray()) {
      return shallowSizeOfArray(obj);
    } else {
      return shallowSizeOfInstance(clz);
    }
  }

  /**
   * Returns the shallow instance size in bytes an instance of the given class would occupy.
   * This works with all conventional classes and primitive types, but not with arrays
   * (the size then depends on the number of elements and varies from object to object).
   * 
   * @see #shallowSizeOf(Object)
   * @throws IllegalArgumentException if {@code clazz} is an array class. 
   */
  public static long shallowSizeOfInstance(Class<?> clazz) {
    if (clazz.isArray())
      throw new IllegalArgumentException("This method does not work with array classes.");
    if (clazz.isPrimitive())
      return primitiveSizes.get(clazz);
    
    long size = NUM_BYTES_OBJECT_HEADER;

    // Walk type hierarchy
    for (;clazz != null; clazz = clazz.getSuperclass()) {
      final Class<?> target = clazz;
      final Field[] fields = AccessController.doPrivileged(new PrivilegedAction<Field[]>() {
        @Override
        public Field[] run() {
          return target.getDeclaredFields();
        }
      });
      for (Field f : fields) {
        if (!Modifier.isStatic(f.getModifiers())) {
          size = adjustForField(size, f);
        }
      }
    }
    return alignObjectSize(size);    
  }

  /**
   * Return shallow size of any <code>array</code>.
   */
  private static long shallowSizeOfArray(Object array) {
    long size = NUM_BYTES_ARRAY_HEADER;
    final int len = Array.getLength(array);
    if (len > 0) {
      Class<?> arrayElementClazz = array.getClass().getComponentType();
      if (arrayElementClazz.isPrimitive()) {
        size += (long) len * primitiveSizes.get(arrayElementClazz);
      } else {
        size += (long) NUM_BYTES_OBJECT_REF * len;
      }
    }
    return alignObjectSize(size);
  }

  /**
   * This method returns the maximum representation size of an object. <code>sizeSoFar</code>
   * is the object's size measured so far. <code>f</code> is the field being probed.
   * 
   * <p>The returned offset will be the maximum of whatever was measured so far and 
   * <code>f</code> field's offset and representation size (unaligned).
   */
  static long adjustForField(long sizeSoFar, final Field f) {
    final Class<?> type = f.getType();
    final int fsize = type.isPrimitive() ? primitiveSizes.get(type) : NUM_BYTES_OBJECT_REF;
    // TODO: No alignments based on field type/ subclass fields alignments?
    return sizeSoFar + fsize;
  }

  /**
   * Returns <code>size</code> in human-readable units (GB, MB, KB or bytes).
   */
  public static String humanReadableUnits(long bytes) {
    return humanReadableUnits(bytes, 
        new DecimalFormat("0.#", DecimalFormatSymbols.getInstance(Locale.ROOT)));
  }

  /**
   * Returns <code>size</code> in human-readable units (GB, MB, KB or bytes). 
   */
  public static String humanReadableUnits(long bytes, DecimalFormat df) {
    if (bytes / ONE_GB > 0) {
      return df.format((float) bytes / ONE_GB) + " GB";
    } else if (bytes / ONE_MB > 0) {
      return df.format((float) bytes / ONE_MB) + " MB";
    } else if (bytes / ONE_KB > 0) {
      return df.format((float) bytes / ONE_KB) + " KB";
    } else {
      return bytes + " bytes";
    }
  }

  /**
   * Return the size of the provided array of {@link Accountable}s by summing
   * up the shallow size of the array and the
   * {@link Accountable#ramBytesUsed() memory usage} reported by each
   * {@link Accountable}.
   */
  public static long sizeOf(Accountable[] accountables) {
    long size = shallowSizeOf(accountables);
    for (Accountable accountable : accountables) {
      if (accountable != null) {
        size += accountable.ramBytesUsed();
      }
    }
    return size;
  }
}
