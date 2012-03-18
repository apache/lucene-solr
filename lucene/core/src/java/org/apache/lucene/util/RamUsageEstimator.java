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

import java.lang.management.ManagementFactory;
import java.lang.reflect.*;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.*;

/**
 * Estimates the size of Java objects using a simple memory model
 * for primitive size information.
 * 
 * @lucene.internal
 */
public final class RamUsageEstimator {
  
  private RamUsageEstimator() {} // no instance

  public final static int NUM_BYTES_BOOLEAN = 1;
  public final static int NUM_BYTES_BYTE = 1;
  public final static int NUM_BYTES_CHAR = 2;
  public final static int NUM_BYTES_SHORT = 2;
  public final static int NUM_BYTES_INT = 4;
  public final static int NUM_BYTES_FLOAT = 4;
  public final static int NUM_BYTES_LONG = 8;
  public final static int NUM_BYTES_DOUBLE = 8;

  public final static int NUM_BYTES_OBJECT_REF;
  
  public final static int NUM_BYTES_OBJECT_HEADER;
  public final static int NUM_BYTES_ARRAY_HEADER;
  
  /**
   * A constant specifying the object alignment boundary inside the JVM. Objects will
   * always take a full multiple of this constant, possibly wasting some space. 
   */
  public final static int NUM_BYTES_OBJECT_ALIGNMENT;

  /**
   * Sizes of primitive classes.
   */
  private static final Map<Class<?>,Integer> primitiveSizes;
  static {
    primitiveSizes = new IdentityHashMap<Class<?>,Integer>();
    primitiveSizes.put(boolean.class, Integer.valueOf(NUM_BYTES_BOOLEAN));
    primitiveSizes.put(byte.class, Integer.valueOf(NUM_BYTES_BYTE));
    primitiveSizes.put(char.class, Integer.valueOf(NUM_BYTES_CHAR));
    primitiveSizes.put(short.class, Integer.valueOf(NUM_BYTES_SHORT));
    primitiveSizes.put(int.class, Integer.valueOf(NUM_BYTES_INT));
    primitiveSizes.put(float.class, Integer.valueOf(NUM_BYTES_FLOAT));
    primitiveSizes.put(double.class, Integer.valueOf(NUM_BYTES_DOUBLE));
    primitiveSizes.put(long.class, Integer.valueOf(NUM_BYTES_LONG));
  }

  private final static Object theUnsafe;
  private final static Method objectFieldOffsetMethod;
  private final static boolean useUnsafe, isSupportedJVM;

  /**
   * Initialize constants and try to collect information about the JVM internals. 
   */
  static {
    // Initialize empirically measured defaults. We'll modify them to the current
    // JVM settings later on if possible.
    int referenceSize = Constants.JRE_IS_64BIT ? 8 : 4;
    int objectHeader = Constants.JRE_IS_64BIT ? 16 : 8;
    // The following is objectHeader + NUM_BYTES_INT, but aligned (object alignment)
    // so on 64 bit JVMs it'll be align(16 + 4, @8) = 24.
    int arrayHeader = Constants.JRE_IS_64BIT ? 24 : 12;

    Object unsafe = null;
    Method objectFieldOffsetM = null;
    boolean supportedJvm = true;
    try {
      final Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
      final Field unsafeField = unsafeClass.getDeclaredField("theUnsafe");
      unsafeField.setAccessible(true);
      unsafe = unsafeField.get(null);
      
      // get object reference size by getting scale factor of Object[] arrays:
      try {
        final Method arrayIndexScaleM = unsafeClass.getMethod("arrayIndexScale", Class.class);
        referenceSize = ((Number) arrayIndexScaleM.invoke(unsafe, Object[].class)).intValue();
      } catch (Exception e) {
        // ignore
        supportedJvm = false;
      }
      
      // updated best guess based on reference size:
      objectHeader = Constants.JRE_IS_64BIT ? (8 + referenceSize) : 8;
      arrayHeader = Constants.JRE_IS_64BIT ? (8 + 2 * referenceSize) : 12;
      
      // get the object header size:
      // - first try out if the field offsets are not scaled (see warning in Unsafe docs)
      // - get the object header size by getting the field offset of the first field of a dummy object
      // If the scaling is byte-wise and unsafe is available, enable dynamic size measurement for
      // estimateRamUsage().
      try {
        objectFieldOffsetM = unsafeClass.getMethod("objectFieldOffset", Field.class);
        final Field dummy1Field = DummyTwoLongObject.class.getDeclaredField("dummy1");
        final int ofs1 = ((Number) objectFieldOffsetM.invoke(unsafe, dummy1Field)).intValue();
        final Field dummy2Field = DummyTwoLongObject.class.getDeclaredField("dummy2");
        final int ofs2 = ((Number) objectFieldOffsetM.invoke(unsafe, dummy2Field)).intValue();
        if (Math.abs(ofs2 - ofs1) == NUM_BYTES_LONG) {
          final Field baseField = DummyOneFieldObject.class.getDeclaredField("base");
          objectHeader = ((Number) objectFieldOffsetM.invoke(unsafe, baseField)).intValue();
        } else {
          // it is not safe to use Unsafe.objectFieldOffset(),
          // as it may be scaled (see "cookie" comment in Unsafe), better use defaults
          // and conventional size estimation:
          objectFieldOffsetM = null;
          supportedJvm = false;
        }
      } catch (Exception e) {
        // on exception ensure useUnsafe will be set to false later:
        objectFieldOffsetM = null;
        supportedJvm = false;
      }

      // Get the array header size by retrieving the array base offset
      // (offset of the first element of an array).
      try {
        final Method arrayBaseOffsetM = unsafeClass.getMethod("arrayBaseOffset", Class.class);
        // we calculate that only for byte[] arrays, it's actually the same for all types:
        arrayHeader = ((Number) arrayBaseOffsetM.invoke(unsafe, byte[].class)).intValue();
      } catch (Exception e) {
        // ignore
        supportedJvm = false;
      }
    } catch (Exception e) {
      // ignore
      supportedJvm = false;
    }

    NUM_BYTES_OBJECT_REF = referenceSize;
    NUM_BYTES_OBJECT_HEADER = objectHeader;
    NUM_BYTES_ARRAY_HEADER = arrayHeader;
    useUnsafe = (unsafe != null && objectFieldOffsetM != null);
    if (useUnsafe) {
      theUnsafe = unsafe;
      objectFieldOffsetMethod = objectFieldOffsetM;
    } else {
      theUnsafe = objectFieldOffsetMethod = null;
    }
    
    // Try to get the object alignment (the default seems to be 8 on Hotspot, 
    // regardless of the architecture).
    int objectAlignment = 8;
    try {
      final Class<?> beanClazz = Class.forName("com.sun.management.HotSpotDiagnosticMXBean");
      final Object hotSpotBean = ManagementFactory.newPlatformMXBeanProxy(
        ManagementFactory.getPlatformMBeanServer(),
        "com.sun.management:type=HotSpotDiagnostic",
        beanClazz
      );
      final Method getVMOptionMethod = beanClazz.getMethod("getVMOption", String.class);
      try {
        final Object vmOption = getVMOptionMethod.invoke(hotSpotBean, "ObjectAlignmentInBytes");
        objectAlignment = Integer.parseInt(
            vmOption.getClass().getMethod("getValue").invoke(vmOption).toString()
        );
      } catch (InvocationTargetException ite) {
        if (!(ite.getCause() instanceof IllegalArgumentException))
          throw ite;
        // ignore the error completely and use default of 8 (32 bit JVMs).
      }
    } catch (Exception e) {
      // ignore
      supportedJvm = false;
    }
    NUM_BYTES_OBJECT_ALIGNMENT = objectAlignment;

    isSupportedJVM = supportedJvm;
  }

  // Object with just one field to determine the object header size by getting the offset of the dummy field:
  @SuppressWarnings("unused")
  private static final class DummyOneFieldObject {
    public byte base;
  }

  // Another test object for checking, if the difference in offsets of dummy1 and dummy2 is 8 bytes.
  // Only then we can be sure that those are real, unscaled offsets:
  @SuppressWarnings("unused")
  private static final class DummyTwoLongObject {
    public long dummy1, dummy2;
  }
  
  /** 
   * Returns true, if the current JVM is supported by {@code RamUsageEstimator}.
   * If this method returns {@code false} you are maybe using a 3rd party Java VM
   * that is not supporting Oracle/Sun private APIs. The memory estimates can be 
   * imprecise then (no way of detecting compressed references, alignments, etc.). 
   * Lucene still tries to use sensible defaults.
   */
  public static boolean isSupportedJVM() {
    return isSupportedJVM;
  }

  /** 
   * Aligns an object size to be the next multiple of {@link #NUM_BYTES_OBJECT_ALIGNMENT}. 
   */
  public static long alignObjectSize(long size) {
    size += (long) NUM_BYTES_OBJECT_ALIGNMENT - 1L;
    return size - (size % NUM_BYTES_OBJECT_ALIGNMENT);
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
    return alignObjectSize((long) NUM_BYTES_ARRAY_HEADER + (long) NUM_BYTES_CHAR * arr.length);
  }

  /** Returns the size in bytes of the short[] object. */
  public static long sizeOf(short[] arr) {
    return alignObjectSize((long) NUM_BYTES_ARRAY_HEADER + (long) NUM_BYTES_SHORT * arr.length);
  }
  
  /** Returns the size in bytes of the int[] object. */
  public static long sizeOf(int[] arr) {
    return alignObjectSize((long) NUM_BYTES_ARRAY_HEADER + (long) NUM_BYTES_INT * arr.length);
  }
  
  /** Returns the size in bytes of the float[] object. */
  public static long sizeOf(float[] arr) {
    return alignObjectSize((long) NUM_BYTES_ARRAY_HEADER + (long) NUM_BYTES_FLOAT * arr.length);
  }
  
  /** Returns the size in bytes of the long[] object. */
  public static long sizeOf(long[] arr) {
    return alignObjectSize((long) NUM_BYTES_ARRAY_HEADER + (long) NUM_BYTES_LONG * arr.length);
  }
  
  /** Returns the size in bytes of the double[] object. */
  public static long sizeOf(double[] arr) {
    return alignObjectSize((long) NUM_BYTES_ARRAY_HEADER + (long) NUM_BYTES_DOUBLE * arr.length);
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
  public static long sizeOf(Object obj) {
    final Set<Object> seen = Collections.newSetFromMap(new IdentityHashMap<Object,Boolean>(64));
    try {
      return measureObjectSize(obj, seen);
    } finally {
      // Help the GC.
      seen.clear();
    }
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
      return measureArraySize(obj, null);
    } else {
      return shallowSizeOfInstance(clz);
    }
  }

  /**
   * Returns the shallow instance size in bytes an instance of the given class would occupy.
   * This works with all conventional classes and primitive types, but not with arrays
   * (the size then depends on the number of elements and varies from object to object).
   * Use the array-instance methods instead.
   * 
   * @throws IllegalArgumentException if {@code clazz} is an array class. 
   */
  public static long shallowSizeOfInstance(Class<?> clazz) {
    if (clazz.isArray())
      throw new IllegalArgumentException("This method does not work with array classes.");
    if (clazz.isPrimitive())
      return primitiveSizes.get(clazz);
    
    long size = NUM_BYTES_OBJECT_HEADER;
    
    // Walk type hierarchy
    while (clazz != null) {
      final Field[] fields = clazz.getDeclaredFields();
      boolean fieldFound = false;
      for (final Field f : fields) {
        if (Modifier.isStatic(f.getModifiers())) {
          continue;
        }

        size = reflectFieldSize(size, f);
        fieldFound = true;
      }
      if (useUnsafe && fieldFound) {
        // no need to recurse to superclasses, as all fields are
        // added at the end, so we won't find any larger offset
        break;
      }
      clazz = clazz.getSuperclass();
    }
    return alignObjectSize(size);    
  }

  /**
   * Recursive descend into an object.
   */
  private static long measureObjectSize(Object obj, Set<Object> seen) {
    if (obj == null) {
      return 0;
    }

    // skip if we have seen before
    if (seen.contains(obj)) {
      return 0;
    }

    // add to seen
    seen.add(obj);

    Class<?> clazz = obj.getClass();
    if (clazz.isArray()) {
      return measureArraySize(obj, seen);
    }

    long size = NUM_BYTES_OBJECT_HEADER;
    long innerSize = 0L;

    // walk type hierarchy
    while (clazz != null) {
      final Field[] fields = clazz.getDeclaredFields();
      for (final Field f : fields) {
        if (Modifier.isStatic(f.getModifiers())) {
          continue;
        }

        size = reflectFieldSize(size, f);
        
        if (!f.getType().isPrimitive()) {
          try {
            f.setAccessible(true);
            innerSize += measureObjectSize(f.get(obj), seen);
          } catch (IllegalAccessException ex) {
            // this should never happen as we enable setAccessible()!
            throw new RuntimeException("Cannot reflect instance field: " +
              f.getDeclaringClass().getName() + "#" + f.getName(), ex);
          }
        }
      }
      clazz = clazz.getSuperclass();
    }
    return alignObjectSize(size) + innerSize;
  }
  
  private static long reflectFieldSize(long size, final Field f) {
    final Class<?> type = f.getType();
    final int fsize = type.isPrimitive() ? primitiveSizes.get(type) : NUM_BYTES_OBJECT_REF;
    if (useUnsafe) {
      try {
        final long offsetPlusSize =
          ((Number) objectFieldOffsetMethod.invoke(theUnsafe, f)).longValue() + fsize;
        return Math.max(size, offsetPlusSize);
      } catch (IllegalAccessException ex) {
        throw new RuntimeException("Access problem with sun.misc.Unsafe", ex);
      } catch (InvocationTargetException ite) {
        final Throwable cause = ite.getCause();
        if (cause instanceof RuntimeException)
          throw (RuntimeException) cause;
        if (cause instanceof Error)
          throw (Error) cause;
        // this should never happen (Unsafe does not declare
        // checked Exceptions for this method), but who knows?
        throw new RuntimeException("Call to Unsafe's objectFieldOffset() throwed "+
          "checked Exception when accessing field " +
          f.getDeclaringClass().getName() + "#" + f.getName(), cause);
      }
    } else {
      return size + fsize;
    }
  }

  /**
   * Return the deep size of an <code>array</code>, including
   * sub-objects if there are any.
   * 
   * @param seen A set of already seen objects. If <code>null</code> no references
   *      are followed and this method returns shallow size.
   */
  private static long measureArraySize(Object array, Set<Object> seen) {
    long size = NUM_BYTES_ARRAY_HEADER;
    final int len = Array.getLength(array);
    if (len > 0) {
      Class<?> arrayElementClazz = array.getClass().getComponentType();
      if (arrayElementClazz.isPrimitive()) {
        size += (long) len * primitiveSizes.get(arrayElementClazz);
      } else {
        size += (long) NUM_BYTES_OBJECT_REF * len;
        if (seen != null) {
          for (int i = 0; i < len; i++) {
            size += measureObjectSize(Array.get(array, i), seen);
          }
        }
      }
    }

    return alignObjectSize(size);
  }

  public static final long ONE_KB = 1024;
  public static final long ONE_MB = ONE_KB * ONE_KB;
  public static final long ONE_GB = ONE_KB * ONE_MB;

  /**
   * Returns <code>size</code> in human-readable units (GB, MB, KB or bytes).
   */
  public static String humanReadableUnits(long bytes) {
    return humanReadableUnits(bytes, 
        new DecimalFormat("0.#", DecimalFormatSymbols.getInstance(Locale.ENGLISH)));
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
}
