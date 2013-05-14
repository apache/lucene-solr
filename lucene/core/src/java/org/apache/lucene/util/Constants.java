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

import java.lang.reflect.Field;
import java.util.Collections;
import org.apache.lucene.LucenePackage;

/**
 * Some useful constants.
 **/

public final class Constants {
  private Constants() {}  // can't construct

  /** JVM vendor info. */
  public static final String JVM_VENDOR = System.getProperty("java.vm.vendor");
  public static final String JVM_VERSION = System.getProperty("java.vm.version");
  public static final String JVM_NAME = System.getProperty("java.vm.name");

  /** The value of <tt>System.getProperty("java.version")</tt>. **/
  public static final String JAVA_VERSION = System.getProperty("java.version");
 
  /** The value of <tt>System.getProperty("os.name")</tt>. **/
  public static final String OS_NAME = System.getProperty("os.name");
  /** True iff running on Linux. */
  public static final boolean LINUX = OS_NAME.startsWith("Linux");
  /** True iff running on Windows. */
  public static final boolean WINDOWS = OS_NAME.startsWith("Windows");
  /** True iff running on SunOS. */
  public static final boolean SUN_OS = OS_NAME.startsWith("SunOS");
  /** True iff running on Mac OS X */
  public static final boolean MAC_OS_X = OS_NAME.startsWith("Mac OS X");

  public static final String OS_ARCH = System.getProperty("os.arch");
  public static final String OS_VERSION = System.getProperty("os.version");
  public static final String JAVA_VENDOR = System.getProperty("java.vendor");

  /** @deprecated With Lucene 4.0, we are always on Java 6 */
  @Deprecated
  public static final boolean JRE_IS_MINIMUM_JAVA6 =
    new Boolean(true).booleanValue(); // prevent inlining in foreign class files
  
  public static final boolean JRE_IS_MINIMUM_JAVA7;
  public static final boolean JRE_IS_MINIMUM_JAVA8;
  
  /** True iff running on a 64bit JVM */
  public static final boolean JRE_IS_64BIT;
  
  static {
    boolean is64Bit = false;
    try {
      final Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
      final Field unsafeField = unsafeClass.getDeclaredField("theUnsafe");
      unsafeField.setAccessible(true);
      final Object unsafe = unsafeField.get(null);
      final int addressSize = ((Number) unsafeClass.getMethod("addressSize")
        .invoke(unsafe)).intValue();
      //System.out.println("Address size: " + addressSize);
      is64Bit = addressSize >= 8;
    } catch (Exception e) {
      final String x = System.getProperty("sun.arch.data.model");
      if (x != null) {
        is64Bit = x.indexOf("64") != -1;
      } else {
        if (OS_ARCH != null && OS_ARCH.indexOf("64") != -1) {
          is64Bit = true;
        } else {
          is64Bit = false;
        }
      }
    }
    JRE_IS_64BIT = is64Bit;
    
    // this method only exists in Java 7:
    boolean v7 = true;
    try {
      Throwable.class.getMethod("getSuppressed");
    } catch (NoSuchMethodException nsme) {
      v7 = false;
    }
    JRE_IS_MINIMUM_JAVA7 = v7;
    
    if (JRE_IS_MINIMUM_JAVA7) {
      // this method only exists in Java 8:
      boolean v8 = true;
      try {
        Collections.class.getMethod("emptySortedSet");
      } catch (NoSuchMethodException nsme) {
        v8 = false;
      }
      JRE_IS_MINIMUM_JAVA8 = v8;
    } else {
      JRE_IS_MINIMUM_JAVA8 = false;
    }
  }

  // this method prevents inlining the final version constant in compiled classes,
  // see: http://www.javaworld.com/community/node/3400
  private static String ident(final String s) {
    return s.toString();
  }
  
  // NOTE: we track per-segment version as a String with the "X.Y" format, e.g.
  // "4.0", "3.1", "3.0". Therefore when we change this constant, we should keep
  // the format.
  /**
   * This is the internal Lucene version, recorded into each segment.
   */
  public static final String LUCENE_MAIN_VERSION = ident("4.3.1");

  /**
   * This is the Lucene version for display purposes.
   */
  public static final String LUCENE_VERSION;
  static {
    Package pkg = LucenePackage.get();
    String v = (pkg == null) ? null : pkg.getImplementationVersion();
    if (v == null) {
      String parts[] = LUCENE_MAIN_VERSION.split("\\.");
      if (parts.length == 4) {
        // alpha/beta
        assert parts[2].equals("0");
        v = parts[0] + "." + parts[1] + "-SNAPSHOT";
      } else {
        v = LUCENE_MAIN_VERSION + "-SNAPSHOT";
      }
    }
    LUCENE_VERSION = ident(v);
  }
}
