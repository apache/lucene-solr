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

import java.util.jar.Manifest;
import java.util.jar.Attributes;
import java.io.InputStream;
import java.net.URL;

/**
 * Some useful constants.
 *
 *
 * @version $Id$
 **/

public final class Constants {
  private Constants() {}			  // can't construct

  /** The value of <tt>System.getProperty("java.version")<tt>. **/
  public static final String JAVA_VERSION = System.getProperty("java.version");
  /** True iff this is Java version 1.1. */
  public static final boolean JAVA_1_1 = JAVA_VERSION.startsWith("1.1.");
  /** True iff this is Java version 1.2. */
  public static final boolean JAVA_1_2 = JAVA_VERSION.startsWith("1.2.");
  /** True iff this is Java version 1.3. */
  public static final boolean JAVA_1_3 = JAVA_VERSION.startsWith("1.3.");
 
  /** The value of <tt>System.getProperty("os.name")<tt>. **/
  public static final String OS_NAME = System.getProperty("os.name");
  /** True iff running on Linux. */
  public static final boolean LINUX = OS_NAME.startsWith("Linux");
  /** True iff running on Windows. */
  public static final boolean WINDOWS = OS_NAME.startsWith("Windows");
  /** True iff running on SunOS. */
  public static final boolean SUN_OS = OS_NAME.startsWith("SunOS");

  public static final String OS_ARCH = System.getProperty("os.arch");
  public static final String OS_VERSION = System.getProperty("os.version");
  public static final String JAVA_VENDOR = System.getProperty("java.vendor");

  public static final String LUCENE_VERSION;

  public static final String LUCENE_MAIN_VERSION = "2.9";

  static {
    String v = LUCENE_MAIN_VERSION + "-dev";
    try {
      // TODO: this should have worked, but doesn't seem to?
      // Package.getPackage("org.apache.lucene.util").getImplementationVersion();
      String classContainer = Constants.class.getProtectionDomain().getCodeSource().getLocation().toString();
      URL manifestUrl = new URL("jar:" + classContainer + "!/META-INF/MANIFEST.MF");
      InputStream s = manifestUrl.openStream();
      try {
        Manifest manifest = new Manifest(s);
        Attributes attr = manifest.getMainAttributes();
        String value = attr.getValue(Attributes.Name.IMPLEMENTATION_VERSION);
        if (value != null) {
          if (value.indexOf(LUCENE_MAIN_VERSION) == -1) {
            v = value + " [" + LUCENE_MAIN_VERSION + "]";
          } else {
            v = value;
          }
        }
      } finally {
        if (s != null) {
          s.close();
        }
      }
    } catch (Throwable t) {
      // ignore
    }

    LUCENE_VERSION = v;
  }
}
