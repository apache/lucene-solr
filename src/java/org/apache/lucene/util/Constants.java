package org.apache.lucene.util;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Some useful constants.
 *
 * @author  Doug Cutting
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
}
