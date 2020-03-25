/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.bootstrap;

import com.sun.jna.LastErrorException;
import org.eclipse.jetty.start.StartLog;

import static org.apache.solr.bootstrap.NativeLibrary.OSType.*;

public final class NativeLibrary {
  public enum OSType {
    LINUX,
    MAC,
    WINDOWS,
    AIX,
    OTHER;
  }

  public static final OSType osType;

  private static final int MCL_CURRENT;

  private static final int ENOMEM = 12;

  private static final NativeLibraryWrapper wrappedLibrary;
  private static boolean jnaLockable = false;

  static {
    // detect the OS type the JVM is running on and then set the CLibraryWrapper
    // instance to a compatable implementation of CLibraryWrapper for that OS type
    osType = getOsType();
    switch (osType) {
      case MAC:
        wrappedLibrary = new NativeLibraryDarwin();
        break;
      case WINDOWS:
        wrappedLibrary = new NativeLibraryWindows();
        break;
      case LINUX:
      case AIX:
      case OTHER:
      default:
        wrappedLibrary = new NativeLibraryLinux();
    }

    if (System.getProperty("os.arch").toLowerCase().contains("ppc")) {
      if (osType == LINUX) {
        MCL_CURRENT = 0x2000;
      } else if (osType == AIX) {
        MCL_CURRENT = 0x100;
      } else {
        MCL_CURRENT = 1;
      }
    } else {
      MCL_CURRENT = 1;
    }
  }

  private NativeLibrary() {
  }

  /**
   * @return the detected OSType of the Operating System running the JVM using crude string matching
   */
  private static OSType getOsType() {
    String osName = System.getProperty("os.name").toLowerCase();
    if (osName.contains("linux"))
      return LINUX;
    else if (osName.contains("mac"))
      return MAC;
    else if (osName.contains("windows"))
      return WINDOWS;

    StartLog.warn("The current operating system, %s, is unsupported by Solr", osName);
    if (osName.contains("aix"))
      return AIX;
    else
      // fall back to the Linux impl for all unknown OS types until otherwise implicitly supported as needed
      return LINUX;
  }

  /**
   * Checks if the library has been successfully linked.
   *
   * @return {@code true} if the library has been successfully linked, {@code false} otherwise.
   */
  public static boolean isAvailable() {
    return wrappedLibrary.isAvailable();
  }

  public static boolean jnaMemoryLockable() {
    return jnaLockable;
  }

  public static void tryMlockall() {
    try {
      wrappedLibrary.callMlockall(MCL_CURRENT);
      jnaLockable = true;
      StartLog.info("JNA mlockall successful");
    } catch (UnsatisfiedLinkError e) {
      // this will have already been logged by CLibrary, no need to repeat it
    } catch (RuntimeException e) {
      if (errno(e) == ENOMEM && osType == LINUX) {
        StartLog.warn("Unable to lock JVM memory (ENOMEM)."
                + " This can result in part of the JVM being swapped out");
      } else if (osType != MAC) {
        // OS X allows mlockall to be called, but always returns an error
        StartLog.warn("Unknown mlockall error %s", errno(e));
      } else {
        StartLog.warn("Cannot lock memory on macOS, error %s", errno(e));
      }
    }
  }

  private static int errno(RuntimeException e) {
    assert e instanceof LastErrorException;
    try {
      return ((LastErrorException) e).getErrorCode();
    } catch (NoSuchMethodError x) {
      StartLog.warn("Obsolete version of JNA present; unable to read errno. Upgrade to JNA 3.2.7 or later");
      return 0;
    }
  }
}
