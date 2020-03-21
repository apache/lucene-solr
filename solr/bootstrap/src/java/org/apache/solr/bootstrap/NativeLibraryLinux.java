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
import com.sun.jna.Native;
import org.eclipse.jetty.start.StartLog;

import java.util.Collections;

/**
 * A {@code NativeLibraryWrapper} implementation for Linux.
 * <p>
 * When JNA is initialized, all methods that have the 'native' keyword
 * will be attmpted to be linked against. As Java doesn't have the equivalent
 * of a #ifdef, this means if a native method like posix_fadvise is defined in the
 * class but not available on the target operating system (e.g.
 * posix_fadvise is not availble on Darwin/Mac) this will cause the entire
 * initial linking and initialization of JNA to fail. This means other
 * native calls that are supported on that target operating system will be
 * unavailable simply because of one native defined method not supported
 * on the runtime operating system.
 *
 * @see NativeLibraryWrapper
 * @see NativeLibrary
 */
public class NativeLibraryLinux implements NativeLibraryWrapper {
  private static boolean available;

  static {
    try {
      Native.register(com.sun.jna.NativeLibrary.getInstance("c", Collections.emptyMap()));
      available = true;
    } catch (NoClassDefFoundError e) {
      StartLog.warn("JNA not found. Native methods will be disabled.");
    } catch (UnsatisfiedLinkError e) {
      StartLog.error("Failed to link the C library against JNA. Native methods will be unavailable.", e);
    } catch (NoSuchMethodError e) {
      StartLog.warn("Obsolete version of JNA present; unable to register C library. Upgrade to JNA 3.2.7 or later");
    }
  }

  private static native int mlockall(int flags) throws LastErrorException;

  private static native int munlockall() throws LastErrorException;

  public int callMlockall(int flags) throws UnsatisfiedLinkError, RuntimeException {
    return mlockall(flags);
  }

  public int callMunlockall() throws UnsatisfiedLinkError, RuntimeException {
    return munlockall();
  }

  public boolean isAvailable() {
    return available;
  }
}
