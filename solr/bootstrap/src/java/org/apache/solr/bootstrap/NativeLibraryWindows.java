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

import com.sun.jna.Native;
import org.eclipse.jetty.start.StartLog;

import java.util.Collections;

/**
 * A {@code NativeLibraryWrapper} implementation for Windows.
 * <p> This implementation only offers support for the {@code callGetpid} method
 * using the Windows/Kernel32 library.</p>
 *
 * @see NativeLibraryWrapper
 * @see NativeLibrary
 */
public class NativeLibraryWindows implements NativeLibraryWrapper {
  private static boolean available;

  static {
    try {
      Native.register(com.sun.jna.NativeLibrary.getInstance("kernel32", Collections.emptyMap()));
      available = true;
    } catch (NoClassDefFoundError e) {
      StartLog.warn("JNA not found. Native methods will be disabled.");
    } catch (UnsatisfiedLinkError e) {
      StartLog.error("Failed to link the Windows/Kernel32 library against JNA. Native methods will be unavailable.", e);
    } catch (NoSuchMethodError e) {
      StartLog.warn("Obsolete version of JNA present; unable to register Windows/Kernel32 library. Upgrade to JNA 3.2.7 or later");
    }
  }

  public int callMlockall(int flags) throws UnsatisfiedLinkError, RuntimeException {
    throw new UnsatisfiedLinkError();
  }

  public int callMunlockall() throws UnsatisfiedLinkError, RuntimeException {
    throw new UnsatisfiedLinkError();
  }

  public boolean isAvailable() {
    return available;
  }
}
