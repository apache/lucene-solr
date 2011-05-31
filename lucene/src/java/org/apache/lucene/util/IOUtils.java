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

import java.io.Closeable;
import java.io.IOException;

/** @lucene.internal */
public final class IOUtils {

  private IOUtils() {} // no instance

  /**
   * <p>Closes all given <tt>Closeable</tt>s, suppressing all thrown exceptions. Some of the <tt>Closeable</tt>s
   * may be null, they are ignored. After everything is closed, method either throws <tt>priorException</tt>,
   * if one is supplied, or the first of suppressed exceptions, or completes normally.</p>
   * <p>Sample usage:<br/>
   * <pre>
   * Closeable resource1 = null, resource2 = null, resource3 = null;
   * ExpectedException priorE = null;
   * try {
   *   resource1 = ...; resource2 = ...; resource3 = ...; // Acquisition may throw ExpectedException
   *   ..do..stuff.. // May throw ExpectedException
   * } catch (ExpectedException e) {
   *   priorE = e;
   * } finally {
   *   closeSafely(priorE, resource1, resource2, resource3);
   * }
   * </pre>
   * </p>
   * @param priorException  <tt>null</tt> or an exception that will be rethrown after method completion
   * @param objects         objects to call <tt>close()</tt> on
   */
  public static <E extends Exception> void closeSafely(E priorException, Closeable... objects) throws E, IOException {
    Throwable th = null;

    for (Closeable object : objects) {
      try {
        if (object != null) {
          object.close();
        }
      } catch (Throwable t) {
        if (th == null) {
          th = t;
        }
      }
    }

    if (priorException != null) {
      throw priorException;
    } else if (th != null) {
      if (th instanceof IOException) throw (IOException) th;
      if (th instanceof RuntimeException) throw (RuntimeException) th;
      if (th instanceof Error) throw (Error) th;
      throw new RuntimeException(th);
    }
  }

  /** @see #closeSafely(Exception, Closeable...) */
  public static <E extends Exception> void closeSafely(E priorException, Iterable<Closeable> objects) throws E, IOException {
    Throwable th = null;

    for (Closeable object : objects) {
      try {
        if (object != null) {
          object.close();
        }
      } catch (Throwable t) {
        if (th == null) {
          th = t;
        }
      }
    }

    if (priorException != null) {
      throw priorException;
    } else if (th != null) {
      if (th instanceof IOException) throw (IOException) th;
      if (th instanceof RuntimeException) throw (RuntimeException) th;
      if (th instanceof Error) throw (Error) th;
      throw new RuntimeException(th);
    }
  }

  /**
   * Closes all given <tt>Closeable</tt>s, suppressing all thrown exceptions.
   * Some of the <tt>Closeable</tt>s may be null, they are ignored. After
   * everything is closed, and if {@code suppressExceptions} is {@code false},
   * method either throws the first of suppressed exceptions, or completes
   * normally.
   * 
   * @param suppressExceptions
   *          if true then exceptions that occur during close() are suppressed
   * @param objects
   *          objects to call <tt>close()</tt> on
   */
  public static void closeSafely(boolean suppressExceptions, Closeable... objects) throws IOException {
    Throwable th = null;

    for (Closeable object : objects) {
      try {
        if (object != null) {
          object.close();
        }
      } catch (Throwable t) {
        if (th == null)
          th = t;
      }
    }

    if (th != null && !suppressExceptions) {
      if (th instanceof IOException) throw (IOException) th;
      if (th instanceof RuntimeException) throw (RuntimeException) th;
      if (th instanceof Error) throw (Error) th;
      throw new RuntimeException(th);
    }
  }
  
  /**
   * @see #closeSafely(boolean, Closeable...)
   */
  public static void closeSafely(boolean suppressExceptions, Iterable<? extends Closeable> objects) throws IOException {
    Throwable th = null;

    for (Closeable object : objects) {
      try {
        if (object != null) {
          object.close();
        }
      } catch (Throwable t) {
        if (th == null)
          th = t;
      }
    }

    if (th != null && !suppressExceptions) {
      if (th instanceof IOException) throw (IOException) th;
      if (th instanceof RuntimeException) throw (RuntimeException) th;
      if (th instanceof Error) throw (Error) th;
      throw new RuntimeException(th);
    }
  }

}
