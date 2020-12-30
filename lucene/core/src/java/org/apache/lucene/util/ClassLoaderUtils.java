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

/**
 * Helper class used by ServiceLoader to investigate parent/child relationships of {@link
 * ClassLoader}s.
 *
 * @lucene.internal
 */
public interface ClassLoaderUtils {

  /**
   * Utility method to check if some class loader is a (grand-)parent of or the same as another one.
   * This means the child will be able to load all classes from the parent, too.
   *
   * <p>If caller's codesource doesn't have enough permissions to do the check, {@code false} is
   * returned (this is fine, because if we get a {@code SecurityException} it is for sure no
   * parent).
   */
  public static boolean isParentClassLoader(final ClassLoader parent, final ClassLoader child) {
    try {
      ClassLoader cl = child;
      while (cl != null) {
        if (cl == parent) {
          return true;
        }
        cl = cl.getParent();
      }
      return false;
    } catch (SecurityException se) {
      return false;
    }
  }
}
