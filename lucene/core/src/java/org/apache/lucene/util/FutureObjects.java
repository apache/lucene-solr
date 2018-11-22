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
 * Additional methods from Java 9's <a href="https://docs.oracle.com/javase/9/docs/api/java/util/Objects.html">
 * {@code java.util.Objects}</a>.
 * <p>
 * This class will be removed when Java 9 is minimum requirement.
 * Currently any bytecode is patched to use the Java 9 native
 * classes through MR-JAR (Multi-Release JAR) mechanism.
 * In Java 8 it will use THIS implementation.
 * Because of patching, inside the Java source files we always
 * refer to the Lucene implementations, but the final Lucene
 * JAR files will use the native Java 9 class names when executed
 * with Java 9.
 * @lucene.internal
 */
public final class FutureObjects {
  
  private FutureObjects() {} // no instance

  /**
   * Behaves like Java 9's Objects.checkIndex
   * @see <a href="http://download.java.net/java/jdk9/docs/api/java/util/Objects.html#checkIndex-int-int-">Objects.checkIndex</a>
   */
  public static int checkIndex(int index, int length) {
    if (index < 0 || index >= length) {
      throw new IndexOutOfBoundsException("Index " + index + " out-of-bounds for length " + length);
    }
    return index;
  }
  
  /**
   * Behaves like Java 9's Objects.checkFromToIndex
   * @see <a href="http://download.java.net/java/jdk9/docs/api/java/util/Objects.html#checkFromToIndex-int-int-int-">Objects.checkFromToIndex</a>
   */
  public static int checkFromToIndex(int fromIndex, int toIndex, int length) {
    if (fromIndex < 0 || fromIndex > toIndex || toIndex > length) {
      throw new IndexOutOfBoundsException("Range [" + fromIndex + ", " + toIndex + ") out-of-bounds for length " + length);
    }
    return fromIndex;
  }
  
  /**
   * Behaves like Java 9's Objects.checkFromIndexSize
   * @see <a href="http://download.java.net/java/jdk9/docs/api/java/util/Objects.html#checkFromIndexSize-int-int-int-">Objects.checkFromIndexSize</a>
   */
  public static int checkFromIndexSize(int fromIndex, int size, int length) {
    int end = fromIndex + size;
    if (fromIndex < 0 || fromIndex > end || end > length) {
      throw new IndexOutOfBoundsException("Range [" + fromIndex + ", " + fromIndex + " + " + size + ") out-of-bounds for length " + length);
    }
    return fromIndex;
  }
}
