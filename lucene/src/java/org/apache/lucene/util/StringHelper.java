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


/**
 * Methods for manipulating strings.
 *
 * @lucene.internal
 */
public abstract class StringHelper {
  /**
   * Expert:
   * The StringInterner implementation used by Lucene.
   * This shouldn't be changed to an incompatible implementation after other Lucene APIs have been used.
   */
  public static StringInterner interner = new SimpleStringInterner(1024,8);

  /** Return the same string object for all equal strings */
  public static String intern(String s) {
    return interner.intern(s);
  }

  /**
   * Compares two byte[] arrays, element by element, and returns the
   * number of elements common to both arrays.
   *
   * @param bytes1 The first byte[] to compare
   * @param bytes2 The second byte[] to compare
   * @return The number of common elements.
   */
  public static int bytesDifference(byte[] bytes1, int len1, byte[] bytes2, int len2) {
    int len = len1 < len2 ? len1 : len2;
    for (int i = 0; i < len; i++)
      if (bytes1[i] != bytes2[i])
        return i;
    return len;
  }

  private StringHelper() {
  }
}
