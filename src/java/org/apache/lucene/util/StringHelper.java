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
 * Methods for manipulating strings.
 *
 * $Id$
 */
public abstract class StringHelper {

  /**
   * Compares two strings, character by character, and returns the
   * first position where the two strings differ from one another.
   *
   * @param s1 The first string to compare
   * @param s2 The second string to compare
   * @return The first position where the two strings differ.
   */
  public static final int stringDifference(String s1, String s2) {
    int len1 = s1.length();
    int len2 = s2.length();
    int len = len1 < len2 ? len1 : len2;
    for (int i = 0; i < len; i++) {
      if (s1.charAt(i) != s2.charAt(i)) {
	      return i;
      }
    }
    return len;
  }


  private StringHelper() {
  }
}
