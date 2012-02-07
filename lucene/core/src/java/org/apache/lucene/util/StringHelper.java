package org.apache.lucene.util;

import java.util.Comparator;
import java.util.StringTokenizer;

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
   * Compares two {@link BytesRef}, element by element, and returns the
   * number of elements common to both arrays.
   *
   * @param left The first {@link BytesRef} to compare
   * @param right The second {@link BytesRef} to compare
   * @return The number of common elements.
   */
  public static int bytesDifference(BytesRef left, BytesRef right) {
    int len = left.length < right.length ? left.length : right.length;
    final byte[] bytesLeft = left.bytes;
    final int offLeft = left.offset;
    byte[] bytesRight = right.bytes;
    final int offRight = right.offset;
    for (int i = 0; i < len; i++)
      if (bytesLeft[i+offLeft] != bytesRight[i+offRight])
        return i;
    return len;
  }

  private StringHelper() {
  }
  
  /**
   * @return a Comparator over versioned strings such as X.YY.Z
   * @lucene.internal
   */
  public static Comparator<String> getVersionComparator() {
    return versionComparator;
  }
  
  private static Comparator<String> versionComparator = new Comparator<String>() {
    public int compare(String a, String b) {
      StringTokenizer aTokens = new StringTokenizer(a, ".");
      StringTokenizer bTokens = new StringTokenizer(b, ".");
      
      while (aTokens.hasMoreTokens()) {
        int aToken = Integer.parseInt(aTokens.nextToken());
        if (bTokens.hasMoreTokens()) {
          int bToken = Integer.parseInt(bTokens.nextToken());
          if (aToken != bToken) {
            return aToken < bToken ? -1 : 1;
          }
        } else {
          // a has some extra trailing tokens. if these are all zeroes, thats ok.
          if (aToken != 0) {
            return 1; 
          }
        }
      }
      
      // b has some extra trailing tokens. if these are all zeroes, thats ok.
      while (bTokens.hasMoreTokens()) {
        if (Integer.parseInt(bTokens.nextToken()) != 0)
          return -1;
      }
      
      return 0;
    }
  };

  public static boolean equals(String s1, String s2) {
    if (s1 == null) {
      return s2 == null;
    } else {
      return s1.equals(s2);
    }
  }

  /**
   * Returns <code>true</code> iff the ref starts with the given prefix.
   * Otherwise <code>false</code>.
   * 
   * @param ref
   *          the {@link BytesRef} to test
   * @param prefix
   *          the expected prefix
   * @return Returns <code>true</code> iff the ref starts with the given prefix.
   *         Otherwise <code>false</code>.
   */
  public static boolean startsWith(BytesRef ref, BytesRef prefix) {
    return sliceEquals(ref, prefix, 0);
  }

  /**
   * Returns <code>true</code> iff the ref ends with the given suffix. Otherwise
   * <code>false</code>.
   * 
   * @param ref
   *          the {@link BytesRef} to test
   * @param suffix
   *          the expected suffix
   * @return Returns <code>true</code> iff the ref ends with the given suffix.
   *         Otherwise <code>false</code>.
   */
  public static boolean endsWith(BytesRef ref, BytesRef suffix) {
    return sliceEquals(ref, suffix, ref.length - suffix.length);
  }
  
  private static boolean sliceEquals(BytesRef sliceToTest, BytesRef other, int pos) {
    if (pos < 0 || sliceToTest.length - pos < other.length) {
      return false;
    }
    int i = sliceToTest.offset + pos;
    int j = other.offset;
    final int k = other.offset + other.length;
    
    while (j < k) {
      if (sliceToTest.bytes[i++] != other.bytes[j++]) {
        return false;
      }
    }
    
    return true;
  }
}
