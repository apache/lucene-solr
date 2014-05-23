package org.apache.lucene.util;

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

import java.util.Comparator;
import java.util.StringTokenizer;

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
    @Override
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

  /** Pass this as the seed to {@link #murmurhash3_x86_32}. */

  // Poached from Guava: set a different salt/seed
  // for each JVM instance, to frustrate hash key collision
  // denial of service attacks, and to catch any places that
  // somehow rely on hash function/order across JVM
  // instances:
  public static final int GOOD_FAST_HASH_SEED;

  static {
    String prop = System.getProperty("tests.seed");
    if (prop != null) {
      // So if there is a test failure that relied on hash
      // order, we remain reproducible based on the test seed:
      if (prop.length() > 8) {
        prop = prop.substring(prop.length()-8);
      }
      GOOD_FAST_HASH_SEED = (int) Long.parseLong(prop, 16);
    } else {
      GOOD_FAST_HASH_SEED = (int) System.currentTimeMillis();
    }
  }

  /** Returns the MurmurHash3_x86_32 hash.
   * Original source/tests at https://github.com/yonik/java_util/
   */
  @SuppressWarnings("fallthrough")
  public static int murmurhash3_x86_32(byte[] data, int offset, int len, int seed) {

    final int c1 = 0xcc9e2d51;
    final int c2 = 0x1b873593;

    int h1 = seed;
    int roundedEnd = offset + (len & 0xfffffffc);  // round down to 4 byte block

    for (int i=offset; i<roundedEnd; i+=4) {
      // little endian load order
      int k1 = (data[i] & 0xff) | ((data[i+1] & 0xff) << 8) | ((data[i+2] & 0xff) << 16) | (data[i+3] << 24);
      k1 *= c1;
      k1 = Integer.rotateLeft(k1, 15);
      k1 *= c2;

      h1 ^= k1;
      h1 = Integer.rotateLeft(h1, 13);
      h1 = h1*5+0xe6546b64;
    }

    // tail
    int k1 = 0;

    switch(len & 0x03) {
      case 3:
        k1 = (data[roundedEnd + 2] & 0xff) << 16;
        // fallthrough
      case 2:
        k1 |= (data[roundedEnd + 1] & 0xff) << 8;
        // fallthrough
      case 1:
        k1 |= (data[roundedEnd] & 0xff);
        k1 *= c1;
        k1 = Integer.rotateLeft(k1, 15);
        k1 *= c2;
        h1 ^= k1;
    }

    // finalization
    h1 ^= len;

    // fmix(h1);
    h1 ^= h1 >>> 16;
    h1 *= 0x85ebca6b;
    h1 ^= h1 >>> 13;
    h1 *= 0xc2b2ae35;
    h1 ^= h1 >>> 16;

    return h1;
  }

  public static int murmurhash3_x86_32(BytesRef bytes, int seed) {
    return murmurhash3_x86_32(bytes.bytes, bytes.offset, bytes.length, seed);
  }
}
