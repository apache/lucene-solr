package org.apache.lucene.util.bkd;

import org.apache.lucene.util.BytesRef;

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

/** Utility methods to handle N-dimensional packed byte[] as if they were numbers! */
final class BKDUtil {

  private BKDUtil() {
    // No instance
  }

  /** result = a - b, where a >= b */
  public static void subtract(int bytesPerDim, int dim, byte[] a, byte[] b, byte[] result) {
    System.out.println("subtract a=" + bytesToInt(a, dim) + " b=" + bytesToInt(b, dim));
    int start = dim * bytesPerDim;
    int end = start + bytesPerDim;
    System.out.println("  a=" + new BytesRef(a, start, bytesPerDim));
    System.out.println("  b=" + new BytesRef(b, start, bytesPerDim));
    int borrow = 0;
    for(int i=end-1;i>=start;i--) {
      int diff = (a[i]&0xff) - (b[i]&0xff) - borrow;
      if (diff < 0) {
        diff += 256;
        borrow = 1;
      } else {
        borrow = 0;
      }
      result[i-start] = (byte) diff;
    }
    if (borrow != 0) {
      throw new IllegalArgumentException("a < b?");
    }
  }
  
  /** Returns positive int if a > b, negative int if a < b and 0 if a == b */
  public static int compare(int bytesPerDim, byte[] a, int aIndex, byte[] b, int bIndex) {
    for(int i=0;i<bytesPerDim;i++) {
      int cmp = (a[aIndex*bytesPerDim+i]&0xff) - (b[bIndex*bytesPerDim+i]&0xff);
      if (cmp != 0) {
        return cmp;
      }
    }

    return 0;
  }

  /** Returns true if N-dim rect A contains N-dim rect B */
  public static boolean contains(int bytesPerDim,
                                 byte[] minPackedA, byte[] maxPackedA,
                                 byte[] minPackedB, byte[] maxPackedB) {
    int dims = minPackedA.length / bytesPerDim;
    for(int dim=0;dim<dims;dim++) {
      if (compare(bytesPerDim, minPackedA, dim, minPackedB, dim) > 0) {
        return false;
      }
      if (compare(bytesPerDim, maxPackedA, dim, maxPackedB, dim) < 0) {
        return false;
      }
    }

    return true;
  }

  // nocommit only used temporarily for debugging test cases that encode ints:
  static int bytesToInt(byte[] src, int index) {
    int x = 0;
    for(int i=0;i<4;i++) {
      x |= (src[4*index+i] & 0xff) << (24-i*8);
    }
    // Re-flip the sign bit to restore the original value:
    return x ^ 0x80000000;
  }
}
