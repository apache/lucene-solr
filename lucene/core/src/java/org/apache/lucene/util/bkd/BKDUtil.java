package org.apache.lucene.util.bkd;

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

import java.math.BigInteger;
import java.util.Arrays;

/** Utility methods to handle N-dimensional packed byte[] as if they were numbers! */
final class BKDUtil {

  private BKDUtil() {
    // No instance
  }

  /** result = a - b, where a >= b */
  public static void subtract(int bytesPerDim, int dim, byte[] a, byte[] b, byte[] result) {
    int start = dim * bytesPerDim;
    int end = start + bytesPerDim;
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

  static void intToBytes(int x, byte[] dest, int index) {
    // Flip the sign bit, so negative ints sort before positive ints correctly:
    x ^= 0x80000000;
    for(int i=0;i<4;i++) {
      dest[4*index+i] = (byte) (x >> 24-i*8);
    }
  }

  static int bytesToInt(byte[] src, int index) {
    int x = 0;
    for(int i=0;i<4;i++) {
      x |= (src[4*index+i] & 0xff) << (24-i*8);
    }
    // Re-flip the sign bit to restore the original value:
    return x ^ 0x80000000;
  }

  static void sortableBigIntBytes(byte[] bytes) {
    bytes[0] ^= 0x80;
    for(int i=1;i<bytes.length;i++)  {
      bytes[i] ^= 0;
    }
  }

  static void bigIntToBytes(BigInteger bigInt, byte[] result, int dim, int numBytesPerDim) {
    byte[] bigIntBytes = bigInt.toByteArray();
    byte[] fullBigIntBytes;

    if (bigIntBytes.length < numBytesPerDim) {
      fullBigIntBytes = new byte[numBytesPerDim];
      System.arraycopy(bigIntBytes, 0, fullBigIntBytes, numBytesPerDim-bigIntBytes.length, bigIntBytes.length);
      if ((bigIntBytes[0] & 0x80) != 0) {
        // sign extend
        Arrays.fill(fullBigIntBytes, 0, numBytesPerDim-bigIntBytes.length, (byte) 0xff);
      }
    } else {
      assert bigIntBytes.length == numBytesPerDim;
      fullBigIntBytes = bigIntBytes;
    }
    sortableBigIntBytes(fullBigIntBytes);

    System.arraycopy(fullBigIntBytes, 0, result, dim * numBytesPerDim, numBytesPerDim);

    assert bytesToBigInt(result, dim, numBytesPerDim).equals(bigInt): "bigInt=" + bigInt + " converted=" + bytesToBigInt(result, dim, numBytesPerDim);
  }

  static BigInteger bytesToBigInt(byte[] bytes, int dim, int numBytesPerDim) {
    byte[] bigIntBytes = new byte[numBytesPerDim];
    System.arraycopy(bytes, dim*numBytesPerDim, bigIntBytes, 0, numBytesPerDim);
    sortableBigIntBytes(bigIntBytes);
    return new BigInteger(bigIntBytes);
  }
}
