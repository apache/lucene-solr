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

import java.util.Arrays;

public class TestSmallFloat extends LuceneTestCase {

  // original lucene byteToFloat
  static float orig_byteToFloat(byte b) {
    if (b == 0)                                   // zero is a special case
      return 0.0f;
    int mantissa = b & 7;
    int exponent = (b >> 3) & 31;
    int bits = ((exponent+(63-15)) << 24) | (mantissa << 21);
    return Float.intBitsToFloat(bits);
  }

  // original lucene floatToByte (since lucene 1.3)
  static byte orig_floatToByte_v13(float f) {
    if (f < 0.0f)                                 // round negatives up to zero
      f = 0.0f;

    if (f == 0.0f)                                // zero is a special case
      return 0;

    int bits = Float.floatToIntBits(f);           // parse float into parts
    int mantissa = (bits & 0xffffff) >> 21;
    int exponent = (((bits >> 24) & 0x7f) - 63) + 15;

    if (exponent > 31) {                          // overflow: use max value
      exponent = 31;
      mantissa = 7;
    }

    if (exponent < 0) {                           // underflow: use min value
      exponent = 0;
      mantissa = 1;
    }

    return (byte)((exponent << 3) | mantissa);    // pack into a byte
  }

  // This is the original lucene floatToBytes (from v1.3)
  // except with the underflow detection bug fixed for values like 5.8123817E-10f
  static byte orig_floatToByte(float f) {
    if (f < 0.0f)                                 // round negatives up to zero
      f = 0.0f;

    if (f == 0.0f)                                // zero is a special case
      return 0;

    int bits = Float.floatToIntBits(f);           // parse float into parts
    int mantissa = (bits & 0xffffff) >> 21;
    int exponent = (((bits >> 24) & 0x7f) - 63) + 15;

    if (exponent > 31) {                          // overflow: use max value
      exponent = 31;
      mantissa = 7;
    }

    if (exponent < 0 || exponent == 0 && mantissa == 0) { // underflow: use min value
      exponent = 0;
      mantissa = 1;
    }

    return (byte)((exponent << 3) | mantissa);    // pack into a byte
  }


  public void testByteToFloat() {
    for (int i=0; i<256; i++) {
      float f1 = orig_byteToFloat((byte)i);
      float f2 = SmallFloat.byteToFloat((byte)i, 3,15);
      float f3 = SmallFloat.byte315ToFloat((byte)i);
      assertEquals(f1,f2,0.0);
      assertEquals(f2,f3,0.0);
    }
  }

  public void testFloatToByte() {
    assertEquals(0, orig_floatToByte_v13(5.8123817E-10f));       // verify the old bug (see LUCENE-2937)
    assertEquals(1, orig_floatToByte(5.8123817E-10f));           // verify it's fixed in this test code
    assertEquals(1, SmallFloat.floatToByte315(5.8123817E-10f));  // verify it's fixed

    // test some constants
    assertEquals(0, SmallFloat.floatToByte315(0));
    assertEquals(1, SmallFloat.floatToByte315(Float.MIN_VALUE));             // underflow rounds up to smallest positive
    assertEquals(255, SmallFloat.floatToByte315(Float.MAX_VALUE) & 0xff);    // overflow rounds down to largest positive
    assertEquals(255, SmallFloat.floatToByte315(Float.POSITIVE_INFINITY) & 0xff);

    // all negatives map to 0
    assertEquals(0, SmallFloat.floatToByte315(-Float.MIN_VALUE));
    assertEquals(0, SmallFloat.floatToByte315(-Float.MAX_VALUE));
    assertEquals(0, SmallFloat.floatToByte315(Float.NEGATIVE_INFINITY));


    // up iterations for more exhaustive test after changing something
    int num = atLeast(100000);
    for (int i = 0; i < num; i++) {
      float f = Float.intBitsToFloat(random().nextInt());
      if (Float.isNaN(f)) continue;    // skip NaN
      byte b1 = orig_floatToByte(f);
      byte b2 = SmallFloat.floatToByte(f,3,15);
      byte b3 = SmallFloat.floatToByte315(f);
      assertEquals(b1,b2);
      assertEquals(b2,b3);
    }
  }

  public void testInt4() {
    for (int i = 0; i <= 16; ++i) {
      // all values in 0-16 are encoded accurately
      assertEquals(i, SmallFloat.int4ToLong(SmallFloat.longToInt4(i)));
    }
    final int maxEncoded = SmallFloat.longToInt4(Long.MAX_VALUE);
    for (int i = 1; i < maxEncoded; ++i) {
      assertTrue(SmallFloat.int4ToLong(i) > SmallFloat.int4ToLong(i - 1));
    }
    final int iters = atLeast(1000);
    for (int iter = 0; iter < iters; ++iter) {
      final long l = TestUtil.nextLong(random(), 0, 1L << TestUtil.nextInt(random(), 5, 61));
      int numBits = 64 - Long.numberOfLeadingZeros(l);
      long expected = l;
      if (numBits > 4) {
        long mask = ~0L << (numBits - 4);
        expected &= mask;
      }
      long l2 = SmallFloat.int4ToLong(SmallFloat.longToInt4(l));
      assertEquals(expected, l2);
    }
  }

  public void testByte4() {
    int[] decoded = new int[256];
    for (int b = 0; b < 256; ++b) {
      decoded[b] = SmallFloat.byte4ToInt((byte) b);
      assertEquals((byte) b, SmallFloat.intToByte4(decoded[b]));
    }
    for (int i = 1; i < 256; ++i) {
      assertTrue(decoded[i] > decoded[i-1]);
    }
    assertEquals((byte) 255, SmallFloat.intToByte4(Integer.MAX_VALUE));
    final int iters = atLeast(1000);
    for (int iter = 0; iter < iters; ++iter) {
      final int i = random().nextInt(1 << TestUtil.nextInt(random(), 5, 30));
      int idx = Arrays.binarySearch(decoded, i);
      if (idx < 0) {
        idx = -2 - idx;
      }
      assertTrue(decoded[idx] <= i);
      assertEquals((byte) idx, SmallFloat.intToByte4(i));
    }
  }

  /***
  // Do an exhaustive test of all possible floating point values
  // for the 315 float against the original norm encoding in Similarity.
  // Takes 75 seconds on my Pentium4 3GHz, with Java5 -server
  public void testAllFloats() {
    for(int i = Integer.MIN_VALUE;;i++) {
      float f = Float.intBitsToFloat(i);
      if (f==f) { // skip non-numbers
        byte b1 = orig_floatToByte(f);
        byte b2 = SmallFloat.floatToByte315(f);
        if (b1!=b2 || b2==0 && f>0) {
          fail("Failed floatToByte315 for float " + f + " source bits="+Integer.toHexString(i) + " float raw bits=" + Integer.toHexString(Float.floatToRawIntBits(i)));
        }
      }
      if (i==Integer.MAX_VALUE) break;
    }
  }
  ***/
}
