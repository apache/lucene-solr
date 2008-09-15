package org.apache.lucene.util;

/**
 * Copyright 2005 The Apache Software Foundation
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

import org.apache.lucene.util.LuceneTestCase;
import java.util.Random;

/**
 * @version $Id$
 */
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

  // original lucene floatToByte
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

    if (exponent < 0) {                           // underflow: use min value
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

      float f4 = SmallFloat.byteToFloat((byte)i,5,2);
      float f5 = SmallFloat.byte52ToFloat((byte)i);
      assertEquals(f4,f5,0.0);
    }
  }

  public void testFloatToByte() {
    Random rand = new Random(0);
    // up iterations for more exhaustive test after changing something
    for (int i=0; i<100000; i++) {
      float f = Float.intBitsToFloat(rand.nextInt());
      if (f!=f) continue;    // skip NaN
      byte b1 = orig_floatToByte(f);
      byte b2 = SmallFloat.floatToByte(f,3,15);
      byte b3 = SmallFloat.floatToByte315(f);
      assertEquals(b1,b2);
      assertEquals(b2,b3);

      byte b4 = SmallFloat.floatToByte(f,5,2);
      byte b5 = SmallFloat.floatToByte52(f);
      assertEquals(b4,b5);
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
        if (b1!=b2) {
          TestCase.fail("Failed floatToByte315 for float " + f);
        }
      }
      if (i==Integer.MAX_VALUE) break;
    }
  }
  ***/

}
