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
package org.apache.lucene.spatial3d;

import org.apache.lucene.spatial3d.geom.PlanetModel;

class Geo3DUtil {

  private static final double MAX_VALUE = PlanetModel.WGS84.getMaximumMagnitude();
  private static final int BITS = 32;
  private static final double MUL = (0x1L<<BITS)/(2*MAX_VALUE);
  static final double DECODE = getNextSafeDouble(1/MUL);
  private static final int MIN_ENCODED_VALUE = encodeValue(-MAX_VALUE);

  public static int encodeValue(double x) {
    if (x > MAX_VALUE) {
      throw new IllegalArgumentException("value=" + x + " is out-of-bounds (greater than WGS84's planetMax=" + MAX_VALUE + ")");
    }
    if (x < -MAX_VALUE) {
      throw new IllegalArgumentException("value=" + x + " is out-of-bounds (less than than WGS84's -planetMax=" + -MAX_VALUE + ")");
    }
    long result = (long) Math.floor(x / DECODE);
    //System.out.println("    enc: " + x + " -> " + result);
    assert result >= Integer.MIN_VALUE;
    assert result <= Integer.MAX_VALUE;
    return (int) result;
  }

  public static double decodeValue(int x) {
    double result;
    if (x == MIN_ENCODED_VALUE) {
      // We must special case this, because -MAX_VALUE is not guaranteed to land precisely at a floor value, and we don't ever want to
      // return a value outside of the planet's range (I think?).  The max value is "safe" because we floor during encode:
      result = -MAX_VALUE;
    } else {
      result = x * DECODE;
    }
    assert result >= -MAX_VALUE && result <= MAX_VALUE;
    return result;
  }

  /** Returns a double value >= x such that if you multiply that value by an int, and then
   *  divide it by that int again, you get precisely the same value back */
  private static double getNextSafeDouble(double x) {

    // Move to double space:
    long bits = Double.doubleToLongBits(x);

    // Make sure we are beyond the actual maximum value:
    bits += Integer.MAX_VALUE;

    // Clear the bottom 32 bits:
    bits &= ~((long) Integer.MAX_VALUE);

    // Convert back to double:
    double result = Double.longBitsToDouble(bits);
    assert result > x;
    return result;
  }
}
