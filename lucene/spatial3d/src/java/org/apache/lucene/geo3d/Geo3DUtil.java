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
package org.apache.lucene.geo3d;

class Geo3DUtil {

  /** Clips the incoming value to the allowed min/max range before encoding, instead of throwing an exception. */
  public static int encodeValueLenient(double planetMax, double x) {
    if (x > planetMax) {
      x = planetMax;
    } else if (x < -planetMax) {
      x = -planetMax;
    }
    return encodeValue(planetMax, x);
  }

  public static int encodeValue(double planetMax, double x) {
    if (x > planetMax) {
      throw new IllegalArgumentException("value=" + x + " is out-of-bounds (greater than planetMax=" + planetMax + ")");
    }
    if (x < -planetMax) {
      throw new IllegalArgumentException("value=" + x + " is out-of-bounds (less than than -planetMax=" + -planetMax + ")");
    }
    long y = Math.round (x * (Integer.MAX_VALUE / planetMax));
    assert y >= Integer.MIN_VALUE;
    assert y <= Integer.MAX_VALUE;

    return (int) y;
  }

  /** Center decode */
  public static double decodeValueCenter(double planetMax, int x) {
    return x * (planetMax / Integer.MAX_VALUE);
  }

  /** More negative decode, at bottom of cell */
  public static double decodeValueMin(double planetMax, int x) {
    return (((double)x) - 0.5) * (planetMax / Integer.MAX_VALUE);
  }
  
  /** More positive decode, at top of cell  */
  public static double decodeValueMax(double planetMax, int x) {
    return (((double)x) + 0.5) * (planetMax / Integer.MAX_VALUE);
  }
}
