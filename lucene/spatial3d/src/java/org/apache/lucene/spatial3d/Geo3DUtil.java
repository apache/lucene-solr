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
  static final double DECODE = 1/MUL;
  private static final int MIN_ENCODED_VALUE = encodeValue(-MAX_VALUE);

  public static int encodeValue(double x) {
    if (x > MAX_VALUE) {
      throw new IllegalArgumentException("value=" + x + " is out-of-bounds (greater than WGS84's planetMax=" + MAX_VALUE + ")");
    }
    if (x < -MAX_VALUE) {
      throw new IllegalArgumentException("value=" + x + " is out-of-bounds (less than than WGS84's -planetMax=" + -MAX_VALUE + ")");
    }
    // the maximum possible value cannot be encoded without overflow
    if (x == MAX_VALUE) {
      x = Math.nextDown(x);
    }
    long result = (long) Math.floor(x / DECODE);
    //System.out.println("    enc: " + x + " -> " + result);
    assert result >= Integer.MIN_VALUE;
    assert result <= Integer.MAX_VALUE;
    return (int) result;
  }

  public static double decodeValue(int x) {
    // We decode to the center value; this keeps the encoding stable
    return (x+0.5) * DECODE;
  }

  /** Returns smallest double that would encode to int x. */
  // NOTE: keep this package private!!
  static double decodeValueFloor(int x) {
    return x * DECODE;
  }
  
  /** Returns largest double that would encode to int x. */
  // NOTE: keep this package private!!
  static double decodeValueCeil(int x) {
    if (x == Integer.MAX_VALUE) {
      return MAX_VALUE;
    } else {
      return Math.nextDown((x+1) * DECODE);
    }
  }
  
}
