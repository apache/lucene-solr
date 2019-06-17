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
package org.apache.lucene.geo;

import org.apache.lucene.util.NumericUtils;


/**
 * reusable cartesian geometry encoding methods
 *
 * @lucene.experimental
 */
public final class XYEncodingUtils {

  public static final double MIN_VAL_INCL = -Float.MAX_VALUE;
  public static final double MAX_VAL_INCL = Float.MAX_VALUE;

  // No instance:
  private XYEncodingUtils() {
  }

  /** validates value is within +/-{@link Float#MAX_VALUE} coordinate bounds */
  public static void checkVal(double x) {
    if (Double.isNaN(x) || x < MIN_VAL_INCL || x > MAX_VAL_INCL) {
      throw new IllegalArgumentException("invalid value " + x + "; must be between " + MIN_VAL_INCL + " and " + MAX_VAL_INCL);
    }
  }

  /**
   * Quantizes double (64 bit) values into 32 bits
   * @param x cartesian value
   * @return encoded value as a 32-bit {@code int}
   * @throws IllegalArgumentException if value is out of bounds
   */
  public static int encode(double x) {
    checkVal(x);
    return NumericUtils.floatToSortableInt((float)x);
  }

  /**
   * Turns quantized value from {@link #encode} back into a double.
   * @param encoded encoded value: 32-bit quantized value.
   * @return decoded value value.
   */
  public static double decode(int encoded) {
    double result = NumericUtils.sortableIntToFloat(encoded);
    assert result >=  MIN_VAL_INCL && result <= MAX_VAL_INCL;
    return result;
  }

  /**
   * Turns quantized value from byte array back into a double.
   * @param src byte array containing 4 bytes to decode at {@code offset}
   * @param offset offset into {@code src} to decode from.
   * @return decoded value.
   */
  public static double decode(byte[] src, int offset) {
    return decode(NumericUtils.sortableBytesToInt(src, offset));
  }
}
