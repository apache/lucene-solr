/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lucene.geo;

import org.apache.lucene.util.NumericUtils;

public class XYEncodingUtils {

  public static final double MIN_VAL_INCL = -Float.MAX_VALUE;
  public static final double MAX_VAL_INCL = Float.MAX_VALUE;

  // No instance:
  private XYEncodingUtils() {
  }

  public static void checkVal(double x) {
    if (Double.isNaN(x) || x < MIN_VAL_INCL || x > MAX_VAL_INCL) {
      throw new IllegalArgumentException("invalid x value " + x + "; must be between " + MIN_VAL_INCL + " and " + MAX_VAL_INCL);
    }
  }

  public static int encode(double x) {
    checkVal(x);
    return NumericUtils.floatToSortableInt((float)x);
  }

  public static double decode(int encoded) {
    double result = NumericUtils.sortableIntToFloat(encoded);
    assert result >=  MIN_VAL_INCL && result <= MAX_VAL_INCL;
    return result;
  }

  public static double decode(byte[] src, int offset) {
    return decode(NumericUtils.sortableBytesToInt(src, offset));
  }
}
