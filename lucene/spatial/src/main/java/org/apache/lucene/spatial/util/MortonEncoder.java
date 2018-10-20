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
package org.apache.lucene.spatial.util;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.util.BitUtil;

import static org.apache.lucene.geo.GeoUtils.checkLatitude;
import static org.apache.lucene.geo.GeoUtils.checkLongitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitudeCeil;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitudeCeil;

/**
 * Quantizes lat/lon points and bit interleaves them into a binary morton code
 * in the range of 0x00000000... : 0xFFFFFFFF...
 * https://en.wikipedia.org/wiki/Z-order_curve
 *
 * This is useful for bitwise operations in raster space
 *
 * @lucene.experimental
 */
public class MortonEncoder {

  private MortonEncoder() {} // no instance

  /**
   * Main encoding method to quantize lat/lon points and bit interleave them into a binary morton code
   * in the range of 0x00000000... : 0xFFFFFFFF...
   *
   * @param latitude latitude value: must be within standard +/-90 coordinate bounds.
   * @param longitude longitude value: must be within standard +/-180 coordinate bounds.
   * @return bit interleaved encoded values as a 64-bit {@code long}
   * @throws IllegalArgumentException if latitude or longitude is out of bounds
   */
  public static final long encode(double latitude, double longitude) {
    checkLatitude(latitude);
    checkLongitude(longitude);
    // encode lat/lon flipping the sign bit so negative ints sort before positive ints
    final int latEnc = encodeLatitude(latitude) ^ 0x80000000;
    final int lonEnc = encodeLongitude(longitude) ^ 0x80000000;
    return BitUtil.interleave(lonEnc, latEnc);
  }

  /**
   * Quantizes lat/lon points and bit interleaves them into a sortable morton code
   * ranging from 0x00 : 0xFF...
   * https://en.wikipedia.org/wiki/Z-order_curve
   * This is useful for bitwise operations in raster space
   * @param latitude latitude value: must be within standard +/-90 coordinate bounds.
   * @param longitude longitude value: must be within standard +/-180 coordinate bounds.
   * @return bit interleaved encoded values as a 64-bit {@code long}
   * @throws IllegalArgumentException if latitude or longitude is out of bounds
   */
  public static final long encodeCeil(double latitude, double longitude) {
    checkLatitude(latitude);
    checkLongitude(longitude);
    // encode lat/lon flipping the sign bit so negative ints sort before positive ints
    final int latEnc = encodeLatitudeCeil(latitude) ^ 0x80000000;
    final int lonEnc = encodeLongitudeCeil(longitude) ^ 0x80000000;
    return BitUtil.interleave(lonEnc, latEnc);
  }

  /** decode latitude value from morton encoded geo point */
  public static final double decodeLatitude(final long hash) {
    // decode lat/lon flipping the sign bit so negative ints sort before positive ints
    return GeoEncodingUtils.decodeLatitude((int) BitUtil.deinterleave(hash >>> 1) ^ 0x80000000);
  }

  /** decode longitude value from morton encoded geo point */
  public static final double decodeLongitude(final long hash) {
    // decode lat/lon flipping the sign bit so negative ints sort before positive ints
    return GeoEncodingUtils.decodeLongitude((int) BitUtil.deinterleave(hash) ^ 0x80000000);
  }

  /** Converts a long value into a full 64 bit string (useful for debugging) */
  public static String geoTermToString(long term) {
    StringBuilder s = new StringBuilder(64);
    final int numberOfLeadingZeros = Long.numberOfLeadingZeros(term);
    for (int i = 0; i < numberOfLeadingZeros; i++) {
      s.append('0');
    }
    if (term != 0) {
      s.append(Long.toBinaryString(term));
    }
    return s.toString();
  }
}
