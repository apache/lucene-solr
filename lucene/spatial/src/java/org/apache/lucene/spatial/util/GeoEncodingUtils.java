package org.apache.lucene.spatial.util;

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

import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;

import static org.apache.lucene.spatial.util.GeoUtils.MIN_LON_INCL;
import static org.apache.lucene.spatial.util.GeoUtils.MIN_LAT_INCL;

/**
 * Basic reusable geopoint encoding methods
 *
 * @lucene.experimental
 */
public final class GeoEncodingUtils {
  /** number of bits used for quantizing latitude and longitude values */
  public static final short BITS = 31;
  private static final double LON_SCALE = (0x1L<<BITS)/360.0D;
  private static final double LAT_SCALE = (0x1L<<BITS)/180.0D;

  /**
   * The maximum term length (used for <code>byte[]</code> buffer size)
   * for encoding <code>geoEncoded</code> values.
   * @see #geoCodedToPrefixCodedBytes(long, int, BytesRefBuilder)
   */
  public static final int BUF_SIZE_LONG = 28/8 + 1;

  /** rounding error for quantized latitude and longitude values */
  public static final double TOLERANCE = 1E-6;

  // No instance:
  private GeoEncodingUtils() {
  }

  public static final Long mortonHash(final double lon, final double lat) {
    return BitUtil.interleave(scaleLon(lon), scaleLat(lat));
  }

  public static final double mortonUnhashLon(final long hash) {
    return unscaleLon(BitUtil.deinterleave(hash));
  }

  public static final double mortonUnhashLat(final long hash) {
    return unscaleLat(BitUtil.deinterleave(hash >>> 1));
  }

  protected static final long scaleLon(final double val) {
    return (long) ((val-MIN_LON_INCL) * LON_SCALE);
  }

  protected static final long scaleLat(final double val) {
    return (long) ((val-MIN_LAT_INCL) * LAT_SCALE);
  }

  protected static final double unscaleLon(final long val) {
    return (val / LON_SCALE) + MIN_LON_INCL;
  }

  protected static final double unscaleLat(final long val) {
    return (val / LAT_SCALE) + MIN_LAT_INCL;
  }

  /**
   * Compare two position values within a {@link GeoEncodingUtils#TOLERANCE} factor
   */
  public static double compare(final double v1, final double v2) {
    final double delta = v1-v2;
    return Math.abs(delta) <= TOLERANCE ? 0 : delta;
  }

  /**
   * Convert a geocoded morton long into a prefix coded geo term
   */
  public static void geoCodedToPrefixCoded(long hash, int shift, BytesRefBuilder bytes) {
    geoCodedToPrefixCodedBytes(hash, shift, bytes);
  }

  /**
   * Convert a prefix coded geo term back into the geocoded morton long
   */
  public static long prefixCodedToGeoCoded(final BytesRef val) {
    final long result = fromBytes((byte)0, (byte)0, (byte)0, (byte)0,
        val.bytes[val.offset+0], val.bytes[val.offset+1], val.bytes[val.offset+2], val.bytes[val.offset+3]);
    return result << 32;
  }

  /**
   * GeoTerms are coded using 4 prefix bytes + 1 byte to record number of prefix bits
   *
   * example prefix at shift 54 (yields 10 significant prefix bits):
   *  pppppppp pp000000 00000000 00000000 00001010
   *  (byte 1) (byte 2) (byte 3) (byte 4) (sigbits)
   */
  private static void geoCodedToPrefixCodedBytes(final long hash, final int shift, final BytesRefBuilder bytes) {
    // ensure shift is 32..63
    if (shift < 32 || shift > 63) {
      throw new IllegalArgumentException("Illegal shift value, must be 32..63; got shift=" + shift);
    }
    int nChars = BUF_SIZE_LONG + 1; // one extra for the byte that contains the number of significant bits
    bytes.setLength(nChars);
    bytes.grow(nChars--);
    final int sigBits = 64 - shift;
    bytes.setByteAt(BUF_SIZE_LONG, (byte)(sigBits));
    long sortableBits = hash;
    sortableBits >>>= shift;
    sortableBits <<= 32 - sigBits;
    do {
      bytes.setByteAt(--nChars, (byte)(sortableBits));
      sortableBits >>>= 8;
    } while (nChars > 0);
  }

  /** Get the prefix coded geo term shift value */
  public static int getPrefixCodedShift(final BytesRef val) {
    final int shift = val.bytes[val.offset + BUF_SIZE_LONG];
    if (shift > 63 || shift < 0)
      throw new NumberFormatException("Invalid shift value (" + shift + ") in prefixCoded bytes (is encoded value really a geo point?)");
    return shift;
  }

  /** Converts 8 bytes to a long value */
  protected static long fromBytes(byte b1, byte b2, byte b3, byte b4, byte b5, byte b6, byte b7, byte b8) {
    return ((long)b1 & 255L) << 56 | ((long)b2 & 255L) << 48 | ((long)b3 & 255L) << 40
        | ((long)b4 & 255L) << 32 | ((long)b5 & 255L) << 24 | ((long)b6 & 255L) << 16
        | ((long)b7 & 255L) << 8 | (long)b8 & 255L;
  }

  /** Converts a long value into a bit string (useful for debugging) */
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
