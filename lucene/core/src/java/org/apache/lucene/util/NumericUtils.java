package org.apache.lucene.util;

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

import java.io.IOException;
import java.math.BigInteger;

import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;

/**
 * This is a helper class to generate prefix-encoded representations for numerical values
 * and supplies converters to represent float/double values as sortable integers/longs.
 *
 * <p>To also index floating point numbers, this class supplies two methods to convert them
 * to integer values by changing their bit layout: {@link #doubleToLong},
 * {@link #floatToInt}. You will have no precision loss by
 * converting floating point numbers to integers and back (only that the integer form
 * is not usable). Other data types like dates can easily converted to longs or ints (e.g.
 * date to long: {@link java.util.Date#getTime}).
 *
 * @lucene.internal
 * @since 2.9, API changed non backwards-compliant in 4.0
 */
public final class NumericUtils {

  private NumericUtils() {} // no instance!

  public static short halfFloatToShort(float value) {
    return sortableHalfFloatBits((short) HalfFloat.floatToShortBits(value));
  }

  /**
   * Converts a <code>float</code> value to a sortable signed <code>int</code>.
   * The value is converted by getting their IEEE 754 floating-point &quot;float format&quot;
   * bit layout and then some bits are swapped, to be able to compare the result as int.
   * By this the precision is not reduced, but the value can easily used as an int.
   * The sort order (including {@link Float#NaN}) is defined by
   * {@link Float#compareTo}; {@code NaN} is greater than positive infinity.
   * @see #intToFloat
   */
  public static int floatToInt(float val) {
    return sortableFloatBits(Float.floatToIntBits(val));
  }

  /**
   * Converts a <code>double</code> value to a sortable signed <code>long</code>.
   * The value is converted by getting their IEEE 754 floating-point &quot;double format&quot;
   * bit layout and then some bits are swapped, to be able to compare the result as long.
   * By this the precision is not reduced, but the value can easily used as a long.
   * The sort order (including {@link Double#NaN}) is defined by
   * {@link Double#compareTo}; {@code NaN} is greater than positive infinity.
   * @see #longToDouble
   */
  public static long doubleToLong(double val) {
    return sortableDoubleBits(Double.doubleToLongBits(val));
  }

  public static float shortToHalfFloat(short v) {
    return HalfFloat.shortBitsToFloat(sortableHalfFloatBits(v));
  }

  /**
   * Converts a sortable <code>int</code> back to a <code>float</code>.
   * @see #floatToInt
   */
  public static float intToFloat(int val) {
    return Float.intBitsToFloat(sortableFloatBits(val));
  }
  
  /**
   * Converts a sortable <code>long</code> back to a <code>double</code>.
   * @see #doubleToLong
   */
  public static double longToDouble(long val) {
    return Double.longBitsToDouble(sortableDoubleBits(val));
  }

  /** Converts IEEE 754 representation of a half float to sortable order (or back to the original) */
  public static short sortableHalfFloatBits(short bits) {
    return (short) (bits ^ (bits >> 15) & 0x7fff);
  }

  /** Converts IEEE 754 representation of a float to sortable order (or back to the original) */
  public static int sortableFloatBits(int bits) {
    return bits ^ (bits >> 31) & 0x7fffffff;
  }

  /** Converts IEEE 754 representation of a double to sortable order (or back to the original) */
  public static long sortableDoubleBits(long bits) {
    return bits ^ (bits >> 63) & 0x7fffffffffffffffL;
  }
  
  public static short bytesToShort(BytesRef bytes) {
    if (bytes.length != 2) {
      throw new IllegalArgumentException("incoming bytes should be length=2; got length=" + bytes.length);
    }
    short sortableBits = 0;
    for(int i=0;i<2;i++) {
      sortableBits = (short) ((sortableBits << 8) | bytes.bytes[bytes.offset + i] & 0xff);
    }

    return (short) (sortableBits ^ 0x8000);
  }

  public static int bytesToInt(BytesRef bytes) {
    if (bytes.length != 4) {
      throw new IllegalArgumentException("incoming bytes should be length=4; got length=" + bytes.length);
    }
    int sortableBits = 0;
    for(int i=0;i<4;i++) {
      sortableBits = (sortableBits << 8) | bytes.bytes[bytes.offset + i] & 0xff;
    }

    return sortableBits ^ 0x80000000;
  }

  public static long bytesToLong(BytesRef bytes) {
    if (bytes.length != 8) {
      throw new IllegalArgumentException("incoming bytes should be length=8; got length=" + bytes.length);
    }
    long sortableBits = 0;
    for(int i=0;i<8;i++) {
      sortableBits = (sortableBits << 8) | bytes.bytes[bytes.offset + i] & 0xff;
    }

    return sortableBits ^ 0x8000000000000000L;
  }

  public static BytesRef shortToBytes(short v) {
    int sortableBits = v ^ 0x8000;
    BytesRef token = new BytesRef(2);
    token.length = 2;
    int index = 1;
    while (index >= 0) {
      token.bytes[index] = (byte) (sortableBits & 0xff);
      index--;
      sortableBits >>>= 8;
    }
    return token;
  }

  public static BytesRef intToBytes(int v) {
    int sortableBits = v ^ 0x80000000;
    BytesRef token = new BytesRef(4);
    token.length = 4;
    int index = 3;
    while (index >= 0) {
      token.bytes[index] = (byte) (sortableBits & 0xff);
      index--;
      sortableBits >>>= 8;
    }
    return token;
  }

  public static BytesRef longToBytes(long v) {
    long sortableBits = v ^ 0x8000000000000000L;
    BytesRef token = new BytesRef(8);
    token.length = 8;
    int index = 7;
    while (index >= 0) {
      token.bytes[index] = (byte) (sortableBits & 0xff);
      index--;
      sortableBits >>>= 8;
    }
    return token;
  }

  public static BytesRef halfFloatToBytes(float value) {
    return shortToBytes(halfFloatToShort(value));
  }

  public static BytesRef floatToBytes(float value) {
    return intToBytes(floatToInt(value));
  }

  public static BytesRef doubleToBytes(double value) {
    return longToBytes(doubleToLong(value));
  }

  public static float bytesToHalfFloat(BytesRef bytes) {
    return HalfFloat.shortBitsToFloat(sortableHalfFloatBits(bytesToShort(bytes)));
  }

  public static float bytesToFloat(BytesRef bytes) {
    return intToFloat(bytesToInt(bytes));
  }

  public static double bytesToDouble(BytesRef bytes) {
    return longToDouble(bytesToLong(bytes));
  }

  public static BytesRef bigIntToBytes(BigInteger value) {
    byte[] bytes = value.toByteArray();
    sortableBigIntBytes(bytes);
    //System.out.println(value + " -> " + new BytesRef(bytes));
    return new BytesRef(bytes);
  }

  public static BigInteger bytesToBigInt(BytesRef bytes) {
    byte[] copy = new byte[bytes.length];
    System.arraycopy(bytes.bytes, bytes.offset, copy, 0, bytes.length);
    sortableBigIntBytes(copy);
    return new BigInteger(copy);
  }

  private static void sortableBigIntBytes(byte[] bytes) {
    // nocommit does NOT work
    //    bytes[0] ^= 0x80;
  }
}
