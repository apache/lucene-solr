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
package org.apache.solr.util;

import java.text.NumberFormat;
import java.util.Locale;

import org.apache.lucene.util.BytesRef;

/**
 *
 */
public class NumberUtils {

  public static String readableSize(long size) {
    NumberFormat formatter = NumberFormat.getNumberInstance(Locale.ROOT);
    formatter.setMaximumFractionDigits(2);
    if (size / (1024 * 1024 * 1024) > 0) {
      return formatter.format(size * 1.0d / (1024 * 1024 * 1024)) + " GB";
    } else if (size / (1024 * 1024) > 0) {
      return formatter.format(size * 1.0d / (1024 * 1024)) + " MB";
    } else if (size / 1024 > 0) {
      return formatter.format(size * 1.0d / 1024) + " KB";
    } else {
      return String.valueOf(size) + " bytes";
    }
  }

  public static String int2sortableStr(int val) {
    char[] arr = new char[3];
    int2sortableStr(val,arr,0);
    return new String(arr,0,3);
  }

  public static String int2sortableStr(String val) {
    return int2sortableStr(Integer.parseInt(val));
  }

  public static String SortableStr2int(String val) {
    int ival = SortableStr2int(val,0,3);
    return Integer.toString(ival);
  }

  public static String SortableStr2int(BytesRef val) {
    // TODO: operate directly on BytesRef
    return SortableStr2int(val.utf8ToString());
  }


  public static String long2sortableStr(long val) {
    char[] arr = new char[5];
    long2sortableStr(val,arr,0);
    return new String(arr,0,5);
  }

  public static String long2sortableStr(String val) {
    return long2sortableStr(Long.parseLong(val));
  }

  public static String SortableStr2long(String val) {
    long ival = SortableStr2long(val,0,5);
    return Long.toString(ival);
  }

  public static String SortableStr2long(BytesRef val) {
    // TODO: operate directly on BytesRef
    return SortableStr2long(val.utf8ToString());
  }

  //
  // IEEE floating point format is defined so that it sorts correctly
  // when interpreted as a signed integer (or signed long in the case
  // of a double) for positive values.  For negative values, all the bits except
  // the sign bit must be inverted.
  // This correctly handles all possible float values including -Infinity and +Infinity.
  // Note that in float-space, NaN<x is false, NaN>x is false, NaN==x is false, NaN!=x is true
  // for all x (including NaN itself).  Internal to Solr, NaN==NaN is true and NaN
  // sorts higher than Infinity, so a range query of [-Infinity TO +Infinity] will
  // exclude NaN values, but a query of "NaN" will find all NaN values.
  // Also, -0==0 in float-space but -0<0 after this transformation.
  //
  public static String float2sortableStr(float val) {
    int f = Float.floatToRawIntBits(val);
    if (f<0) f ^= 0x7fffffff;
    return int2sortableStr(f);
  }

  public static String float2sortableStr(String val) {
    return float2sortableStr(Float.parseFloat(val));
  }

  public static float SortableStr2float(String val) {
    int f = SortableStr2int(val,0,3);
    if (f<0) f ^= 0x7fffffff;
    return Float.intBitsToFloat(f);
  }

  public static float SortableStr2float(BytesRef val) {
    // TODO: operate directly on BytesRef
    return SortableStr2float(val.utf8ToString());
  }

  public static String SortableStr2floatStr(String val) {
    return Float.toString(SortableStr2float(val));
  }


  public static String double2sortableStr(double val) {
    long f = Double.doubleToRawLongBits(val);
    if (f<0) f ^= 0x7fffffffffffffffL;
    return long2sortableStr(f);
  }

  public static String double2sortableStr(String val) {
    return double2sortableStr(Double.parseDouble(val));
  }

  public static double SortableStr2double(String val) {
    long f = SortableStr2long(val,0,6);
    if (f<0) f ^= 0x7fffffffffffffffL;
    return Double.longBitsToDouble(f);
  }

  public static double SortableStr2double(BytesRef val) {
    // TODO: operate directly on BytesRef
    return SortableStr2double(val.utf8ToString());
  }

  public static String SortableStr2doubleStr(String val) {
    return Double.toString(SortableStr2double(val));
  }



  // uses binary representation of an int to build a string of
  // chars that will sort correctly.  Only char ranges
  // less than 0xd800 will be used to avoid UCS-16 surrogates.
  public static int int2sortableStr(int val, char[] out, int offset) {
    val += Integer.MIN_VALUE;
    out[offset++] = (char)(val >>> 24);
    out[offset++] = (char)((val >>> 12) & 0x0fff);
    out[offset++] = (char)(val & 0x0fff);
    return 3;
  }

  public static int SortableStr2int(String sval, int offset, int len) {
    int val = sval.charAt(offset++) << 24;
    val |= sval.charAt(offset++) << 12;
    val |= sval.charAt(offset++);
    val -= Integer.MIN_VALUE;
    return val;
  }

  public static int SortableStr2int(BytesRef sval, int offset, int len) {
    // TODO: operate directly on BytesRef
    return SortableStr2int(sval.utf8ToString(), offset, len);
  }

  // uses binary representation of an int to build a string of
  // chars that will sort correctly.  Only char ranges
  // less than 0xd800 will be used to avoid UCS-16 surrogates.
  // we can use the lowest 15 bits of a char, (or a mask of 0x7fff)
  public static int long2sortableStr(long val, char[] out, int offset) {
    val += Long.MIN_VALUE;
    out[offset++] = (char)(val >>>60);
    out[offset++] = (char)(val >>>45 & 0x7fff);
    out[offset++] = (char)(val >>>30 & 0x7fff);
    out[offset++] = (char)(val >>>15 & 0x7fff);
    out[offset] = (char)(val & 0x7fff);
    return 5;
  }

  public static long SortableStr2long(String sval, int offset, int len) {
    long val = (long)(sval.charAt(offset++)) << 60;
    val |= ((long)sval.charAt(offset++)) << 45;
    val |= ((long)sval.charAt(offset++)) << 30;
    val |= sval.charAt(offset++) << 15;
    val |= sval.charAt(offset);
    val -= Long.MIN_VALUE;
    return val;
  }

  public static long SortableStr2long(BytesRef sval, int offset, int len) {
    // TODO: operate directly on BytesRef
    return SortableStr2long(sval.utf8ToString(), offset, len);
  }

  public static byte[] intToBytes(int val) {
    byte[] result = new byte[4];

    result[0] = (byte) (val >> 24);
    result[1] = (byte) (val >> 16);
    result[2] = (byte) (val >> 8);
    result[3] = (byte) val;
    return result;
  }

  public static int bytesToInt(byte[] bytes) {
    if (bytes == null) return 0;
    assert bytes.length == 4;
    return bytes[0] << 24 | (bytes[1] & 255) << 16 | (bytes[2] & 255) << 8 | bytes[3] & 255;
  }
}
