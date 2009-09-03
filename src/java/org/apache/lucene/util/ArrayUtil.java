package org.apache.lucene.util;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Methods for manipulating arrays.
 */
public final class ArrayUtil {
  /*
     Begin Apache Harmony code

     Revision taken on Friday, June 12. https://svn.apache.org/repos/asf/harmony/enhanced/classlib/archive/java6/modules/luni/src/main/java/java/lang/Integer.java

   */

  /**
   * Parses the string argument as if it was an int value and returns the
   * result. Throws NumberFormatException if the string does not represent an
   * int quantity.
   *
   * @param chars a string representation of an int quantity.
   * @return int the value represented by the argument
   * @throws NumberFormatException if the argument could not be parsed as an int quantity.
   */
  public static int parseInt(char[] chars) throws NumberFormatException {
    return parseInt(chars, 0, chars.length, 10);
  }

  /**
   * Parses a char array into an int.
   * @param chars the character array
   * @param offset The offset into the array
   * @param len The length
   * @return the int
   * @throws NumberFormatException if it can't parse
   */
  public static int parseInt(char[] chars, int offset, int len) throws NumberFormatException {
    return parseInt(chars, offset, len, 10);
  }

  /**
   * Parses the string argument as if it was an int value and returns the
   * result. Throws NumberFormatException if the string does not represent an
   * int quantity. The second argument specifies the radix to use when parsing
   * the value.
   *
   * @param chars a string representation of an int quantity.
   * @param radix the base to use for conversion.
   * @return int the value represented by the argument
   * @throws NumberFormatException if the argument could not be parsed as an int quantity.
   */
  public static int parseInt(char[] chars, int offset, int len, int radix)
          throws NumberFormatException {
    if (chars == null || radix < Character.MIN_RADIX
            || radix > Character.MAX_RADIX) {
      throw new NumberFormatException();
    }
    int  i = 0;
    if (len == 0) {
      throw new NumberFormatException("chars length is 0");
    }
    boolean negative = chars[offset + i] == '-';
    if (negative && ++i == len) {
      throw new NumberFormatException("can't convert to an int");
    }
    if (negative == true){
      offset++;
      len--;
    }
    return parse(chars, offset, len, radix, negative);
  }


  private static int parse(char[] chars, int offset, int len, int radix,
                           boolean negative) throws NumberFormatException {
    int max = Integer.MIN_VALUE / radix;
    int result = 0;
    for (int i = 0; i < len; i++){
      int digit = Character.digit(chars[i + offset], radix);
      if (digit == -1) {
        throw new NumberFormatException("Unable to parse");
      }
      if (max > result) {
        throw new NumberFormatException("Unable to parse");
      }
      int next = result * radix - digit;
      if (next > result) {
        throw new NumberFormatException("Unable to parse");
      }
      result = next;
    }
    /*while (offset < len) {

    }*/
    if (!negative) {
      result = -result;
      if (result < 0) {
        throw new NumberFormatException("Unable to parse");
      }
    }
    return result;
  }


  /*

 END APACHE HARMONY CODE
  */


  public static int getNextSize(int targetSize) {
    /* This over-allocates proportional to the list size, making room
     * for additional growth.  The over-allocation is mild, but is
     * enough to give linear-time amortized behavior over a long
     * sequence of appends() in the presence of a poorly-performing
     * system realloc().
     * The growth pattern is:  0, 4, 8, 16, 25, 35, 46, 58, 72, 88, ...
     */
    return (targetSize >> 3) + (targetSize < 9 ? 3 : 6) + targetSize;
  }

  public static int getShrinkSize(int currentSize, int targetSize) {
    final int newSize = getNextSize(targetSize);
    // Only reallocate if we are "substantially" smaller.
    // This saves us from "running hot" (constantly making a
    // bit bigger then a bit smaller, over and over):
    if (newSize < currentSize / 2)
      return newSize;
    else
      return currentSize;
  }

  public static int[] grow(int[] array, int minSize) {
    if (array.length < minSize) {
      int[] newArray = new int[getNextSize(minSize)];
      System.arraycopy(array, 0, newArray, 0, array.length);
      return newArray;
    } else
      return array;
  }

  public static int[] grow(int[] array) {
    return grow(array, 1 + array.length);
  }

  public static int[] shrink(int[] array, int targetSize) {
    final int newSize = getShrinkSize(array.length, targetSize);
    if (newSize != array.length) {
      int[] newArray = new int[newSize];
      System.arraycopy(array, 0, newArray, 0, newSize);
      return newArray;
    } else
      return array;
  }

  public static long[] grow(long[] array, int minSize) {
    if (array.length < minSize) {
      long[] newArray = new long[getNextSize(minSize)];
      System.arraycopy(array, 0, newArray, 0, array.length);
      return newArray;
    } else
      return array;
  }

  public static long[] grow(long[] array) {
    return grow(array, 1 + array.length);
  }

  public static long[] shrink(long[] array, int targetSize) {
    final int newSize = getShrinkSize(array.length, targetSize);
    if (newSize != array.length) {
      long[] newArray = new long[newSize];
      System.arraycopy(array, 0, newArray, 0, newSize);
      return newArray;
    } else
      return array;
  }

  public static byte[] grow(byte[] array, int minSize) {
    if (array.length < minSize) {
      byte[] newArray = new byte[getNextSize(minSize)];
      System.arraycopy(array, 0, newArray, 0, array.length);
      return newArray;
    } else
      return array;
  }

  public static byte[] grow(byte[] array) {
    return grow(array, 1 + array.length);
  }

  public static byte[] shrink(byte[] array, int targetSize) {
    final int newSize = getShrinkSize(array.length, targetSize);
    if (newSize != array.length) {
      byte[] newArray = new byte[newSize];
      System.arraycopy(array, 0, newArray, 0, newSize);
      return newArray;
    } else
      return array;
  }

  /**
   * Returns hash of chars in range start (inclusive) to
   * end (inclusive)
   */
  public static int hashCode(char[] array, int start, int end) {
    int code = 0;
    for (int i = end - 1; i >= start; i--)
      code = code * 31 + array[i];
    return code;
  }

  /**
   * Returns hash of chars in range start (inclusive) to
   * end (inclusive)
   */
  public static int hashCode(byte[] array, int start, int end) {
    int code = 0;
    for (int i = end - 1; i >= start; i--)
      code = code * 31 + array[i];
    return code;
  }
}
