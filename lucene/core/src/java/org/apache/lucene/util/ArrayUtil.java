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
package org.apache.lucene.util;

import java.lang.reflect.Array;
import java.util.Comparator;

/**
 * Methods for manipulating arrays.
 *
 * @lucene.internal
 */

public final class ArrayUtil {

  /** Maximum length for an array (Integer.MAX_VALUE - RamUsageEstimator.NUM_BYTES_ARRAY_HEADER). */
  public static final int MAX_ARRAY_LENGTH = Integer.MAX_VALUE - RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;

  private ArrayUtil() {} // no instance

  /*
     Begin Apache Harmony code

     Revision taken on Friday, June 12. https://svn.apache.org/repos/asf/harmony/enhanced/classlib/archive/java6/modules/luni/src/main/java/java/lang/Integer.java

   */

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

  /** Returns an array size &gt;= minTargetSize, generally
   *  over-allocating exponentially to achieve amortized
   *  linear-time cost as the array grows.
   *
   *  NOTE: this was originally borrowed from Python 2.4.2
   *  listobject.c sources (attribution in LICENSE.txt), but
   *  has now been substantially changed based on
   *  discussions from java-dev thread with subject "Dynamic
   *  array reallocation algorithms", started on Jan 12
   *  2010.
   *
   * @param minTargetSize Minimum required value to be returned.
   * @param bytesPerElement Bytes used by each element of
   * the array.  See constants in {@link RamUsageEstimator}.
   *
   * @lucene.internal
   */

  public static int oversize(int minTargetSize, int bytesPerElement) {

    if (minTargetSize < 0) {
      // catch usage that accidentally overflows int
      throw new IllegalArgumentException("invalid array size " + minTargetSize);
    }

    if (minTargetSize == 0) {
      // wait until at least one element is requested
      return 0;
    }

    if (minTargetSize > MAX_ARRAY_LENGTH) {
      throw new IllegalArgumentException("requested array size " + minTargetSize + " exceeds maximum array in java (" + MAX_ARRAY_LENGTH + ")");
    }

    // asymptotic exponential growth by 1/8th, favors
    // spending a bit more CPU to not tie up too much wasted
    // RAM:
    int extra = minTargetSize >> 3;

    if (extra < 3) {
      // for very small arrays, where constant overhead of
      // realloc is presumably relatively high, we grow
      // faster
      extra = 3;
    }

    int newSize = minTargetSize + extra;

    // add 7 to allow for worst case byte alignment addition below:
    if (newSize+7 < 0 || newSize+7 > MAX_ARRAY_LENGTH) {
      // int overflowed, or we exceeded the maximum array length
      return MAX_ARRAY_LENGTH;
    }

    if (Constants.JRE_IS_64BIT) {
      // round up to 8 byte alignment in 64bit env
      switch(bytesPerElement) {
      case 4:
        // round up to multiple of 2
        return (newSize + 1) & 0x7ffffffe;
      case 2:
        // round up to multiple of 4
        return (newSize + 3) & 0x7ffffffc;
      case 1:
        // round up to multiple of 8
        return (newSize + 7) & 0x7ffffff8;
      case 8:
        // no rounding
      default:
        // odd (invalid?) size
        return newSize;
      }
    } else {
      // round up to 4 byte alignment in 64bit env
      switch(bytesPerElement) {
      case 2:
        // round up to multiple of 2
        return (newSize + 1) & 0x7ffffffe;
      case 1:
        // round up to multiple of 4
        return (newSize + 3) & 0x7ffffffc;
      case 4:
      case 8:
        // no rounding
      default:
        // odd (invalid?) size
        return newSize;
      }
    }
  }

  /** Returns a new array whose size is exact the specified {@code newLength} without over-allocating */
  public static <T> T[] growExact(T[] array, int newLength) {
    Class<? extends Object[]> type = array.getClass();
    @SuppressWarnings("unchecked")
    T[] copy = (type == Object[].class)
        ? (T[]) new Object[newLength]
        : (T[]) Array.newInstance(type.getComponentType(), newLength);
    System.arraycopy(array, 0, copy, 0, array.length);
    return copy;
  }

  /** Returns an array whose size is at least {@code minSize}, generally over-allocating exponentially */
  public static <T> T[] grow(T[] array, int minSize) {
    assert minSize >= 0 : "size must be positive (got " + minSize + "): likely integer overflow?";
    if (array.length < minSize) {
      final int newLength = oversize(minSize, RamUsageEstimator.NUM_BYTES_OBJECT_REF);
      return growExact(array, newLength);
    } else
      return array;
  }

  /** Returns a new array whose size is exact the specified {@code newLength} without over-allocating */
  public static short[] growExact(short[] array, int newLength) {
    short[] copy = new short[newLength];
    System.arraycopy(array, 0, copy, 0, array.length);
    return copy;
  }

  /** Returns an array whose size is at least {@code minSize}, generally over-allocating exponentially */
  public static short[] grow(short[] array, int minSize) {
    assert minSize >= 0: "size must be positive (got " + minSize + "): likely integer overflow?";
    if (array.length < minSize) {
      return growExact(array, oversize(minSize, Short.BYTES));
    } else
      return array;
  }

  /** Returns a larger array, generally over-allocating exponentially */
  public static short[] grow(short[] array) {
    return grow(array, 1 + array.length);
  }

  /** Returns a new array whose size is exact the specified {@code newLength} without over-allocating */
  public static float[] growExact(float[] array, int newLength) {
    float[] copy = new float[newLength];
    System.arraycopy(array, 0, copy, 0, array.length);
    return copy;
  }

  /** Returns an array whose size is at least {@code minSize}, generally over-allocating exponentially */
  public static float[] grow(float[] array, int minSize) {
    assert minSize >= 0: "size must be positive (got " + minSize + "): likely integer overflow?";
    if (array.length < minSize) {
      float[] copy = new float[oversize(minSize, Float.BYTES)];
      System.arraycopy(array, 0, copy, 0, array.length);
      return copy;
    } else
      return array;
  }

  /** Returns a larger array, generally over-allocating exponentially */
  public static float[] grow(float[] array) {
    return grow(array, 1 + array.length);
  }

  /** Returns a new array whose size is exact the specified {@code newLength} without over-allocating */
  public static double[] growExact(double[] array, int newLength) {
    double[] copy = new double[newLength];
    System.arraycopy(array, 0, copy, 0, array.length);
    return copy;
  }

  /** Returns an array whose size is at least {@code minSize}, generally over-allocating exponentially */
  public static double[] grow(double[] array, int minSize) {
    assert minSize >= 0: "size must be positive (got " + minSize + "): likely integer overflow?";
    if (array.length < minSize) {
      return growExact(array, oversize(minSize, Double.BYTES));
    } else
      return array;
  }

  /** Returns a larger array, generally over-allocating exponentially */
  public static double[] grow(double[] array) {
    return grow(array, 1 + array.length);
  }

  /** Returns a new array whose size is exact the specified {@code newLength} without over-allocating */
  public static int[] growExact(int[] array, int newLength) {
    int[] copy = new int[newLength];
    System.arraycopy(array, 0, copy, 0, array.length);
    return copy;
  }

  /** Returns an array whose size is at least {@code minSize}, generally over-allocating exponentially */
  public static int[] grow(int[] array, int minSize) {
    assert minSize >= 0: "size must be positive (got " + minSize + "): likely integer overflow?";
    if (array.length < minSize) {
      return growExact(array, oversize(minSize, Integer.BYTES));
    } else
      return array;
  }

  /** Returns a larger array, generally over-allocating exponentially */
  public static int[] grow(int[] array) {
    return grow(array, 1 + array.length);
  }

  /** Returns a new array whose size is exact the specified {@code newLength} without over-allocating */
  public static long[] growExact(long[] array, int newLength) {
    long[] copy = new long[newLength];
    System.arraycopy(array, 0, copy, 0, array.length);
    return copy;
  }

  /** Returns an array whose size is at least {@code minSize}, generally over-allocating exponentially */
  public static long[] grow(long[] array, int minSize) {
    assert minSize >= 0: "size must be positive (got " + minSize + "): likely integer overflow?";
    if (array.length < minSize) {
      return growExact(array, oversize(minSize, Long.BYTES));
    } else
      return array;
  }

  /** Returns a larger array, generally over-allocating exponentially */
  public static long[] grow(long[] array) {
    return grow(array, 1 + array.length);
  }

  /** Returns a new array whose size is exact the specified {@code newLength} without over-allocating */
  public static byte[] growExact(byte[] array, int newLength) {
    byte[] copy = new byte[newLength];
    System.arraycopy(array, 0, copy, 0, array.length);
    return copy;
  }

  /** Returns an array whose size is at least {@code minSize}, generally over-allocating exponentially */
  public static byte[] grow(byte[] array, int minSize) {
    assert minSize >= 0: "size must be positive (got " + minSize + "): likely integer overflow?";
    if (array.length < minSize) {
      return growExact(array, oversize(minSize, Byte.BYTES));
    } else
      return array;
  }

  /** Returns a larger array, generally over-allocating exponentially */
  public static byte[] grow(byte[] array) {
    return grow(array, 1 + array.length);
  }

  /** Returns a new array whose size is exact the specified {@code newLength} without over-allocating */
  public static char[] growExact(char[] array, int newLength) {
    char[] copy = new char[newLength];
    System.arraycopy(array, 0, copy, 0, array.length);
    return copy;
  }

  /** Returns an array whose size is at least {@code minSize}, generally over-allocating exponentially */
  public static char[] grow(char[] array, int minSize) {
    assert minSize >= 0: "size must be positive (got " + minSize + "): likely integer overflow?";
    if (array.length < minSize) {
      return growExact(array, oversize(minSize, Character.BYTES));
    } else
      return array;
  }

  /** Returns a larger array, generally over-allocating exponentially */
  public static char[] grow(char[] array) {
    return grow(array, 1 + array.length);
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

  /** Swap values stored in slots <code>i</code> and <code>j</code> */
  public static <T> void swap(T[] arr, int i, int j) {
    final T tmp = arr[i];
    arr[i] = arr[j];
    arr[j] = tmp;
  }

  // intro-sorts
  
  /**
   * Sorts the given array slice using the {@link Comparator}. This method uses the intro sort
   * algorithm, but falls back to insertion sort for small arrays.
   * @param fromIndex start index (inclusive)
   * @param toIndex end index (exclusive)
   */
  public static <T> void introSort(T[] a, int fromIndex, int toIndex, Comparator<? super T> comp) {
    if (toIndex-fromIndex <= 1) return;
    new ArrayIntroSorter<>(a, comp).sort(fromIndex, toIndex);
  }
  
  /**
   * Sorts the given array using the {@link Comparator}. This method uses the intro sort
   * algorithm, but falls back to insertion sort for small arrays.
   */
  public static <T> void introSort(T[] a, Comparator<? super T> comp) {
    introSort(a, 0, a.length, comp);
  }
  
  /**
   * Sorts the given array slice in natural order. This method uses the intro sort
   * algorithm, but falls back to insertion sort for small arrays.
   * @param fromIndex start index (inclusive)
   * @param toIndex end index (exclusive)
   */
  public static <T extends Comparable<? super T>> void introSort(T[] a, int fromIndex, int toIndex) {
    if (toIndex-fromIndex <= 1) return;
    introSort(a, fromIndex, toIndex, Comparator.naturalOrder());
  }
  
  /**
   * Sorts the given array in natural order. This method uses the intro sort
   * algorithm, but falls back to insertion sort for small arrays.
   */
  public static <T extends Comparable<? super T>> void introSort(T[] a) {
    introSort(a, 0, a.length);
  }

  // tim sorts:
  
  /**
   * Sorts the given array slice using the {@link Comparator}. This method uses the Tim sort
   * algorithm, but falls back to binary sort for small arrays.
   * @param fromIndex start index (inclusive)
   * @param toIndex end index (exclusive)
   */
  public static <T> void timSort(T[] a, int fromIndex, int toIndex, Comparator<? super T> comp) {
    if (toIndex-fromIndex <= 1) return;
    new ArrayTimSorter<>(a, comp, a.length / 64).sort(fromIndex, toIndex);
  }
  
  /**
   * Sorts the given array using the {@link Comparator}. This method uses the Tim sort
   * algorithm, but falls back to binary sort for small arrays.
   */
  public static <T> void timSort(T[] a, Comparator<? super T> comp) {
    timSort(a, 0, a.length, comp);
  }
  
  /**
   * Sorts the given array slice in natural order. This method uses the Tim sort
   * algorithm, but falls back to binary sort for small arrays.
   * @param fromIndex start index (inclusive)
   * @param toIndex end index (exclusive)
   */
  public static <T extends Comparable<? super T>> void timSort(T[] a, int fromIndex, int toIndex) {
    if (toIndex-fromIndex <= 1) return;
    timSort(a, fromIndex, toIndex, Comparator.naturalOrder());
  }
  
  /**
   * Sorts the given array in natural order. This method uses the Tim sort
   * algorithm, but falls back to binary sort for small arrays.
   */
  public static <T extends Comparable<? super T>> void timSort(T[] a) {
    timSort(a, 0, a.length);
  }

  /**
   * Reorganize {@code arr[from:to[} so that the element at offset k is at the
   * same position as if {@code arr[from:to]} was sorted, and all elements on
   * its left are less than or equal to it, and all elements on its right are
   * greater than or equal to it.
   *
   * This runs in linear time on average and in {@code n log(n)} time in the
   * worst case.
   *
   * @param arr Array to be re-organized.
   * @param from Starting index for re-organization. Elements before this index
   *             will be left as is.
   * @param to Ending index. Elements after this index will be left as is.
   * @param k Index of element to sort from. Value must be less than 'to' and greater than or equal to 'from'.
   * @param comparator Comparator to use for sorting
   *
   */
  public static <T> void select(T[] arr, int from, int to, int k, Comparator<? super T> comparator) {
    new IntroSelector() {

      T pivot;

      @Override
      protected void swap(int i, int j) {
        ArrayUtil.swap(arr, i, j);
      }

      @Override
      protected void setPivot(int i) {
        pivot = arr[i];
      }

      @Override
      protected int comparePivot(int j) {
        return comparator.compare(pivot, arr[j]);
      }
    }.select(from, to, k);
  }

  /**
   * Copies the specified range of the given array into a new sub array.
   * @param array the input array
   * @param from  the initial index of range to be copied (inclusive)
   * @param to    the final index of range to be copied (exclusive)
   */
  public static byte[] copyOfSubArray(byte[] array, int from, int to) {
    final byte[] copy = new byte[to-from];
    System.arraycopy(array, from, copy, 0, to-from);
    return copy;
  }

  /**
   * Copies the specified range of the given array into a new sub array.
   * @param array the input array
   * @param from  the initial index of range to be copied (inclusive)
   * @param to    the final index of range to be copied (exclusive)
   */
  public static char[] copyOfSubArray(char[] array, int from, int to) {
    final char[] copy = new char[to-from];
    System.arraycopy(array, from, copy, 0, to-from);
    return copy;
  }

  /**
   * Copies the specified range of the given array into a new sub array.
   * @param array the input array
   * @param from  the initial index of range to be copied (inclusive)
   * @param to    the final index of range to be copied (exclusive)
   */
  public static short[] copyOfSubArray(short[] array, int from, int to) {
    final short[] copy = new short[to-from];
    System.arraycopy(array, from, copy, 0, to-from);
    return copy;
  }

  /**
   * Copies the specified range of the given array into a new sub array.
   * @param array the input array
   * @param from  the initial index of range to be copied (inclusive)
   * @param to    the final index of range to be copied (exclusive)
   */
  public static int[] copyOfSubArray(int[] array, int from, int to) {
    final int[] copy = new int[to-from];
    System.arraycopy(array, from, copy, 0, to-from);
    return copy;
  }

  /**
   * Copies the specified range of the given array into a new sub array.
   * @param array the input array
   * @param from  the initial index of range to be copied (inclusive)
   * @param to    the final index of range to be copied (exclusive)
   */
  public static long[] copyOfSubArray(long[] array, int from, int to) {
    final long[] copy = new long[to-from];
    System.arraycopy(array, from, copy, 0, to-from);
    return copy;
  }

  /**
   * Copies the specified range of the given array into a new sub array.
   * @param array the input array
   * @param from  the initial index of range to be copied (inclusive)
   * @param to    the final index of range to be copied (exclusive)
   */
  public static float[] copyOfSubArray(float[] array, int from, int to) {
    final float[] copy = new float[to-from];
    System.arraycopy(array, from, copy, 0, to-from);
    return copy;
  }

  /**
   * Copies the specified range of the given array into a new sub array.
   * @param array the input array
   * @param from  the initial index of range to be copied (inclusive)
   * @param to    the final index of range to be copied (exclusive)
   */
  public static double[] copyOfSubArray(double[] array, int from, int to) {
    final double[] copy = new double[to-from];
    System.arraycopy(array, from, copy, 0, to-from);
    return copy;
  }

  /**
   * Copies the specified range of the given array into a new sub array.
   * @param array the input array
   * @param from  the initial index of range to be copied (inclusive)
   * @param to    the final index of range to be copied (exclusive)
   */
  public static <T> T[] copyOfSubArray(T[] array, int from, int to) {
    final int subLength = to - from;
    final Class<? extends Object[]> type = array.getClass();
    @SuppressWarnings("unchecked")
    final T[] copy = (type == Object[].class)
        ? (T[]) new Object[subLength]
        : (T[]) Array.newInstance(type.getComponentType(), subLength);
    System.arraycopy(array, from, copy, 0, subLength);
    return copy;
  }
}
