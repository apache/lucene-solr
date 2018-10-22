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
/** Utilities for primitive Java data types. */
public class PrimUtils {

  public static abstract class IntComparator {
    public abstract int compare(int a, int b);
    public boolean lessThan(int a, int b) {
      return compare(a,b) < 0;
    }
    public boolean equals(int a, int b) {
      return compare(a,b) == 0;
    }
  }

  /** Sort the integer array from "start" inclusive to "end" exclusive in ascending order,
   *  using the provided comparator.
   * TODO: is this an unstable sort?
   */
  public static void sort(int start, int end, int[] array, IntComparator comparator) {
    // This code was copied from Apache Harmony's Arrays.sort(double[]) and modified
    // to use a comparator, in addition to other small efficiency enhancements
    // like replacing divisions with shifts.

    int temp;
    int length = end - start;
    if (length < 7) {
      for (int i = start + 1; i < end; i++) {
        for (int j = i; j > start && comparator.lessThan(array[j], array[j - 1]); j--) {
          temp = array[j];
          array[j] = array[j - 1];
          array[j - 1] = temp;
        }
      }
      return;
    }
    int middle = (start + end) >>> 1;
    if (length > 7) {
      int bottom = start;
      int top = end - 1;
      if (length > 40) {
        length >>= 3;
        bottom = med3(array, bottom, bottom + length, bottom
            + (length<<1), comparator);
        middle = med3(array, middle - length, middle, middle + length, comparator);
        top = med3(array, top - (length<<1), top - length, top, comparator);
      }
      middle = med3(array, bottom, middle, top, comparator);
    }
    int partionValue = array[middle];
    int a, b, c, d;
    a = b = start;
    c = d = end - 1;
    while (true) {
      while (b <= c && !comparator.lessThan(partionValue, array[b])) {
        if (comparator.equals(array[b], partionValue)) {
          temp = array[a];
          array[a++] = array[b];
          array[b] = temp;
        }
        b++;
      }
      while (c >= b && !comparator.lessThan(array[c], partionValue)) {
        if (comparator.equals(array[c], partionValue)) {
          temp = array[c];
          array[c] = array[d];
          array[d--] = temp;
        }
        c--;
      }
      if (b > c) {
        break;
      }
      temp = array[b];
      array[b++] = array[c];
      array[c--] = temp;
    }
    length = a - start < b - a ? a - start : b - a;
    int l = start;
    int h = b - length;
    while (length-- > 0) {
      temp = array[l];
      array[l++] = array[h];
      array[h++] = temp;
    }
    length = d - c < end - 1 - d ? d - c : end - 1 - d;
    l = b;
    h = end - length;
    while (length-- > 0) {
      temp = array[l];
      array[l++] = array[h];
      array[h++] = temp;
    }
    if ((length = b - a) > 0) {
      sort(start, start + length, array, comparator);
    }
    if ((length = d - c) > 0) {
      sort(end - length, end, array, comparator);
    }
  }

  private static int med3(int[] array, int a, int b, int c, IntComparator comparator) {
    int x = array[a], y = array[b], z = array[c];
    return comparator.lessThan(x, y) ? (comparator.lessThan(y, z) ? b : (comparator.lessThan(x, z) ? c : a))
        : (comparator.lessThan(z, y) ? b : (comparator.lessThan(z, x) ? c : a));
  }
}
