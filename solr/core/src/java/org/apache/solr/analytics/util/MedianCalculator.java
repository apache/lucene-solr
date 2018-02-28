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
package org.apache.solr.analytics.util;

import java.util.List;

/**
 * Only used for testing.
 * Medians are calculated with the {@link OrdinalCalculator} for actual analytics requests.
 */
public class MedianCalculator {

  /**
   * Calculates the median of the given list of numbers.
   *
   * @param list A list of {@link Comparable} {@link Number} objects
   * @return The median of the given list as a double.
   */
  public static <T extends Number & Comparable<T>> double getMedian(List<T> list) {
    int size = list.size() - 1;
    if (size == -1) {
      return 0;
    }

    select(list, .5 * size, 0, size);

    int firstIdx = (int) (Math.floor(.5 * size));
    int secondIdx = (firstIdx <= size && size % 2 == 1) ? firstIdx + 1 : firstIdx;
    double result = list.get(firstIdx).doubleValue() * .5 + list.get(secondIdx).doubleValue() * .5;

    return result;
  }

  private static <T extends Comparable<T>> void select(List<T> list, double place, int begin, int end) {
    T split;
    if (end - begin < 10) {
      split = list.get((int) (Math.random() * (end - begin + 1)) + begin);
    } else {
      split = split(list, begin, end);
    }

    Point result = partition(list, begin, end, split);

    if (place < result.low) {
      select(list, place, begin, result.low);
    } else if (place > result.high) {
      select(list, place, result.high, end);
    } else {
      if (result.low == (int) (Math.floor(place)) && result.low > begin) {
        select(list, result.low, begin, result.low);
      }
      if (result.high == (int) (Math.ceil(place)) && result.high < end) {
        select(list, result.high, result.high, end);
      }
    }
  }

  private static <T extends Comparable<T>> T split(List<T> list, int begin, int end) {
    T temp;
    int num = (end - begin + 1);
    int recursiveSize = (int) Math.sqrt((double) num);
    int step = num / recursiveSize;
    for (int i = 1; i < recursiveSize; i++) {
      int swapFrom = i * step + begin;
      int swapTo = i + begin;
      temp = list.get(swapFrom);
      list.set(swapFrom, list.get(swapTo));
      list.set(swapTo, temp);
    }
    recursiveSize--;
    select(list, recursiveSize / 2 + begin, begin, recursiveSize + begin);
    return list.get(recursiveSize / 2 + begin);
  }

  private static <T extends Comparable<T>> Point partition(List<T> list, int begin, int end, T indexElement) {
    T temp;
    int left, right;
    for (left = begin, right = end; left < right; left++, right--) {
      while (list.get(left).compareTo(indexElement) < 0) {
        left++;
      }
      while (right != begin - 1 && list.get(right).compareTo(indexElement) >= 0) {
        right--;
      }
      if (right <= left) {
        left--;
        right++;
        break;
      }
      temp = list.get(left);
      list.set(left, list.get(right));
      list.set(right, temp);
    }
    while (left != begin - 1 && list.get(left).compareTo(indexElement) >= 0) {
      left--;
    }
    while (right != end + 1 && list.get(right).compareTo(indexElement) <= 0) {
      right++;
    }
    int rightMove = right + 1;
    while (rightMove < end + 1) {
      if (list.get(rightMove).equals(indexElement)) {
        temp = list.get(rightMove);
        list.set(rightMove, list.get(right));
        list.set(right, temp);
        do {
          right++;
        } while (list.get(right).equals(indexElement));
        if (rightMove <= right) {
          rightMove = right;
        }
      }
      rightMove++;
    }
    return new Point(left, right);
  }
}
