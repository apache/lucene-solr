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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PercentileCalculator {
  /**
   * Calculates a list of percentile values for a given list of objects and percentiles.
   *
   * @param list     The list of {@link Comparable} objects to calculate the percentiles of.
   * @param percents The array of percentiles (.01 to .99) to calculate.
   * @return a list of comparables
   */
  public static <T extends Comparable<T>> List<T> getPercentiles(List<T> list, double[] percents) {
    int size = list.size();
    if (size == 0) {
      return null;
    }

    int[] percs = new int[percents.length];
    for (int i = 0; i < percs.length; i++) {
      percs[i] = (int) Math.round(percents[i] * size - .5);
    }
    int[] percentiles = Arrays.copyOf(percs, percs.length);
    Arrays.sort(percentiles);

    if (percentiles[0] < 0 || percentiles[percentiles.length - 1] > size - 1) {
      throw new IllegalArgumentException();
    }

    List<T> results = new ArrayList<>(percs.length);

    distributeAndFind(list, percentiles, 0, percentiles.length - 1);

    for (int i = 0; i < percs.length; i++) {
      results.add(list.get(percs[i]));
    }
    return results;
  }

  private static <T extends Comparable<T>> void distributeAndFind(List<T> list, int[] percentiles, int beginIdx, int endIdx) {
    if (endIdx < beginIdx) {
      return;
    }
    int middleIdxb = beginIdx;
    int middleIdxe = beginIdx;
    int begin = (beginIdx == 0) ? -1 : percentiles[beginIdx - 1];
    int end = (endIdx == percentiles.length - 1) ? list.size() : percentiles[endIdx + 1];
    double middle = (begin + end) / 2.0;
    for (int i = beginIdx; i <= endIdx; i++) {
      double value = Math.abs(percentiles[i] - middle) - Math.abs(percentiles[middleIdxb] - middle);
      if (percentiles[i] == percentiles[middleIdxb]) {
        middleIdxe = i;
      } else if (value < 0) {
        middleIdxb = i;
        do {
          middleIdxe = i;
          i++;
        } while (i <= endIdx && percentiles[middleIdxb] == percentiles[i]);
        break;
      }
    }

    int middlePlace = percentiles[middleIdxb];
    int beginPlace = begin + 1;
    int endPlace = end - 1;

    select(list, middlePlace, beginPlace, endPlace);
    distributeAndFind(list, percentiles, beginIdx, middleIdxb - 1);
    distributeAndFind(list, percentiles, middleIdxe + 1, endIdx);
  }

  private static <T extends Comparable<T>> void select(List<T> list, int place, int begin, int end) {
    T split;
    if (end - begin < 10) {
      split = list.get((int) (Math.random() * (end - begin + 1)) + begin);
    } else {
      split = split(list, begin, end);
    }

    Point result = partition(list, begin, end, split);

    if (place <= result.low) {
      select(list, place, begin, result.low);
    } else if (place >= result.high) {
      select(list, place, result.high, end);
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
    for (left = begin, right = end; left <= right; left++, right--) {
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
    while (left > begin - 1 && list.get(left).compareTo(indexElement) >= 0) {
      left--;
    }
    while (right < end + 1 && list.get(right).compareTo(indexElement) <= 0) {
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

class Point {
  public int low;
  public int high;

  public Point(int low, int high) {
    this.low = low;
    this.high = high;
  }
}
