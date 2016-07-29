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

import java.util.Arrays;

/** Radix selector.
 *  <p>This implementation works similarly to a MSB radix sort except that it
 *  only recurses into the sub partition that contains the desired value.
 *  @lucene.internal */
public abstract class RadixSelector extends Selector {

  // after that many levels of recursion we fall back to introselect anyway
  // this is used as a protection against the fact that radix sort performs
  // worse when there are long common prefixes (probably because of cache
  // locality)
  private static final int LEVEL_THRESHOLD = 8;
  // size of histograms: 256 + 1 to indicate that the string is finished
  private static final int HISTOGRAM_SIZE = 257;
  // buckets below this size will be sorted with introselect
  private static final int LENGTH_THRESHOLD = 100;

  // we store one histogram per recursion level
  private final int[] histogram = new int[HISTOGRAM_SIZE];
  private final int[] commonPrefix;

  private final int maxLength;

  /**
   * Sole constructor.
   * @param maxLength the maximum length of keys, pass {@link Integer#MAX_VALUE} if unknown.
   */
  protected RadixSelector(int maxLength) {
    this.maxLength = maxLength;
    this.commonPrefix = new int[Math.min(24, maxLength)];
  }

  /** Return the k-th byte of the entry at index {@code i}, or {@code -1} if
   * its length is less than or equal to {@code k}. This may only be called
   * with a value of {@code i} between {@code 0} included and
   * {@code maxLength} excluded. */
  protected abstract int byteAt(int i, int k);

  /** Get a fall-back selector which may assume that the first {@code d} bytes
   *  of all compared strings are equal. This fallback selector is used when
   *  the range becomes narrow or when the maximum level of recursion has
   *  been exceeded. */
  protected Selector getFallbackSelector(int d) {
    return new IntroSelector() {
      @Override
      protected void swap(int i, int j) {
        RadixSelector.this.swap(i, j);
      }

      @Override
      protected int compare(int i, int j) {
        for (int o = d; o < maxLength; ++o) {
          final int b1 = byteAt(i, o);
          final int b2 = byteAt(j, o);
          if (b1 != b2) {
            return b1 - b2;
          } else if (b1 == -1) {
            break;
          }
        }
        return 0;
      }

      @Override
      protected void setPivot(int i) {
        pivot.setLength(0);
        for (int o = d; o < maxLength; ++o) {
          final int b = byteAt(i, o);
          if (b == -1) {
            break;
          }
          pivot.append((byte) b);
        }
      }

      @Override
      protected int comparePivot(int j) {
        for (int o = 0; o < pivot.length(); ++o) {
          final int b1 = pivot.byteAt(o) & 0xff;
          final int b2 = byteAt(j, d + o);
          if (b1 != b2) {
            return b1 - b2;
          }
        }
        if (d + pivot.length() == maxLength) {
          return 0;
        }
        return -1 - byteAt(j, d + pivot.length());
      }

      private final BytesRefBuilder pivot = new BytesRefBuilder();
    };
  }

  @Override
  public void select(int from, int to, int k) {
    checkArgs(from, to, k);
    select(from, to, k, 0, 0);
  }

  private void select(int from, int to, int k, int d, int l) {
    if (to - from <= LENGTH_THRESHOLD || d >= LEVEL_THRESHOLD) {
      getFallbackSelector(d).select(from, to, k);
    } else {
      radixSelect(from, to, k, d, l);
    }
  }

  /**
   * @param d the character number to compare
   * @param l the level of recursion
   */
  private void radixSelect(int from, int to, int k, int d, int l) {
    final int[] histogram = this.histogram;
    Arrays.fill(histogram, 0);

    final int commonPrefixLength = computeCommonPrefixLengthAndBuildHistogram(from, to, d, histogram);
    if (commonPrefixLength > 0) {
      // if there are no more chars to compare or if all entries fell into the
      // first bucket (which means strings are shorter than d) then we are done
      // otherwise recurse
      if (d + commonPrefixLength < maxLength
          && histogram[0] < to - from) {
        radixSelect(from, to, k, d + commonPrefixLength, l);
      }
      return;
    }
    assert assertHistogram(commonPrefixLength, histogram);

    int bucketFrom = from;
    for (int bucket = 0; bucket < HISTOGRAM_SIZE; ++bucket) {
      final int bucketTo = bucketFrom + histogram[bucket];

      if (bucketTo > k) {
        partition(from, to, bucket, bucketFrom, bucketTo, d);

        if (bucket != 0 && d + 1 < maxLength) {
          // all elements in bucket 0 are equal so we only need to recurse if bucket != 0
          select(bucketFrom, bucketTo, k, d + 1, l + 1);
        }
        return;
      }
      bucketFrom = bucketTo;
    }
    throw new AssertionError("Unreachable code");
  }

  // only used from assert
  private boolean assertHistogram(int commonPrefixLength, int[] histogram) {
    int numberOfUniqueBytes = 0;
    for (int freq : histogram) {
      if (freq > 0) {
        numberOfUniqueBytes++;
      }
    }
    if (numberOfUniqueBytes == 1) {
      assert commonPrefixLength >= 1;
    } else {
      assert commonPrefixLength == 0;
    }
    return true;
  }

  /** Return a number for the k-th character between 0 and {@link #HISTOGRAM_SIZE}. */
  private int getBucket(int i, int k) {
    return byteAt(i, k) + 1;
  }

  /** Build a histogram of the number of values per {@link #getBucket(int, int) bucket}
   *  and return a common prefix length for all visited values.
   *  @see #buildHistogram */
  private int computeCommonPrefixLengthAndBuildHistogram(int from, int to, int k, int[] histogram) {
    final int[] commonPrefix = this.commonPrefix;
    int commonPrefixLength = Math.min(commonPrefix.length, maxLength - k);
    for (int j = 0; j < commonPrefixLength; ++j) {
      final int b = byteAt(from, k + j);
      commonPrefix[j] = b;
      if (b == -1) {
        commonPrefixLength = j + 1;
        break;
      }
    }

    int i;
    outer: for (i = from + 1; i < to; ++i) {
      for (int j = 0; j < commonPrefixLength; ++j) {
        final int b = byteAt(i, k + j);
        if (b != commonPrefix[j]) {
          commonPrefixLength = j;
          if (commonPrefixLength == 0) { // we have no common prefix
            histogram[commonPrefix[0] + 1] = i - from;
            histogram[b + 1] = 1;
            break outer;
          }
          break;
        }
      }
    }

    if (i < to) {
      // the loop got broken because there is no common prefix
      assert commonPrefixLength == 0;
      buildHistogram(i + 1, to, k, histogram);
    } else {
      assert commonPrefixLength > 0;
      histogram[commonPrefix[0] + 1] = to - from;
    }

    return commonPrefixLength;
  }

  /** Build an histogram of the k-th characters of values occurring between
   *  offsets {@code from} and {@code to}, using {@link #getBucket}. */
  private void buildHistogram(int from, int to, int k, int[] histogram) {
    for (int i = from; i < to; ++i) {
      histogram[getBucket(i, k)]++;
    }
  }

  /** Reorder elements so that all of them that fall into {@code bucket} are
   *  between offsets {@code bucketFrom} and {@code bucketTo}. */
  private void partition(int from, int to, int bucket, int bucketFrom, int bucketTo, int d) {
    int left = from;
    int right = to - 1;

    int slot = bucketFrom;

    for (;;) {
      int leftBucket = getBucket(left, d);
      int rightBucket = getBucket(right, d);

      while (leftBucket <= bucket && left < bucketFrom) {
        if (leftBucket == bucket) {
          swap(left, slot++);
        } else {
          ++left;
        }
        leftBucket = getBucket(left, d);
      }

      while (rightBucket >= bucket && right >= bucketTo) {
        if (rightBucket == bucket) {
          swap(right, slot++);
        } else {
          --right;
        }
        rightBucket = getBucket(right, d);
      }

      if (left < bucketFrom && right >= bucketTo) {
        swap(left++, right--);
      } else {
        assert left == bucketFrom;
        assert right == bucketTo - 1;
        break;
      }
    }
  }
}
