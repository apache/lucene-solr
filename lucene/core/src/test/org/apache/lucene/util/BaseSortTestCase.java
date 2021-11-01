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
import java.util.Random;

public abstract class BaseSortTestCase extends LuceneTestCase {

  public static class Entry implements java.lang.Comparable<Entry> {

    public final int value;
    public final int ord;

    public Entry(int value, int ord) {
      this.value = value;
      this.ord = ord;
    }

    @Override
    public int compareTo(Entry other) {
      return Integer.compare(value, other.value);
    }
  }

  private final boolean stable;

  public BaseSortTestCase(boolean stable) {
    this.stable = stable;
  }

  public abstract Sorter newSorter(Entry[] arr);

  public void assertSorted(Entry[] original, Entry[] sorted) {
    assertEquals(original.length, sorted.length);
    Entry[] actuallySorted = ArrayUtil.copyOfSubArray(original, 0, original.length);
    Arrays.sort(actuallySorted);
    for (int i = 0; i < original.length; ++i) {
      assertEquals(actuallySorted[i].value, sorted[i].value);
      if (stable) {
        assertEquals(actuallySorted[i].ord, sorted[i].ord);
      }
    }
  }

  public void test(Entry[] arr) {
    final int o = random().nextInt(1000);
    final Entry[] toSort = new Entry[o + arr.length + random().nextInt(3)];
    System.arraycopy(arr, 0, toSort, o, arr.length);
    final Sorter sorter = newSorter(toSort);
    sorter.sort(o, o + arr.length);
    assertSorted(arr, ArrayUtil.copyOfSubArray(toSort, o, o + arr.length));
  }

  enum Strategy {
    RANDOM {
      @Override
      public void set(Entry[] arr, int i, Random random) {
        arr[i] = new Entry(random.nextInt(), i);
      }
    },
    RANDOM_LOW_CARDINALITY {
      @Override
      public void set(Entry[] arr, int i, Random random) {
        arr[i] = new Entry(random.nextInt(6), i);
      }
    },
    RANDOM_MEDIUM_CARDINALITY {
      @Override
      public void set(Entry[] arr, int i, Random random) {
        arr[i] = new Entry(random.nextInt(arr.length / 2), i);
      }
    },
    ASCENDING {
      @Override
      public void set(Entry[] arr, int i, Random random) {
        arr[i] =
            i == 0
                ? new Entry(random.nextInt(6), 0)
                : new Entry(arr[i - 1].value + random.nextInt(6), i);
      }
    },
    DESCENDING {
      @Override
      public void set(Entry[] arr, int i, Random random) {
        arr[i] =
            i == 0
                ? new Entry(random.nextInt(6), 0)
                : new Entry(arr[i - 1].value - random.nextInt(6), i);
      }
    },
    STRICTLY_DESCENDING {
      @Override
      public void set(Entry[] arr, int i, Random random) {
        arr[i] =
            i == 0
                ? new Entry(random.nextInt(6), 0)
                : new Entry(arr[i - 1].value - TestUtil.nextInt(random, 1, 5), i);
      }
    },
    ASCENDING_SEQUENCES {
      @Override
      public void set(Entry[] arr, int i, Random random) {
        arr[i] =
            i == 0
                ? new Entry(random.nextInt(6), 0)
                : new Entry(
                rarely(random) ? random.nextInt(1000) : arr[i - 1].value + random.nextInt(6),
                i);
      }
    },
    MOSTLY_ASCENDING {
      @Override
      public void set(Entry[] arr, int i, Random random) {
        arr[i] =
            i == 0
                ? new Entry(random.nextInt(6), 0)
                : new Entry(arr[i - 1].value + TestUtil.nextInt(random, -8, 10), i);
      }
    };

    public abstract void set(Entry[] arr, int i, Random random);
  }

  public void test(Strategy strategy, int length) {
    Random random = random();
    final Entry[] arr = new Entry[length];
    for (int i = 0; i < arr.length; ++i) {
      strategy.set(arr, i, random);
    }
    test(arr);
  }

  public void test(Strategy strategy) {
    test(strategy, random().nextInt(20000));
  }

  public void testEmpty() {
    test(new Entry[0]);
  }

  public void testOne() {
    test(Strategy.RANDOM, 1);
  }

  public void testTwo() {
    test(Strategy.RANDOM_LOW_CARDINALITY, 2);
  }

  public void testRandom() {
    test(Strategy.RANDOM);
  }

  public void testRandomLowCardinality() {
    test(Strategy.RANDOM_LOW_CARDINALITY);
  }

  public void testAscending() {
    test(Strategy.ASCENDING);
  }

  public void testAscendingSequences() {
    test(Strategy.ASCENDING_SEQUENCES);
  }

  public void testDescending() {
    test(Strategy.DESCENDING);
  }

  public void testStrictlyDescending() {
    test(Strategy.STRICTLY_DESCENDING);
  }
}
