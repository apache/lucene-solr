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

public class TestIntroSelector extends LuceneTestCase {

  public void testSelect() {
    for (int iter = 0; iter < 100; ++iter) {
      doTestSelect(false);
    }
  }

  public void testSlowSelect() {
    for (int iter = 0; iter < 100; ++iter) {
      doTestSelect(true);
    }
  }

  private void doTestSelect(boolean slow) {
    final int from = random().nextInt(5);
    final int to = from + TestUtil.nextInt(random(), 1, 10000);
    final int max = random().nextBoolean() ? random().nextInt(100) : random().nextInt(100000);
    Integer[] arr = new Integer[from + to + random().nextInt(5)];
    for (int i = 0; i < arr.length; ++i) {
      arr[i] = TestUtil.nextInt(random(), 0, max);
    }
    final int k = TestUtil.nextInt(random(), from, to - 1);

    Integer[] expected = arr.clone();
    Arrays.sort(expected, from, to);

    Integer[] actual = arr.clone();
    IntroSelector selector = new IntroSelector() {

      Integer pivot;

      @Override
      protected void swap(int i, int j) {
        ArrayUtil.swap(actual, i, j);
      }

      @Override
      protected void setPivot(int i) {
        pivot = actual[i];
      }

      @Override
      protected int comparePivot(int j) {
        return pivot.compareTo(actual[j]);
      }
    };
    if (slow) {
      selector.slowSelect(from, to, k);
    } else {
      selector.select(from, to, k);
    }

    assertEquals(expected[k], actual[k]);
    for (int i = 0; i < actual.length; ++i) {
      if (i < from || i >= to) {
        assertSame(arr[i], actual[i]);
      } else if (i <= k) {
        assertTrue(actual[i].intValue() <= actual[k].intValue());
      } else {
        assertTrue(actual[i].intValue() >= actual[k].intValue());
      }
    }
  }

}
