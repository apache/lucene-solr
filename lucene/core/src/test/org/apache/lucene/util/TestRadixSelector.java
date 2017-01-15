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

public class TestRadixSelector extends LuceneTestCase {

  public void testSelect() {
    for (int iter = 0; iter < 100; ++iter) {
      doTestSelect();
    }
  }

  private void doTestSelect() {
    final int from = random().nextInt(5);
    final int to = from + TestUtil.nextInt(random(), 1, 10000);
    final int maxLen = TestUtil.nextInt(random(), 1, 12);
    BytesRef[] arr = new BytesRef[from + to + random().nextInt(5)];
    for (int i = 0; i < arr.length; ++i) {
      byte[] bytes = new byte[TestUtil.nextInt(random(), 0, maxLen)];
      random().nextBytes(bytes);
      arr[i] = new BytesRef(bytes);
    }
    doTest(arr, from, to, maxLen);
  }

  public void testSharedPrefixes() {
    for (int iter = 0; iter < 100; ++iter) {
      doTestSharedPrefixes();
    }
  }

  private void doTestSharedPrefixes() {
    final int from = random().nextInt(5);
    final int to = from + TestUtil.nextInt(random(), 1, 10000);
    final int maxLen = TestUtil.nextInt(random(), 1, 12);
    BytesRef[] arr = new BytesRef[from + to + random().nextInt(5)];
    for (int i = 0; i < arr.length; ++i) {
      byte[] bytes = new byte[TestUtil.nextInt(random(), 0, maxLen)];
      random().nextBytes(bytes);
      arr[i] = new BytesRef(bytes);
    }
    final int sharedPrefixLength = Math.min(arr[0].length, TestUtil.nextInt(random(), 1, maxLen));
    for (int i = 1; i < arr.length; ++i) {
      System.arraycopy(arr[0].bytes, arr[0].offset, arr[i].bytes, arr[i].offset, Math.min(sharedPrefixLength, arr[i].length));
    }
    doTest(arr, from, to, maxLen);
  }

  private void doTest(BytesRef[] arr, int from, int to, int maxLen) {
    final int k = TestUtil.nextInt(random(), from, to - 1);

    BytesRef[] expected = arr.clone();
    Arrays.sort(expected, from, to);

    BytesRef[] actual = arr.clone();
    final int enforcedMaxLen = random().nextBoolean() ? maxLen : Integer.MAX_VALUE;
    RadixSelector selector = new RadixSelector(enforcedMaxLen) {

      @Override
      protected void swap(int i, int j) {
        ArrayUtil.swap(actual, i, j);
      }

      @Override
      protected int byteAt(int i, int k) {
        assertTrue(k < enforcedMaxLen);
        BytesRef b = actual[i];
        if (k >= b.length) {
          return -1;
        } else {
          return Byte.toUnsignedInt(b.bytes[b.offset + k]);
        }
      }

    };
    selector.select(from, to, k);

    assertEquals(expected[k], actual[k]);
    for (int i = 0; i < actual.length; ++i) {
      if (i < from || i >= to) {
        assertSame(arr[i], actual[i]);
      } else if (i <= k) {
        assertTrue(actual[i].compareTo(actual[k]) <= 0);
      } else {
        assertTrue(actual[i].compareTo(actual[k]) >= 0);
      }
    }
  }

}
