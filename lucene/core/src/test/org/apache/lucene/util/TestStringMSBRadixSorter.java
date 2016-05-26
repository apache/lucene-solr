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

public class TestStringMSBRadixSorter extends LuceneTestCase {

  private void test(BytesRef[] refs, int len) {
    BytesRef[] expected = Arrays.copyOf(refs, len);
    Arrays.sort(expected);

    new StringMSBRadixSorter() {

      @Override
      protected BytesRef get(int i) {
        return refs[i];
      }

      @Override
      protected void swap(int i, int j) {
        BytesRef tmp = refs[i];
        refs[i] = refs[j];
        refs[j] = tmp;
      }
    }.sort(0, len);
    BytesRef[] actual = Arrays.copyOf(refs, len);
    assertArrayEquals(expected, actual);
  }

  public void testEmpty() {
    test(new BytesRef[random().nextInt(5)], 0);
  }

  public void testOneValue() {
    BytesRef bytes = new BytesRef(TestUtil.randomSimpleString(random()));
    test(new BytesRef[] { bytes }, 1);
  }

  public void testTwoValues() {
    BytesRef bytes1 = new BytesRef(TestUtil.randomSimpleString(random()));
    BytesRef bytes2 = new BytesRef(TestUtil.randomSimpleString(random()));
    test(new BytesRef[] { bytes1, bytes2 }, 2);
  }

  private void testRandom(int commonPrefixLen, int maxLen) {
    byte[] commonPrefix = new byte[commonPrefixLen];
    random().nextBytes(commonPrefix);
    final int len = random().nextInt(100000);
    BytesRef[] bytes = new BytesRef[len + random().nextInt(50)];
    for (int i = 0; i < len; ++i) {
      byte[] b = new byte[commonPrefixLen + random().nextInt(maxLen)];
      random().nextBytes(b);
      System.arraycopy(commonPrefix, 0, b, 0, commonPrefixLen);
      bytes[i] = new BytesRef(b);
    }
    test(bytes, len);
  }

  public void testRandom() {
    for (int iter = 0; iter < 10; ++iter) {
      testRandom(0, 10);
    }
  }

  public void testRandomWithLotsOfDuplicates() {
    for (int iter = 0; iter < 10; ++iter) {
      testRandom(0, 2);
    }
  }

  public void testRandomWithSharedPrefix() {
    for (int iter = 0; iter < 10; ++iter) {
      testRandom(TestUtil.nextInt(random(), 1, 30), 10);
    }
  }

  public void testRandomWithSharedPrefixAndLotsOfDuplicates() {
    for (int iter = 0; iter < 10; ++iter) {
      testRandom(TestUtil.nextInt(random(), 1, 30), 2);
    }
  }
}
