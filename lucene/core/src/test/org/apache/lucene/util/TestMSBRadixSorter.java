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
import java.util.HashSet;
import java.util.Set;

public class TestMSBRadixSorter extends LuceneTestCase {

  private void test(BytesRef[] refs, int len) {
    BytesRef[] expected = Arrays.copyOf(refs, len);
    Arrays.sort(expected);

    int maxLength = 0;
    for (int i = 0; i < len; ++i) {
      BytesRef ref = refs[i];
      maxLength = Math.max(maxLength, ref.length);
    }
    switch (random().nextInt(3)) {
      case 0:
        maxLength += TestUtil.nextInt(random(), 1, 5);
        break;
      case 1:
        maxLength = Integer.MAX_VALUE;
        break;
      default:
        // leave unchanged
        break;
    }

    final int finalMaxLength = maxLength;
    new MSBRadixSorter(maxLength) {

      @Override
      protected int byteAt(int i, int k) {
        assertTrue(k < finalMaxLength);
        BytesRef ref = refs[i];
        if (ref.length <= k) {
          return -1;
        }
        return ref.bytes[ref.offset + k] & 0xff;
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

  public void testRandom2() {
    // how large our alphabet is
    int letterCount = TestUtil.nextInt(random(), 2, 10);

    // how many substring fragments to use
    int substringCount = TestUtil.nextInt(random(), 2, 10);
    Set<BytesRef> substringsSet = new HashSet<>();

    // how many strings to make
    int stringCount = atLeast(10000);

    //System.out.println("letterCount=" + letterCount + " substringCount=" + substringCount + " stringCount=" + stringCount);
    while(substringsSet.size() < substringCount) {
      int length = TestUtil.nextInt(random(), 2, 10);
      byte[] bytes = new byte[length];
      for(int i=0;i<length;i++) {
        bytes[i] = (byte) random().nextInt(letterCount);
      }
      BytesRef br = new BytesRef(bytes);
      substringsSet.add(br);
      //System.out.println("add substring count=" + substringsSet.size() + ": " + br);
    }

    BytesRef[] substrings = substringsSet.toArray(new BytesRef[substringsSet.size()]);
    double[] chance = new double[substrings.length];
    double sum = 0.0;
    for(int i=0;i<substrings.length;i++) {
      chance[i] = random().nextDouble();
      sum += chance[i];
    }

    // give each substring a random chance of occurring:
    double accum = 0.0;
    for(int i=0;i<substrings.length;i++) {
      accum += chance[i]/sum;
      chance[i] = accum;
    }

    Set<BytesRef> stringsSet = new HashSet<>();
    int iters = 0;
    while (stringsSet.size() < stringCount && iters < stringCount*5) {
      int count = TestUtil.nextInt(random(), 1, 5);
      BytesRefBuilder b = new BytesRefBuilder();
      for(int i=0;i<count;i++) {
        double v = random().nextDouble();
        accum = 0.0;
        for(int j=0;j<substrings.length;j++) {
          accum += chance[j];
          if (accum >= v) {
            b.append(substrings[j]);
            break;
          }
        }
      }
      BytesRef br = b.toBytesRef();
      stringsSet.add(br);
      //System.out.println("add string count=" + stringsSet.size() + ": " + br);
      iters++;
    }

    test(stringsSet.toArray(new BytesRef[stringsSet.size()]), stringsSet.size());
  }
}
