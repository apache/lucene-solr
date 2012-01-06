package org.apache.lucene.util;

/**
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

import java.io.IOException;
import java.util.BitSet;


import org.apache.lucene.search.DocIdSetIterator;

public class TestSortedVIntList extends LuceneTestCase {
  
  void tstIterator (
          SortedVIntList vintList,
          int[] ints) throws IOException {
    for (int i = 0; i < ints.length; i++) {
      if ((i > 0) && (ints[i-1] == ints[i])) {
        return; // DocNrSkipper should not skip to same document.
      }
    }
    DocIdSetIterator m = vintList.iterator();
    for (int i = 0; i < ints.length; i++) {
      assertTrue("No end of Matcher at: " + i, m.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
      assertEquals(ints[i], m.docID());
    }
    assertTrue("End of Matcher", m.nextDoc() == DocIdSetIterator.NO_MORE_DOCS);
  }

  void tstVIntList(
          SortedVIntList vintList,
          int[] ints,
          int expectedByteSize) throws IOException {
    assertEquals("Size", ints.length, vintList.size());
    assertEquals("Byte size", expectedByteSize, vintList.getByteSize());
    tstIterator(vintList, ints);
  }

  public void tstViaBitSet(int [] ints, int expectedByteSize) throws IOException {
    final int MAX_INT_FOR_BITSET = 1024 * 1024;
    BitSet bs = new BitSet();
    for (int i = 0; i < ints.length; i++) {
      if (ints[i] > MAX_INT_FOR_BITSET) {
        return; // BitSet takes too much memory
      }
      if ((i > 0) && (ints[i-1] == ints[i])) {
        return; // BitSet cannot store duplicate.
      }
      bs.set(ints[i]);
    }
    SortedVIntList svil = new SortedVIntList(bs);
    tstVIntList(svil, ints, expectedByteSize);
    tstVIntList(new SortedVIntList(svil.iterator()), ints, expectedByteSize);
  }
  
  private static final int VB1 = 0x7F;
  private static final int BIT_SHIFT = 7;
  private static final int VB2 = (VB1 << BIT_SHIFT) | VB1;
  private static final int VB3 = (VB2 << BIT_SHIFT) | VB1;
  private static final int VB4 = (VB3 << BIT_SHIFT) | VB1;

  private int vIntByteSize(int i) {
    assert i >= 0;
    if (i <= VB1) return 1;
    if (i <= VB2) return 2;
    if (i <= VB3) return 3;
    if (i <= VB4) return 4;
    return 5;
  }

  private int vIntListByteSize(int [] ints) {
    int byteSize = 0;
    int last = 0;
    for (int i = 0; i < ints.length; i++) {
      byteSize += vIntByteSize(ints[i] - last);
      last = ints[i];
    }
    return byteSize;
  }
  
  public void tstInts(int [] ints) {
    int expectedByteSize = vIntListByteSize(ints);
    try {
      tstVIntList(new SortedVIntList(ints), ints, expectedByteSize);
      tstViaBitSet(ints, expectedByteSize);
    } catch (IOException ioe) {
      throw new Error(ioe);
    }
  }

  public void tstIllegalArgExc(int [] ints) {
    try {
      new SortedVIntList(ints);
    }
    catch (IllegalArgumentException e) {
      return;
    }
    fail("Expected IllegalArgumentException");    
  }

  private int[] fibArray(int a, int b, int size) {
    final int[] fib = new int[size];
    fib[0] = a;
    fib[1] = b;
    for (int i = 2; i < size; i++) {
      fib[i] = fib[i-1] + fib[i-2];
    }
    return fib;
  }

  private int[] reverseDiffs(int []ints) { // reverse the order of the successive differences
    final int[] res = new int[ints.length];
    for (int i = 0; i < ints.length; i++) {
      res[i] = ints[ints.length - 1] + (ints[0] - ints[ints.length - 1 - i]);
    }
    return res;
  }

  public void test01() {
    tstInts(new int[] {});
  }
  public void test02() {
    tstInts(new int[] {0});
  }
  public void test04a() {
    tstInts(new int[] {0, VB2 - 1});
  }
  public void test04b() {
    tstInts(new int[] {0, VB2});
  }
  public void test04c() {
    tstInts(new int[] {0, VB2 + 1});
  }
  public void test05() {
    tstInts(fibArray(0,1,7)); // includes duplicate value 1
  }
  public void test05b() {
    tstInts(reverseDiffs(fibArray(0,1,7)));
  }
  public void test06() {
    tstInts(fibArray(1,2,45)); // no duplicates, size 46 exceeds max int.
  }
  public void test06b() {
    tstInts(reverseDiffs(fibArray(1,2,45)));
  }
  public void test07a() {
    tstInts(new int[] {0, VB3});
  }
  public void test07b() {
    tstInts(new int[] {1, VB3 + 2});
  }
  public void test07c() {
    tstInts(new int[] {2, VB3 + 4});
  }
  public void test08a() {
    tstInts(new int[] {0, VB4 + 1});
  }
  public void test08b() {
    tstInts(new int[] {1, VB4 + 1});
  }
  public void test08c() {
    tstInts(new int[] {2, VB4 + 1});
  }

  public void test10() {
    tstIllegalArgExc(new int[] {-1});
  }
  public void test11() {
    tstIllegalArgExc(new int[] {1,0});
  }
  public void test12() {
   tstIllegalArgExc(new int[] {0,1,1,2,3,5,8,0});
  }
  public void test13Allocation() throws Exception {
    int [] a = new int[2000]; // SortedVIntList initial byte size is 128
    for (int i = 0; i < a.length; i++) {
      a[i] = (107 + i) * i;
    }
    tstIterator(new SortedVIntList(a), a);
  }
}
