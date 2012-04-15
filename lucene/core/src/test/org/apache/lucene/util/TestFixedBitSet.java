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

package org.apache.lucene.util;

import java.io.IOException;
import java.util.BitSet;

import org.apache.lucene.search.DocIdSetIterator;

public class TestFixedBitSet extends LuceneTestCase {

  void doGet(BitSet a, FixedBitSet b) {
    int max = b.length();
    for (int i=0; i<max; i++) {
      if (a.get(i) != b.get(i)) {
        fail("mismatch: BitSet=["+i+"]="+a.get(i));
      }
    }
  }

  void doNextSetBit(BitSet a, FixedBitSet b) {
    int aa=-1,bb=-1;
    do {
      aa = a.nextSetBit(aa+1);
      bb = bb < b.length()-1 ? b.nextSetBit(bb+1) : -1;
      assertEquals(aa,bb);
    } while (aa>=0);
  }

  void doPrevSetBit(BitSet a, FixedBitSet b) {
    int aa = a.size() + random().nextInt(100);
    int bb = aa;
    do {
      // aa = a.prevSetBit(aa-1);
      aa--;
      while ((aa >= 0) && (! a.get(aa))) {
      	aa--;
      }
      if (b.length() == 0) {
        bb = -1;
      } else if (bb > b.length()-1) {
        bb = b.prevSetBit(b.length()-1);
      } else if (bb < 1) {
        bb = -1;
      } else {
        bb = bb >= 1 ? b.prevSetBit(bb-1) : -1;
      }
      assertEquals(aa,bb);
    } while (aa>=0);
  }

  // test interleaving different FixedBitSetIterator.next()/skipTo()
  void doIterate(BitSet a, FixedBitSet b, int mode) throws IOException {
    if (mode==1) doIterate1(a, b);
    if (mode==2) doIterate2(a, b);
  }

  void doIterate1(BitSet a, FixedBitSet b) throws IOException {
    int aa=-1,bb=-1;
    DocIdSetIterator iterator = b.iterator();
    do {
      aa = a.nextSetBit(aa+1);
      bb = (bb < b.length() && random().nextBoolean()) ? iterator.nextDoc() : iterator.advance(bb + 1);
      assertEquals(aa == -1 ? DocIdSetIterator.NO_MORE_DOCS : aa, bb);
    } while (aa>=0);
  }

  void doIterate2(BitSet a, FixedBitSet b) throws IOException {
    int aa=-1,bb=-1;
    DocIdSetIterator iterator = b.iterator();
    do {
      aa = a.nextSetBit(aa+1);
      bb = random().nextBoolean() ? iterator.nextDoc() : iterator.advance(bb + 1);
      assertEquals(aa == -1 ? DocIdSetIterator.NO_MORE_DOCS : aa, bb);
    } while (aa>=0);
  }

  void doRandomSets(int maxSize, int iter, int mode) throws IOException {
    BitSet a0=null;
    FixedBitSet b0=null;

    for (int i=0; i<iter; i++) {
      int sz = _TestUtil.nextInt(random(), 2, maxSize);
      BitSet a = new BitSet(sz);
      FixedBitSet b = new FixedBitSet(sz);

      // test the various ways of setting bits
      if (sz>0) {
        int nOper = random().nextInt(sz);
        for (int j=0; j<nOper; j++) {
          int idx;         

          idx = random().nextInt(sz);
          a.set(idx);
          b.set(idx);
          
          idx = random().nextInt(sz);
          a.clear(idx);
          b.clear(idx);
          
          idx = random().nextInt(sz);
          a.flip(idx);
          b.flip(idx, idx+1);

          idx = random().nextInt(sz);
          a.flip(idx);
          b.flip(idx, idx+1);

          boolean val2 = b.get(idx);
          boolean val = b.getAndSet(idx);
          assertTrue(val2 == val);
          assertTrue(b.get(idx));
          
          if (!val) b.clear(idx);
          assertTrue(b.get(idx) == val);
        }
      }

      // test that the various ways of accessing the bits are equivalent
      doGet(a,b);

      // test ranges, including possible extension
      int fromIndex, toIndex;
      fromIndex = random().nextInt(sz/2);
      toIndex = fromIndex + random().nextInt(sz - fromIndex);
      BitSet aa = (BitSet)a.clone(); aa.flip(fromIndex,toIndex);
      FixedBitSet bb = b.clone(); bb.flip(fromIndex,toIndex);

      doIterate(aa,bb, mode);   // a problem here is from flip or doIterate

      fromIndex = random().nextInt(sz/2);
      toIndex = fromIndex + random().nextInt(sz - fromIndex);
      aa = (BitSet)a.clone(); aa.clear(fromIndex,toIndex);
      bb = b.clone(); bb.clear(fromIndex,toIndex);

      doNextSetBit(aa,bb); // a problem here is from clear() or nextSetBit
      
      doPrevSetBit(aa,bb);

      fromIndex = random().nextInt(sz/2);
      toIndex = fromIndex + random().nextInt(sz - fromIndex);
      aa = (BitSet)a.clone(); aa.set(fromIndex,toIndex);
      bb = b.clone(); bb.set(fromIndex,toIndex);

      doNextSetBit(aa,bb); // a problem here is from set() or nextSetBit
    
      doPrevSetBit(aa,bb);

      if (b0 != null && b0.length() <= b.length()) {
        assertEquals(a.cardinality(), b.cardinality());

        BitSet a_and = (BitSet)a.clone(); a_and.and(a0);
        BitSet a_or = (BitSet)a.clone(); a_or.or(a0);
        BitSet a_andn = (BitSet)a.clone(); a_andn.andNot(a0);

        FixedBitSet b_and = b.clone(); assertEquals(b,b_and); b_and.and(b0);
        FixedBitSet b_or = b.clone(); b_or.or(b0);
        FixedBitSet b_andn = b.clone(); b_andn.andNot(b0);

        assertEquals(a0.cardinality(), b0.cardinality());
        assertEquals(a_or.cardinality(), b_or.cardinality());

        doIterate(a_and,b_and, mode);
        doIterate(a_or,b_or, mode);
        doIterate(a_andn,b_andn, mode);

        assertEquals(a_and.cardinality(), b_and.cardinality());
        assertEquals(a_or.cardinality(), b_or.cardinality());
        assertEquals(a_andn.cardinality(), b_andn.cardinality());
      }

      a0=a;
      b0=b;
    }
  }
  
  // large enough to flush obvious bugs, small enough to run in <.5 sec as part of a
  // larger testsuite.
  public void testSmall() throws IOException {
    doRandomSets(atLeast(1200), atLeast(1000), 1);
    doRandomSets(atLeast(1200), atLeast(1000), 2);
  }

  // uncomment to run a bigger test (~2 minutes).
  /*
  public void testBig() {
    doRandomSets(2000,200000, 1);
    doRandomSets(2000,200000, 2);
  }
  */

  public void testEquals() {
    // This test can't handle numBits==0:
    final int numBits = random().nextInt(2000) + 1;
    FixedBitSet b1 = new FixedBitSet(numBits);
    FixedBitSet b2 = new FixedBitSet(numBits);
    assertTrue(b1.equals(b2));
    assertTrue(b2.equals(b1));
    for(int iter=0;iter<10*RANDOM_MULTIPLIER;iter++) {
      int idx = random().nextInt(numBits);
      if (!b1.get(idx)) {
        b1.set(idx);
        assertFalse(b1.equals(b2));
        assertFalse(b2.equals(b1));
        b2.set(idx);
        assertTrue(b1.equals(b2));
        assertTrue(b2.equals(b1));
      }
    }

    // try different type of object
    assertFalse(b1.equals(new Object()));
  }
  
  public void testHashCodeEquals() {
    // This test can't handle numBits==0:
    final int numBits = random().nextInt(2000) + 1;
    FixedBitSet b1 = new FixedBitSet(numBits);
    FixedBitSet b2 = new FixedBitSet(numBits);
    assertTrue(b1.equals(b2));
    assertTrue(b2.equals(b1));
    for(int iter=0;iter<10*RANDOM_MULTIPLIER;iter++) {
      int idx = random().nextInt(numBits);
      if (!b1.get(idx)) {
        b1.set(idx);
        assertFalse(b1.equals(b2));
        assertFalse(b1.hashCode() == b2.hashCode());
        b2.set(idx);
        assertEquals(b1, b2);
        assertEquals(b1.hashCode(), b2.hashCode());
      }
    }
  } 

  public void testSmallBitSets() {
    // Make sure size 0-10 bit sets are OK:
    for(int numBits=0;numBits<10;numBits++) {
      FixedBitSet b1 = new FixedBitSet(numBits);
      FixedBitSet b2 = new FixedBitSet(numBits);
      assertTrue(b1.equals(b2));
      assertEquals(b1.hashCode(), b2.hashCode());
      assertEquals(0, b1.cardinality());
      if (numBits > 0) {
        b1.set(0, numBits);
        assertEquals(numBits, b1.cardinality());
        b1.flip(0, numBits);
        assertEquals(0, b1.cardinality());
      }
    }
  }
  
  private FixedBitSet makeFixedBitSet(int[] a, int numBits) {
    FixedBitSet bs = new FixedBitSet(numBits);
    for (int e: a) {
      bs.set(e);
    }
    return bs;
  }

  private BitSet makeBitSet(int[] a) {
    BitSet bs = new BitSet();
    for (int e: a) {
      bs.set(e);
    }
    return bs;
  }

  private void checkPrevSetBitArray(int [] a, int numBits) {
    FixedBitSet obs = makeFixedBitSet(a, numBits);
    BitSet bs = makeBitSet(a);
    doPrevSetBit(bs, obs);
  }

  public void testPrevSetBit() {
    checkPrevSetBitArray(new int[] {}, 0);
    checkPrevSetBitArray(new int[] {0}, 1);
    checkPrevSetBitArray(new int[] {0,2}, 3);
  }
}



