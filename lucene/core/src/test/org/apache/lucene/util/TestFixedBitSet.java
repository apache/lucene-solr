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


import java.io.IOException;
import java.util.Random;

import org.apache.lucene.search.DocIdSetIterator;

public class TestFixedBitSet extends BaseBitSetTestCase<FixedBitSet> {

  @Override
  public FixedBitSet copyOf(BitSet bs, int length) throws IOException {
    final FixedBitSet set = new FixedBitSet(length);
    for (int doc = bs.nextSetBit(0); doc != DocIdSetIterator.NO_MORE_DOCS; doc = doc + 1 >= length ? DocIdSetIterator.NO_MORE_DOCS : bs.nextSetBit(doc + 1)) {
      set.set(doc);
    }
    return set;
  }

  void doGet(java.util.BitSet a, FixedBitSet b) {
    assertEquals(a.cardinality(), b.cardinality());
    int max = b.length();
    for (int i=0; i<max; i++) {
      if (a.get(i) != b.get(i)) {
        fail("mismatch: BitSet=["+i+"]="+a.get(i));
      }
    }
  }

  void doNextSetBit(java.util.BitSet a, FixedBitSet b) {
    assertEquals(a.cardinality(), b.cardinality());
    int aa=-1;
    int bb=-1;
    do {
      aa = a.nextSetBit(aa+1);
      if (aa == -1) {
        aa = DocIdSetIterator.NO_MORE_DOCS;
      }
      bb = bb < b.length()-1 ? b.nextSetBit(bb+1) : DocIdSetIterator.NO_MORE_DOCS;
      assertEquals(aa,bb);
    } while (aa != DocIdSetIterator.NO_MORE_DOCS);
  }

  void doPrevSetBit(java.util.BitSet a, FixedBitSet b) {
    assertEquals(a.cardinality(), b.cardinality());
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
  void doIterate(java.util.BitSet a, FixedBitSet b, int mode) throws IOException {
    if (mode==1) doIterate1(a, b);
    if (mode==2) doIterate2(a, b);
  }

  void doIterate1(java.util.BitSet a, FixedBitSet b) throws IOException {
    assertEquals(a.cardinality(), b.cardinality());
    int aa=-1,bb=-1;
    DocIdSetIterator iterator = new BitSetIterator(b, 0);
    do {
      aa = a.nextSetBit(aa+1);
      bb = (bb < b.length() && random().nextBoolean()) ? iterator.nextDoc() : iterator.advance(bb + 1);
      assertEquals(aa == -1 ? DocIdSetIterator.NO_MORE_DOCS : aa, bb);
    } while (aa>=0);
  }

  void doIterate2(java.util.BitSet a, FixedBitSet b) throws IOException {
    assertEquals(a.cardinality(), b.cardinality());
    int aa=-1,bb=-1;
    DocIdSetIterator iterator = new BitSetIterator(b, 0);
    do {
      aa = a.nextSetBit(aa+1);
      bb = random().nextBoolean() ? iterator.nextDoc() : iterator.advance(bb + 1);
      assertEquals(aa == -1 ? DocIdSetIterator.NO_MORE_DOCS : aa, bb);
    } while (aa>=0);
  }

  void doRandomSets(int maxSize, int iter, int mode) throws IOException {
    java.util.BitSet a0=null;
    FixedBitSet b0=null;

    for (int i=0; i<iter; i++) {
      int sz = TestUtil.nextInt(random(), 2, maxSize);
      java.util.BitSet a = new java.util.BitSet(sz);
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
          a.flip(idx, idx+1);
          b.flip(idx, idx+1);

          idx = random().nextInt(sz);
          a.flip(idx);
          b.flip(idx);

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
      java.util.BitSet aa = (java.util.BitSet)a.clone(); aa.flip(fromIndex,toIndex);
      FixedBitSet bb = b.clone(); bb.flip(fromIndex,toIndex);

      doIterate(aa,bb, mode);   // a problem here is from flip or doIterate

      fromIndex = random().nextInt(sz/2);
      toIndex = fromIndex + random().nextInt(sz - fromIndex);
      aa = (java.util.BitSet)a.clone(); aa.clear(fromIndex,toIndex);
      bb = b.clone(); bb.clear(fromIndex,toIndex);

      doNextSetBit(aa,bb); // a problem here is from clear() or nextSetBit
      
      doPrevSetBit(aa,bb);

      fromIndex = random().nextInt(sz/2);
      toIndex = fromIndex + random().nextInt(sz - fromIndex);
      aa = (java.util.BitSet)a.clone(); aa.set(fromIndex,toIndex);
      bb = b.clone(); bb.set(fromIndex,toIndex);

      doNextSetBit(aa,bb); // a problem here is from set() or nextSetBit
    
      doPrevSetBit(aa,bb);

      if (b0 != null && b0.length() <= b.length()) {
        assertEquals(a.cardinality(), b.cardinality());

        java.util.BitSet a_and = (java.util.BitSet)a.clone(); a_and.and(a0);
        java.util.BitSet a_or = (java.util.BitSet)a.clone(); a_or.or(a0);
        java.util.BitSet a_xor = (java.util.BitSet)a.clone(); a_xor.xor(a0);
        java.util.BitSet a_andn = (java.util.BitSet)a.clone(); a_andn.andNot(a0);

        FixedBitSet b_and = b.clone(); assertEquals(b,b_and); b_and.and(b0);
        FixedBitSet b_or = b.clone(); b_or.or(b0);
        FixedBitSet b_xor = b.clone(); b_xor.xor(b0);
        FixedBitSet b_andn = b.clone(); b_andn.andNot(b0);

        assertEquals(a0.cardinality(), b0.cardinality());
        assertEquals(a_or.cardinality(), b_or.cardinality());

        doIterate(a_and,b_and, mode);
        doIterate(a_or,b_or, mode);
        doIterate(a_andn,b_andn, mode);
        doIterate(a_xor,b_xor, mode);
        
        assertEquals(a_and.cardinality(), b_and.cardinality());
        assertEquals(a_or.cardinality(), b_or.cardinality());
        assertEquals(a_xor.cardinality(), b_xor.cardinality());
        assertEquals(a_andn.cardinality(), b_andn.cardinality());
      }

      a0=a;
      b0=b;
    }
  }

  // large enough to flush obvious bugs, small enough to run in <.5 sec as part of a
  // larger testsuite.
  public void testSmall() throws IOException {
    final int iters = TEST_NIGHTLY ? atLeast(1000) : 100;
    doRandomSets(atLeast(1200), iters, 1);
    doRandomSets(atLeast(1200), iters, 2);
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
    FixedBitSet bs;
    if (random().nextBoolean()) {
      int bits2words = FixedBitSet.bits2words(numBits);
      long[] words = new long[bits2words + random().nextInt(100)];
      bs = new FixedBitSet(words, numBits);
    } else {
      bs = new FixedBitSet(numBits);
    }
    for (int e: a) {
      bs.set(e);
    }
    return bs;
  }

  private java.util.BitSet makeBitSet(int[] a) {
    java.util.BitSet bs = new java.util.BitSet();
    for (int e: a) {
      bs.set(e);
    }
    return bs;
  }

  private void checkPrevSetBitArray(int [] a, int numBits) {
    FixedBitSet obs = makeFixedBitSet(a, numBits);
    java.util.BitSet bs = makeBitSet(a);
    doPrevSetBit(bs, obs);
  }

  public void testPrevSetBit() {
    checkPrevSetBitArray(new int[] {}, 0);
    checkPrevSetBitArray(new int[] {0}, 1);
    checkPrevSetBitArray(new int[] {0,2}, 3);
  }
  
  
  private void checkNextSetBitArray(int [] a, int numBits) {
    FixedBitSet obs = makeFixedBitSet(a, numBits);
    java.util.BitSet bs = makeBitSet(a);
    doNextSetBit(bs, obs);
  }
  
  public void testNextBitSet() {
    int[] setBits = new int[0+random().nextInt(1000)];
    for (int i = 0; i < setBits.length; i++) {
      setBits[i] = random().nextInt(setBits.length);
    }
    checkNextSetBitArray(setBits, setBits.length + random().nextInt(10));
    
    checkNextSetBitArray(new int[0], setBits.length + random().nextInt(10));
  }
  
  public void testEnsureCapacity() {
    FixedBitSet bits = new FixedBitSet(5);
    bits.set(1);
    bits.set(4);
    
    FixedBitSet newBits = FixedBitSet.ensureCapacity(bits, 8); // grow within the word
    assertTrue(newBits.get(1));
    assertTrue(newBits.get(4));
    newBits.clear(1);
    // we align to 64-bits, so even though it shouldn't have, it re-allocated a long[1]
    assertTrue(bits.get(1));
    assertFalse(newBits.get(1));

    newBits.set(1);
    newBits = FixedBitSet.ensureCapacity(newBits, newBits.length() - 2); // reuse
    assertTrue(newBits.get(1));

    bits.set(1);
    newBits = FixedBitSet.ensureCapacity(bits, 72); // grow beyond one word
    assertTrue(newBits.get(1));
    assertTrue(newBits.get(4));
    newBits.clear(1);
    // we grew the long[], so it's not shared
    assertTrue(bits.get(1));
    assertFalse(newBits.get(1));
  }
  
  public void testBits2Words() {
    assertEquals(0, FixedBitSet.bits2words(0));
    assertEquals(1, FixedBitSet.bits2words(1));
    // ...
    assertEquals(1, FixedBitSet.bits2words(64));
    assertEquals(2, FixedBitSet.bits2words(65));
    // ...
    assertEquals(2, FixedBitSet.bits2words(128));
    assertEquals(3, FixedBitSet.bits2words(129));
    // ...
    assertEquals(1024, FixedBitSet.bits2words(65536));
    assertEquals(1025, FixedBitSet.bits2words(65537));
    // ...
    assertEquals(1 << (31-6), FixedBitSet.bits2words(Integer.MAX_VALUE));
  }
  
  private int[] makeIntArray(Random random, int count, int min, int max) {
    int[] rv = new int[count];
    
    for (int i = 0; i < count; ++i) {
      rv[i] = TestUtil.nextInt(random, min, max);
    }
    
    return rv;
  }
  
  // Demonstrates that the presence of ghost bits in the last used word can cause spurious failures
  public void testIntersectionCount() {
    Random random = random();
    
    int numBits1 = TestUtil.nextInt(random, 1000, 2000);
    int numBits2 = TestUtil.nextInt(random, 1000, 2000);
    
    int count1 = TestUtil.nextInt(random, 0, numBits1 - 1);
    int count2 = TestUtil.nextInt(random, 0, numBits2 - 1);

    int[] bits1 = makeIntArray(random, count1, 0, numBits1 - 1);
    int[] bits2 = makeIntArray(random, count2, 0, numBits2 - 1);
    
    FixedBitSet fixedBitSet1 = makeFixedBitSet(bits1, numBits1);
    FixedBitSet fixedBitSet2 = makeFixedBitSet(bits2, numBits2);
    
    // If ghost bits are present, these may fail too, but that's not what we want to demonstrate here
    //assertTrue(fixedBitSet1.cardinality() <= bits1.length);
    //assertTrue(fixedBitSet2.cardinality() <= bits2.length);
    
    long intersectionCount = FixedBitSet.intersectionCount(fixedBitSet1, fixedBitSet2);
    
    java.util.BitSet bitSet1 = makeBitSet(bits1);
    java.util.BitSet bitSet2 = makeBitSet(bits2);
    
    // If ghost bits are present, these may fail too, but that's not what we want to demonstrate here
    //assertEquals(bitSet1.cardinality(), fixedBitSet1.cardinality());
    //assertEquals(bitSet2.cardinality(), fixedBitSet2.cardinality());

    bitSet1.and(bitSet2);
    
    assertEquals(bitSet1.cardinality(), intersectionCount);
  }
  
  // Demonstrates that the presence of ghost bits in the last used word can cause spurious failures
  public void testUnionCount() {
    Random random = random();
    
    int numBits1 = TestUtil.nextInt(random, 1000, 2000);
    int numBits2 = TestUtil.nextInt(random, 1000, 2000);
    
    int count1 = TestUtil.nextInt(random, 0, numBits1 - 1);
    int count2 = TestUtil.nextInt(random, 0, numBits2 - 1);

    int[] bits1 = makeIntArray(random, count1, 0, numBits1 - 1);
    int[] bits2 = makeIntArray(random, count2, 0, numBits2 - 1);
    
    FixedBitSet fixedBitSet1 = makeFixedBitSet(bits1, numBits1);
    FixedBitSet fixedBitSet2 = makeFixedBitSet(bits2, numBits2);
    
    // If ghost bits are present, these may fail too, but that's not what we want to demonstrate here
    //assertTrue(fixedBitSet1.cardinality() <= bits1.length);
    //assertTrue(fixedBitSet2.cardinality() <= bits2.length);
    
    long unionCount = FixedBitSet.unionCount(fixedBitSet1, fixedBitSet2);
    
    java.util.BitSet bitSet1 = makeBitSet(bits1);
    java.util.BitSet bitSet2 = makeBitSet(bits2);
    
    // If ghost bits are present, these may fail too, but that's not what we want to demonstrate here
    //assertEquals(bitSet1.cardinality(), fixedBitSet1.cardinality());
    //assertEquals(bitSet2.cardinality(), fixedBitSet2.cardinality());

    bitSet1.or(bitSet2);
    
    assertEquals(bitSet1.cardinality(), unionCount);
  }
  
  // Demonstrates that the presence of ghost bits in the last used word can cause spurious failures
  public void testAndNotCount() {
    Random random = random();
    
    int numBits1 = TestUtil.nextInt(random, 1000, 2000);
    int numBits2 = TestUtil.nextInt(random, 1000, 2000);
    
    int count1 = TestUtil.nextInt(random, 0, numBits1 - 1);
    int count2 = TestUtil.nextInt(random, 0, numBits2 - 1);

    int[] bits1 = makeIntArray(random, count1, 0, numBits1 - 1);
    int[] bits2 = makeIntArray(random, count2, 0, numBits2 - 1);
    
    FixedBitSet fixedBitSet1 = makeFixedBitSet(bits1, numBits1);
    FixedBitSet fixedBitSet2 = makeFixedBitSet(bits2, numBits2);
    
    // If ghost bits are present, these may fail too, but that's not what we want to demonstrate here
    //assertTrue(fixedBitSet1.cardinality() <= bits1.length);
    //assertTrue(fixedBitSet2.cardinality() <= bits2.length);
    
    long andNotCount = FixedBitSet.andNotCount(fixedBitSet1, fixedBitSet2);
    
    java.util.BitSet bitSet1 = makeBitSet(bits1);
    java.util.BitSet bitSet2 = makeBitSet(bits2);
    
    // If ghost bits are present, these may fail too, but that's not what we want to demonstrate here
    //assertEquals(bitSet1.cardinality(), fixedBitSet1.cardinality());
    //assertEquals(bitSet2.cardinality(), fixedBitSet2.cardinality());

    bitSet1.andNot(bitSet2);
    
    assertEquals(bitSet1.cardinality(), andNotCount);
  }
}
