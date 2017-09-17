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

public class TestLongBitSet extends LuceneTestCase {

  void doGet(java.util.BitSet a, LongBitSet b) {
    assertEquals(a.cardinality(), b.cardinality());
    long max = b.length();
    for (int i=0; i<max; i++) {
      if (a.get(i) != b.get(i)) {
        fail("mismatch: BitSet=["+i+"]="+a.get(i));
      }
    }
  }

  void doNextSetBit(java.util.BitSet a, LongBitSet b) {
    assertEquals(a.cardinality(), b.cardinality());
    int aa=-1;
    long bb=-1;
    do {
      aa = a.nextSetBit(aa+1);
      bb = bb < b.length()-1 ? b.nextSetBit(bb+1) : -1;
      assertEquals(aa,bb);
    } while (aa>=0);
  }

  void doPrevSetBit(java.util.BitSet a, LongBitSet b) {
    assertEquals(a.cardinality(), b.cardinality());
    int aa = a.size() + random().nextInt(100);
    long bb = aa;
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

  void doRandomSets(int maxSize, int iter, int mode) throws IOException {
    java.util.BitSet a0=null;
    LongBitSet b0=null;

    for (int i=0; i<iter; i++) {
      int sz = TestUtil.nextInt(random(), 2, maxSize);
      java.util.BitSet a = new java.util.BitSet(sz);
      LongBitSet b = new LongBitSet(sz);

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
      LongBitSet bb = b.clone(); bb.flip(fromIndex,toIndex);

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

        LongBitSet b_and = b.clone(); assertEquals(b,b_and); b_and.and(b0);
        LongBitSet b_or = b.clone(); b_or.or(b0);
        LongBitSet b_xor = b.clone(); b_xor.xor(b0);
        LongBitSet b_andn = b.clone(); b_andn.andNot(b0);

        assertEquals(a0.cardinality(), b0.cardinality());
        assertEquals(a_or.cardinality(), b_or.cardinality());

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
    LongBitSet b1 = new LongBitSet(numBits);
    LongBitSet b2 = new LongBitSet(numBits);
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
    LongBitSet b1 = new LongBitSet(numBits);
    LongBitSet b2 = new LongBitSet(numBits);
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

  public void testTooLarge() {
    Exception e = expectThrows(IllegalArgumentException.class,
                               () -> {
                                 new LongBitSet(LongBitSet.MAX_NUM_BITS + 1);
                               });
    assertTrue(e.getMessage().startsWith("numBits must be 0 .. "));
  }

  public void testNegativeNumBits() {
    Exception e = expectThrows(IllegalArgumentException.class,
                               () -> {
                                 new LongBitSet(-17);
                               });
    assertTrue(e.getMessage().startsWith("numBits must be 0 .. "));
  }

  public void testSmallBitSets() {
    // Make sure size 0-10 bit sets are OK:
    for(int numBits=0;numBits<10;numBits++) {
      LongBitSet b1 = new LongBitSet(numBits);
      LongBitSet b2 = new LongBitSet(numBits);
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
  
  private LongBitSet makeLongBitSet(int[] a, int numBits) {
    LongBitSet bs;
    if (random().nextBoolean()) {
      int bits2words = LongBitSet.bits2words(numBits);
      long[] words = new long[bits2words + random().nextInt(100)];
      bs = new LongBitSet(words, numBits);
    } else {
      bs = new LongBitSet(numBits);
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
    LongBitSet obs = makeLongBitSet(a, numBits);
    java.util.BitSet bs = makeBitSet(a);
    doPrevSetBit(bs, obs);
  }

  public void testPrevSetBit() {
    checkPrevSetBitArray(new int[] {}, 0);
    checkPrevSetBitArray(new int[] {0}, 1);
    checkPrevSetBitArray(new int[] {0,2}, 3);
  }
  
  
  private void checkNextSetBitArray(int [] a, int numBits) {
    LongBitSet obs = makeLongBitSet(a, numBits);
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
    LongBitSet bits = new LongBitSet(5);
    bits.set(1);
    bits.set(4);
    
    LongBitSet newBits = LongBitSet.ensureCapacity(bits, 8); // grow within the word
    assertTrue(newBits.get(1));
    assertTrue(newBits.get(4));
    newBits.clear(1);
    // we align to 64-bits, so even though it shouldn't have, it re-allocated a long[1]
    assertTrue(bits.get(1));
    assertFalse(newBits.get(1));

    newBits.set(1);
    newBits = LongBitSet.ensureCapacity(newBits, newBits.length() - 2); // reuse
    assertTrue(newBits.get(1));
    
    bits.set(1);
    newBits = LongBitSet.ensureCapacity(bits, 72); // grow beyond one word
    assertTrue(newBits.get(1));
    assertTrue(newBits.get(4));
    newBits.clear(1);
    // we grew the long[], so it's not shared
    assertTrue(bits.get(1));
    assertFalse(newBits.get(1));
  }

  @Nightly
  public void testHugeCapacity() {
    long moreThanMaxInt = (long)Integer.MAX_VALUE + 5;
    
    LongBitSet bits = new LongBitSet(42);
    
    assertEquals(42, bits.length());
    
    LongBitSet hugeBits = LongBitSet.ensureCapacity(bits, moreThanMaxInt);
    
    assertTrue(hugeBits.length() >= moreThanMaxInt);
  }
  
  public void testBits2Words() {
    assertEquals(0, LongBitSet.bits2words(0));
    assertEquals(1, LongBitSet.bits2words(1));
    // ...
    assertEquals(1, LongBitSet.bits2words(64));
    assertEquals(2, LongBitSet.bits2words(65));
    // ...
    assertEquals(2, LongBitSet.bits2words(128));
    assertEquals(3, LongBitSet.bits2words(129));
    // ...
    assertEquals(1 << (31-6), LongBitSet.bits2words((long)Integer.MAX_VALUE + 1)); // == 1L << 31
    assertEquals((1 << (31-6)) + 1, LongBitSet.bits2words((long)Integer.MAX_VALUE + 2)); // == (1L << 31) + 1
    // ...
    assertEquals(1 << (32-6), LongBitSet.bits2words(1L << 32));
    assertEquals((1 << (32-6)) + 1, LongBitSet.bits2words((1L << 32)) + 1);

    // ensure the claimed max num_bits doesn't throw exc; we can't enforce exact values here
    // because the value variees with JVM:
    assertTrue(LongBitSet.bits2words(LongBitSet.MAX_NUM_BITS) > 0);
  }
}
