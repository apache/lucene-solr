package org.apache.lucene.codecs.lucene40;

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

import java.io.IOException;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

/**
 * <code>TestBitVector</code> tests the <code>BitVector</code>, obviously.
 */
public class TestBitVector extends LuceneTestCase
{

    /**
     * Test the default constructor on BitVectors of various sizes.
     */
    public void testConstructSize() throws Exception {
        doTestConstructOfSize(8);
        doTestConstructOfSize(20);
        doTestConstructOfSize(100);
        doTestConstructOfSize(1000);
    }

    private void doTestConstructOfSize(int n) {
        BitVector bv = new BitVector(n);
        assertEquals(n,bv.size());
    }

    /**
     * Test the get() and set() methods on BitVectors of various sizes.
     */
    public void testGetSet() throws Exception {
        doTestGetSetVectorOfSize(8);
        doTestGetSetVectorOfSize(20);
        doTestGetSetVectorOfSize(100);
        doTestGetSetVectorOfSize(1000);
    }

    private void doTestGetSetVectorOfSize(int n) {
        BitVector bv = new BitVector(n);
        for(int i=0;i<bv.size();i++) {
            // ensure a set bit can be git'
            assertFalse(bv.get(i));
            bv.set(i);
            assertTrue(bv.get(i));
        }
    }

    /**
     * Test the clear() method on BitVectors of various sizes.
     */
    public void testClear() throws Exception {
        doTestClearVectorOfSize(8);
        doTestClearVectorOfSize(20);
        doTestClearVectorOfSize(100);
        doTestClearVectorOfSize(1000);
    }

    private void doTestClearVectorOfSize(int n) {
        BitVector bv = new BitVector(n);
        for(int i=0;i<bv.size();i++) {
            // ensure a set bit is cleared
            assertFalse(bv.get(i));
            bv.set(i);
            assertTrue(bv.get(i));
            bv.clear(i);
            assertFalse(bv.get(i));
        }
    }

    /**
     * Test the count() method on BitVectors of various sizes.
     */
    public void testCount() throws Exception {
        doTestCountVectorOfSize(8);
        doTestCountVectorOfSize(20);
        doTestCountVectorOfSize(100);
        doTestCountVectorOfSize(1000);
    }

    private void doTestCountVectorOfSize(int n) {
        BitVector bv = new BitVector(n);
        // test count when incrementally setting bits
        for(int i=0;i<bv.size();i++) {
            assertFalse(bv.get(i));
            assertEquals(i,bv.count());
            bv.set(i);
            assertTrue(bv.get(i));
            assertEquals(i+1,bv.count());
        }

        bv = new BitVector(n);
        // test count when setting then clearing bits
        for(int i=0;i<bv.size();i++) {
            assertFalse(bv.get(i));
            assertEquals(0,bv.count());
            bv.set(i);
            assertTrue(bv.get(i));
            assertEquals(1,bv.count());
            bv.clear(i);
            assertFalse(bv.get(i));
            assertEquals(0,bv.count());
        }
    }

    /**
     * Test writing and construction to/from Directory.
     */
    public void testWriteRead() throws Exception {
        doTestWriteRead(8);
        doTestWriteRead(20);
        doTestWriteRead(100);
        doTestWriteRead(1000);
    }

    private void doTestWriteRead(int n) throws Exception {
        MockDirectoryWrapper d = new  MockDirectoryWrapper(random(), new RAMDirectory());
        d.setPreventDoubleWrite(false);
        BitVector bv = new BitVector(n);
        // test count when incrementally setting bits
        for(int i=0;i<bv.size();i++) {
            assertFalse(bv.get(i));
            assertEquals(i,bv.count());
            bv.set(i);
            assertTrue(bv.get(i));
            assertEquals(i+1,bv.count());
            bv.write(d, "TESTBV", newIOContext(random()));
            BitVector compare = new BitVector(d, "TESTBV", newIOContext(random()));
            // compare bit vectors with bits set incrementally
            assertTrue(doCompare(bv,compare));
        }
    }
    
    /**
     * Test r/w when size/count cause switching between bit-set and d-gaps file formats.  
     */
    public void testDgaps() throws IOException {
      doTestDgaps(1,0,1);
      doTestDgaps(10,0,1);
      doTestDgaps(100,0,1);
      doTestDgaps(1000,4,7);
      doTestDgaps(10000,40,43);
      doTestDgaps(100000,415,418);
      doTestDgaps(1000000,3123,3126);
      // now exercise skipping of fully populated byte in the bitset (they are omitted if bitset is sparse)
      MockDirectoryWrapper d = new  MockDirectoryWrapper(random(), new RAMDirectory());
      d.setPreventDoubleWrite(false);
      BitVector bv = new BitVector(10000);
      bv.set(0);
      for (int i = 8; i < 16; i++) {
        bv.set(i);
      } // make sure we have once byte full of set bits
      for (int i = 32; i < 40; i++) {
        bv.set(i);
      } // get a second byte full of set bits
      // add some more bits here 
      for (int i = 40; i < 10000; i++) {
        if (random().nextInt(1000) == 0) {
          bv.set(i);
        }
      }
      bv.write(d, "TESTBV", newIOContext(random()));
      BitVector compare = new BitVector(d, "TESTBV", newIOContext(random()));
      assertTrue(doCompare(bv,compare));
    }
    
    private void doTestDgaps(int size, int count1, int count2) throws IOException {
      MockDirectoryWrapper d = new  MockDirectoryWrapper(random(), new RAMDirectory());
      d.setPreventDoubleWrite(false);
      BitVector bv = new BitVector(size);
      bv.invertAll();
      for (int i=0; i<count1; i++) {
        bv.clear(i);
        assertEquals(i+1,size-bv.count());
      }
      bv.write(d, "TESTBV", newIOContext(random()));
      // gradually increase number of set bits
      for (int i=count1; i<count2; i++) {
        BitVector bv2 = new BitVector(d, "TESTBV", newIOContext(random()));
        assertTrue(doCompare(bv,bv2));
        bv = bv2;
        bv.clear(i);
        assertEquals(i+1, size-bv.count());
        bv.write(d, "TESTBV", newIOContext(random()));
      }
      // now start decreasing number of set bits
      for (int i=count2-1; i>=count1; i--) {
        BitVector bv2 = new BitVector(d, "TESTBV", newIOContext(random()));
        assertTrue(doCompare(bv,bv2));
        bv = bv2;
        bv.set(i);
        assertEquals(i,size-bv.count());
        bv.write(d, "TESTBV", newIOContext(random()));
      }
    }

    public void testSparseWrite() throws IOException {
      Directory d = newDirectory();
      final int numBits = 10240;
      BitVector bv = new BitVector(numBits);
      bv.invertAll();
      int numToClear = random().nextInt(5);
      for(int i=0;i<numToClear;i++) {
        bv.clear(random().nextInt(numBits));
      }
      bv.write(d, "test", newIOContext(random()));
      final long size = d.fileLength("test");
      assertTrue("size=" + size, size < 100);
      d.close();
    }

    public void testClearedBitNearEnd() throws IOException {
      Directory d = newDirectory();
      final int numBits = _TestUtil.nextInt(random(), 7, 1000);
      BitVector bv = new BitVector(numBits);
      bv.invertAll();
      bv.clear(numBits-_TestUtil.nextInt(random(), 1, 7));
      bv.write(d, "test", newIOContext(random()));
      assertEquals(numBits-1, bv.count());
      d.close();
    }

    public void testMostlySet() throws IOException {
      Directory d = newDirectory();
      final int numBits = _TestUtil.nextInt(random(), 30, 1000);
      for(int numClear=0;numClear<20;numClear++) {
        BitVector bv = new BitVector(numBits);
        bv.invertAll();
        int count = 0;
        while(count < numClear) {
          final int bit = random().nextInt(numBits);
          // Don't use getAndClear, so that count is recomputed
          if (bv.get(bit)) {
            bv.clear(bit);
            count++;
            assertEquals(numBits-count, bv.count());
          }
        }
      }

      d.close();
    }

    /**
     * Compare two BitVectors.
     * This should really be an equals method on the BitVector itself.
     * @param bv One bit vector
     * @param compare The second to compare
     */
    private boolean doCompare(BitVector bv, BitVector compare) {
        boolean equal = true;
        for(int i=0;i<bv.size();i++) {
            // bits must be equal
            if(bv.get(i)!=compare.get(i)) {
                equal = false;
                break;
            }
        }
        assertEquals(bv.count(), compare.count());
        return equal;
    }
}
