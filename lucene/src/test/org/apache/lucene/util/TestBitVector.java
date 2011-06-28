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

import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.store.RAMDirectory;

/**
 * <code>TestBitVector</code> tests the <code>BitVector</code>, obviously.
 */
public class TestBitVector extends LuceneTestCase
{

    /**
     * Test the default constructor on BitVectors of various sizes.
     * @throws Exception
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
     * @throws Exception
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
     * @throws Exception
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
     * @throws Exception
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
     * @throws Exception
     */
    public void testWriteRead() throws Exception {
        doTestWriteRead(8);
        doTestWriteRead(20);
        doTestWriteRead(100);
        doTestWriteRead(1000);
    }

    private void doTestWriteRead(int n) throws Exception {
        MockDirectoryWrapper d = new  MockDirectoryWrapper(random, new RAMDirectory());
        d.setPreventDoubleWrite(false);
        BitVector bv = new BitVector(n);
        // test count when incrementally setting bits
        for(int i=0;i<bv.size();i++) {
            assertFalse(bv.get(i));
            assertEquals(i,bv.count());
            bv.set(i);
            assertTrue(bv.get(i));
            assertEquals(i+1,bv.count());
            bv.write(d, "TESTBV");
            BitVector compare = new BitVector(d, "TESTBV");
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
    }
    
    private void doTestDgaps(int size, int count1, int count2) throws IOException {
      MockDirectoryWrapper d = new  MockDirectoryWrapper(random, new RAMDirectory());
      d.setPreventDoubleWrite(false);
      BitVector bv = new BitVector(size);
      for (int i=0; i<count1; i++) {
        bv.set(i);
        assertEquals(i+1,bv.count());
      }
      bv.write(d, "TESTBV");
      // gradually increase number of set bits
      for (int i=count1; i<count2; i++) {
        BitVector bv2 = new BitVector(d, "TESTBV");
        assertTrue(doCompare(bv,bv2));
        bv = bv2;
        bv.set(i);
        assertEquals(i+1,bv.count());
        bv.write(d, "TESTBV");
      }
      // now start decreasing number of set bits
      for (int i=count2-1; i>=count1; i--) {
        BitVector bv2 = new BitVector(d, "TESTBV");
        assertTrue(doCompare(bv,bv2));
        bv = bv2;
        bv.clear(i);
        assertEquals(i,bv.count());
        bv.write(d, "TESTBV");
      }
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
        return equal;
    }
}
