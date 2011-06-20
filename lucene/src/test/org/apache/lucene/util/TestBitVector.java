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

import org.apache.lucene.index.IOContext;
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
            bv.write(d, "TESTBV", IOContext.DEFAULT);
            BitVector compare = new BitVector(d, "TESTBV", IOContext.DEFAULT);
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
      bv.write(d, "TESTBV", IOContext.DEFAULT);
      // gradually increase number of set bits
      for (int i=count1; i<count2; i++) {
        BitVector bv2 = new BitVector(d, "TESTBV", IOContext.DEFAULT);
        assertTrue(doCompare(bv,bv2));
        bv = bv2;
        bv.set(i);
        assertEquals(i+1,bv.count());
        bv.write(d, "TESTBV", IOContext.DEFAULT);
      }
      // now start decreasing number of set bits
      for (int i=count2-1; i>=count1; i--) {
        BitVector bv2 = new BitVector(d, "TESTBV", IOContext.DEFAULT);
        assertTrue(doCompare(bv,bv2));
        bv = bv2;
        bv.clear(i);
        assertEquals(i,bv.count());
        bv.write(d, "TESTBV", IOContext.DEFAULT);
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
    
    private static int[] subsetPattern = new int[] { 1, 1, 1, 0, 0, 1, 0, 1, 1, 0, 1, 0, 0, 0, 1, 0, 1, 1, 0, 1 };
    
    /**
     * Tests BitVector.subset() against the above pattern
     */
    public void testSubset() {
    	doTestSubset(0, 0);
    	doTestSubset(0, 20);
    	doTestSubset(0, 7);
    	doTestSubset(0, 8);
    	doTestSubset(0, 9);
    	doTestSubset(0, 15);
    	doTestSubset(0, 16);
    	doTestSubset(0, 17);
    	doTestSubset(1, 7);
    	doTestSubset(1, 8);
    	doTestSubset(1, 9);
    	doTestSubset(1, 15);
    	doTestSubset(1, 16);
    	doTestSubset(1, 17);
    	doTestSubset(2, 20);
    	doTestSubset(3, 20);
    	doTestSubset(4, 20);
    	doTestSubset(5, 20);
    	doTestSubset(6, 20);
    	doTestSubset(7, 14);
    	doTestSubset(7, 15);
    	doTestSubset(7, 16);
    	doTestSubset(8, 15);
    	doTestSubset(9, 20);
    	doTestSubset(10, 20);
    	doTestSubset(11, 20);
    	doTestSubset(12, 20);
    	doTestSubset(13, 20);
    }
    
    /**
     * Compare a subset against the corresponding portion of the test pattern
     */
    private void doTestSubset(int start, int end) {
    	BitVector full = createSubsetTestVector();
    	BitVector subset = full.subset(start, end);
    	assertEquals(end - start, subset.size());
    	int count = 0;
    	for (int i = start, j = 0; i < end; i++, j++) {
    		if (subsetPattern[i] == 1) {
    			count++;
    			assertTrue(subset.get(j));
    		} else {
    			assertFalse(subset.get(j));
    		}
    	}
    	assertEquals(count, subset.count());
    }
    
    private BitVector createSubsetTestVector() {
    	BitVector bv = new BitVector(subsetPattern.length);
    	for (int i = 0; i < subsetPattern.length; i++) {
    		if (subsetPattern[i] == 1) {
    			bv.set(i);
    		}
    	}
    	return bv;
    }
    
}
