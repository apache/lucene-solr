package org.apache.lucene.util;

/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2001 The Apache Software Foundation.  All rights
 * reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The end-user documentation included with the redistribution,
 *    if any, must include the following acknowledgment:
 *       "This product includes software developed by the
 *        Apache Software Foundation (http://www.apache.org/)."
 *    Alternately, this acknowledgment may appear in the software itself,
 *    if and wherever such third-party acknowledgments normally appear.
 *
 * 4. The names "Apache" and "Apache Software Foundation" and
 *    "Apache Lucene" must not be used to endorse or promote products
 *    derived from this software without prior written permission. For
 *    written permission, please contact apache@apache.org.
 *
 * 5. Products derived from this software may not be called "Apache",
 *    "Apache Lucene", nor may "Apache" appear in their name, without
 *    prior written permission of the Apache Software Foundation.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL THE APACHE SOFTWARE FOUNDATION OR
 * ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 * USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 */

import junit.framework.TestCase;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

/**
 * <code>TestBitVector</code> tests the <code>BitVector</code>, obviously.
 *
 * @author "Peter Mularien" <pmularien@deploy.com>
 * @version $Id$
 */
public class TestBitVector extends TestCase
{
    public TestBitVector(String s) {
        super(s);
    }

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
        Directory d = new  RAMDirectory();

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
