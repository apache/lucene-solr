package org.apache.lucene.util;

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


public class TestBitUtil extends LuceneTestCase {

  private void tstSelect(long x, int r, int exp) {
    if ((0 <= exp) && (exp <= 63)) {
      assertEquals("selectNaive(" + x + "," + r + ")", exp, BitUtil.selectNaive(x, r));
      assertEquals("select(" + x + "," + r + ")", exp, BitUtil.select(x, r));
    } else {
      int act = BitUtil.selectNaive(x, r);
      assertTrue("selectNaive(" + x + "," + r + ")", act < 0 || act > 63);
      act = BitUtil.select(x, r);
      assertTrue("select(" + x + "," + r + ")", act < 0 || act > 63);
    }
  }

  public void testSelectFromZero() {
    tstSelect(0L,1,72);
  }
  public void testSelectSingleBit() {
    for (int i = 0; i < 64; i++) {
      tstSelect((1L << i),1,i);
    }
  }
  public void testSelectTwoBits() {
    for (int i = 0; i < 64; i++) {
      for (int j = i+1; j < 64; j++) {
        long x = (1L << i) | (1L << j);
        //System.out.println(getName() + " i: " + i + " j: " + j);
        tstSelect(x,1,i);
        tstSelect(x,2,j);
        tstSelect(x,3,72);
      }
    }
  }
  public void testSelectThreeBits() {
    for (int i = 0; i < 64; i++) {
      for (int j = i+1; j < 64; j++) {
        for (int k = j+1; k < 64; k++) {
          long x = (1L << i) | (1L << j) | (1L << k);
          tstSelect(x,1,i);
          tstSelect(x,2,j);
          tstSelect(x,3,k);
          tstSelect(x,4,72);
        }
      }
    }
  }
  public void testSelectAllBits() {
    for (int i = 0; i < 64; i++) {
      tstSelect(0xFFFFFFFFFFFFFFFFL,i+1,i);
    }
  }
  public void testPerfSelectAllBits() {
    for (int j = 0; j < 100000; j++) { // 1000000 for real perf test
      for (int i = 0; i < 64; i++) {
        assertEquals(i, BitUtil.select(0xFFFFFFFFFFFFFFFFL, i+1));
      }
    }
  }
  public void testPerfSelectAllBitsNaive() {
    for (int j = 0; j < 10000; j++) { // real perftest: 1000000
      for (int i = 0; i < 64; i++) {
        assertEquals(i, BitUtil.selectNaive(0xFFFFFFFFFFFFFFFFL, i+1));
      }
    }
  }
}

