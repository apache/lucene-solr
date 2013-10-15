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


public class TestBroadWord extends LuceneTestCase {
  private void tstRank(long x) {
    assertEquals("rank(" + x + ")", Long.bitCount(x), BroadWord.bitCount(x));
  }

  public void testRank1() {
    tstRank(0L);
    tstRank(1L);
    tstRank(3L);
    tstRank(0x100L);
    tstRank(0x300L);
    tstRank(0x8000000000000001L);
  }

  private void tstSelect(long x, int r, int exp) {
    assertEquals("selectNaive(" + x + "," + r + ")", exp, BroadWord.selectNaive(x, r));
    assertEquals("select(" + x + "," + r + ")", exp, BroadWord.select(x, r));
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
  public void testPerfSelectAllBitsBroad() {
    for (int j = 0; j < 100000; j++) { // 1000000 for real perf test
      for (int i = 0; i < 64; i++) {
        assertEquals(i, BroadWord.select(0xFFFFFFFFFFFFFFFFL, i+1));
      }
    }
  }
  public void testPerfSelectAllBitsNaive() {
    for (int j = 0; j < 10000; j++) { // real perftest: 1000000
      for (int i = 0; i < 64; i++) {
        assertEquals(i, BroadWord.selectNaive(0xFFFFFFFFFFFFFFFFL, i+1));
      }
    }
  }
  public void testSmalleru_87_01() {
    // 0 <= arguments < 2 ** (k-1), k=8, see paper
    for (long i = 0x0L; i <= 0x7FL; i++) {
      for (long j = 0x0L; i <= 0x7FL; i++) {
        long ii = i * BroadWord.L8_L;
        long jj = j * BroadWord.L8_L;
        assertEquals(ToStringUtils.longHex(ii) + " < " + ToStringUtils.longHex(jj),
            ToStringUtils.longHex((i<j) ? (0x80L * BroadWord.L8_L) : 0x0L),
            ToStringUtils.longHex(BroadWord.smallerUpTo7_8(ii,jj)));
      }
    }
  }

  public void testSmalleru_8_01() {
    // 0 <= arguments < 2 ** k, k=8, see paper
    for (long i = 0x0L; i <= 0xFFL; i++) {
      for (long j = 0x0L; i <= 0xFFL; i++) {
        long ii = i * BroadWord.L8_L;
        long jj = j * BroadWord.L8_L;
        assertEquals(ToStringUtils.longHex(ii) + " < " + ToStringUtils.longHex(jj),
            ToStringUtils.longHex((i<j) ? (0x80L * BroadWord.L8_L): 0x0L),
            ToStringUtils.longHex(BroadWord.smalleru_8(ii,jj)));
      }
    }
  }

  public void testNotEquals0_8() {
    // 0 <= arguments < 2 ** k, k=8, see paper
    for (long i = 0x0L; i <= 0xFFL; i++) {
      long ii = i * BroadWord.L8_L;
      assertEquals(ToStringUtils.longHex(ii) + " <> 0",
          ToStringUtils.longHex((i != 0L) ? (0x80L * BroadWord.L8_L) : 0x0L),
          ToStringUtils.longHex(BroadWord.notEquals0_8(ii)));
    }
  }
}

