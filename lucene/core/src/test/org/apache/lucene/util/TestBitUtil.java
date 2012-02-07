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

public class TestBitUtil extends LuceneTestCase {

  private static int slowNlz(long x) {
    if (x == 0L) return 64;
    int nlz = 0;
    while ( ((x << nlz) & (1L << 63)) == 0) {
      nlz++;
    }
    return nlz;
  }

  private void checkNlz(long x) {
    assertEquals(slowNlz(x), BitUtil.nlz(x));
    assertEquals(Long.numberOfLeadingZeros(x), BitUtil.nlz(x));
  }
  
  public void testNlz() {
    checkNlz(0L);
    checkNlz(1L);
    checkNlz(-1L);
    for (int i = 1; i <= 63; i++) {
      checkNlz(1L << i);
      checkNlz((1L << i) + (1L << (i>>1)));
    }
  }

  public void testBitUtils() {
    long num = 100000;
    assertEquals( 5, BitUtil.ntz(num) );
    assertEquals( 5, BitUtil.ntz2(num) );
    assertEquals( 5, BitUtil.ntz3(num) );
    
    num = 10;
    assertEquals( 1, BitUtil.ntz(num) );
    assertEquals( 1, BitUtil.ntz2(num) );
    assertEquals( 1, BitUtil.ntz3(num) );

    for (int i=0; i<64; i++) {
      num = 1L << i;
      assertEquals( i, BitUtil.ntz(num) );
      assertEquals( i, BitUtil.ntz2(num) );
      assertEquals( i, BitUtil.ntz3(num) );
    }
  }


  private long testArg(int shift) {
    return (1L << shift) + (1L << (shift>>1));
  }
  
  private long nlzBitUtilBasicLoop(int iters) {
    long sumRes = 0;
    while (iters-- >= 0) {
      for (int i = 1; i <= 63; i++) {
      	long a = testArg(i);
	sumRes += BitUtil.nlz(a);
	sumRes += BitUtil.nlz(a+1);
	sumRes += BitUtil.nlz(a-1);
	sumRes += BitUtil.nlz(a+10);
	sumRes += BitUtil.nlz(a-10);
      }
    }
    return sumRes;
  }
    
  private long nlzLongBasicLoop(int iters) {
    long sumRes = 0;
    while (iters-- >= 0) {
      for (int i = 1; i <= 63; i++) {
      	long a = testArg(i);
	sumRes += Long.numberOfLeadingZeros(a);
	sumRes += Long.numberOfLeadingZeros(a+1);
	sumRes += Long.numberOfLeadingZeros(a-1);
	sumRes += Long.numberOfLeadingZeros(a+10);
	sumRes += Long.numberOfLeadingZeros(a-10);
      }
    }
    return sumRes;
  }

  public void tstPerfNlz() { // See LUCENE-3197, prefer to use Long.numberOfLeadingZeros() over BitUtil.nlz().
    final long measureMilliSecs = 2000;
    final int basicIters = 100000;
    long startTime;
    long endTime;
    long curTime;
    long dummy = 0; // avoid optimizing away

    dummy = 0;
    int bitUtilLoops = 0;
    startTime = System.currentTimeMillis();
    endTime = startTime + measureMilliSecs;
    do {
      dummy += nlzBitUtilBasicLoop(basicIters);
      bitUtilLoops++;
      curTime = System.currentTimeMillis();
    } while (curTime < endTime);
    int bitUtilPsTime = (int) (1000000000 * (curTime - startTime) / (basicIters * 5 * 63 * (float) bitUtilLoops));
    System.out.println("BitUtil nlz time: " + (bitUtilPsTime/1) + " picosec/call, dummy: " + dummy);


    dummy = 0;
    int longLoops = 0;
    startTime = System.currentTimeMillis();
    endTime = startTime + measureMilliSecs;
    do {
      dummy += nlzLongBasicLoop(basicIters);
      longLoops++;
      curTime = System.currentTimeMillis();
    } while (curTime < endTime);
    int longPsTime = (int) (1000000000 * (curTime - startTime) / (basicIters * 5 * 63 * (float) longLoops));
    System.out.println("Long    nlz time: " + longPsTime + " picosec/call, dummy: " + dummy);
  }
}
