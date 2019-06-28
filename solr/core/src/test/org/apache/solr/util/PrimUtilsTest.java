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
package org.apache.solr.util;

import org.apache.solr.SolrTestCase;

import java.util.Arrays;

public class PrimUtilsTest extends SolrTestCase {

  public void testSort() {
    int maxSize = 100;
    int maxVal = 100;
    int[] a = new int[maxSize];
    int[] b = new int[maxSize];

    PrimUtils.IntComparator comparator = new PrimUtils.IntComparator() {
      @Override
      public int compare(int a, int b) {
        return b - a;     // sort in reverse
      }
    };

    for (int iter=0; iter<100; iter++) {
      int start = random().nextInt(maxSize+1);
      int end = start==maxSize ? maxSize : start + random().nextInt(maxSize-start);
      for (int i=start; i<end; i++) {
        a[i] = b[i] = random().nextInt(maxVal);
      }
      PrimUtils.sort(start, end, a, comparator);
      Arrays.sort(b, start, end);
      for (int i=start; i<end; i++) {
        assertEquals(a[i], b[end-(i-start+1)]);
      }
    }
  }

  public void testLongPriorityQueue() {
    int maxSize = 100;
    long[] a = new long[maxSize];
    long[] discards = new long[maxSize];

    for (int iter=0; iter<100; iter++) {
      int discardCount = 0;
      int startSize = random().nextInt(maxSize) + 1;
      int endSize = startSize==maxSize ? maxSize : startSize + random().nextInt(maxSize-startSize);
      int adds = random().nextInt(maxSize+1);
      // System.out.println("startSize=" + startSize + " endSize=" + endSize + " adds="+adds);
      LongPriorityQueue pq = new LongPriorityQueue(startSize, endSize, Long.MIN_VALUE);

      for (int i=0; i<adds; i++) {
        long v = random().nextLong();
        a[i] = v;
        long out = pq.insertWithOverflow(v);
        if (i < endSize) {
          assertEquals(out, Long.MIN_VALUE);
        } else {
          discards[discardCount++] = out;
        }
      }
      assertEquals(Math.min(adds,endSize), pq.size());
      assertEquals(adds, pq.size() + discardCount);

      Arrays.sort(a, 0, adds);
      Arrays.sort(discards, 0, discardCount);
      for (int i=0; i<discardCount; i++) {
        assertEquals(a[i], discards[i]);
      }

      for (int i=discardCount; i<adds; i++) {
        assertEquals(a[i], pq.pop());
      }

      assertEquals(0, pq.size());
    }
  }

}
