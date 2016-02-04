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


import java.util.Arrays;

/**
 * Estimates how {@link RamUsageEstimator} estimates physical memory consumption
 * of Java objects. 
 */
public class StressRamUsageEstimator extends LuceneTestCase {
  static class Entry {
    Object o;
    Entry next;

    public Entry createNext(Object o) {
      Entry e = new Entry();
      e.o = o;
      e.next = next;
      this.next = e;
      return e;
    }
  }

  volatile Object guard;
  
  // This shows an easy stack overflow because we're counting recursively.
  public void testLargeSetOfByteArrays() {

    System.gc();
    long before = Runtime.getRuntime().totalMemory();
    Object [] all = new Object [1000000]; 
    for (int i = 0; i < all.length; i++) {
      all[i] = new byte[random().nextInt(3)];
    }
    System.gc();
    long after = Runtime.getRuntime().totalMemory();
    System.out.println("mx:  " + RamUsageEstimator.humanReadableUnits(after - before));
    System.out.println("rue: " + RamUsageEstimator.humanReadableUnits(shallowSizeOf(all)));

    guard = all;
  }
 
  private long shallowSizeOf(Object[] all) {
    long s = RamUsageEstimator.shallowSizeOf(all);
    for (Object o : all) {
      s+= RamUsageEstimator.shallowSizeOf(o);
    }
    return s;
  }

  private long shallowSizeOf(Object[][] all) {
    long s = RamUsageEstimator.shallowSizeOf(all);
    for (Object[] o : all) {
      s += RamUsageEstimator.shallowSizeOf(o);
      for (Object o2 : o) {
        s += RamUsageEstimator.shallowSizeOf(o2);
      }
    }
    return s;
  }

  public void testSimpleByteArrays() {
    Object [][] all = new Object [0][];
    try {
      while (true) {
        // Check the current memory consumption and provide the estimate.
        System.gc();
        long estimated = shallowSizeOf(all);
        if (estimated > 50 * RamUsageEstimator.ONE_MB) {
          break;
        }

        // Make another batch of objects.
        Object[] seg =  new Object[10000];
        all = Arrays.copyOf(all, all.length + 1);
        all[all.length - 1] = seg;
        for (int i = 0; i < seg.length; i++) {
          seg[i] = new byte[random().nextInt(7)];
        }
      }
    } catch (OutOfMemoryError e) {
      // Release and quit.
    }
  }
}
