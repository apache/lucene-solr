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

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Random;

import org.junit.Ignore;

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

  // This shows an easy stack overflow because we're counting recursively.
  @Ignore
  public void testChainedEstimation() {
    MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();

    Random rnd = random();
    Entry first = new Entry();
    try {
      while (true) {
        // Check the current memory consumption and provide the estimate.
        long jvmUsed = memoryMXBean.getHeapMemoryUsage().getUsed(); 
        long estimated = RamUsageEstimator.sizeOf(first);
        System.out.println(String.format(Locale.ROOT, "%10d, %10d",
            jvmUsed, estimated));

        // Make a batch of objects.
        for (int i = 0; i < 5000; i++) {
          first.createNext(new byte[rnd.nextInt(1024)]);
        }
      }
    } catch (OutOfMemoryError e) {
      // Release and quit.
    }
  }

  volatile Object guard;
  
  // This shows an easy stack overflow because we're counting recursively.
  public void testLargeSetOfByteArrays() {
    MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();

    causeGc();
    long before = memoryMXBean.getHeapMemoryUsage().getUsed(); 
    Object [] all = new Object [1000000]; 
    for (int i = 0; i < all.length; i++) {
      all[i] = new byte[random().nextInt(3)];
    }
    causeGc();
    long after = memoryMXBean.getHeapMemoryUsage().getUsed();
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
    MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();

    Object [][] all = new Object [0][];
    try {
      while (true) {
        // Check the current memory consumption and provide the estimate.
        causeGc();
        MemoryUsage mu = memoryMXBean.getHeapMemoryUsage();
        long estimated = shallowSizeOf(all);
        if (estimated > 50 * RamUsageEstimator.ONE_MB) {
          break;
        }

        System.out.println(String.format(Locale.ROOT, "%10s\t%10s\t%10s", 
            RamUsageEstimator.humanReadableUnits(mu.getUsed()),
            RamUsageEstimator.humanReadableUnits(mu.getMax()), 
            RamUsageEstimator.humanReadableUnits(estimated)));

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

  /**
   * Very hacky, very crude, but (sometimes) works. 
   * Don't look, it will burn your eyes out. 
   */
  private void causeGc() {
    List<GarbageCollectorMXBean> garbageCollectorMXBeans = ManagementFactory.getGarbageCollectorMXBeans();
    List<Long> ccounts = new ArrayList<Long>();
    for (GarbageCollectorMXBean g : garbageCollectorMXBeans) {
      ccounts.add(g.getCollectionCount());
    }
    List<Long> ccounts2 = new ArrayList<Long>();
    do {
      System.gc();
      ccounts.clear();
      for (GarbageCollectorMXBean g : garbageCollectorMXBeans) {
        ccounts2.add(g.getCollectionCount());
      }
    } while (ccounts2.equals(ccounts));
  }  
}
