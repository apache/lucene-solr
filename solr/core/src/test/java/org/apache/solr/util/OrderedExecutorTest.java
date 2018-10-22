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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.common.util.ExecutorUtil;
import org.junit.Test;

public class OrderedExecutorTest extends LuceneTestCase {

  @Test
  public void testExecutionInOrder() {
    OrderedExecutor orderedExecutor = new OrderedExecutor(10, ExecutorUtil.newMDCAwareCachedThreadPool("executeInOrderTest"));
    IntBox intBox = new IntBox();
    for (int i = 0; i < 100; i++) {
      orderedExecutor.execute(1, () -> intBox.value++);
    }
    orderedExecutor.shutdownAndAwaitTermination();
    assertEquals(intBox.value, 100);
  }

  @Test
  public void testLockWhenQueueIsFull() {
    OrderedExecutor orderedExecutor = new OrderedExecutor(10, ExecutorUtil.newMDCAwareCachedThreadPool("testLockWhenQueueIsFull"));
    IntBox intBox = new IntBox();
    long t = System.nanoTime();
    orderedExecutor.execute(1, () -> {
      try {
        Thread.sleep(500L);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      intBox.value++;
    });
    assertTrue(System.nanoTime() - t < 100 * 1000000);

    t = System.nanoTime();
    orderedExecutor.execute(1, () -> {
      intBox.value++;
    });
    assertTrue(System.nanoTime() - t > 300 * 1000000);
    orderedExecutor.shutdownAndAwaitTermination();
    assertEquals(intBox.value, 2);
  }

  @Test
  public void testRunInParallel() {
    OrderedExecutor orderedExecutor = new OrderedExecutor(10, ExecutorUtil.newMDCAwareCachedThreadPool("testLockWhenQueueIsFull"));
    AtomicInteger atomicInteger = new AtomicInteger(0);
    orderedExecutor.execute(1, () -> {
      try {
        Thread.sleep(500L);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      if (atomicInteger.get() == 1) atomicInteger.incrementAndGet();
    });

    orderedExecutor.execute(2, atomicInteger::incrementAndGet);
    orderedExecutor.shutdownAndAwaitTermination();
    assertEquals(atomicInteger.get(), 2);
  }

  @Test
  public void testStress() {
    int N = random().nextInt(50) + 20;
    Map<Integer, Integer> base = new HashMap<>();
    Map<Integer, Integer> run = new HashMap<>();
    for (int i = 0; i < N; i++) {
      base.put(i, i);
      run.put(i, i);
    }
    OrderedExecutor orderedExecutor = new OrderedExecutor(10, ExecutorUtil.newMDCAwareCachedThreadPool("testStress"));
    for (int i = 0; i < 1000; i++) {
      int key = random().nextInt(N);
      base.put(key, base.get(key) + 1);
      orderedExecutor.execute(key, () -> run.put(key, run.get(key) + 1));
    }
    orderedExecutor.shutdownAndAwaitTermination();
    assertTrue(base.equals(run));
  }

  private static class IntBox {
    int value;
  }
}
